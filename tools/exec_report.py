#!/usr/bin/env python3
"""Summarize the execution journal (logs/exec_attempts.jsonl) per trading day.

Prints, for each America/New_York trading day:

  * attempt counts and outcomes: Entropy fill vs zero-fill (reject rate),
    paired fills, skipped follow-ups, imbalanced and unresolved attempts;
  * single-leg exposure duration (median / P95 / max, count over --exposure-limit);
  * latency of each segment between "strategy selected the signal" and the
    exchange acknowledgements (median / P95), to locate where time is spent;
  * signal persistence time for filled vs zero-fill Entropy orders.

按美东交易日汇总执行日志：Entropy 成交/拒单率、单腿暴露时长、各段发单耗时、
信号持续时长与成交率的关系。用于第一周验收和延迟排查。

Usage:
    python3 tools/exec_report.py                         # logs/exec_attempts.jsonl
    python3 tools/exec_report.py --file path.jsonl --days 7 --exposure-limit 10
    python3 tools/exec_report.py --json                  # machine-readable output
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")

# (label, start, end). Points are "mark:<name>" or "<leg>:<field>" in ms from selection.
SEGMENTS = [
    ("lock wait + prepare / 等锁+准备", "mark:selected", "mark:prepared"),
    ("journal fsync / 订单日志落盘", "mark:prepared", "mark:staged"),
    ("pre-send recheck / 发送前复核", "mark:staged", "mark:prechecked"),
    ("entropy: to adapter / 进入适配器", "mark:prechecked", "entropy:call_start_ms"),
    ("entropy: sign / 签名", "entropy:call_start_ms", "entropy:signed_ms"),
    ("entropy: sign→POST / 签名到发出", "entropy:signed_ms", "entropy:post_start_ms"),
    ("entropy: POST round trip / 请求往返(含出块)", "entropy:post_start_ms", "entropy:post_end_ms"),
    ("entropy: parse/poll / 解析或轮询", "entropy:post_end_ms", "entropy:ack_ms"),
    ("TOTAL selected→entropy ack / 决定到 Entropy 回执", "mark:selected", "entropy:ack_ms"),
    ("follow: entropy ack→journal / 回执到对冲落盘", "entropy:ack_ms", "mark:follow_staged"),
    ("follow: journal→hedge call / 落盘到对冲发单", "mark:follow_staged", "hedge:call_start_ms"),
    ("hedge: submit / 对冲提交", "hedge:submit_start_ms", "hedge:submit_end_ms"),
    ("hedge: submit→settle / 提交到成交确认", "hedge:submit_end_ms", "hedge:settle_ms"),
    ("TOTAL selected→hedge ack / 决定到对冲回执", "mark:selected", "hedge:ack_ms"),
]


def pctl(vals, q):
    vals = sorted(v for v in vals if v is not None and math.isfinite(v))
    if not vals:
        return None
    k = (len(vals) - 1) * q / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return vals[int(k)]
    return vals[lo] * (hi - k) + vals[hi] * (k - lo)


def point(rec, ref):
    kind, name = ref.split(":", 1)
    if kind == "mark":
        return (rec.get("marks_ms") or {}).get(name)
    return ((rec.get("legs") or {}).get(kind) or {}).get(name)


def trading_day(ts):
    return datetime.fromtimestamp(float(ts), NY).date().isoformat()


def load(path, days):
    cutoff = time.time() - days * 86400 if days > 0 else 0.0
    attempts, exposures = defaultdict(list), defaultdict(list)
    bad = 0
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                ts = float(rec["ts"])
            except (ValueError, KeyError, TypeError):
                bad += 1
                continue
            if ts < cutoff:
                continue
            bucket = attempts if rec.get("type") == "attempt" else exposures
            if rec.get("type") in ("attempt", "exposure"):
                bucket[trading_day(ts)].append(rec)
    return attempts, exposures, bad


def summarize_day(attempts, exposures, exposure_limit_ms):
    e_sent = [a for a in attempts if "entropy" in (a.get("legs") or {})]
    e_resolved = [a for a in e_sent if not a["legs"]["entropy"].get("unresolved")]
    e_filled = [a for a in e_resolved if (a["legs"]["entropy"].get("filled_base") or 0) > 0]
    e_zero = [a for a in e_resolved if (a["legs"]["entropy"].get("filled_base") or 0) <= 0]
    paired = [a for a in attempts if (a.get("matched") or 0) > 0 and not a.get("unresolved")]
    follow_missing = [a for a in e_filled if "hedge" not in a["legs"]]

    # Exposure: closed inside the attempt (leg gap) or by a later hedge (exposure record).
    expo_ms = [a["leg_gap_ms"] for a in attempts
               if a.get("leg_gap_ms") is not None and not a.get("exposure_open")]
    expo_ms += [x["duration_ms"] for x in exposures if x.get("duration_ms") is not None]
    open_now = [a for a in attempts if a.get("exposure_open")]

    seg = {}
    for label, start, end in SEGMENTS:
        diffs = []
        for a in attempts:
            s, e = point(a, start), point(a, end)
            if s is not None and e is not None:
                diffs.append(e - s)
        if diffs:
            seg[label] = {"n": len(diffs), "p50_ms": pctl(diffs, 50), "p95_ms": pctl(diffs, 95)}

    persist_filled = [a.get("persist_ms") for a in e_filled]
    persist_zero = [a.get("persist_ms") for a in e_zero]
    return {
        "attempts": len(attempts),
        "entropy_sent": len(e_sent),
        "entropy_filled": len(e_filled),
        "entropy_zero_fill": len(e_zero),
        "entropy_unresolved": len(e_sent) - len(e_resolved),
        "entropy_reject_rate": (len(e_zero) / len(e_resolved)) if e_resolved else None,
        "paired_fills": len(paired),
        "entropy_filled_without_follow": len(follow_missing),
        "imbalanced": sum(1 for a in attempts if a.get("imbalanced")),
        "unresolved": sum(1 for a in attempts if a.get("unresolved")),
        "exposure": {
            "n": len(expo_ms),
            "p50_ms": pctl(expo_ms, 50),
            "p95_ms": pctl(expo_ms, 95),
            "max_ms": max(expo_ms) if expo_ms else None,
            "over_limit": sum(1 for v in expo_ms if v > exposure_limit_ms),
            "still_open_after_attempt": len(open_now),
        },
        "segments": seg,
        "persist_ms": {
            "entropy_filled_p50": pctl(persist_filled, 50),
            "entropy_zero_fill_p50": pctl(persist_zero, 50),
        },
    }


def fmt(v, unit=""):
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.1f}{unit}"
    return f"{v}{unit}"


def print_day(day, s, exposure_limit_ms):
    print(f"\n=== {day} (America/New_York) ===")
    rr = s["entropy_reject_rate"]
    print(f"attempts {s['attempts']} | entropy sent {s['entropy_sent']} "
          f"filled {s['entropy_filled']} zero-fill {s['entropy_zero_fill']} "
          f"unresolved {s['entropy_unresolved']} | reject rate "
          f"{'—' if rr is None else f'{rr * 100:.1f}%'}")
    print(f"paired fills {s['paired_fills']} | entropy filled w/o follow "
          f"{s['entropy_filled_without_follow']} | imbalanced {s['imbalanced']} "
          f"| unresolved {s['unresolved']}")
    e = s["exposure"]
    print(f"single-leg exposure: n {e['n']} p50 {fmt(e['p50_ms'], 'ms')} "
          f"p95 {fmt(e['p95_ms'], 'ms')} max {fmt(e['max_ms'], 'ms')} | "
          f"> {exposure_limit_ms / 1000:.0f}s: {e['over_limit']} | "
          f"open after attempt: {e['still_open_after_attempt']}")
    if s["segments"]:
        print("latency (ms)                                         n     p50      p95")
        for label, v in s["segments"].items():
            print(f"  {label:<48} {v['n']:>4} {fmt(v['p50_ms']):>8} {fmt(v['p95_ms']):>8}")
    p = s["persist_ms"]
    print(f"signal persisted before send (p50): entropy filled {fmt(p['entropy_filled_p50'], 'ms')}"
          f" | zero-fill {fmt(p['entropy_zero_fill_p50'], 'ms')}")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--file", default="logs/exec_attempts.jsonl")
    ap.add_argument("--days", type=float, default=0.0, help="only the last N days (0 = all)")
    ap.add_argument("--exposure-limit", type=float, default=10.0,
                    help="seconds; count exposures longer than this (default 10)")
    ap.add_argument("--json", action="store_true", help="print JSON instead of a table")
    args = ap.parse_args(argv)
    try:
        attempts, exposures, bad = load(args.file, args.days)
    except FileNotFoundError:
        print(f"no journal at {args.file}", file=sys.stderr)
        return 1
    limit_ms = args.exposure_limit * 1000.0
    days = sorted(set(attempts) | set(exposures))
    out = {d: summarize_day(attempts.get(d, []), exposures.get(d, []), limit_ms) for d in days}
    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        if not days:
            print("journal is empty")
        for d in days:
            print_day(d, out[d], limit_ms)
        if bad:
            print(f"\n(skipped {bad} unreadable lines)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
