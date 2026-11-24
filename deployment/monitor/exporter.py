"""Export a sanitized trading dashboard snapshot without opening any network listener."""
import asyncio
import csv
import io
import json
import math
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path("/opt/entropy-arb")
NY = ZoneInfo("America/New_York")
OUTPUT = Path("/run/entropy-monitor-export/overview.json")

STATUS = re.compile(
    r"^\s*(?P<timestamp>\d{2}:\d{2}:\d{2})[^\n]*?\[status\] [^\n]*?"
    r"\| trades (?P<trades>\d+) hedges (?P<hedges>\d+) \| MTM "
    r"(?P<mtm>\$[-+]?\d+(?:\.\d+)?|—)[^\n]*$", re.MULTILINE,
)
STATUS_CODES = {
    "filled": "成交", "canceled": "取消", "cancelled": "取消", "rejected": "拒单",
    "timeout": "超时", "send-failed": "提交失败", "send-unknown": "待确认",
    "unresolved": "待确认", "skipped": "已跳过", "unknown": "待确认",
}
EVENTS = (
    ("*** HALTED ***", "停机"),
    ("CIRCUIT BREAKER", "风控熔断"),
    ("UNHEDGED EXPOSURE", "单腿敞口"),
    ("[PENDING]", "订单对账"),
    ("[HEDGE SETTLED]", "补单成交"),
    ("[SETTLED]", "双边订单返回"),
    ("[PRE-SEND BLOCKED]", "订单发送前被拦截"),
)


def number(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError, OverflowError):
        return None


def read_tail(path, cap=192_000):
    try:
        with path.open("rb") as stream:
            size = stream.seek(0, 2)
            stream.seek(max(0, size - cap))
            data = stream.read(cap)
        if size > cap:
            data = data.partition(b"\n")[2]
        if data and not data.endswith(b"\n"):
            data = data.rsplit(b"\n", 1)[0] if b"\n" in data else b""
        return data.decode("utf-8", errors="replace")
    except OSError:
        return ""


def csv_rows(path, cap):
    try:
        with path.open("rb") as stream:
            header = stream.readline(4096).decode("utf-8", errors="replace")
            header_end = stream.tell()
            size = stream.seek(0, 2)
            offset = max(header_end, size - cap)
            stream.seek(offset)
            data = stream.read(cap)
        if offset > header_end:
            data = data.partition(b"\n")[2]
        if data and not data.endswith(b"\n"):
            data = data.rsplit(b"\n", 1)[0] if b"\n" in data else b""
        return list(csv.DictReader(io.StringIO(header + data.decode("utf-8", errors="replace"))))
    except (OSError, csv.Error):
        return []


def mapping(value):
    return value if isinstance(value, dict) else {}


def snapshot_state(path):
    try:
        with path.open("rb") as stream:
            raw = stream.read(524_289)
        if len(raw) > 524_288:
            return {}
        data = mapping(json.loads(raw))
        return data if (data.get("symbol"), data.get("hedge_venue")) == ("NVDA", "lighter") else {}
    except (OSError, ValueError):
        return {}


def venue_label(raw):
    name = str(raw).lower()
    if name == "entropy":
        return "Entropy"
    if name.startswith("lighter"):
        return "Lighter"
    return "—"


def market_data(root, now, hours):
    choices = [
        (root / "logs/nvda-lighter-minutes.csv", "采集行情"),
        (root / "logs/live-test-minutes.csv", "实盘行情"),
    ]
    all_points = {}
    for path, source in choices:
        for row in csv_rows(path, 388_000):
            ts = number(row.get("minute_ts"))
            spread = number(row.get("premium_mean_bps"))
            samples = number(row.get("samples"))
            if ts is None or spread is None or not samples or not now - hours*3600 <= ts <= now + 60:
                continue
            fields = [number(row.get(k)) for k in ("entropy_bid", "entropy_ask", "hedge_bid", "hedge_ask")]
            all_points[ts] = {"ts": ts, "spread": spread, "quotes": fields, "samples": samples, "source": source}
    points = sorted(all_points.values(), key=lambda row: row["ts"])
    chart = [[row["ts"], row["spread"]] for row in points]
    return {"quotes": points[-1] if points else None, "chart": chart}


def take_trades(path):
    trades = []
    for row in reversed(csv_rows(path, 90_000)):
        ts = number(row.get("ts"))
        if ts is None or (int(row.get("ok") or "0") if row.get("ok") in ("0", "1") else -1) < 0:
            continue
        direction = {"buy_entropy": "买 Entropy", "sell_entropy": "卖 Entropy"}.get(row.get("direction"), "—")
        trades.append({
            "ts": ts, "side": direction, "qty": number(row.get("qty")),
            "buy": venue_label(row.get("buy_venue")), "sell": venue_label(row.get("sell_venue")),
            "buy_fill": number(row.get("buy_fill")), "sell_fill": number(row.get("sell_fill")),
            "buy_status": STATUS_CODES.get(str(row.get("buy_status", "")).lower(), "见交易所"),
            "sell_status": STATUS_CODES.get(str(row.get("sell_status", "")).lower(), "见交易所"),
            "edge": number(row.get("fill_edge_usd")) if row.get("ok") == "1" else None,
        })
        if len(trades) == 14:
            break
    return trades


async def unit_state(unit):
    try:
        proc = await asyncio.create_subprocess_exec(
            "/usr/bin/systemctl", "show", unit, "--property=ActiveState,ActiveEnterTimestamp",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            data, _ = await asyncio.wait_for(proc.communicate(), 2)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            return "unknown", None
    except OSError:
        return "unknown", None
    props = dict(line.split("=", 1) for line in data.decode().splitlines() if "=" in line)
    started = props.get("ActiveEnterTimestamp", "")
    try:
        started = datetime.strptime(started, "%a %Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        started = None
    return props.get("ActiveState", "unknown"), started


def load_live(root, on, started, now):
    s = snapshot_state(root / "data/live-test/NVDA_lighter_state.json")
    cb = mapping(s.get("circuit_breaker"))
    parts = mapping(s.get("daily_pnl_breakdown"))
    ledger = mapping(s.get("venue_ledger"))
    today = datetime.fromtimestamp(now, NY).strftime("%Y-%m-%d")
    day_matches = parts.get("day_key") == today
    risk_matches = cb.get("day_key") == today
    log = read_tail(root / "logs/live-test-engine.log")
    path = root / "logs/live-test-engine.log"
    try:
        log_updated = path.stat().st_mtime
    except OSError:
        log_updated = None
    latest = None
    for match in STATUS.finditer(log):
        latest = match
    runtime = None
    if on and started and log_updated and log_updated >= started and latest:
        # The logger emits UTC time-of-day (the VPS is UTC). Reject older-session
        # status lines after a restart even if a newer startup entry touched the log.
        local = datetime.fromtimestamp(log_updated, timezone.utc)
        hour, minute, second = map(int, latest["timestamp"].split(":"))
        observed = local.replace(hour=hour, minute=minute, second=second, microsecond=0).timestamp()
        if observed > log_updated + 1:
            observed -= 86400
        if started - 1 <= observed <= now and now - observed <= 100:
            runtime = {
                "ts": observed, "mtm": number(latest["mtm"].lstrip("$")),
                "trades": int(latest["trades"]), "hedges": int(latest["hedges"]),
                "reduce_only": "[REDUCE-ONLY]" in latest[0], "halted": "*** HALTED ***" in latest[0],
                "stale": " STALE" in latest[0], "degraded": " DOWN" in latest[0] or " RATE-LTD" in latest[0],
            }
    events = []
    for line in reversed(log.splitlines()):
        match = next((label for marker, label in EVENTS if marker in line), None)
        stamp = re.match(r"\s*(\d\d:\d\d:\d\d)", line)
        if match and stamp and (not events or events[-1] != {"at": stamp[1], "label": match}):
            events.append({"at": stamp[1], "label": match})
        if len(events) == 8:
            break
    return {
        "saved": number(s.get("saved_at")),
        "positions": {name: number(mapping(ledger.get(name)).get("position")) for name in ("entropy", "hedge")},
        "daily": number(cb.get("daily_realized_pnl_usd")) if risk_matches else None,
        "loss_streak": number(cb.get("consecutive_losses")) if risk_matches else None,
        "loss_breaker": cb.get("loss_tripped") is True if risk_matches else None,
        "actual_fee": number(parts.get("actual_fee_usd")) if day_matches else None,
        "estimated_fee": number(parts.get("estimated_fee_usd")) if day_matches else None,
        "funding": number(parts.get("funding_pnl_usd")) if day_matches and parts.get("funding_status") == "confirmed" else None,
        "pending": len(mapping(s.get("pending_orders"))) if s else None,
        "reconcile": s.get("reconcile_required") is True if s else None,
        "runtime": runtime, "events": events,
        "trades": take_trades(root / "logs/live-test-trades.csv"),
    }



async def export_snapshot():
    live, collect = await asyncio.gather(unit_state("entropy-arb-live.service"),
                                          unit_state("entropy-arb-record.service"))
    now = time.time()
    market, portfolio = await asyncio.gather(
        asyncio.to_thread(market_data, ROOT, now, 24),
        asyncio.to_thread(load_live, ROOT, live[0] == "active", live[1], now),
    )
    payload = {"now": now, "mode": live[0], "collector": collect[0],
               **market, "portfolio": portfolio}
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False,
                                     dir=OUTPUT.parent, prefix=".overview-") as temporary:
        path = Path(temporary.name)
        try:
            json.dump(payload, temporary, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
            temporary.flush()
            os.fchmod(temporary.fileno(), 0o640)
            path.replace(OUTPUT)
        finally:
            path.unlink(missing_ok=True)

async def main():
    while True:
        await export_snapshot()
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
