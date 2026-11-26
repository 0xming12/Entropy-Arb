"""Read NVDA operational data without changing the trader or collector."""
import argparse
from datetime import datetime, time as wall_time, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
from zoneinfo import ZoneInfo

import boto3
from botocore.config import Config

ROOT = Path("/opt/entropy-arb")
OUT = Path("/var/lib/entropy-nvda-ops")
STATE = ROOT / "data/live-test/NVDA_lighter_state.json"
ENGINE = ROOT / "logs/live-test-engine.log"
JOURNAL = ROOT / "logs/exec_attempts.jsonl"
RELEASE = Path("/opt/entropy-arb-releases/f13c897569b27a203d42ccae84e3f9ea405a1a3e")
CUTOVER = Path("/opt/entropy-arb-releases/rollout-client-id-f13c897/status.json")
UNITS = ("entropy-arb-live.service", "entropy-arb-record.service")
NY = ZoneInfo("America/New_York")
DAYS = {"2026-11-25", "2026-11-27", "2026-11-30",
        "2026-12-01", "2026-12-02", "2026-12-03", "2026-12-04"}
BREAKER = re.compile(r"\b(?:CIRCUIT BREAKER (?:HALTED|REDUCE-ONLY):|HALTED(?::| after \d+ consecutive))|"
                     r"\b(?:FEE|MARGIN) BREAKER -> REDUCE-ONLY:|position drift:.*adopting chain", re.I)


def write_json(path, value):
    pending = path.with_suffix(".tmp")
    pending.write_text(json.dumps(value, ensure_ascii=False) + "\n")
    pending.replace(path)


def snapshot():
    result = json.loads(STATE.read_text())
    if (result.get("symbol"), result.get("hedge_venue")) != ("NVDA", "lighter"):
        raise ValueError("unexpected live state")
    return result


def systemd(unit):
    data = subprocess.check_output(
        ["systemctl", "show", unit, "-p", "ActiveState", "-p", "ExecMainStartTimestampMonotonic"],
        text=True, timeout=3,
    )
    return dict(line.split("=", 1) for line in data.splitlines())


def sns():
    return boto3.client("sns", region_name=os.environ.get("AWS_REGION"),
                        config=Config(connect_timeout=3, read_timeout=5,
                                      retries={"total_max_attempts": 1}))


def publish(subject, content):
    topic = os.environ["ENTROPY_NVDA_SNS_TOPIC_ARN"]
    return sns().publish(TopicArn=topic, Subject=subject, Message=content)["MessageId"]


def report(deliver=False):
    now = datetime.now(NY)
    day = now.date().isoformat()
    close = wall_time(13 if day == "2026-11-27" else 16)
    if day not in DAYS or now.time() < close:
        raise SystemExit("not a scheduled post-close report window")
    result = subprocess.run(
        ["python3", str(RELEASE / "tools/exec_report.py"), "--file", "logs/exec_attempts.jsonl", "--days", "1"],
        cwd=ROOT, text=True, capture_output=True, timeout=25,
    )
    state = snapshot()
    parts = state.get("daily_pnl_breakdown", {})
    breaker = state.get("circuit_breaker", {})
    same_day = parts.get("day_key") == day
    ledger_day = breaker.get("day_key") == day
    funding_status = parts.get("funding_status") if same_day else "unknown"
    funding = parts.get("funding_pnl_usd") if funding_status == "confirmed" else None
    fee = parts.get("actual_fee_usd") if same_day else None
    est = parts.get("estimated_fee_usd") if same_day else None
    realized = breaker.get("daily_realized_pnl_usd") if ledger_day else None
    source_time = datetime.fromtimestamp(state["saved_at"], NY)
    meta = {
        "date_et": day, "sample_et": now.isoformat(timespec="seconds"),
        "state_asof_et": source_time.isoformat(timespec="seconds"),
        "exec_report_returncode": result.returncode,
        "realized_net_including_booked_funding_usd": realized,
        "realized_net_excluding_funding_usd": (realized - funding
                                               if realized is not None and funding is not None else None),
        "confirmed_funding_net_usd": funding, "funding_status": funding_status,
        "actual_fees_usd": fee, "estimated_fees_usd": est,
        "live_service": systemd(UNITS[0])["ActiveState"],
    }
    destination = OUT / "reports"
    destination.mkdir(exist_ok=True)
    if result.returncode != 0:
        meta["exec_report_error"] = result.stderr.strip()
    path = destination / f"{day}.txt"
    pending = path.with_suffix(".tmp")
    pending.write_text(result.stdout)
    pending.replace(path)
    write_json(destination / f"{day}.json", meta)
    if deliver:
        marker = destination / f"{day}.sent"
        if not marker.exists():
            def fmt(value):
                return f"{value:+.6f} USD" if value is not None else "unavailable"
            body = (f"NVDA report {day} ET (snapshot {now.isoformat(timespec='seconds')})\n"
                    f"Net realized incl. booked funding: {fmt(realized)}\n"
                    f"Net realized excl. funding: {fmt(meta['realized_net_excluding_funding_usd'])}\n"
                    f"Confirmed net funding: {fmt(funding)} ({funding_status})\n"
                    f"Actual fees: {fmt(fee)}. Fees still estimated: {fmt(est)}.\n"
                    f"Live service: {meta['live_service']}. Ledger timestamp: {meta['state_asof_et']}.\n"
                    f"{meta.get('exec_report_error', '')}\n--- exec_report.py stdout ---\n{result.stdout}")
            marker.write_text(publish(f"NVDA close report {day} ET", body) + "\n")
    print(json.dumps(meta, ensure_ascii=False))


def complete_lines(path, cursor, skip_backlog=False):
    try:
        with path.open("rb") as source:
            info = os.fstat(source.fileno())
            identity = [info.st_dev, info.st_ino]
            old = cursor.get(str(path))
            offset = info.st_size if skip_backlog else 0
            if old and old["identity"] == identity and old["offset"] <= info.st_size:
                offset = old["offset"]
            source.seek(offset)
            for raw in source:
                if not raw.endswith(b"\n"):
                    break
                yield raw.decode("utf-8", errors="replace").rstrip("\n")
                offset += len(raw)
            cursor[str(path)] = {"identity": identity, "offset": offset}
    except FileNotFoundError:
        pass


def check(tracker, now):
    previous = not tracker["cursor"]
    cursor = tracker["cursor"]
    state = snapshot()
    day = datetime.fromtimestamp(now, NY).date().isoformat()

    def event(key, title, detail):
        if key not in tracker["issues"]:
            tracker["issues"][key] = {"when": now, "subject": title, "detail": detail}

    for unit in UNITS:
        current = systemd(unit)
        status = current["ActiveState"]
        issue = unit + f"/{current['ExecMainStartTimestampMonotonic']}"
        if status in {"inactive", "failed", "deactivating"}:
            event(issue, "NVDA service stopped", f"{unit} is {status}.")
    if CUTOVER.is_file():
        switch = json.loads(CUTOVER.read_text())
        phase = switch.get("status")
        if phase == "deployed":
            event("f13c897/deployed", "NVDA live cutover f13c897",
                  f"Restart UTC: {switch.get('restart_utc')}. ET: {switch.get('restart_et')}. "
                  f"Service active: {switch.get('active')}. Heartbeats: {switch.get('status_rows')}. "
                  f"Error lines: {switch.get('error_lines')}.")
        elif phase == "failed":
            event("f13c897/failed", "NVDA live cutover failed",
                  "The scheduled guarded cutover failed. Check its AWS status record.")
        elif phase == "skipped" and switch.get("terminal"):
            latest = switch.get("last_check") or {}
            def position(key, coin):
                rows = latest.get(key)
                if rows is None:
                    return "unverified"
                return next((r["signed_size"] for r in rows if r.get("symbol") == coin), "0")
            eth = position("entropy_positions", "io:NVDA")
            lig = position("lighter_positions", "NVDA")
            event("f13c897/skipped-final", "NVDA cutover skipped at 09:00 ET",
                  f"Friday 2026-11-27 cutoff reached. Entropy NVDA {eth}, Lighter NVDA {lig} "
                  f"(last verified {latest.get('checked_et', 'unavailable')}). "
                  "Automatic deployment has ended. The exchange calendar was already configured "
                  "for the US Thanksgiving holiday and the Friday 13:00 ET close.")
    breaker = state.get("circuit_breaker", {})
    if breaker.get("day_key") == day:
        net = breaker.get("daily_realized_pnl_usd")
        if net is not None and net < -15:
            event(day + "/loss", "NVDA cumulative loss above 15 U",
                  f"Engine's daily realized net is {net:+.6f} USD, including booked funding.")
        if breaker.get("loss_tripped"):
            event(day + "/loss-breaker", "NVDA circuit breaker",
                  "The daily loss circuit breaker is active. Check the engine log for the cause.")

    for line in complete_lines(ENGINE, cursor, previous):
        if BREAKER.search(line):
            key = hashlib.sha256(line.encode()).hexdigest()
            event(key, "NVDA circuit breaker or engine halt",
                  "A circuit-breaker or HALTED line was written to the live engine log.")

    for line in complete_lines(JOURNAL, cursor, previous):
        try:
            row = json.loads(line)
            stamp = float(row["ts"])
        except (ValueError, KeyError, TypeError):
            continue
        duration = row.get("duration_ms") if row.get("type") == "exposure" else row.get("leg_gap_ms")
        ident = row.get("attempt_id") or str(stamp)
        if duration is not None and duration > 10000:
            event("exposure/" + ident, "NVDA single leg over 10s",
                  f"Journal measured an exposure of {duration / 1000:.3f}s.")
        if row.get("type") == "attempt" and row.get("exposure_open"):
            acks = [x["ack_ms"] for x in row.get("legs", {}).values()
                    if (x.get("filled_base") or 0) > 0 and x.get("ack_ms") is not None]
            opened = stamp + (min(acks) / 1000 if acks else 0)
            tracker["episode"] = tracker.get("episode") or {"id": ident, "start": opened}
        elif row.get("type") == "exposure":
            if (tracker.get("episode") or {}).get("id") == ident:
                tracker["episode"] = None

    positions = state["venue_ledger"]
    net = float(positions["entropy"]["position"]) + float(positions["hedge"]["position"])
    if abs(net) > tracker["net_tolerance"] + 1e-12:
        episode = tracker.get("episode")
        if episode is None:
            t = min(float(state["saved_at"]), now)
            tracker["episode"] = episode = {"id": str(t), "start": t}
        duration = now - episode["start"]
        if duration > 10:
            event("exposure/" + episode["id"], "NVDA single leg still open over 10s",
                  f"Persisted unhedged NVDA quantity {net:+.6g}, open for about {duration:.1f}s.")
    else:
        tracker["episode"] = None


def watch():
    import yaml

    path = OUT / "watch.json"
    try:
        tracker = json.loads(path.read_text())
    except FileNotFoundError:
        tracker = {"cursor": {}, "issues": {}, "episode": None}
    config = yaml.safe_load((ROOT / "config.live.yaml").read_text())
    tracker["net_tolerance"] = float(config.get("execution", {}).get("net_tolerance_base", 0.001))
    while True:
        now = time.time()
        try:
            check(tracker, now)
            write_json(path, tracker)
        except (OSError, subprocess.SubprocessError, ValueError, KeyError) as error:
            print(f"Watch read failed: {type(error).__name__}", flush=True)
        for item in tracker["issues"].values():
            if "message_id" in item or item.get("last_attempt", 0) > now - 20:
                continue
            item["last_attempt"] = now
            when = datetime.fromtimestamp(item["when"], NY).isoformat(timespec="seconds")
            try:
                item["message_id"] = publish(item["subject"], f"{when} ET\n{item['detail']}")
                print(f"Published: {item['subject']}", flush=True)
            except Exception as error:
                print(f"SNS publish retry: {type(error).__name__}", flush=True)
            write_json(path, tracker)
            break
        time.sleep(3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("watch", "report", "test-sns"))
    parser.add_argument("--deliver", action="store_true")
    args = parser.parse_args()
    if args.command == "watch":
        watch()
    elif args.command == "report":
        report(args.deliver)
    else:
        print(publish("NVDA alerts connection check",
                      "This is an AWS SNS delivery check for the NVDA watcher. No trade or order was placed."))
