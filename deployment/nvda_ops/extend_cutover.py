"""Stage a second, mutually exclusive cutover window through Friday 09:00 ET."""
from pathlib import Path
import argparse

from server import ssm

HERE = Path(__file__).parent
SOURCE = HERE / "live-cutover.sh"
DEADLINE_UTC = "2026-11-27T14:00:00Z"
DEPLOY = "/opt/entropy-arb-releases/rollout-client-id-f13c897"
UNIT = "entropy-client-id-cutover-continuation"
WATCH = "/opt/entropy-nvda-ops/ops.py"


def replace_once(body, old, new):
    if body.count(old) != 1:
        raise ValueError("unexpected cutover source")
    return body.replace(old, new)


def continued_script():
    s = SOURCE.read_text()
    s = replace_once(s, "flock -n 9", """flock -w 180 9
deadline=$(date -d '2026-11-27T14:00:00Z' +%s)
earliest=$(date -d '2026-11-26T05:00:00Z' +%s)
if test -e "$dropin"; then
    systemctl is-active --quiet "$unit"
    case "$(systemctl show "$unit" -p ExecStart --value)" in
        *"$release/main.py --live --symbol NVDA "*) exit 0 ;;
        *) exit 2 ;;
    esac
fi""")
    s = replace_once(s, "assert '2026-11-27' in {str(d) for d in c.session_filter.early_close_dates}",
                     """assert '2026-11-26' in {str(d) for d in c.session_filter.holidays}
assert '2026-11-27' in {str(d) for d in c.session_filter.early_close_dates}
assert str(c.session_filter.early_close_end) == '13:00'""")
    s = replace_once(s, 's["session"]=="off_hours"', 's["session"] in ("off_hours", "holiday")')
    start = s.index("while true; do\n    ny_today=")
    end = s.index("printf 'Before stop ", start)
    s = s[:start] + r'''last=""
while true; do
    now=$(date +%s)
    if (( now < earliest )); then
        exit 2
    fi
    if (( now >= deadline )); then
        details=$("$python" - "$last" <<'FINAL_CHECK'
import json,sys
last=json.loads(sys.argv[1]) if sys.argv[1] else {}
positions={name:last.get(name+'_positions') for name in ('entropy','lighter')}
open_orders={name:last.get(name+'_open_orders') for name in ('entropy','lighter')}
reason=("positions_not_flat" if any(positions.values()) else
        "open_or_pending_orders" if any(open_orders.values()) or last.get("pending_orders")
        or last.get("reconcile_required") else "no_safe_cutover_confirmed")
print(json.dumps({"terminal":True, "reason":reason, "deadline_et":"2026-11-27T09:00:00-05:00",
                  "last_check":last}))
FINAL_CHECK
)
        record_status skipped "$details"
        committed=1
        exit 0
    fi
    if before=$(guard_flat); then
        last=$before
        # Allow a second exchange check and a graceful stop before the cutoff.
        # This also prevents starting the deployment too close to the deadline.
        if (( $(date +%s) + 75 < deadline )); then break; fi
        record_status waiting "$before"
    elif [[ "$before" == \{* ]]; then
        last=$before
        record_status waiting "$before"
    else
        last=""
        record_status waiting '{"reason":"exchange or order state check has not cleared"}'
    fi
    remaining=$(( deadline - $(date +%s) ))
    if (( remaining > 60 )); then sleep 60
    elif (( remaining > 0 )); then sleep "$remaining"
    fi
done
''' + s[end:]
    s = replace_once(s, 'if ! after=$(guard_flat); then', 'if ! after=$(guard_flat) || (( $(date +%s) >= deadline )); then')
    return s


def updated_watch():
    p = HERE / "ops.py"
    body = p.read_text()
    old = '''        elif phase in {"failed", "skipped"}:
            event("f13c897/" + phase, "NVDA live cutover " + phase,
                  "The scheduled guarded cutover did not complete. Check its AWS status record.")'''
    new = '''        elif phase == "failed":
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
                  "for the US Thanksgiving holiday and the Friday 13:00 ET close.")'''
    return body if new in body else replace_once(body, old, new)


def install():
    cutover = continued_script()
    watcher = updated_watch()
    timer = f"""[Unit]
Description=Continue guarded NVDA deploy until 09:00 ET Friday

[Timer]
OnCalendar=2026-11-26 00:01:00 America/New_York
AccuracySec=1s
Persistent=yes
Unit={UNIT}.service

[Install]
WantedBy=timers.target
"""
    service = f"""[Unit]
Description=Continue guarded staging of f13c897 until Friday 09:00 ET
After=network-online.target

[Service]
Type=simple
ExecStart=/bin/bash {DEPLOY}/cutover-through-friday.sh
Restart=no
RuntimeMaxSec=34h
UMask=0077
"""
    io_base = "/run/entropy-nvda-iostat"
    io_script = r'''#!/bin/bash
set -eo pipefail
export LC_ALL=C
printf 'utc_epoch,writes_per_sec,write_kib_per_sec,w_await_ms,flushes_per_sec,f_await_ms,queue,util_percent\n'
stdbuf -oL iostat -d -x -y nvme0n1 1 |
    awk '$1=="nvme0n1"{printf "%d,%s,%s,%s,%s,%s,%s,%s\n",systime(),$8,$9,$12,$18,$19,$20,$21;fflush()}'
'''
    io_service = f"""[Unit]
Description=Collect one-second NVDA gp3 disk metrics in memory during the trading session

[Service]
Type=simple
ExecStart=/bin/bash /opt/entropy-nvda-ops/observe-iostat.sh
RuntimeDirectory=entropy-nvda-iostat
RuntimeDirectoryMode=0700
RuntimeDirectoryPreserve=yes
StandardOutput=append:{io_base}/2026-11-27.csv
Nice=19
CPUWeight=10
RuntimeMaxSec=12690
SuccessExitStatus=SIGTERM
"""
    io_timer = """[Unit]
Description=Sample gp3 for the November 27 shortened regular session

[Timer]
OnCalendar=2026-11-27 09:29:30 America/New_York
AccuracySec=1s
Persistent=no

[Install]
WantedBy=timers.target
"""
    bundle = {
        f"{DEPLOY}/cutover-through-friday.sh": (cutover, "0700"),
        f"/etc/systemd/system/{UNIT}.service": (service, "0644"),
        f"/etc/systemd/system/{UNIT}.timer": (timer, "0644"),
        WATCH: (watcher, "0644"),
        "/opt/entropy-nvda-ops/observe-iostat.sh": (io_script, "0755"),
        "/etc/systemd/system/entropy-nvda-iostat.service": (io_service, "0644"),
        "/etc/systemd/system/entropy-nvda-iostat.timer": (io_timer, "0644"),
    }
    cmd = """set -eu
before=$(systemctl show entropy-arb-live entropy-arb-record -p MainPID)
test -d /opt/entropy-arb-releases/rollout-client-id-f13c897
install -d -m 0700 /run/entropy-nvda-iostat
"""
    for path, (body, mode) in bundle.items():
        cmd += f"cat > '{path}.next' <<'ALPHA_PAYLOAD'\n{body}\nALPHA_PAYLOAD\n"
        if path.endswith(".sh"):
            cmd += f"bash -n '{path}.next'\n"
        elif path == WATCH:
            cmd += f"python3 -c 'from pathlib import Path; p=Path(\"{path}.next\"); compile(p.read_text(),str(p),\"exec\")'\n"
        cmd += f"chmod {mode} '{path}.next'\nmv -f '{path}.next' '{path}'\n"
    cmd += f"""systemd-analyze verify {UNIT}.timer {UNIT}.service entropy-nvda-iostat.timer entropy-nvda-iostat.service
systemctl daemon-reload
systemctl enable --now {UNIT}.timer entropy-nvda-iostat.timer
systemctl restart entropy-nvda-watch.service
systemctl is-active entropy-nvda-watch.service {UNIT}.timer entropy-nvda-iostat.timer
systemctl list-timers --all --no-legend --no-pager {UNIT}.timer entropy-nvda-iostat.timer
test "$before" = "$(systemctl show entropy-arb-live entropy-arb-record -p MainPID)"
"""
    print(ssm(cmd, timeout=65))
    (HERE / "cutover-through-friday.sh").write_text(cutover)
    (HERE / "ops.py").write_text(watcher)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--install", action="store_true")
    args = parser.parse_args()
    if args.install:
        install()
    else:
        print(continued_script(), end="")
