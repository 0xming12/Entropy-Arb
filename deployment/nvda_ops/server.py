"""Deploy or read the independent NVDA reporting and alert jobs."""
import argparse
import json
from pathlib import Path
import subprocess
import time

from remote import cli, instance, invocation

HERE = Path(__file__).parent
WATCH = """[Unit]
Description=Read-only NVDA service, risk and exposure monitor
After=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 /opt/entropy-nvda-ops/ops.py watch
User=root
StateDirectory=entropy-nvda-ops
StateDirectoryMode=0700
UMask=0077
EnvironmentFile=-/etc/entropy-nvda-ops.conf
Nice=19
CPUWeight=10
IOWeight=10
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
"""
SERVICE = """[Unit]
Description=NVDA after-close execution and PnL report
After=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/bin/python3 /opt/entropy-nvda-ops/ops.py report{deliver}
StateDirectory=entropy-nvda-ops
StateDirectoryMode=0700
UMask=0077
EnvironmentFile=-/etc/entropy-nvda-ops.conf
Nice=19
CPUWeight=10
IOWeight=10
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
Restart=on-failure
RestartSec=30
"""
CALENDAR = """[Unit]
Description=NVDA execution report at US equity close plus ten minutes

[Timer]
OnCalendar=2026-11-25 16:10:00 America/New_York
OnCalendar=2026-11-27 13:10:00 America/New_York
OnCalendar=2026-11-30 16:10:00 America/New_York
OnCalendar=2026-12-01..04 16:10:00 America/New_York
AccuracySec=1s
Persistent=no

[Install]
WantedBy=timers.target
"""


def ssm(script, timeout=40):
    command = cli("send-command", "--instance-ids", instance(), "--document-name", "AWS-RunShellScript",
                  "--comment", "NVDA independent reporting, monitoring or public data check",
                  "--parameters", json.dumps({"commands": [script], "executionTimeout": [str(timeout)]}),
                  "--query", "Command.CommandId", "--output", "text")
    for attempt in range(timeout + 12):
        try:
            outcome = invocation(command)
        except subprocess.CalledProcessError:
            time.sleep(1)
            continue
        if outcome["Status"] not in {"Pending", "InProgress", "Delayed"}:
            if outcome["Status"] != "Success":
                raise RuntimeError(outcome["Status"] + "\n" + outcome["StandardErrorContent"])
            return outcome["StandardOutputContent"]
        time.sleep(1)
    raise TimeoutError(command)


def deploy(mail):
    script = """set -eu
install -d -m 0755 /opt/entropy-nvda-ops
install -d -m 0700 /var/lib/entropy-nvda-ops
"""
    files = {
        "/opt/entropy-nvda-ops/ops.py": (HERE / "ops.py").read_text(),
        "/etc/systemd/system/entropy-nvda-watch.service": WATCH,
        "/etc/systemd/system/entropy-nvda-report.service": SERVICE.format(deliver=" --deliver" if mail else ""),
        "/etc/systemd/system/entropy-nvda-report.timer": CALENDAR,
    }
    for path, body in files.items():
        script += f"cat > {path}.new <<'FILE_PAYLOAD'\n{body}\nFILE_PAYLOAD\n"
        script += f"chmod 0644 {path}.new\nmv {path}.new {path}\n"
    script += """systemd-analyze verify entropy-nvda-watch.service entropy-nvda-report.service entropy-nvda-report.timer
before=$(systemctl show entropy-arb-live entropy-arb-record -p MainPID)
systemctl daemon-reload
systemctl enable --now entropy-nvda-watch.service entropy-nvda-report.timer
systemctl restart entropy-nvda-watch.service
sleep 4
systemctl is-active entropy-nvda-watch.service entropy-nvda-report.timer
systemctl list-timers --all --no-legend --no-pager entropy-nvda-report.timer
test "$before" = "$(systemctl show entropy-arb-live entropy-arb-record -p MainPID)"
python3 -c 'import json; from pathlib import Path; s=json.loads(Path("/var/lib/entropy-nvda-ops/watch.json").read_text()); print("logs tailed:",len(s["cursor"]),"queued alerts:",len(s["issues"]))'
"""
    print(ssm(script))


def read_report(day):
    from datetime import date
    day = date.fromisoformat(day).isoformat()
    result = ssm(f"""set -eu
cd /var/lib/entropy-nvda-ops/reports
cat {day}.json
printf '%s\n' '--- unmodified stdout ---'
cat {day}.txt
""", timeout=15)
    print(result, end="")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("deploy", "report", "test"))
    parser.add_argument("day", nargs="?")
    parser.add_argument("--email-reports", action="store_true")
    args = parser.parse_args()
    if args.action == "deploy":
        deploy(args.email_reports)
    elif args.action == "report":
        read_report(args.day)
    else:
        print(ssm("python3 /opt/entropy-nvda-ops/ops.py test-sns"))
