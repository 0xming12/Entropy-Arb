"""Install sidecar monitoring or archival jobs without changing the live or recorder units."""
from pathlib import Path

from server import ssm

HERE=Path(__file__).parent
SYSTEM="/etc/systemd/system"
REMOTE="/opt/entropy-nvda-ops"


def install(files,timers,extra=""):
    cmd="""set -eu
before=$(systemctl show entropy-arb-live entropy-arb-record -p MainPID)
install -d -m 0750 /opt/entropy-arb/logs/archives
install -d -m 0755 /opt/entropy-nvda-ops
install -d -m 0700 /var/lib/entropy-nvda-ops
"""
    for path,body in files.items():
        cmd += f"cat > '{path}.next' <<'OPS_JOB_PAYLOAD'\n{body}\nOPS_JOB_PAYLOAD\n"
        if path.endswith(".py"):
            cmd += f"python3 -c 'from pathlib import Path;p=Path(\"{path}.next\");compile(p.read_text(),str(p),\"exec\")'\n"
        cmd += f"chmod 0644 '{path}.next'\nmv -f '{path}.next' '{path}'\n"
    names=" ".join(Path(n).name for n in files if n.startswith(SYSTEM))
    units=" ".join(timers)
    cmd += f"""systemd-analyze verify {names}
systemctl daemon-reload
systemctl enable --now {units}
systemctl list-timers --all --no-legend --no-pager {units}
test "$before" = "$(systemctl show entropy-arb-live entropy-arb-record -p MainPID)"
"""
    print(ssm(cmd+extra,timeout=42))


def sidecar(name, cmd, seconds=40):
    return f"""[Unit]
Description={name}
After=network-online.target

[Service]
Type=oneshot
ExecStart={cmd}
TimeoutStartSec={seconds}
Nice=19
CPUWeight=10
IOWeight=10
IOSchedulingClass=idle
UMask=0077
EnvironmentFile=-/etc/entropy-nvda-ops.conf
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
StateDirectory=entropy-nvda-ops
StateDirectoryMode=0700
"""


def timer(name,rules):
    return f"""[Unit]
Description={name}

[Timer]
{rules}
AccuracySec=1s

[Install]
WantedBy=timers.target
"""


def ops_and_risk():
    jobs={
        f"{REMOTE}/risk_snapshot.py":(HERE/"risk_snapshot.py").read_text(),
        f"{REMOTE}/archive_logs.py":(HERE/"archive_logs.py").read_text(),
        f"{SYSTEM}/entropy-nvda-risk-watch.service":sidecar("Check NVDA liquidation distances",f"/usr/bin/python3 {REMOTE}/risk_snapshot.py watch"),
        f"{SYSTEM}/entropy-nvda-risk-watch.timer": timer("Watch NVDA liquidation distances",
                "OnBootSec=20s\nOnUnitInactiveSec=45s"),
        f"{SYSTEM}/entropy-nvda-risk-snapshot.service":sidecar("Save scheduled NVDA risk report",f"/usr/bin/python3 {REMOTE}/risk_snapshot.py snapshot"),
        f"{SYSTEM}/entropy-nvda-risk-snapshot.timer":timer("NVDA risk reports at 20:00 ET and before open",
                "OnCalendar=2026-11-26 20:00:00 America/New_York\nOnCalendar=2026-11-27 08:45:00 America/New_York"),
        f"{SYSTEM}/entropy-nvda-archive.service":sidecar("Archive old public NVDA logs and check disk capacity",
                f"/usr/bin/nice -n 19 /usr/bin/python3 {REMOTE}/archive_logs.py",1200)+"ReadWritePaths=/opt/entropy-arb/logs/archives\n",
        f"{SYSTEM}/entropy-nvda-archive.timer":timer("Archive and check free space every UTC day",
                "OnCalendar=*-*-* 05:25:00 UTC\nPersistent=yes"),
    }
    install(jobs,[f"entropy-nvda-{s}.timer" for s in ("risk-watch","risk-snapshot","archive")],
            """systemctl start --no-block entropy-nvda-risk-watch.service
systemctl start --no-block entropy-nvda-archive.service
""")


if __name__=="__main__":
    ops_and_risk()
