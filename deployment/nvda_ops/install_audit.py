"""Stage the disk and early-close audit timers for the dated NVDA rollout."""
from io_report import REMOTE as IO_METRICS
from staged_jobs import HERE, REMOTE, SYSTEM, install, sidecar, timer


def main():
    audit = "entropy-nvda-friday-audit"
    disk = "entropy-nvda-iostat"
    deadline = "entropy-client-id-cutover-deadline"
    files = {
        f"{REMOTE}/io_metrics.py": IO_METRICS,
        f"{REMOTE}/observe-iostat.sh": (HERE / "observe-iostat.sh").read_text(),
        f"{REMOTE}/friday_audit.py": (HERE / "friday_audit.py").read_text(),
        f"{REMOTE}/deadline.py": (HERE / "deadline.py").read_text(),
        f"{SYSTEM}/{audit}.service": sidecar("Audit early-close execution and disk timing",
                                            f"/usr/bin/python3 {REMOTE}/friday_audit.py", 70),
        f"{SYSTEM}/{audit}.timer": timer("Save both early-close audits",
                                         "OnCalendar=2026-11-27 13:12:00 America/New_York\n"
                                         "OnCalendar=2026-11-28 00:03:00 America/New_York"),
        f"{SYSTEM}/{disk}.service": f"""[Unit]
Description=Sample gp3 writes at one-second intervals

[Service]
Type=simple
ExecStart=/bin/bash {REMOTE}/observe-iostat.sh
EnvironmentFile=-/etc/entropy-nvda-ops.conf
RuntimeDirectory=entropy-nvda-iostat
RuntimeDirectoryMode=0700
RuntimeDirectoryPreserve=yes
StandardOutput=append:/run/entropy-nvda-iostat/2026-11-27.csv
Nice=19
CPUWeight=10
RuntimeMaxSec=12690
SuccessExitStatus=SIGTERM
""",
        f"{SYSTEM}/{disk}.timer": timer("Sample gp3 during the shortened regular session",
                                       "OnCalendar=2026-11-27 09:29:30 America/New_York"),
        f"{SYSTEM}/{deadline}.service": f"""[Unit]
Description=Record the final cutoff outcome

[Service]
Type=oneshot
ExecStart=/usr/bin/python3 {REMOTE}/deadline.py
UMask=0077
TimeoutStartSec=55s
""",
        f"{SYSTEM}/{deadline}.timer": timer("Enforce the guarded early-close cutoff",
                                           "OnCalendar=2026-11-27 09:00:00 America/New_York\nPersistent=yes"),
    }
    install(files, [f"{audit}.timer",f"{disk}.timer",f"{deadline}.timer"])


if __name__ == "__main__":
    main()
