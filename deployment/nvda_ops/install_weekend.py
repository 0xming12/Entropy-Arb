from staged_jobs import HERE,REMOTE,SYSTEM,install,sidecar,timer

name="entropy-nvda-weekend-roll"
watch="entropy-nvda-weekend-proof"
unit=f"""[Unit]
Description=Authorized one-time Saturday NVDA held-position recovery check
After=network-online.target

[Service]
Type=oneshot
ExecStart=/opt/entropy-arb/.venv-live/bin/python {REMOTE}/weekend_restart.py
TimeoutStartSec=230
Restart=no
EnvironmentFile=-/etc/entropy-nvda-ops.conf
UMask=0077
"""
files={
    f"{REMOTE}/weekend_restart.py":(HERE/"weekend_restart.py").read_text(),
    f"{REMOTE}/weekend_watch.py":(HERE/"weekend_watch.py").read_text(),
    f"{SYSTEM}/{name}.service":unit,
    f"{SYSTEM}/{name}.timer":timer("Saturday conditional NVDA restart at 11:00 ET",
                                "OnCalendar=2026-11-28 11:00:00 America/New_York"),
    f"{SYSTEM}/{watch}.service":sidecar("Verify organic post-restart client order IDs",
                                      f"/usr/bin/python3 {REMOTE}/weekend_watch.py"),
    f"{SYSTEM}/{watch}.timer":timer("Check Saturday NVDA outcome and order IDs",
                                  "OnCalendar=2026-11-28 11:00:30 America/New_York\nOnUnitInactiveSec=45s"),
}
if __name__ == "__main__":
    install(files,[f"{name}.timer",f"{watch}.timer"])
