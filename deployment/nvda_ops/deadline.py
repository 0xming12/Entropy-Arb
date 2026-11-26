"""Record the terminal cutover result if the main controller exited early."""
from datetime import datetime, timezone
import fcntl
import json
from pathlib import Path
import subprocess
from zoneinfo import ZoneInfo

CONTROL = Path("/opt/entropy-arb-releases/rollout-client-id-f13c897")
DEADLINE = datetime.fromisoformat("2026-11-27T09:00:00-05:00")
LIVE = "entropy-arb-live.service"
RELEASE = "f13c897569b27a203d42ccae84e3f9ea405a1a3e"


def finalize():
    now = datetime.now(timezone.utc)
    if now < DEADLINE:
        raise SystemExit("cutoff has not arrived")
    path = CONTROL / "status.json"
    with (CONTROL / "rollout.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            old = json.loads(path.read_text())
        except (FileNotFoundError, ValueError):
            old = {}
        if old.get("status") in {"deployed", "started"} or old.get("terminal"):
            return
        active = subprocess.check_output(
            ["systemctl", "show", LIVE, "-p", "ExecStart", "-p", "MainPID", "-p", "ActiveState"],
            text=True, timeout=5,
        )
        if RELEASE in active and "\nActiveState=active" in "\n" + active:
            return
        checked = {k: old[k] for k in ("checked_utc","checked_et","pending_orders","reconcile_required",
                                        "entropy_open_orders","entropy_positions",
                                        "lighter_open_orders","lighter_positions") if k in old}
        now = datetime.now(timezone.utc)
        status = {"status":"skipped","terminal":True, "reason":"no_safe_cutover_confirmed",
                  "deadline_et":DEADLINE.isoformat(),"checked_utc":now.isoformat(),
                  "checked_et":now.astimezone(ZoneInfo("America/New_York")).isoformat(),
                  "last_check":checked}
        temp = path.with_suffix(".deadline")
        temp.write_text(json.dumps(status, indent=2) + "\n")
        temp.replace(path)


if __name__=="__main__":
    finalize()
