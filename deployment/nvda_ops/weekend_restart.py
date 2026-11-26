"""Run the authorized Saturday NVDA held-position restart exactly once."""
from datetime import datetime, time as wall_time, timezone
from decimal import Decimal
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
from zoneinfo import ZoneInfo

ROOT=Path("/opt/entropy-arb")
OPS=Path("/var/lib/entropy-nvda-ops")
BASE=OPS/"weekend-restart"
STATE=ROOT/"data/live-test/NVDA_lighter_state.json"
CONFIG=ROOT/"config.live.yaml"
JOURNAL=ROOT/"logs/exec_attempts.jsonl"
LOG=ROOT/"logs/live-test-engine.log"
CONTROL=Path("/opt/entropy-arb-releases/rollout-client-id-f13c897")
COMMIT="f13c897569b27a203d42ccae84e3f9ea405a1a3e"
OLD="abd080fbeb66462a364d2dd1475d394438529952"
RELEASE=CONTROL.parent/COMMIT
GUARD=CONTROL.parent/f"pr3-{OLD}"/"check_orders.py"
LIVE="entropy-arb-live.service"
DROPIN=Path(f"/etc/systemd/system/{LIVE}.d/zz-client-order-id.conf")
PY=ROOT/".venv-live/bin/python"
NY=ZoneInfo("America/New_York")
VERIFY_MISMATCH=re.compile(r"differs from exchange position|exists with no saved ledger|"
                           r"startup blocked|position drift:|unresolved pending order\(s\)",re.I)
START_POS=re.compile(r"starting positions: ENTROPY=([+-]?\S+) LIGHTER=([+-]?\S+)")
ZERO=Decimal(0)


class GuardError(Exception):
    pass


def cmd(*args,timeout=15):
    data=subprocess.run(args,cwd=ROOT,text=True,capture_output=True,timeout=timeout)
    if data.returncode:
        raise GuardError(f"{args[0]} command did not succeed")
    return data.stdout.strip()


def now_stamp():
    n=datetime.now(timezone.utc)
    return {"utc":n.isoformat(),"et":n.astimezone(NY).isoformat()}


def save(path,data):
    tmp=path.with_suffix(".pending")
    tmp.write_text(json.dumps(data,ensure_ascii=False,default=str)+"\n")
    tmp.replace(path)


def status(name,**fields):
    report={"status":name,**now_stamp(),**fields}
    save(BASE/"status.json",report)
    print(json.dumps(report,default=str),flush=True)


def service():
    return dict(line.split("=",1) for line in cmd(
        "systemctl","show",LIVE,"-p","ActiveState","-p","Restart","-p","ExecStart",
        "-p","MainPID","-p","ExecMainStartTimestamp").splitlines())


def validate_config():
    from yaml import safe_load
    config=safe_load(CONFIG.read_text())
    if config["execution"]["execution_mode"]!="entropy_first":
        raise GuardError("execution mode differs from entropy_first")
    if (not config["session_filter"].get("enabled") or
        config["session_filter"].get("off_hours_mode")!="reduce_only"):
        raise GuardError("weekend reduce-only could not be established")
    if (not config["persistence"].get("enabled") or
        (ROOT/config["persistence"]["state_dir"]).resolve()!=STATE.parent):
        raise GuardError("unexpected persistence path")


def live_state():
    r=json.loads(STATE.read_text())
    if (r.get("symbol"),r.get("hedge_venue"))!=("NVDA","lighter"):
        raise GuardError("saved state belongs to another pair")
    if r.get("pending_orders")!={} or r.get("reconcile_required") is not False:
        raise GuardError("saved state has orders or reconciliation pending")
    return {venue:Decimal(str(r["venue_ledger"][venue]["position"]))
            for venue in ("entropy","hedge")}


def exchange():
    scan=subprocess.run(["runuser","-u","entropyarb","--","env",
                         "PYTHONDONTWRITEBYTECODE=1",f"PYTHONPATH={RELEASE}",str(PY),str(GUARD)],
                        cwd=ROOT,text=True,capture_output=True,timeout=25)
    try:
        data=json.loads(scan.stdout)
    except ValueError as error:
        raise GuardError("exchange account verification failed") from error
    if scan.returncode!=0 or any(data.get(k) != 0 for k in
                                 ("pending_orders","entropy_open_orders","lighter_open_orders")) or data.get("reconcile_required") is not False:
        raise GuardError("a venue has open orders or local reconciliation is pending")
    if data.get("session")!="weekend" or data.get("reduce_only") is not True:
        raise GuardError("the engine is outside confirmed weekend reduce-only")
    return {key:sum((Decimal(r["signed_size"]) for r in data[field]
                      if r["symbol"]==symbol),ZERO)
            for key,field,symbol in (("entropy","entropy_positions","io:NVDA"),
                                     ("hedge","lighter_positions","NVDA"))}


def equal(one,two):
    return all(x.is_finite() and two[k].is_finite() and abs(x-two[k])<=Decimal("0.000000001")
               for k,x in one.items())


def backup(label):
    source=STATE.read_bytes()
    destination=BASE/f"NVDA_lighter_state.{label}.json"
    # Store independent copies with exclusive creation, and preserve the originals.
    with destination.open("xb") as fp:
        fp.write(source)
        fp.flush()
        os.fsync(fp.fileno())
    if source!=destination.read_bytes():
        raise GuardError("state backup failed byte-for-byte verification")
    return hashlib.sha256(source).hexdigest()


def start_metrics(offset, watch_pid):
    deadline=time.monotonic()+65
    while time.monotonic()<deadline:
        with LOG.open("rb") as stream:
            stream.seek(offset)
            log=stream.read(1_000_000).decode(errors="replace")
        if VERIFY_MISMATCH.search(log):
            raise GuardError("engine reported restored-state and exchange mismatch")
        now=service()
        if now["ActiveState"]!="active" or now["MainPID"]!=watch_pid:
            raise GuardError("new live service did not stay active")
        positions=START_POS.search(log)
        restored="restored persisted state" in log
        heartbeat="[status]" in log
        mode="mode=entropy_first" in log
        if positions and restored and heartbeat and mode:
            return {"entropy":Decimal(positions[1]),"hedge":Decimal(positions[2])}
        time.sleep(2)
    raise GuardError("startup did not show restored state, entropy_first and a heartbeat")


def perform():
    local=datetime.now(NY)
    if local.date().isoformat()!="2026-11-28" or not wall_time(11)<=local.time()<wall_time(11,10):
        raise GuardError("outside authorized Saturday window")
    validate_config()
    if cmd("git","-C",str(RELEASE),"rev-parse","HEAD")!=COMMIT:
        raise GuardError("staged release SHA does not match f13c897")
    info=service()
    if (info["ActiveState"]!="active" or info["Restart"]!="no"
        or not any(f"{CONTROL.parent}/{sha}/main.py --live --symbol NVDA " in info["ExecStart"] for sha in (OLD,COMMIT))
        or str(CONFIG) not in info["ExecStart"]):
        raise GuardError("unexpected active live unit or restart settings")
    have=exchange()
    if not any(have.values()):
        status("skipped_flat",positions=have)
        return
    if not equal(live_state(),have):
        raise GuardError("persisted positions differ from exchange before restart")
    sha_pre=backup("before_stop")
    config_hash=hashlib.sha256(CONFIG.read_bytes()).hexdigest()
    status("starting",positions=have,old_pid=info["MainPID"],backup_sha256=sha_pre)
    cmd("systemctl","stop",LIVE,timeout=70)
    sha_final=backup("before_start")
    when=exchange()
    if not equal(live_state(),when) or not equal(have,when):
        raise GuardError("verified positions changed while the live service was stopping")
    if hashlib.sha256(CONFIG.read_bytes()).hexdigest()!=config_hash:
        raise GuardError("configuration changed during the maintenance window")
    if COMMIT not in info["ExecStart"]:
        if DROPIN.exists():
            raise GuardError("unexpected competing release drop-in")
        pending=DROPIN.with_suffix(".pending")
        pending.write_text(
            "[Service]\nExecStart=\n"
            f"ExecStart={PY} {RELEASE}/main.py --live --symbol NVDA --hedge lighter "
            f"--config {CONFIG} --env-file {ROOT}/.env --no-dashboard\n"
        )
        pending.chmod(0o644)
        pending.replace(DROPIN)
        cmd("systemctl","daemon-reload")
        cmd("systemd-analyze","verify",LIVE)
    if COMMIT not in service()["ExecStart"]:
        raise GuardError("effective ExecStart does not use f13c897")
    log_offset=LOG.stat().st_size
    journal_offset=JOURNAL.stat().st_size
    cmd("systemctl","start",LIVE,timeout=40)
    new=service()
    if new["MainPID"] in ("0",info["MainPID"]):
        raise GuardError("new live service has no new PID")
    args=Path("/proc",new["MainPID"],"cmdline").read_bytes().split(b"\0")
    if str(RELEASE/"main.py").encode() not in args:
        raise GuardError("new process is running an unexpected release")
    stamp=int(cmd("date","-d",new["ExecMainStartTimestamp"],"+%s"))
    restart=datetime.fromtimestamp(stamp,timezone.utc)
    opened=start_metrics(log_offset,new["MainPID"])
    after=exchange()
    if not equal(opened,when) or not equal(live_state(),after):
        raise GuardError("restored positions or current saved positions differ from the exchanges")
    status("verified",commit=COMMIT,restart_utc=restart.isoformat(),restart_et=restart.astimezone(NY).isoformat(),
           startup_positions=opened,current_positions=after,pending=0,active_orders=0,mode="entropy_first",
           startup_restore_log=True,heartbeats=True,backup_sha256=sha_pre,final_backup_sha256=sha_final,
           journal_offset=journal_offset,client_order_id="awaiting_natural_order",new_pid=new["MainPID"])


def main():
    BASE.mkdir(mode=0o700,exist_ok=True)
    with (CONTROL/"rollout.lock").open("a") as lock:
        try:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            status("failed",reason="an earlier cutover still holds the exclusive lock")
            raise SystemExit(2)
        if (BASE/"status.json").is_file():
            return
        try:
            perform()
        except Exception as error:
            reason=str(error) if isinstance(error,GuardError) else type(error).__name__
            status("failed",reason=reason)
            try:
                from ops import publish
                marker=BASE/"failure-mail.sent"
                mid=publish("NVDA planned Saturday restart failed or blocked",
                            f"{now_stamp()['et']} ET\n{reason}\nNo further restart or manual state-file modification was attempted.")
                marker.write_text(mid+"\n")
            except Exception as mail_error:
                print(f"Failure message delivery pending: {type(mail_error).__name__}",flush=True)
            raise SystemExit(1)


if __name__=="__main__":
    main()
