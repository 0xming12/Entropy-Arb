#!/bin/bash
set -Eeuo pipefail
umask 077
root=/opt/entropy-arb
unit=entropy-arb-live.service
release=/opt/entropy-arb-releases/f13c897569b27a203d42ccae84e3f9ea405a1a3e
control=/opt/entropy-arb-releases/rollout-client-id-f13c897
guard=/opt/entropy-arb-releases/pr3-abd080fbeb66462a364d2dd1475d394438529952/check_orders.py
dropin="/etc/systemd/system/$unit.d/zz-client-order-id.conf"
python="$root/.venv-live/bin/python"
cd "$root"
exec 9>"$control/rollout.lock"
flock -w 180 9
deadline=$(date -d '2026-11-27T14:00:00Z' +%s)
earliest=$(date -d '2026-11-26T05:00:00Z' +%s)
if test -e "$dropin"; then
    systemctl is-active --quiet "$unit"
    case "$(systemctl show "$unit" -p ExecStart --value)" in
        *"$release/main.py --live --symbol NVDA "*) exit 0 ;;
        *) exit 2 ;;
    esac
fi

record_status() {
    "$python" - "$control/status.json" "$1" "${2:-}" <<'PYREMOTE'
from datetime import datetime,timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import json,sys
target=Path(sys.argv[1]); now=datetime.now(timezone.utc)
payload={"status":sys.argv[2],"checked_utc":now.isoformat(),
         "checked_et":now.astimezone(ZoneInfo('America/New_York')).isoformat()}
if sys.argv[3]:
    payload.update(json.loads(sys.argv[3]))
pending=target.with_suffix('.tmp')
pending.write_text(json.dumps(payload,indent=2)+"\n")
pending.replace(target)
PYREMOTE
}

stopped=0
changed=0
started=0
committed=0
recover() {
    local rc=$?
    trap - EXIT
    if (( rc != 0 && ! committed )); then
        if (( started )); then
            systemctl stop "$unit" || true
        fi
        if (( changed )); then
            rm -f -- "$dropin"
            systemctl daemon-reload
        fi
        if (( stopped )); then
            systemctl start "$unit" || true
        fi
        record_status failed '{"reason":"cutover command failed before the new process was confirmed"}'
    fi
    exit "$rc"
}
trap recover EXIT
test ! -e "$dropin"
test "$(git -C "$release" rev-parse HEAD)" = f13c897569b27a203d42ccae84e3f9ea405a1a3e
actual=$(systemctl show "$unit" -p ExecStart --value)
case "$actual" in
    *"/opt/entropy-arb-releases/abd080fbeb66462a364d2dd1475d394438529952/main.py --live --symbol NVDA --hedge lighter --config /opt/entropy-arb/config.live.yaml "* ) ;;
    *) echo 'Live command changed since scheduling' >&2; exit 2 ;;
esac
systemctl is-active --quiet "$unit"
runuser -u entropyarb -- env PYTHONPATH="$release" PYTHONDONTWRITEBYTECODE=1 "$python" -P - <<'VALIDATE'
from entropy_arb.config import load_config
from datetime import date
c=load_config('config.live.yaml','.env',symbol='NVDA',hedge_venue='lighter')
assert c.session_filter.enabled and c.session_filter.off_hours_mode=='reduce_only'
assert '2026-11-26' in {str(d) for d in c.session_filter.holidays}
assert '2026-11-27' in {str(d) for d in c.session_filter.early_close_dates}
assert str(c.session_filter.early_close_end) == '13:00'
VALIDATE
guard_flat() {
    local state
    state=$(runuser -u entropyarb -- env PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$release" "$python" "$guard" --require-flat) || {
        printf '%s\n' "$state"
        return 2
    }
    "$python" -c 'import json,sys; s=json.loads(sys.argv[1]); assert s["session"] in ("off_hours", "holiday") and s["reduce_only"]; print(json.dumps(s))' "$state"
}
last=""
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
printf 'Before stop %s\n' "$before"
cfg_hash=$(sha256sum config.live.yaml)
stopped=1
systemctl stop "$unit"
if ! after=$(guard_flat) || (( $(date +%s) >= deadline )); then
    systemctl start "$unit"
    stopped=0
    if [[ "$after" == \{* ]]; then
        record_status skipped "$after"
    else
        record_status skipped '{"reason":"account verification changed while the old service was stopping"}'
    fi
    committed=1
    exit 0
fi
printf 'Before start %s\n' "$after"
test "$(sha256sum config.live.yaml)" = "$cfg_hash"
offset=$(wc -c < logs/live-test-engine.log)
changed=1
cat > "$dropin" <<EOF
[Service]
ExecStart=
ExecStart=$root/.venv-live/bin/python $release/main.py --live --symbol NVDA --hedge lighter --config $root/config.live.yaml --env-file $root/.env --no-dashboard
EOF
chmod 0644 "$dropin"
systemctl daemon-reload
systemd-analyze verify "$(systemctl show "$unit" -p FragmentPath --value)"
case "$(systemctl show "$unit" -p ExecStart --value)" in
    *"$release/main.py --live --symbol NVDA "* ) ;;
    *) exit 2 ;;
esac
systemctl start "$unit"
started=1
systemctl is-active --quiet "$unit"
stamp=$(systemctl show "$unit" -p ExecMainStartTimestamp --value)
pid=$(systemctl show "$unit" -p MainPID --value)
details=$("$python" - "$stamp" "$pid" "$before" "$after" <<'PYREMOTE'
from datetime import datetime,timezone
from zoneinfo import ZoneInfo
import json,subprocess,sys
t=datetime.fromtimestamp(int(subprocess.check_output(['date','-d',sys.argv[1],'+%s'])),timezone.utc)
print(json.dumps({'commit':'f13c897569b27a203d42ccae84e3f9ea405a1a3e',
    'restart_utc':t.isoformat(),'restart_et':t.astimezone(ZoneInfo('America/New_York')).isoformat(),
    'pid':int(sys.argv[2]),'pre_stop':json.loads(sys.argv[3]),'pre_start':json.loads(sys.argv[4])}))
PYREMOTE
)
record_status started "$details"
committed=1
verified=$("$python" - "$offset" "$pid" "$release" <<'PYREMOTE'
import json,subprocess,sys,time
from pathlib import Path
offset,pid,release=sys.argv[1:]
assert str(Path(release)/"main.py").encode() in Path("/proc",pid,"cmdline").read_bytes().split(b"\0")
out=""
for attempt in range(42):
    with Path("logs/live-test-engine.log").open("rb") as log:
        log.seek(int(offset))
        out=log.read().decode(errors="replace")
    if "[status]" in out:
        break
    time.sleep(1)
active=subprocess.run(["systemctl","is-active","--quiet","entropy-arb-live.service"]).returncode==0
print(json.dumps({"active":active,"status_rows":out.count("[status]"),
                  "error_lines":sum(" ERROR " in line or " CRITICAL " in line for line in out.splitlines())}))
PYREMOTE
)
summary=$("$python" -c 'import json,sys; print(json.dumps({**json.loads(sys.argv[1]),**json.loads(sys.argv[2])}))' "$details" "$verified")
record_status deployed "$summary"
printf '%s\n' "$summary"
