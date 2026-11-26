"""Check natural post-restart execution records and deliver held-restart failures."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from ops import publish

BASE=Path("/var/lib/entropy-nvda-ops/weekend-restart")
JOURNAL=Path("/opt/entropy-arb/logs/exec_attempts.jsonl")


def save(path,payload):
    temp=path.with_suffix(".pending")
    temp.write_text(json.dumps(payload)+"\n")
    temp.replace(path)


def run():
    status_path=BASE/"status.json"
    if not status_path.is_file():
        return
    state=json.loads(status_path.read_text())
    phase=state["status"]
    if phase=="failed":
        marker=BASE/"failure-mail.sent"
        if not marker.exists():
            message=publish("NVDA Saturday planned restart failed or blocked",
                            f"{state['et']} ET\n{state.get('reason','unknown cause')}\nPreserved logs and state.")
            marker.write_text(message+"\n")
        return
    if phase=="starting":
        age=datetime.now(timezone.utc)-datetime.fromisoformat(state["utc"])
        marker=BASE/"startup-unverified-mail.sent"
        if age>=timedelta(minutes=5) and not marker.exists():
            message=publish("NVDA Saturday startup result unverified",
                            f"Started at {state['et']} ET, still no verified outcome. Check the service and startup logs.")
            marker.write_text(message+"\n")
        return
    if phase!="verified" or (BASE/"order-id-proof.json").is_file():
        return
    attempt_after=datetime.fromisoformat(state["restart_utc"]).timestamp()
    cursor=BASE/"order-id-cursor.json"
    offset=json.loads(cursor.read_text())["offset"] if cursor.exists() else state["journal_offset"]
    with JOURNAL.open("rb") as handle:
        end=handle.seek(0,2)
        handle.seek(offset if offset<=end else 0)
        for line in iter(handle.readline,b""):
            if not line.endswith(b"\n"):
                break
            offset=handle.tell()
            row=json.loads(line)
            if row.get("type")!="attempt" or row["ts"]<attempt_after:
                continue
            legs=row.get("legs") or {}
            sent={k:v for k,v in legs.items()
                  if any(v.get(field) is not None for field in ("post_start_ms","submit_start_ms"))
                  or v.get("client_order_id")}
            if not sent:
                continue
            valid=row.get("mode")=="entropy_first" and all(v.get("client_order_id") for v in sent.values())
            proof={"time_utc":datetime.fromtimestamp(row["ts"],timezone.utc).isoformat(),
                   "mode":row.get("mode"),"legs_sent":sorted(sent),
                   "legs_with_client_order_id":sorted(k for k,v in sent.items() if v.get("client_order_id")),
                   "valid":valid}
            if not valid:
                publish("NVDA weekend restart order ID check failed",json.dumps(proof))
            save(BASE/"order-id-proof.json",proof)
            break
    save(cursor,{"offset":offset})


if __name__=="__main__":
    run()
