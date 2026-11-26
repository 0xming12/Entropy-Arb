import json
from pathlib import Path
import sys
import tempfile
import time
import types
from unittest.mock import patch

try:
    import boto3
except ImportError:
    sys.modules["boto3"] = types.ModuleType("boto3")
    sys.modules["botocore"] = types.ModuleType("botocore")
    sys.modules["botocore.config"] = types.SimpleNamespace(Config=object)

import ops

with tempfile.TemporaryDirectory() as temp:
    folder = Path(temp)
    eng, journal = folder / "engine.log", folder / "attempts.jsonl"
    eng.touch()
    journal.touch()
    now = time.time()
    date = ops.datetime.fromtimestamp(now, ops.NY).date().isoformat()
    state = {
        "saved_at": now - 18,
        "circuit_breaker": {"day_key": date, "daily_realized_pnl_usd": -16, "loss_tripped": True},
        "venue_ledger": {"entropy": {"position": -0.02}, "hedge": {"position": 0}},
    }
    tracker = {"cursor": {}, "issues": {}, "episode": None, "net_tolerance": 0.001}
    active = {"ActiveState": "active", "ExecMainStartTimestampMonotonic": "1"}
    stopped = {"ActiveState": "inactive", "ExecMainStartTimestampMonotonic": "1"}
    with patch.multiple(ops, ENGINE=eng, JOURNAL=journal), patch.object(ops, "snapshot", return_value=state):
        with patch.object(ops, "systemd", side_effect=[active, active]):
            ops.check(tracker, now)
        eng.write_text("16:00:00.000 CRITICAL engine: MARGIN BREAKER -> REDUCE-ONLY: free margin\n")
        journal.write_text(json.dumps({"type": "exposure", "attempt_id": "closed",
                                       "ts": now - 15, "duration_ms": 12000}) + "\n")
        with patch.object(ops, "systemd", side_effect=[stopped, active]):
            ops.check(tracker, now + 3)
        with patch.object(ops, "systemd", side_effect=[stopped, active]):
            ops.check(tracker, now + 6)
    summaries = [item["subject"] for item in tracker["issues"].values()]
    assert len(summaries) == len(set(summaries)) == 6, summaries
    print("Six alert conditions and deduplication verified with mock log and state data.")
