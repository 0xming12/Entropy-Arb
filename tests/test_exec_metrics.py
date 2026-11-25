"""Execution timing journal and single-leg exposure tracking.

Covers: per-attempt timing marks and leg outcomes in entropy_first and parallel
modes, exposure episodes that outlive the attempt, adapter-level timing marks,
journal disabling/robustness, and the tools/exec_report.py summary.
"""
import asyncio
import json
import os
import sys
import time
from types import ModuleType, SimpleNamespace

import pytest

from entropy_arb.config import HLCreds, VenueConf
from entropy_arb.exec_metrics import ExecJournal, ExposureTracker
from entropy_arb.venue_hl import HLVenue
from test_live_risk_regression import make_engine
from test_option_b_execution import _attach_order_ids

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))
import exec_report  # noqa: E402

FIRST_YAML = """
thresholds:
  midline_bps: 0.0
  upper_bps: 4.0
  lower_bps: 4.0
  latency_buffer_bps: 4.0
sizing:
  max_order_notional_usd: 15.0
execution:
  execution_mode: {mode}
  premium_persist_sec: 0.0
  leg_slippage_bps: 15.0
logging:
  exec_journal: "{journal}"
persistence:
  enabled: true
  state_dir: "{state_dir}"
"""


def _engine(tmp_path, mode="entropy_first"):
    journal = tmp_path / "exec.jsonl"
    eng = make_engine(FIRST_YAML.format(mode=mode, journal=journal, state_dir=tmp_path),
                      hedge="lighter", px_step_e=0.01, px_step_h=0.001)
    eng._log_csv = lambda *_: None
    eng._step, eng._min_base = 0.001, 0.025
    eng.entropy.size_decimals, eng.entropy.min_base = 3, 0.025
    eng.hedge.size_decimals, eng.hedge.min_base = 3, 0.025
    _attach_order_ids(eng)
    eng.hedge.set_book(164.99, 165.00)
    eng.entropy.set_book(165.20, 165.22)
    return eng, journal


def _records(path):
    with open(path, encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def test_entropy_first_zero_fill_journals_entropy_leg_only(tmp_path):
    eng, journal = _engine(tmp_path)
    eng.entropy.send_responses = [
        {"status": "canceled", "outcome": "canceled", "filled_base": 0.0,
         "avg_px": None, "err": None, "unresolved": False}
    ]
    plan, _ = eng._plan(eng.hedge, eng.entropy, 15.0)
    decision = {"selected_mono": time.monotonic() - 0.010, "selected_wall": time.time() - 0.010,
                "persist_sec": 0.75}
    asyncio.run(eng._execute(eng.hedge, eng.entropy, plan, decision=decision))

    (rec,) = _records(journal)
    assert rec["type"] == "attempt" and rec["mode"] == "entropy_first"
    assert rec["direction"] == "sell_entropy"
    assert set(rec["legs"]) == {"entropy"}
    leg = rec["legs"]["entropy"]
    assert leg["status"] == "canceled" and leg["filled_base"] == 0.0
    assert leg["client_order_id"] == "entropy-cid-1"
    assert rec["leg_gap_ms"] is None and rec["exposure_open"] is False
    assert rec["persist_ms"] == pytest.approx(750.0)
    marks = rec["marks_ms"]
    # The decision happened ~10 ms before execution started (lock wait).
    assert marks["selected"] == 0.0 and marks["exec_start"] >= 9.0
    assert marks["exec_start"] <= marks["prepared"] <= marks["staged"] <= marks["prechecked"]
    assert marks["prechecked"] <= leg["call_start_ms"] <= leg["ack_ms"]
    assert rec["entropy_book_age_ms"] is not None and rec["hedge_book_age_ms"] is not None
    assert not eng.exposure.is_open


def test_entropy_first_paired_fill_records_follow_timing_and_leg_gap(tmp_path):
    eng, journal = _engine(tmp_path)
    eng.entropy.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 0.09, "avg_px": 165.20,
         "err": None, "unresolved": False, "fee_bps": 0.86, "fee_usd": 0.0013}
    ]
    eng.hedge.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 0.09, "avg_px": 165.00,
         "err": None, "unresolved": False, "fee_bps": 0.0, "fee_usd": 0.0,
         "timing": {"submit_start_mono": time.monotonic(), "submit_end_mono": time.monotonic(),
                    "settle_mono": time.monotonic()}}
    ]
    plan, _ = eng._plan(eng.hedge, eng.entropy, 15.0)
    asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))

    (rec,) = _records(journal)
    assert set(rec["legs"]) == {"entropy", "hedge"}
    e, h = rec["legs"]["entropy"], rec["legs"]["hedge"]
    assert (e["client_order_id"], h["client_order_id"]) == ("entropy-cid-1", "hedge-cid-2")
    assert e["ack_ms"] <= rec["marks_ms"]["follow_staged"] <= h["call_start_ms"] <= h["ack_ms"]
    assert {"submit_start_ms", "submit_end_ms", "settle_ms"} <= set(h)
    # Both timestamps and the gap are independently rounded to 0.001 ms.
    assert rec["leg_gap_ms"] == pytest.approx(h["ack_ms"] - e["ack_ms"], abs=2e-3)
    assert rec["matched"] == pytest.approx(0.09)
    assert rec["exposure_open"] is False and not eng.exposure.is_open


def test_exposure_episode_outlives_attempt_until_hedged_flat(tmp_path):
    eng, journal = _engine(tmp_path)
    orig = eng.entropy.send_taker

    async def fill_while_hedge_disconnects(**kwargs):
        eng.hedge.book.clear()  # follow-up cannot be sent
        res = await orig(**kwargs)
        return dict(res, filled_base=0.090, avg_px=165.20, fee_bps=0.86, fee_usd=0.0013)

    eng.entropy.send_taker = fill_while_hedge_disconnects
    plan, _ = eng._plan(eng.hedge, eng.entropy, 15.0)
    asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))
    (attempt,) = _records(journal)
    assert attempt["exposure_open"] is True
    assert set(attempt["legs"]) == {"entropy"}
    assert eng.exposure.is_open and eng.exposure.attempt_id == attempt["attempt_id"]

    asyncio.run(eng._maybe_hedge())  # flattens on Entropy (see option B tests)
    assert eng._net_position() == pytest.approx(0.0)
    attempt2, exposure = _records(journal)
    assert exposure["type"] == "exposure"
    assert exposure["attempt_id"] == attempt["attempt_id"]
    assert exposure["duration_ms"] >= 0.0
    assert exposure["peak_abs_net"] == pytest.approx(0.090)
    assert not eng.exposure.is_open


def test_parallel_mode_records_both_legs_with_concurrent_timing(tmp_path):
    eng, journal = _engine(tmp_path, mode="parallel")
    plan, _ = eng._plan(eng.hedge, eng.entropy, 15.0)
    asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))
    (rec,) = _records(journal)
    assert rec["mode"] == "parallel"
    assert set(rec["legs"]) == {"entropy", "hedge"}
    assert "follow_staged" not in rec["marks_ms"]
    assert rec["leg_gap_ms"] is not None and rec["exposure_open"] is False


def test_empty_journal_path_disables_writes(tmp_path):
    eng, journal = _engine(tmp_path)
    eng.exec_journal.path = ""
    plan, _ = eng._plan(eng.hedge, eng.entropy, 15.0)
    asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))
    assert not journal.exists()


def test_journal_write_failure_never_raises(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("x")
    j = ExecJournal(str(blocker / "sub" / "exec.jsonl"))  # parent is a regular file
    j.write({"type": "attempt", "ts": 1.0})  # must not raise


def test_exposure_tracker_waits_for_reconcile_before_closing(tmp_path):
    j = ExecJournal(str(tmp_path / "x.jsonl"))
    t = ExposureTracker(j)
    t.open(start_mono=10.0, start_wall=1000.0, net=0.5, attempt_id="a1")
    assert t.update(net=0.0, tolerance=0.001, can_close=False, now_mono=11.0) is None
    assert t.is_open
    assert t.update(net=0.0, tolerance=0.001, can_close=True, now_mono=12.5) == pytest.approx(2.5)
    (rec,) = _records(tmp_path / "x.jsonl")
    assert rec["duration_ms"] == pytest.approx(2500.0) and rec["attempt_id"] == "a1"


def test_hl_adapter_attaches_sign_and_post_timing(monkeypatch):
    fake_types = ModuleType("hyperliquid.utils.types")
    fake_types.Cloid = SimpleNamespace(from_str=lambda s: s)
    monkeypatch.setitem(sys.modules, "hyperliquid.utils.types", fake_types)
    conf = VenueConf(
        key="entropy", kind="hl", label="ENTROPY", symbol="NVDA",
        fee_bps=1.0, cap_usd=1000.0, orders_per_min=120, hl_dex="io",
        hl_creds=HLCreds("0x" + "1" * 64, "0x" + "a" * 40),
    )
    hl = HLVenue(conf, "https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws",
                 session=None, settle_timeout_sec=5.0)
    hl.coin, hl.asset_id = "io:NVDA", 110000
    hl._signing = SimpleNamespace(
        order_request_to_order_wire=lambda req, aid: {"a": aid},
        order_wires_to_order_action=lambda wires: {"type": "order", "orders": wires},
        sign_l1_action=lambda *a, **k: {"r": "0x1", "s": "0x2", "v": 27},
    )
    hl.account = SimpleNamespace(
        wallet=SimpleNamespace(address="0x" + "b" * 40), query_address="0x" + "a" * 40,
        is_mainnet=True, nonces=SimpleNamespace(next=lambda: int(time.time() * 1000)),
    )

    async def fake_post(payload):
        return ({"status": "ok", "response": {"data": {"statuses": [
            {"filled": {"totalSz": "0.1", "avgPx": "144.0", "oid": 1}}]}}}, None, False)

    hl._post_exchange = fake_post
    res = asyncio.run(hl.send_taker(is_buy=True, qty=0.1, limit_px=144.0))
    t = res["timing"]
    assert t["signed_mono"] <= t["post_start_mono"] <= t["post_end_mono"]


def test_exec_report_summarizes_reject_rate_exposure_and_segments(tmp_path):
    path = tmp_path / "exec.jsonl"
    ts = 1795500000.0  # 2026-11-24 ~ 06:40 New York
    base = {"type": "attempt", "mode": "entropy_first", "direction": "sell_entropy",
            "reduce_only": False}
    recs = [
        dict(base, attempt_id="1", ts=ts, persist_ms=500.0, leg_gap_ms=None,
             exposure_open=False, matched=0.0,
             marks_ms={"selected": 0.0, "prepared": 2.0, "staged": 12.0, "prechecked": 13.0},
             legs={"entropy": {"filled_base": 0.0, "unresolved": False, "call_start_ms": 13.0,
                               "signed_ms": 20.0, "post_start_ms": 21.0, "post_end_ms": 500.0,
                               "ack_ms": 501.0}}),
        dict(base, attempt_id="2", ts=ts + 60, persist_ms=900.0, leg_gap_ms=400.0,
             exposure_open=False, matched=0.1,
             marks_ms={"selected": 0.0, "prepared": 2.0, "staged": 10.0, "prechecked": 11.0,
                       "follow_staged": 330.0},
             legs={"entropy": {"filled_base": 0.1, "unresolved": False, "call_start_ms": 11.0,
                               "ack_ms": 300.0},
                   "hedge": {"filled_base": 0.1, "unresolved": False, "call_start_ms": 331.0,
                             "ack_ms": 700.0}}),
        dict(base, attempt_id="3", ts=ts + 120, persist_ms=600.0, leg_gap_ms=None,
             exposure_open=True, matched=0.0,
             marks_ms={"selected": 0.0},
             legs={"entropy": {"filled_base": 0.1, "unresolved": False, "ack_ms": 250.0}}),
        {"type": "exposure", "attempt_id": "3", "ts": ts + 120, "duration_ms": 12000.0},
    ]
    path.write_text("\n".join(json.dumps(r) for r in recs) + "\nnot json\n")
    attempts, exposures, bad = exec_report.load(str(path), 0)
    assert bad == 1
    (day,) = attempts
    s = exec_report.summarize_day(attempts[day], exposures[day], 10_000.0)
    assert s["entropy_sent"] == 3 and s["entropy_filled"] == 2 and s["entropy_zero_fill"] == 1
    assert s["entropy_reject_rate"] == pytest.approx(1 / 3)
    assert s["paired_fills"] == 1 and s["entropy_filled_without_follow"] == 1
    assert s["exposure"]["n"] == 2 and s["exposure"]["max_ms"] == 12000.0
    assert s["exposure"]["over_limit"] == 1 and s["exposure"]["still_open_after_attempt"] == 1
    post = s["segments"]["entropy: POST round trip / 请求往返(含出块)"]
    assert post["n"] == 1 and post["p50_ms"] == pytest.approx(479.0)
    assert s["segments"]["journal fsync / 订单日志落盘"]["p50_ms"] == pytest.approx(9.0)
    assert s["persist_ms"]["entropy_zero_fill_p50"] == pytest.approx(500.0)
    assert exec_report.main(["--file", str(path)]) == 0
