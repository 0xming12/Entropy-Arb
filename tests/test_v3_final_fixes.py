"""Regression tests for the final pre-live freeze fixes."""
import asyncio
import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from entropy_arb.book import ArbPlan, OrderBook  # noqa: E402
from entropy_arb.config import (  # noqa: E402
    ConfigError,
    HLCreds,
    LIGHTER_PROFILES,
    LighterCreds,
    VenueConf,
    load_config,
)
from entropy_arb.feeds import HLBookFeed  # noqa: E402
from entropy_arb.risk import CircuitBreaker  # noqa: E402
from entropy_arb.state import InstanceLock  # noqa: E402
from entropy_arb.venue_hl import HLVenue  # noqa: E402
from entropy_arb.venue_lighter import LighterVenue, _is_ambiguous_submit_error  # noqa: E402
from test_live_risk_regression import approx, make_engine, run_scan  # noqa: E402
from tools.inspect_order_state import apply_supervised_recovery  # noqa: E402


def test_reduce_only_thresholds_respect_midline_and_do_not_widen_default_slippage():
    eng = make_engine(midline=20.0, upper=6.0, lower=6.0, leg_slip=15.0)
    eng.cfg.latency_buffer_bps = 3.0
    eng.cfg.reduce_only_min_worst_bps = -25.0
    # Default reduce_only (reduce_only_exit_threshold_bps=None):
    # Signal triggers at mid+upper+lat = 29 bps (sell_entropy), floor stays mid+upper = 26 bps,
    # NOT -25 bps (which would widen limit slippage to 15 bps per leg!).
    eng.circuit_breaker.trigger_reconcile_drift("test")
    eng.entropy.position = 1.0
    eng.hedge.position = -1.0
    eng.entropy.set_book(100.30, 100.31)
    eng.hedge.set_book(99.99, 100.00)
    plan, reason = eng._plan(eng.hedge, eng.entropy, 500.0, reduce_only=True)
    assert reason == "ok" and plan is not None
    bounds = eng._order_price_bounds(eng.hedge, eng.entropy, plan, plan.qty, reduce_only=True)
    assert bounds is not None
    buy_bound, sell_bound = bounds
    # Spread is 30 bps, floor is 26 bps -> at most ~2 bps per leg (never 15 bps!)
    assert buy_bound <= 100.03
    assert sell_bound >= 100.27
    ok, worst_bps, _, req_bps = eng._worst_case_check(
        eng.hedge, eng.entropy, plan.qty, buy_bound, sell_bound, reduce_only=True
    )
    assert ok and approx(req_bps, 26.0, tol=1e-6) is None and worst_bps >= 26.0 - 1e-6

    # Opt-in reduce_only_exit_threshold_bps = -5.0 is midline-relative:
    # sell_entropy triggers at +20 + (-5) = +15 bps; buy_entropy triggers at -5 - 20 = -25 bps.
    eng.cfg.reduce_only_exit_threshold_bps = -5.0
    approx(eng._plan_threshold_bps(eng.hedge, eng.entropy, reduce_only=True), 15.0)
    approx(eng._plan_threshold_bps(eng.entropy, eng.hedge, reduce_only=True), -25.0)
    approx(eng._required_spread_bps(eng.hedge, eng.entropy, reduce_only=True), -5.0)
    approx(eng._required_spread_bps(eng.entropy, eng.hedge, reduce_only=True), -45.0)

    # Short Entropy / Long Hedge closing via buy_entropy at midline (+20 bps -> buy_edge = -20 bps)
    # MUST be allowed since -20 bps >= -25 bps (profitable vs midline)!
    eng.entropy.position = -1.0
    eng.hedge.position = 1.0
    eng.entropy.set_book(100.19, 100.20)
    eng.hedge.set_book(100.00, 100.01)
    best = run_scan(eng)
    assert best is not None and best[0] is eng.entropy and best[1] is eng.hedge


def test_hl_fetch_taker_fee_refreshes_deployer_fee_scale_from_perp_dexs():
    hl_conf = VenueConf(
        key="entropy", kind="hl", label="ENTROPY", symbol="NVDA",
        fee_bps=1.0, cap_usd=1000.0, orders_per_min=120, hl_dex="io",
        hl_creds=HLCreds("0x" + "1" * 64, "0x" + "a" * 40),
    )
    hl = HLVenue(hl_conf, "https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws",
                 session=None, settle_timeout_sec=1.0)
    hl.coin = "io:NVDA"
    hl.deployer_fee_scale = 0.0  # cached at startup

    live_dfs = ["1.0"]

    async def fake_info(payload):
        t = payload["type"]
        if t == "userFees":
            return {"userCrossRate": "0.00045", "activeReferralDiscount": "0.0"}
        if t == "meta":
            # Real HL meta response omits deployerFeeScale
            return {"universe": [{"name": "io:NVDA", "growthMode": "enabled"}]}
        if t == "perpDexs":
            return [None, {"name": "io", "deployerFeeScale": live_dfs[0]}]
        raise AssertionError(payload)

    hl._info = fake_info
    approx(asyncio.run(hl.fetch_taker_fee_bps()), 0.9, tol=1e-6)
    # Deployer hikes deployerFeeScale live from 1.0 -> 3.0 (scale_if_hip3: 2 -> 6)
    live_dfs[0] = "3.0"
    approx(asyncio.run(hl.fetch_taker_fee_bps()), 2.7, tol=1e-6)

    # perpDexs failure fails closed (None)
    async def dex_fail(payload):
        if payload["type"] == "perpDexs":
            raise RuntimeError("503")
        return await fake_info(payload)

    hl._info = dex_fail
    assert asyncio.run(hl.fetch_taker_fee_bps()) is None


def test_hl_send_taker_sets_bounded_expires_after(monkeypatch):
    import sys as _sys
    from types import ModuleType

    fake_types = ModuleType("hyperliquid.utils.types")

    class _Cloid:
        @staticmethod
        def from_str(s):
            return s

    fake_types.Cloid = _Cloid
    monkeypatch.setitem(_sys.modules, "hyperliquid.utils.types", fake_types)

    hl_conf = VenueConf(
        key="entropy", kind="hl", label="ENTROPY", symbol="NVDA",
        fee_bps=1.0, cap_usd=1000.0, orders_per_min=120, hl_dex="io",
        hl_creds=HLCreds("0x" + "1" * 64, "0x" + "a" * 40),
    )
    hl = HLVenue(hl_conf, "https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws",
                 session=None, settle_timeout_sec=10.0)
    hl.coin = "io:NVDA"
    hl.asset_id = 110000

    seen = {}

    def fake_sign(wallet, action, vault, nonce, *, expires_after, is_mainnet):
        seen["signed_expires_after"] = expires_after
        return {"r": "0x1", "s": "0x2", "v": 27}

    hl._signing = SimpleNamespace(
        order_request_to_order_wire=lambda req, aid: {"a": aid},
        order_wires_to_order_action=lambda wires: {"type": "order", "orders": wires},
        sign_l1_action=fake_sign,
    )
    hl.account = SimpleNamespace(
        wallet=SimpleNamespace(address="0x" + "b" * 40),
        query_address="0x" + "a" * 40,
        is_mainnet=True,
        nonces=SimpleNamespace(next=lambda: int(time.time() * 1000)),
    )

    async def fake_post(payload):
        seen["payload_expires_after"] = payload["expiresAfter"]
        return (
            {"status": "ok", "response": {"data": {"statuses": [
                {"filled": {"totalSz": "0.1", "avgPx": "144.0", "oid": 1}}
            ]}}},
            None,
            False,
        )

    hl._post_exchange = fake_post
    now_ms = int(time.time() * 1000)
    res = asyncio.run(hl.send_taker(is_buy=True, qty=0.1, limit_px=144.0))
    assert res["outcome"] == "filled"
    exp = seen["signed_expires_after"]
    assert isinstance(exp, int) and exp == seen["payload_expires_after"]
    assert now_ms + 14_000 <= exp <= now_ms + 20_000


def test_hl_fetch_equity_fallback_and_core_asset_id():
    hl_conf = VenueConf(
        key="entropy", kind="hl", label="ENTROPY", symbol="NVDA",
        fee_bps=1.0, cap_usd=1000.0, orders_per_min=120, hl_dex="io",
        hl_creds=HLCreds("0x" + "1" * 64, "0x" + "a" * 40),
    )
    hl = HLVenue(hl_conf, "https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws",
                 session=None, settle_timeout_sec=1.0)

    # Non-unified account when portfolio fails: free must be HIP-3 withdrawable ONLY (not + core!)
    async def non_unified_fallback(payload):
        t = payload["type"]
        if t == "userAbstraction":
            return "default"
        if t == "portfolio":
            raise RuntimeError("portfolio down")
        if t == "clearinghouseState":
            if payload["dex"] == "io":
                return {"marginSummary": {"accountValue": "50.0"}, "withdrawable": "12.0"}
            return {"marginSummary": {"accountValue": "500.0"}, "withdrawable": "400.0"}
        raise AssertionError(payload)

    hl._info = non_unified_fallback
    eq, free = asyncio.run(hl.fetch_equity())
    approx(eq, 550.0)
    approx(free, 12.0)

    # Unified account when spotClearinghouseState fails: free must be None (unknown), not 0.0
    hl._user_abstraction = None

    async def unified_spot_fail(payload):
        t = payload["type"]
        if t == "userAbstraction":
            return "unifiedAccount"
        if t == "spotClearinghouseState":
            raise RuntimeError("spot 503")
        if t == "portfolio":
            return [["day", {"accountValueHistory": [[0, "300.0"]]}]]
        raise AssertionError(payload)

    hl._info = unified_spot_fail
    eq2, free2 = asyncio.run(hl.fetch_equity())
    approx(eq2, 300.0)
    assert free2 is None


def test_hl_feed_logs_clock_skew_drops_and_book_filters_nan(caplog):
    book = OrderBook()
    book.apply_lighter(
        {"bids": [{"price": "100", "size": "2"}, {"price": "101", "size": "nan"},
                  {"price": "-5", "size": "1"}],
         "asks": [{"price": "102", "size": "3"}, {"price": "inf", "size": "1"}]},
        snapshot=True,
    )
    assert list(book.bids.items()) == [(100.0, 2.0)]
    assert list(book.asks.items()) == [(102.0, 3.0)]

    book.apply_hl([
        [{"px": "100", "sz": "2"}, {"px": "nan", "sz": "1"}, {"px": "99", "sz": "inf"}],
        [{"px": "101", "sz": "2"}, {"px": "0", "sz": "1"}],
    ])
    assert list(book.bids.items()) == [(100.0, 2.0)]
    assert list(book.asks.items()) == [(101.0, 2.0)]

    feed = HLBookFeed("ENTROPY", "unused", "io:NVDA", OrderBook(), lambda: None)
    future_ms = int((time.time() + 10.0) * 1000)
    with caplog.at_level(logging.WARNING, logger="feeds"):
        feed._on_frame({"channel": "l2Book", "data": {
            "coin": "io:NVDA", "time": future_ms,
            "levels": [[{"px": "100", "sz": "1"}], [{"px": "101", "sz": "1"}]],
        }})
    assert any("NTP clock sync" in r.message for r in caplog.records)


def test_lighter_ambiguous_error_avoids_numeric_false_positives_and_rejects_zero_qty(monkeypatch):
    monkeypatch.setitem(sys.modules, "lighter", SimpleNamespace(SignerClient=SimpleNamespace()))
    assert _is_ambiguous_submit_error("HTTP 502 Bad Gateway") is True
    assert _is_ambiguous_submit_error("HTTP 500 Internal Server Error") is True
    assert _is_ambiguous_submit_error("status=408 request timeout") is True
    assert _is_ambiguous_submit_error("order price 1500 out of range") is False
    assert _is_ambiguous_submit_error("account 40821 insufficient margin") is False
    assert _is_ambiguous_submit_error("nonce 50012 invalid") is False

    conf = VenueConf(
        key="hedge", kind="lighter", label="LIGHTER", symbol="NVDA",
        fee_bps=0.0, cap_usd=100.0, orders_per_min=30,
        lighter_profile=LIGHTER_PROFILES["lighter"],
        lighter_creds=LighterCreds(1, 2, "0x" + "1" * 64),
    )
    lv = LighterVenue(conf, session=None, settle_timeout_sec=1.0)
    lv.size_decimals = 2
    lv.price_decimals = 2
    lv.signer = SimpleNamespace()
    res = asyncio.run(lv.send_taker(is_buy=True, qty=0.001, limit_px=100.0))
    assert res["unresolved"] is False and res["outcome"] == "rejected"


def test_circuit_breaker_margin_alert_and_cross_day_halt_persistence():
    cb = CircuitBreaker(min_available_margin_usd=100.0, breaker_action="halt")
    cb.require_initial_observations(("ENTROPY", "LIGHTER"))
    # Transitioning from unknown -> low margin MUST return newly_tripped=True to emit alert!
    tripped, recovered, msg = cb.update_available_margin("ENTROPY", 20.0)
    assert tripped is True and recovered is False and "$20.00" in msg

    # Cross-day restart must NOT silently clear breaker_action="halt"
    t_day1 = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc).timestamp()
    t_day2 = datetime(2026, 7, 16, 14, 0, tzinfo=timezone.utc).timestamp()
    cb2 = CircuitBreaker(max_daily_loss_usd=10.0, breaker_action="halt")
    cb2.record_trade_pnl(-15.0, now=t_day1)
    assert cb2.should_halt is True
    saved = cb2.to_state(now=t_day1)

    cb3 = CircuitBreaker(max_daily_loss_usd=10.0, breaker_action="halt")
    cb3.load_state(saved, now=t_day2)
    assert cb3.should_halt is True


def test_watchdog_does_not_reconnect_when_flat_in_off_hours_reduce_only():
    eng = make_engine("""
risk:
  max_book_age_sec: 0
  max_reduce_book_age_sec: 5
session_filter:
  enabled: true
  off_hours_mode: reduce_only
""", hedge="lighter")
    eng.circuit_breaker.trigger_reconcile_drift("flat-ro")
    reconnects = []
    for v in eng.venues.values():
        v.set_book(100.0, 100.02)
        v.book.last_update_mono -= 10.0
        v.book.touch()
        v.request_reconnect = lambda k=v.key: reconnects.append(k)
    # Completely flat -> no reconnect churn
    eng._book_watchdog_tick()
    assert reconnects == []
    # Once holding a position -> reconnect DOES trigger
    eng.entropy.position = 0.5
    eng._book_watchdog_tick()
    assert set(reconnects) == {"entropy", "hedge"}


def test_hedge_receipt_fee_updates_cash_and_trips_fee_breaker():
    eng = make_engine("""
risk:
  max_taker_fee_bps: 2.0
""", hedge="lighter")
    eng.entropy.position = 0.1
    eng.entropy.set_book(144.0, 144.02)
    eng.hedge.set_book(144.0, 144.02)
    eng.entropy.send_responses = [{
        "status": "filled", "outcome": "filled", "filled_base": 0.1,
        "avg_px": 144.0, "fee_usd": 0.0648, "fee_bps": 4.5,
        "err": None, "unresolved": False, "client_order_id": "h-fee-1",
    }]
    asyncio.run(eng._maybe_hedge())
    assert eng.circuit_breaker.fee_tripped is True
    approx(eng.entropy.cash, 14.4 - 0.0648, tol=1e-6)


def test_inspect_order_state_respects_lock_and_resets_mtm_baseline(tmp_path):
    state_path = tmp_path / "NVDA_lighter_state.json"
    state_path.write_text(json.dumps({
        "symbol": "NVDA",
        "hedge_venue": "lighter",
        "daily_mtm_day": "2026-11-22",
        "daily_mtm_baseline": 123.45,
        "venue_ledger": {
            "entropy": {"cash": -144.0, "position": 1.0},
            "hedge": {"cash": 144.0, "position": -1.0},
        },
        "pending_orders": {},
        "reconcile_required": True,
    }), encoding="utf-8")

    # Holding the pair lock blocks concurrent mutation
    lk = InstanceLock(str(tmp_path / "NVDA_lighter.lock"), label="NVDA_lighter")
    lk.acquire()
    try:
        with pytest.raises(RuntimeError):
            apply_supervised_recovery(state_path, set_positions=["entropy=0.0"], clear_reconcile=True)
    finally:
        lk.release()

    # Once unlocked, --set-position succeeds and clears daily_mtm_baseline
    apply_supervised_recovery(state_path, set_positions=["entropy=0.0"], clear_reconcile=True)
    updated = json.loads(state_path.read_text(encoding="utf-8"))
    assert updated["venue_ledger"]["entropy"]["position"] == 0.0
    assert updated["daily_mtm_baseline"] is None
    assert updated["reconcile_required"] is False


def test_config_rejects_bad_env_int_and_invalid_reduce_only_bps(tmp_path, monkeypatch):
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(
        "thresholds:\n  midline_bps: 0\n  upper_bps: 4\n  lower_bps: 4\n"
        "execution:\n  reduce_only_min_worst_bps: -10000\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_config(str(cfg_path), "/dev/null", symbol="NVDA", hedge_venue="lighter")

    cfg_path.write_text(
        "thresholds:\n  midline_bps: 0\n  upper_bps: 4\n  lower_bps: 4\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("LIGHTER_ACCOUNT_INDEX", "not-an-int")
    with pytest.raises(ConfigError):
        load_config(str(cfg_path), "/dev/null", symbol="NVDA", hedge_venue="lighter")
