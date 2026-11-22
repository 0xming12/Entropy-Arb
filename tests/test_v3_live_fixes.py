"""Comprehensive regression tests for v3 live-readiness fixes (P0-1..P0-4, P1-1..P1-5).

Run:  python3 tests/test_v3_live_fixes.py  (or pytest)
"""
import asyncio
import math
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from entropy_arb.book import ArbPlan, OrderBook  # noqa: E402
from entropy_arb.config import (  # noqa: E402
    HLCreds,
    LIGHTER_PROFILES,
    LighterCreds,
    VenueConf,
    load_config,
)
from entropy_arb.feeds import HLBookFeed, LighterBookFeed  # noqa: E402
from entropy_arb.venue_hl import HLVenue  # noqa: E402
from entropy_arb.venue_lighter import LighterVenue  # noqa: E402
from test_live_risk_regression import (  # noqa: E402
    _prepare_durable_pair,
    approx,
    make_cfg,
    make_engine,
    run_scan,
)

ROOT = os.path.join(os.path.dirname(__file__), "..")
LIVE_TEST_EXAMPLE = os.path.join(ROOT, "config.live-test.example.yaml")
NO_ENV = os.path.join(tempfile.gettempdir(), "entropy-arb-no-such.env")


# ------------------------------------------- P0-1: no panic dump in reduce_only

def test_p0_1_startup_risk_and_reduce_only_do_not_panic_dump_positions():
    eng = make_engine(
        midline=0.0, upper=4.0, lower=4.0, leg_slip=15.0,
        extra_yaml="""
risk:
  max_taker_fee_bps: 2.0
  min_available_margin_usd: 100.0
session_filter:
  enabled: true
  off_hours_mode: reduce_only
""",
    )
    eng.entropy.position = 1.0
    eng.hedge.position = -1.0
    # Flat / slightly negative spread (-1 bps) while holding a delta-neutral pair.
    eng.entropy.set_book(99.99, 100.00)
    eng.hedge.set_book(99.99, 100.00)

    # 1. Startup fail-closed window (fee_unknown + margin_unknown)
    eng._prime_startup_risk()
    assert eng.is_reduce_only() is True
    assert run_scan(eng) is None

    # 2. Off-hours reduce_only after fee/margin recover
    eng._handle_fee_observation(0.9)
    eng.circuit_breaker.update_available_margin("ENTROPY", 200.0)
    eng.circuit_breaker.update_available_margin("RH", 200.0)
    t_off = datetime(2026, 7, 15, 22, 0, tzinfo=timezone.utc).timestamp()
    assert eng.is_reduce_only(t_off) is True
    assert run_scan(eng, now=t_off) is None

    # 3. Spread mean-reverts into profit (+6 bps > 4 bps) -> clean reducing exit fires!
    eng.entropy.set_book(100.06, 100.07)
    eng.hedge.set_book(99.99, 100.00)
    best = run_scan(eng, now=t_off)
    assert best is not None
    assert best[1].key == "entropy" and best[2].qty <= 1.0 + 1e-9

    # 4. Opt-in active bounded-loss exit: -5 bps allows -1 bps, still blocks -20 bps
    eng.cfg.reduce_only_exit_threshold_bps = -5.0
    eng.entropy.set_book(99.99, 100.00)
    eng.hedge.set_book(99.99, 100.00)
    assert run_scan(eng, now=t_off) is not None
    eng.entropy.set_book(99.80, 99.81)  # -20 bps
    assert run_scan(eng, now=t_off) is None


# ------------------------------------ P0-2: non-zero midline breaker conservation

def test_p0_2_nonzero_midline_entries_do_not_trip_consecutive_or_daily_loss():
    eng = make_engine(
        midline=-25.0, upper=5.0, lower=5.0, leg_slip=2.0,
        extra_yaml="""
risk:
  max_daily_loss_usd: 5.0
  max_consecutive_losses: 3
  breaker_action: reduce_only
""",
    )
    # Upper trigger for sell_entropy is midline + upper = -20 bps.
    # Execute 5 normal profitable entries at -18 bps (sell_e = 99.82, buy_h = 100.00).
    for i in range(5):
        eng.entropy.set_book(99.82, 99.83)
        eng.hedge.set_book(99.99, 100.00)
        plan = ArbPlan(
            qty=1.0, buy_limit=100.00, sell_limit=99.82,
            buy_notional=100.00, sell_notional=99.82,
            q_max=10.0, q_max_notional=1000.0,
            top_premium_bps=-18.0, marginal_premium_bps=-18.0,
            buy_fee=0.0, sell_fee=0.0,
        )
        eng.entropy.send_responses = [
            {"status": "filled", "outcome": "filled", "filled_base": 1.0, "avg_px": 99.82,
             "err": None, "unresolved": False, "client_order_id": f"e-{i}"}
        ]
        eng.hedge.send_responses = [
            {"status": "filled", "outcome": "filled", "filled_base": 1.0, "avg_px": 100.00,
             "err": None, "unresolved": False, "client_order_id": f"h-{i}"}
        ]
        asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))

    # Each slice had +7 bps (+$0.07) vs midline -> ZERO consecutive losses, no breaker trip!
    assert eng.circuit_breaker.consecutive_losses == 0
    assert eng.circuit_breaker.loss_tripped is False
    approx(eng.circuit_breaker.daily_realized_pnl_usd, 5 * 0.07, tol=1e-4)

    # Now unwind all 5 at lower band (-32 bps: buy_e = 99.68, sell_h = 100.00).
    eng.entropy.set_book(99.67, 99.68)
    eng.hedge.set_book(100.00, 100.01)
    unwind = ArbPlan(
        qty=5.0, buy_limit=99.68, sell_limit=100.00,
        buy_notional=498.40, sell_notional=500.00,
        q_max=10.0, q_max_notional=1000.0,
        top_premium_bps=32.10, marginal_premium_bps=32.10,
        buy_fee=0.0, sell_fee=0.0,
    )
    eng.entropy.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 5.0, "avg_px": 99.68,
         "err": None, "unresolved": False, "client_order_id": "e-u"}
    ]
    eng.hedge.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 5.0, "avg_px": 100.00,
         "err": None, "unresolved": False, "client_order_id": "h-u"}
    ]
    asyncio.run(eng._execute(eng.entropy, eng.hedge, unwind))
    # Conservation: entry + exit realized PnL == exact round-trip cash profit = 5*(99.82-99.68) = $0.70!
    approx(eng.circuit_breaker.daily_realized_pnl_usd, 0.70, tol=1e-4)

    # Genuine adverse fill worse than midline (-30 bps on sell_entropy) DOES count as a loss.
    eng.entropy.set_book(99.82, 99.83)
    eng.hedge.set_book(99.99, 100.00)
    bad = ArbPlan(
        qty=1.0, buy_limit=100.00, sell_limit=99.82,
        buy_notional=100.00, sell_notional=99.82,
        q_max=10.0, q_max_notional=1000.0,
        top_premium_bps=-18.0, marginal_premium_bps=-18.0,
        buy_fee=0.0, sell_fee=0.0,
    )
    eng.entropy.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 1.0, "avg_px": 99.70,
         "err": None, "unresolved": False, "client_order_id": "e-bad"}
    ]
    eng.hedge.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 1.0, "avg_px": 100.00,
         "err": None, "unresolved": False, "client_order_id": "h-bad"}
    ]
    asyncio.run(eng._execute(eng.hedge, eng.entropy, bad))
    assert eng.circuit_breaker.consecutive_losses == 1


# ----------------------------------------- P0-3: Official HL HIP-3 fee formula

def test_p0_3_hl_hip3_taker_fee_formula_and_fail_closed():
    hl_conf = VenueConf(
        key="entropy", kind="hl", label="ENTROPY", symbol="NVDA",
        fee_bps=1.0, cap_usd=1000.0, orders_per_min=120, hl_dex="io",
        hl_creds=HLCreds("0x" + "1" * 64, "0x" + "a" * 40),
    )
    hl = HLVenue(hl_conf, "https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws",
                 session=None, settle_timeout_sec=1.0)
    hl.coin = "io:NVDA"

    cases = [
        # (userCrossRate, referral, deployerFeeScale, growthMode, expected_bps)
        ("0.00045", "0.0", "1.0", None, 9.0),
        ("0.00045", "0.0", "1.0", "enabled", 0.9),
        ("0.00045", "0.0", "0.0", None, 4.5),        # dfs=0 -> scaleIfHip3=1, NOT 0!
        ("0.00045", "0.0", "0.0", "enabled", 0.45),
        ("0.00045", "0.0", "2.0", None, 18.0),
        ("0.00045", "0.04", "1.0", "enabled", 0.9 * 0.96),
    ]
    for base, ref, dfs, gm, expected in cases:
        async def fake_info(payload, _base=base, _ref=ref, _dfs=dfs, _gm=gm):
            if payload["type"] == "userFees":
                return {"userCrossRate": _base, "activeReferralDiscount": _ref}
            if payload["type"] == "meta":
                return {"universe": [{"name": "io:NVDA", "deployerFeeScale": _dfs, "growthMode": _gm}]}
            raise AssertionError(payload)

        hl._info = fake_info
        got = asyncio.run(hl.fetch_taker_fee_bps())
        approx(got, expected, tol=1e-6)

    # Meta failure MUST fail closed (None), never silently return unscaled base rate!
    async def meta_boom(payload):
        if payload["type"] == "userFees":
            return {"userCrossRate": "0.00045"}
        raise RuntimeError("503")

    hl._info = meta_boom
    assert asyncio.run(hl.fetch_taker_fee_bps()) is None

    # Missing asset in meta MUST fail closed (None)
    async def meta_missing(payload):
        if payload["type"] == "userFees":
            return {"userCrossRate": "0.00045"}
        return {"universe": [{"name": "io:OTHER"}]}

    hl._info = meta_missing
    assert asyncio.run(hl.fetch_taker_fee_bps()) is None


def test_p0_3_tradexyz_hedge_fee_monitored_alongside_entropy():
    eng = make_engine(
        hedge="tradexyz",
        extra_yaml="""
risk:
  max_taker_fee_bps: 2.0
""",
    )
    eng.hedge.kind = "hl"
    eng.hedge.name = "XYZ"
    eng.entropy.live_fee_bps = 0.9
    eng.hedge.live_fee_bps = 9.0  # Growth Mode off on trade.xyz
    asyncio.run(eng._check_all_fees_once())
    assert eng.circuit_breaker.fee_tripped is True
    approx(eng.effective_fee_bps(eng.hedge), 9.0)

    eng.hedge.live_fee_bps = 0.9
    asyncio.run(eng._check_all_fees_once())
    assert eng.circuit_breaker.fee_tripped is False


# ------------------- P0-4: single pre-send fsync + userFillsByTime + async fee

def test_p0_4_single_save_before_gather_zero_fsync_between_legs():
    tmp_dir = tempfile.mkdtemp(prefix="entropy_v3_fsync_")
    engine, plan = _prepare_durable_pair(tmp_dir)
    save_calls = []
    orig_save = engine.state_store.save

    def spy_save(payload):
        save_calls.append(dict(payload.get("pending_orders") or {}))
        return orig_save(payload)

    engine.state_store.save = spy_save
    saves_seen_at_send = []

    async def spy_send(**kwargs):
        saves_seen_at_send.append(len(save_calls))
        return {"status": "filled", "outcome": "filled", "filled_base": kwargs["qty"],
                "avg_px": kwargs["limit_px"], "err": None, "unresolved": False,
                "client_order_id": kwargs["client_order_id"]}

    engine.entropy.send_taker = spy_send
    engine.hedge.send_taker = spy_send

    asyncio.run(engine._execute(engine.hedge, engine.entropy, plan))
    # Exactly 1 save BEFORE both sends, 0 saves BETWEEN leg 1 and leg 2, 1 commit save AFTER!
    assert saves_seen_at_send == [1, 1], f"expected [1, 1], got {saves_seen_at_send}"
    assert len(save_calls) == 2
    assert set(save_calls[0].keys()) == {"entropy:hl-id", "hedge:lt-id"}
    assert all(o["submission"] == "attempted" for o in save_calls[0].values())


def test_p0_4_hl_fetch_fill_fee_uses_by_time_and_rejects_non_usd_token():
    hl_conf = VenueConf(
        key="entropy", kind="hl", label="ENTROPY", symbol="NVDA",
        fee_bps=1.0, cap_usd=1000.0, orders_per_min=120, hl_dex="io",
        hl_creds=HLCreds("0x" + "1" * 64, "0x" + "a" * 40),
    )
    hl = HLVenue(hl_conf, "https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws",
                 session=None, settle_timeout_sec=1.0)
    hl.coin = "io:NVDA"
    seen = []

    async def ok_fills(payload):
        seen.append(payload)
        return [{"coin": "io:NVDA", "oid": 42, "sz": "1.0", "px": "100.0",
                 "fee": "0.01", "feeToken": "USDC"}]

    hl._info = ok_fills
    res = asyncio.run(hl.fetch_fill_fee(order_id=42))
    assert seen[0]["type"] == "userFillsByTime" and "startTime" in seen[0]
    assert res is not None and abs(res["fee_bps"] - 1.0) < 1e-6

    async def hype_fee_fills(payload):
        return [{"coin": "io:NVDA", "oid": 43, "sz": "1.0", "px": "100.0",
                 "fee": "0.005", "feeToken": "HYPE"}]

    hl._info = hype_fee_fills
    assert asyncio.run(hl.fetch_fill_fee(order_id=43)) is None


def test_p0_4_imbalanced_fill_hedges_without_waiting_on_slow_fill_fee():
    async def verify():
        eng = make_engine(
            midline=0.0, upper=2.0, lower=2.0, leg_slip=2.0,
            extra_yaml="risk:\n  max_taker_fee_bps: 2.0\n",
        )
        eng.entropy.set_book(100.14, 100.16)
        eng.hedge.set_book(99.99, 100.01)

        fee_gate = asyncio.Event()

        async def slow_fee(**kwargs):
            await fee_gate.wait()
            return {"fee_usd": 0.09, "fee_bps": 9.0, "fee_token": "USDC",
                    "filled_base": 1.0, "estimated": False}

        eng.entropy.fetch_fill_fee = slow_fee
        # Entropy fills 1.0, Lighter cancels 0.0 -> legging failure!
        eng.entropy.send_responses = [
            {"status": "filled", "outcome": "filled", "filled_base": 1.0, "avg_px": 100.14,
             "order_id": 11, "err": None, "unresolved": False, "client_order_id": "hl-imbal"},
            # Hedge buyback on Entropy
            {"status": "filled", "outcome": "filled", "filled_base": 1.0, "avg_px": 100.16,
             "err": None, "unresolved": False, "client_order_id": "hl-hedge"},
        ]
        eng.hedge.send_responses = [
            {"status": "canceled", "outcome": "canceled", "filled_base": 0.0, "avg_px": None,
             "err": None, "unresolved": False, "client_order_id": "lt-miss"},
        ]
        plan = ArbPlan(
            qty=1.0, buy_limit=100.01, sell_limit=100.14,
            buy_notional=100.01, sell_notional=100.14,
            q_max=10.0, q_max_notional=1000.0,
            top_premium_bps=13.0, marginal_premium_bps=13.0,
            buy_fee=0.0, sell_fee=0.0,
        )
        # _execute_locked MUST complete and hedge EVEN WHILE slow_fee is still blocked!
        await eng._vlock("entropy").acquire()
        await eng._vlock("hedge").acquire()
        await asyncio.wait_for(
            eng._execute_locked(eng.hedge, eng.entropy, plan,
                                acquired_locks=[eng._vlock("entropy"), eng._vlock("hedge")]),
            timeout=1.0,
        )
        approx(eng.entropy.position, 0.0)
        assert len(eng.entropy.sent_orders) == 2  # entry + immediate hedge!
        # Release background fee check -> trips fee breaker asynchronously!
        fee_gate.set()
        if eng._bg_tasks:
            await asyncio.wait(list(eng._bg_tasks), timeout=1.0)
        assert eng.circuit_breaker.fee_tripped is True

    asyncio.run(verify())


# ----------------------- P1-1: latency_buffer provides real boundary slippage

def test_p1_1_latency_buffer_provides_slippage_room_at_entry_boundary():
    eng = make_engine(
        midline=0.0, upper=4.0, lower=4.0, leg_slip=15.0,
        px_step_e=0.001, px_step_h=0.001,
        extra_yaml="thresholds:\n  midline_bps: 0.0\n  upper_bps: 4.0\n  lower_bps: 4.0\n  latency_buffer_bps: 3.0\n",
    )
    # Re-create with latency_buffer_bps cleanly via direct cfg attribute
    eng.cfg.latency_buffer_bps = 3.0
    # Visible spread = +7.1 bps (just 0.1 bps above entry trigger 4.0 + 3.0 = 7.0 bps).
    eng.entropy.set_book(100.071, 100.080)
    eng.hedge.set_book(99.990, 100.000)
    plan, reason = eng._plan(eng.hedge, eng.entropy, 500.0)
    assert reason == "ok" and plan is not None
    bounds = eng._order_price_bounds(eng.hedge, eng.entropy, plan, plan.qty, reduce_only=False)
    assert bounds is not None
    buy_bound, sell_bound = bounds
    # Each leg gets ~1.55 bps of real slippage room (not squeezed to ~0.05 bps!)
    assert buy_bound >= 100.014, f"buy_bound too tight: {buy_bound}"
    assert sell_bound <= 100.056, f"sell_bound too tight: {sell_bound}"
    # Worst-case net spread STILL clears upper_bps (4.0 bps)!
    ok, worst_bps, _, req_bps = eng._worst_case_check(
        eng.hedge, eng.entropy, plan.qty, buy_bound, sell_bound, reduce_only=False)
    assert ok and worst_bps >= 4.0 - 1e-6 and req_bps == 4.0


# ----------------- P1-2: quiet book does not block reduce_only / hedge / alert

def test_p1_2_quiet_book_blocks_new_entry_but_allows_reduce_only_and_hedge():
    eng = make_engine(
        midline=0.0, upper=2.0, lower=2.0,
        extra_yaml="risk:\n  max_book_age_sec: 5.0\n",
    )
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    # Quiet book: 12s since last L2 change, but WS connection is alive (alive_ts fresh).
    now_mono = time.monotonic()
    eng.entropy.book.last_update_mono = now_mono - 12.0
    eng.entropy.book.touch()

    # Flat -> new entry MUST be blocked
    assert run_scan(eng) is None

    # Matched position in reduce_only -> reducing exit IS allowed
    eng.circuit_breaker.trigger_reconcile_drift("test")
    eng.entropy.position = 1.0
    eng.hedge.position = -1.0
    best = run_scan(eng)
    assert best is not None and best[1].key == "entropy"

    # Unmatched naked position -> _hedge() IS allowed on quiet live book!
    eng.entropy.position = 1.0
    eng.hedge.position = 0.0
    asyncio.run(eng._maybe_hedge())
    approx(eng.entropy.position, 0.0)
    assert len(eng.entropy.sent_orders) == 1


# -------------------- P1-3: hedge priority & multi-slice success retry counter

def test_p1_3_hedge_priority_and_multislices_do_not_exhaust_retry_budget():
    eng = make_engine(
        midline=0.0, upper=2.0, lower=2.0,
        extra_yaml="""
sizing:
  max_order_notional_usd: 500
execution:
  cooldown_sec: 60.0
  hedge_max_retries: 2
  hedge_retry_delay_sec: 0.0
""",
    )
    eng.entropy.set_book(100.00, 100.02)
    eng.hedge.set_book(100.00, 100.02)
    eng.last_trade_ts = time.time()  # Inside 60s cooldown!
    # $1800 imbalance requires 4 slices of <= $500 (> hedge_max_retries=2).
    eng.entropy.position = 18.0
    asyncio.run(eng._evaluate())
    approx(eng.entropy.position, 0.0)
    assert eng.halted is False
    assert len(eng.entropy.sent_orders) == 4


# ---------------- P1-4: Lighter ambiguous err & query_order AttributeError fallback

def test_p1_4_lighter_ambiguous_err_is_unresolved_and_query_falls_back():
    orig_lighter = sys.modules.get("lighter")
    sys.modules["lighter"] = SimpleNamespace(SignerClient=SimpleNamespace(
        ORDER_TYPE_MARKET=1, ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL=0, DEFAULT_IOC_EXPIRY=0,
    ))
    try:
        venue = LighterVenue(make_cfg().hedge, session=None, settle_timeout_sec=1.0)
        venue.market_id = 15
        venue.conf.lighter_creds = LighterCreds(123, 0, "priv")

        async def submit_502(**kwargs):
            return None, None, "HTTP 502 Bad Gateway"

        venue.signer = SimpleNamespace(create_order=submit_502)
        res = asyncio.run(venue.send_taker(is_buy=True, qty=1.0, limit_px=100.0, client_order_id="777"))
        assert res["unresolved"] is True and res["status"] == "send-unknown"

        async def submit_reject(**kwargs):
            return None, None, "insufficient margin"

        venue.signer = SimpleNamespace(create_order=submit_reject)
        res2 = asyncio.run(venue.send_taker(is_buy=True, qty=1.0, limit_px=100.0, client_order_id="778"))
        assert res2["unresolved"] is False and res2["outcome"] == "rejected"

        # query_order falls back to account_inactive_orders on AttributeError
        class FallbackOrderApi:
            async def account_inactive_orders(self, **kwargs):
                return SimpleNamespace(to_dict=lambda: {
                    "code": 200,
                    "orders": [{"client_order_index": 777, "market_index": 15,
                                "status": "filled", "filled_base_amount": "1.0",
                                "filled_quote_amount": "100.0"}],
                })

        venue.signer = SimpleNamespace(
            create_auth_token_with_expiry=lambda: ("tok", None),
            order_api=FallbackOrderApi(),
        )
        q = asyncio.run(venue.query_order("777"))
        assert q["unresolved"] is False and q["outcome"] == "filled" and q["filled_base"] == 1.0
    finally:
        if orig_lighter is None:
            sys.modules.pop("lighter", None)
        else:
            sys.modules["lighter"] = orig_lighter


# ------------- P1-5: Unified Account margin, post-lock check, drift persistence

def test_p1_5_hl_unified_account_fetch_equity_and_drift_persistence():
    hl_conf = VenueConf(
        key="entropy", kind="hl", label="ENTROPY", symbol="NVDA",
        fee_bps=1.0, cap_usd=1000.0, orders_per_min=120, hl_dex="io",
        hl_creds=HLCreds("0x" + "1" * 64, "0x" + "a" * 40),
    )
    hl = HLVenue(hl_conf, "https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws",
                 session=None, settle_timeout_sec=1.0)

    async def unified_info(payload):
        t = payload["type"]
        if t == "userAbstraction":
            return "unifiedAccount"
        if t == "spotClearinghouseState":
            return {"balances": [{"coin": "USDC", "total": "250.0", "hold": "40.0"}]}
        if t == "portfolio":
            return [["day", {"accountValueHistory": [[1, "250.0"]]}]]
        if t == "clearinghouseState":
            return {"withdrawable": "0.0", "marginSummary": {"accountValue": "0.0"}}
        raise AssertionError(payload)

    hl._info = unified_info
    eq, free = asyncio.run(hl.fetch_equity())
    approx(eq, 250.0)
    approx(free, 210.0)  # 250 - 40 from spotClearinghouseState, NOT 0.0!

    # Position drift adoption persists updated ledger to state.json
    tmp_dir = tempfile.mkdtemp(prefix="entropy_v3_drift_")
    eng = make_engine(
        f'persistence:\n  enabled: true\n  state_dir: "{tmp_dir}"\n',
        midline=0.0, upper=2.0, lower=2.0,
    )
    eng.entropy.set_book(100.0, 100.02)
    eng.hedge.set_book(100.0, 100.02)
    eng.entropy.chain_position = 1.5
    asyncio.run(eng._reconcile_venue(eng.entropy, strict=False))
    saved = eng.state_store.load()
    assert saved is not None
    approx(saved["venue_ledger"]["entropy"]["position"], 1.5)


def test_p1_5_post_lock_pre_send_recheck_and_live_test_template_loads():
    eng = make_engine(midline=0.0, upper=2.0, lower=2.0)
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    plan, _ = eng._plan(eng.hedge, eng.entropy, 100.0)
    assert plan is not None
    # Simulate venue going down while waiting for locks
    eng._venue_down["hedge"] = time.time()
    asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))
    assert eng.entropy.sent_orders == [] and eng.hedge.sent_orders == []

    # Verify small-capital live-test template parses cleanly for all 3 hedge venues
    for hv in ("lighter", "lighter-rh", "tradexyz"):
        cfg = load_config(LIVE_TEST_EXAMPLE, NO_ENV, symbol="NVDA", hedge_venue=hv)
        assert cfg.replay_only is False
        assert cfg.reduce_only_exit_threshold_bps is None
        assert cfg.risk.max_book_age_sec == 30.0
        assert cfg.entropy.fee_bps == 1.0


def test_p1_5_unhedged_pending_order_emits_alert_and_proactive_reconnect_quiet():
    # 1. Pending unresolved order with open net exposure emits high-priority unhedged alert
    eng = make_engine(midline=0.0, upper=2.0, lower=2.0)
    eng.entropy.position = 1.0
    eng.hedge.position = 0.0
    okey = eng._register_pending_order(
        eng.hedge, "lt-stuck", is_buy=False, qty=1.0, limit_px=100.0,
        known_filled=0.0, purpose="arb", reduce_only=False, persist=False,
    )
    eng._pending_orders[okey]["created_ts"] = time.time() - 30.0
    eng.hedge.order_queries["lt-stuck"] = {
        "status": "not-found", "outcome": "unknown", "filled_base": 0.0,
        "avg_px": None, "err": None, "unresolved": True, "client_order_id": "lt-stuck",
    }
    notified = []
    eng.notifier.notify = lambda key, text, **kw: notified.append((key, text))
    assert asyncio.run(eng._reconcile_pending_orders()) is False
    assert any(k == f"pending_unhedged:{okey}" and "UNHEDGED EXPOSURE FROZEN" in t
               for k, t in notified)

    # 2. Proactive reconnect in feeds.py does not emit false on_disconnect alert
    disconnects = []
    feed = HLBookFeed("ENTROPY", "wss://unused", "io:NVDA", OrderBook(), lambda: None,
                      on_disconnect=lambda n, d: disconnects.append((n, d)))
    stop = asyncio.Event()

    async def run_proactive():
        class BoomWS:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def send(self, _):
                pass

            async def close(self):
                pass

            def __aiter__(self):
                return self

            async def __anext__(self):
                feed.reconnect_evt.set()
                stop.set()
                raise RuntimeError("closed locally")

        import entropy_arb.feeds as feeds_mod
        orig = feeds_mod.ws_connect
        feeds_mod.ws_connect = lambda *a, **kw: BoomWS()
        try:
            await feed.run(stop)
        finally:
            feeds_mod.ws_connect = orig

    asyncio.run(run_proactive())
    assert disconnects == []


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:65s} OK")
