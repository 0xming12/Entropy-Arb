"""Tests for Option B execution upgrades:
- Entropy-first sequential execution (skip hedge on zero fill, follow exact fill/partial,
  unresolved timeout P0-1 reconciliation, reduce-only full close, deferred hedge fallback)
- Asymmetric slippage allocation (hedge_leg_slippage_bps)
- Entry quote freshness cap (max_entry_book_age_sec) and Entropy-update persistence gate
- Balanced-fill delayed actual fee verification retry (fill_fee_retry_delay_sec)
- Strict config validation & live config smoke test
"""
import asyncio
import os
import time

import pytest

from entropy_arb.config import ConfigError, load_config
from test_live_risk_regression import NO_ENV, ROOT, make_engine


def _attach_order_ids(eng):
    seq = {"n": 0}
    for venue in (eng.entropy, eng.hedge):
        def make_reserver(v):
            def reserve():
                seq["n"] += 1
                return f"{v.key}-cid-{seq['n']}"
            return reserve

        def make_sender(v, orig):
            async def send_with_id(*, client_order_id=None, **kwargs):
                res = await orig(**kwargs)
                if client_order_id is not None:
                    res = dict(res, client_order_id=client_order_id)
                return res
            return send_with_id

        venue.reserve_order_id = make_reserver(venue)
        venue.send_taker = make_sender(venue, venue.send_taker)


def test_live_config_loads_option_b_settings():
    cfg = load_config(
        os.path.join(ROOT, "config.live.yaml"),
        NO_ENV,
        symbol="NVDA",
        hedge_venue="lighter",
    )
    assert cfg.midline_bps == 0.0
    assert cfg.upper_bps == 6.0 and cfg.lower_bps == 6.0
    assert cfg.latency_buffer_bps == 6.0
    assert cfg.execution_mode == "entropy_first"
    assert cfg.hedge_leg_slippage_bps == 1.5
    assert cfg.entropy_first_hedge_slippage_bps == 20.0
    assert cfg.require_entropy_update_on_persist is True
    assert cfg.max_entry_book_age_sec == 2.0
    assert cfg.fill_fee_retry_delay_sec == 1.5
    assert cfg.dynamic_midline.enabled is True
    assert cfg.dynamic_midline.method == "rolling_median"


@pytest.mark.parametrize("field,value", [
    ("execution_mode", "invalid_mode"),
    ("hedge_leg_slippage_bps", "-1"),
    ("hedge_leg_slippage_bps", "10000"),
    ("hedge_leg_slippage_bps", ".nan"),
    ("entropy_first_hedge_slippage_bps", "-0.5"),
    ("entropy_first_hedge_slippage_bps", ".inf"),
    ("max_entry_book_age_sec", "-1"),
    ("fill_fee_retry_delay_sec", "-0.1"),
])
def test_option_b_config_validation_rejects_bad_values(tmp_path, field, value):
    cfg = tmp_path / "bad.yaml"
    cfg.write_text(
        "thresholds:\n  midline_bps: 0\n  upper_bps: 6\n  lower_bps: 6\n"
        f"execution:\n  {field}: {value}\n"
    )
    with pytest.raises(ConfigError, match=field):
        load_config(str(cfg), NO_ENV, symbol="NVDA", hedge_venue="lighter")


def test_asymmetric_slippage_gives_entropy_wider_bounds_while_preserving_worst_case():
    extra = """
thresholds:
  midline_bps: 0.0
  upper_bps: 6.0
  lower_bps: 6.0
  latency_buffer_bps: 6.0
entropy:
  taker_fee_bps: 1.0
hedge:
  taker_fee_bps: 0.0
execution:
  premium_persist_sec: 0.0
  leg_slippage_bps: 15.0
  check_worst_case_spread: true
"""
    sym_eng = make_engine(extra, hedge="lighter", px_step_e=0.01, px_step_h=0.001)
    asym_eng = make_engine(
        extra + "  hedge_leg_slippage_bps: 1.5\n",
        hedge="lighter", px_step_e=0.01, px_step_h=0.001,
    )
    for eng in (sym_eng, asym_eng):
        eng._step, eng._min_base = 0.001, 0.025
        eng.entropy.fee_bps = 1.0
        eng.hedge.fee_bps = 0.0
        eng.hedge.set_book(165.670, 165.684)
        eng.entropy.set_book(165.91, 165.93)  # ~13.6 bps gross prem (>= 6+6+1=13 bps)

    # sell_entropy direction: sell Entropy, buy Lighter
    plan_s, reason_s = sym_eng._plan(sym_eng.hedge, sym_eng.entropy, 15.0)
    plan_a, reason_a = asym_eng._plan(asym_eng.hedge, asym_eng.entropy, 15.0)
    assert reason_s == "ok" and reason_a == "ok"
    sym_buy, sym_sell = sym_eng._order_price_bounds(sym_eng.hedge, sym_eng.entropy, plan_s, plan_s.qty)
    asym_buy, asym_sell = asym_eng._order_price_bounds(asym_eng.hedge, asym_eng.entropy, plan_a, plan_a.qty)
    assert asym_buy < sym_buy          # Lighter gets capped <= 1.5 bps
    assert asym_sell < sym_sell        # Entropy gets strictly wider sell slippage (lower limit floor)
    assert (165.91 - asym_sell) >= 0.07  # >= 7 cents instead of ~4 cents
    ok, worst_bps, _, req_bps = asym_eng._worst_case_check(
        asym_eng.hedge, asym_eng.entropy, plan_a.qty, asym_buy, asym_sell
    )
    assert ok and worst_bps >= req_bps - 1e-9

    # buy_entropy direction: buy Entropy, sell Lighter
    for eng in (sym_eng, asym_eng):
        eng.entropy.set_book(165.66, 165.68)
        eng.hedge.set_book(165.905, 165.920)
    plan_bs, _ = sym_eng._plan(sym_eng.entropy, sym_eng.hedge, 15.0)
    plan_ba, _ = asym_eng._plan(asym_eng.entropy, asym_eng.hedge, 15.0)
    sym_b2, sym_s2 = sym_eng._order_price_bounds(sym_eng.entropy, sym_eng.hedge, plan_bs, plan_bs.qty)
    asym_b2, asym_s2 = asym_eng._order_price_bounds(asym_eng.entropy, asym_eng.hedge, plan_ba, plan_ba.qty)
    assert asym_b2 > sym_b2            # Entropy buy limit gets strictly more upward room
    assert asym_s2 > sym_s2            # Lighter sell limit stays tighter (<= 1.5 bps)
    ok2, _, _, _ = asym_eng._worst_case_check(
        asym_eng.entropy, asym_eng.hedge, plan_ba.qty, asym_b2, asym_s2
    )
    assert ok2


def test_entropy_first_skips_hedge_on_ioc_cancel_with_zero_loss(tmp_path):
    eng = make_engine(
        f"""
thresholds:
  midline_bps: 0.0
  upper_bps: 4.0
  lower_bps: 4.0
  latency_buffer_bps: 4.0
sizing:
  max_order_notional_usd: 15.0
execution:
  execution_mode: entropy_first
  premium_persist_sec: 0.0
  leg_slippage_bps: 15.0
  hedge_leg_slippage_bps: 1.5
persistence:
  enabled: true
  state_dir: "{tmp_path}"
""",
        hedge="lighter", px_step_e=0.01, px_step_h=0.001,
    )
    eng._log_csv = lambda *_: None
    eng._step, eng._min_base = 0.001, 0.025
    _attach_order_ids(eng)
    eng.hedge.set_book(164.99, 165.00)
    eng.entropy.set_book(165.20, 165.22)
    eng.entropy.send_responses = [
        {"status": "canceled", "outcome": "canceled", "filled_base": 0.0,
         "avg_px": None, "err": None, "unresolved": False}
    ]
    plan, reason = eng._plan(eng.hedge, eng.entropy, 15.0)
    assert reason == "ok"
    unresolved = asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))
    assert not unresolved
    assert len(eng.entropy.sent_orders) == 1
    assert len(eng.hedge.sent_orders) == 0
    assert eng.hedge.last_traded_ts == 0.0
    assert eng.entropy.position == 0.0 and eng.hedge.position == 0.0
    assert eng.entropy.cash == 0.0 and eng.hedge.cash == 0.0
    assert eng.consec_errors == 0 and not eng.halted
    assert not eng.has_pending_reconcile()
    assert eng.recent_trades[-1]["status"] == "skipped/canceled"
    assert eng.state_store.load()["pending_orders"] == {}


def test_entropy_first_follows_fill_with_live_hedge_price_and_exact_partial_qty(tmp_path):
    eng = make_engine(
        f"""
thresholds:
  midline_bps: 0.0
  upper_bps: 4.0
  lower_bps: 4.0
  latency_buffer_bps: 4.0
sizing:
  max_order_notional_usd: 15.0
execution:
  execution_mode: entropy_first
  premium_persist_sec: 0.0
  leg_slippage_bps: 15.0
  hedge_leg_slippage_bps: 1.5
  entropy_first_hedge_slippage_bps: 20.0
persistence:
  enabled: true
  state_dir: "{tmp_path}"
""",
        hedge="lighter", px_step_e=0.01, px_step_h=0.001,
    )
    eng._log_csv = lambda *_: None
    eng._step, eng._min_base = 0.001, 0.025
    eng.hedge.size_decimals, eng.hedge.min_base = 3, 0.025
    _attach_order_ids(eng)
    eng.hedge.set_book(164.99, 165.00)
    eng.entropy.set_book(165.20, 165.22)

    # Simulate Lighter moving up to 165.05 while Entropy executes and partially fills 0.070.
    orig_entropy_send = eng.entropy.send_taker

    async def entropy_partial_fill(**kwargs):
        eng.hedge.set_book(165.04, 165.05)
        res = await orig_entropy_send(**kwargs)
        return dict(res, filled_base=0.070, avg_px=165.20, fee_bps=0.86, fee_usd=0.001)

    eng.entropy.send_taker = entropy_partial_fill
    eng.hedge.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 0.070,
         "avg_px": 165.05, "err": None, "unresolved": False, "fee_bps": 0.0, "fee_usd": 0.0}
    ]
    plan, _ = eng._plan(eng.hedge, eng.entropy, 15.0)
    unresolved = asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))
    assert not unresolved
    assert len(eng.entropy.sent_orders) == 1
    assert len(eng.hedge.sent_orders) == 1
    hedge_order = eng.hedge.sent_orders[0]
    assert hedge_order["is_buy"] is True
    assert hedge_order["qty"] == pytest.approx(0.070)
    # Fresh live ask 165.05 + 20 bps = 165.380 (rounded down to 0.001)
    assert hedge_order["limit_px"] == pytest.approx(165.38)
    assert eng.entropy.position == pytest.approx(-0.070)
    assert eng.hedge.position == pytest.approx(0.070)
    assert eng._net_position() == pytest.approx(0.0)
    assert eng.trades == 1
    assert eng.state_store.load()["pending_orders"] == {}


def test_entropy_first_unresolved_primary_never_sends_hedge(tmp_path):
    eng = make_engine(
        f"""
thresholds:
  midline_bps: 0.0
  upper_bps: 4.0
  lower_bps: 4.0
  latency_buffer_bps: 4.0
sizing:
  max_order_notional_usd: 15.0
execution:
  execution_mode: entropy_first
  premium_persist_sec: 0.0
  leg_slippage_bps: 15.0
persistence:
  enabled: true
  state_dir: "{tmp_path}"
""",
        hedge="lighter", px_step_e=0.01, px_step_h=0.001,
    )
    eng._log_csv = lambda *_: None
    eng._step, eng._min_base = 0.001, 0.025
    _attach_order_ids(eng)
    eng.hedge.set_book(164.99, 165.00)
    eng.entropy.set_book(165.20, 165.22)
    eng.entropy.send_responses = [
        {"status": "timeout", "outcome": "unknown", "filled_base": 0.0,
         "avg_px": None, "err": None, "unresolved": True}
    ]
    plan, _ = eng._plan(eng.hedge, eng.entropy, 15.0)
    unresolved = asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))
    assert unresolved is True
    assert len(eng.entropy.sent_orders) == 1
    assert len(eng.hedge.sent_orders) == 0
    assert eng.has_pending_reconcile()
    assert len(eng.state_store.load()["pending_orders"]) == 1


def test_entropy_first_unresolved_followup_reconciles_via_pair_ctx(tmp_path):
    eng = make_engine(
        f"""
thresholds:
  midline_bps: 0.0
  upper_bps: 4.0
  lower_bps: 4.0
  latency_buffer_bps: 4.0
sizing:
  max_order_notional_usd: 15.0
execution:
  execution_mode: entropy_first
  premium_persist_sec: 0.0
  leg_slippage_bps: 15.0
persistence:
  enabled: true
  state_dir: "{tmp_path}"
""",
        hedge="lighter", px_step_e=0.01, px_step_h=0.001,
    )
    eng._log_csv = lambda *_: None
    eng._step, eng._min_base = 0.001, 0.025
    eng.hedge.size_decimals, eng.hedge.min_base = 3, 0.025
    _attach_order_ids(eng)
    eng.hedge.set_book(164.99, 165.00)
    eng.entropy.set_book(165.20, 165.22)

    eng.entropy.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 0.090,
         "avg_px": 165.20, "err": None, "unresolved": False, "fee_bps": 0.86, "fee_usd": 0.0013}
    ]
    eng.hedge.send_responses = [
        {"status": "timeout", "outcome": "unknown", "filled_base": 0.0,
         "avg_px": None, "err": None, "unresolved": True}
    ]
    plan, _ = eng._plan(eng.hedge, eng.entropy, 15.0)
    unresolved = asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))
    assert unresolved is True
    persisted = eng.state_store.load()
    assert len(persisted["pending_orders"]) == 1
    pending_key, pending_rec = next(iter(persisted["pending_orders"].items()))
    assert pending_rec["venue_key"] == "hedge"
    assert pending_rec["qty"] == pytest.approx(0.090)
    assert pending_rec["pair_ctx"]["sibling_venue"] == "entropy"
    assert pending_rec["pair_ctx"]["sibling_filled"] == pytest.approx(0.090)

    # Resolve pending hedge order through P0-1 reconciliation
    eng.hedge.order_queries[pending_rec["client_order_id"]] = {
        "status": "filled", "outcome": "filled", "filled_base": 0.090,
        "avg_px": 165.00, "err": None, "unresolved": False,
    }
    eng.entropy.chain_position = -0.090
    eng.hedge.chain_position = 0.090
    assert asyncio.run(eng._reconcile_pending_orders()) is True
    assert not eng.has_pending_reconcile()
    assert eng.hedge.position == pytest.approx(0.090)
    assert eng.total_fill_edge == pytest.approx(0.090 * (165.20 - 165.00) - 0.0013)


def test_entropy_first_reduce_only_closes_both_legs(tmp_path):
    eng = make_engine(
        f"""
thresholds:
  midline_bps: 0.0
  upper_bps: 4.0
  lower_bps: 4.0
  latency_buffer_bps: 4.0
sizing:
  max_order_notional_usd: 20.0
execution:
  execution_mode: entropy_first
  premium_persist_sec: 0.0
  leg_slippage_bps: 15.0
persistence:
  enabled: true
  state_dir: "{tmp_path}"
""",
        hedge="lighter", px_step_e=0.01, px_step_h=0.001,
    )
    eng._log_csv = lambda *_: None
    eng._step, eng._min_base = 0.001, 0.025
    eng.entropy.fee_bps = 1.0
    eng.hedge.size_decimals, eng.hedge.min_base = 3, 0.025
    _attach_order_ids(eng)
    eng.entropy.position = -0.091
    eng.hedge.position = 0.091
    eng.entropy.set_book(164.98, 165.00)
    eng.hedge.set_book(165.20, 165.22)

    eng.entropy.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 0.091,
         "avg_px": 165.00, "err": None, "unresolved": False, "fee_bps": 0.86, "fee_usd": 0.0013}
    ]
    eng.hedge.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 0.091,
         "avg_px": 165.20, "err": None, "unresolved": False, "fee_bps": 0.0, "fee_usd": 0.0}
    ]
    plan, _ = eng._plan(eng.entropy, eng.hedge, 20.0, reduce_only=True, cap_base=0.091)
    assert plan is not None
    unresolved = asyncio.run(eng._execute(eng.entropy, eng.hedge, plan, reduce_only=True))
    assert not unresolved
    assert eng.entropy.sent_orders[0]["reduce_only"] is True
    assert eng.hedge.sent_orders[0]["reduce_only"] is True
    assert eng.entropy.position == pytest.approx(0.0)
    assert eng.hedge.position == pytest.approx(0.0)


def test_require_entropy_update_on_persist_and_max_entry_book_age():
    eng = make_engine(
        """
thresholds:
  midline_bps: 0.0
  upper_bps: 4.0
  lower_bps: 4.0
execution:
  premium_persist_sec: 0.2
  require_entropy_update_on_persist: true
  max_entry_book_age_sec: 2.0
risk:
  max_book_age_sec: 30.0
  max_reduce_book_age_sec: 15.0
""",
        hedge="lighter",
    )
    eng._schedule_poke = lambda delay: None
    t0 = time.monotonic()
    now = time.time()
    eng.entropy.set_book(165.20, 165.22)
    eng.hedge.set_book(164.99, 165.00)
    eng.entropy.book.last_update_mono = t0
    eng.hedge.book.last_update_mono = t0

    # Arms at t0 + 0.05
    assert eng._scan(now, now_mono=t0 + 0.05) is None
    # After 0.30s (>= 0.2s persist), Lighter updated but Entropy still stale -> MUST NOT fire
    eng.hedge.book.last_update_mono = t0 + 0.30
    assert eng._scan(now + 0.30, now_mono=t0 + 0.30) is None
    # Once Entropy pushes a fresh snapshot -> fires immediately!
    eng.entropy.book.last_update_mono = t0 + 0.50
    res = eng._scan(now + 0.50, now_mono=t0 + 0.50)
    assert res is not None and res[1] is eng.entropy

    # Entry book age cap (2.0s) blocks new entries at 2.5s without tripping 30s watchdog,
    # while reduce_only exits remain allowed under 15.0s reduce limit.
    eng._reset_signal()
    eng.entropy.book.last_update_mono = t0
    eng.hedge.book.last_update_mono = t0 + 2.5
    assert eng._entry_book_fresh(eng.entropy, reduce_only=False, now_mono=t0 + 2.5) is False
    assert eng._trade_book_fresh(eng.entropy, reduce_only=False, now_mono=t0 + 2.5) is True
    assert eng._entry_book_fresh(eng.entropy, reduce_only=True, now_mono=t0 + 2.5) is True


def test_balanced_fill_retries_deferred_actual_fee_verification(tmp_path):
    async def exercise():
        eng = make_engine(
            f"""
thresholds:
  midline_bps: 0.0
  upper_bps: 4.0
  lower_bps: 4.0
  latency_buffer_bps: 4.0
entropy:
  taker_fee_bps: 1.0
sizing:
  max_order_notional_usd: 15.0
execution:
  execution_mode: entropy_first
  premium_persist_sec: 0.0
  leg_slippage_bps: 15.0
  fill_fee_retry_delay_sec: 0.05
persistence:
  enabled: true
  state_dir: "{tmp_path}"
""",
            hedge="lighter", px_step_e=0.01, px_step_h=0.001,
        )
        eng._log_csv = lambda *_: None
        eng._step, eng._min_base = 0.001, 0.025
        eng.entropy.fee_bps = 1.0
        eng.hedge.size_decimals, eng.hedge.min_base = 3, 0.025
        _attach_order_ids(eng)
        eng.hedge.set_book(164.99, 165.00)
        eng.entropy.set_book(165.20, 165.22)
        eng.entropy.send_responses = [
            {"status": "filled", "outcome": "filled", "filled_base": 0.090,
             "avg_px": 165.20, "order_id": 777, "err": None, "unresolved": False}
        ]
        eng.hedge.send_responses = [
            {"status": "filled", "outcome": "filled", "filled_base": 0.090,
             "avg_px": 165.00, "err": None, "unresolved": False, "fee_bps": 0.0, "fee_usd": 0.0}
        ]
        calls = {"n": 0}

        async def delayed_fee(**kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                return None  # simulate exchange indexing lag right after fill
            return {"fee_usd": 0.001294, "fee_bps": 0.87, "filled_base": 0.090}

        eng.entropy.fetch_fill_fee = delayed_fee
        plan, _ = eng._plan(eng.hedge, eng.entropy, 15.0)
        await eng._execute(eng.hedge, eng.entropy, plan)
        assert calls["n"] == 1
        assert eng._bg_tasks
        assert eng.daily_pnl_breakdown["actual_fee_usd"] == pytest.approx(0.0)
        assert eng.daily_pnl_breakdown["estimated_fee_usd"] > 0.0

        await asyncio.gather(*tuple(eng._bg_tasks))
        assert calls["n"] == 2
        assert eng.daily_pnl_breakdown["actual_fee_usd"] == pytest.approx(0.001294)
        assert eng.daily_pnl_breakdown["estimated_fee_usd"] == pytest.approx(0.0)
        persisted = eng.state_store.load()
        assert persisted["daily_pnl_breakdown"]["actual_fee_usd"] == pytest.approx(0.001294)

    asyncio.run(exercise())


def test_entropy_first_defers_to_maybe_hedge_if_hedge_book_drops_mid_flight(tmp_path):
    eng = make_engine(
        f"""
thresholds:
  midline_bps: 0.0
  upper_bps: 4.0
  lower_bps: 4.0
  latency_buffer_bps: 4.0
sizing:
  max_order_notional_usd: 15.0
execution:
  execution_mode: entropy_first
  premium_persist_sec: 0.0
  leg_slippage_bps: 15.0
persistence:
  enabled: true
  state_dir: "{tmp_path}"
""",
        hedge="lighter", px_step_e=0.01, px_step_h=0.001,
    )
    eng._log_csv = lambda *_: None
    eng._step, eng._min_base = 0.001, 0.025
    eng.entropy.size_decimals, eng.entropy.min_base = 3, 0.025
    eng.hedge.size_decimals, eng.hedge.min_base = 3, 0.025
    _attach_order_ids(eng)
    eng.hedge.set_book(164.99, 165.00)
    eng.entropy.set_book(165.20, 165.22)

    orig_entropy_send = eng.entropy.send_taker

    async def fill_while_hedge_disconnects(**kwargs):
        # Hedge feed disconnects while Entropy is in flight
        eng.hedge.book.clear()
        res = await orig_entropy_send(**kwargs)
        return dict(res, filled_base=0.090, avg_px=165.20, fee_bps=0.86, fee_usd=0.0013)

    eng.entropy.send_taker = fill_while_hedge_disconnects
    plan, _ = eng._plan(eng.hedge, eng.entropy, 15.0)
    unresolved = asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))
    assert not unresolved
    assert len(eng.entropy.sent_orders) == 1
    assert len(eng.hedge.sent_orders) == 0
    assert eng.entropy.position == pytest.approx(-0.090)
    assert eng._net_position() == pytest.approx(-0.090)

    # _maybe_hedge immediately flattens the exposure on Entropy without halting
    asyncio.run(eng._maybe_hedge())
    assert eng._net_position() == pytest.approx(0.0)
    assert not eng.halted


def test_entropy_first_prepare_reserves_followup_slippage_cap():
    eng = make_engine(
        """
thresholds:
  midline_bps: 0.0
  upper_bps: 4.0
  lower_bps: 4.0
  latency_buffer_bps: 4.0
sizing:
  max_order_notional_usd: 15.0
execution:
  execution_mode: entropy_first
  premium_persist_sec: 0.0
  leg_slippage_bps: 15.0
  hedge_leg_slippage_bps: 1.5
  entropy_first_hedge_slippage_bps: 20.0
""",
        hedge="lighter", px_step_e=0.01, px_step_h=0.001,
    )
    eng._step, eng._min_base = 0.001, 0.025
    eng.entropy.fee_bps = 1.0
    # At ask 164.60 / bid 164.82, naive cap/164.82 = 0.091, which would exceed $15 once
    # Lighter +20 bps follow-up bound (164.929 * 0.091 = $15.0085) is applied.
    eng.hedge.set_book(164.59, 164.60)
    eng.entropy.set_book(164.82, 164.84)
    plan, reason = eng._plan(eng.hedge, eng.entropy, 15.0)
    assert reason == "ok" and plan.qty == pytest.approx(0.091)
    prepared = eng._prepare_pair_order(eng.hedge, eng.entropy, plan, reduce_only=False, now=time.time())
    assert prepared is not None
    prep_plan, _, _ = prepared
    projected_follow = eng.hedge.px_round(164.60 * 1.002, round_up=False)
    assert prep_plan.qty == pytest.approx(0.090)
    assert prep_plan.qty * projected_follow <= 15.0 + 1e-9


def test_entropy_first_followup_strictly_respects_max_order_notional_on_price_jump(tmp_path):
    # Case 1: sell_entropy — Lighter ask jumps mid-flight while Entropy is executing
    eng = make_engine(
        f"""
thresholds:
  midline_bps: 0.0
  upper_bps: 4.0
  lower_bps: 4.0
  latency_buffer_bps: 4.0
sizing:
  max_order_notional_usd: 15.0
execution:
  execution_mode: entropy_first
  premium_persist_sec: 0.0
  leg_slippage_bps: 15.0
  hedge_leg_slippage_bps: 1.5
  entropy_first_hedge_slippage_bps: 20.0
persistence:
  enabled: true
  state_dir: "{tmp_path}/case1"
""",
        hedge="lighter", px_step_e=0.01, px_step_h=0.001,
    )
    eng._log_csv = lambda *_: None
    eng._step, eng._min_base = 0.001, 0.025
    eng.entropy.fee_bps = 1.0
    eng.hedge.size_decimals, eng.hedge.min_base = 3, 0.025
    _attach_order_ids(eng)
    eng.hedge.set_book(164.99, 165.00)
    eng.entropy.set_book(165.22, 165.24)

    orig_send = eng.entropy.send_taker

    async def entropy_fill_then_lighter_jumps(**kwargs):
        # Jump Lighter ask to 166.80 so 0.090 * 166.80 = $15.012 (> $15 cap)
        eng.hedge.set_book(166.79, 166.80)
        res = await orig_send(**kwargs)
        return dict(res, filled_base=kwargs["qty"], avg_px=165.22, fee_bps=0.86, fee_usd=0.0013)

    eng.entropy.send_taker = entropy_fill_then_lighter_jumps
    plan, _ = eng._plan(eng.hedge, eng.entropy, 15.0)
    unresolved = asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))
    assert not unresolved
    assert len(eng.hedge.sent_orders) == 1
    h_ord = eng.hedge.sent_orders[0]
    assert h_ord["qty"] == pytest.approx(0.089)
    assert h_ord["qty"] * max(166.80, h_ord["limit_px"]) <= 15.0 + 1e-9

    # Case 2: buy_entropy reduce-only (0.091 position) — Lighter bid jumps to 164.97
    # (0.091 * 164.97 = $15.01227 > $15 cap) while Entropy buy executes.
    eng2 = make_engine(
        f"""
thresholds:
  midline_bps: 0.0
  upper_bps: 4.0
  lower_bps: 4.0
  latency_buffer_bps: 4.0
sizing:
  max_order_notional_usd: 15.0
execution:
  execution_mode: entropy_first
  premium_persist_sec: 0.0
  leg_slippage_bps: 15.0
  hedge_leg_slippage_bps: 1.5
  entropy_first_hedge_slippage_bps: 20.0
persistence:
  enabled: true
  state_dir: "{tmp_path}/case2"
""",
        hedge="lighter", px_step_e=0.01, px_step_h=0.001,
    )
    eng2._log_csv = lambda *_: None
    eng2._step, eng2._min_base = 0.001, 0.025
    eng2.entropy.fee_bps = 1.0
    eng2.hedge.size_decimals, eng2.hedge.min_base = 3, 0.025
    _attach_order_ids(eng2)
    eng2.entropy.position = -0.091
    eng2.hedge.position = 0.091
    eng2.entropy.set_book(164.48, 164.50)
    eng2.hedge.set_book(164.72, 164.74)

    orig_send2 = eng2.entropy.send_taker

    async def entropy_buy_then_lighter_bid_jumps(**kwargs):
        # 0.091 * 164.97 = $15.01227 > $15 cap
        eng2.hedge.set_book(164.97, 164.99)
        res = await orig_send2(**kwargs)
        return dict(res, filled_base=kwargs["qty"], avg_px=164.50, fee_bps=0.86, fee_usd=0.0013)

    eng2.entropy.send_taker = entropy_buy_then_lighter_bid_jumps
    plan2, _ = eng2._plan(eng2.entropy, eng2.hedge, 15.0, reduce_only=True, cap_base=0.091)
    assert plan2 is not None and plan2.qty == pytest.approx(0.091)
    unresolved2 = asyncio.run(eng2._execute(eng2.entropy, eng2.hedge, plan2, reduce_only=True))
    assert not unresolved2
    assert len(eng2.hedge.sent_orders) == 1
    h_ord2 = eng2.hedge.sent_orders[0]
    assert h_ord2["reduce_only"] is True
    assert h_ord2["qty"] == pytest.approx(0.090)
    assert h_ord2["qty"] * max(164.97, h_ord2["limit_px"]) <= 15.0 + 1e-9


