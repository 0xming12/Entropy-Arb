"""Regression tests for entry eligibility and reducing unmatched positions."""
import asyncio
import time

from test_live_risk_regression import make_engine, run_scan


def _profitable_pair(engine, depth=1.0):
    engine.entropy.set_book(143.98, 144.0, sz=depth)
    engine.hedge.set_book(144.25, 144.27, sz=depth)


def test_enabled_risk_guards_wait_for_fee_and_both_account_balances():
    engine = make_engine("""
risk:
  max_taker_fee_bps: 2
  min_available_margin_usd: 100
""", hedge="lighter")
    _profitable_pair(engine)
    engine._prime_startup_risk()
    assert run_scan(engine) is None

    engine._handle_fee_observation(0.5)
    engine.circuit_breaker.update_available_margin("ENTROPY", 110)
    assert run_scan(engine) is None
    engine.circuit_breaker.update_available_margin(engine.hedge.name, float("nan"))
    assert run_scan(engine) is None
    engine.circuit_breaker.update_available_margin(engine.hedge.name, 120)
    assert run_scan(engine) is not None


def test_signal_waits_two_seconds_with_eligible_depth_and_resets_after_gap():
    engine = make_engine("""
sizing:
  max_order_notional_usd: 15
""", hedge="lighter")
    engine.cfg.premium_persist_sec = 2.0

    async def check():
        now = time.time()
        _profitable_pair(engine, depth=0.05)
        assert engine._scan(now, now_mono=100) is None
        assert engine._scan(now + 1.0, now_mono=101) is None
        _profitable_pair(engine)
        assert engine._scan(now + 2.0, now_mono=102) is None
        assert engine._scan(now + 3.0, now_mono=103) is None
        assert engine._scan(now + 5.0, now_mono=105) is None
        assert engine._scan(now + 6.0, now_mono=106) is None
        plan = engine._scan(now + 7.0, now_mono=107)
        assert plan is not None and 10 <= plan[2].buy_notional <= 15

    asyncio.run(check())


def test_reduce_only_exit_can_take_a_bounded_loss_below_normal_entry_threshold():
    # Default reduce_only (reduce_only_exit_threshold_bps=None) must NOT dump a
    # delta-neutral pair at a loss on flat books.
    engine = make_engine(leg_slip=50, hedge="lighter")
    engine.circuit_breaker.trigger_reconcile_drift("test")
    engine.entropy.position = -0.20
    engine.hedge.position = 0.20
    engine.entropy.set_book(143.99, 144.00)
    engine.hedge.set_book(143.99, 144.00)
    assert run_scan(engine) is None

    # Explicit opt-in reduce_only_exit_threshold_bps allows bounded-loss unwind.
    engine.cfg.reduce_only_exit_threshold_bps = -5.0
    buy, sell, plan = run_scan(engine)
    assert buy is engine.entropy and sell is engine.hedge
    assert plan.marginal_premium_bps < 0 and plan.qty <= 0.20
    bounds = engine._order_price_bounds(buy, sell, plan, plan.qty, reduce_only=True)
    assert bounds is not None
    assert engine._worst_case_check(buy, sell, plan.qty, *bounds, reduce_only=True)[0]
    asyncio.run(engine._execute(buy, sell, plan))
    assert buy.sent_orders[0]["reduce_only"] and sell.sent_orders[0]["reduce_only"]


def test_subminimum_unmatched_exposure_blocks_new_pair():
    engine = make_engine("""
sizing:
  max_order_notional_usd: 15
""", hedge="lighter")
    engine._step, engine._min_base = 0.001, 0.025
    engine.entropy.position = 0.035  # about $5 at these NVDA prices
    _profitable_pair(engine)
    asyncio.run(engine._maybe_hedge())
    assert engine.is_reduce_only() and run_scan(engine) is None
    assert engine.entropy.sent_orders == engine.hedge.sent_orders == []
    engine.entropy.position = 0.0
    assert not engine.is_reduce_only() and run_scan(engine) is not None


def test_guard_rechecks_rounded_limits_when_live_fee_erodes_a_previous_plan():
    engine = make_engine(leg_slip=50, hedge="lighter")
    engine.entropy.set_book(100.10, 100.12)
    engine.hedge.set_book(99.98, 100.00)
    plan, _ = engine._plan(engine.hedge, engine.entropy, 15.0)
    assert plan is not None
    engine._handle_fee_observation(8.0)  # Bps, the cap guard is disabled in this test.
    assert engine._order_price_bounds(engine.hedge, engine.entropy, plan, plan.qty) is None
    asyncio.run(engine._execute(engine.hedge, engine.entropy, plan))
    assert engine.entropy.sent_orders == engine.hedge.sent_orders == []


def test_exhausted_hedge_budget_cannot_immediately_start_a_second_retry_cycle():
    engine = make_engine("""
execution:
  hedge_max_retries: 2
  hedge_retry_delay_sec: 0
""", hedge="lighter")
    engine.entropy.position = 0.2
    _profitable_pair(engine)
    engine.entropy.send_responses = [
        {"status": "canceled", "outcome": "canceled", "filled_base": 0.0,
         "err": None, "unresolved": False, "client_order_id": f"test-{i}"}
        for i in range(2)
    ]
    asyncio.run(engine._maybe_hedge())
    asyncio.run(engine._maybe_hedge())
    assert engine.halted and len(engine.entropy.sent_orders) == 2
