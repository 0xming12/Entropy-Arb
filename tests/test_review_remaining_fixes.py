import asyncio
import time

import pytest

from entropy_arb.feeds import HLBookFeed
from test_live_risk_regression import make_engine


def l2(source_ms, bid="144.00"):
    return {"channel": "l2Book", "data": {
        "coin": "io:NVDA", "time": source_ms,
        "levels": [[{"px": bid, "sz": "4"}], [{"px": "144.02", "sz": "4"}]],
    }}


def test_delayed_hl_message_cannot_trigger_hedge_or_refresh_quotes():
    eng = make_engine(hedge="lighter")
    ent = eng.entropy
    ent.position = 0.15
    feed = HLBookFeed("ENTROPY", "unused", "io:NVDA", ent.book, lambda: None)
    now = int(time.time() * 1000)
    feed._on_frame(l2(now - 3_600_000))
    asyncio.run(eng._maybe_hedge())
    assert ent.sent_orders == [] and not ent.book.ready
    # Within the transport age ceiling, but already too old for a reduce order.
    feed._on_frame(l2(now - 16_000))
    assert ent.book.age_mono() >= 16
    asyncio.run(eng._maybe_hedge())
    assert ent.sent_orders == []
    feed._on_frame(l2(now))
    before = ent.book.last_update_mono
    feed._on_frame(l2(now - 100, bid="138"))
    feed._on_frame(l2(now, bid="136"))
    feed._on_frame(l2(now + 3000, bid="135"))
    assert ent.book.best_bid() == 144 and ent.book.last_update_mono == before
    asyncio.run(eng._maybe_hedge())
    assert len(ent.sent_orders) == 1 and ent.sent_orders[0]["reduce_only"]


@pytest.mark.parametrize("budget", [0, 10])
def test_asymmetric_rebalance_skips_if_worst_single_leg_would_exceed_budget(budget):
    engine = make_engine(f"""
sizing:
  max_order_notional_usd: 15
risk:
  max_rebalance_exposure_usd: {budget}
""", hedge="lighter")
    engine._step = 0.001
    for v in engine.venues.values():
        v.size_decimals = 3
        v.min_base = 0.025
        v.set_book(144, 144.02)
    engine.entropy.position = 0.17
    engine.hedge.position = -0.15
    # Previous rebalancing placed 0.09 sell and 0.07 buy, which would leave
    # +0.09 NVDA (about $13) if only the 0.07 buy filled.
    asyncio.run(engine._maybe_hedge())
    assert engine._net_position() == pytest.approx(0.02)
    assert all(not venue.sent_orders for venue in engine.venues.values())
    assert engine._hedge_attempt_count == 0
    # A separately authorized risk budget permits the same matched cleanup.
    engine.cfg.risk.max_rebalance_exposure_usd = 13.1
    engine._unhedgeable_until = 0
    asyncio.run(engine._maybe_hedge())
    assert engine._net_position() == pytest.approx(0)
    assert all(venue.sent_orders[0]["reduce_only"] for venue in engine.venues.values())


@pytest.mark.parametrize("late_sibling", [False, True])
def test_deferred_actual_fee_corrects_basis_streak_and_durable_ledger(tmp_path, late_sibling):
    async def exercise():
        eng = make_engine(f"""
sizing:
  max_order_notional_usd: 15
persistence:
  enabled: true
  state_dir: "{tmp_path}"
""", hedge="lighter", leg_slip=0)
        eng._log_csv = lambda *_: None
        e, h = eng.entropy, eng.hedge
        e.set_book(143.98, 144)
        h.set_book(144.19, 144.20)
        for venue in (e, h):
            venue.reserve_order_id = lambda: "pair-order"
            original_send = venue.send_taker

            async def send_with_id(*, client_order_id, original_send=original_send, **kwargs):
                return await original_send(**kwargs)

            venue.send_taker = send_with_id
        e.send_responses = [{"status": "filled", "outcome": "filled", "filled_base": 0.10,
                             "avg_px": 144, "err": None, "unresolved": False}]
        hedge_fill = {"status": "filled", "outcome": "filled", "filled_base": 0.08,
                      "avg_px": 144.19, "err": None, "unresolved": False,
                      "fee_bps": 0, "fee_usd": 0}
        if late_sibling:
            h.send_responses = [{"status": "timeout", "outcome": "unknown", "filled_base": 0,
                                 "avg_px": None, "err": "timeout", "unresolved": True}]
            h.order_queries["pair-order"] = hedge_fill
            e.chain_position, h.chain_position = 0.10, -0.08
        else:
            h.send_responses = [hedge_fill]
        e.fill_fee_response = {"fee_usd": 0.02, "fee_bps": 0.02 / (0.1 * 144) * 1e4,
                               "filled_base": 0.10}
        plan, _ = eng._plan(e, h, 15)
        assert plan is not None
        await eng._execute(e, h, plan)
        assert eng._bg_tasks
        if late_sibling:
            assert not await eng._reconcile_pending_orders()
        await asyncio.gather(*tuple(eng._bg_tasks))
        if late_sibling:
            pending = eng.state_store.load()["pending_orders"]["hedge:pair-order"]
            assert pending["pair_ctx"]["sibling_fee_unit"] == pytest.approx(0.2)
            assert await eng._reconcile_pending_orders()
            assert await eng._reconcile_pending_orders()  # A retry must not rebook the fee.
        persisted = eng.state_store.load()
        assert e.cash == pytest.approx(-14.42)
        assert persisted["venue_ledger"]["entropy"]["cash"] == pytest.approx(-14.42)
        basis = persisted["inv_basis"]
        assert basis["signed_qty"] * basis["avg_cash"] == pytest.approx(-0.0008)
        assert basis["signed_qty"] * basis["avg_booked"] == pytest.approx(-0.0008)
        assert eng.circuit_breaker.daily_realized_pnl_usd == pytest.approx(-0.0008)
        assert eng.circuit_breaker.consecutive_losses == 1
        assert eng.total_fill_edge == pytest.approx(-0.0008)
        if not late_sibling:
            assert eng.recent_trades[-1]["fill"] == pytest.approx(-0.0008)

    asyncio.run(exercise())
