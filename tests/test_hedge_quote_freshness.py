"""Freshness at single-leg and asymmetric reduce-only dispatch."""
import asyncio

import pytest

from test_live_risk_regression import make_engine


def _engine(*, paired, state_dir=None):
    persistence = ""
    if state_dir is not None:
        persistence = f"\npersistence:\n  enabled: true\n  state_dir: '{state_dir}'\n"
    engine = make_engine("""
sizing:
  max_order_notional_usd: 15
  min_order_notional_usd: 10
risk:
  max_book_age_sec: 2
  max_reduce_book_age_sec: 10
""" + persistence, hedge="lighter")
    engine._step = 0.001
    engine._min_base = 0.025
    engine.entropy.size_decimals = engine.hedge.size_decimals = 3
    engine.entropy.position = 0.535 if paired else 0.035
    engine.hedge.position = -0.500 if paired else 0.0
    engine.entropy.set_book(143.98, 144.00)
    engine.hedge.set_book(144.25, 144.27)
    return engine


def _expire_quote_with_heartbeat(engine, venue):
    venue.book.last_update_mono -= engine.book_age_limit(reduce_only=True) + 1
    venue.book.touch()
    assert venue.book.is_fresh(engine.cfg.staleness_sec)
    assert not engine._trade_book_fresh(venue, reduce_only=True)


@pytest.mark.parametrize("paired", [False, True])
def test_heartbeat_without_recent_book_never_stages_cleanup(paired, monkeypatch):
    engine = _engine(paired=paired)
    stale_leg = engine.hedge if paired else engine.entropy
    _expire_quote_with_heartbeat(engine, stale_leg)
    engine._hedge_attempt_count = 1
    staged = []
    stage = engine._stage_orders

    def observe_stage(*args, **kwargs):
        staged.append(kwargs["purpose"])
        return stage(*args, **kwargs)

    monkeypatch.setattr(engine, "_stage_orders", observe_stage)
    asyncio.run(engine._maybe_hedge())
    assert not staged
    assert not engine.entropy.sent_orders and not engine.hedge.sent_orders
    assert engine._hedge_attempt_count == 1 and not engine.halted

    stale_leg.set_book(145.00, 145.01)
    asyncio.run(engine._maybe_hedge())
    assert staged
    assert all(order["reduce_only"] for order in engine.entropy.sent_orders)
    assert engine.entropy.sent_orders
    assert bool(engine.hedge.sent_orders) is paired
    assert abs(engine._net_position()) <= engine.cfg.net_tolerance_base


def test_hedge_can_use_quotes_inside_exit_age_even_when_too_old_to_open():
    engine = _engine(paired=False)
    engine.entropy.book.last_update_mono -= 4
    engine.entropy.book.touch()
    assert not engine._trade_book_fresh(engine.entropy, reduce_only=False)
    assert engine._trade_book_fresh(engine.entropy, reduce_only=True)
    asyncio.run(engine._maybe_hedge())
    assert len(engine.entropy.sent_orders) == 1


class _RefreshOnAcquire(asyncio.Lock):
    def __init__(self, refresh):
        super().__init__()
        self.refresh = refresh

    async def acquire(self):
        acquired = await super().acquire()
        self.refresh()
        return acquired


def test_single_leg_reads_new_quotes_and_inventory_after_lock():
    engine = _engine(paired=False)

    def venue_changes_while_lock_acquired():
        engine.entropy.position = 0.050
        engine.entropy.set_book(150.00, 150.02)

    engine._venue_locks["entropy"] = _RefreshOnAcquire(venue_changes_while_lock_acquired)
    asyncio.run(engine._maybe_hedge())
    order, = engine.entropy.sent_orders
    assert order["qty"] == 0.050
    assert order["limit_px"] == 149.70


def test_asymmetric_rebalance_prices_both_legs_after_lock():
    engine = _engine(paired=True)

    def venue_changes_while_lock_acquired():
        engine.entropy.position, engine.hedge.position = 0.530, -0.500
        engine.entropy.set_book(148.00, 148.02)
        engine.hedge.set_book(149.00, 149.02)

    engine._venue_locks["hedge"] = _RefreshOnAcquire(venue_changes_while_lock_acquired)
    result = asyncio.run(engine._try_rebalance_submin_hedge(20.0))
    assert result[0] is True
    sell, = engine.entropy.sent_orders
    buy, = engine.hedge.sent_orders
    assert sell["limit_px"] == 147.71 and buy["limit_px"] == 149.31
    assert sell["qty"] - buy["qty"] == pytest.approx(0.030)
    assert abs(engine._net_position()) <= engine.cfg.net_tolerance_base


@pytest.mark.parametrize("paired", [False, True])
def test_quote_expiry_during_staging_clears_known_unsent_ids(paired, tmp_path, monkeypatch):
    engine = _engine(paired=paired, state_dir=tmp_path)
    engine.entropy.reserve_order_id = lambda: "entropy-test"
    engine.hedge.reserve_order_id = lambda: "hedge-test"
    stage = engine._stage_orders
    calls = []

    def age_after_journal(*args, **kwargs):
        ids = stage(*args, **kwargs)
        assert ids
        calls.append(ids)
        _expire_quote_with_heartbeat(engine, engine.hedge if paired else engine.entropy)
        return ids

    monkeypatch.setattr(engine, "_stage_orders", age_after_journal)
    asyncio.run(engine._maybe_hedge())
    assert len(calls) == 1 and len(calls[0]) == (2 if paired else 1)
    assert not engine.entropy.sent_orders and not engine.hedge.sent_orders
    assert not engine.has_pending_reconcile()
    assert engine.state_store.load()["pending_orders"] == {}
    assert engine.hedges == engine._hedge_attempt_count == 0
