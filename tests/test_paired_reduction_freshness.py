"""The ordinary paired send uses the appropriate actual quote age on both legs."""
import asyncio
import time

from entropy_arb.state import StateStore
from test_live_risk_regression import make_engine, run_scan


def _matched_pair(*, reduce_only=True):
    engine = make_engine("""
risk:
  max_book_age_sec: 1
  max_reduce_book_age_sec: 5
""", hedge="lighter")
    engine.cfg.reduce_only_exit_threshold_bps = -5
    engine.entropy.position = -0.20
    engine.hedge.position = 0.20
    # Both directions are liquid, with a usable spread for closing the pair.
    engine.entropy.set_book(143.98, 144.00)
    engine.hedge.set_book(144.12, 144.15)
    if reduce_only:
        engine.circuit_breaker.trigger_reconcile_drift("test")
    return engine


def _age_book(venue, seconds):
    venue.book.last_update_mono = time.monotonic() - seconds
    venue.book.touch()  # A fresh heartbeat is not a fresh book.


def test_pair_reduction_uses_separate_quote_limit_and_never_heartbeat_age_alone():
    engine = _matched_pair(reduce_only=False)
    _age_book(engine.entropy, 2)
    assert run_scan(engine) is None  # Too old for ordinary strategy.

    engine.circuit_breaker.trigger_reconcile_drift("test")
    buy, sell, _ = run_scan(engine)
    assert buy is engine.entropy and sell is engine.hedge

    _age_book(sell, 7)
    assert run_scan(engine) is None  # Also too old for a reduction.
    sell.set_book(144.12, 144.15)  # Only an actual book update restores eligibility.
    assert run_scan(engine) is not None
    sell.book.alive_ts = time.time() - engine.cfg.staleness_sec - 1
    assert run_scan(engine) is None


def test_pair_reduction_rechecks_freshness_after_both_locks_are_acquired():
    engine = _matched_pair()
    buy, sell, plan = run_scan(engine)

    def cannot_stage(*args, **kwargs):
        raise AssertionError("stale paired plan must never reach the journal")

    engine._stage_orders = cannot_stage

    async def execute():
        locks = [engine._vlock(buy.key), engine._vlock(sell.key)]
        for lock in locks:
            await lock.acquire()
        _age_book(buy, 7)
        await engine._execute_locked(buy, sell, plan, reduce_only=True,
                                     acquired_locks=locks)
        assert all(not lock.locked() for lock in locks)

    asyncio.run(execute())
    assert buy.sent_orders == sell.sent_orders == []
    assert not engine.has_pending_reconcile()


def test_quote_expiry_during_pair_journal_fsync_clears_both_unsent_ids(tmp_path):
    engine = _matched_pair()
    buy, sell, plan = run_scan(engine)
    engine.state_store = StateStore(str(tmp_path / "nvda-state.json"))
    buy.reserve_order_id = lambda: "buy-id"
    sell.reserve_order_id = lambda: "sell-id"
    store_save = engine.state_store.save
    snapshots = []

    def save(payload):
        snapshots.append(dict(payload["pending_orders"]))
        saved = store_save(payload)
        if len(snapshots) == 1:
            # Reproduce remaining quote lifetime being consumed by a slow fsync.
            _age_book(sell, 7)
        return saved

    engine.state_store.save = save
    assert asyncio.run(engine._execute(buy, sell, plan, reduce_only=True)) is False
    assert len(snapshots) == 2 and len(snapshots[0]) == 2
    assert snapshots[-1] == engine.state_store.load(strict=True)["pending_orders"] == {}
    assert buy.sent_orders == sell.sent_orders == []
    assert not engine.has_pending_reconcile() and not engine.halted
