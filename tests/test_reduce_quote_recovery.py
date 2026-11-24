"""An offline regression for safe residual hedging after a stale public feed."""
import asyncio
import time

from entropy_arb.feeds import HLBookFeed, INITIAL_SNAPSHOT_GRACE_SEC
from test_live_risk_regression import make_engine


class _OfflineSocket:
    def __init__(self):
        self.close_calls = 0

    async def close(self):
        self.close_calls += 1


def test_watchdog_retries_residual_after_one_fresh_snapshot_during_cooldown():
    async def scenario():
        engine = make_engine("""
risk:
  max_book_age_sec: 0
  max_reduce_book_age_sec: 8
sizing:
  max_order_notional_usd: 15
""", hedge="lighter")
        holding = engine.entropy
        holding.position = 0.09
        holding.set_book(142.20, 142.25)
        engine.hedge.set_book(143.99, 144.01)
        holding.book.last_update_mono -= 60

        feed = HLBookFeed("ENTROPY", "unused", "io:NVDA", holding.book,
                          engine._update_evt.set)
        holding.request_reconnect = feed.request_reconnect
        feed._on_frame({"channel": "pong"})
        assert holding.book.is_fresh(engine.cfg.staleness_sec)
        await engine._evaluate()
        assert not holding.sent_orders
        assert engine._hedge_attempt_count == 0
        assert not engine._is_unhedgeable_cooldown()

        # A separate temporary constraint, such as a venue rate limit,
        # can leave a retry cooldown active during quote recovery.
        engine._unhedgeable_until = time.monotonic() + 60
        engine._unhedgeable_snapshot = engine._pos_snapshot()

        old_socket = _OfflineSocket()
        feed._active_ws = old_socket
        feed._opened_at = time.monotonic()
        engine._book_watchdog_tick()
        await asyncio.sleep(0)
        assert old_socket.close_calls == 0

        feed._opened_at -= INITIAL_SNAPSHOT_GRACE_SEC + 1
        engine._book_watchdog_tick()
        engine._book_watchdog_tick()
        await asyncio.sleep(0)
        assert old_socket.close_calls == 1
        assert not holding.sent_orders

        new_socket = _OfflineSocket()
        feed._active_ws = new_socket
        feed._opened_at = time.monotonic()
        feed.reconnect_evt.clear()
        holding.book.clear()
        evaluations = asyncio.Queue()
        original_evaluate = engine._evaluate

        async def observe_evaluate():
            await original_evaluate()
            evaluations.put_nowait(engine._net_position())

        engine._evaluate = observe_evaluate
        strategy = asyncio.create_task(engine._strategy_loop())
        try:
            # This is the only fresh book notification after reconnect. The
            # strategy receives it while the original retry delay is active.
            feed._on_frame({"channel": "l2Book", "data": {
                "coin": "io:NVDA", "time": int(time.time() * 1000),
                "levels": [[{"px": "144.00", "sz": "20"}],
                           [{"px": "144.02", "sz": "20"}]],
            }})
            assert await asyncio.wait_for(evaluations.get(), 2.0) == 0.09
            assert not holding.sent_orders

            # Expire only the cooldown, without ageing the new book or
            # injecting another market event. A pending journal still blocks.
            engine._unhedgeable_until = time.monotonic() - 1
            engine._reconcile_required = True
            engine._book_watchdog_tick()
            assert not engine._update_evt.is_set()
            engine._reconcile_required = False
            engine._book_watchdog_tick()
            assert abs(await asyncio.wait_for(evaluations.get(), 2.0)) < 1e-10
            assert new_socket.close_calls == 0
            assert not engine.hedge.sent_orders
            assert len(holding.sent_orders) == 1
            order = holding.sent_orders[0]
            assert order["reduce_only"] and not order["is_buy"]
            assert order["qty"] == 0.09
            assert 143.0 < order["limit_px"] <= 144.0
        finally:
            engine.request_stop()
            await strategy

    asyncio.run(scenario())


def test_watchdog_uses_reduction_age_while_holding_unmatched_exposure():
    engine = make_engine("""
risk:
  max_book_age_sec: 2
  max_reduce_book_age_sec: 8
""", hedge="lighter")
    engine.entropy.position = 0.09
    reconnects = []
    for venue in engine.venues.values():
        venue.set_book(143.99, 144.01)
        venue.book.last_update_mono -= 4
        venue.request_reconnect = lambda key=venue.key: reconnects.append(key)

    assert not engine._trade_book_fresh(engine.entropy, reduce_only=False)
    engine._book_watchdog_tick()

    assert engine._update_evt.is_set()
    assert not reconnects
