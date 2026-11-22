"""Book regressions for reconnect timing and non-advancing depth updates."""
import asyncio
import time

from entropy_arb.book import OrderBook
from entropy_arb.feeds import INITIAL_SNAPSHOT_GRACE_SEC, HLBookFeed, LighterBookFeed


class FakeSocket:
    def __init__(self):
        self.closed = 0

    async def close(self):
        self.closed += 1


def test_reconnect_allows_initial_snapshot_and_cools_down_across_sockets():
    async def verify():
        feeds = [
            LighterBookFeed("LIGHTER", "unused", 15, OrderBook(), lambda: None),
            HLBookFeed("ENTROPY", "unused", "io:NVDA", OrderBook(), lambda: None),
        ]
        for feed in feeds:
            first = FakeSocket()
            feed._active_ws = first
            feed._opened_at = time.monotonic()
            feed.request_reconnect()
            await asyncio.sleep(0)
            assert first.closed == 0

            feed._opened_at -= INITIAL_SNAPSHOT_GRACE_SEC + 1
            feed.request_reconnect()
            feed.request_reconnect()
            await asyncio.sleep(0)
            assert first.closed == 1

            second = FakeSocket()
            feed._active_ws = second
            feed.reconnect_evt.clear()
            feed.request_reconnect()
            await asyncio.sleep(0)
            assert second.closed == 0

    asyncio.run(verify())


def test_lighter_discards_old_and_duplicate_sequence_updates():
    async def verify():
        book = OrderBook()
        feed = LighterBookFeed("LIGHTER", "unused", 15, book, lambda: None)
        snapshot = {"channel": "order_book:15", "order_book": {
            "nonce": 100, "bids": [{"price": "143", "size": "1"}, {"price": "144", "size": "5"}],
            "asks": [{"price": "145", "size": "3"}]}}
        await feed._handle_book(None, snapshot, snapshot=True)

        removal = {"channel": "order_book:15", "order_book": {
            "begin_nonce": 101, "nonce": 101, "bids": [{"price": "144", "size": "0"}]}}
        await feed._handle_book(None, removal, snapshot=False)
        fresh_at = book.last_update_mono
        old = {"channel": "order_book:15", "order_book": {
            "begin_nonce": 100, "nonce": 100, "bids": [{"price": "144", "size": "5"}]}}
        await feed._handle_book(None, old, snapshot=False)
        await feed._handle_book(None, removal, snapshot=False)
        assert feed._nonce == 101
        assert book.best_bid() == 143
        assert book.last_update_mono == fresh_at

    asyncio.run(verify())


def test_crossed_book_is_not_usable():
    book = OrderBook()
    book.apply_hl([[{"px": "145", "sz": "1"}], [{"px": "144", "sz": "1"}]])
    assert not book.is_fresh(10)
