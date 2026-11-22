"""Websocket order-book feeds, writing into entropy_arb.book.OrderBook.

Two protocols, one per exchange family:

LighterBookFeed: zkLighter order_book channel (snapshot + diffs, server
    pings, diff-nonce gap detection — a gapped book is dropped and
    resubscribed rather than traded as a fiction).
HLBookFeed: the official Hyperliquid websocket (wss://api.hyperliquid.xyz/ws)
    l2Book channel with fast snapshots and client app-pings. Every price this
    bot trades on comes straight from the exchange that will fill the order.

Both touch the book on any inbound frame (connection-based freshness: a quiet
market is not stale, only a dead feed is), support proactive reconnect triggers
(T4 stale-book recovery), and reconnect with backoff.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Callable, Optional

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:
    try:
        from websockets import connect as ws_connect  # type: ignore
    except ImportError:  # pragma: no cover - allows offline unit tests without websockets
        ws_connect = None  # type: ignore

from .book import OrderBook

log = logging.getLogger("feeds")
INITIAL_SNAPSHOT_GRACE_SEC = 10.0
RECONNECT_COOLDOWN_SEC = 30.0


def _chan_id(channel: str) -> Optional[int]:
    """'order_book:32' / 'order_book/32' -> 32."""
    for sep in (":", "/"):
        if sep in channel:
            try:
                return int(channel.rsplit(sep, 1)[1])
            except ValueError:
                return None
    return None


class LighterBookFeed:
    """zkLighter order book for one market over one connection."""

    def __init__(self, name: str, ws_url: str, market_id: int, book: OrderBook,
                 notify: Callable[[], None],
                 on_disconnect: Optional[Callable[[str, str], None]] = None,
                 reconnect_evt: Optional[asyncio.Event] = None) -> None:
        self.name = name
        self.ws_url = ws_url
        self.market_id = market_id
        self.book = book
        self.notify = notify
        self.on_disconnect = on_disconnect
        self.reconnect_evt = reconnect_evt or asyncio.Event()
        self._nonce: Optional[int] = None
        self._synced = False
        self._active_ws = None
        self._opened_at = 0.0
        self._next_reconnect_at = 0.0

    def request_reconnect(self) -> None:
        """Proactively force a clean WS reconnect (e.g. on stale book detection)."""
        ws = self._active_ws
        now = time.monotonic()
        if (ws is None or self.reconnect_evt.is_set()
                or now < self._next_reconnect_at
                or now - self._opened_at < INITIAL_SNAPSHOT_GRACE_SEC):
            return
        self.reconnect_evt.set()
        self._next_reconnect_at = now + RECONNECT_COOLDOWN_SEC
        asyncio.create_task(ws.close())

    async def _subscribe(self, ws) -> None:
        await ws.send(json.dumps({"type": "subscribe",
                                  "channel": f"order_book/{self.market_id}"}))

    async def _handle_book(self, ws, msg: dict, snapshot: bool) -> None:
        if _chan_id(msg.get("channel", "")) != self.market_id:
            return
        ob = msg["order_book"]
        raw_end = ob.get("nonce")
        end = int(raw_end) if raw_end is not None else None
        prev = self._nonce
        if self._synced and prev is not None and end is not None and end <= prev:
            return
        if snapshot:
            self._nonce = end
            self._synced = True
            self.book.apply_lighter(ob, snapshot=True)
            log.info("[%s] snapshot: %d bids / %d asks", self.name,
                     len(self.book.bids), len(self.book.asks))
            self.notify()
            return
        if not self._synced:
            return
        raw_begin = ob.get("begin_nonce")
        begin = int(raw_begin) if raw_begin is not None else None
        if prev is not None and begin is not None and begin > prev + 1:
            log.warning("[%s] diff gap (had %s, got %s) — resubscribing",
                        self.name, prev, begin)
            self._nonce = None
            self._synced = False
            self.book.clear()
            self.notify()
            await ws.send(json.dumps({"type": "unsubscribe",
                                      "channel": f"order_book/{self.market_id}"}))
            await self._subscribe(ws)
            return
        if end is not None:
            self._nonce = end
        self.book.apply_lighter(ob, snapshot=False)
        self.notify()

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            self.reconnect_evt.clear()
            try:
                async with ws_connect(self.ws_url, max_size=2**23, open_timeout=10,
                                      ping_interval=15, ping_timeout=15) as ws:
                    self._active_ws = ws
                    self._opened_at = time.monotonic()
                    log.info("[%s] connected (%s)", self.name, self.ws_url)
                    self.book.clear()
                    self._nonce = None
                    self._synced = False
                    async for raw in ws:
                        backoff = 1.0
                        msg = json.loads(raw)
                        t = msg.get("type")
                        self.book.touch()
                        if t == "update/order_book":
                            await self._handle_book(ws, msg, snapshot=False)
                        elif t == "subscribed/order_book":
                            await self._handle_book(ws, msg, snapshot=True)
                        elif t == "connected":
                            await self._subscribe(ws)
                        elif t == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                        if stop.is_set() or self.reconnect_evt.is_set():
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not self.reconnect_evt.is_set():
                    log.warning("[%s] ws error: %s — reconnect in %.0fs",
                                self.name, e, backoff)
                    if self.on_disconnect is not None and not stop.is_set():
                        self.on_disconnect(self.name, f"book ws error: {e} (retry in {backoff:.0f}s)")
                else:
                    log.info("[%s] proactive ws reconnect closing old socket", self.name)
            finally:
                self._active_ws = None
            self.book.ready = False
            self.notify()
            if stop.is_set():
                break
            await asyncio.sleep(0.5 if self.reconnect_evt.is_set() else backoff)
            if not self.reconnect_evt.is_set():
                backoff = min(backoff * 2, 30.0)


class HLBookFeed:
    """Official Hyperliquid l2Book consumer for one coin (e.g. 'io:SNDK')."""

    def __init__(self, name: str, ws_url: str, coin: str, book: OrderBook,
                 notify: Callable[[], None], ping_sec: float = 5.0,
                 on_disconnect: Optional[Callable[[str, str], None]] = None,
                 reconnect_evt: Optional[asyncio.Event] = None) -> None:
        self.name = name
        self.ws_url = ws_url
        self.coin = coin
        self.book = book
        self.notify = notify
        self.ping_sec = ping_sec
        self.on_disconnect = on_disconnect
        self.reconnect_evt = reconnect_evt or asyncio.Event()
        self._snapped = False
        self._active_ws = None
        self._opened_at = 0.0
        self._next_reconnect_at = 0.0

    def request_reconnect(self) -> None:
        """Proactively force a clean WS reconnect (e.g. on stale book detection)."""
        ws = self._active_ws
        now = time.monotonic()
        if (ws is None or self.reconnect_evt.is_set()
                or now < self._next_reconnect_at
                or now - self._opened_at < INITIAL_SNAPSHOT_GRACE_SEC):
            return
        self.reconnect_evt.set()
        self._next_reconnect_at = now + RECONNECT_COOLDOWN_SEC
        asyncio.create_task(ws.close())

    def _on_frame(self, msg: dict) -> None:
        self.book.touch()
        if msg.get("channel") == "l2Book":
            d = msg.get("data") or {}
            if d.get("coin") == self.coin:
                self.book.apply_hl(d["levels"])
                if not self._snapped:
                    self._snapped = True
                    log.info("[%s] snapshot: %d bids / %d asks", self.name,
                             len(self.book.bids), len(self.book.asks))
                self.notify()

    async def _pinger(self, ws) -> None:
        try:
            while True:
                await asyncio.sleep(self.ping_sec)
                await ws.send(json.dumps({"method": "ping"}))
        except asyncio.CancelledError:
            raise
        except Exception:
            try:
                await ws.close()
            except Exception:
                pass

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            self.reconnect_evt.clear()
            ptask = None
            try:
                async with ws_connect(self.ws_url, max_size=2**23, open_timeout=10,
                                      ping_interval=15, ping_timeout=15) as ws:
                    self._active_ws = ws
                    self._opened_at = time.monotonic()
                    log.info("[%s] connected (official ws, %s)", self.name, self.coin)
                    self.book.clear()
                    self._snapped = False
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "l2Book", "coin": self.coin,
                                         "fast": True}}))
                    ptask = asyncio.create_task(self._pinger(ws))
                    async for raw in ws:
                        backoff = 1.0
                        self._on_frame(json.loads(raw))
                        if stop.is_set() or self.reconnect_evt.is_set():
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not self.reconnect_evt.is_set():
                    log.warning("[%s] ws error: %s — reconnect in %.0fs",
                                self.name, e, backoff)
                    if self.on_disconnect is not None and not stop.is_set():
                        self.on_disconnect(self.name, f"book ws error: {e} (retry in {backoff:.0f}s)")
                else:
                    log.info("[%s] proactive ws reconnect closing old socket", self.name)
            finally:
                self._active_ws = None
                if ptask is not None:
                    ptask.cancel()
            self.book.ready = False
            self.notify()
            if stop.is_set():
                break
            await asyncio.sleep(0.5 if self.reconnect_evt.is_set() else backoff)
            if not self.reconnect_evt.is_set():
                backoff = min(backoff * 2, 30.0)
