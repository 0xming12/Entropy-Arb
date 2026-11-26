"""Public Entropy trades and BBO changes for record-only execution replay."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from datetime import datetime, timezone
import json
import logging
import math
from pathlib import Path
import time

from .feeds import ws_connect

log = logging.getLogger("market_capture")


class _DailyJsonl:
    def __init__(self, stem: Path, kind: str) -> None:
        self.stem = stem
        self.kind = kind
        self.day = None
        self.file = None

    def append(self, row: dict) -> None:
        day = datetime.fromtimestamp(row["exchange_ts_ms"] / 1000.0, timezone.utc).date()
        if day != self.day:
            self.stem.parent.mkdir(parents=True, exist_ok=True)
            self.close()
            path = self.stem.parent / f"{self.stem.name}.entropy-{self.kind}-{day}.jsonl"
            self.file = path.open("a", encoding="utf-8", buffering=1)
            self.day = day
        self.file.write(json.dumps(row, separators=(",", ":")) + "\n")

    def close(self) -> None:
        if self.file is not None:
            self.file.close()
            self.file = None
        self.day = None


def _positive(value) -> bool:
    try:
        return math.isfinite(float(value)) and float(value) > 0
    except (TypeError, ValueError, OverflowError):
        return False


class EntropyMarketTape:
    """Records the public trade tape and changes to either top quote."""

    def __init__(self, minute_csv: str, coin: str) -> None:
        stem = Path(minute_csv).with_suffix("")
        self.coin = coin
        self.trades = _DailyJsonl(stem, "trades")
        self.bbo = _DailyJsonl(stem, "bbo")
        self._seen_trades = OrderedDict()
        self._last_source_ms = 0
        self._last_quotes = None

    def observe(self, message: dict, receive_ns: int) -> None:
        channel = message.get("channel")
        data = message.get("data")
        if channel == "l2Book" and isinstance(data, dict):
            self._observe_book(data, receive_ns)
        elif channel == "trades" and isinstance(data, list):
            for trade in data:
                if isinstance(trade, dict):
                    self._observe_trade(trade, receive_ns)
        elif channel == "subscriptionResponse" and isinstance(data, dict):
            subscription = data.get("subscription") or {}
            if subscription.get("coin") == self.coin:
                log.info("[%s] public %s subscribed", self.coin, subscription.get("type"))

    def _observe_trade(self, trade: dict, receive_ns: int) -> None:
        ts, tid, side = trade.get("time"), trade.get("tid"), trade.get("side")
        if (trade.get("coin") != self.coin or type(ts) is not int or ts <= 0
                or type(tid) is not int or side not in ("A", "B")):
            return
        price, quantity = trade.get("px"), trade.get("sz")
        if not (_positive(price) and _positive(quantity)):
            return
        key = (ts, tid)
        if key in self._seen_trades:
            return
        self.trades.append({"coin": self.coin, "exchange_ts_ms": ts,
                            "received_ts_ns": receive_ns, "trade_id": tid,
                            "price": str(price), "quantity": str(quantity),
                            "side": "buy" if side == "B" else "sell", "raw_side": side})
        self._seen_trades[key] = None
        if len(self._seen_trades) > 4096:
            self._seen_trades.popitem(last=False)

    def _observe_book(self, data: dict, receive_ns: int) -> None:
        ts = data.get("time")
        if (data.get("coin") != self.coin or type(ts) is not int
                or ts <= 0 or ts < self._last_source_ms):
            return
        try:
            bids, asks = data["levels"]
            bid = (bids[0]["px"], bids[0]["sz"])
            ask = (asks[0]["px"], asks[0]["sz"])
        except (IndexError, KeyError, TypeError, ValueError):
            return
        if not all(_positive(value) for value in (*bid, *ask)):
            return
        if float(bid[0]) > float(ask[0]):
            return
        quotes = (tuple(map(str, bid)), tuple(map(str, ask)))
        if quotes != self._last_quotes:
            self.bbo.append({"coin": self.coin, "exchange_ts_ms": ts,
                             "received_ts_ns": receive_ns, "bid": quotes[0], "ask": quotes[1]})
            self._last_quotes = quotes
        self._last_source_ms = ts

    def close(self) -> None:
        try:
            self.trades.close()
        finally:
            self.bbo.close()


async def record_entropy_market(ws_url: str, coin: str, minute_csv: str, stop: asyncio.Event) -> None:
    """Use a public socket for the extra tape without sharing the trading book."""
    tape = EntropyMarketTape(minute_csv, coin)
    clock = asyncio.get_running_loop().time
    backoff = 1.0
    try:
        while not stop.is_set():
            try:
                async with ws_connect(ws_url, open_timeout=10, close_timeout=2,
                                      ping_interval=15, max_size=2**23) as ws:
                    for subscription in ({"type": "l2Book", "coin": coin, "fast": True},
                                         {"type": "trades", "coin": coin}):
                        await ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))
                    ping_due = clock() + 25
                    while not stop.is_set():
                        if clock() >= ping_due:
                            await ws.send(json.dumps({"method": "ping"}))
                            ping_due = clock() + 25
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=max(0.0, ping_due - clock()))
                        except asyncio.TimeoutError:
                            continue
                        received = time.time_ns()
                        backoff = 1.0
                        message = json.loads(raw)
                        if isinstance(message, dict):
                            tape.observe(message, received)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if not stop.is_set():
                    log.warning("[%s] public capture interrupted (%s); retry in %.0fs", coin, error, backoff)
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=backoff)
                    except asyncio.TimeoutError:
                        pass
                    backoff = min(backoff * 2, 30.0)
    finally:
        tape.close()
