"""Automatic 1-minute orderbook data recorder.

While the bot runs (live or --record-only), both venues' actual order books
are sampled once per second and aggregated into one CSV row per minute.
This is the dataset users analyze (tools/analyze.py) to choose
thresholds.midline_bps / upper_bps / lower_bps for config.yaml.

Definitions (all in bps, fees NOT included — the engine adds fees on top):

    premium    = (entropy_mid / hedge_mid - 1) * 1e4
                 the mid-to-mid premium of Entropy over the hedge venue;
                 its long-run center is what midline_bps hardcodes.
    sell_edge  = (entropy_bid / hedge_ask - 1) * 1e4
                 the EXECUTABLE premium for SELL-entropy/BUY-hedge; the
                 engine fires this direction when sell_edge clears
                 midline_bps + upper_bps (plus fees).
    buy_edge   = (hedge_bid / entropy_ask - 1) * 1e4
                 the executable premium for BUY-entropy/SELL-hedge; fires
                 when buy_edge clears lower_bps - midline_bps (plus fees).

Bid/ask columns are the minute's last fresh sample (close). A row is only
written for minutes with at least one sample where both books were fresh;
`samples` says how many of the ~60 seconds qualified.

When ENTROPY_RECORD_DEPTH=1, optional ENTROPY_RECORD_DEPTH_INTERVAL_SEC and
ENTROPY_RECORD_DEPTH_LEVELS control the JSONL snapshots independently of the
minute CSV. For example, 0.1 and 5 keep five bid and ask levels per venue at 10 Hz.
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import math
import os
import stat
import tempfile
import time
from copy import copy
from datetime import datetime, timezone
from typing import Optional

from .book import OrderBook

log = logging.getLogger("recorder")

HEADER = ["minute_ts", "time_utc",
          "entropy_bid", "entropy_ask", "hedge_bid", "hedge_ask",
          "premium_open_bps", "premium_high_bps", "premium_low_bps",
          "premium_close_bps", "premium_mean_bps", "premium_std_bps",
          "sell_edge_mean_bps", "sell_edge_max_bps",
          "buy_edge_mean_bps", "buy_edge_max_bps", "samples"]


def _next_tick(due: float, interval: float, now: float) -> float:
    """Skip missed intervals if sampling or the event loop fell behind."""
    return due + (math.floor((now - due) / interval) + 1) * interval


class _MinuteAgg:
    __slots__ = ("minute", "n", "p_open", "p_high", "p_low", "p_close",
                 "p_sum", "p_sumsq", "s_sum", "s_max", "b_sum", "b_max",
                 "e_bid", "e_ask", "h_bid", "h_ask")

    def __init__(self, minute: int) -> None:
        self.minute = minute
        self.n = 0
        self.p_open = self.p_high = self.p_low = self.p_close = 0.0
        self.p_sum = self.p_sumsq = 0.0
        self.s_sum = 0.0
        self.s_max = -math.inf
        self.b_sum = 0.0
        self.b_max = -math.inf
        self.e_bid = self.e_ask = self.h_bid = self.h_ask = 0.0

    def add(self, e_bid: float, e_ask: float, h_bid: float, h_ask: float) -> None:
        e_mid = (e_bid + e_ask) / 2.0
        h_mid = (h_bid + h_ask) / 2.0
        prem = (e_mid / h_mid - 1.0) * 1e4
        sell_edge = (e_bid / h_ask - 1.0) * 1e4
        buy_edge = (h_bid / e_ask - 1.0) * 1e4
        if self.n == 0:
            self.p_open = self.p_high = self.p_low = prem
        self.n += 1
        self.p_high = max(self.p_high, prem)
        self.p_low = min(self.p_low, prem)
        self.p_close = prem
        self.p_sum += prem
        self.p_sumsq += prem * prem
        self.s_sum += sell_edge
        self.s_max = max(self.s_max, sell_edge)
        self.b_sum += buy_edge
        self.b_max = max(self.b_max, buy_edge)
        self.e_bid, self.e_ask, self.h_bid, self.h_ask = e_bid, e_ask, h_bid, h_ask

    def row(self) -> list:
        mean = self.p_sum / self.n
        var = max(self.p_sumsq / self.n - mean * mean, 0.0)
        ts = self.minute * 60
        return [ts,
                datetime.fromtimestamp(ts, tz=timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%SZ"),
                f"{self.e_bid:.10g}", f"{self.e_ask:.10g}",
                f"{self.h_bid:.10g}", f"{self.h_ask:.10g}",
                f"{self.p_open:.3f}", f"{self.p_high:.3f}",
                f"{self.p_low:.3f}", f"{self.p_close:.3f}",
                f"{mean:.3f}", f"{math.sqrt(var):.3f}",
                f"{self.s_sum / self.n:.3f}", f"{self.s_max:.3f}",
                f"{self.b_sum / self.n:.3f}", f"{self.b_max:.3f}",
                self.n]

    def with_previous(self, previous: dict[str, str]) -> _MinuteAgg:
        """Merge the flushed portion of this minute after a service restart."""
        whole = copy(self)
        n = int(previous["samples"])
        mean = float(previous["premium_mean_bps"])
        std = float(previous["premium_std_bps"])
        whole.n += n
        whole.p_open = float(previous["premium_open_bps"])
        whole.p_high = max(self.p_high, float(previous["premium_high_bps"]))
        whole.p_low = min(self.p_low, float(previous["premium_low_bps"]))
        whole.p_sum += mean * n
        whole.p_sumsq += (mean * mean + std * std) * n
        whole.s_sum += float(previous["sell_edge_mean_bps"]) * n
        whole.s_max = max(self.s_max, float(previous["sell_edge_max_bps"]))
        whole.b_sum += float(previous["buy_edge_mean_bps"]) * n
        whole.b_max = max(self.b_max, float(previous["buy_edge_max_bps"]))
        return whole


class MinuteRecorder:
    def __init__(self, path: str, entropy_book: OrderBook, hedge_book: OrderBook,
                 staleness_sec: float, interval_sec: float = 1.0, *,
                 max_book_age_sec: float = 0.0) -> None:
        self.path = path
        self.entropy_book = entropy_book
        self.hedge_book = hedge_book
        self.staleness_sec = staleness_sec
        self.max_book_age_sec = max_book_age_sec if max_book_age_sec > 0 else staleness_sec
        self.interval_sec = interval_sec
        self.record_depth = os.environ.get("ENTROPY_RECORD_DEPTH") == "1"
        self.depth_interval_sec = interval_sec
        self.depth_levels = 10
        if self.record_depth:
            self.depth_interval_sec = float(os.getenv("ENTROPY_RECORD_DEPTH_INTERVAL_SEC", str(interval_sec)))
            self.depth_levels = int(os.getenv("ENTROPY_RECORD_DEPTH_LEVELS", "10"))
            if not math.isfinite(self.depth_interval_sec) or self.depth_interval_sec <= 0:
                raise ValueError("ENTROPY_RECORD_DEPTH_INTERVAL_SEC must be finite and positive")
            if self.depth_levels < 1:
                raise ValueError("ENTROPY_RECORD_DEPTH_LEVELS must be a positive integer")
        self.rows_written = 0
        self._agg: Optional[_MinuteAgg] = None
        self._fh = None
        self._writer = None
        self._depth_fh = None
        self._depth_day = None

    def _write_depth(self, now: float, e_fresh: bool, h_fresh: bool) -> None:
        day = datetime.fromtimestamp(now, tz=timezone.utc).date()
        if day != self._depth_day:
            if self._depth_fh is not None:
                self._depth_fh.close()
                self._depth_fh = None
            stem = os.path.splitext(self.path)[0]
            os.makedirs(os.path.dirname(stem) or ".", exist_ok=True)
            self._depth_fh = open(f"{stem}.depth-{day}.jsonl", "a", buffering=1)
            self._depth_day = day

        books = []
        for book, fresh in ((self.entropy_book, e_fresh), (self.hedge_book, h_fresh)):
            age = book.age_mono()
            books.append({
                "fresh": fresh,
                "update_age_sec": round(age, 3) if age is not None else None,
                "bids": book.sorted_bids()[:self.depth_levels] if fresh else [],
                "asks": book.sorted_asks()[:self.depth_levels] if fresh else [],
            })
        row = {"ts": round(now, 3), "entropy": books[0], "hedge": books[1]}
        self._depth_fh.write(json.dumps(row, separators=(",", ":")) + "\n")

    def _open(self) -> None:
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        if os.path.exists(self.path) and os.path.getsize(self.path) > 0:
            # never append rows under a different schema's header
            with open(self.path) as fh0:
                if fh0.readline().strip() != ",".join(HEADER):
                    log.warning("%s has an old header — rotated to %s.old",
                                self.path, self.path)
                    os.replace(self.path, self.path + ".old")
        new = not os.path.exists(self.path) or os.path.getsize(self.path) == 0
        self._fh = open(self.path, "a", newline="")
        self._writer = csv.writer(self._fh)
        if new:
            self._writer.writerow(HEADER)
            self._fh.flush()
        log.info("recording 1-minute orderbook data -> %s", self.path)

    def _merge_last_written_minute(self, agg: _MinuteAgg) -> bool:
        """Replace a previous partial row atomically so readers see complete CSV rows."""
        with open(self.path, "rb") as source:
            end = source.seek(0, os.SEEK_END)
            source.seek(max(0, end - 4096))
            last = source.read().splitlines(keepends=True)[-1]
            fields = next(csv.reader([last.decode().strip()]))
            if len(fields) != len(HEADER) or fields[0] != str(agg.minute * 60):
                return False

            row = io.StringIO(newline="")
            csv.writer(row).writerow(agg.with_previous(dict(zip(HEADER, fields))).row())
            remaining = end - len(last)
            source.seek(0)
            directory = os.path.dirname(self.path) or "."
            with tempfile.NamedTemporaryFile(dir=directory, delete=False) as temp:
                try:
                    os.fchmod(temp.fileno(), stat.S_IMODE(os.fstat(source.fileno()).st_mode))
                    while remaining > 0:
                        chunk = source.read(min(remaining, 65536))
                        if not chunk:
                            raise OSError("minute CSV shortened during merge")
                        temp.write(chunk)
                        remaining -= len(chunk)
                    temp.write(row.getvalue().encode())
                    temp.close()
                    self._fh.close()
                    self._fh = self._writer = None
                    os.replace(temp.name, self.path)
                finally:
                    if os.path.exists(temp.name):
                        os.unlink(temp.name)
        self._open()
        return True

    def _flush_agg(self) -> None:
        if self._agg is None or self._agg.n == 0:
            self._agg = None
            return
        if self._writer is None:
            self._open()
            if self._merge_last_written_minute(self._agg):
                self.rows_written += 1
                self._agg = None
                return
        self._writer.writerow(self._agg.row())
        self._fh.flush()
        self.rows_written += 1
        self._agg = None

    def sample(self, now: Optional[float] = None) -> None:
        """Capture a minute-aggregation observation and, if enabled, depth."""
        self._sample(time.time() if now is None else now, minute=True, depth=self.record_depth)

    def _sample(self, now: float, *, minute: bool, depth: bool) -> None:
        bucket = int(now // 60)
        if minute and self._agg is not None and self._agg.minute != bucket:
            self._flush_agg()
        e_fresh = self.entropy_book.is_fresh(self.staleness_sec,
                                            max_book_age_sec=self.max_book_age_sec)
        h_fresh = self.hedge_book.is_fresh(self.staleness_sec,
                                          max_book_age_sec=self.max_book_age_sec)
        if depth:
            self._write_depth(now, e_fresh, h_fresh)
        if not (minute and e_fresh and h_fresh):
            return
        e_bid, e_ask = self.entropy_book.best_bid(), self.entropy_book.best_ask()
        h_bid, h_ask = self.hedge_book.best_bid(), self.hedge_book.best_ask()
        if None in (e_bid, e_ask, h_bid, h_ask):
            return
        if self._agg is None:
            self._agg = _MinuteAgg(bucket)
        self._agg.add(e_bid, e_ask, h_bid, h_ask)

    def close(self) -> None:
        """Flush the partial minute and close the file (call on shutdown)."""
        try:
            self._flush_agg()
        finally:
            if self._fh is not None:
                self._fh.close()
                self._fh = self._writer = None
            if self._depth_fh is not None:
                self._depth_fh.close()
                self._depth_fh = self._depth_day = None

    async def run(self, stop: asyncio.Event) -> None:
        clock = asyncio.get_running_loop().time
        minute_due = clock()
        depth_due = minute_due if self.record_depth else math.inf
        try:
            while not stop.is_set():
                now = clock()
                minute = now >= minute_due
                depth = now >= depth_due
                if minute or depth:
                    try:
                        self._sample(time.time(), minute=minute, depth=depth)
                    except Exception:
                        log.exception("recorder sample failed")
                    now = clock()
                    if minute:
                        minute_due = _next_tick(minute_due, self.interval_sec, now)
                    if depth:
                        depth_due = _next_tick(depth_due, self.depth_interval_sec, now)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=max(0.0, min(minute_due, depth_due) - now))
                except asyncio.TimeoutError:
                    pass
        finally:
            self.close()
            log.info("recorder stopped — %d minute row(s) written to %s",
                     self.rows_written, self.path)
