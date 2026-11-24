"""Execution timing and outcome journal / 执行耗时与结果日志.

Each live order attempt appends one JSON line (``"type": "attempt"``) with
monotonic timestamps measured relative to the moment the strategy selected the
signal, plus the outcome of each leg. Single-leg exposure that is still open
when an attempt settles is tracked until the net position is back inside
``net_tolerance_base`` and then written as ``"type": "exposure"``.

The journal is written only after both legs have returned, never between the
first fill and the follow-up hedge, and any write error is logged and ignored:
measurement must never block or break trading.

用途：拆解「策略决定发单 → 交易所回执」的耗时，并统计第一周验收指标
（Entropy 成交率、对冲腿结果、单腿暴露时长）。``tools/exec_report.py`` 汇总此文件。
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any, Dict, Optional

log = logging.getLogger("entropy_arb.exec")

#: Ordered timestamp marks recorded by the engine for every attempt.
#: selected   — strategy picked the signal (before waiting for venue locks)
#: exec_start — locks held, entering order preparation
#: prepared   — final size/limit checks passed (the ``[ARB]`` log line)
#: staged     — order journal durably written (fsync) before the first send
#: prechecked — post-staging book freshness re-check passed
#: follow_staged — follow-up hedge order journaled (entropy_first only)
MARKS = ("selected", "exec_start", "prepared", "staged", "prechecked", "follow_staged")


def _finite(value: Any) -> Optional[float]:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


class ExecTrace:
    """Timing marks and leg outcomes for one order attempt (monotonic clock)."""

    def __init__(self, *, attempt_id: str, mode: str, direction: str, reduce_only: bool,
                 selected_mono: float, selected_wall: float) -> None:
        self.attempt_id = attempt_id
        self.mode = mode
        self.direction = direction
        self.reduce_only = reduce_only
        self.t0 = float(selected_mono)
        self.selected_wall = float(selected_wall)
        self.marks: Dict[str, float] = {"selected": self.t0}
        self.legs: Dict[str, Dict[str, Any]] = {}
        self.leg_ack_mono: Dict[str, float] = {}
        self.fields: Dict[str, Any] = {}

    def mark(self, name: str, t: Optional[float] = None) -> float:
        t = time.monotonic() if t is None else float(t)
        self.marks[name] = t
        return t

    def ms(self, t: Optional[float]) -> Optional[float]:
        if t is None:
            return None
        return round((float(t) - self.t0) * 1000.0, 3)

    def set(self, **fields: Any) -> None:
        self.fields.update(fields)

    def leg(self, venue, info: Dict[str, Any], *, call_start: float, call_end: float,
            qty: float, limit_px: Optional[float]) -> None:
        """Record one venue call. Adapter-internal ``*_mono`` timings are optional."""
        timing = info.get("timing") if isinstance(info.get("timing"), dict) else {}
        rec: Dict[str, Any] = {
            "name": getattr(venue, "name", venue.key),
            "qty": _finite(qty),
            "limit_px": _finite(limit_px),
            "status": info.get("status"),
            "outcome": info.get("outcome"),
            "filled_base": _finite(info.get("filled_base")) or 0.0,
            "avg_px": _finite(info.get("avg_px")),
            "unresolved": bool(info.get("unresolved")),
            "err": None if info.get("err") is None else str(info.get("err"))[:200],
            "call_start_ms": self.ms(call_start),
            "ack_ms": self.ms(call_end),
        }
        for key, value in timing.items():
            if key.endswith("_mono") and _finite(value) is not None:
                rec[f"{key[:-5]}_ms"] = self.ms(value)
        self.legs[venue.key] = rec
        self.leg_ack_mono[venue.key] = float(call_end)

    def filled_ack_monos(self) -> Dict[str, float]:
        return {k: self.leg_ack_mono[k] for k, rec in self.legs.items()
                if rec["filled_base"] > 0.0 and k in self.leg_ack_mono}

    def to_record(self) -> Dict[str, Any]:
        acks = self.filled_ack_monos()
        leg_gap_ms = (round((max(acks.values()) - min(acks.values())) * 1000.0, 3)
                      if len(acks) >= 2 else None)
        return {
            "type": "attempt",
            "attempt_id": self.attempt_id,
            "ts": round(self.selected_wall, 3),
            "mode": self.mode,
            "direction": self.direction,
            "reduce_only": self.reduce_only,
            "marks_ms": {name: self.ms(self.marks[name]) for name in MARKS
                         if name in self.marks},
            "legs": self.legs,
            "leg_gap_ms": leg_gap_ms,
            **self.fields,
        }


class ExecJournal:
    """Append-only JSONL writer that never raises into the trading path."""

    def __init__(self, path: Optional[str]) -> None:
        self.path = path or ""

    @property
    def enabled(self) -> bool:
        return bool(self.path)

    def write(self, record: Dict[str, Any]) -> None:
        if not self.path:
            return
        try:
            d = os.path.dirname(self.path)
            if d:
                os.makedirs(d, exist_ok=True)
            line = json.dumps(record, ensure_ascii=False, separators=(",", ":"),
                              allow_nan=False, default=str)
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except Exception:
            log.exception("exec journal write failed (%s)", self.path)


class ExposureTracker:
    """Tracks one open single-leg exposure episode at a time."""

    def __init__(self, journal: ExecJournal) -> None:
        self.journal = journal
        self.open_since_mono: Optional[float] = None
        self.open_since_wall: Optional[float] = None
        self.attempt_id: Optional[str] = None
        self.peak_abs_net = 0.0

    @property
    def is_open(self) -> bool:
        return self.open_since_mono is not None

    def open(self, *, start_mono: float, start_wall: float, net: float,
             attempt_id: Optional[str]) -> None:
        if self.is_open:
            self.peak_abs_net = max(self.peak_abs_net, abs(net))
            return
        self.open_since_mono = float(start_mono)
        self.open_since_wall = float(start_wall)
        self.attempt_id = attempt_id
        self.peak_abs_net = abs(net)

    def update(self, *, net: float, tolerance: float, can_close: bool,
               now_mono: Optional[float] = None, now_wall: Optional[float] = None) -> Optional[float]:
        """Open on unexplained net exposure, close once flat. Returns closed duration (s)."""
        now_mono = time.monotonic() if now_mono is None else float(now_mono)
        now_wall = time.time() if now_wall is None else float(now_wall)
        if abs(net) > tolerance:
            self.open(start_mono=now_mono, start_wall=now_wall, net=net, attempt_id=None)
            return None
        if not self.is_open or not can_close:
            return None
        duration = max(now_mono - self.open_since_mono, 0.0)
        self.journal.write({
            "type": "exposure",
            "attempt_id": self.attempt_id,
            "ts": round(self.open_since_wall, 3),
            "duration_ms": round(duration * 1000.0, 3),
            "peak_abs_net": self.peak_abs_net,
        })
        log.info("[EXPOSURE] closed after %.0f ms (attempt %s, peak |net| %.6g)",
                 duration * 1000.0, self.attempt_id or "-", self.peak_abs_net)
        self.open_since_mono = self.open_since_wall = None
        self.attempt_id = None
        self.peak_abs_net = 0.0
        return duration
