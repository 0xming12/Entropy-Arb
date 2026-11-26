"""Live risk controls: circuit breaker (loss, fee spike/unknown, reconcile drift,
margin floor/unknown) and US market session filter (with DST, holidays, and
early-close dates).

All modules use Python stdlib only (zoneinfo for America/New_York DST handling)
so they can be unit-tested deterministically without network access.
"""
from __future__ import annotations

import math

from dataclasses import dataclass
from datetime import datetime, time as dt_time, timezone
from typing import Any, Dict, Iterable, Optional, Set, Tuple
from zoneinfo import ZoneInfo


def _parse_hhmm(value: str) -> dt_time:
    parts = value.strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"invalid HH:MM time: {value!r}")
    hour, minute = int(parts[0]), int(parts[1])
    return dt_time(hour=hour, minute=minute)


def _validate_yyyy_mm_dd(value: str) -> str:
    s = str(value).strip()
    datetime.strptime(s, "%Y-%m-%d")
    return s


@dataclass(frozen=True)
class SessionState:
    session: str               # "regular" | "off_hours" | "weekend" | "holiday" | "disabled"
    reduce_only: bool
    cap_multiplier: float
    spread_buffer_bps: float


class SessionFilter:
    """US equity perpetuals market-hours filter in America/New_York (auto DST).

    Regular trading hours: Mon-Fri [regular_start, regular_end) ET (default 09:30-16:00).
    Early close days: Mon-Fri [regular_start, early_close_end) ET (default 09:30-13:00).
    Holidays: full-day off-hours / reduce-only (same protection as weekend).
    Off-hours: Mon-Fri outside regular hours (pre-market / after-hours).
    Weekend: Sat-Sun all day.
    """

    def __init__(
        self,
        *,
        enabled: bool = False,
        tz_name: str = "America/New_York",
        regular_start: str = "09:30",
        regular_end: str = "16:00",
        early_close_end: str = "13:00",
        off_hours_mode: str = "reduce_only",
        off_hours_cap_multiplier: float = 0.3,
        off_hours_spread_buffer_bps: float = 5.0,
        weekend_reduce_only: bool = True,
        holidays: Optional[Iterable[str]] = None,
        early_close_dates: Optional[Iterable[str]] = None,
    ) -> None:
        self.enabled = bool(enabled)
        self.tz_name = tz_name
        self.tz = ZoneInfo(tz_name)
        self.regular_start = _parse_hhmm(regular_start)
        self.regular_end = _parse_hhmm(regular_end)
        self.early_close_end = _parse_hhmm(early_close_end)
        self.off_hours_mode = off_hours_mode
        self.off_hours_cap_multiplier = float(off_hours_cap_multiplier)
        self.off_hours_spread_buffer_bps = float(off_hours_spread_buffer_bps)
        self.weekend_reduce_only = bool(weekend_reduce_only)
        self.holidays: Set[str] = {_validate_yyyy_mm_dd(d) for d in (holidays or ())}
        self.early_close_dates: Set[str] = {
            _validate_yyyy_mm_dd(d) for d in (early_close_dates or ())
        }

    def _to_local(self, now: Optional[float | datetime]) -> datetime:
        if now is None:
            return datetime.now(tz=self.tz)
        if isinstance(now, datetime):
            if now.tzinfo is None:
                return now.replace(tzinfo=timezone.utc).astimezone(self.tz)
            return now.astimezone(self.tz)
        return datetime.fromtimestamp(float(now), tz=self.tz)

    def classify(self, now: Optional[float | datetime] = None) -> str:
        local_dt = self._to_local(now)
        if local_dt.weekday() >= 5:
            return "weekend"
        day_str = local_dt.strftime("%Y-%m-%d")
        if day_str in self.holidays:
            return "holiday"
        end_time = self.early_close_end if day_str in self.early_close_dates else self.regular_end
        t = local_dt.time()
        if self.regular_start <= t < end_time:
            return "regular"
        return "off_hours"

    def evaluate(self, now: Optional[float | datetime] = None) -> SessionState:
        if not self.enabled:
            return SessionState(
                session="disabled",
                reduce_only=False,
                cap_multiplier=1.0,
                spread_buffer_bps=0.0,
            )
        session = self.classify(now)
        if session == "regular":
            return SessionState(
                session="regular",
                reduce_only=False,
                cap_multiplier=1.0,
                spread_buffer_bps=0.0,
            )
        if session in ("weekend", "holiday") and (
            self.weekend_reduce_only or self.off_hours_mode == "reduce_only"
        ):
            return SessionState(
                session=session,
                reduce_only=True,
                cap_multiplier=0.0,
                spread_buffer_bps=self.off_hours_spread_buffer_bps,
            )
        if self.off_hours_mode == "reduce_only":
            return SessionState(
                session=session,
                reduce_only=True,
                cap_multiplier=0.0,
                spread_buffer_bps=self.off_hours_spread_buffer_bps,
            )
        return SessionState(
            session=session,
            reduce_only=False,
            cap_multiplier=self.off_hours_cap_multiplier,
            spread_buffer_bps=self.off_hours_spread_buffer_bps,
        )


class CircuitBreaker:
    """Tracks daily PnL, consecutive losing trades, Entropy taker fee spikes
    and unknown-fee fail-safe (P1-3), position reconcile drift, and available
    margin floors / unknown-margin fail-safe (P1-3), with JSON persistence."""

    def __init__(
        self,
        *,
        max_daily_loss_usd: float = 0.0,
        max_consecutive_losses: int = 0,
        breaker_action: str = "reduce_only",
        max_taker_fee_bps: float = 0.0,
        min_available_margin_usd: float = 0.0,
        tz_name: str = "America/New_York",
    ) -> None:
        self.max_daily_loss_usd = max(float(max_daily_loss_usd), 0.0)
        self.max_consecutive_losses = max(int(max_consecutive_losses), 0)
        self.breaker_action = breaker_action  # "reduce_only" | "halt"
        self.max_taker_fee_bps = max(float(max_taker_fee_bps), 0.0)
        self.min_available_margin_usd = max(float(min_available_margin_usd), 0.0)
        self._tz = ZoneInfo(tz_name)

        self._day_key: Optional[str] = None
        self.daily_realized_pnl_usd: float = 0.0
        self.consecutive_losses: int = 0

        self.loss_tripped: bool = False
        self.loss_reason: str = ""
        self.fee_tripped: bool = False
        self.fee_unknown: bool = False
        self.fee_reason: str = ""
        self.last_observed_fee_bps: Optional[float] = None
        self._venue_fees: Dict[str, float] = {}
        self._fee_unknown_venues: Set[str] = set()
        self.reconcile_tripped: bool = False
        self.reconcile_reason: str = ""
        self.margin_tripped: bool = False
        self.margin_unknown_venues: Set[str] = set()
        self.margin_reason: str = ""
        self._venue_free_margin: Dict[str, float] = {}

    def require_initial_observations(self, venue_names, fee_venue_names=None) -> None:
        """Fail closed before the first live fee and balance requests return."""
        if self.max_taker_fee_bps > 0.0:
            self.fee_unknown = True
            self._fee_unknown_venues.update(fee_venue_names or ("ENTROPY",))
            self.fee_reason = "Taker fee has not been verified"
        if self.min_available_margin_usd > 0.0:
            self.margin_unknown_venues.update(venue_names)
            self.margin_reason = "Available margin has not been verified on both venues"

    def day_for(self, now: Optional[float | datetime] = None) -> str:
        if now is None:
            dt = datetime.now(tz=self._tz)
        elif isinstance(now, datetime):
            dt = (now.replace(tzinfo=timezone.utc) if now.tzinfo is None else now).astimezone(self._tz)
        else:
            dt = datetime.fromtimestamp(float(now), tz=self._tz)
        return dt.strftime("%Y-%m-%d")

    _day_for = day_for

    def maybe_roll_day(self, now: Optional[float | datetime] = None) -> bool:
        """Reset daily loss counters on calendar day rollover. Returns True if rolled."""
        day = self.day_for(now)
        if self._day_key is None:
            self._day_key = day
            return False
        if day != self._day_key:
            self._day_key = day
            self.daily_realized_pnl_usd = 0.0
            self.consecutive_losses = 0
            if self.loss_tripped and self.breaker_action == "reduce_only":
                self.loss_tripped = False
                self.loss_reason = ""
            return True
        return False

    def record_trade_pnl(
        self,
        pnl_usd: float,
        *,
        mtm_pnl_usd: Optional[float] = None,
        now: Optional[float | datetime] = None,
        count_streak: bool = True,
        streak_pnl_usd: Optional[float] = None,
    ) -> Tuple[bool, str]:
        """Record a completed trade/hedge/funding realized PnL.

        `streak_pnl_usd` (optional) separates consecutive-loss attribution (e.g.
        exact round-trip PnL on a closing fill) from the incremental delta added
        to `daily_realized_pnl_usd`. Returns (newly_tripped, reason).
        """
        self.maybe_roll_day(now)
        self.daily_realized_pnl_usd += float(pnl_usd)
        if count_streak:
            streak_val = float(pnl_usd if streak_pnl_usd is None else streak_pnl_usd)
            if streak_val < 0.0:
                self.consecutive_losses += 1
            elif streak_val > 0.0:
                self.consecutive_losses = 0

        return self.check_losses(mtm_pnl_usd=mtm_pnl_usd, now=now)

    def check_losses(
        self,
        *,
        mtm_pnl_usd: Optional[float] = None,
        now: Optional[float | datetime] = None,
    ) -> Tuple[bool, str]:
        """Evaluate daily loss and consecutive loss limits. Returns (newly_tripped, reason)."""
        self.maybe_roll_day(now)
        if self.loss_tripped:
            return False, self.loss_reason

        if self.max_daily_loss_usd > 0.0:
            worst = self.daily_realized_pnl_usd
            if mtm_pnl_usd is not None:
                worst = min(worst, float(mtm_pnl_usd))
            if worst <= -self.max_daily_loss_usd:
                self.loss_tripped = True
                self.loss_reason = (
                    f"daily loss limit breached (pnl=${worst:.2f} <= "
                    f"-${self.max_daily_loss_usd:.2f})"
                )
                return True, self.loss_reason

        if (
            self.max_consecutive_losses > 0
            and self.consecutive_losses >= self.max_consecutive_losses
        ):
            self.loss_tripped = True
            self.loss_reason = (
                f"max consecutive losses reached ({self.consecutive_losses} >= "
                f"{self.max_consecutive_losses})"
            )
            return True, self.loss_reason

        return False, ""

    def update_taker_fee(self, fee_bps: Optional[float],
                         venue_name: str = "ENTROPY") -> Tuple[bool, bool, str]:
        """Update observed taker fee (bps) for a monitored venue (Entropy / HL hedge).

        P1-3: When max_taker_fee_bps > 0 and fee_bps is None (query failed /
        unknown), pauses new risk (reduce-only) until a valid fee is observed.
        Returns (newly_tripped, recovered, reason).
        """
        if fee_bps is not None and not math.isfinite(float(fee_bps)):
            fee_bps = None
        if self.max_taker_fee_bps <= 0.0:
            if fee_bps is not None:
                self._venue_fees[venue_name] = float(fee_bps)
                if venue_name == "ENTROPY":
                    self.last_observed_fee_bps = float(fee_bps)
            return False, False, ""

        was_blocked = self.fee_tripped or self.fee_unknown

        if fee_bps is None:
            self._fee_unknown_venues.add(venue_name)
            self.fee_unknown = True
            reason = (
                f"taker fee unknown on {', '.join(sorted(self._fee_unknown_venues))} "
                f"(query failed) — pausing new entries"
            )
            self.fee_reason = reason
            return (not was_blocked), False, reason

        val = float(fee_bps)
        self._fee_unknown_venues.discard(venue_name)
        self._venue_fees[venue_name] = val
        if venue_name == "ENTROPY":
            self.last_observed_fee_bps = val
        self.fee_unknown = bool(self._fee_unknown_venues)

        high = [
            f"{k}={v:.2f}bps"
            for k, v in sorted(self._venue_fees.items())
            if v > self.max_taker_fee_bps
        ]
        if high:
            reason = (
                f"taker fee {', '.join(high)} exceeds "
                f"limit {self.max_taker_fee_bps:.2f} bps (Growth Mode off?)"
            )
            was_tripped = self.fee_tripped
            self.fee_tripped = True
            self.fee_reason = reason
            return (not was_tripped), False, reason

        if self.fee_unknown:
            reason = (
                f"taker fee unknown on {', '.join(sorted(self._fee_unknown_venues))} "
                f"(query failed) — pausing new entries"
            )
            self.fee_tripped = False
            self.fee_reason = reason
            return False, False, reason

        if was_blocked:
            self.fee_tripped = False
            self.fee_reason = ""
            return False, True, (
                f"{venue_name} taker fee recovered to {val:.2f} bps"
            )
        return False, False, ""

    def trigger_reconcile_drift(self, reason: str) -> bool:
        """Enter reduce-only on unexpected position drift. Returns True if newly tripped."""
        was = self.reconcile_tripped
        self.reconcile_tripped = True
        self.reconcile_reason = reason
        return not was

    def update_available_margin(
        self, venue_name: str, free_usd: Optional[float]
    ) -> Tuple[bool, bool, str]:
        """Track per-venue available margin against min_available_margin_usd (P1-3).

        When min_available_margin_usd > 0 and free_usd is None (unknown),
        pauses new risk (reduce-only) until valid margin is observed.
        Returns (newly_tripped, recovered, message).
        """
        if free_usd is not None and not math.isfinite(float(free_usd)):
            free_usd = None
        if self.min_available_margin_usd <= 0.0:
            if free_usd is not None:
                self._venue_free_margin[venue_name] = float(free_usd)
            return False, False, ""

        was_blocked = self.margin_tripped or bool(self.margin_unknown_venues)

        if free_usd is None:
            self.margin_unknown_venues.add(venue_name)
            reason = (
                f"available margin unknown on {', '.join(sorted(self.margin_unknown_venues))} "
                f"— pausing new entries"
            )
            self.margin_reason = reason
            return (not was_blocked), False, reason

        self.margin_unknown_venues.discard(venue_name)
        val = float(free_usd)
        self._venue_free_margin[venue_name] = val
        low = [
            f"{k}=${v:.2f}"
            for k, v in self._venue_free_margin.items()
            if v < self.min_available_margin_usd
        ]
        if low:
            reason = (
                f"available margin below ${self.min_available_margin_usd:.2f}: "
                + ", ".join(low)
            )
            was_tripped = self.margin_tripped
            self.margin_tripped = True
            self.margin_reason = reason
            return (not was_tripped), False, reason
        if self.margin_unknown_venues:
            reason = (
                f"available margin unknown on {', '.join(sorted(self.margin_unknown_venues))} "
                f"— pausing new entries"
            )
            self.margin_tripped = False
            self.margin_reason = reason
            return False, False, reason
        if was_blocked:
            self.margin_tripped = False
            self.margin_reason = ""
            return False, True, (
                f"available margin recovered above ${self.min_available_margin_usd:.2f}"
            )
        return False, False, ""

    def to_state(self, now: Optional[float | datetime] = None) -> Dict[str, Any]:
        if now is not None or self._day_key is None:
            self.maybe_roll_day(now)
        return {
            "day_key": self._day_key,
            "daily_realized_pnl_usd": self.daily_realized_pnl_usd,
            "consecutive_losses": self.consecutive_losses,
            "loss_tripped": self.loss_tripped,
            "loss_reason": self.loss_reason,
        }

    def load_state(
        self,
        data: Optional[Dict[str, Any]],
        now: Optional[float | datetime] = None,
    ) -> bool:
        """Restore daily loss counters if saved state matches the current trading day."""
        today = self.day_for(now)
        self._day_key = today
        if not isinstance(data, dict):
            return False
        if data.get("day_key") != today:
            self.daily_realized_pnl_usd = 0.0
            self.consecutive_losses = 0
            if self.breaker_action == "halt" and bool(data.get("loss_tripped", False)):
                self.loss_tripped = True
                self.loss_reason = str(data.get("loss_reason") or "prior halt requires manual clear")
            else:
                self.loss_tripped = False
                self.loss_reason = ""
            return False
        self.daily_realized_pnl_usd = float(data.get("daily_realized_pnl_usd") or 0.0)
        self.consecutive_losses = int(data.get("consecutive_losses") or 0)
        self.loss_tripped = bool(data.get("loss_tripped", False))
        self.loss_reason = str(data.get("loss_reason") or "")
        return True

    @property
    def should_halt(self) -> bool:
        return self.loss_tripped and self.breaker_action == "halt"

    @property
    def is_reduce_only(self) -> bool:
        return (
            self.fee_tripped
            or self.fee_unknown
            or self.reconcile_tripped
            or self.margin_tripped
            or bool(self.margin_unknown_venues)
            or (self.loss_tripped and self.breaker_action == "reduce_only")
        )

    @property
    def active_reasons(self) -> list[str]:
        out = []
        if self.loss_tripped and self.loss_reason:
            out.append(self.loss_reason)
        if (self.fee_tripped or self.fee_unknown) and self.fee_reason:
            out.append(self.fee_reason)
        if self.reconcile_tripped and self.reconcile_reason:
            out.append(self.reconcile_reason)
        if (self.margin_tripped or self.margin_unknown_venues) and self.margin_reason:
            out.append(self.margin_reason)
        return out
