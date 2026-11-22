"""Strategy enhancements: dynamic rolling midline and funding carry skew.

Both components are opt-in via config.yaml so the engine stays 100% compatible
with the original static-midline strategy when disabled.
"""
from __future__ import annotations

import math
import statistics
import time
from collections import deque
from typing import Any, Deque, Dict, Optional, Tuple


class DynamicMidline:
    """Rolling dynamic midline estimator (EWMA or rolling median) with warmup
    fallback to static midline_bps, optional rolling-std band widening, and
    JSON state persistence across restarts (T8)."""

    def __init__(
        self,
        *,
        enabled: bool = False,
        static_midline_bps: float = 0.0,
        method: str = "ewma",
        window_minutes: float = 180.0,
        min_samples: int = 60,
        sample_interval_sec: float = 5.0,
        std_band_multiplier: float = 0.0,
    ) -> None:
        self.enabled = bool(enabled)
        self.static_midline_bps = float(static_midline_bps)
        self.method = method
        self.window_sec = max(float(window_minutes) * 60.0, 1.0)
        self.min_samples = max(int(min_samples), 1)
        self.sample_interval_sec = max(float(sample_interval_sec), 0.0)
        self.std_band_multiplier = max(float(std_band_multiplier), 0.0)

        self._samples: Deque[Tuple[float, float]] = deque()
        self._last_sample_ts: Optional[float] = None
        self._ewma: Optional[float] = None
        self._tau = max(self.window_sec / 3.0, 1.0)

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_sec
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

    def update(self, premium_bps: float, now: Optional[float] = None) -> bool:
        """Ingest one mid-premium observation (bps). Returns True if accepted."""
        if not self.enabled:
            return False
        ts = time.time() if now is None else float(now)
        if (
            self._last_sample_ts is not None
            and self.sample_interval_sec > 0.0
            and (ts - self._last_sample_ts) < self.sample_interval_sec
        ):
            return False

        val = float(premium_bps)
        if self._ewma is None:
            self._ewma = val
        else:
            dt = max(ts - (self._last_sample_ts or ts), 0.0)
            if dt > 0.0:
                alpha = 1.0 - math.exp(-dt / self._tau)
            else:
                alpha = 2.0 / (max(len(self._samples), 1) + 2.0)
            self._ewma = alpha * val + (1.0 - alpha) * self._ewma

        self._samples.append((ts, val))
        self._last_sample_ts = ts
        self._prune(ts)
        return True

    @property
    def sample_count(self) -> int:
        return len(self._samples)

    @property
    def is_warm(self) -> bool:
        return self.enabled and len(self._samples) >= self.min_samples

    def value(self, fallback: Optional[float] = None) -> float:
        """Current midline in bps (falls back to static midline during warmup)."""
        base = self.static_midline_bps if fallback is None else float(fallback)
        if not self.is_warm:
            return base
        if self.method == "rolling_median":
            return float(statistics.median(v for _, v in self._samples))
        return float(self._ewma) if self._ewma is not None else base

    def std_bps(self) -> float:
        """Rolling standard deviation of premiums in the current window."""
        if not self.is_warm or len(self._samples) < 2:
            return 0.0
        return float(statistics.pstdev(v for _, v in self._samples))

    def band_adjustment_bps(self) -> float:
        """Optional volatility-based band widening (k * rolling_std)."""
        if self.std_band_multiplier <= 0.0 or not self.is_warm:
            return 0.0
        return self.std_band_multiplier * self.std_bps()

    def to_state(self, now: Optional[float] = None) -> Dict[str, Any]:
        ts = time.time() if now is None else float(now)
        self._prune(ts)
        return {
            "samples": [[t, v] for t, v in self._samples],
            "ewma": self._ewma,
            "last_sample_ts": self._last_sample_ts,
        }

    def load_state(self, data: Optional[Dict[str, Any]],
                   now: Optional[float] = None) -> int:
        """Restore non-expired samples from persisted state. Returns restored count."""
        if not isinstance(data, dict):
            return 0
        ts = time.time() if now is None else float(now)
        cutoff = ts - self.window_sec
        raw_samples = data.get("samples") or []
        restored: list[Tuple[float, float]] = []
        for item in raw_samples:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                try:
                    st, sv = float(item[0]), float(item[1])
                except (TypeError, ValueError):
                    continue
                if cutoff <= st <= ts + 60.0:
                    restored.append((st, sv))
        restored.sort(key=lambda x: x[0])
        self._samples = deque(restored)
        if restored:
            self._last_sample_ts = restored[-1][0]
            ewma = data.get("ewma")
            self._ewma = float(ewma) if ewma is not None else restored[-1][1]
        else:
            self._last_sample_ts = None
            self._ewma = None
        return len(self._samples)


class FundingSkew:
    """Translates cross-venue current hourly funding rate spread into bps
    threshold skew (T1 hardened).

    Sign convention:
      rate > 0 means LONGS pay SHORTS (per hour, as a decimal, e.g. 0.0001 = 1 bps/h).
      If entropy_rate > hedge_rate:
        - Selling Entropy / Buying Hedge EARNS positive funding carry
          -> threshold is LOWERED (easier to enter with carry).
        - Buying Entropy / Selling Hedge PAYS negative funding carry
          -> threshold is RAISED (harder to enter against carry).

    Safety (T1):
      - If either venue's funding rate fetch fails (None), the entire pair's
        funding_skew_bps is forced to 0.0 to prevent artificial fake-spread skew.
      - min_net_edge_bps floors the post-skew net edge so carry never forces
        negative-edge entries.
    """

    def __init__(
        self,
        *,
        enabled: bool = False,
        expected_hold_hours: float = 4.0,
        max_skew_bps: float = 10.0,
        min_net_edge_bps: float = 0.0,
    ) -> None:
        self.enabled = bool(enabled)
        self.expected_hold_hours = max(float(expected_hold_hours), 0.0)
        self.max_skew_bps = max(float(max_skew_bps), 0.0)
        self.min_net_edge_bps = max(float(min_net_edge_bps), 0.0)
        self.entropy_rate_hourly: float = 0.0
        self.hedge_rate_hourly: float = 0.0
        self.valid: bool = False
        self.last_update_ts: float = 0.0

    def update_rates(
        self,
        entropy_rate_hourly: Optional[float],
        hedge_rate_hourly: Optional[float],
        now: Optional[float] = None,
    ) -> bool:
        """Update both venues' hourly funding rates.

        Returns True if both sides succeeded; False if either side failed
        (which resets skew to 0.0 to avoid fake-spread bias).
        """
        self.last_update_ts = time.time() if now is None else float(now)
        if entropy_rate_hourly is None or hedge_rate_hourly is None:
            self.valid = False
            self.entropy_rate_hourly = 0.0
            self.hedge_rate_hourly = 0.0
            return False
        self.entropy_rate_hourly = float(entropy_rate_hourly)
        self.hedge_rate_hourly = float(hedge_rate_hourly)
        self.valid = True
        return True

    def skew_bps(self) -> float:
        """Expected funding carry (bps) for SELL entropy / BUY hedge over
        expected_hold_hours. Returns 0.0 when disabled or degraded."""
        if not self.enabled or not self.valid or self.expected_hold_hours <= 0.0:
            return 0.0
        raw = (self.entropy_rate_hourly - self.hedge_rate_hourly) * 1e4 * self.expected_hold_hours
        if self.max_skew_bps > 0.0:
            raw = max(-self.max_skew_bps, min(self.max_skew_bps, raw))
        return raw

    def threshold_adjustment(self, sell_entropy: bool) -> float:
        """Bps adjustment to add to the entry threshold for a given direction.

        Negative adjustment lowers the entry hurdle (with-carry direction);
        positive adjustment raises the entry hurdle (against-carry direction).
        """
        s = self.skew_bps()
        return -s if sell_entropy else +s
