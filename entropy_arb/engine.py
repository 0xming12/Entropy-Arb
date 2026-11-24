"""Two-venue arbitrage engine: Entropy vs one hedge venue.

Hardened for live trading with:
- P0-1: Order terminal-state classification (rejected / filled / canceled / unknown),
  pending-order tracking that pauses new pairs, dedicated order-query + two-venue
  position reconciliation bypassing the 5s grace window, fill deduplication, and
  restart recovery before resuming entries.
- P0-2: Strict two-leg reduce_only sizing (min of both legs' reducible inventory,
  floored to size_step), pre-send re-verification, explicit reduce_only=True on
  both legs, and delegation of unmatched single-leg exposure to _hedge().
- P1-1: Single-leg _hedge() enters P0-1 reconciliation on unknown outcomes (zero
  duplicate hedge orders), recomputes remaining qty from confirmed fills, rounds
  prices strictly inside hedge_max_slippage_bps, and halts/alerts if net exposure
  still exceeds tolerance after retries.
- P1-2: Post-slippage & post-rounding worst-case net spread re-check using final
  rounded bounds, qty, and conservative effective fees, separating normal entries
  from active reduce-only exits.
- P1-3: Conservative effective fees (max of configured & observed) in _plan() and
  final validation; HL userFills fee verification when IOC receipts omit fee;
  estimated vs actual fee separation; fail-safe reduce-only pause when fee or
  available margin is unknown while enabled.
- P1-4: Trading-day-aligned daily MTM baseline with same-day persistence &
  cross-day rollover reset, plus explicit PnL decomposition (spread, actual vs
  estimated fees, funding, hedge PnL, external transfers).
"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import logging
import math
import os
import time
from collections import deque
from dataclasses import replace
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

try:
    import aiohttp
except ImportError:  # pragma: no cover - allows offline unit tests without aiohttp
    aiohttp = None  # type: ignore

from .book import ArbPlan, floor_step, plan_arb, walk_depth
from .config import Config
from .notifier import TelegramNotifier
from .recorder import MinuteRecorder
from .risk import CircuitBreaker, SessionFilter, SessionState
from .state import InstanceLock, StateStore, global_lock_dir
from .strategy import DynamicMidline, FundingSkew
from .venue_hl import HLVenue
from .venue_lighter import LighterVenue

log = logging.getLogger("engine")

CSV_HEADER = ["ts", "direction", "buy_venue", "sell_venue", "qty",
              "buy_limit", "sell_limit", "buy_notional", "sell_notional",
              "exp_edge_usd", "gross_edge_usd", "marginal_premium_bps",
              "midline_bps", "inv_add_bps", "ok", "buy_fill", "sell_fill",
              "buy_status", "sell_status", "fill_edge_usd"]
BALANCE_POLL_SEC = 30.0
MAX_SIGNAL_GAP_SEC = 1.5


class Engine:
    def __init__(self, cfg: Config, record_only: bool = False) -> None:
        self.cfg = cfg
        self.record_only = record_only
        self.session: Optional["aiohttp.ClientSession"] = None
        self.entropy = None
        self.hedge = None
        self.venues: Dict[str, object] = {}
        self.recorder: Optional[MinuteRecorder] = None
        self.markets_ready = False
        self.stop = asyncio.Event()
        self._update_evt = asyncio.Event()
        self._reconcile_evt = asyncio.Event()
        self._venue_locks: Dict[str, asyncio.Lock] = {}
        self._exec_tasks: set = set()
        self._bg_tasks: set = set()
        self.halted = False
        self.consec_errors = 0
        self.last_trade_ts = 0.0
        self.trades = 0
        self.hedges = 0
        self.total_exp_edge = 0.0
        self.total_fill_edge = 0.0
        self.start_ts = time.time()
        self._last_skiplog = 0.0
        self._poke_due: Optional[float] = None
        self._armed: Dict[str, Optional[float]] = {"sell_entropy": None,
                                                   "buy_entropy": None}
        self._last_eligible: Dict[str, float] = {}
        self._armed_entropy_mono: Dict[str, Optional[float]] = {}
        self._signal_reduce_only: Optional[bool] = None
        self._step = 1e-4
        self._min_base = 0.0
        self._min_notional = 10.0
        self._mtm_baseline: Optional[float] = None
        self._sends: Dict[str, deque] = {}
        self._venue_limited_until: Dict[str, float] = {}
        self._venue_down: Dict[str, float] = {}
        self._venue_probe_at: Dict[str, float] = {}
        self._venue_fetch_fails: Dict[str, int] = {}
        self.recent_trades: deque = deque(maxlen=50)

        # P0-1: pending order tracking, deduplication, and mandatory reconcile gate
        self._pending_orders: Dict[str, Dict[str, Any]] = {}
        self._pending_not_found_counts: Dict[str, int] = {}
        self._settled_order_order: deque = deque(maxlen=512)
        self._settled_order_ids: set = set()
        self._reconcile_required: bool = False
        self._orders_in_flight = 0
        self._state_can_write = True
        self._state_fault = False
        self._hedge_retry_exhausted = False
        self._hedge_attempt_count = 0
        self._hedge_running = False
        self._unhedgeable_until = 0.0
        self._unhedgeable_snapshot: Optional[Tuple[float, ...]] = None
        self._restored_ledger = False

        # P1-3: per-venue observed live taker fees (bps)
        self._observed_fee_bps: Dict[str, Optional[float]] = {}

        # P0: Telegram notifier, circuit breaker, US market session filter
        self.notifier = TelegramNotifier(
            cfg.telegram.bot_token,
            cfg.telegram.chat_id,
            throttle_sec=cfg.notify_throttle_sec,
            prefix=f"[entropy-arb {cfg.symbol}/{cfg.hedge_venue}]",
        )
        self.circuit_breaker = CircuitBreaker(
            max_daily_loss_usd=cfg.risk.max_daily_loss_usd,
            max_consecutive_losses=cfg.risk.max_consecutive_losses,
            breaker_action=cfg.risk.breaker_action,
            max_taker_fee_bps=cfg.risk.max_taker_fee_bps,
            min_available_margin_usd=cfg.risk.min_available_margin_usd,
            tz_name=cfg.session_filter.timezone,
        )
        self.session_filter = SessionFilter(
            enabled=cfg.session_filter.enabled,
            tz_name=cfg.session_filter.timezone,
            regular_start=cfg.session_filter.regular_start,
            regular_end=cfg.session_filter.regular_end,
            early_close_end=cfg.session_filter.early_close_end,
            off_hours_mode=cfg.session_filter.off_hours_mode,
            off_hours_cap_multiplier=cfg.session_filter.off_hours_cap_multiplier,
            off_hours_spread_buffer_bps=cfg.session_filter.off_hours_spread_buffer_bps,
            weekend_reduce_only=cfg.session_filter.weekend_reduce_only,
            holidays=cfg.session_filter.holidays,
            early_close_dates=cfg.session_filter.early_close_dates,
        )
        # P1: dynamic midline & funding carry skew
        self.dynamic_midline = DynamicMidline(
            enabled=cfg.dynamic_midline.enabled,
            static_midline_bps=cfg.midline_bps,
            method=cfg.dynamic_midline.method,
            window_minutes=cfg.dynamic_midline.window_minutes,
            min_samples=cfg.dynamic_midline.min_samples,
            sample_interval_sec=cfg.dynamic_midline.sample_interval_sec,
            std_band_multiplier=cfg.dynamic_midline.std_band_multiplier,
        )
        self.funding_skew = FundingSkew(
            enabled=cfg.funding_skew.enabled,
            expected_hold_hours=cfg.funding_skew.expected_hold_hours,
            max_skew_bps=cfg.funding_skew.max_skew_bps,
            min_net_edge_bps=cfg.funding_skew.min_net_edge_bps,
        )
        # P1-4: trading-day-aligned daily MTM baseline & PnL breakdown
        self._daily_mtm_day: Optional[str] = None
        self._daily_mtm_baseline: Optional[float] = None
        self.daily_pnl_breakdown: Dict[str, Any] = self._fresh_daily_breakdown(None)
        # Inventory cost basis for exact round-trip PnL attribution under dynamic midline
        self._inv_signed_qty: float = 0.0
        self._inv_avg_cash: float = 0.0
        self._inv_avg_booked: float = 0.0

        # P1 (T8): persistence & instance lock
        safe_pair = f"{cfg.symbol}_{cfg.hedge_venue}".replace("/", "_")
        state_path = os.path.join(cfg.persistence.state_dir, f"{safe_pair}_state.json")
        lock_path = os.path.join(cfg.persistence.state_dir, f"{safe_pair}.lock")
        self._state_path = state_path
        self._lock_path = lock_path
        self.state_store: Optional[StateStore] = (
            StateStore(state_path) if cfg.persistence.enabled else None
        )
        self.instance_lock: Optional[InstanceLock] = (
            InstanceLock(lock_path, label=safe_pair)
            if (cfg.persistence.lock_enabled and not record_only)
            else None
        )
        self._venue_instance_locks: List[InstanceLock] = []

    @staticmethod
    def _fresh_daily_breakdown(day_key: Optional[str]) -> Dict[str, Any]:
        return {
            "day_key": day_key,
            "spread_pnl_usd": 0.0,
            "actual_fee_usd": 0.0,
            "estimated_fee_usd": 0.0,
            "funding_pnl_usd": None,      # None = unknown until confirmed ledger update
            "funding_status": "unknown",
            "hedge_pnl_usd": 0.0,
            "transfers_usd": 0.0,
            "transfer_status": "unknown",
        }

    # ------------------------------------------------------------- utilities

    def _vlock(self, key: str) -> asyncio.Lock:
        lock = self._venue_locks.get(key)
        if lock is None:
            lock = self._venue_locks[key] = asyncio.Lock()
        return lock

    def _venue_rate_ok(self, v) -> bool:
        dq = self._sends.setdefault(v.key, deque())
        now = time.time()
        while dq and now - dq[0] > 60.0:
            dq.popleft()
        return len(dq) < v.orders_per_min

    def _venue_limited(self, v) -> bool:
        return time.time() < self._venue_limited_until.get(v.key, 0.0)

    def _mark_limited(self, v) -> None:
        self._venue_limited_until[v.key] = time.time() + self.cfg.rate_limit_pause_sec
        log.warning("[%s] rate limited — trading paused for %.0fs",
                    v.name, self.cfg.rate_limit_pause_sec)

    def _record_send(self, v) -> None:
        self._sends.setdefault(v.key, deque()).append(time.time())

    def _on_ws_disconnect(self, venue_name: str, detail: str) -> None:
        self.notifier.notify(
            f"ws_disconnect:{venue_name}",
            f"⚠️ WebSocket disconnected [{venue_name}]: {detail}",
        )

    def has_pending_reconcile(self) -> bool:
        """P0-1: True while any order outcome is unknown or post-restart account check is pending."""
        return bool(self._pending_orders) or self._reconcile_required

    def _mark_order_settled(self, order_key: str) -> bool:
        """Record an order key as settled. Returns False if already settled (dedup)."""
        if order_key in self._settled_order_ids:
            return False
        if len(self._settled_order_order) == self._settled_order_order.maxlen:
            oldest = self._settled_order_order.popleft()
            self._settled_order_ids.discard(oldest)
        self._settled_order_order.append(order_key)
        self._settled_order_ids.add(order_key)
        return True

    def _register_pending_order(
        self,
        venue,
        client_order_id: str,
        *,
        is_buy: bool,
        qty: float,
        limit_px: float,
        known_filled: float,
        purpose: str,
        reduce_only: bool,
        persist: bool = True,
        pair_id: Optional[str] = None,
        pair_ctx: Optional[Dict[str, Any]] = None,
    ) -> str:
        cid = str(client_order_id or f"unknown-{int(time.time() * 1000)}")
        okey = f"{venue.key}:{cid}"
        rec: Dict[str, Any] = {
            "venue_key": venue.key,
            "venue_name": venue.name,
            "client_order_id": cid,
            "is_buy": bool(is_buy),
            "qty": float(qty),
            "limit_px": float(limit_px),
            "known_filled": float(known_filled),
            "purpose": purpose,
            "reduce_only": bool(reduce_only),
            "submission": "attempted",
            "created_ts": time.time(),
        }
        if pair_id is not None:
            rec["pair_id"] = pair_id
        if pair_ctx is not None:
            rec["pair_ctx"] = pair_ctx
        self._pending_orders[okey] = rec
        self._reconcile_required = True
        self._armed["sell_entropy"] = None
        self._armed["buy_entropy"] = None
        self._reconcile_evt.set()
        if persist and self.state_store is not None and not self.save_state():
            self._stop_on_state_error("could not checkpoint pending order")
        log.warning(
            "[PENDING] registered unresolved %s order %s (%s %.6g @%.6g, known_fill=%.6g) "
            "— pausing new pairs until reconciled",
            purpose, okey, "BUY" if is_buy else "SELL", qty, limit_px, known_filled,
        )
        self.notifier.notify(
            f"pending_order:{okey}",
            f"⚠️ Unresolved {purpose} order {okey} — new pairs PAUSED until order & positions verified",
            force=True,
        )
        return okey

    def _stage_orders(self, orders, *, qty: float, purpose: str,
                      reduce_only: bool,
                      submission: str = "prepared") -> Optional[Dict[str, str]]:
        """Record all client IDs in one durable write before submitting any leg.

        Each item in `orders` is `(venue, is_buy, limit_px)` (using shared `qty`)
        or `(venue, is_buy, limit_px, leg_qty)` for asymmetric rebalance legs.
        """
        ids = {}
        staged = {}
        for item in orders:
            if len(item) == 4:
                venue, is_buy, limit_px, leg_qty = item
            else:
                venue, is_buy, limit_px = item
                leg_qty = qty
            if not hasattr(venue, "reserve_order_id"):
                continue
            cid = venue.reserve_order_id()
            ids[venue.key] = cid
            staged[f"{venue.key}:{cid}"] = {
                "venue_key": venue.key,
                "client_order_id": cid,
                "is_buy": is_buy,
                "qty": float(leg_qty),
                "limit_px": limit_px,
                "known_filled": 0.0,
                "purpose": purpose,
                "reduce_only": reduce_only,
                "submission": submission,
                "created_ts": time.time(),
            }
        if purpose == "arb" and len(staged) == 2:
            stage_pair_id = ":".join(sorted(staged.keys()))
            for rec in staged.values():
                rec["pair_id"] = stage_pair_id
        self._pending_orders.update(staged)
        if ids and not self.save_state():
            # No order has been sent. If the write partially succeeded, keep
            # the engine halted and allow the next successful save to clear it.
            for key, cid in ids.items():
                self._pending_orders.pop(f"{key}:{cid}", None)
            self._stop_on_state_error("could not persist orders before submission")
            return None
        return ids

    def _stage_followup_order(
        self, ids: Dict[str, str], venue, *, is_buy: bool, qty: float,
        limit_px: float, purpose: str, reduce_only: bool,
    ) -> bool:
        """Persist a sequential follow-up leg alongside the already-staged primary leg."""
        if not hasattr(venue, "reserve_order_id"):
            return True
        cid = venue.reserve_order_id()
        okey = f"{venue.key}:{cid}"
        ids[venue.key] = cid
        self._pending_orders[okey] = {
            "venue_key": venue.key,
            "client_order_id": cid,
            "is_buy": is_buy,
            "qty": float(qty),
            "limit_px": limit_px,
            "known_filled": 0.0,
            "purpose": purpose,
            "reduce_only": reduce_only,
            "submission": "attempted",
            "created_ts": time.time(),
        }
        if purpose == "arb" and len(ids) == 2:
            stage_pair_id = ":".join(sorted(f"{k}:{v}" for k, v in ids.items()))
            for k, v in ids.items():
                rec = self._pending_orders.get(f"{k}:{v}")
                if rec is not None:
                    rec["pair_id"] = stage_pair_id
        if not self.save_state():
            self._pending_orders.pop(okey, None)
            ids.pop(venue.key, None)
            self._stop_on_state_error("could not persist follow-up order before submission")
            return False
        return True

    def _stop_on_state_error(self, reason: str) -> None:
        self._state_fault = True
        self.halted = True
        log.critical("HALTED: %s", reason)
        self.notifier.notify("order_state_error", f"HALTED: {reason}", force=True)

    async def _send_staged_order(self, venue, ids, **kwargs) -> dict:
        cid = ids.get(venue.key)
        if cid is not None:
            order = self._pending_orders[f"{venue.key}:{cid}"]
            if not self._state_fault and order.get("submission") != "attempted":
                # A prepared order has never reached the venue adapter. A
                # successfully persisted attempted marker must precede the call.
                order["submission"] = "attempted"
                if not self.save_state():
                    order["submission"] = "prepared"
                    self._stop_on_state_error("could not checkpoint submission attempt")
            if self._state_fault:
                return {"status": "not-submitted", "outcome": "rejected",
                        "filled_base": 0.0, "avg_px": None, "unresolved": False,
                        "client_order_id": cid, "err": "order journal unavailable"}
        try:
            if cid is not None:
                kwargs["client_order_id"] = cid
            result = await venue.send_taker(**kwargs)
            if not isinstance(result, dict):
                raise TypeError("invalid order response")
            filled = float(result["filled_base"])
            price = result.get("avg_px")
            if (not math.isfinite(filled) or not 0.0 <= filled <= kwargs["qty"] + 1e-9
                    or not isinstance(result.get("unresolved"), bool)
                    or not isinstance(result.get("status"), str)
                    or (price is not None and (not math.isfinite(float(price)) or float(price) <= 0))):
                raise ValueError("invalid order outcome")
            result["filled_base"] = filled
            if price is not None:
                result["avg_px"] = float(price)
            if cid is not None:
                result["client_order_id"] = cid
            return result
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return {"status": "send-unknown", "outcome": "unknown",
                    "filled_base": 0.0, "avg_px": None, "unresolved": True,
                    "client_order_id": cid, "err": type(exc).__name__}

    def _commit_orders(self, ids, results) -> bool:
        for venue, info in results:
            cid = ids.get(venue.key)
            if cid is not None and not info.get("unresolved"):
                self._pending_orders.pop(f"{venue.key}:{cid}", None)
        if self.state_store is not None and not self.save_state():
            self._stop_on_state_error("could not checkpoint order outcomes")
            return False
        return True

    def _state_accounts(self) -> Dict[str, str]:
        return {
            key: venue.state_identity() for key, venue in self.venues.items()
            if hasattr(venue, "state_identity")
        }

    def save_state(self, now: Optional[float] = None) -> bool:
        if self.state_store is None or self.record_only or not self._state_can_write:
            return False
        # Only persist diagnostic order metadata + risk/strategy state — never secrets.
        payload = {
            "saved_at": time.time() if now is None else float(now),
            "symbol": self.cfg.symbol,
            "hedge_venue": self.cfg.hedge_venue,
            "accounts": self._state_accounts(),
            "circuit_breaker": self.circuit_breaker.to_state(now=now),
            "dynamic_midline": self.dynamic_midline.to_state(now=now),
            "daily_mtm_day": self._daily_mtm_day,
            "daily_mtm_baseline": self._daily_mtm_baseline,
            "daily_pnl_breakdown": dict(self.daily_pnl_breakdown),
            "venue_ledger": {
                key: {"cash": v.cash, "position": v.position}
                for key, v in self.venues.items()
            },
            "pending_orders": dict(self._pending_orders),
            "reconcile_required": bool(self._reconcile_required or self._pending_orders),
            "hedge_attempt_count": self._hedge_attempt_count,
            "inv_basis": {
                "signed_qty": self._inv_signed_qty,
                "avg_cash": self._inv_avg_cash,
                "avg_booked": self._inv_avg_booked,
            },
        }
        return self.state_store.save(payload)

    def load_state(self, now: Optional[float] = None) -> bool:
        if self.state_store is None or self.record_only:
            return False
        data = self.state_store.load(strict=True)
        if not data:
            return False
        if data.get("symbol") != self.cfg.symbol or data.get("hedge_venue") != self.cfg.hedge_venue:
            raise RuntimeError("saved order state belongs to another market pair")
        if data.get("accounts") and data["accounts"] != self._state_accounts():
            raise RuntimeError("saved order state belongs to another trading account")
        ledger = data.get("venue_ledger") or {}
        ledger_complete = bool(self.venues) and all(key in ledger for key in self.venues)
        if ledger_complete:
            for key, venue in self.venues.items():
                cash, position = float(ledger[key]["cash"]), float(ledger[key]["position"])
                if not (math.isfinite(cash) and math.isfinite(position)):
                    raise RuntimeError(f"[{venue.name}] saved order ledger contains non-finite values")
                venue.cash, venue.position = cash, position
        self._restored_ledger = ledger_complete
        today = self.circuit_breaker.day_for(now)
        cb_ok = self.circuit_breaker.load_state(data.get("circuit_breaker"), now=now)
        dm_n = self.dynamic_midline.load_state(data.get("dynamic_midline"), now=now)
        if self.circuit_breaker.should_halt:
            self.halted = True

        if (ledger_complete and data.get("daily_mtm_day") == today
                and data.get("daily_mtm_baseline") is not None):
            self._daily_mtm_day = today
            self._daily_mtm_baseline = float(data["daily_mtm_baseline"])
        self.daily_pnl_breakdown = self._fresh_daily_breakdown(today)
        saved_breakdown = data.get("daily_pnl_breakdown")
        if isinstance(saved_breakdown, dict) and saved_breakdown.get("day_key") == today:
            self.daily_pnl_breakdown.update(saved_breakdown)

        # P0-1: restore unresolved orders & require verification before opening new pairs
        saved_pending = data.get("pending_orders")
        if saved_pending is not None and not isinstance(saved_pending, dict):
            raise RuntimeError("saved pending-order journal is malformed")
        if saved_pending and not ledger_complete:
            raise RuntimeError("saved pending orders are missing their position ledger; "
                               "preserve the state file and reconcile the order IDs manually")
        if isinstance(saved_pending, dict):
            self._pending_orders = dict(saved_pending)
        self._reconcile_required = bool(
            data.get("reconcile_required", False) or self._pending_orders
        )
        self._hedge_attempt_count = max(0, int(data.get("hedge_attempt_count", 0)))
        saved_basis = data.get("inv_basis")
        if isinstance(saved_basis, dict):
            try:
                sq = float(saved_basis.get("signed_qty", 0.0))
                ac = float(saved_basis.get("avg_cash", 0.0))
                ab = float(saved_basis.get("avg_booked", 0.0))
                if math.isfinite(sq) and math.isfinite(ac) and math.isfinite(ab):
                    self._inv_signed_qty, self._inv_avg_cash, self._inv_avg_booked = sq, ac, ab
            except (TypeError, ValueError):
                pass
        if self.entropy is not None and self.hedge is not None and not self._pending_orders:
            self._sync_inv_basis_to_positions(self.entropy.position, self.hedge.position)
        if self._reconcile_required:
            log.warning(
                "restored %d unresolved pending order(s) — trading paused until reconciled",
                len(self._pending_orders),
            )
        log.info(
            "restored persisted state (cb_same_day=%s daily_pnl=$%.2f consec_losses=%d "
            "dm_samples=%d pending=%d reconcile_req=%s)",
            cb_ok, self.circuit_breaker.daily_realized_pnl_usd,
            self.circuit_breaker.consecutive_losses, dm_n,
            len(self._pending_orders), self._reconcile_required,
        )
        return True

    def request_stop(self) -> None:
        self.stop.set()
        self._update_evt.set()
        self._reconcile_evt.set()

    # ------------------------------------------------------------- lifecycle

    def _acquire_venue_locks(self) -> None:
        """Acquire exclusive file locks for every venue's market identity and signer.

        Locks live in a single fixed absolute directory (`~/.entropy-arb/locks`
        or `ENTROPY_ARB_LOCK_DIR`), independent of `persistence.state_dir` and
        CWD, so two live bots launched from different configs or directories
        cannot concurrently trade the same shared account+coin position OR share
        the same signing key's nonce stream across different symbols.
        """
        if self.record_only:
            return
        lock_specs: List[Tuple[str, str]] = []
        seen: set = set()
        for prefix, attr in (("venue", "state_identity"), ("signer", "signer_identity")):
            for v in self.venues.values():
                fn = getattr(v, attr, None)
                if fn is None:
                    continue
                ident = fn()
                if not ident:
                    continue
                key = (prefix, str(ident))
                if key not in seen:
                    seen.add(key)
                    lock_specs.append(key)
        lock_specs.sort()
        lock_dir = global_lock_dir()
        acquired: List[InstanceLock] = []
        try:
            for prefix, ident in lock_specs:
                digest = hashlib.sha256(ident.encode("utf-8")).hexdigest()[:16]
                path = os.path.join(lock_dir, f"{prefix}_{digest}.lock")
                lk = InstanceLock(path, label=ident)
                lk.acquire()
                acquired.append(lk)
        except BaseException:
            for lk in reversed(acquired):
                lk.release()
            raise
        self._venue_instance_locks.extend(acquired)

    def _release_venue_locks(self) -> None:
        for lk in reversed(self._venue_instance_locks):
            lk.release()
        self._venue_instance_locks.clear()

    async def run(self) -> None:
        if not self.record_only and self.cfg.replay_only:
            raise RuntimeError("cannot trade with a replay_only configuration")
        if aiohttp is None:
            raise RuntimeError(
                "aiohttp is required to run the engine — pip install -r requirements.txt")
        if not self.record_only and self.instance_lock is None:
            self.instance_lock = InstanceLock(
                self._lock_path, label=f"{self.cfg.symbol}_{self.cfg.hedge_venue}")
        if self.instance_lock is not None:
            self.instance_lock.acquire()
        self._state_can_write = False
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(
            keepalive_timeout=75.0, ttl_dns_cache=300))
        self.notifier.attach_session(self.session)
        try:
            await self._run_inner()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            await self.notifier.send(
                "crash",
                f"🚨 Engine crashed: {type(e).__name__}: {e}",
                force=True,
            )
            raise
        finally:
            self.save_state()
            await self.notifier.flush(timeout=5.0)
            self.notifier.attach_session(None)
            await self.session.close()
            self._release_venue_locks()
            if self.instance_lock is not None:
                self.instance_lock.release()

    def _make_venue(self, vc):
        if vc.kind == "lighter":
            return LighterVenue(vc, self.session, self.cfg.settle_timeout_sec)
        return HLVenue(vc, self.cfg.hl_api_url, self.cfg.hl_ws_url,
                       self.session, self.cfg.settle_timeout_sec)

    async def _run_inner(self) -> None:
        cfg = self.cfg
        self.entropy = self._make_venue(cfg.entropy)
        self.hedge = self._make_venue(cfg.hedge)
        self.venues = {"entropy": self.entropy, "hedge": self.hedge}
        await asyncio.gather(self.entropy.load_market(), self.hedge.load_market())
        self.markets_ready = True

        live = not self.record_only
        if live:
            self._prime_startup_risk()
            if not cfg.creds_complete:
                raise RuntimeError(
                    "live trading needs credentials (including HL_ACCOUNT_ADDRESS) for both "
                    "venues in .env (see .env.example); use --record-only to run without "
                    "them / 实盘需要在 .env 中完整配置两个交易所密钥及 HL_ACCOUNT_ADDRESS，"
                    "仅采集数据请用 --record-only")
            self.entropy.init_signer()
            self.hedge.init_signer()
            verify_tasks = [
                v.verify_account() for v in self.venues.values()
                if hasattr(v, "verify_account")
            ]
            if verify_tasks:
                await asyncio.gather(*verify_tasks)
            self._acquire_venue_locks()
            if self.hedge.kind == "hl":
                self.entropy.share_nonces_with(self.hedge)
            # Even configurations that predate state persistence need an order journal.
            if self.state_store is None:
                self.state_store = StateStore(self._state_path)
            self.load_state()
        if (self.hedge.kind == "hl"
                and self.entropy._query_address()
                and self.entropy._query_address() == self.hedge._query_address()):
            self.hedge.include_core_equity = False

        self._step = 10 ** -min(self.entropy.size_decimals,
                                self.hedge.size_decimals)
        self._min_base = max(self.entropy.min_base, self.hedge.min_base,
                             self._step)
        self._min_notional = max(cfg.min_order_notional,
                                 self.entropy.min_quote, self.hedge.min_quote)
        log.info("pair ENTROPY(%s)-%s(%s): midline=%+.2fbps band=[-%.2f, +%.2f] "
                 "lat_buf=%.2fbps fees=%.2f+%.2f mode=%s step=%g min_ntl=$%g",
                 self.entropy.conf.symbol, self.hedge.name,
                 self.hedge.conf.symbol, cfg.midline_bps, cfg.lower_bps,
                 cfg.upper_bps, cfg.latency_buffer_bps,
                 self.entropy.fee_bps, self.hedge.fee_bps,
                 cfg.execution_mode, self._step, self._min_notional)

        if self.record_only:
            log.warning("RECORD-ONLY — collecting minute data, no strategy, "
                        "no orders")
        else:
            log.warning("LIVE — real orders will be sent (use --record-only "
                        "for credential-less data collection)")
            if self.has_pending_reconcile():
                # P0-1: resolve restored pending orders & verify both accounts before trading
                self._state_can_write = True
                await self._reconcile_pending_orders()
            else:
                await self._reconcile_positions(hedge=False, strict=True)
                self._state_can_write = True
            log.info("starting positions: %s (net %+.6g)",
                     " ".join(f"{v.name}={v.position:+.6g}"
                              for v in self.venues.values()),
                     sum(v.position for v in self.venues.values()))
            # Populate initial fee and margin observations BEFORE starting strategy_loop
            # so live trading does not open in a spurious unknown-fee / unknown-margin window.
            init_checks = []
            if cfg.risk.max_taker_fee_bps > 0.0:
                init_checks.append(self._check_all_fees_once())
            if cfg.risk.min_available_margin_usd > 0.0:
                init_checks.append(self._check_margin_once())
            if init_checks:
                await asyncio.gather(*init_checks, return_exceptions=True)

        mode_label = "RECORD-ONLY" if self.record_only else "LIVE"
        self.notifier.notify(
            "startup",
            f"🚀 Started ({mode_label}) | pair ENTROPY({cfg.symbol})-{self.hedge.name} | "
            f"band [-{cfg.lower_bps:.2f}, +{cfg.upper_bps:.2f}] bps, lat_buf={cfg.latency_buffer_bps:.2f} bps",
            force=True,
        )

        tasks: List[asyncio.Task] = []
        for v in self.venues.values():
            tasks += v.start_tasks(
                self.stop, self._update_evt.set, live,
                on_disconnect=self._on_ws_disconnect,
            )
        tasks.append(asyncio.create_task(self._book_watchdog(), name="book-watchdog"))
        if cfg.recorder_enabled or self.record_only:
            self.recorder = MinuteRecorder(cfg.recorder_csv, self.entropy.book,
                                           self.hedge.book, cfg.staleness_sec,
                                           max_book_age_sec=cfg.risk.max_book_age_sec)
            tasks.append(asyncio.create_task(self.recorder.run(self.stop),
                                             name="recorder"))
        if not self.record_only:
            tasks.append(asyncio.create_task(self._strategy_loop(),
                                             name="strategy"))
            tasks.append(asyncio.create_task(self._balance_loop(),
                                             name="balances"))
            tasks.append(asyncio.create_task(self._http_keepalive_loop(),
                                             name="keepalive"))
            if cfg.risk.max_taker_fee_bps > 0.0:
                tasks.append(asyncio.create_task(self._fee_monitor_loop(),
                                                 name="fee-monitor"))
            if cfg.funding_skew.enabled:
                tasks.append(asyncio.create_task(self._funding_loop(),
                                                 name="funding-poll"))
            if cfg.risk.max_daily_loss_usd > 0:
                tasks.append(asyncio.create_task(self._funding_history_loop(),
                                                 name="funding-history"))
                tasks.append(asyncio.create_task(self._transfer_history_loop(),
                                                 name="transfer-history"))
        tasks.append(asyncio.create_task(self._status_loop(), name="status"))
        if live:
            tasks.append(asyncio.create_task(self._reconcile_loop(),
                                             name="reconcile"))

        await self.stop.wait()
        if self._exec_tasks:
            log.info("waiting for %d in-flight execution(s) to settle",
                     len(self._exec_tasks))
            await asyncio.wait(self._exec_tasks,
                               timeout=cfg.settle_timeout_sec + 5.0)
        if self._bg_tasks:
            await asyncio.wait(list(self._bg_tasks), timeout=3.0)
            for bt in list(self._bg_tasks):
                if not bt.done():
                    bt.cancel()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for v in self.venues.values():
            await v.close()
        self.save_state()
        pnl = self.session_pnl()
        pnl_s = f"${pnl:+.4f}" if pnl is not None else "n/a"
        log.info("shutdown — %d trades, %d hedges, exp edge $%.4f, "
                 "fill edge $%.4f", self.trades, self.hedges,
                 self.total_exp_edge, self.total_fill_edge)
        await self.notifier.send(
            "shutdown",
            f"🛑 Stopped ({mode_label}) | trades={self.trades}, hedges={self.hedges}, "
            f"MTM={pnl_s}, fillEdge=${self.total_fill_edge:+.4f}",
            force=True,
        )

    # --------------------------------------------------------------- signals

    def _fee_monitored_venues(self) -> list:
        out = []
        if self.entropy is not None and hasattr(self.entropy, "fetch_taker_fee_bps"):
            out.append(self.entropy)
        if (self.hedge is not None and getattr(self.hedge, "kind", None) == "hl"
                and hasattr(self.hedge, "fetch_taker_fee_bps")):
            out.append(self.hedge)
        return out

    def _prime_startup_risk(self) -> None:
        fee_names = [v.name for v in self._fee_monitored_venues()] or ["ENTROPY"]
        self.circuit_breaker.require_initial_observations(
            (v.name for v in self.venues.values()),
            fee_venue_names=fee_names,
        )

    def effective_fee_bps(self, v) -> float:
        """P1-3: Conservative effective taker fee (bps) = max(configured, observed)."""
        obs = self._observed_fee_bps.get(v.key)
        if obs is None:
            return float(v.fee_bps)
        return max(float(v.fee_bps), float(obs))

    def current_midline_bps(self) -> float:
        return self.dynamic_midline.value(fallback=self.cfg.midline_bps)

    def session_state(self, now: Optional[float] = None) -> SessionState:
        return self.session_filter.evaluate(now)

    def is_reduce_only(self, now: Optional[float] = None) -> bool:
        return (self.circuit_breaker.is_reduce_only or self.session_state(now).reduce_only
                or abs(self._net_position()) > self.cfg.net_tolerance_base)

    def _net_position(self) -> float:
        return sum(v.position for v in self.venues.values())

    def _two_leg_reducible_qty(self, buy, sell) -> float:
        """P0-2: Maximum base qty that strictly reduces BOTH legs simultaneously,
        floored to exchange size_step."""
        buy_reducible = max(-float(buy.position), 0.0)
        sell_reducible = max(float(sell.position), 0.0)
        raw = min(buy_reducible, sell_reducible)
        if raw <= 0.0:
            return 0.0
        return floor_step(raw, self._step)

    def _is_reducing_direction(self, buy, sell) -> bool:
        """P0-2: True only when BOTH legs have verified reducible inventory >= min_base."""
        return self._two_leg_reducible_qty(buy, sell) >= max(self._min_base, 1e-12)

    def _inv_add_bps(self, buy, sell) -> float:
        scale = self.cfg.inventory_scale_bps
        if scale <= 0:
            return 0.0
        floor = min(max(self.cfg.inventory_floor_frac, 0.0), 0.99)

        def ramp(v, adding: bool) -> float:
            if not adding:
                return 0.0
            ref = v.book.mid()
            if ref is None:
                return 0.0
            u = min(abs(v.position) * ref / v.cap_usd, 1.0)
            if u <= floor:
                return 0.0
            return scale * (u - floor) / (1.0 - floor)

        return max(ramp(buy, buy.position >= 0), ramp(sell, sell.position <= 0))

    def _eff_threshold(self, buy, sell, now: Optional[float] = None,
                       *, include_latency_buffer: bool = True) -> float:
        mid = self.current_midline_bps()
        dyn_band = self.dynamic_midline.band_adjustment_bps()
        lat_buf = self.cfg.latency_buffer_bps if include_latency_buffer else 0.0
        sess_buf = self.session_state(now).spread_buffer_bps
        sell_entropy = (sell.key == "entropy")
        fund_adj = self.funding_skew.threshold_adjustment(sell_entropy=sell_entropy)

        in_reduce_only = self.is_reduce_only(now)
        is_reducing = self._is_reducing_direction(buy, sell)
        if in_reduce_only and is_reducing:
            if self.cfg.risk.reduce_only_ignore_session_buffer:
                sess_buf = 0.0
            if fund_adj > 0.0:
                fund_adj = 0.0

        raw_band = (self.cfg.upper_bps if sell_entropy else self.cfg.lower_bps)
        edge = raw_band + lat_buf + dyn_band + sess_buf + fund_adj
        if self.funding_skew.enabled:
            edge = max(edge, self.funding_skew.min_net_edge_bps)

        base = (mid + edge) if sell_entropy else (edge - mid)
        return base + self._inv_add_bps(buy, sell)

    def _position_qty_headroom(self, buy, sell, cap_multiplier: float) -> float:
        buy_mark, sell_mark = buy.book.mid(), sell.book.mid()
        if any(price is None or not math.isfinite(price) or price <= 0
               for price in (buy_mark, sell_mark)):
            return 0.0
        return max(0.0, min(buy.cap_usd * cap_multiplier / buy_mark - buy.position,
                            sell.cap_usd * cap_multiplier / sell_mark + sell.position))

    def _plan_threshold_bps(self, buy, sell, *, reduce_only: bool,
                            now: Optional[float] = None) -> float:
        """Signal trigger threshold for plan_arb().

        In reduce_only mode, defaults to _eff_threshold() (which already exempts
        session_buffer & adverse funding_skew) so entering reduce_only (off-hours,
        transient fee/margin query failure, startup, or breaker) does NOT
        panic-liquidate a delta-neutral pair at -50 bps.
        When reduce_only_exit_threshold_bps is set, it is applied relative to
        the current midline_bps so non-zero midlines remain symmetric.
        """
        if not reduce_only or self.cfg.reduce_only_exit_threshold_bps is None:
            return self._eff_threshold(buy, sell, now=now, include_latency_buffer=True)
        mid = self.current_midline_bps()
        sell_entropy = (sell.key == "entropy")
        edge = self.cfg.reduce_only_exit_threshold_bps
        if not self.cfg.risk.reduce_only_ignore_session_buffer:
            edge += self.session_state(now).spread_buffer_bps
        return (mid + edge) if sell_entropy else (edge - mid)

    def _required_spread_bps(self, buy, sell, *, reduce_only: bool,
                             now: Optional[float] = None) -> float:
        """Post-slippage worst-case floor for _order_price_bounds & _worst_case_check.

        Excludes latency_buffer_bps on normal entries and default mean-reversion
        exits so the latency buffer provides actual slippage room without
        widening limit slippage to reduce_only_min_worst_bps unless active
        bounded-loss reduce_only exits are explicitly enabled.
        """
        if not reduce_only or self.cfg.reduce_only_exit_threshold_bps is None:
            return self._eff_threshold(buy, sell, now=now, include_latency_buffer=False)
        mid = self.current_midline_bps()
        sell_entropy = (sell.key == "entropy")
        edge = self.cfg.reduce_only_min_worst_bps
        if not self.cfg.risk.reduce_only_ignore_session_buffer:
            edge += self.session_state(now).spread_buffer_bps
        return (mid + edge) if sell_entropy else (edge - mid)

    def _plan(self, buy, sell, cap_notional: float,
              now: Optional[float] = None, *, reduce_only: bool = False,
              cap_base: float = math.inf):
        return plan_arb(
            buy.book, sell.book,
            threshold_bps=self._plan_threshold_bps(buy, sell, reduce_only=reduce_only, now=now),
            buy_fee_bps=self.effective_fee_bps(buy),
            sell_fee_bps=self.effective_fee_bps(sell),
            take_fraction=self.cfg.take_fraction,
            cap_notional=cap_notional,
            min_base=self._min_base,
            min_notional=self._min_notional,
            size_step=self._step,
            cap_base=cap_base,
        )

    def _worst_case_check(
        self,
        buy,
        sell,
        qty: float,
        buy_bound: float,
        sell_bound: float,
        *,
        reduce_only: bool = False,
        now: Optional[float] = None,
    ) -> Tuple[bool, float, float, float]:
        """P1-2: Recompute worst-case net spread using final rounded bounds,
        qty, and conservative effective fees.

        Returns (ok, worst_net_bps, worst_net_usd, required_bps).
        """
        buy_fee = self.effective_fee_bps(buy) / 1e4
        sell_fee = self.effective_fee_bps(sell) / 1e4
        eff_buy = buy_bound * (1.0 + buy_fee)
        eff_sell = sell_bound * (1.0 - sell_fee)
        if eff_buy <= 0.0:
            return False, -1e9, -1e9, 0.0
        worst_net_usd = qty * (eff_sell - eff_buy)
        worst_net_bps = (eff_sell / eff_buy - 1.0) * 1e4
        req_bps = self._required_spread_bps(buy, sell, reduce_only=reduce_only, now=now)
        ok = worst_net_bps >= req_bps - 1e-9
        return ok, worst_net_bps, worst_net_usd, req_bps

    def _order_price_bounds(
        self, buy, sell, plan: ArbPlan, qty: float, *,
        reduce_only: bool = False, now: Optional[float] = None,
    ) -> Optional[Tuple[float, float]]:
        """Round limits inward while spending only the spread above the required bps.

        When execution.hedge_leg_slippage_bps is configured, caps the hedge leg's
        slippage budget at that tighter limit and allocates the remaining spread
        headroom (up to leg_slippage_bps) to the slower Entropy leg so coarse
        tick rounding and Hyperliquid latency do not starve Entropy fills.
        """
        slip = self.cfg.leg_slippage_bps / 1e4
        if not 0.0 <= slip < 1.0:
            return None
        hedge_slip_cfg = self.cfg.hedge_leg_slippage_bps
        asymmetric = (
            hedge_slip_cfg is not None
            and buy.key != sell.key
            and "hedge" in (buy.key, sell.key)
        )
        if asymmetric:
            hedge_cap = min(slip, hedge_slip_cfg / 1e4)
            buy_is_hedge = (buy.key == "hedge")
            if self.cfg.check_worst_case_spread:
                buy_fee = self.effective_fee_bps(buy) / 1e4
                sell_fee = self.effective_fee_bps(sell) / 1e4
                net_buy = plan.buy_limit * (1.0 + buy_fee)
                net_sell = plan.sell_limit * (1.0 - sell_fee)
                required = 1.0 + self._required_spread_bps(
                    buy, sell, reduce_only=reduce_only, now=now) / 1e4
                if not (math.isfinite(net_buy) and math.isfinite(net_sell)
                        and math.isfinite(required) and min(net_buy, net_sell, required) > 0.0):
                    return None
                required_value = required * net_buy
                if buy_is_hedge:
                    max_feas_buy = (net_sell / required_value) - 1.0
                    buy_slip = min(hedge_cap, max(max_feas_buy, 0.0))
                    buy_bound = buy.px_round(plan.buy_limit * (1.0 + buy_slip), round_up=False)
                    if not (math.isfinite(buy_bound) and buy_bound > 0.0):
                        return None
                    eff_buy_actual = buy_bound * (1.0 + buy_fee)
                    sell_avail = 1.0 - (required * eff_buy_actual) / net_sell
                    sell_slip = min(slip, max(sell_avail, 0.0))
                    sell_bound = sell.px_round(plan.sell_limit * (1.0 - sell_slip), round_up=True)
                else:
                    max_feas_sell = 1.0 - required_value / net_sell
                    sell_slip = min(hedge_cap, max(max_feas_sell, 0.0))
                    sell_bound = sell.px_round(plan.sell_limit * (1.0 - sell_slip), round_up=True)
                    if not (math.isfinite(sell_bound) and sell_bound > 0.0):
                        return None
                    eff_sell_actual = sell_bound * (1.0 - sell_fee)
                    buy_avail = (eff_sell_actual / required_value) - 1.0
                    buy_slip = min(slip, max(buy_avail, 0.0))
                    buy_bound = buy.px_round(plan.buy_limit * (1.0 + buy_slip), round_up=False)
            else:
                buy_slip = hedge_cap if buy_is_hedge else slip
                sell_slip = slip if buy_is_hedge else hedge_cap
                buy_bound = buy.px_round(plan.buy_limit * (1.0 + buy_slip), round_up=False)
                sell_bound = sell.px_round(plan.sell_limit * (1.0 - sell_slip), round_up=True)
        else:
            if self.cfg.check_worst_case_spread:
                net_buy = plan.buy_limit * (1.0 + self.effective_fee_bps(buy) / 1e4)
                net_sell = plan.sell_limit * (1.0 - self.effective_fee_bps(sell) / 1e4)
                required = 1.0 + self._required_spread_bps(
                    buy, sell, reduce_only=reduce_only, now=now) / 1e4
                if not (math.isfinite(net_buy) and math.isfinite(net_sell)
                        and math.isfinite(required) and min(net_buy, net_sell, required) > 0.0):
                    return None
                # For slippage s on each leg, require
                # net_sell * (1-s) >= required * net_buy * (1+s).
                required_value = required * net_buy
                available = (net_sell - required_value) / (net_sell + required_value)
                slip = min(slip, max(available, 0.0))
            buy_bound = buy.px_round(plan.buy_limit * (1.0 + slip), round_up=False)
            sell_bound = sell.px_round(plan.sell_limit * (1.0 - slip), round_up=True)
        if not (math.isfinite(buy_bound) and math.isfinite(sell_bound)
                and buy_bound > 0.0 and sell_bound > 0.0):
            return None
        if buy_bound < plan.buy_limit - 1e-9 or sell_bound > plan.sell_limit + 1e-9:
            return None
        if self.cfg.check_worst_case_spread and not self._worst_case_check(
            buy, sell, qty, buy_bound, sell_bound, reduce_only=reduce_only, now=now
        )[0]:
            return None
        return buy_bound, sell_bound

    def _entropy_first_follow_slip(self) -> float:
        raw = (
            self.cfg.entropy_first_hedge_slippage_bps
            if self.cfg.entropy_first_hedge_slippage_bps is not None
            else self.cfg.hedge_slippage_bps
        )
        if self.cfg.hedge_max_slippage_bps > 0.0:
            raw = min(raw, self.cfg.hedge_max_slippage_bps)
        return max(raw, 0.0) / 1e4

    def _prepare_pair_order(
        self, buy, sell, plan: ArbPlan, *, reduce_only: bool, now: float,
        max_qty: float = math.inf,
    ) -> Optional[Tuple[ArbPlan, float, float]]:
        """Apply the same final size, position, and limit checks in live and replay."""
        cap = self.cfg.max_order_notional
        qty = min(plan.qty, max_qty)
        if reduce_only:
            qty = min(qty, self._two_leg_reducible_qty(buy, sell))
        else:
            qty = min(qty, self._position_qty_headroom(
                buy, sell, self.session_state(now).cap_multiplier))
        qty = floor_step(qty, self._step)
        if qty <= 0.0 or qty < self._min_base:
            return None

        bounds = self._order_price_bounds(buy, sell, plan, qty, reduce_only=reduce_only, now=now)
        sell_best = sell.book.best_bid()
        if bounds is None or sell_best is None or not math.isfinite(sell_best) or sell_best <= 0:
            return None
        buy_bound, sell_bound = bounds
        # A sell limit is a price floor. Use the current best bid as the
        # snapshot ceiling for the sell leg, allowing for price improvement.
        order_price = max(buy_bound, sell_best, sell_bound)
        if (self.cfg.execution_mode == "entropy_first"
                and buy.key != sell.key and sell.key == "entropy"):
            # Pre-reserve the follow-up hedge buy's protection price so normal
            # fills fit inside max_order_notional without trimming follow_qty.
            buy_best = buy.book.best_ask()
            if buy_best is None or not math.isfinite(buy_best) or buy_best <= 0:
                return None
            projected_follow = buy.px_round(
                buy_best * (1.0 + self._entropy_first_follow_slip()),
                round_up=False,
            )
            if not math.isfinite(projected_follow) or projected_follow <= 0:
                return None
            order_price = max(order_price, buy_best, projected_follow)
        qty = floor_step(min(qty, cap / order_price), self._step)
        if qty <= 0.0 or qty < self._min_base or qty * order_price > cap + 1e-9:
            return None

        asks, bids = buy.book.sorted_asks(), sell.book.sorted_bids()
        if not asks or sum(size for price, size in asks if price <= buy_bound) + 1e-12 < qty:
            return None
        if not bids or sum(size for price, size in bids if price >= sell_bound) + 1e-12 < qty:
            return None
        buy_price, buy_notional = walk_depth(asks, qty)
        sell_price, sell_notional = walk_depth(bids, qty)
        if (min(buy_notional, sell_notional, qty * sell_bound) < self._min_notional
                or max(buy_notional, sell_notional) > cap + 1e-9):
            return None
        return (replace(plan, qty=qty, buy_limit=buy_price, sell_limit=sell_price,
                        buy_notional=buy_notional, sell_notional=sell_notional,
                        marginal_premium_bps=(sell_price / buy_price - 1.0) * 1e4),
                buy_bound, sell_bound)

    def _handle_loss_breaker(self, newly_tripped: bool, reason: str) -> None:
        if not newly_tripped:
            return
        if self.circuit_breaker.should_halt:
            self.halted = True
            log.critical("CIRCUIT BREAKER HALTED: %s", reason)
            self.notifier.notify(
                "circuit_breaker:loss",
                f"🚨 Circuit Breaker HALTED: {reason}",
                force=True,
            )
        else:
            log.critical("CIRCUIT BREAKER REDUCE-ONLY: %s", reason)
            self.notifier.notify(
                "circuit_breaker:loss",
                f"⚠️ Circuit Breaker -> REDUCE-ONLY: {reason}",
                force=True,
            )
        self.save_state()

    def _handle_fee_observation(self, fee_bps: Optional[float],
                                venue_key: str = "entropy") -> None:
        if fee_bps is not None and not math.isfinite(float(fee_bps)):
            fee_bps = None
        if fee_bps is not None:
            self._observed_fee_bps[venue_key] = float(fee_bps)
        v = self.venues.get(venue_key)
        vname = v.name if v is not None else venue_key.upper()
        tripped, recovered, msg = self.circuit_breaker.update_taker_fee(
            fee_bps, venue_name=vname)
        if tripped:
            log.critical("FEE BREAKER -> REDUCE-ONLY: %s", msg)
            self.notifier.notify(
                "circuit_breaker:fee_spike",
                f"🚨 {msg} — new entries BLOCKED (reduce-only)",
                force=True,
            )
        elif recovered:
            log.warning("FEE BREAKER RECOVERED: %s", msg)
            self.notifier.notify(
                "circuit_breaker:fee_spike",
                f"✅ {msg} — entries resumed",
                force=True,
            )

    def book_age_limit(self, *, reduce_only: bool = False) -> float:
        if reduce_only:
            return self.cfg.risk.max_reduce_book_age_sec
        limit = self.cfg.risk.max_book_age_sec
        return self.cfg.staleness_sec if self.record_only and limit <= 0.0 else limit

    def _trade_book_fresh(self, venue, *, reduce_only: bool,
                          now_mono: Optional[float] = None) -> bool:
        if not venue.book.is_fresh(self.cfg.staleness_sec):
            return False
        limit = self.book_age_limit(reduce_only=reduce_only)
        if limit <= 0.0:
            return True
        age = venue.book.age_mono(now_mono)
        return age is not None and age <= limit

    def _entry_book_fresh(self, venue, *, reduce_only: bool,
                          now_mono: Optional[float] = None) -> bool:
        """Apply optional tighter quote-age cap strictly to new entries without
        triggering watchdog WebSocket disconnects or blocking reduce-only exits."""
        if not self._trade_book_fresh(venue, reduce_only=reduce_only, now_mono=now_mono):
            return False
        if not reduce_only and self.cfg.max_entry_book_age_sec > 0.0:
            age = venue.book.age_mono(now_mono)
            return age is not None and age <= self.cfg.max_entry_book_age_sec
        return True

    def _check_book_age_guard(self, now_mono: Optional[float] = None, *,
                              reconnect: bool = False, reduce_only: bool = False) -> bool:
        limit = self.book_age_limit(reduce_only=reduce_only)
        if limit <= 0.0:
            return True
        ok = True
        for v in self.venues.values():
            if not self._trade_book_fresh(v, reduce_only=reduce_only, now_mono=now_mono):
                ok = False
                if not reconnect:
                    continue
                age = v.book.age_mono(now_mono)
                age_s = ("feed not ready" if not v.book.is_fresh(self.cfg.staleness_sec)
                         else "no usable quote" if age is None else f"{age:.1f}s")
                self._skiplog("[%s] stale book (%s, age limit %.1fs)",
                              v.name, age_s, limit)
                self.notifier.notify(
                    f"stale_book:{v.name}",
                    f"⚠️ Stale book on [{v.name}] ({age_s}, age limit {limit:.1f}s)",
                )
                if hasattr(v, "request_reconnect"):
                    v.request_reconnect()
        return ok

    def _book_watchdog_tick(self) -> None:
        has_position = any(abs(v.position) > 1e-12 for v in self.venues.values())
        reduce_only = not self.record_only and self.is_reduce_only()
        self._check_book_age_guard(reconnect=True, reduce_only=(reduce_only and has_position))
        # The first fresh snapshot may arrive during an unhedgeable cooldown.
        # Wake the strategy after that cooldown even if no new update arrives.
        net = self._net_position()
        if (reduce_only and abs(net) > self.cfg.net_tolerance_base and not self.halted
                and not self._state_fault and not self.has_pending_reconcile()
                and not self._hedge_running and not self._hedge_retry_exhausted
                and not self._is_unhedgeable_cooldown()
                and any(v.position * net > 0 and self._trade_book_fresh(v, reduce_only=True)
                        for v in self.venues.values())):
            self._update_evt.set()

    async def _book_watchdog(self) -> None:
        """Check quote age even when neither feed emits an order-book event."""
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                self._book_watchdog_tick()

    # -------------------------------------------------------------- strategy

    async def _strategy_loop(self) -> None:
        while not self.stop.is_set():
            await self._update_evt.wait()
            self._update_evt.clear()
            if self.stop.is_set():
                break
            try:
                await self._evaluate()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("evaluate failed")

    def _schedule_poke(self, delay: float) -> None:
        loop = asyncio.get_running_loop()
        due = loop.time() + max(delay, 0.01)
        if self._poke_due is not None and self._poke_due <= due + 0.02:
            return

        def _fire() -> None:
            self._poke_due = None
            self._update_evt.set()

        self._poke_due = due
        loop.call_at(due, _fire)

    def _skiplog(self, fmt: str, *args) -> None:
        now = time.time()
        if now - self._last_skiplog >= 2.0:
            self._last_skiplog = now
            log.info(fmt, *args)

    def _reset_signal(self, direction: Optional[str] = None) -> None:
        keys = (direction,) if direction is not None else self._armed
        for key in keys:
            self._armed[key] = None
            self._last_eligible.pop(key, None)
            self._armed_entropy_mono.pop(key, None)

    async def _evaluate(self) -> None:
        cfg = self.cfg
        if self.halted or self.has_pending_reconcile():
            self._reset_signal()
            return
        # Prioritize closing any unmatched single-leg delta before scanning two-leg pairs.
        net = self._net_position()
        if (abs(net) > cfg.net_tolerance_base
                and not self._hedge_retry_exhausted
                and not self._is_unhedgeable_cooldown()):
            await self._maybe_hedge()
            if self.halted or self.has_pending_reconcile():
                self._reset_signal()
                return
        now = time.time()
        if now - self.last_trade_ts < cfg.cooldown_sec:
            self._reset_signal()
            self._schedule_poke(cfg.cooldown_sec - (now - self.last_trade_ts))
            return
        best = self._scan(now)
        if best is None:
            # P0-2: if in reduce_only with unmatched single-leg exposure, delegate to _maybe_hedge
            if self.is_reduce_only(now) and not self.has_pending_reconcile():
                net = self._net_position()
                if abs(net) > cfg.net_tolerance_base:
                    await self._maybe_hedge()
            return
        buy, sell, plan = best
        reduce_only = self.is_reduce_only(now)
        lock_keys = sorted({buy.key, sell.key})
        acquired = []
        try:
            for k in lock_keys:
                lk = self._vlock(k)
                await lk.acquire()
                acquired.append(lk)
        except BaseException:
            for lk in reversed(acquired):
                lk.release()
            raise
        self._reset_signal()
        t = asyncio.create_task(
            self._execute_locked(buy, sell, plan, reduce_only=reduce_only,
                                 acquired_locks=acquired))
        self._exec_tasks.add(t)
        t.add_done_callback(self._exec_tasks.discard)
        await asyncio.shield(t)

    async def _execute_locked(self, buy, sell, plan: ArbPlan,
                              reduce_only: bool = False,
                              acquired_locks: Optional[list] = None) -> None:
        unresolved = False
        try:
            unresolved = await self._execute(buy, sell, plan, reduce_only=reduce_only)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("execute failed")
        finally:
            if acquired_locks is not None:
                for lk in reversed(acquired_locks):
                    lk.release()
            else:
                self._vlock(buy.key).release()
                self._vlock(sell.key).release()
        if unresolved or self.has_pending_reconcile():
            self._reconcile_evt.set()
        else:
            await self._maybe_hedge()
        self._update_evt.set()

    def _scan(self, now: float, now_mono: Optional[float] = None):
        cfg = self.cfg
        if self.halted or self.has_pending_reconcile() or self._bg_tasks:
            self._reset_signal()
            return None

        signal_time = time.monotonic() if now_mono is None else now_mono
        reduce_only_hint = self.is_reduce_only(now)
        if not all(self._entry_book_fresh(venue, reduce_only=reduce_only_hint,
                                          now_mono=signal_time)
                   for venue in (self.entropy, self.hedge)):
            self._reset_signal()
            return None

        prem = self.premium_bps()
        if cfg.risk.max_spread_bps > 0.0 and prem is not None and abs(prem) > cfg.risk.max_spread_bps:
            self._reset_signal()
            self._skiplog("extreme spread %.2fbps exceeds limit %.2fbps — blocking orders",
                          prem, cfg.risk.max_spread_bps)
            self.notifier.notify(
                "extreme_spread",
                f"⚠️ Extreme spread detected ({prem:+.2f} bps > ±{cfg.risk.max_spread_bps:.1f} bps) "
                f"— orders blocked",
            )
            return None

        if (
            self.dynamic_midline.enabled
            and all(self._trade_book_fresh(venue, reduce_only=False,
                                           now_mono=signal_time)
                    for venue in (self.entropy, self.hedge))
            and prem is not None
        ):
            self.dynamic_midline.update(prem, now=now)

        # P1-4: use trading-day-aligned daily MTM PnL (not cumulative session PnL)
        tripped, reason = self.circuit_breaker.check_losses(
            mtm_pnl_usd=self.daily_mtm_pnl(now=now), now=now)
        self._handle_loss_breaker(tripped, reason)
        if self.halted:
            self._reset_signal()
            return None

        sess = self.session_state(now)
        net = self._net_position()
        if abs(net) > cfg.net_tolerance_base:
            self.notifier.notify(
                "unbalanced_position", f"Net exposure {net:+.6g}; new entries paused until balanced")
        reduce_only = self.is_reduce_only(now)
        if reduce_only != self._signal_reduce_only:
            self._reset_signal()
            self._signal_reduce_only = reduce_only
        # A loss breaker can change the applicable age limit during this scan.
        if reduce_only != reduce_only_hint and not all(
            self._entry_book_fresh(venue, reduce_only=reduce_only, now_mono=signal_time)
            for venue in (self.entropy, self.hedge)
        ):
            return None

        best = None
        for buy, sell, dkey in ((self.hedge, self.entropy, "sell_entropy"),
                                (self.entropy, self.hedge, "buy_entropy")):
            if (not (buy.ready_to_trade() and sell.ready_to_trade()) or self._venue_down
                    or self._vlock(buy.key).locked() or self._vlock(sell.key).locked()
                    or self._venue_limited(buy) or self._venue_limited(sell)):
                self._reset_signal(dkey)
                continue
            if not (self._venue_rate_ok(buy) and self._venue_rate_ok(sell)):
                self._skiplog("%s deferred: venue order budget exhausted", dkey)
                self._reset_signal(dkey)
                continue
            if (buy.book.last_update_ts <= buy.last_traded_ts
                    or sell.book.last_update_ts <= sell.last_traded_ts):
                self._reset_signal(dkey)
                continue

            cap_base = (self._two_leg_reducible_qty(buy, sell) if reduce_only else
                        self._position_qty_headroom(buy, sell, sess.cap_multiplier))
            if cap_base < max(self._min_base, 1e-12):
                self._reset_signal(dkey)
                continue

            plan, _ = self._plan(buy, sell, cfg.max_order_notional, now=now,
                                 reduce_only=reduce_only, cap_base=cap_base)
            if plan is None:
                self._reset_signal(dkey)
                continue
            armed = self._armed.get(dkey)
            last_eligible = self._last_eligible.get(dkey, signal_time)
            self._last_eligible[dkey] = signal_time
            if armed is None or not 0.0 <= signal_time - last_eligible <= MAX_SIGNAL_GAP_SEC:
                self._armed[dkey] = signal_time
                self._armed_entropy_mono[dkey] = self.entropy.book.last_update_mono
                self._schedule_poke(min(cfg.premium_persist_sec, MAX_SIGNAL_GAP_SEC / 2))
                continue
            remaining = cfg.premium_persist_sec - (signal_time - armed)
            if remaining > 0.0:
                self._schedule_poke(min(remaining, MAX_SIGNAL_GAP_SEC / 2))
                continue
            if cfg.require_entropy_update_on_persist and cfg.premium_persist_sec > 0.0:
                armed_e_mono = self._armed_entropy_mono.get(dkey)
                if (armed_e_mono is not None
                        and self.entropy.book.last_update_mono <= armed_e_mono):
                    self._schedule_poke(min(0.2, MAX_SIGNAL_GAP_SEC / 2))
                    continue
            if best is None or plan.exp_edge_usd > best[2].exp_edge_usd:
                best = (buy, sell, plan)
        return best

    # ------------------------------------------------------------- execution

    async def _execute(self, buy, sell, plan: ArbPlan,
                       reduce_only: bool = False) -> bool:
        if self.stop.is_set() or self.halted or self.has_pending_reconcile():
            return False
        self._orders_in_flight += 1
        try:
            return await self._execute_pair(buy, sell, plan, reduce_only)
        finally:
            self._orders_in_flight -= 1
            if self.has_pending_reconcile():
                self._reconcile_evt.set()

    @staticmethod
    def _matched_inventory_signed(entropy_pos: float, hedge_pos: float) -> float:
        """Signed matched pair inventory (positive = long Entropy / short Hedge)."""
        if entropy_pos > 1e-12 and hedge_pos < -1e-12:
            return min(entropy_pos, -hedge_pos)
        if entropy_pos < -1e-12 and hedge_pos > 1e-12:
            return -min(-entropy_pos, hedge_pos)
        return 0.0

    def _sync_inv_basis_to_positions(
        self,
        entropy_pos: Optional[float] = None,
        hedge_pos: Optional[float] = None,
    ) -> None:
        """Clamp tracked inventory cost basis to actual matched positions (0 when flat)."""
        if self.entropy is None or self.hedge is None:
            return
        e_pos = self.entropy.position if entropy_pos is None else float(entropy_pos)
        h_pos = self.hedge.position if hedge_pos is None else float(hedge_pos)
        real_matched = self._matched_inventory_signed(e_pos, h_pos)
        if abs(real_matched) <= 1e-12 or self._inv_signed_qty * real_matched <= 0.0:
            self._inv_signed_qty = 0.0
            self._inv_avg_cash = 0.0
            self._inv_avg_booked = 0.0
        elif abs(self._inv_signed_qty) > abs(real_matched) + 1e-12:
            self._inv_signed_qty = real_matched

    def _pair_entry_edge(self, sell, buy_px: float, sell_px: float, fee_per_unit: float) -> float:
        mid = self.current_midline_bps() / 1e4
        if sell.key == "entropy":
            return sell_px - buy_px * (1.0 + mid) - fee_per_unit
        return sell_px * (1.0 + mid) - buy_px - fee_per_unit

    def _attribute_pair_pnl(
        self, buy, sell, matched: float, buy_px: float, sell_px: float,
        buy_fee_unit: float, sell_fee_unit: float,
    ) -> Tuple[float, float]:
        """Attribute matched pair fill PnL using tracked inventory cost basis.

        Returns `(pnl_delta_usd, streak_pnl_usd)`:
        - Opening quantity is booked against the current midline (preventing
          false consecutive losses when persistent midline != 0) and stored in
          the weighted-average basis (`_inv_avg_cash`, `_inv_avg_booked`).
        - Closing quantity computes exact realized round-trip cash PnL
          `closing * (avg_entry_cash + close_cash)` (invariant to dynamic
          midline drift and underlying price moves), reverses the entry-booked
          portion so `entry_booked + close_delta == exact round-trip cash`, and
          uses the real round-trip PnL for `max_consecutive_losses`.
        """
        fee_per_unit = buy_fee_unit + sell_fee_unit
        cash_per_unit = (sell_px - buy_px) - fee_per_unit
        half_turn_per_unit = self._pair_entry_edge(sell, buy_px, sell_px, fee_per_unit)
        fill_sign = -1.0 if sell.key == "entropy" else 1.0

        cur_signed = self._inv_signed_qty
        closing_qty = 0.0
        if cur_signed * fill_sign < 0.0:
            closing_qty = min(matched, abs(cur_signed))
        opening_qty = matched - closing_qty

        pnl_delta = 0.0
        streak_pnl = 0.0

        if closing_qty > 0.0:
            rt_pnl = closing_qty * (self._inv_avg_cash + cash_per_unit)
            close_delta = rt_pnl - closing_qty * self._inv_avg_booked
            pnl_delta += close_delta
            streak_pnl += rt_pnl
            rem = abs(cur_signed) - closing_qty
            if rem <= 1e-12:
                self._inv_signed_qty = 0.0
                self._inv_avg_cash = 0.0
                self._inv_avg_booked = 0.0
            else:
                self._inv_signed_qty = math.copysign(rem, cur_signed)

        if opening_qty > 0.0:
            open_pnl = opening_qty * half_turn_per_unit
            pnl_delta += open_pnl
            # A pure opening fill has not closed a round trip yet: only count
            # adverse slippage (< 0) as a loss; NEVER let a normal opening fill
            # reset an active consecutive-loss streak from prior losing round trips!
            if open_pnl < 0.0:
                streak_pnl += open_pnl
            prev_abs = abs(self._inv_signed_qty)
            new_abs = prev_abs + opening_qty
            self._inv_avg_cash = (
                prev_abs * self._inv_avg_cash + opening_qty * cash_per_unit
            ) / new_abs
            self._inv_avg_booked = (
                prev_abs * self._inv_avg_booked + opening_qty * half_turn_per_unit
            ) / new_abs
            self._inv_signed_qty = fill_sign * new_abs

        return pnl_delta, streak_pnl

    async def _lookup_deferred_fee(self, v, info: dict, filled: float) -> Optional[Tuple[float, float]]:
        try:
            verified = await asyncio.wait_for(v.fetch_fill_fee(
                order_id=info.get("order_id"),
                client_order_id=info.get("client_order_id"),
                coin=getattr(v, "coin", None),
            ), timeout=min(self.cfg.settle_timeout_sec, 5.0))
            if verified is None or not math.isclose(
                float(verified["filled_base"]), filled, rel_tol=0.0, abs_tol=1e-9
            ):
                return None
            fee, rate = float(verified["fee_usd"]), float(verified["fee_bps"])
            if not (math.isfinite(fee) and math.isfinite(rate) and fee >= 0 and rate >= 0):
                return None
            return fee, rate
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug("[%s] async fill fee verify failed: %r", v.name, e)
            return None

    async def _verify_fill_fees_bg(
        self, checks, *, pair_id: str, matched_qty: float, opened_qty: float,
        fill_sign: float, basis_qty: float, opening_edge: float,
        streak_pnl: float, booked_day: str, trade: dict,
        initial_delay: float = 0.0,
    ) -> None:
        """Reconcile fees as a batch without blocking an emergency single-leg hedge."""
        if initial_delay > 0.0:
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=initial_delay)
                return
            except asyncio.TimeoutError:
                pass
        verified = await asyncio.gather(*(
            self._lookup_deferred_fee(venue, info, filled)
            for venue, info, filled, _ in checks
        ))
        same_day = self._ensure_daily_breakdown() == booked_day
        total_delta = matched_fee_per_unit = 0.0
        for (venue, _, filled, estimate), actual in zip(checks, verified):
            if actual is None:
                continue
            fee, rate = actual
            delta = fee - estimate
            venue.cash -= delta
            total_delta += delta
            per_unit = delta / filled
            matched_fee_per_unit += per_unit
            if same_day:
                self.daily_pnl_breakdown["estimated_fee_usd"] = max(
                    0.0, self.daily_pnl_breakdown["estimated_fee_usd"] - estimate)
                self.daily_pnl_breakdown["actual_fee_usd"] += fee
            self._handle_fee_observation(rate, venue_key=venue.key)
            # A confirmed leg may be waiting for its sibling. Update the
            # pending journal so late settlement uses the same actual fee.
            for pending in self._pending_orders.values():
                sibling = pending.get("pair_ctx")
                if (pending.get("pair_id") == pair_id and isinstance(sibling, dict)
                        and sibling.get("sibling_venue") == venue.key):
                    sibling["sibling_fee_unit"] = fee / filled

        if not same_day and self._daily_mtm_day == self.circuit_breaker.day_for():
            if self._daily_mtm_baseline is not None:
                self._daily_mtm_baseline -= total_delta

        # Until this batch finishes, new paired orders and pending settlement
        # cannot change the inventory basis. A concurrent single-leg hedge
        # can only trim it; prorate the cost if some inventory was removed.
        self._sync_inv_basis_to_positions()
        remaining = abs(self._inv_signed_qty)
        if basis_qty > 0 and remaining > 0 and self._inv_signed_qty * fill_sign > 0:
            surviving = opened_qty * min(1.0, remaining / basis_qty)
            change = surviving * matched_fee_per_unit / remaining
            self._inv_avg_cash -= change
            self._inv_avg_booked -= change

        correction = matched_qty * matched_fee_per_unit
        self.total_fill_edge -= correction
        if trade["fill"] is not None:
            trade["fill"] -= correction
        if same_day:
            closing_fee = (matched_qty - opened_qty) * matched_fee_per_unit
            opening_after_fees = opening_edge - opened_qty * matched_fee_per_unit
            corrected_streak = (streak_pnl - closing_fee
                                + min(0.0, opening_after_fees) - min(0.0, opening_edge))
            tripped, reason = self.circuit_breaker.record_trade_pnl(
                -correction if matched_qty else 0.0, mtm_pnl_usd=self.daily_mtm_pnl(),
                count_streak=matched_qty > 0, streak_pnl_usd=corrected_streak)
            self._handle_loss_breaker(tripped, reason)
        if self.state_store is not None and not self.save_state():
            self._stop_on_state_error("could not persist verified fill fees")

    def _fee_batch_done(self, task: asyncio.Task) -> None:
        self._bg_tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            log.error("fee verification batch failed: %s", task.exception())
            self._stop_on_state_error("deferred fill fee accounting failed")
        self._reconcile_evt.set()
        self._update_evt.set()

    async def _execute_pair(self, buy, sell, plan: ArbPlan, reduce_only: bool) -> bool:
        if self._bg_tasks:
            return False
        cfg = self.cfg
        now = time.time()
        send_reduce_only = bool(reduce_only or self.is_reduce_only(now))
        checked_at = time.monotonic()
        order_type = "REDUCE" if send_reduce_only else "ENTRY"
        # Post-lock re-verification: state or books may have changed while waiting for locks.
        if (not (buy.ready_to_trade() and sell.ready_to_trade())
                or self._venue_down
                or self._venue_limited(buy) or self._venue_limited(sell)
                or not all(self._entry_book_fresh(venue, reduce_only=send_reduce_only,
                                                  now_mono=checked_at)
                           for venue in (buy, sell))):
            log.warning("[PRE-SEND BLOCKED] %s %s/%s: venue readiness or book freshness changed",
                        order_type, buy.name, sell.name)
            return False
        prem = self.premium_bps()
        if cfg.risk.max_spread_bps > 0.0 and prem is not None and abs(prem) > cfg.risk.max_spread_bps:
            log.warning("[PRE-SEND BLOCKED] %s %s/%s: extreme spread %.2fbps after lock",
                        order_type, buy.name, sell.name, prem)
            return False
        prepared = self._prepare_pair_order(buy, sell, plan, reduce_only=send_reduce_only, now=now)
        if prepared is None:
            log.warning("[BOUNDS BLOCKED] %s %s/%s: order failed final depth, spread, or size limits",
                        order_type, buy.name, sell.name)
            return False
        plan, buy_bound, sell_bound = prepared
        qty = plan.qty

        inv_bps = self._inv_add_bps(buy, sell)
        direction = "sell_entropy" if sell.key == "entropy" else "buy_entropy"
        log.info("[ARB] %s%s: BUY %s %.6g @<=%.6g | SELL %s @>=%.6g | "
                 "take $%.0f of $%.0f | prem %.2fbps | exp $%.4f",
                 direction, " [RO]" if send_reduce_only else "",
                 buy.name, qty, plan.buy_limit, sell.name,
                 plan.sell_limit, plan.buy_notional, plan.q_max_notional,
                 plan.marginal_premium_bps, plan.exp_edge_usd)

        entropy_first = (
            cfg.execution_mode == "entropy_first"
            and buy.key != sell.key
            and "entropy" in (buy.key, sell.key)
        )
        buy_sent_qty = sell_sent_qty = qty
        buy_sent_bound, sell_sent_bound = buy_bound, sell_bound
        sent_buy = sent_sell = True
        if entropy_first:
            entropy_is_buy = (buy.key == "entropy")
            first_venue, first_is_buy, first_bound = (
                (buy, True, buy_bound) if entropy_is_buy else (sell, False, sell_bound)
            )
            second_venue, second_is_buy, second_bound = (
                (sell, False, sell_bound) if entropy_is_buy else (buy, True, buy_bound)
            )
            ids = self._stage_orders(
                ((first_venue, first_is_buy, first_bound),),
                qty=qty, purpose="arb", reduce_only=send_reduce_only,
                submission="attempted",
            )
            if ids is None:
                return False
            checked_at = time.monotonic()
            if not all(self._entry_book_fresh(venue, reduce_only=send_reduce_only,
                                              now_mono=checked_at)
                       for venue in (buy, sell)):
                not_submitted = {"unresolved": False}
                self._commit_orders(ids, ((first_venue, not_submitted),))
                log.warning("[PRE-SEND BLOCKED] %s %s/%s: book expired during order staging",
                            order_type, buy.name, sell.name)
                return False
            self.last_trade_ts = now
            self._record_send(first_venue)
            first_info = await self._send_staged_order(
                first_venue, ids, is_buy=first_is_buy, qty=qty, limit_px=first_bound,
                reduce_only=send_reduce_only,
            )
            first_fill = float(first_info.get("filled_base") or 0.0)
            first_unresolved = bool(first_info.get("unresolved"))
            second_sent = False
            follow_qty = qty
            follow_bound = second_bound
            if not first_unresolved and first_fill > 0.0:
                follow_qty = floor_step(first_fill, self._step)
                follow_slip = self._entropy_first_follow_slip()
                ref = (second_venue.book.best_ask() if second_is_buy
                       else second_venue.book.best_bid())
                can_follow = (
                    not self.stop.is_set()
                    and not self._state_fault
                    and second_venue.ready_to_trade()
                    and second_venue.key not in self._venue_down
                    and not self._venue_limited(second_venue)
                    and self._venue_rate_ok(second_venue)
                    and self._trade_book_fresh(second_venue, reduce_only=True)
                    and ref is not None and math.isfinite(ref) and ref > 0.0
                )
                if can_follow:
                    follow_bound = second_venue.px_round(
                        ref * (1.0 + follow_slip) if second_is_buy else ref * (1.0 - follow_slip),
                        round_up=not second_is_buy,
                    )
                    if not (math.isfinite(follow_bound) and follow_bound > 0.0):
                        can_follow = False
                    else:
                        follow_order_price = max(ref, follow_bound)
                        follow_qty = floor_step(
                            min(follow_qty, cfg.max_order_notional / follow_order_price),
                            self._step,
                        )
                        venue_step = 10 ** -second_venue.size_decimals
                        is_full_close = (
                            send_reduce_only
                            and abs(second_venue.position) >= venue_step - 1e-12
                            and (second_venue.position < 0 if second_is_buy else second_venue.position > 0)
                            and math.isclose(
                                follow_qty, floor_step(abs(second_venue.position), venue_step),
                                rel_tol=0.0, abs_tol=1e-9,
                            )
                        )
                        min_base_req = venue_step if is_full_close else second_venue.min_base
                        min_notional_ok = (
                            is_full_close
                            or follow_qty * min(ref, follow_bound)
                            >= max(cfg.min_order_notional, second_venue.min_quote)
                        )
                        can_follow = (
                            follow_qty >= max(min_base_req, 1e-12)
                            and follow_qty * follow_order_price <= cfg.max_order_notional + 1e-9
                            and min_notional_ok
                        )
                if can_follow:
                    if self._stage_followup_order(
                        ids, second_venue, is_buy=second_is_buy, qty=follow_qty,
                        limit_px=follow_bound, purpose="arb", reduce_only=send_reduce_only,
                    ):
                        log.info("[ARB-FOLLOW] %s%s: %s %s %.6g @%.6g (matched %s fill %.6g)",
                                 direction, " [RO]" if send_reduce_only else "",
                                 "BUY" if second_is_buy else "SELL",
                                 second_venue.name, follow_qty, follow_bound,
                                 first_venue.name, first_fill)
                        self._record_send(second_venue)
                        second_sent = True
                        second_info = await self._send_staged_order(
                            second_venue, ids, is_buy=second_is_buy, qty=follow_qty,
                            limit_px=follow_bound, reduce_only=send_reduce_only,
                        )
                    else:
                        second_info = {
                            "status": "not-submitted", "outcome": "rejected",
                            "filled_base": 0.0, "avg_px": None,
                            "err": "order journal unavailable",
                            "unresolved": False, "client_order_id": None,
                        }
                else:
                    second_info = {
                        "status": "skipped", "outcome": "skipped",
                        "filled_base": 0.0, "avg_px": None,
                        "err": None, "unresolved": False, "client_order_id": None,
                    }
            else:
                second_info = {
                    "status": "skipped", "outcome": "skipped",
                    "filled_base": 0.0, "avg_px": None,
                    "err": None, "unresolved": False, "client_order_id": None,
                }
            if entropy_is_buy:
                binfo, sinfo = first_info, second_info
                sent_buy, sent_sell = True, second_sent
                sell_sent_qty, sell_sent_bound = follow_qty, follow_bound
            else:
                binfo, sinfo = second_info, first_info
                sent_buy, sent_sell = second_sent, True
                buy_sent_qty, buy_sent_bound = follow_qty, follow_bound
        else:
            # Persist both client IDs as "attempted" in ONE atomic fsync before firing
            # both legs concurrently (zero fsync stagger between leg 1 and leg 2).
            ids = self._stage_orders(
                ((buy, True, buy_bound), (sell, False, sell_bound)),
                qty=qty, purpose="arb", reduce_only=send_reduce_only,
                submission="attempted",
            )
            if ids is None:
                return False
            checked_at = time.monotonic()
            if not all(self._entry_book_fresh(venue, reduce_only=send_reduce_only,
                                              now_mono=checked_at)
                       for venue in (buy, sell)):
                # No adapter was called. Checkpoint the removal of both known-unsent IDs.
                not_submitted = {"unresolved": False}
                self._commit_orders(ids, ((buy, not_submitted), (sell, not_submitted)))
                log.warning("[PRE-SEND BLOCKED] %s %s/%s: book expired during order staging",
                            order_type, buy.name, sell.name)
                return False
            self.last_trade_ts = now
            self._record_send(buy)
            self._record_send(sell)
            binfo, sinfo = await asyncio.gather(
                self._send_staged_order(buy, ids, is_buy=True, qty=qty, limit_px=buy_bound,
                                        reduce_only=send_reduce_only),
                self._send_staged_order(sell, ids, is_buy=False, qty=qty, limit_px=sell_bound,
                                        reduce_only=send_reduce_only),
            )

        bfill = float(binfo.get("filled_base") or 0.0)
        sfill = float(sinfo.get("filled_base") or 0.0)
        imbalanced = (
            abs(bfill - sfill) > cfg.net_tolerance_base
            or bool(binfo.get("unresolved") or sinfo.get("unresolved"))
        )

        # P1-3: verify actual fees (via userFillsByTime when HL IOC receipt omits fee).
        # When imbalanced, NEVER block hedging on a REST fee lookup — defer to background task.
        # On balanced fills where exchange indexing lags, retry in background if configured.
        deferred_fee_checks = []
        for v, info, side in (
            (buy, binfo, "buy"),
            (sell, sinfo, "sell"),
        ):
            if info.get("err"):
                log.error("[%s] %s leg: %s", v.name, side, info["err"])
            filled = float(info.get("filled_base") or 0.0)
            if filled > 0.0:
                can_verify = (
                    info.get("fee_bps") is None
                    and (v.key == "entropy" or getattr(v, "kind", None) == "hl")
                    and hasattr(v, "fetch_fill_fee")
                )
                if can_verify and not imbalanced:
                    verified = await v.fetch_fill_fee(
                        order_id=info.get("order_id"),
                        client_order_id=info.get("client_order_id"),
                        coin=getattr(v, "coin", None),
                    )
                    if verified is not None and math.isclose(
                        verified.get("filled_base", 0.0), filled, rel_tol=0.0, abs_tol=1e-9
                    ):
                        info["fee_bps"] = verified["fee_bps"]
                        info["fee_usd"] = verified["fee_usd"]
                        info["fee_estimated"] = False
                if info.get("fee_bps") is not None:
                    info["fee_estimated"] = False
                    if v.key == "entropy" or getattr(v, "kind", None) == "hl":
                        self._handle_fee_observation(float(info["fee_bps"]), venue_key=v.key)
                else:
                    info["fee_estimated"] = True
                    if can_verify and (imbalanced or cfg.fill_fee_retry_delay_sec > 0.0):
                        deferred_fee_checks.append((v, info, filled))

        # Apply confirmed fills once (with client_order_id deduplication)
        b_cid = binfo.get("client_order_id")
        s_cid = sinfo.get("client_order_id")
        b_fee_rate = (
            float(binfo["fee_bps"]) / 1e4
            if binfo.get("fee_bps") is not None
            else self.effective_fee_bps(buy) / 1e4
        )
        s_fee_rate = (
            float(sinfo["fee_bps"]) / 1e4
            if sinfo.get("fee_bps") is not None
            else self.effective_fee_bps(sell) / 1e4
        )

        pre_e_pos = self.entropy.position if self.entropy is not None else 0.0
        pre_h_pos = self.hedge.position if self.hedge is not None else 0.0
        actual_fee_total = est_fee_total = 0.0
        posted = {}
        posted_px: Dict[str, float] = {}
        posted_est_fee: Dict[str, float] = {}
        for v, info, filled, cid, fallback_px, fee_rate, sign in (
            (buy, binfo, bfill, b_cid, buy_sent_bound, b_fee_rate, 1),
            (sell, sinfo, sfill, s_cid, sell_sent_bound, s_fee_rate, -1),
        ):
            if filled <= 0.0 or (cid and f"{v.key}:{cid}" in self._settled_order_ids):
                continue
            px = info.get("avg_px") or fallback_px
            fee = info.get("fee_usd")
            fee = float(fee) if fee is not None else filled * px * fee_rate
            if info["fee_estimated"]:
                est_fee_total += fee
                posted_est_fee[v.key] = fee
            else:
                actual_fee_total += fee
            v.position += sign * filled
            v.cash -= sign * filled * px + fee
            v.volume_usd += filled * px
            posted[v.key] = (filled, fee / filled)
            posted_px[v.key] = float(px)

        if posted:
            self._ensure_daily_breakdown()
            self.daily_pnl_breakdown["actual_fee_usd"] += actual_fee_total
            self.daily_pnl_breakdown["estimated_fee_usd"] += est_fee_total

        deferred_checks = [(v, info, filled, posted_est_fee[v.key])
                           for v, info, filled in deferred_fee_checks if v.key in posted_est_fee]

        unresolved = False
        pair_id = f"{b_cid or ''}:{s_cid or ''}:{int(now * 1000)}"
        for v, info, is_buy, leg_qty, bound, other, other_is_buy in (
            (buy, binfo, True, buy_sent_qty, buy_sent_bound, sell, False),
            (sell, sinfo, False, sell_sent_qty, sell_sent_bound, buy, True),
        ):
            cid = info.get("client_order_id")
            key = f"{v.key}:{cid}" if cid is not None else None
            known_fill = float(info.get("filled_base") or 0.0)
            if key in self._pending_orders:
                self._pending_orders[key]["known_filled"] = known_fill
            if info.get("unresolved"):
                unresolved = True
                pair_ctx = None
                if other.key in posted and other.key in posted_px:
                    other_filled, other_fee_unit = posted[other.key]
                    pair_ctx = {
                        "sibling_venue": other.key,
                        "sibling_is_buy": other_is_buy,
                        "sibling_filled": other_filled,
                        "sibling_px": posted_px[other.key],
                        "sibling_fee_unit": other_fee_unit,
                    }
                self._register_pending_order(
                    v, cid, is_buy=is_buy, qty=leg_qty, limit_px=bound,
                    known_filled=known_fill, purpose="arb", reduce_only=send_reduce_only,
                    persist=False, pair_id=pair_id, pair_ctx=pair_ctx,
                )
            elif key is not None:
                self._mark_order_settled(key)

        posted_buy, buy_fee_unit = posted.get(buy.key, (0.0, 0.0))
        posted_sell, sell_fee_unit = posted.get(sell.key, (0.0, 0.0))
        matched = min(posted_buy, posted_sell)
        fill_edge = 0.0
        pnl_delta = 0.0
        streak_pnl = 0.0
        attributed_qty = opened_qty = opening_edge = 0.0
        fill_sign = 1.0 if buy.key == "entropy" else -1.0
        if matched > 0 and binfo.get("avg_px") and sinfo.get("avg_px"):
            self._sync_inv_basis_to_positions(pre_e_pos, pre_h_pos)
            attributed_qty = matched
            closing_qty = (min(matched, abs(self._inv_signed_qty))
                           if self._inv_signed_qty * fill_sign < 0 else 0.0)
            opened_qty = matched - closing_qty
            opening_edge = opened_qty * self._pair_entry_edge(
                sell, binfo["avg_px"], sinfo["avg_px"], buy_fee_unit + sell_fee_unit)
            gross_spread = matched * (sinfo["avg_px"] - binfo["avg_px"])
            fill_edge = gross_spread - matched * (buy_fee_unit + sell_fee_unit)
            pnl_delta, streak_pnl = self._attribute_pair_pnl(
                buy, sell, matched, binfo["avg_px"], sinfo["avg_px"],
                buy_fee_unit, sell_fee_unit,
            )
            self.total_fill_edge += fill_edge
            # P1-4: record daily PnL breakdown
            self.daily_pnl_breakdown["spread_pnl_usd"] += gross_spread
        elif not unresolved:
            self._sync_inv_basis_to_positions()

        log.info("[SETTLED] %s: buy %s %s %.6g/%.6g | sell %s %s %.6g/%.6g | "
                 "matched %.6g | fill edge $%.4f (actFee=$%.4f estFee=$%.4f)",
                 direction, buy.name, binfo["status"], bfill, buy_sent_qty,
                 sell.name, sinfo["status"], sfill, sell_sent_qty, matched,
                 fill_edge, actual_fee_total, est_fee_total)
        settled_now = time.time()
        if sent_buy:
            buy.last_traded_ts = settled_now
        if sent_sell:
            sell.last_traded_ts = settled_now

        if abs(bfill - sfill) > cfg.net_tolerance_base:
            self.notifier.notify(
                "legging_failure",
                f"⚠️ Legging failure ({direction}): "
                f"BUY {buy.name} {bfill:.6g}/{buy_sent_qty:.6g} ({binfo['status']}) | "
                f"SELL {sell.name} {sfill:.6g}/{sell_sent_qty:.6g} ({sinfo['status']}) | "
                f"imbalance {bfill - sfill:+.6g}",
            )

        hard_err = (binfo.get("err") is not None
                    or sinfo.get("err") is not None)
        rate_limited = False
        for v, info in ((buy, binfo), (sell, sinfo)):
            if str(info.get("err", "")).startswith("RATE_LIMITED"):
                rate_limited = True
                self._mark_limited(v)
            elif "margin" in str(info.get("status", "")).lower():
                log.warning("[%s] margin rejection — collateral exhausted, "
                            "pausing venue", v.name)
                self._mark_limited(v)
        sent_ok = not hard_err and not unresolved
        if sent_ok:
            self.consec_errors = 0
        elif not rate_limited:
            self.consec_errors += 1
            if self.consec_errors >= cfg.max_consecutive_errors:
                self.halted = True
                log.critical("HALTED after %d consecutive execution problems "
                             "— flatten manually and restart / 连续执行异常，"
                             "引擎已停止，请手动平仓后重启", self.consec_errors)
                self.notifier.notify(
                    "circuit_breaker:consec_errors",
                    f"🚨 HALTED after {self.consec_errors} consecutive execution errors — "
                    f"flatten manually and restart",
                    force=True,
                )
        paired_fill = matched > 0 and not unresolved
        matched_expected = plan.exp_edge_usd * matched / plan.qty if paired_fill else 0.0
        if paired_fill:
            self.trades += 1
            self.total_exp_edge += matched_expected
        if matched > 0 and binfo.get("avg_px") and sinfo.get("avg_px"):
            tripped, reason = self.circuit_breaker.record_trade_pnl(
                pnl_delta,
                mtm_pnl_usd=self.daily_mtm_pnl(),
                count_streak=not deferred_checks, streak_pnl_usd=streak_pnl,
            )
            self._handle_loss_breaker(tripped, reason)

        self._record_trade(direction, plan,
                           fill_edge if paired_fill else None,
                           f"{binfo['status']}/{sinfo['status']}", paired_fill, matched_expected)
        self._log_csv(direction, buy, sell, plan, paired_fill, bfill, sfill,
                      binfo["status"], sinfo["status"], fill_edge, inv_bps)
        self.last_trade_ts = time.time()
        committed = self._commit_orders(ids, ((buy, binfo), (sell, sinfo)))
        if committed and deferred_checks:
            fee_delay = 0.0 if imbalanced else cfg.fill_fee_retry_delay_sec
            task = asyncio.create_task(self._verify_fill_fees_bg(
                deferred_checks, pair_id=pair_id, matched_qty=attributed_qty,
                opened_qty=opened_qty, fill_sign=fill_sign, basis_qty=abs(self._inv_signed_qty),
                opening_edge=opening_edge, streak_pnl=streak_pnl,
                booked_day=self.circuit_breaker.day_for(), trade=self.recent_trades[-1],
                initial_delay=fee_delay,
            ), name="verify-pair-fill-fees")
            self._bg_tasks.add(task)
            task.add_done_callback(self._fee_batch_done)
        return bool(unresolved)

    def _record_trade(self, direction: str, plan: ArbPlan, fill_edge,
                      status: str, ok: bool, expected_edge: float) -> None:
        self.recent_trades.append({
            "ts": time.time(), "direction": direction, "qty": plan.qty,
            "notional": plan.buy_notional,
            "prem_bps": plan.marginal_premium_bps,
            "exp": expected_edge, "fill": fill_edge, "status": status,
            "ok": ok})

    async def _maybe_hedge(self) -> None:
        if self._state_fault or self.has_pending_reconcile():
            return
        net = self._net_position()
        if abs(net) <= self.cfg.net_tolerance_base:
            self._reset_hedge_attempts()
            return
        await self._hedge(net)

    def _pos_snapshot(self) -> Tuple[float, ...]:
        return tuple(round(float(v.position), 12) for v in self.venues.values())

    def _is_unhedgeable_cooldown(self) -> bool:
        if time.monotonic() >= self._unhedgeable_until:
            return False
        if self._unhedgeable_snapshot != self._pos_snapshot():
            self._unhedgeable_until = 0.0
            self._unhedgeable_snapshot = None
            return False
        return True

    async def _hedge(self, net: float) -> None:
        """P1-1: Reduce net imbalance with:
        - Immediate P0-1 escalation on unknown/timeout outcome (zero duplicate hedges)
        - Price rounding strictly inside hedge_max_slippage_bps
        - Recomputed qty from confirmed fills
        - Halt + alert only if remaining net > tolerance after retry budget exhausted
        """
        if (self.has_pending_reconcile() or self._hedge_running or self._hedge_retry_exhausted
                or self._is_unhedgeable_cooldown()):
            return
        self._hedge_running = True
        try:
            await self._hedge_attempts()
        finally:
            self._hedge_running = False

    def _reset_hedge_attempts(self) -> None:
        if self._hedge_attempt_count:
            self._hedge_attempt_count = 0
            if self.state_store is not None and not self.save_state():
                self._stop_on_state_error("could not checkpoint resolved hedge exposure")
        self._unhedgeable_until = 0.0
        self._unhedgeable_snapshot = None

    async def _hedge_attempts(self) -> None:
        cfg = self.cfg
        max_retries = max(int(cfg.hedge_max_retries), 1)
        max_slices = max(max_retries * 4, 20)
        eff_slip_bps = (
            min(cfg.hedge_slippage_bps, cfg.hedge_max_slippage_bps)
            if cfg.hedge_max_slippage_bps > 0.0
            else cfg.hedge_slippage_bps
        )
        slip = eff_slip_bps / 1e4
        last_err = ""
        slices_sent = 0

        while self._hedge_attempt_count < max_retries and slices_sent < max_slices:
            if self.stop.is_set() or self._state_fault or self.has_pending_reconcile():
                return
            cur_net = self._net_position()
            if abs(cur_net) <= cfg.net_tolerance_base:
                self._reset_hedge_attempts()
                return
            attempt = self._hedge_attempt_count + 1
            sgn = 1.0 if cur_net > 0 else -1.0
            tried_venue = False
            made_progress = False

            for v in sorted(self.venues.values(),
                            key=lambda x: (self._venue_limited(x), -x.position * sgn)):
                lk = self._vlock(v.key)
                if lk.locked():
                    continue
                await lk.acquire()
                try:
                    if self.stop.is_set() or self._state_fault or self.has_pending_reconcile():
                        return
                    cur_net = self._net_position()
                    if abs(cur_net) <= cfg.net_tolerance_base:
                        self._reset_hedge_attempts()
                        return
                    is_sell = cur_net > 0
                    if (v.position * cur_net <= 0 or v.key in self._venue_down
                            or not v.ready_to_trade() or self._venue_limited(v)
                            or not self._venue_rate_ok(v)
                            or not self._trade_book_fresh(v, reduce_only=True)):
                        continue
                    venue_step = 10 ** -v.size_decimals
                    closing_all_on_venue = abs(cur_net) >= abs(v.position) - 1e-9
                    step = min(self._step, venue_step) if closing_all_on_venue else self._step
                    qty = floor_step(min(abs(cur_net), abs(v.position)), step)
                    ref = v.book.best_bid() if is_sell else v.book.best_ask()
                    if qty <= 0.0 or ref is None:
                        continue
                    # Round inward so the order's limit stays inside the slippage cap.
                    limit = v.px_round(ref * (1.0 - slip) if is_sell else ref * (1.0 + slip),
                                       round_up=is_sell)
                    if not math.isfinite(limit) or limit <= 0.0:
                        continue
                    order_price = max(ref, limit)
                    qty = floor_step(min(qty, cfg.max_order_notional / order_price), step)
                    full_venue_qty = floor_step(abs(v.position), venue_step)
                    is_full_close = (
                        closing_all_on_venue
                        and full_venue_qty >= venue_step
                        and math.isclose(qty, full_venue_qty, rel_tol=0.0, abs_tol=1e-9)
                    )
                    min_base_req = venue_step if is_full_close else v.min_base
                    min_notional_ok = (
                        is_full_close
                        or qty * limit >= max(cfg.min_order_notional, v.min_quote)
                    )
                    if (qty <= 0.0 or qty < min_base_req or not min_notional_ok
                            or not self._trade_book_fresh(v, reduce_only=True)):
                        continue
                    side_str = "SELL" if is_sell else "BUY"
                    ids = self._stage_orders(
                        ((v, not is_sell, limit),), qty=qty, purpose="hedge",
                        reduce_only=True, submission="attempted",
                    )
                    if ids is None:
                        return
                    # The durable write can take long enough to age out this
                    # quote. No adapter has been called, so the ID can be cleared.
                    if not self._trade_book_fresh(v, reduce_only=True):
                        if ids and not self._commit_orders(ids, ((v, {"unresolved": False}),)):
                            return
                        continue
                    tried_venue = True
                    log.warning("[HEDGE %d/%d] net %+.6g — %s %.6g on %s @%.6g (slip<=%.1fbps)",
                                attempt, max_retries, cur_net, side_str, qty, v.name,
                                limit, eff_slip_bps)
                    slices_sent += 1
                    self.hedges += 1
                    self._record_send(v)
                    self._orders_in_flight += 1
                    try:
                        info = await self._send_staged_order(
                            v, ids, is_buy=not is_sell, qty=qty, limit_px=limit,
                            reduce_only=True)
                    finally:
                        self._orders_in_flight -= 1
                    v.last_traded_ts = time.time()

                    if info.get("unresolved"):
                        self._register_pending_order(
                            v, info.get("client_order_id"), is_buy=not is_sell,
                            qty=qty, limit_px=limit, known_filled=0.0,
                            purpose="hedge", reduce_only=True,
                        )
                        return

                    if info.get("err"):
                        self._hedge_attempt_count += 1
                        last_err = str(info["err"])
                        log.error("[HEDGE %d/%d] %s: %s", self._hedge_attempt_count, max_retries,
                                  v.name, last_err)
                        if last_err.startswith("RATE_LIMITED"):
                            self._mark_limited(v)
                        self._reconcile_evt.set()
                        self.notifier.notify(
                            "hedge_result",
                            f"🚨 Auto-hedge failed ({self._hedge_attempt_count}/{max_retries}) on {v.name}: "
                            f"{side_str} {qty:.6g} ({last_err})",
                        )
                    else:
                        fill = float(info.get("filled_base") or 0.0)
                        cid = info.get("client_order_id")
                        okey = f"{v.key}:{cid}" if cid else ""
                        if fill > 0.0 and (not okey or self._mark_order_settled(okey)):
                            made_progress = True
                            self._hedge_attempt_count = 0
                            v.position += -fill if is_sell else fill
                            self._sync_inv_basis_to_positions()
                            px = info.get("avg_px") or limit
                            if info.get("fee_bps") is not None and (
                                v.key == "entropy" or getattr(v, "kind", None) == "hl"
                            ):
                                self._handle_fee_observation(float(info["fee_bps"]), venue_key=v.key)
                            fee_usd = (
                                float(info["fee_usd"])
                                if info.get("fee_usd") is not None
                                else fill * px * self.effective_fee_bps(v) / 1e4
                            )
                            v.cash += (fill * px if is_sell else -fill * px) - fee_usd
                            v.volume_usd += fill * px
                            if ids:
                                self._pending_orders[f"{v.key}:{ids[v.key]}"]["known_filled"] = fill
                            # P1-4: track hedge cost/slippage vs mid separately
                            mid = v.book.mid()
                            if mid is not None:
                                slip_pnl = (
                                    fill * (px - mid) if is_sell else fill * (mid - px)
                                ) - fee_usd
                                self._ensure_daily_breakdown()
                                self.daily_pnl_breakdown["hedge_pnl_usd"] += slip_pnl
                                tripped, reason = self.circuit_breaker.record_trade_pnl(
                                    slip_pnl,
                                    mtm_pnl_usd=self.daily_mtm_pnl(),
                                    count_streak=False,
                                )
                                self._handle_loss_breaker(tripped, reason)
                            log.info("[HEDGE SETTLED] %s %s %.6g/%.6g",
                                     v.name, info["status"], fill, qty)
                            self.notifier.notify(
                                "hedge_result",
                                f"✅ Auto-hedge settled on {v.name}: {side_str} "
                                f"{fill:.6g}/{qty:.6g} ({info['status']})",
                            )
                        elif fill <= 0.0:
                            self._hedge_attempt_count += 1
                            last_err = f"zero fill ({info.get('status')})"
                            log.warning("[HEDGE %d/%d] %s returned zero fill (%s)",
                                        self._hedge_attempt_count, max_retries, v.name, info.get("status"))
                    if not self._commit_orders(ids, ((v, info),)):
                        return
                finally:
                    lk.release()
                break

            if not tried_venue:
                rebal = await self._try_rebalance_submin_hedge(eff_slip_bps)
                if rebal is not None:
                    slices_sent += 1
                    rebal_progress, should_return, rebal_err = rebal
                    if rebal_err:
                        last_err = rebal_err
                    if should_return:
                        return
                    made_progress = rebal_progress
                else:
                    cur_net = self._net_position()
                    if abs(cur_net) <= cfg.net_tolerance_base:
                        self._reset_hedge_attempts()
                        return
                    if any(v.position and not self._trade_book_fresh(v, reduce_only=True)
                           for v in self.venues.values()):
                        self._skiplog("[HEDGE] net %+.6g; waiting for fresh quotes", cur_net)
                        return
                    self._unhedgeable_until = time.monotonic() + max(cfg.hedge_retry_delay_sec, 2.0)
                    self._unhedgeable_snapshot = self._pos_snapshot()
                    self._skiplog("[HEDGE] net %+.6g; no venue can reduce it within freshness, "
                                  "rate or order-size limits", cur_net)
                    self.notifier.notify(
                        "unhedgeable_position", f"Net exposure {cur_net:+.6g} could not be hedged; "
                        "new entries remain paused")
                    return

            rem_net = self._net_position()
            if abs(rem_net) <= cfg.net_tolerance_base:
                self._reset_hedge_attempts()
                return
            if not made_progress and self._hedge_attempt_count < max_retries and cfg.hedge_retry_delay_sec > 0.0:
                await asyncio.sleep(cfg.hedge_retry_delay_sec)

        # P1-1: retry budget exhausted — halt & alert ONLY if remaining net > tolerance
        rem_net = self._net_position()
        if abs(rem_net) > cfg.net_tolerance_base:
            self.halted = True
            self._hedge_retry_exhausted = True
            log.critical("HALTED: auto-hedge exhausted %d retries (slices=%d) with remaining net=%+.6g "
                         "(last=%s)", max_retries, slices_sent, rem_net, last_err)
            self.notifier.notify(
                "circuit_breaker:hedge_failed",
                f"🚨 HALTED: Auto-hedge exhausted {max_retries} retries "
                f"(remaining net={rem_net:+.6g}, last={last_err}) — manual intervention required",
                force=True,
            )

    async def _try_rebalance_submin_hedge(
        self, eff_slip_bps: float,
    ) -> Optional[Tuple[bool, bool, str]]:
        """Flatten a sub-min-notional or sub-step residual sitting on top of matched
        inventory using a two-leg asymmetric reduce_only rebalance.

        Returns None when not executable, else `(made_progress, should_return, last_err)`.
        """
        cfg = self.cfg
        exposure_cap = cfg.risk.max_rebalance_exposure_usd
        if exposure_cap <= 0 or len(self.venues) != 2 or self._bg_tasks:
            return None
        locks = [self._vlock(key) for key in sorted(self.venues)]
        if any(lock.locked() for lock in locks):
            return None
        acquired = []
        try:
            for lock in locks:
                await lock.acquire()
                acquired.append(lock)
            if self.stop.is_set() or self._state_fault or self.has_pending_reconcile():
                return None
            cur_net = self._net_position()
            if abs(cur_net) <= cfg.net_tolerance_base:
                return None
            sgn = 1.0 if cur_net > 0 else -1.0
            is_sell_over = cur_net > 0
            over = next((v for v in self.venues.values() if v.position * sgn > 0), None)
            under = next((v for v in self.venues.values() if v.position * sgn < 0), None)
            if over is None or under is None:
                return None
            for v in (over, under):
                if (v.key in self._venue_down or not v.ready_to_trade()
                        or self._venue_limited(v) or not self._venue_rate_ok(v)
                        or not self._trade_book_fresh(v, reduce_only=True)):
                    return None

            slip = eff_slip_bps / 1e4
            over_ref = over.book.best_bid() if is_sell_over else over.book.best_ask()
            under_ref = under.book.best_ask() if is_sell_over else under.book.best_bid()
            if over_ref is None or under_ref is None:
                return None
            over_limit = (
                over.px_round(over_ref * (1.0 - slip), round_up=True)
                if is_sell_over else over.px_round(over_ref * (1.0 + slip), round_up=False)
            )
            under_limit = (
                under.px_round(under_ref * (1.0 + slip), round_up=False)
                if is_sell_over else under.px_round(under_ref * (1.0 - slip), round_up=True)
            )
            if over_limit <= 0.0 or under_limit <= 0.0:
                return None

            over_step = 10 ** -over.size_decimals
            under_step = 10 ** -under.size_decimals
            over_order_px = max(over_ref, over_limit)
            under_order_px = max(under_ref, under_limit)
            min_ntl_over = max(cfg.min_order_notional, over.min_quote)
            min_ntl_under = max(cfg.min_order_notional, under.min_quote)

            max_q_under = floor_step(
                min(abs(under.position), cfg.max_order_notional / under_order_px), under_step)
            max_q_over = floor_step(
                min(abs(over.position), cfg.max_order_notional / over_order_px), over_step)
            if max_q_under <= 0.0 or max_q_over <= 0.0:
                return None

            full_under = floor_step(abs(under.position), under_step)
            full_over = floor_step(abs(over.position), over_step)
            under_is_dust = (
                abs(under.position) * under_limit < min_ntl_under
                and math.isclose(max_q_under, full_under, rel_tol=0.0, abs_tol=1e-9)
            )
            if under_is_dust:
                q_under = full_under
                under_full = True
                q_over = min(max_q_over, floor_step(q_under + abs(cur_net), over_step))
                over_full = (
                    full_over >= over_step
                    and math.isclose(q_over, full_over, rel_tol=0.0, abs_tol=1e-9)
                )
            elif over_step > under_step + 1e-12:
                # Coarser `over` venue, finer `under` venue (e.g. over=0.01, under=0.001,
                # over=+0.50, under=-0.495, net=+0.005): anchor q_over on the coarser
                # over_step grid and absorb the fine residual into q_under = q_over - delta.
                raw_min_under = max(under.min_base, min_ntl_under / under_limit)
                min_q_under = round(math.ceil(raw_min_under / under_step - 1e-9) * under_step, 12)
                raw_min_over = max(over.min_base, min_ntl_over / over_limit)
                min_q_over = round(math.ceil(raw_min_over / over_step - 1e-9) * over_step, 12)
                if min_q_under > max_q_under + 1e-12 or min_q_over > max_q_over + 1e-12:
                    return None
                exact_q_over = max(
                    min_q_over,
                    round(math.ceil((min_q_under + abs(cur_net)) / over_step - 1e-9) * over_step, 12),
                )
                if exact_q_over <= max_q_over + 1e-12:
                    q_over = exact_q_over
                    q_under = floor_step(q_over - abs(cur_net), under_step)
                else:
                    q_over = max_q_over
                    q_under = max(min_q_under, floor_step(q_over - abs(cur_net), under_step))
                q_under = min(q_under, max_q_under)
                under_full = (
                    full_under >= under_step
                    and math.isclose(q_under, full_under, rel_tol=0.0, abs_tol=1e-9)
                )
                over_full = (
                    full_over >= over_step
                    and math.isclose(q_over, full_over, rel_tol=0.0, abs_tol=1e-9)
                )
            else:
                raw_min_under = max(under.min_base, min_ntl_under / under_limit)
                q_under = round(math.ceil(raw_min_under / under_step - 1e-9) * under_step, 12)
                under_full = False
                if q_under > max_q_under + 1e-12:
                    return None
                q_over = min(max_q_over, floor_step(q_under + abs(cur_net), over_step))
                over_full = (
                    full_over >= over_step
                    and math.isclose(q_over, full_over, rel_tol=0.0, abs_tol=1e-9)
                )
                if not over_full and q_over * over_limit < min_ntl_over:
                    raw_min_over = max(over.min_base, min_ntl_over / over_limit)
                    bumped_over = round(math.ceil(raw_min_over / over_step - 1e-9) * over_step, 12)
                    delta_target = floor_step(abs(cur_net), over_step)
                    if delta_target <= 0.0:
                        return None
                    bumped_under = floor_step(bumped_over - delta_target, under_step)
                    if (bumped_over <= max_q_over + 1e-12
                            and bumped_under <= max_q_under + 1e-12
                            and bumped_under >= under.min_base
                            and bumped_under * under_limit >= min_ntl_under):
                        q_over = bumped_over
                        q_under = bumped_under
                    else:
                        return None

            if q_over <= q_under + 1e-12:
                return None
            if not over_full and (q_over < over.min_base or q_over * over_limit < min_ntl_over):
                return None
            if not under_full and (q_under < under.min_base or q_under * under_limit < min_ntl_under):
                return None
            if (q_over * over_order_px > cfg.max_order_notional + 1e-9
                    or q_under * under_order_px > cfg.max_order_notional + 1e-9):
                return None

            # IOC legs can fill independently. The maximum absolute exposure
            # over all partial-fill combinations occurs at a corner, including
            # the case where only the smaller (under) leg fills.
            worst_net = max(abs(cur_net - sgn * q_over), abs(cur_net + sgn * q_under))
            mark = max(over.book.best_ask(), under.book.best_ask(),
                       over_order_px, under_order_px)
            if worst_net * mark > exposure_cap + 1e-9:
                self._skiplog("[HEDGE REBALANCE] worst single-leg exposure $%.4f exceeds limit $%.4f",
                              worst_net * mark, exposure_cap)
                return None

            over_buy = not is_sell_over
            under_buy = is_sell_over
            if not all(self._trade_book_fresh(v, reduce_only=True) for v in (over, under)):
                return None
            ids = self._stage_orders(
                ((over, over_buy, over_limit, q_over),
                 (under, under_buy, under_limit, q_under)),
                qty=q_over, purpose="hedge_rebalance", reduce_only=True,
                submission="attempted",
            )
            if ids is None:
                return (False, True, "state journal unavailable")
            # Validate both legs together. If either quote expired during
            # the journal write, neither order has been submitted.
            if not all(self._trade_book_fresh(v, reduce_only=True) for v in (over, under)):
                unsent = ((over, {"unresolved": False}), (under, {"unresolved": False}))
                if ids and not self._commit_orders(ids, unsent):
                    return (False, True, "state checkpoint failed")
                return None
            log.warning(
                "[HEDGE REBALANCE] net %+.6g — %s %s %.6g @%.6g | %s %s %.6g @%.6g (RO, slip<=%.1fbps)",
                cur_net,
                "BUY" if over_buy else "SELL", over.name, q_over, over_limit,
                "BUY" if under_buy else "SELL", under.name, q_under, under_limit,
                eff_slip_bps,
            )
            self.hedges += 1
            self._record_send(over)
            self._record_send(under)
            self._orders_in_flight += 1
            try:
                over_info, under_info = await asyncio.gather(
                    self._send_staged_order(over, ids, is_buy=over_buy, qty=q_over,
                                            limit_px=over_limit, reduce_only=True),
                    self._send_staged_order(under, ids, is_buy=under_buy, qty=q_under,
                                            limit_px=under_limit, reduce_only=True),
                )
            finally:
                self._orders_in_flight -= 1
            over.last_traded_ts = under.last_traded_ts = time.time()

            unresolved = False
            last_err = ""
            total_filled = 0.0
            for v, info, is_buy, leg_qty, bound in (
                (over, over_info, over_buy, q_over, over_limit),
                (under, under_info, under_buy, q_under, under_limit),
            ):
                if info.get("unresolved"):
                    unresolved = True
                    self._register_pending_order(
                        v, info.get("client_order_id"), is_buy=is_buy,
                        qty=leg_qty, limit_px=bound, known_filled=0.0,
                        purpose="hedge_rebalance", reduce_only=True, persist=False,
                    )
                    continue
                if info.get("err"):
                    last_err = str(info["err"])
                    log.error("[HEDGE REBALANCE] %s: %s", v.name, last_err)
                    if last_err.startswith("RATE_LIMITED"):
                        self._mark_limited(v)
                    self._reconcile_evt.set()
                    continue
                fill = float(info.get("filled_base") or 0.0)
                cid = info.get("client_order_id")
                okey = f"{v.key}:{cid}" if cid else ""
                if fill > 0.0 and (not okey or self._mark_order_settled(okey)):
                    total_filled += fill
                    v.position += fill if is_buy else -fill
                    px = info.get("avg_px") or bound
                    if info.get("fee_bps") is not None and (
                        v.key == "entropy" or getattr(v, "kind", None) == "hl"
                    ):
                        self._handle_fee_observation(float(info["fee_bps"]), venue_key=v.key)
                    fee_usd = (
                        float(info["fee_usd"])
                        if info.get("fee_usd") is not None
                        else fill * px * self.effective_fee_bps(v) / 1e4
                    )
                    v.cash += (-fill * px if is_buy else fill * px) - fee_usd
                    v.volume_usd += fill * px
                    if ids and v.key in ids and f"{v.key}:{ids[v.key]}" in self._pending_orders:
                        self._pending_orders[f"{v.key}:{ids[v.key]}"]["known_filled"] = fill
                    mid = v.book.mid()
                    if mid is not None:
                        slip_pnl = (
                            fill * (mid - px) if is_buy else fill * (px - mid)
                        ) - fee_usd
                        self._ensure_daily_breakdown()
                        self.daily_pnl_breakdown["hedge_pnl_usd"] += slip_pnl
                        tripped, reason = self.circuit_breaker.record_trade_pnl(
                            slip_pnl, mtm_pnl_usd=self.daily_mtm_pnl(), count_streak=False,
                        )
                        self._handle_loss_breaker(tripped, reason)

            if not unresolved:
                self._sync_inv_basis_to_positions()
            if not self._commit_orders(ids, ((over, over_info), (under, under_info))):
                return (False, True, "state checkpoint failed")
            if unresolved:
                return (False, True, "unresolved")

            new_net = self._net_position()
            reduced = abs(new_net) < abs(cur_net) - 1e-12
            if reduced:
                self._hedge_attempt_count = 0
                return (True, False, last_err)
            self._hedge_attempt_count += 1
            if not last_err and total_filled <= 0.0:
                last_err = f"zero fill ({over_info.get('status')}/{under_info.get('status')})"
            return (False, False, last_err)
        finally:
            for lk in reversed(acquired):
                lk.release()

    # --------------------------------------------------- P0-1 pending reconcile

    async def _reconcile_pending_orders(self) -> bool:
        """Settle pending fills only when both position snapshots agree."""
        if self._orders_in_flight or self._bg_tasks:
            return False
        if not self.has_pending_reconcile():
            return True

        expected = {key: v.position for key, v in self.venues.items()}
        settlements = []
        unsent = []
        expired_not_found = []
        for key, rec in self._pending_orders.items():
            venue = self.venues.get(rec["venue_key"])
            if venue is None:
                return False
            if rec.get("submission") == "prepared":
                # Unlike an exchange 'not found', this local marker proves
                # that submission was never attempted.
                if rec.get("known_filled") != 0.0:
                    log.warning("[PENDING] inconsistent unsubmitted order %s; staying paused", key)
                    return False
                unsent.append(key)
                continue
            if not hasattr(venue, "query_order"):
                return False
            result = await venue.query_order(rec["client_order_id"])
            if not isinstance(result, dict) or result.get("unresolved") is not False:
                status = result.get("status") if isinstance(result, dict) else "invalid-response"
                age = max(0.0, time.time() - float(rec.get("created_ts") or time.time()))
                known = float(rec.get("known_filled") or 0.0)
                if status == "not-found":
                    nf_count = self._pending_not_found_counts.get(key, 0) + 1
                    self._pending_not_found_counts[key] = nf_count
                    expire_sec = self.cfg.pending_not_found_expire_sec
                    if (expire_sec > 0.0 and known == 0.0
                            and age >= expire_sec and nf_count >= 3):
                        # IOC order never appeared on exchange past expiry window;
                        # tentatively treat as zero-fill ONLY if both on-chain
                        # positions strictly agree below.
                        expired_not_found.append(key)
                        settlements.append((
                            key, venue, rec,
                            {"status": "expired-not-found", "outcome": "canceled",
                             "filled_base": 0.0, "avg_px": None, "unresolved": False},
                            0.0,
                        ))
                        continue
                else:
                    self._pending_not_found_counts.pop(key, None)
                log.warning("[PENDING] %s unresolved (%s)", key, status)
                cur_net = self._net_position()
                if abs(cur_net) > self.cfg.net_tolerance_base and age >= 15.0:
                    self.notifier.notify(
                        f"pending_unhedged:{key}",
                        f"🚨 UNHEDGED EXPOSURE FROZEN: local net={cur_net:+.6g}, "
                        f"pending order {key} still {status} after {age:.0f}s — "
                        f"verify exchange fills & positions manually",
                    )
                return False
            self._pending_not_found_counts.pop(key, None)
            try:
                total = float(result["filled_base"])
            except (KeyError, TypeError, ValueError):
                log.warning("[PENDING] missing or invalid confirmed fill for %s; staying paused", key)
                return False
            known = float(rec.get("known_filled") or 0.0)
            if not math.isfinite(total) or total < known - 1e-9 or total > rec["qty"] + 1e-9:
                log.warning("[PENDING] inconsistent fill size for %s; staying paused", key)
                return False
            additional = max(total - known, 0.0)
            expected[venue.key] += additional if rec["is_buy"] else -additional
            settlements.append((key, venue, rec, result, additional))

        for key, venue in self.venues.items():
            async with self._vlock(key):
                try:
                    position = await venue.fetch_position()
                except Exception as exc:
                    log.warning("[PENDING] position query failed on %s (%s); staying paused",
                                venue.name, type(exc).__name__)
                    return False
            epsilon = max(1e-9, 10 ** -venue.size_decimals * 1e-4)
            if not math.isfinite(position) or not math.isclose(
                    position, expected[key], rel_tol=0.0, abs_tol=epsilon):
                log.warning("[PENDING] %s position %.9g != expected %.9g; staying paused",
                            venue.name, position, expected[key])
                self.notifier.notify(
                    f"pending_mismatch:{key}",
                    f"{venue.name} position differs from confirmed fills; new orders paused",
                )
                return False

        # Commit the ledger and clear the journal together. A crash before the
        # replacement replays from the previous ledger with its pending IDs.
        pre_positions = {k: v.position for k, v in self.venues.items()}
        self._ensure_daily_breakdown()
        settled_arb_legs: List[Tuple[Any, Dict[str, Any], float, float, float]] = []
        for key, venue, rec, result, additional in settlements:
            if self._mark_order_settled(key) and additional > 0:
                px = float(result.get("avg_px") or rec["limit_px"])
                if result.get("fee_bps") is not None and (
                    venue.key == "entropy" or getattr(venue, "kind", None) == "hl"
                ):
                    self._handle_fee_observation(float(result["fee_bps"]), venue_key=venue.key)
                if result.get("fee_usd") is not None:
                    fee_usd = result["fee_usd"] * additional / result["filled_base"]
                    self.daily_pnl_breakdown["actual_fee_usd"] += fee_usd
                else:
                    fee_usd = additional * px * self.effective_fee_bps(venue) / 1e4
                    self.daily_pnl_breakdown["estimated_fee_usd"] += fee_usd
                venue.cash += (-1 if rec["is_buy"] else 1) * additional * px - fee_usd
                venue.volume_usd += additional * px
                if rec.get("purpose") == "arb":
                    settled_arb_legs.append((venue, rec, additional, px, fee_usd / additional))
            venue.position = expected[venue.key]

        # Attribute late-settled arb pairs through the exact same inventory cost-basis
        # attribution as synchronous fills so round-trip PnL & consecutive losses stay exact.
        attributed_pair_ids: set = set()
        by_pair_id: Dict[str, List[Tuple[Any, Dict[str, Any], float, float, float]]] = {}
        for item in settled_arb_legs:
            pid = item[1].get("pair_id")
            if pid:
                by_pair_id.setdefault(pid, []).append(item)

        def _post_late_pair(buy_v, sell_v, m_qty, b_px, s_px, b_fee_u, s_fee_u, pre_e, pre_h):
            if m_qty <= 0.0:
                return
            self._sync_inv_basis_to_positions(pre_e, pre_h)
            pnl_delta, streak_pnl = self._attribute_pair_pnl(
                buy_v, sell_v, m_qty, b_px, s_px, b_fee_u, s_fee_u)
            gross_spread = m_qty * (s_px - b_px)
            fill_edge = gross_spread - m_qty * (b_fee_u + s_fee_u)
            self.total_fill_edge += fill_edge
            self.daily_pnl_breakdown["spread_pnl_usd"] += gross_spread
            tripped, reason = self.circuit_breaker.record_trade_pnl(
                pnl_delta, mtm_pnl_usd=self.daily_mtm_pnl(), streak_pnl_usd=streak_pnl)
            self._handle_loss_breaker(tripped, reason)

        for pid, items in by_pair_id.items():
            if len(items) == 2 and items[0][1]["is_buy"] != items[1][1]["is_buy"]:
                b_item = items[0] if items[0][1]["is_buy"] else items[1]
                s_item = items[1] if items[0][1]["is_buy"] else items[0]
                m_qty = min(b_item[2], s_item[2])
                _post_late_pair(
                    b_item[0], s_item[0], m_qty, b_item[3], s_item[3],
                    b_item[4], s_item[4],
                    pre_positions.get("entropy", 0.0), pre_positions.get("hedge", 0.0),
                )
                attributed_pair_ids.add(pid)

        for venue, rec, additional, px, fee_unit in settled_arb_legs:
            pid = rec.get("pair_id")
            if pid and pid in attributed_pair_ids:
                continue
            pctx = rec.get("pair_ctx")
            if not isinstance(pctx, dict):
                continue
            sib = self.venues.get(pctx.get("sibling_venue"))
            if sib is None or bool(pctx.get("sibling_is_buy")) == bool(rec["is_buy"]):
                continue
            sib_rem = max(
                0.0,
                float(pctx.get("sibling_filled") or 0.0) - float(rec.get("known_filled") or 0.0),
            )
            m_qty = min(additional, sib_rem)
            if m_qty <= 0.0:
                continue
            # Pre-pair positions exclude the sibling's already-posted fill so clamping
            # matches the state right before this arb pair executed.
            sib_signed = sib_rem * (1.0 if pctx["sibling_is_buy"] else -1.0)
            pre_e = pre_positions.get("entropy", 0.0) - (sib_signed if sib.key == "entropy" else 0.0)
            pre_h = pre_positions.get("hedge", 0.0) - (sib_signed if sib.key == "hedge" else 0.0)
            if rec["is_buy"]:
                _post_late_pair(
                    venue, sib, m_qty, px, float(pctx["sibling_px"]),
                    fee_unit, float(pctx.get("sibling_fee_unit") or 0.0), pre_e, pre_h,
                )
            else:
                _post_late_pair(
                    sib, venue, m_qty, float(pctx["sibling_px"]), px,
                    float(pctx.get("sibling_fee_unit") or 0.0), fee_unit, pre_e, pre_h,
                )

        self._sync_inv_basis_to_positions()
        self._pending_orders.clear()
        self._pending_not_found_counts.clear()
        self._reconcile_required = False
        if self.state_store is not None and not self.save_state():
            self._stop_on_state_error("could not checkpoint reconciled orders")
            return False
        self._update_evt.set()
        if unsent:
            log.info("[PENDING] cleared locally prepared, never submitted orders: %s", ", ".join(unsent))
        if expired_not_found:
            log.warning("[PENDING] settled expired not-found IOC order(s) as zero-fill after "
                        "confirming both on-chain positions: %s", ", ".join(expired_not_found))
        log.info("[PENDING] orders and both positions reconciled")
        self.notifier.notify(
            "pending_order:resolved", "Orders and both positions reconciled", force=True)
        return True

    # --------------------------------------------------- background monitors

    async def _check_entropy_fee_once(self) -> None:
        if self.entropy is None or not hasattr(self.entropy, "fetch_taker_fee_bps"):
            return
        try:
            fee_bps = await self.entropy.fetch_taker_fee_bps()
        except Exception as e:
            log.warning("[ENTROPY] taker fee check exception: %r", e)
            fee_bps = None
        self._handle_fee_observation(fee_bps, venue_key="entropy")

    async def _check_all_fees_once(self) -> None:
        await self._check_entropy_fee_once()
        if (self.hedge is not None and getattr(self.hedge, "kind", None) == "hl"
                and hasattr(self.hedge, "fetch_taker_fee_bps")):
            try:
                fee_bps = await self.hedge.fetch_taker_fee_bps()
            except Exception as e:
                log.warning("[%s] taker fee check exception: %r", self.hedge.name, e)
                fee_bps = None
            self._handle_fee_observation(fee_bps, venue_key="hedge")

    async def _fee_monitor_loop(self) -> None:
        interval = self.cfg.risk.fee_check_interval_sec
        while not self.stop.is_set():
            try:
                await self._check_all_fees_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("fee monitor error: %r", e)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                pass

    async def _poll_funding_once(self) -> None:
        e_rate: Optional[float] = None
        h_rate: Optional[float] = None
        if hasattr(self.entropy, "fetch_funding_rate_hourly"):
            e_rate = await self.entropy.fetch_funding_rate_hourly()
        if hasattr(self.hedge, "fetch_funding_rate_hourly"):
            h_rate = await self.hedge.fetch_funding_rate_hourly()
        ok = self.funding_skew.update_rates(e_rate, h_rate)
        if not ok:
            log.warning(
                "[funding] degraded (ENTROPY=%s %s=%s) -> skew forced to 0.0 bps",
                e_rate, self.hedge.name, h_rate,
            )
            self.notifier.notify(
                "funding_degraded",
                f"⚠️ Funding rate fetch incomplete (ENTROPY={e_rate}, {self.hedge.name}={h_rate}) "
                f"— funding skew forced to 0.0 bps",
            )
            return
        log.info(
            "[funding] ENTROPY=%.4fbps/h %s=%.4fbps/h -> skew=%+.2fbps (hold=%.1fh)",
            e_rate * 1e4, self.hedge.name, h_rate * 1e4,
            self.funding_skew.skew_bps(), self.funding_skew.expected_hold_hours,
        )

    async def _funding_loop(self) -> None:
        interval = self.cfg.funding_skew.poll_interval_sec
        while not self.stop.is_set():
            try:
                await self._poll_funding_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("funding poll error: %r", e)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                pass

    async def _check_margin_once(self) -> None:
        """P1-3: Poll account equity & free margin; pause new risk when below floor OR unknown."""
        for v in self.venues.values():
            free_val: Optional[float] = None
            try:
                got = await v.fetch_equity()
                if got is not None:
                    v.equity, v.free = got
                    if v.start_equity is None:
                        v.start_equity = v.equity
                    free_val = v.free
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.debug("[%s] equity poll failed: %r", v.name, e)
                free_val = None
            tripped, recovered, msg = self.circuit_breaker.update_available_margin(
                v.name, free_val)
            if tripped:
                log.critical("MARGIN BREAKER -> REDUCE-ONLY: %s", msg)
                self.notifier.notify(
                    "circuit_breaker:margin",
                    f"🚨 {msg} — switched to REDUCE-ONLY",
                    force=True,
                )
            elif recovered:
                log.warning("MARGIN BREAKER RECOVERED: %s", msg)
                self.notifier.notify(
                    "circuit_breaker:margin",
                    f"✅ {msg}",
                    force=True,
                )

    # --------------------------------------------------- reconcile / status

    RECONCILE_GRACE_SEC = 5.0

    async def _reconcile_positions(self, hedge: bool,
                                   strict: bool = False) -> None:
        if not strict and self.has_pending_reconcile():
            cleared = await self._reconcile_pending_orders()
            if cleared and hedge:
                await self._maybe_hedge()
            return
        now = time.time()
        vs = []
        for v in self.venues.values():
            if now - v.last_traded_ts <= self.RECONCILE_GRACE_SEC:
                continue
            if v.key in self._venue_down \
                    and now < self._venue_probe_at.get(v.key, 0.0):
                continue
            vs.append(v)
        if not vs:
            return
        got = await asyncio.gather(
            *(self._reconcile_venue(v, strict) for v in vs),
            return_exceptions=True)
        for r in got:
            if isinstance(r, BaseException):
                raise r
        if hedge:
            await self._maybe_hedge()

    async def _reconcile_venue(self, v, strict: bool) -> None:
        async with self._vlock(v.key):
            now = time.time()
            if not strict and now - v.last_traded_ts <= self.RECONCILE_GRACE_SEC:
                return
            try:
                r = await v.fetch_position()
                if not math.isfinite(r):
                    raise ValueError("non-finite position")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if strict:
                    raise RuntimeError(
                        f"[{v.name}] cannot fetch starting position: {e!r}")
                n = self._venue_fetch_fails.get(v.key, 0) + 1
                self._venue_fetch_fails[v.key] = n
                self._venue_probe_at[v.key] = now + self.cfg.venue_probe_sec
                if n >= 3 and v.key not in self._venue_down:
                    self._venue_down[v.key] = now
                    log.critical("[%s] API unreachable (%d attempts) — "
                                 "trading PAUSED; probing every %.0fs until "
                                 "it recovers", v.name, n,
                                 self.cfg.venue_probe_sec)
                    self.notifier.notify(
                        f"venue_down:{v.name}",
                        f"🚨 [{v.name}] API unreachable ({n} attempts) — trading PAUSED",
                    )
                elif v.key not in self._venue_down:
                    log.warning("[%s] position fetch failed (%d): %r",
                                v.name, n, e)
                return
            if strict:
                epsilon = max(1e-9, 10 ** -v.size_decimals * 1e-4)
                if not math.isclose(r, v.position, rel_tol=0.0, abs_tol=epsilon):
                    if self._restored_ledger:
                        detail = f"saved position {v.position:+.9g} differs from exchange position {r:+.9g}"
                    else:
                        detail = f"exchange position {r:+.9g} exists with no saved ledger"
                    raise RuntimeError(
                        f"[{v.name}] {detail}; startup blocked. Check both accounts "
                        "and resolve the discrepancy before restarting. Preserve the state file.")
                return
            if v.key in self._venue_down:
                outage = now - self._venue_down.pop(v.key)
                log.warning("[%s] API recovered after %.0fs outage — "
                            "trading RESUMED", v.name, outage)
                self.notifier.notify(
                    f"venue_up:{v.name}",
                    f"✅ [{v.name}] API recovered after {outage:.0f}s — trading RESUMED",
                )
                self._update_evt.set()
            self._venue_fetch_fails[v.key] = 0
            delta = r - v.position
            if abs(delta) > 1e-12:
                if abs(delta) > self.cfg.net_tolerance_base:
                    msg = (f"[{v.name}] position drift: chain {r:+.6g} vs "
                           f"local {v.position:+.6g} (delta {delta:+.6g})")
                    log.warning("%s — adopting chain", msg)
                    if self.cfg.risk.reconcile_drift_reduce_only:
                        if self.circuit_breaker.trigger_reconcile_drift(msg):
                            self.notifier.notify(
                                "circuit_breaker:reconcile_drift",
                                f"🚨 {msg} — synced chain & switched to REDUCE-ONLY",
                                force=True,
                            )
                mid = v.book.mid()
                if mid is not None:
                    v.cash -= delta * mid
                v.position = r
                self._sync_inv_basis_to_positions()
                if self.state_store is not None and not self.save_state():
                    self._stop_on_state_error("could not checkpoint reconciled position drift")

    async def _reconcile_loop(self) -> None:
        while not self.stop.is_set():
            timeout = 2.0 if self.has_pending_reconcile() else self.cfg.reconcile_sec
            try:
                await asyncio.wait_for(self._reconcile_evt.wait(),
                                       timeout=timeout)
                self._reconcile_evt.clear()
                if not self.has_pending_reconcile():
                    await asyncio.sleep(1.0)
            except asyncio.TimeoutError:
                pass
            if self.stop.is_set():
                break
            try:
                await self._reconcile_positions(hedge=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("reconcile failed")

    async def _balance_loop(self) -> None:
        while not self.stop.is_set():
            await self._check_margin_once()
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=BALANCE_POLL_SEC)
            except asyncio.TimeoutError:
                pass

    async def _http_keepalive_loop(self) -> None:
        if self.cfg.http_keepalive_sec <= 0:
            return
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(),
                                       timeout=self.cfg.http_keepalive_sec)
                return
            except asyncio.TimeoutError:
                pass
            await asyncio.gather(*(v.warm_http() for v in self.venues.values()),
                                 return_exceptions=True)

    # --------------------------------------------------- P1-4 PnL & Daily MTM

    def _ensure_daily_breakdown(self, now: Optional[float] = None) -> str:
        today = self.circuit_breaker.day_for(now)
        if self.daily_pnl_breakdown.get("day_key") != today:
            self.daily_pnl_breakdown = self._fresh_daily_breakdown(today)
        return today

    def _trading_day_start(self, day: str) -> float:
        timezone = ZoneInfo(self.cfg.session_filter.timezone)
        return datetime.fromisoformat(day).replace(tzinfo=timezone).timestamp()

    async def _poll_transfer_history(self) -> None:
        now = time.time()
        day = self.circuit_breaker.day_for(now)
        start = self._trading_day_start(day)
        # A shared Hyperliquid account has one transfer history across both DEXs.
        accounts = [v for v in self.venues.values() if getattr(v, "include_core_equity", True)]
        values = await asyncio.gather(
            *(v.fetch_external_transfers(start, now) for v in accounts),
            return_exceptions=True,
        )
        if not values or any(not isinstance(x, (int, float)) or not math.isfinite(x)
                             for x in values):
            log.warning("Daily transfer history incomplete; retaining the last verified amounts")
            return
        if self.circuit_breaker.day_for() != day:
            return
        self._ensure_daily_breakdown(now)
        net = sum(values)
        if (self.daily_pnl_breakdown["transfer_status"] != "confirmed"
                or self.daily_pnl_breakdown["transfers_usd"] != net):
            # This is a separate account cashflow series. Trading MTM uses
            # locally posted fills, so neither the cash nor its baseline moves.
            self.daily_pnl_breakdown["transfers_usd"] = net
            self.daily_pnl_breakdown["transfer_status"] = "confirmed"
            if self.state_store is not None and not self.save_state():
                self._stop_on_state_error("could not checkpoint daily transfers")

    async def _transfer_history_loop(self) -> None:
        while not self.stop.is_set():
            await self._poll_transfer_history()
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=300.0)
            except asyncio.TimeoutError:
                pass

    async def _poll_funding_history(self, now: Optional[float] = None) -> None:
        live_clock = now is None
        now = time.time() if live_clock else now
        day = self.circuit_breaker.day_for(now)
        start = self._trading_day_start(day)
        values = await asyncio.gather(
            *(v.fetch_funding_pnl(start, now) for v in self.venues.values()),
            return_exceptions=True,
        )
        if len(values) != 2 or any(not isinstance(x, (int, float)) or not math.isfinite(x)
                                   for x in values):
            log.warning("Daily funding history incomplete; retaining the last verified amounts")
            return
        if live_clock and self.circuit_breaker.day_for() != day:
            # A slow response across the day boundary belongs to the previous day.
            return
        self._ensure_daily_breakdown(now)
        previous = self.daily_pnl_breakdown["funding_pnl_usd"] or 0.0
        change = sum(values) - previous
        if change or self.daily_pnl_breakdown["funding_status"] != "confirmed":
            self.record_funding_pnl(change, now=now)
            if self.state_store is not None and not self.save_state():
                self._stop_on_state_error("could not checkpoint daily funding")

    async def _funding_history_loop(self) -> None:
        while not self.stop.is_set():
            await self._poll_funding_history()
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                pass

    def record_funding_pnl(self, amount_usd: float,
                           now: Optional[float] = None) -> Tuple[bool, str]:
        """P1-4: Record confirmed funding cashflow into daily PnL & breaker."""
        self._ensure_daily_breakdown(now)
        cur = self.daily_pnl_breakdown["funding_pnl_usd"]
        self.daily_pnl_breakdown["funding_pnl_usd"] = (0.0 if cur is None else cur) + float(amount_usd)
        self.daily_pnl_breakdown["funding_status"] = "confirmed"
        tripped, reason = self.circuit_breaker.record_trade_pnl(
            float(amount_usd), mtm_pnl_usd=self.daily_mtm_pnl(now=now),
            now=now, count_streak=False,
        )
        self._handle_loss_breaker(tripped, reason)
        return tripped, reason

    def record_transfer(self, amount_usd: float,
                        now: Optional[float] = None) -> None:
        """P1-4: Record external deposit (+)/withdrawal (-) so transfers do not
        distort daily trading MTM loss."""
        self._ensure_daily_breakdown(now)
        self.daily_pnl_breakdown["transfers_usd"] += float(amount_usd)
        self.daily_pnl_breakdown["transfer_status"] = "confirmed"
        if self._daily_mtm_baseline is not None:
            self._daily_mtm_baseline += float(amount_usd)

    def _portfolio_mtm(self) -> Optional[float]:
        total = 0.0
        for v in self.venues.values():
            m = v.book.mid()
            if m is None:
                return None
            total += v.cash + v.position * m
        return total

    def daily_mtm_pnl(self, now: Optional[float] = None) -> Optional[float]:
        """P1-4: Trading-day-aligned MTM PnL in configured timezone.
        Resets baseline automatically at day rollover so prior-day losses never
        re-trigger today's daily loss circuit breaker."""
        total = self._portfolio_mtm()
        if total is None:
            return None
        today = self.circuit_breaker.day_for(now)
        if self._daily_mtm_day != today or self._daily_mtm_baseline is None:
            self._daily_mtm_day = today
            self._daily_mtm_baseline = total
        self._ensure_daily_breakdown(now)
        funding = self.daily_pnl_breakdown["funding_pnl_usd"] or 0.0
        return total - self._daily_mtm_baseline + funding

    def account_delta(self) -> Optional[float]:
        total = 0.0
        for v in self.venues.values():
            if v.equity is None or v.start_equity is None:
                return None
            total += v.equity - v.start_equity
        return total

    def session_pnl(self) -> Optional[float]:
        total = self._portfolio_mtm()
        if total is None:
            return None
        if self._mtm_baseline is None:
            self._mtm_baseline = total
        return total - self._mtm_baseline

    def premium_bps(self) -> Optional[float]:
        em, hm = self.entropy.book.mid(), self.hedge.book.mid()
        if not (em and hm):
            return None
        return (em / hm - 1.0) * 1e4

    async def _status_loop(self) -> None:
        cfg = self.cfg
        while not self.stop.is_set():
            try:
                await asyncio.sleep(cfg.status_interval_sec)
            except asyncio.CancelledError:
                raise
            age_limit = self.book_age_limit(
                reduce_only=not self.record_only and self.is_reduce_only())
            books = " | ".join(
                f"{v.name} {v.book.best_bid() or '—'}/{v.book.best_ask() or '—'}"
                + ("" if v.book.is_fresh(cfg.staleness_sec, max_book_age_sec=age_limit)
                   else " STALE")
                + (" RATE-LTD" if self._venue_limited(v) else "")
                + (" DOWN" if v.key in self._venue_down else "")
                for v in self.venues.values())
            prem = self.premium_bps()
            prem_s = f"{prem:+.2f}" if prem is not None else "—"
            pos = " ".join(f"{v.name} {v.position:+.6g}"
                           for v in self.venues.values())
            net = sum(v.position for v in self.venues.values())
            pnl = self.session_pnl()
            rec = (f" | rec {self.recorder.rows_written} rows"
                   if self.recorder else "")
            mid = self.current_midline_bps()
            ro_tag = " [REDUCE-ONLY]" if self.is_reduce_only() else ""
            pend_tag = f" [PENDING:{len(self._pending_orders)}]" if self.has_pending_reconcile() else ""
            log.info("[status] %s | prem %s bps (mid %+.2f band %+.2f..%+.2f) | pos %s "
                     "net %+.6g | trades %d hedges %d | MTM %s expEdge $%.4f "
                     "fillEdge $%.4f%s%s%s%s",
                     books, prem_s, mid, mid - cfg.lower_bps,
                     mid + cfg.upper_bps, pos, net, self.trades,
                     self.hedges,
                     f"${pnl:+.4f}" if pnl is not None else "—",
                     self.total_exp_edge, self.total_fill_edge, rec,
                     ro_tag, pend_tag, " *** HALTED ***" if self.halted else "")

    def _log_csv(self, direction, buy, sell, plan: ArbPlan, ok: bool, bfill,
                 sfill, bstatus, sstatus, fill_edge, inv_bps) -> None:
        try:
            path = self.cfg.trades_csv
            d = os.path.dirname(path)
            if d:
                os.makedirs(d, exist_ok=True)
            if os.path.exists(path):
                with open(path) as fh0:
                    if fh0.readline().strip() != ",".join(CSV_HEADER):
                        os.replace(path, path + ".old")
            new = not os.path.exists(path)
            with open(path, "a", newline="") as fh:
                w = csv.writer(fh)
                if new:
                    w.writerow(CSV_HEADER)
                w.writerow([f"{time.time():.3f}",
                            direction, buy.name, sell.name, f"{plan.qty:.8g}",
                            plan.buy_limit, plan.sell_limit,
                            f"{plan.buy_notional:.2f}", f"{plan.sell_notional:.2f}",
                            f"{plan.exp_edge_usd:.4f}", f"{plan.gross_edge_usd:.4f}",
                            f"{plan.marginal_premium_bps:.3f}",
                            f"{self.current_midline_bps():.3f}",
                            f"{inv_bps:.3f}", int(ok), f"{bfill:.8g}",
                            f"{sfill:.8g}", bstatus, sstatus, f"{fill_edge:.4f}"])
        except Exception:
            log.exception("csv write failed")
