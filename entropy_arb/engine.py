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
from .state import InstanceLock, StateStore
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
    ) -> str:
        cid = str(client_order_id or f"unknown-{int(time.time() * 1000)}")
        okey = f"{venue.key}:{cid}"
        self._pending_orders[okey] = {
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
        """Record all client IDs in one durable write before submitting any leg."""
        ids = {}
        staged = {}
        for venue, is_buy, limit_px in orders:
            if not hasattr(venue, "reserve_order_id"):
                continue
            cid = venue.reserve_order_id()
            ids[venue.key] = cid
            staged[f"{venue.key}:{cid}"] = {
                "venue_key": venue.key,
                "client_order_id": cid,
                "is_buy": is_buy,
                "qty": qty,
                "limit_px": limit_px,
                "known_filled": 0.0,
                "purpose": purpose,
                "reduce_only": reduce_only,
                "submission": submission,
                "created_ts": time.time(),
            }
        self._pending_orders.update(staged)
        if ids and not self.save_state():
            # No order has been sent. If the write partially succeeded, keep
            # the engine halted and allow the next successful save to clear it.
            for key, cid in ids.items():
                self._pending_orders.pop(f"{key}:{cid}", None)
            self._stop_on_state_error("could not persist orders before submission")
            return None
        return ids

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
        if cb_ok and self.circuit_breaker.should_halt:
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
        """Acquire exclusive file locks keyed on every venue's account+market identity.

        Prevents two live bots (e.g. --hedge lighter and --hedge lighter-rh for the
        same symbol) from concurrently trading the same shared Entropy io:<symbol>
        account position.
        """
        if self.record_only:
            return
        identities = sorted({
            v.state_identity() for v in self.venues.values()
            if hasattr(v, "state_identity")
        })
        acquired: List[InstanceLock] = []
        try:
            for ident in identities:
                digest = hashlib.sha256(ident.encode("utf-8")).hexdigest()[:16]
                path = os.path.join(self.cfg.persistence.state_dir, f"venue_{digest}.lock")
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
                 "lat_buf=%.2fbps fees=%.2f+%.2f step=%g min_ntl=$%g",
                 self.entropy.conf.symbol, self.hedge.name,
                 self.hedge.conf.symbol, cfg.midline_bps, cfg.lower_bps,
                 cfg.upper_bps, cfg.latency_buffer_bps,
                 self.entropy.fee_bps, self.hedge.fee_bps,
                 self._step, self._min_notional)

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
        """
        if not reduce_only or self.cfg.reduce_only_exit_threshold_bps is None:
            return self._eff_threshold(buy, sell, now=now, include_latency_buffer=True)
        threshold = self.cfg.reduce_only_exit_threshold_bps
        if not self.cfg.risk.reduce_only_ignore_session_buffer:
            threshold += self.session_state(now).spread_buffer_bps
        return threshold

    def _required_spread_bps(self, buy, sell, *, reduce_only: bool,
                             now: Optional[float] = None) -> float:
        """Post-slippage worst-case floor for _order_price_bounds & _worst_case_check.

        Excludes latency_buffer_bps on normal entries so the latency buffer
        provides actual slippage room at the entry boundary rather than squeezing
        limit slippage to 0 bps.
        """
        if not reduce_only:
            return self._eff_threshold(buy, sell, now=now, include_latency_buffer=False)
        threshold = self.cfg.reduce_only_min_worst_bps
        if not self.cfg.risk.reduce_only_ignore_session_buffer:
            threshold += self.session_state(now).spread_buffer_bps
        return threshold

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
        """Round limits inward while spending only the spread above the required bps."""
        slip = self.cfg.leg_slippage_bps / 1e4
        if not 0.0 <= slip < 1.0:
            return None
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

    def book_age_limit(self) -> float:
        limit = self.cfg.risk.max_book_age_sec
        return self.cfg.staleness_sec if self.record_only and limit <= 0.0 else limit

    def _check_book_age_guard(self, now_mono: Optional[float] = None, *,
                              reconnect: bool = False) -> bool:
        limit = self.book_age_limit()
        if limit <= 0.0:
            return True
        ok = True
        for v in self.venues.values():
            has_quotes = v.book.has_usable_quotes()
            age = v.book.age_mono(now_mono)
            if not has_quotes or age is None or age > limit:
                ok = False
                if not reconnect:
                    continue
                age_s = "no usable quote" if not has_quotes or age is None else f"{age:.1f}s"
                self._skiplog("[%s] stale book (%s, age limit %.1fs)",
                              v.name, age_s, limit)
                self.notifier.notify(
                    f"stale_book:{v.name}",
                    f"⚠️ Stale book on [{v.name}] ({age_s}, age limit {limit:.1f}s)",
                )
                if hasattr(v, "request_reconnect"):
                    v.request_reconnect()
        return ok

    async def _book_watchdog(self) -> None:
        """Check quote age even when neither feed emits an order-book event."""
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                self._check_book_age_guard(reconnect=True)

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
        if self.halted or self.has_pending_reconcile():
            self._reset_signal()
            return None

        reduce_only_hint = self.is_reduce_only(now)
        if not self._check_book_age_guard(now_mono) and not reduce_only_hint:
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
            and self.entropy.book.is_fresh(cfg.staleness_sec)
            and self.hedge.book.is_fresh(cfg.staleness_sec)
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
        signal_time = time.monotonic() if now_mono is None else now_mono

        best = None
        for buy, sell, dkey in ((self.hedge, self.entropy, "sell_entropy"),
                                (self.entropy, self.hedge, "buy_entropy")):
            if not (buy.book.is_fresh(cfg.staleness_sec)
                    and sell.book.is_fresh(cfg.staleness_sec)):
                self._reset_signal(dkey)
                continue
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
                self._schedule_poke(min(cfg.premium_persist_sec, MAX_SIGNAL_GAP_SEC / 2))
                continue
            remaining = cfg.premium_persist_sec - (signal_time - armed)
            if remaining > 0.0:
                self._schedule_poke(min(remaining, MAX_SIGNAL_GAP_SEC / 2))
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

    def _midline_adjusted_edge_usd(
        self, buy, sell, matched: float, buy_px: float, sell_px: float,
        buy_fee_unit: float, sell_fee_unit: float,
    ) -> float:
        """Half-turn net edge measured relative to the current midline.

        Equals exact cash fill_edge when midline_bps == 0, and prevents
        persistent non-zero basis (e.g. midline = -25 bps) from counting normal
        profitable entries as consecutive losses. Entry + exit sum identically
        to realized round-trip PnL.
        """
        mid = self.current_midline_bps() / 1e4
        if sell.key == "entropy":
            gross = matched * (sell_px - buy_px * (1.0 + mid))
        else:
            gross = matched * (sell_px * (1.0 + mid) - buy_px)
        return gross - matched * (buy_fee_unit + sell_fee_unit)

    async def _verify_fill_fee_bg(
        self, v, *, order_id, client_order_id, filled: float, est_fee_usd: float,
    ) -> None:
        """Background fee verification used on imbalanced fills so hedging is never delayed."""
        try:
            verified = await v.fetch_fill_fee(
                order_id=order_id,
                client_order_id=client_order_id,
                coin=getattr(v, "coin", None),
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug("[%s] async fill fee verify failed: %r", v.name, e)
            return
        if verified is None or not math.isclose(
            verified.get("filled_base", 0.0), filled, rel_tol=0.0, abs_tol=1e-9
        ):
            return
        actual_fee = float(verified["fee_usd"])
        delta_fee = actual_fee - est_fee_usd
        v.cash -= delta_fee
        self._ensure_daily_breakdown()
        self.daily_pnl_breakdown["estimated_fee_usd"] = max(
            0.0, self.daily_pnl_breakdown["estimated_fee_usd"] - est_fee_usd
        )
        self.daily_pnl_breakdown["actual_fee_usd"] += actual_fee
        if v.key == "entropy" or getattr(v, "kind", None) == "hl":
            self._handle_fee_observation(float(verified["fee_bps"]), venue_key=v.key)

    async def _execute_pair(self, buy, sell, plan: ArbPlan, reduce_only: bool) -> bool:
        cfg = self.cfg
        now = time.time()
        send_reduce_only = bool(reduce_only or self.is_reduce_only(now))
        # Post-lock re-verification: state or books may have changed while waiting for locks.
        if (not (buy.ready_to_trade() and sell.ready_to_trade())
                or self._venue_down
                or self._venue_limited(buy) or self._venue_limited(sell)
                or not (buy.book.is_fresh(cfg.staleness_sec)
                        and sell.book.is_fresh(cfg.staleness_sec))):
            log.warning("[PRE-SEND BLOCKED] %s %s/%s: venue readiness or book freshness changed",
                        "REDUCE" if send_reduce_only else "ENTRY", buy.name, sell.name)
            return False
        if not send_reduce_only and not self._check_book_age_guard():
            log.warning("[PRE-SEND BLOCKED] ENTRY %s/%s: book age exceeded limit after lock",
                        buy.name, sell.name)
            return False
        prem = self.premium_bps()
        if cfg.risk.max_spread_bps > 0.0 and prem is not None and abs(prem) > cfg.risk.max_spread_bps:
            log.warning("[PRE-SEND BLOCKED] %s %s/%s: extreme spread %.2fbps after lock",
                        "REDUCE" if send_reduce_only else "ENTRY", buy.name, sell.name, prem)
            return False
        prepared = self._prepare_pair_order(buy, sell, plan, reduce_only=send_reduce_only, now=now)
        if prepared is None:
            log.warning("[BOUNDS BLOCKED] %s %s/%s: order failed final depth, spread, or size limits",
                        "REDUCE" if send_reduce_only else "ENTRY", buy.name, sell.name)
            return False
        plan, buy_bound, sell_bound = prepared
        qty = plan.qty

        inv_bps = self._inv_add_bps(buy, sell)
        direction = "sell_entropy" if sell.key == "entropy" else "buy_entropy"
        self.last_trade_ts = now
        log.info("[ARB] %s%s: BUY %s %.6g @<=%.6g | SELL %s @>=%.6g | "
                 "take $%.0f of $%.0f | prem %.2fbps | exp $%.4f",
                 direction, " [RO]" if send_reduce_only else "",
                 buy.name, qty, plan.buy_limit, sell.name,
                 plan.sell_limit, plan.buy_notional, plan.q_max_notional,
                 plan.marginal_premium_bps, plan.exp_edge_usd)
        # Persist both client IDs as "attempted" in ONE atomic fsync before firing
        # both legs concurrently (zero fsync stagger between leg 1 and leg 2).
        ids = self._stage_orders(
            ((buy, True, buy_bound), (sell, False, sell_bound)),
            qty=qty, purpose="arb", reduce_only=send_reduce_only,
            submission="attempted",
        )
        if ids is None:
            return False
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
                    if can_verify and imbalanced:
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

        actual_fee_total = est_fee_total = 0.0
        posted = {}
        posted_est_fee: Dict[str, float] = {}
        for v, info, filled, cid, fallback_px, fee_rate, sign in (
            (buy, binfo, bfill, b_cid, buy_bound, b_fee_rate, 1),
            (sell, sinfo, sfill, s_cid, sell_bound, s_fee_rate, -1),
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

        if posted:
            self._ensure_daily_breakdown()
            self.daily_pnl_breakdown["actual_fee_usd"] += actual_fee_total
            self.daily_pnl_breakdown["estimated_fee_usd"] += est_fee_total

        for v, info, filled in deferred_fee_checks:
            if v.key in posted_est_fee:
                bt = asyncio.create_task(
                    self._verify_fill_fee_bg(
                        v,
                        order_id=info.get("order_id"),
                        client_order_id=info.get("client_order_id"),
                        filled=filled,
                        est_fee_usd=posted_est_fee[v.key],
                    ),
                    name=f"fee-verify-{v.key}",
                )
                self._bg_tasks.add(bt)
                bt.add_done_callback(self._bg_tasks.discard)

        unresolved = False
        for v, info, is_buy, bound in (
            (buy, binfo, True, buy_bound),
            (sell, sinfo, False, sell_bound),
        ):
            cid = info.get("client_order_id")
            key = f"{v.key}:{cid}" if cid is not None else None
            known_fill = float(info.get("filled_base") or 0.0)
            if key in self._pending_orders:
                self._pending_orders[key]["known_filled"] = known_fill
            if info.get("unresolved"):
                unresolved = True
                self._register_pending_order(
                    v, cid, is_buy=is_buy, qty=qty, limit_px=bound,
                    known_filled=known_fill, purpose="arb", reduce_only=send_reduce_only,
                    persist=False,
                )
            elif key is not None:
                self._mark_order_settled(key)

        posted_buy, buy_fee_unit = posted.get(buy.key, (0.0, 0.0))
        posted_sell, sell_fee_unit = posted.get(sell.key, (0.0, 0.0))
        matched = min(posted_buy, posted_sell)
        fill_edge = 0.0
        mid_adj_edge = 0.0
        if matched > 0 and binfo.get("avg_px") and sinfo.get("avg_px"):
            gross_spread = matched * (sinfo["avg_px"] - binfo["avg_px"])
            fill_edge = gross_spread - matched * (buy_fee_unit + sell_fee_unit)
            mid_adj_edge = self._midline_adjusted_edge_usd(
                buy, sell, matched, binfo["avg_px"], sinfo["avg_px"],
                buy_fee_unit, sell_fee_unit,
            )
            self.total_fill_edge += fill_edge
            # P1-4: record daily PnL breakdown
            self.daily_pnl_breakdown["spread_pnl_usd"] += gross_spread

        log.info("[SETTLED] %s: buy %s %s %.6g/%.6g | sell %s %s %.6g/%.6g | "
                 "matched %.6g | fill edge $%.4f (actFee=$%.4f estFee=$%.4f)",
                 direction, buy.name, binfo["status"], bfill, qty,
                 sell.name, sinfo["status"], sfill, qty, matched,
                 fill_edge, actual_fee_total, est_fee_total)
        buy.last_traded_ts = sell.last_traded_ts = time.time()

        if abs(bfill - sfill) > cfg.net_tolerance_base:
            self.notifier.notify(
                "legging_failure",
                f"⚠️ Legging failure ({direction}): "
                f"BUY {buy.name} {bfill:.6g}/{qty:.6g} ({binfo['status']}) | "
                f"SELL {sell.name} {sfill:.6g}/{qty:.6g} ({sinfo['status']}) | "
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
                mid_adj_edge, mtm_pnl_usd=self.daily_mtm_pnl())
            self._handle_loss_breaker(tripped, reason)

        self._record_trade(direction, plan,
                           fill_edge if paired_fill else None,
                           f"{binfo['status']}/{sinfo['status']}", paired_fill, matched_expected)
        self._log_csv(direction, buy, sell, plan, paired_fill, bfill, sfill,
                      binfo["status"], sinfo["status"], fill_edge, inv_bps)
        self.last_trade_ts = time.time()
        self._commit_orders(ids, ((buy, binfo), (sell, sinfo)))
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
            is_sell = cur_net > 0
            sgn = 1.0 if cur_net > 0 else -1.0
            tried_venue = False
            made_progress = False

            for v in sorted(self.venues.values(),
                            key=lambda x: (self._venue_limited(x), -x.position * sgn)):
                if v.position * sgn <= 0:
                    continue
                # Use connection freshness (staleness_sec), NOT max_book_age_sec, so a
                # quiet live book never traps an unhedged directional exposure.
                if (v.key in self._venue_down or not v.ready_to_trade()
                        or self._venue_limited(v) or not self._venue_rate_ok(v)
                        or not v.book.is_fresh(cfg.staleness_sec)):
                    continue
                lk = self._vlock(v.key)
                if lk.locked():
                    continue
                venue_step = 10 ** -v.size_decimals
                closing_all_on_venue = abs(cur_net) >= abs(v.position) - 1e-9
                step = min(self._step, venue_step) if closing_all_on_venue else self._step
                qty = floor_step(min(abs(cur_net), abs(v.position)), step)
                if qty <= 0.0:
                    continue
                ref = v.book.best_bid() if is_sell else v.book.best_ask()
                if ref is None:
                    continue
                # P1-1: round conservatively INWARD so rounded limit never breaches slip cap
                limit = (
                    v.px_round(ref * (1.0 - slip), round_up=True)
                    if is_sell
                    else v.px_round(ref * (1.0 + slip), round_up=False)
                )
                order_price = max(ref, limit)
                qty = floor_step(min(qty, cfg.max_order_notional / order_price), step)
                # Both Hyperliquid and Lighter exempt a full-close reduce-only order
                # from the $10 minimum notional rule when flattening residual dust.
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
                if qty <= 0.0 or qty < min_base_req or not min_notional_ok:
                    continue
                tried_venue = True
                await lk.acquire()
                try:
                    side_str = "SELL" if is_sell else "BUY"
                    log.warning("[HEDGE %d/%d] net %+.6g — %s %.6g on %s @%.6g (slip<=%.1fbps)",
                                attempt, max_retries, cur_net, side_str, qty, v.name,
                                limit, eff_slip_bps)
                    ids = self._stage_orders(
                        ((v, not is_sell, limit),), qty=qty, purpose="hedge",
                        reduce_only=True, submission="attempted",
                    )
                    if ids is None:
                        return
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
                            px = info.get("avg_px") or limit
                            fee = self.effective_fee_bps(v) / 1e4
                            v.cash += fill * px * (1.0 - fee) if is_sell \
                                else -fill * px * (1.0 + fee)
                            v.volume_usd += fill * px
                            if ids:
                                self._pending_orders[f"{v.key}:{ids[v.key]}"]["known_filled"] = fill
                            # P1-4: track hedge cost/slippage vs mid separately
                            mid = v.book.mid()
                            if mid is not None:
                                slip_pnl = (
                                    fill * (px * (1.0 - fee) - mid)
                                    if is_sell
                                    else fill * (mid - px * (1.0 + fee))
                                )
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

    # --------------------------------------------------- P0-1 pending reconcile

    async def _reconcile_pending_orders(self) -> bool:
        """Settle pending fills only when both position snapshots agree."""
        if self._orders_in_flight:
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
        self._ensure_daily_breakdown()
        for key, venue, rec, result, additional in settlements:
            if self._mark_order_settled(key) and additional > 0:
                px = result.get("avg_px") or rec["limit_px"]
                if result.get("fee_usd") is not None:
                    fee_usd = result["fee_usd"] * additional / result["filled_base"]
                    self.daily_pnl_breakdown["actual_fee_usd"] += fee_usd
                else:
                    fee_usd = additional * px * self.effective_fee_bps(venue) / 1e4
                    self.daily_pnl_breakdown["estimated_fee_usd"] += fee_usd
                venue.cash += (-1 if rec["is_buy"] else 1) * additional * px - fee_usd
                venue.volume_usd += additional * px
            venue.position = expected[venue.key]
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
            age_limit = self.book_age_limit()
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
