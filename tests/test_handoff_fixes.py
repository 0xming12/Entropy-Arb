"""Comprehensive unit tests for T1-T8 review handoff fixes & safeguards.

Run:  python3 -m pytest tests/  (or  python3 tests/test_handoff_fixes.py)
"""
import asyncio
import os
import sys
import tempfile
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.book import ArbPlan, OrderBook  # noqa: E402
from entropy_arb.config import LIGHTER_PROFILES, VenueConf, load_config  # noqa: E402
from entropy_arb.engine import Engine  # noqa: E402
from entropy_arb.risk import SessionFilter  # noqa: E402
from entropy_arb.state import InstanceLock  # noqa: E402
from entropy_arb.venue_hl import HLVenue  # noqa: E402
from entropy_arb.venue_lighter import LighterVenue  # noqa: E402

NO_ENV = os.path.join(tempfile.gettempdir(), "entropy-arb-no-such.env")


def approx(a, b, tol=1e-6):
    assert abs(a - b) <= tol, f"{a} != {b}"


def make_cfg(extra_yaml: str = "", midline=0.0, upper=4.0, lower=4.0, lat=0.0):
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    f.write(
        f"""
thresholds:
  midline_bps: {midline}
  upper_bps: {upper}
  lower_bps: {lower}
  latency_buffer_bps: {lat}
execution:
  premium_persist_sec: 0.0
  leg_slippage_bps: 2.0
"""
        + extra_yaml
    )
    f.close()
    return load_config(f.name, NO_ENV, symbol="SNDK", hedge_venue="lighter-rh")


class MockVenue:
    def __init__(self, key, label, cap=10000.0, fee=0.0):
        self.key, self.name = key, label
        self.cap_usd, self.fee_bps = cap, fee
        self.size_decimals, self.min_base, self.min_quote = 4, 1e-4, 10.0
        self.position, self.cash, self.volume_usd = 0.0, 0.0, 0.0
        self.equity = self.free = self.start_equity = None
        self.orders_per_min = 30
        self.last_traded_ts = 0.0
        self.book = OrderBook()
        self.reconnect_calls = 0
        self.chain_position = 0.0
        self.funding_rate = 0.0
        self.send_responses = []
        self.sent_orders = []

    def ready_to_trade(self):
        return True

    def set_book(self, bid, ask, sz=50.0):
        self.book.apply_hl([[{"px": str(bid), "sz": str(sz)}],
                            [{"px": str(ask), "sz": str(sz)}]])

    def request_reconnect(self):
        self.reconnect_calls += 1

    def px_round(self, px, round_up=False):
        return round(px, 4)

    async def fetch_funding_rate_hourly(self):
        return self.funding_rate

    async def fetch_position(self):
        return self.chain_position

    async def fetch_equity(self):
        if self.equity is None and self.free is None:
            return None
        return self.equity, self.free

    async def send_taker(self, *, is_buy, qty, limit_px, reduce_only=False):
        self.sent_orders.append(
            {"is_buy": is_buy, "qty": qty, "limit_px": limit_px, "reduce_only": reduce_only}
        )
        if self.send_responses:
            return self.send_responses.pop(0)
        return {"status": "filled", "outcome": "filled", "filled_base": qty,
                "avg_px": limit_px, "err": None, "unresolved": False,
                "client_order_id": f"{self.key}-{len(self.sent_orders)}"}


def make_engine(extra_yaml: str = "", **kw):
    eng = Engine(make_cfg(extra_yaml, **kw))
    eng.entropy = MockVenue("entropy", "ENTROPY")
    eng.hedge = MockVenue("hedge", "RH")
    eng.venues = {"entropy": eng.entropy, "hedge": eng.hedge}
    eng._step, eng._min_base, eng._min_notional = 1e-4, 1e-4, 10.0
    return eng


def run_scan(eng, now=None, now_mono=None):
    ts = time.time() if now is None else now

    async def go():
        eng._scan(ts, now_mono=now_mono)
        return eng._scan(ts, now_mono=now_mono)

    return asyncio.run(go())


def test_t1_single_side_funding_failure_zeros_skew():
    eng = make_engine(
        """
funding_skew:
  enabled: true
  expected_hold_hours: 4.0
  max_skew_bps: 10.0
"""
    )
    eng.entropy.funding_rate = 0.0002
    eng.hedge.funding_rate = 0.00005
    asyncio.run(eng._poll_funding_once())
    approx(eng.funding_skew.skew_bps(), 6.0)

    eng.hedge.funding_rate = None
    asyncio.run(eng._poll_funding_once())
    assert eng.funding_skew.valid is False
    approx(eng.funding_skew.skew_bps(), 0.0)


def test_t1_min_net_edge_floor_prevents_negative_hurdle():
    eng = make_engine(
        midline=0.0,
        upper=4.0,
        lower=4.0,
        lat=3.0,
        extra_yaml="""
funding_skew:
  enabled: true
  expected_hold_hours: 4.0
  max_skew_bps: 10.0
  min_net_edge_bps: 1.0
""",
    )
    eng.funding_skew.update_rates(0.005, 0.0)
    approx(eng.funding_skew.skew_bps(), 10.0)
    approx(eng._eff_threshold(buy=eng.hedge, sell=eng.entropy), 1.0)
    approx(eng._eff_threshold(buy=eng.entropy, sell=eng.hedge), 17.0)


def test_t2_reduce_only_exempts_session_buffer_and_adverse_funding():
    eng = make_engine(
        midline=0.0,
        upper=2.0,
        lower=2.0,
        extra_yaml="""
risk:
  reduce_only_ignore_session_buffer: true
session_filter:
  enabled: true
  off_hours_mode: reduce_only
  off_hours_spread_buffer_bps: 8.0
funding_skew:
  enabled: true
  expected_hold_hours: 4.0
  max_skew_bps: 10.0
""",
    )
    t_off = datetime(2026, 7, 15, 22, 0, tzinfo=timezone.utc).timestamp()
    eng.funding_skew.update_rates(0.0, 0.0001)

    eng.entropy.position = 5.0
    eng.hedge.position = -5.0
    approx(eng._eff_threshold(buy=eng.hedge, sell=eng.entropy, now=t_off), 2.0)

    eng.entropy.set_book(100.05, 100.07)
    eng.hedge.set_book(99.99, 100.01)
    best = run_scan(eng, now=t_off)
    assert best is not None
    assert best[1].key == "entropy" and best[2].qty <= 5.0 + 1e-9


def test_t3_hl_hip3_dex_and_delisted_filter():
    conf = VenueConf(
        key="entropy", kind="hl", label="ENTROPY", symbol="SNDK",
        fee_bps=0.0, cap_usd=1000.0, orders_per_min=120, hl_dex="io",
    )
    hl = HLVenue(conf, "https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws",
                 session=None, settle_timeout_sec=5.0)
    seen_payloads = []

    async def fake_info(payload):
        seen_payloads.append(payload)
        return [
            {
                "universe": [
                    {"name": "io:SNDK", "isDelisted": True},
                    {"name": "io:SNDK", "isDelisted": False},
                ]
            },
            [
                {"funding": "0.9999"},
                {"funding": "0.00015"},
            ],
        ]

    hl._info = fake_info
    rate = asyncio.run(hl.fetch_funding_rate_hourly())
    assert seen_payloads == [{"type": "metaAndAssetCtxs", "dex": "io"}]
    approx(rate, 0.00015)


def test_t3_lighter_strict_exchange_and_market_filter():
    conf = VenueConf(
        key="hedge", kind="lighter", label="RH", symbol="SNDK",
        fee_bps=0.0, cap_usd=1000.0, orders_per_min=30,
        lighter_profile=LIGHTER_PROFILES["lighter-rh"],
    )
    lt = LighterVenue(conf, session=None, settle_timeout_sec=5.0)
    lt.market_id = 42

    async def fake_get(_path, params=None):
        return {
            "funding_rates": [
                {"market_id": 42, "exchange": "binance", "rate": "0.08"},
                {"market_id": 99, "exchange": "lighter", "rate": "0.04"},
                {"market_id": 42, "exchange": "lighter", "symbol": "SNDK", "rate": "0.0008"},
            ]
        }

    lt._get = fake_get
    approx(asyncio.run(lt.fetch_funding_rate_hourly()), 0.0001)

    async def fake_get_wrong_ex(_path, params=None):
        return {
            "funding_rates": [
                {"market_id": 42, "exchange": "hyperliquid", "rate": "0.01"},
            ]
        }

    lt._get = fake_get_wrong_ex
    assert asyncio.run(lt.fetch_funding_rate_hourly()) is None


def test_t3_fill_receipt_fee_triggers_breaker_immediately():
    eng = make_engine(
        """
risk:
  max_taker_fee_bps: 2.0
"""
    )
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    eng.entropy.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 1.0, "avg_px": 100.14,
         "err": None, "unresolved": False, "fee_bps": 9.0, "client_order_id": "hl-1"}
    ]
    eng.hedge.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 1.0, "avg_px": 100.01,
         "err": None, "unresolved": False, "client_order_id": "lt-1"}
    ]
    plan = ArbPlan(
        qty=1.0, buy_limit=100.01, sell_limit=100.14,
        buy_notional=100.01, sell_notional=100.14,
        q_max=10.0, q_max_notional=1000.0,
        top_premium_bps=13.0, marginal_premium_bps=13.0,
        buy_fee=0.0, sell_fee=0.0,
    )
    asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))
    assert eng.circuit_breaker.fee_tripped is True
    assert eng.is_reduce_only() is True


def test_t4_stale_book_blocks_and_triggers_reconnect():
    eng = make_engine(
        """
risk:
  max_book_age_sec: 5.0
"""
    )
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    now_mono = time.monotonic()

    assert run_scan(eng, now_mono=now_mono + 1.0) is not None
    assert eng.entropy.reconnect_calls == 0

    eng.entropy.book.last_update_mono = now_mono - 10.0
    assert run_scan(eng, now_mono=now_mono) is None
    assert eng.entropy.reconnect_calls == 0
    assert not eng._check_book_age_guard(now_mono, reconnect=True)
    assert eng.entropy.reconnect_calls == 1


def test_record_only_watchdog_runs_without_book_events():
    async def verify():
        eng = make_engine()
        eng.record_only = True
        eng.entropy.set_book(100.14, 100.16)
        eng.hedge.set_book(99.99, 100.01)
        eng.entropy.book.last_update_mono -= eng.cfg.staleness_sec + 2
        reconnect_requested = asyncio.Event()
        eng.entropy.request_reconnect = reconnect_requested.set
        task = asyncio.create_task(eng._book_watchdog())
        try:
            await asyncio.wait_for(reconnect_requested.wait(), timeout=3)
            assert not eng._update_evt.is_set()
        finally:
            eng.request_stop()
            await task

    asyncio.run(verify())


def test_t4_extreme_spread_blocks_orders():
    eng = make_engine(
        """
risk:
  max_spread_bps: 100.0
"""
    )
    eng.entropy.set_book(101.99, 102.01)
    eng.hedge.set_book(99.99, 100.01)
    assert run_scan(eng) is None


def test_t5_reconcile_drift_triggers_reduce_only():
    eng = make_engine(
        """
risk:
  reconcile_drift_reduce_only: true
"""
    )
    eng.entropy.set_book(100.0, 100.02)
    eng.hedge.set_book(100.0, 100.02)
    eng.entropy.position = 0.0
    eng.entropy.chain_position = 3.5

    asyncio.run(eng._reconcile_venue(eng.entropy, strict=False))
    approx(eng.entropy.position, 3.5)
    assert eng.circuit_breaker.reconcile_tripped is True
    assert eng.is_reduce_only() is True


def test_t5_available_margin_floor_triggers_reduce_only_and_recovers():
    eng = make_engine(
        """
risk:
  min_available_margin_usd: 100.0
"""
    )
    eng.entropy.equity, eng.entropy.free = 500.0, 80.0
    eng.hedge.equity, eng.hedge.free = 500.0, 400.0
    asyncio.run(eng._check_margin_once())
    assert eng.circuit_breaker.margin_tripped is True
    assert eng.is_reduce_only() is True

    eng.entropy.free = 250.0
    asyncio.run(eng._check_margin_once())
    assert eng.circuit_breaker.margin_tripped is False
    assert eng.is_reduce_only() is False


def test_t6_hedge_bounded_retries_and_halt_escalation():
    eng = make_engine(
        """
execution:
  hedge_max_retries: 2
  hedge_retry_delay_sec: 0.0
  hedge_max_slippage_bps: 15.0
  hedge_slippage_bps: 40.0
"""
    )
    eng.entropy.set_book(100.00, 100.02)
    eng.hedge.set_book(100.00, 100.02)
    eng.entropy.position = 2.0
    eng.entropy.send_responses = [
        {"status": "send-failed", "outcome": "rejected", "filled_base": 0.0, "avg_px": None,
         "err": "liquidity dry", "unresolved": False, "client_order_id": "h1"},
        {"status": "send-failed", "outcome": "rejected", "filled_base": 0.0, "avg_px": None,
         "err": "liquidity dry", "unresolved": False, "client_order_id": "h2"},
    ]

    asyncio.run(eng._hedge(2.0))
    assert len(eng.entropy.sent_orders) == 2
    assert eng.halted is True
    approx(eng.entropy.sent_orders[0]["limit_px"], 99.85, tol=1e-3)


def test_t7_us_holidays_and_early_close_dates():
    sf = SessionFilter(
        enabled=True,
        tz_name="America/New_York",
        regular_start="09:30",
        regular_end="16:00",
        early_close_end="13:00",
        off_hours_mode="throttle",
        weekend_reduce_only=True,
        holidays=["2026-11-26"],
        early_close_dates=["2026-11-27"],
    )
    dt_holiday = datetime(2026, 11, 26, 16, 0, tzinfo=timezone.utc)
    assert sf.classify(dt_holiday) == "holiday"
    assert sf.evaluate(dt_holiday).reduce_only is True

    dt_early_open = datetime(2026, 11, 27, 17, 30, tzinfo=timezone.utc)
    assert sf.classify(dt_early_open) == "regular"

    dt_early_closed = datetime(2026, 11, 27, 18, 5, tzinfo=timezone.utc)
    assert sf.classify(dt_early_closed) == "off_hours"


def test_t8_state_persistence_restore_and_cross_day_reset():
    tmp_dir = tempfile.mkdtemp(prefix="entropy_state_")
    extra = f"""
risk:
  max_daily_loss_usd: 100.0
  max_consecutive_losses: 5
dynamic_midline:
  enabled: true
  method: rolling_median
  window_minutes: 60.0
  min_samples: 3
  sample_interval_sec: 0.0
persistence:
  enabled: true
  state_dir: "{tmp_dir}"
  lock_enabled: false
"""
    t_day1 = datetime(2026, 7, 15, 15, 0, tzinfo=timezone.utc).timestamp()
    eng1 = make_engine(extra)
    eng1.circuit_breaker.record_trade_pnl(-40.0, now=t_day1)
    eng1.circuit_breaker.record_trade_pnl(-15.0, now=t_day1 + 10)
    for i, v in enumerate([8.0, 10.0, 12.0]):
        eng1.dynamic_midline.update(v, now=t_day1 + i * 5)
    assert eng1.save_state(now=t_day1 + 20) is True

    eng2 = make_engine(extra)
    assert eng2.load_state(now=t_day1 + 60) is True
    approx(eng2.circuit_breaker.daily_realized_pnl_usd, -55.0)
    assert eng2.circuit_breaker.consecutive_losses == 2
    assert eng2.dynamic_midline.is_warm is True
    approx(eng2.dynamic_midline.value(), 10.0)

    t_day2 = datetime(2026, 7, 16, 15, 0, tzinfo=timezone.utc).timestamp()
    eng3 = make_engine(extra)
    assert eng3.load_state(now=t_day2) is True
    approx(eng3.circuit_breaker.daily_realized_pnl_usd, 0.0)
    assert eng3.circuit_breaker.consecutive_losses == 0
    assert eng3.dynamic_midline.sample_count == 0


def test_t8_instance_lock_prevents_duplicate_live_start():
    tmp_dir = tempfile.mkdtemp(prefix="entropy_lock_")
    lock_path = os.path.join(tmp_dir, "SNDK_lighter-rh.lock")
    lock1 = InstanceLock(lock_path)
    lock2 = InstanceLock(lock_path)
    lock1.acquire()
    try:
        raised = False
        try:
            lock2.acquire()
        except RuntimeError as e:
            raised = "already running" in str(e)
        assert raised, "expected second InstanceLock.acquire() to fail"
    finally:
        lock1.release()
    lock2.acquire()
    lock2.release()


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:55s} OK")
