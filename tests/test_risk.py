"""P0 risk & notification tests: circuit breaker, fee spike, US session filter,
Telegram notifier throttling/sanitization, and --record-only compatibility.

Run:  python3 -m pytest tests/  (or  python3 tests/test_risk.py)
"""
import asyncio
import os
import sys
import tempfile
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.book import OrderBook  # noqa: E402
from entropy_arb.config import load_config  # noqa: E402
from entropy_arb.engine import Engine  # noqa: E402
from entropy_arb.notifier import TelegramNotifier  # noqa: E402
from entropy_arb.risk import CircuitBreaker, SessionFilter  # noqa: E402

NO_ENV = os.path.join(tempfile.gettempdir(), "entropy-arb-no-such.env")


def make_cfg(extra_yaml: str = ""):
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    f.write(
        """
thresholds:
  midline_bps: 0.0
  upper_bps: 2.0
  lower_bps: 2.0
execution:
  premium_persist_sec: 0.0
"""
        + extra_yaml
    )
    f.close()
    return load_config(f.name, NO_ENV, symbol="SNDK", hedge_venue="lighter-rh")


class StubVenue:
    def __init__(self, key, label, cap=10000.0, fee=0.0):
        self.key, self.name = key, label
        self.cap_usd, self.fee_bps = cap, fee
        self.size_decimals, self.min_base, self.min_quote = 4, 1e-4, 10.0
        self.position, self.cash, self.volume_usd = 0.0, 0.0, 0.0
        self.orders_per_min = 30
        self.last_traded_ts = 0.0
        self.book = OrderBook()
        self.live_fee_bps = 0.9

    def ready_to_trade(self):
        return True

    def set_book(self, bid, ask, sz=50.0):
        self.book.apply_hl([[{"px": str(bid), "sz": str(sz)}],
                            [{"px": str(ask), "sz": str(sz)}]])

    async def fetch_taker_fee_bps(self):
        return self.live_fee_bps


def make_engine(extra_yaml: str = "", record_only: bool = False):
    eng = Engine(make_cfg(extra_yaml), record_only=record_only)
    eng.entropy = StubVenue("entropy", "ENTROPY")
    eng.hedge = StubVenue("hedge", "RH")
    eng.venues = {"entropy": eng.entropy, "hedge": eng.hedge}
    eng._step, eng._min_base, eng._min_notional = 1e-4, 1e-4, 10.0
    return eng


def run_scan(eng, now=None):
    ts = time.time() if now is None else now

    async def go():
        eng._scan(ts)
        return eng._scan(ts)

    return asyncio.run(go())


# ------------------------------------------------------- Circuit Breaker tests

def test_daily_loss_circuit_breaker_reduce_only():
    cb = CircuitBreaker(max_daily_loss_usd=50.0, breaker_action="reduce_only")
    t0 = datetime(2026, 6, 10, 14, 0, tzinfo=timezone.utc).timestamp()

    tripped, _ = cb.record_trade_pnl(-30.0, now=t0)
    assert not tripped and not cb.is_reduce_only

    tripped, reason = cb.record_trade_pnl(-25.0, now=t0 + 60)
    assert tripped and "daily loss" in reason
    assert cb.is_reduce_only and not cb.should_halt

    # Next calendar day (ET) resets daily PnL and clears reduce-only
    t_next = datetime(2026, 6, 11, 14, 0, tzinfo=timezone.utc).timestamp()
    assert cb.maybe_roll_day(t_next) is True
    assert cb.daily_realized_pnl_usd == 0.0
    assert not cb.is_reduce_only


def test_consecutive_losses_breaker_and_halt_mode():
    cb = CircuitBreaker(max_consecutive_losses=3, breaker_action="halt")
    t0 = datetime(2026, 6, 10, 14, 0, tzinfo=timezone.utc).timestamp()

    assert cb.record_trade_pnl(-1.0, now=t0)[0] is False
    assert cb.record_trade_pnl(-1.0, now=t0 + 1)[0] is False
    # A winning trade resets the consecutive loss streak
    assert cb.record_trade_pnl(+2.0, now=t0 + 2)[0] is False
    assert cb.consecutive_losses == 0

    assert cb.record_trade_pnl(-1.0, now=t0 + 3)[0] is False
    assert cb.record_trade_pnl(-1.0, now=t0 + 4)[0] is False
    tripped, reason = cb.record_trade_pnl(-1.0, now=t0 + 5)
    assert tripped and "consecutive losses" in reason
    assert cb.should_halt is True


def test_fee_spike_blocks_new_entries_and_recovers():
    eng = make_engine(
        """
risk:
  max_taker_fee_bps: 2.0
"""
    )
    # Entropy rich -> would normally trigger sell_entropy
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)

    # Normal Growth Mode fee (0.9 bps) -> allowed
    eng.entropy.live_fee_bps = 0.9
    asyncio.run(eng._check_entropy_fee_once())
    assert not eng.circuit_breaker.fee_tripped
    assert run_scan(eng) is not None

    # Growth Mode disabled -> fee spikes to 9.0 bps -> blocks opening when flat
    eng.entropy.live_fee_bps = 9.0
    asyncio.run(eng._check_entropy_fee_once())
    assert eng.circuit_breaker.fee_tripped
    assert eng.is_reduce_only()
    assert run_scan(eng) is None

    # If we hold a long Entropy position, sell_entropy IS reducing -> allowed,
    # capped to existing position size
    eng.entropy.position = 2.0
    eng.hedge.position = -2.0
    best = run_scan(eng)
    assert best is not None
    buy, sell, plan = best
    assert sell.key == "entropy"
    assert plan.qty <= 2.0 + 1e-9

    # Fee drops back to 0.9 bps -> recovers
    eng.entropy.position = 0.0
    eng.hedge.position = 0.0
    eng.entropy.live_fee_bps = 0.9
    asyncio.run(eng._check_entropy_fee_once())
    assert not eng.circuit_breaker.fee_tripped
    assert run_scan(eng) is not None


# -------------------------------------------------------- Session Filter tests

def test_session_filter_dst_and_weekend():
    sf = SessionFilter(
        enabled=True,
        tz_name="America/New_York",
        regular_start="09:30",
        regular_end="16:00",
        off_hours_mode="reduce_only",
        weekend_reduce_only=True,
    )

    # Summer (EDT = UTC-4): 2026-07-15 (Wed)
    # 13:30 UTC == 09:30 EDT -> regular
    dt_edt_open = datetime(2026, 7, 15, 13, 30, tzinfo=timezone.utc)
    assert sf.classify(dt_edt_open) == "regular"
    st = sf.evaluate(dt_edt_open)
    assert not st.reduce_only and st.cap_multiplier == 1.0 and st.spread_buffer_bps == 0.0

    # 13:29 UTC == 09:29 EDT -> off_hours (pre-market)
    dt_edt_pre = datetime(2026, 7, 15, 13, 29, tzinfo=timezone.utc)
    assert sf.classify(dt_edt_pre) == "off_hours"
    assert sf.evaluate(dt_edt_pre).reduce_only is True

    # 20:00 UTC == 16:00 EDT -> off_hours (after-hours)
    dt_edt_after = datetime(2026, 7, 15, 20, 0, tzinfo=timezone.utc)
    assert sf.classify(dt_edt_after) == "off_hours"

    # Winter (EST = UTC-5): 2026-01-14 (Wed)
    # 14:29 UTC == 09:29 EST -> off_hours
    dt_est_pre = datetime(2026, 1, 14, 14, 29, tzinfo=timezone.utc)
    assert sf.classify(dt_est_pre) == "off_hours"
    # 14:30 UTC == 09:30 EST -> regular
    dt_est_open = datetime(2026, 1, 14, 14, 30, tzinfo=timezone.utc)
    assert sf.classify(dt_est_open) == "regular"
    # 20:30 UTC == 15:30 EST -> regular (would be after-hours if DST weren't handled!)
    dt_est_late = datetime(2026, 1, 14, 20, 30, tzinfo=timezone.utc)
    assert sf.classify(dt_est_late) == "regular"

    # Weekend: 2026-07-18 (Sat) 15:00 UTC == 11:00 EDT
    dt_sat = datetime(2026, 7, 18, 15, 0, tzinfo=timezone.utc)
    assert sf.classify(dt_sat) == "weekend"
    assert sf.evaluate(dt_sat).reduce_only is True


def test_session_filter_throttle_mode_in_engine():
    eng = make_engine(
        """
session_filter:
  enabled: true
  off_hours_mode: throttle
  off_hours_cap_multiplier: 0.25
  off_hours_spread_buffer_bps: 6.0
  weekend_reduce_only: true
"""
    )
    eng.entropy.set_book(100.05, 100.07)   # +5 bps rich vs hedge
    eng.hedge.set_book(99.99, 100.01)

    # Regular hours (Wed 11:00 EDT = 15:00 UTC): threshold = 2.0 bps -> fires
    t_reg = datetime(2026, 7, 15, 15, 0, tzinfo=timezone.utc).timestamp()
    assert abs(eng._eff_threshold(eng.hedge, eng.entropy, now=t_reg) - 2.0) < 1e-9
    assert run_scan(eng, now=t_reg) is not None

    # After-hours (Wed 18:00 EDT = 22:00 UTC): threshold = 2.0 + 6.0 = 8.0 bps -> blocked
    t_off = datetime(2026, 7, 15, 22, 0, tzinfo=timezone.utc).timestamp()
    assert abs(eng._eff_threshold(eng.hedge, eng.entropy, now=t_off) - 8.0) < 1e-9
    assert run_scan(eng, now=t_off) is None

    # Check reduced headroom cap multiplier (0.25 * $10k = $2500)
    hr_reg = eng._position_qty_headroom(eng.hedge, eng.entropy, cap_multiplier=1.0)
    hr_off = eng._position_qty_headroom(eng.hedge, eng.entropy, cap_multiplier=0.25)
    assert abs(hr_reg * 100.06 - 10000.0) < 1e-6 and abs(hr_off * 100.06 - 2500.0) < 1e-6

    # Weekend forces reduce_only even when off_hours_mode == throttle
    t_sun = datetime(2026, 7, 19, 15, 0, tzinfo=timezone.utc).timestamp()
    assert eng.session_state(t_sun).reduce_only is True


# ------------------------------------------------------- Notifier & record-only

def test_telegram_notifier_silent_and_throttled():
    # Unconfigured -> disabled, silent skip
    n_off = TelegramNotifier(None, None)
    assert not n_off.enabled
    assert asyncio.run(n_off.send("test", "hello")) is False

    # Configured -> throttles same key within 60s, allows distinct keys & force=True
    n = TelegramNotifier("123456:SECRET_TOKEN", "999999", throttle_sec=60.0)
    assert n.enabled
    assert n._sanitize("error at /bot123456:SECRET_TOKEN/sendMessage") == (
        "error at /bot<REDACTED>/sendMessage"
    )
    sent = []

    async def fake_post(text):
        sent.append(text)
        return True

    n._post = fake_post
    assert asyncio.run(n.send("ws", "err1", now=100.0)) is True
    assert asyncio.run(n.send("ws", "err2", now=120.0)) is False   # throttled
    assert asyncio.run(n.send("other", "msg", now=120.0)) is True  # different key
    assert asyncio.run(n.send("ws", "err3", now=120.0, force=True)) is True
    assert asyncio.run(n.send("ws", "err4", now=181.0)) is True    # 60s elapsed since 120.0
    assert len(sent) == 4


def test_record_only_engine_without_creds_or_telegram():
    eng = make_engine(record_only=True)
    assert eng.record_only is True
    assert not eng.cfg.creds_complete
    assert not eng.notifier.enabled


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:45s} OK")
