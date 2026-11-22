"""P1 strategy tests: dynamic rolling midline (warmup fallback + EWMA/median +
std band), funding carry skew (threshold impact + graceful degradation), and
Lighter 300ms latency slippage buffer.

Run:  python3 -m pytest tests/  (or  python3 tests/test_strategy.py)
"""
import asyncio
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.book import OrderBook  # noqa: E402
from entropy_arb.config import LIGHTER_PROFILES, VenueConf, load_config  # noqa: E402
from entropy_arb.engine import Engine  # noqa: E402
from entropy_arb.strategy import DynamicMidline, FundingSkew  # noqa: E402
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

    def ready_to_trade(self):
        return True

    def set_book(self, bid, ask, sz=50.0):
        self.book.apply_hl([[{"px": str(bid), "sz": str(sz)}],
                            [{"px": str(ask), "sz": str(sz)}]])


def make_engine(**kw):
    eng = Engine(make_cfg(**kw))
    eng.entropy = StubVenue("entropy", "ENTROPY")
    eng.hedge = StubVenue("hedge", "RH")
    eng.venues = {"entropy": eng.entropy, "hedge": eng.hedge}
    eng._step, eng._min_base, eng._min_notional = 1e-4, 1e-4, 10.0
    return eng


# ------------------------------------------------------- Dynamic Midline tests

def test_dynamic_midline_warmup_and_rolling_median():
    dm = DynamicMidline(
        enabled=True,
        static_midline_bps=2.5,
        method="rolling_median",
        window_minutes=10.0,
        min_samples=5,
        sample_interval_sec=1.0,
        std_band_multiplier=0.5,
    )
    t0 = 1_700_000_000.0
    for i, val in enumerate([10.0, 12.0, 14.0, 16.0]):
        assert dm.update(val, now=t0 + i * 2.0) is True
        assert not dm.is_warm
        approx(dm.value(), 2.5)
        approx(dm.band_adjustment_bps(), 0.0)

    assert dm.update(18.0, now=t0 + 8.0) is True
    assert dm.is_warm
    approx(dm.value(), 14.0)
    assert dm.band_adjustment_bps() > 0.0

    assert dm.update(999.0, now=t0 + 8.2) is False
    approx(dm.value(), 14.0)

    t_later = t0 + 15 * 60.0
    for i in range(5):
        dm.update(20.0 + i, now=t_later + i * 2.0)
    assert dm.sample_count == 5
    approx(dm.value(), 22.0)


def test_dynamic_midline_ewma_in_engine():
    eng = make_engine(
        midline=1.0,
        upper=4.0,
        lower=4.0,
        extra_yaml="""
dynamic_midline:
  enabled: true
  method: ewma
  window_minutes: 60.0
  min_samples: 3
  sample_interval_sec: 0.0
""",
    )
    t0 = time.time()
    eng.dynamic_midline.update(10.0, now=t0)
    eng.dynamic_midline.update(10.0, now=t0 + 10)
    approx(eng.current_midline_bps(), 1.0)

    eng.dynamic_midline.update(10.0, now=t0 + 20)
    approx(eng.current_midline_bps(), 10.0)
    approx(eng._eff_threshold(buy=eng.hedge, sell=eng.entropy), 14.0)
    approx(eng._eff_threshold(buy=eng.entropy, sell=eng.hedge), -6.0)


# ---------------------------------------------------------- Funding Skew tests

def test_funding_skew_direction_and_cap():
    fs = FundingSkew(enabled=True, expected_hold_hours=4.0, max_skew_bps=10.0)
    assert fs.update_rates(0.0002, 0.00005) is True
    approx(fs.skew_bps(), 6.0)
    approx(fs.threshold_adjustment(sell_entropy=True), -6.0)
    approx(fs.threshold_adjustment(sell_entropy=False), +6.0)

    assert fs.update_rates(0.005, 0.0) is True
    approx(fs.skew_bps(), 10.0)
    approx(fs.threshold_adjustment(sell_entropy=True), -10.0)
    approx(fs.threshold_adjustment(sell_entropy=False), +10.0)


def test_funding_skew_and_latency_buffer_in_engine():
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
""",
    )
    approx(eng._eff_threshold(buy=eng.hedge, sell=eng.entropy), 7.0)
    approx(eng._eff_threshold(buy=eng.entropy, sell=eng.hedge), 7.0)

    eng.funding_skew.update_rates(0.0001, 0.00005)
    approx(eng._eff_threshold(buy=eng.hedge, sell=eng.entropy), 5.0)
    approx(eng._eff_threshold(buy=eng.entropy, sell=eng.hedge), 9.0)

    eng.entropy.set_book(100.06, 100.08)
    eng.hedge.set_book(99.98, 100.00)

    async def go():
        eng._scan(time.time())
        return eng._scan(time.time())

    best = asyncio.run(go())
    assert best is not None and best[1].key == "entropy"


def test_funding_fetch_graceful_degradation():
    hl_conf = VenueConf(
        key="entropy", kind="hl", label="ENTROPY", symbol="SNDK",
        fee_bps=0.0, cap_usd=1000.0, orders_per_min=120, hl_dex="io",
    )
    hl = HLVenue(hl_conf, "https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws",
                 session=None, settle_timeout_sec=5.0)

    async def boom_hl(_payload):
        raise RuntimeError("network down")

    hl._info = boom_hl
    assert asyncio.run(hl.fetch_funding_rate_hourly()) is None

    lt_conf = VenueConf(
        key="hedge", kind="lighter", label="RH", symbol="SNDK",
        fee_bps=0.0, cap_usd=1000.0, orders_per_min=30,
        lighter_profile=LIGHTER_PROFILES["lighter-rh"],
    )
    lt = LighterVenue(lt_conf, session=None, settle_timeout_sec=5.0)
    lt.market_id = 42

    async def boom_lt(_path, params=None):
        raise RuntimeError("503 unavailable")

    lt._get = boom_lt
    assert asyncio.run(lt.fetch_funding_rate_hourly()) is None


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:45s} OK")
