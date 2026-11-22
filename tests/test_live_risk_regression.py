"""Offline regression tests for P0-1, P0-2, P1-1, P1-2, P1-3, P1-4 and all
three hedge venue entries (lighter, lighter-rh, tradexyz).

Run:  python3 -m pytest tests/  (or  python3 tests/test_live_risk_regression.py)
"""
import asyncio
import math
import os
import sys
import tempfile
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.book import ArbPlan, OrderBook  # noqa: E402
from entropy_arb.config import (  # noqa: E402
    HLCreds,
    LIGHTER_PROFILES,
    LighterCreds,
    VenueConf,
    load_config,
)
from entropy_arb.engine import Engine  # noqa: E402
from entropy_arb.venue_hl import HLVenue  # noqa: E402
from entropy_arb.venue_lighter import AccountOrdersFeed, LighterVenue  # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), "..")
EXAMPLE = os.path.join(ROOT, "config.example.yaml")
NO_ENV = os.path.join(tempfile.gettempdir(), "entropy-arb-no-such.env")


def approx(a, b, tol=1e-6):
    assert abs(a - b) <= tol, f"{a} != {b}"


def make_cfg(extra_yaml: str = "", midline=0.0, upper=4.0, lower=4.0,
             leg_slip=2.0, hedge="lighter-rh"):
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    f.write(
        f"""
thresholds:
  midline_bps: {midline}
  upper_bps: {upper}
  lower_bps: {lower}
execution:
  premium_persist_sec: 0.0
  leg_slippage_bps: {leg_slip}
"""
        + extra_yaml
    )
    f.close()
    return load_config(f.name, NO_ENV, symbol="SNDK", hedge_venue=hedge)


class SimVenue:
    def __init__(self, key, label, cap=10000.0, fee=0.0, px_step=0.01):
        self.key, self.name = key, label
        self.cap_usd, self.fee_bps = cap, fee
        self.px_step = px_step
        self.size_decimals, self.min_base, self.min_quote = 4, 1e-4, 10.0
        self.position, self.cash, self.volume_usd = 0.0, 0.0, 0.0
        self.equity = self.free = self.start_equity = None
        self.orders_per_min = 60
        self.last_traded_ts = 0.0
        self.book = OrderBook()
        self.chain_position = 0.0
        self.chain_pos_error = None
        self.send_responses = []
        self.sent_orders = []
        self.order_queries = {}
        self.fill_fee_response = None
        self.live_fee_bps = None
        self.live_fee_error = None
        self.equity_error = None
        self.coin = "io:SNDK"

    def ready_to_trade(self):
        return True

    def set_book(self, bid, ask, sz=50.0):
        self.book.apply_hl([[{"px": str(bid), "sz": str(sz)}],
                            [{"px": str(ask), "sz": str(sz)}]])

    def px_round(self, px, round_up=False):
        f = 1.0 / self.px_step
        v = math.ceil(px * f - 1e-9) / f if round_up else math.floor(px * f + 1e-9) / f
        return round(v, 8)

    async def send_taker(self, *, is_buy, qty, limit_px, reduce_only=False):
        self.sent_orders.append(
            {"is_buy": is_buy, "qty": qty, "limit_px": limit_px, "reduce_only": reduce_only}
        )
        if self.send_responses:
            return self.send_responses.pop(0)
        return {"status": "filled", "outcome": "filled", "filled_base": qty,
                "avg_px": limit_px, "err": None, "unresolved": False,
                "client_order_id": f"{self.key}-{len(self.sent_orders)}"}

    async def query_order(self, client_order_id):
        res = self.order_queries.get(client_order_id)
        if callable(res):
            return res(client_order_id)
        if res is not None:
            return res
        return {"status": "unknown", "outcome": "unknown", "filled_base": 0.0,
                "avg_px": None, "err": "not found", "unresolved": True,
                "client_order_id": client_order_id}

    async def fetch_position(self):
        if self.chain_pos_error:
            raise self.chain_pos_error
        return self.chain_position

    async def fetch_taker_fee_bps(self):
        if self.live_fee_error:
            raise self.live_fee_error
        return self.live_fee_bps

    async def fetch_fill_fee(self, *, order_id=None, client_order_id=None, coin=None):
        return self.fill_fee_response

    async def fetch_equity(self):
        if self.equity_error:
            raise self.equity_error
        if self.equity is None and self.free is None:
            return None
        return self.equity, self.free


def make_engine(extra_yaml: str = "", px_step_e=0.01, px_step_h=0.01, **kw):
    eng = Engine(make_cfg(extra_yaml, **kw))
    eng.entropy = SimVenue("entropy", "ENTROPY", px_step=px_step_e)
    eng.hedge = SimVenue("hedge", "RH", px_step=px_step_h)
    eng.venues = {"entropy": eng.entropy, "hedge": eng.hedge}
    eng._step, eng._min_base, eng._min_notional = 1e-4, 1e-4, 10.0
    return eng


def run_scan(eng, now=None):
    ts = time.time() if now is None else now

    async def go():
        eng._scan(ts)
        return eng._scan(ts)

    return asyncio.run(go())


# --------------------------------------------------------------- P0-1 tests

def test_p0_1_entropy_filled_lighter_timeout_pauses_and_recovers():
    eng = make_engine(midline=0.0, upper=2.0, lower=2.0, leg_slip=2.0)
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)

    # Entropy fills 1.0, Lighter times out with unknown outcome
    eng.entropy.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 1.0, "avg_px": 100.14,
         "err": None, "unresolved": False, "client_order_id": "hl-101"}
    ]
    eng.hedge.send_responses = [
        {"status": "timeout", "outcome": "unknown", "filled_base": 0.0, "avg_px": None,
         "err": None, "unresolved": True, "client_order_id": "lt-202"}
    ]
    plan = ArbPlan(
        qty=1.0, buy_limit=100.01, sell_limit=100.14,
        buy_notional=100.01, sell_notional=100.14,
        q_max=10.0, q_max_notional=1000.0,
        top_premium_bps=13.0, marginal_premium_bps=13.0,
        buy_fee=0.0, sell_fee=0.0,
    )
    unresolved = asyncio.run(eng._execute(eng.hedge, eng.entropy, plan))
    assert unresolved is True
    assert eng.has_pending_reconcile() is True
    assert "hedge:lt-202" in eng._pending_orders

    # Continuous book refreshes MUST NOT fire any 2nd pair while pending
    for _ in range(5):
        eng.entropy.set_book(100.20, 100.22)
        eng.hedge.set_book(99.99, 100.01)
        assert run_scan(eng) is None

    # Query fails first -> remains paused
    eng.hedge.order_queries["lt-202"] = {
        "status": "query-failed", "outcome": "unknown", "filled_base": 0.0,
        "avg_px": None, "err": "503", "unresolved": True, "client_order_id": "lt-202"
    }
    assert asyncio.run(eng._reconcile_pending_orders()) is False
    assert eng.has_pending_reconcile() is True
    assert run_scan(eng) is None

    # Late fill confirmed (bypasses normal 5s grace window!), both chain positions match
    eng.hedge.order_queries["lt-202"] = {
        "status": "filled", "outcome": "filled", "filled_base": 1.0,
        "avg_px": 100.01, "err": None, "unresolved": False, "client_order_id": "lt-202"
    }
    eng.entropy.chain_position = -1.0
    eng.hedge.chain_position = +1.0
    assert asyncio.run(eng._reconcile_pending_orders()) is True
    assert eng.has_pending_reconcile() is False
    approx(eng.hedge.position, +1.0)
    approx(eng.entropy.position, -1.0)

    # Duplicate receipt / second reconcile does NOT double-count fill
    assert "hedge:lt-202" in eng._settled_order_ids
    assert eng._mark_order_settled("hedge:lt-202") is False

    # Now book refresh resumes normal scanning
    eng.entropy.set_book(100.20, 100.22)
    eng.hedge.set_book(99.99, 100.01)
    assert run_scan(eng) is not None


def test_p0_1_restart_with_pending_order_blocks_until_reconciled():
    tmp_dir = tempfile.mkdtemp(prefix="entropy_p01_")
    extra = f"""
persistence:
  enabled: true
  state_dir: "{tmp_dir}"
  lock_enabled: false
"""
    eng1 = make_engine(extra, midline=0.0, upper=2.0, lower=2.0)
    eng1.entropy.set_book(100.14, 100.16)
    eng1.hedge.set_book(99.99, 100.01)
    eng1._register_pending_order(
        eng1.hedge, "lt-999", is_buy=True, qty=1.0, limit_px=100.02,
        known_filled=0.0, purpose="arb", reduce_only=False,
    )

    # Restart into fresh Engine -> must load pending order and block _scan()
    eng2 = make_engine(extra, midline=0.0, upper=2.0, lower=2.0)
    eng2.entropy.set_book(100.14, 100.16)
    eng2.hedge.set_book(99.99, 100.01)
    assert eng2.load_state() is True
    assert eng2.has_pending_reconcile() is True
    assert run_scan(eng2) is None

    # Resolve pending order (explicitly rejected / canceled) + check positions
    eng2.hedge.order_queries["lt-999"] = {
        "status": "canceled", "outcome": "canceled", "filled_base": 0.0,
        "avg_px": None, "err": None, "unresolved": False, "client_order_id": "lt-999"
    }
    eng2.entropy.chain_position = 0.0
    eng2.hedge.chain_position = 0.0
    assert asyncio.run(eng2._reconcile_pending_orders()) is True
    assert eng2.has_pending_reconcile() is False
    assert run_scan(eng2) is not None


def test_p0_1_venue_adapters_query_order_hl_and_lighter():
    # HL adapter query_order: filled, unknownOid, network error
    hl_conf = VenueConf(
        key="entropy", kind="hl", label="ENTROPY", symbol="SNDK",
        fee_bps=0.0, cap_usd=1000.0, orders_per_min=120, hl_dex="io",
        hl_creds=HLCreds("0x" + "1" * 64, "0x" + "a" * 40),
    )
    hl = HLVenue(hl_conf, "https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws",
                 session=None, settle_timeout_sec=1.0)

    async def hl_filled(_p):
        return {"status": "order", "order": {"status": "filled", "order": {
            "oid": 777, "origSz": "2.0", "sz": "0.0", "limitPx": "100.10"}}}

    hl._info = hl_filled
    r = asyncio.run(hl.query_order("0xcloid1"))
    assert not r["unresolved"] and r["outcome"] == "filled" and abs(r["filled_base"] - 2.0) < 1e-9

    async def hl_unknown(_p):
        return {"status": "unknownOid"}

    hl._info = hl_unknown
    r2 = asyncio.run(hl.query_order("0xcloid2"))
    assert r2["unresolved"] and r2["outcome"] == "unknown" and r2["filled_base"] == 0.0

    # Lighter adapter query_order: late WS terminal cache + REST accountInactiveOrders
    lt_conf = VenueConf(
        key="hedge", kind="lighter", label="RH", symbol="SNDK",
        fee_bps=0.0, cap_usd=1000.0, orders_per_min=30,
        lighter_profile=LIGHTER_PROFILES["lighter-rh"],
        lighter_creds=LighterCreds(123, 0, "priv"),
    )
    lt = LighterVenue(lt_conf, session=None, settle_timeout_sec=1.0)
    lt.market_id = 42
    lt.orders_feed = AccountOrdersFeed("RH", "wss://x", 42, 123, signer=None)
    lt.orders_feed._resolve(555, {"status": "filled", "filled_base": 1.5, "avg_px": 100.0})
    r3 = asyncio.run(lt.query_order("555"))
    assert not r3["unresolved"] and r3["outcome"] == "filled" and abs(r3["filled_base"] - 1.5) < 1e-9

    async def lt_rest(_path, params=None):
        return {"orders": [{"client_order_index": 556, "status": "filled",
                            "filled_base_amount": "0.75", "filled_quote_amount": "75.0"}]}

    lt._get = lt_rest
    r4 = asyncio.run(lt.query_order("556"))
    assert not r4["unresolved"] and r4["outcome"] == "filled" and abs(r4["filled_base"] - 0.75) < 1e-9


# --------------------------------------------------------------- P0-2 tests

def test_p0_2_two_leg_strict_reduce_only_asymmetric_positions():
    eng = make_engine(
        midline=0.0, upper=2.0, lower=2.0, leg_slip=2.0,
        extra_yaml="""
risk:
  max_taker_fee_bps: 2.0
""",
    )
    # Trigger reduce-only mode via fee spike
    eng.circuit_breaker.update_taker_fee(9.0)
    assert eng.is_reduce_only() is True
    eng.entropy.set_book(100.14, 100.16, sz=50.0)
    eng.hedge.set_book(99.99, 100.01, sz=50.0)

    # Case 1: Entropy +2.0, Hedge 0.0 -> NO two-leg arb (Hedge must NOT open long!)
    eng.entropy.position = 2.0
    eng.hedge.position = 0.0
    assert run_scan(eng) is None
    # Unmatched +2.0 exposure is delegated to single-leg _maybe_hedge with reduce_only=True
    asyncio.run(eng._evaluate())
    assert len(eng.hedge.sent_orders) == 0
    assert len(eng.entropy.sent_orders) == 1
    assert eng.entropy.sent_orders[0]["reduce_only"] is True
    assert eng.entropy.sent_orders[0]["is_buy"] is False

    # Case 2: Entropy +2.0, Hedge -0.5 -> two-leg qty strictly capped to 0.5, BOTH legs reduce_only=True
    eng.entropy.sent_orders.clear()
    eng.hedge.sent_orders.clear()
    eng.entropy.position = 2.0
    eng.hedge.position = -0.5
    eng.entropy.set_book(100.14, 100.16, sz=50.0)
    eng.hedge.set_book(99.99, 100.01, sz=50.0)
    best = run_scan(eng)
    assert best is not None
    buy, sell, plan = best
    assert sell.key == "entropy" and buy.key == "hedge"
    assert plan.qty <= 0.5 + 1e-9
    asyncio.run(eng._execute(buy, sell, plan, reduce_only=True))
    assert eng.hedge.sent_orders[0]["reduce_only"] is True
    assert eng.entropy.sent_orders[0]["reduce_only"] is True
    assert eng.hedge.sent_orders[0]["qty"] <= 0.5 + 1e-9

    # Case 3: Opposite positions (Entropy -1.5, Hedge +1.0) -> buy_entropy capped to 1.0
    eng.entropy.position = -1.5
    eng.hedge.position = +1.0
    approx(eng._two_leg_reducible_qty(buy=eng.entropy, sell=eng.hedge), 1.0)
    approx(eng._two_leg_reducible_qty(buy=eng.hedge, sell=eng.entropy), 0.0)

    # Case 4: Same-side positions (Entropy +2.0, Hedge +1.0) -> 0 two-leg reducible
    eng.entropy.position = +2.0
    eng.hedge.position = +1.0
    approx(eng._two_leg_reducible_qty(buy=eng.entropy, sell=eng.hedge), 0.0)
    approx(eng._two_leg_reducible_qty(buy=eng.hedge, sell=eng.entropy), 0.0)


# --------------------------------------------------------------- P1-1 tests

def test_p1_1_hedge_timeout_no_duplicate_retry_and_rounding_cap():
    # Coarse tick 0.10 to verify rounding NEVER breaches hedge_max_slippage_bps=15 bps!
    eng = make_engine(
        px_step_e=0.10, px_step_h=0.10,
        extra_yaml="""
execution:
  hedge_max_retries: 3
  hedge_retry_delay_sec: 0.0
  hedge_slippage_bps: 40.0
  hedge_max_slippage_bps: 15.0
""",
    )
    eng.entropy.set_book(100.00, 100.10)
    eng.hedge.set_book(100.00, 100.10)
    eng.entropy.position = 2.0

    # Hedge times out -> MUST enter P0-1 pending reconcile and NOT send a 2nd retry!
    eng.entropy.send_responses = [
        {"status": "timeout", "outcome": "unknown", "filled_base": 0.0, "avg_px": None,
         "err": None, "unresolved": True, "client_order_id": "hedge-t1"}
    ]
    asyncio.run(eng._hedge(2.0))
    assert len(eng.entropy.sent_orders) == 1
    assert eng.has_pending_reconcile() is True
    assert eng.halted is False
    # 100.00 * (1 - 0.0015) = 99.85; with 0.10 tick, conservative round_up -> 99.90 (10 bps <= 15 bps!)
    approx(eng.entropy.sent_orders[0]["limit_px"], 99.90)

    # Late fill resolves full 2.0 -> net returns to 0 cleanly
    eng.entropy.order_queries["hedge-t1"] = {
        "status": "filled", "outcome": "filled", "filled_base": 2.0,
        "avg_px": 99.95, "err": None, "unresolved": False, "client_order_id": "hedge-t1"
    }
    eng.entropy.chain_position = 0.0
    eng.hedge.chain_position = 0.0
    assert asyncio.run(eng._reconcile_pending_orders()) is True
    assert eng.has_pending_reconcile() is False
    approx(eng.entropy.position, 0.0)


def test_p1_1_hedge_partial_fill_recomputes_qty_and_finishes_within_tolerance():
    eng = make_engine(
        extra_yaml="""
execution:
  hedge_max_retries: 3
  hedge_retry_delay_sec: 0.0
  hedge_slippage_bps: 10.0
  hedge_max_slippage_bps: 10.0
""",
    )
    eng.entropy.set_book(100.00, 100.02)
    eng.hedge.set_book(100.00, 100.02)
    eng.entropy.position = 2.0
    eng.entropy.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 1.2, "avg_px": 99.98,
         "err": None, "unresolved": False, "client_order_id": "h-p1"},
        {"status": "filled", "outcome": "filled", "filled_base": 0.8, "avg_px": 99.97,
         "err": None, "unresolved": False, "client_order_id": "h-p2"},
    ]
    asyncio.run(eng._hedge(2.0))
    assert len(eng.entropy.sent_orders) == 2
    approx(eng.entropy.sent_orders[0]["qty"], 2.0)
    approx(eng.entropy.sent_orders[1]["qty"], 0.8)  # recomputed after 1.2 partial fill
    assert eng.halted is False
    approx(eng.entropy.position, 0.0)


# --------------------------------------------------------------- P1-2 tests

def test_p1_2_tightens_500_dollar_entry_to_a_profitable_worst_case():
    # With 10 bps available and a 4 bps floor, a full 50 bps on each leg
    # would lose money. The actual order limits must use only the excess.
    eng = make_engine(midline=0.0, upper=4.0, lower=4.0, leg_slip=50.0)
    eng.entropy.set_book(100.10, 100.12)
    eng.hedge.set_book(99.98, 100.00)
    plan = ArbPlan(
        qty=5.0, buy_limit=100.00, sell_limit=100.10,
        buy_notional=500.00, sell_notional=500.50,
        q_max=10.0, q_max_notional=1000.0,
        top_premium_bps=10.0, marginal_premium_bps=10.0,
        buy_fee=0.0, sell_fee=0.0,
    )
    approx(plan.exp_edge_usd, 0.50)
    asyncio.run(eng._execute(eng.hedge, eng.entropy, plan, reduce_only=False))
    buy = eng.hedge.sent_orders[0]["limit_px"]
    sell = eng.entropy.sent_orders[0]["limit_px"]
    assert plan.buy_limit < buy < 100.5
    assert 100.10 * 0.995 < sell < plan.sell_limit
    assert eng._worst_case_check(eng.hedge, eng.entropy, plan.qty, buy, sell)[0]

    # Active reduce-only exit with wide slippage still allowed when within reduce_only_min_worst_bps (-100)
    eng.cfg.reduce_only_min_worst_bps = -100.0
    eng.entropy.position = 5.0
    eng.hedge.position = -5.0
    asyncio.run(eng._execute(eng.hedge, eng.entropy, plan, reduce_only=True))
    assert len(eng.hedge.sent_orders) == 2
    assert len(eng.entropy.sent_orders) == 2
    assert eng.hedge.sent_orders[-1]["reduce_only"]
    assert eng.entropy.sent_orders[-1]["reduce_only"]


def test_p1_2_worst_case_passes_when_rounded_bounds_clear_threshold():
    eng = make_engine(midline=0.0, upper=4.0, lower=4.0, leg_slip=2.0,
                      px_step_e=0.01, px_step_h=0.01)
    eng.entropy.set_book(100.10, 100.12)
    eng.hedge.set_book(99.98, 100.00)
    plan = ArbPlan(
        qty=5.0, buy_limit=100.00, sell_limit=100.10,
        buy_notional=500.00, sell_notional=500.50,
        q_max=10.0, q_max_notional=1000.0,
        top_premium_bps=10.0, marginal_premium_bps=10.0,
        buy_fee=0.0, sell_fee=0.0,
    )
    # buy_bound=100.02, sell_bound=100.08 -> worst_net_bps = +5.998 bps >= 4.0 -> PASSES
    asyncio.run(eng._execute(eng.hedge, eng.entropy, plan, reduce_only=False))
    assert len(eng.hedge.sent_orders) == 1 and len(eng.entropy.sent_orders) == 1


# --------------------------------------------------------------- P1-3 tests

def test_p1_3_conservative_fee_in_plan_and_hl_userfills_lookup():
    eng = make_engine(
        midline=0.0, upper=4.0, lower=4.0, leg_slip=1.0,
        extra_yaml="""
risk:
  max_taker_fee_bps: 2.0
""",
    )
    # Config fee = 0.0 bps, observed = 1.8 bps (< 2.0 limit -> not tripped)
    eng.entropy.live_fee_bps = 1.8
    asyncio.run(eng._check_entropy_fee_once())
    assert not eng.circuit_breaker.is_reduce_only
    approx(eng.effective_fee_bps(eng.entropy), 1.8)

    # Spread = +5.0 bps: would pass with 0 bps fee (> 4.0), MUST fail with 1.8 bps fee (5.0 - 1.8 = 3.2 < 4.0)
    eng.entropy.set_book(100.05, 100.07)
    eng.hedge.set_book(99.98, 100.00)
    plan, reason = eng._plan(eng.hedge, eng.entropy, 500.0)
    assert plan is None and reason == "no_edge"

    # HL IOC receipt omits fee -> falls back to userFills (fetch_fill_fee) and separates actual vs estimated
    eng.entropy.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 1.0, "avg_px": 100.14,
         "order_id": 888, "err": None, "unresolved": False, "client_order_id": "hl-no-fee"}
    ]
    eng.entropy.fill_fee_response = {
        "fee_usd": 0.09, "fee_bps": 9.0, "fee_token": "USDC",
        "order_id": 888, "coin": "io:SNDK", "estimated": False, "filled_base": 1.0,
    }
    eng.hedge.send_responses = [
        {"status": "filled", "outcome": "filled", "filled_base": 1.0, "avg_px": 100.00,
         "err": None, "unresolved": False, "client_order_id": "lt-ok"}
    ]
    p2 = ArbPlan(
        qty=1.0, buy_limit=100.00, sell_limit=100.14,
        buy_notional=100.00, sell_notional=100.14,
        q_max=10.0, q_max_notional=1000.0,
        top_premium_bps=14.0, marginal_premium_bps=14.0,
        buy_fee=0.0, sell_fee=0.00018,
    )
    eng.entropy.set_book(100.14, 100.16)
    asyncio.run(eng._execute(eng.hedge, eng.entropy, p2))
    assert eng.circuit_breaker.fee_tripped is True
    approx(eng.daily_pnl_breakdown["actual_fee_usd"], 0.09)
    approx(eng.daily_pnl_breakdown["estimated_fee_usd"], 0.0)


def test_p1_3_unknown_fee_and_margin_pause_new_risk_and_recover():
    eng = make_engine(
        midline=0.0, upper=2.0, lower=2.0,
        extra_yaml="""
risk:
  max_taker_fee_bps: 2.0
  min_available_margin_usd: 100.0
""",
    )
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)

    # Fee query fails (None) -> pauses new entries (reduce-only)
    eng.entropy.live_fee_bps = None
    asyncio.run(eng._check_entropy_fee_once())
    assert eng.circuit_breaker.fee_unknown is True
    assert eng.is_reduce_only() is True
    assert run_scan(eng) is None

    # Verified two-leg reduction is still permitted while fee is unknown
    eng.entropy.position = 1.0
    eng.hedge.position = -1.0
    assert run_scan(eng) is not None

    # Fee recovers to 0.9 bps -> fee_unknown cleared
    eng.entropy.position = 0.0
    eng.hedge.position = 0.0
    eng.entropy.live_fee_bps = 0.9
    asyncio.run(eng._check_entropy_fee_once())
    assert eng.circuit_breaker.fee_unknown is False

    # Margin query fails (free=None) -> pauses new entries
    eng.entropy.equity, eng.entropy.free = 500.0, None
    eng.hedge.equity, eng.hedge.free = 500.0, 300.0
    asyncio.run(eng._check_margin_once())
    assert "ENTROPY" in eng.circuit_breaker.margin_unknown_venues
    assert eng.is_reduce_only() is True

    # Margin recovers -> entries resume
    eng.entropy.free = 250.0
    asyncio.run(eng._check_margin_once())
    assert not eng.circuit_breaker.margin_unknown_venues
    assert eng.is_reduce_only() is False


# --------------------------------------------------------------- P1-4 tests

def test_p1_4_daily_mtm_cross_day_reset_same_day_restart_and_breakdown():
    tmp_dir = tempfile.mkdtemp(prefix="entropy_p14_")
    extra = f"""
risk:
  max_daily_loss_usd: 100.0
  breaker_action: reduce_only
persistence:
  enabled: true
  state_dir: "{tmp_dir}"
  lock_enabled: false
"""
    t_day1 = datetime(2026, 7, 15, 15, 0, tzinfo=timezone.utc).timestamp()
    t_day2 = datetime(2026, 7, 16, 15, 0, tzinfo=timezone.utc).timestamp()

    eng1 = make_engine(extra, midline=0.0, upper=2.0, lower=2.0)
    eng1.entropy.set_book(100.00, 100.02)
    eng1.hedge.set_book(100.00, 100.02)
    # Establish Day 1 MTM baseline at $0
    approx(eng1.daily_mtm_pnl(now=t_day1), 0.0)

    # Day 1 loses $110 -> breaches $100 daily loss limit
    eng1.entropy.cash = -110.0
    approx(eng1.daily_mtm_pnl(now=t_day1 + 60), -110.0)
    tripped, _ = eng1.circuit_breaker.check_losses(
        mtm_pnl_usd=eng1.daily_mtm_pnl(now=t_day1 + 60), now=t_day1 + 60)
    assert tripped is True and eng1.circuit_breaker.is_reduce_only is True
    eng1.save_state(now=t_day1 + 60)

    # Same-day restart restores Day 1 baseline & keeps breaker tripped
    eng_same = make_engine(extra, midline=0.0, upper=2.0, lower=2.0)
    eng_same.entropy.set_book(100.00, 100.02)
    eng_same.hedge.set_book(100.00, 100.02)
    eng_same.load_state(now=t_day1 + 120)
    approx(eng_same.daily_mtm_pnl(now=t_day1 + 120), -110.0)
    assert eng_same.circuit_breaker.is_reduce_only is True

    # Next trading day with zero new moves: prior-day -$110 MUST NOT trigger today's $100 rule!
    eng_next = make_engine(extra, midline=0.0, upper=2.0, lower=2.0)
    eng_next.entropy.set_book(100.00, 100.02)
    eng_next.hedge.set_book(100.00, 100.02)
    eng_next.entropy.cash = -110.0
    eng_next.load_state(now=t_day2)
    approx(eng_next.daily_mtm_pnl(now=t_day2), 0.0)
    assert eng_next.circuit_breaker.is_reduce_only is False
    # External transfer does not count as trading loss
    eng_next.record_transfer(-200.0, now=t_day2 + 10)
    eng_next.entropy.cash -= 200.0
    approx(eng_next.daily_mtm_pnl(now=t_day2 + 20), 0.0)
    # Confirmed funding loss IS tracked separately and triggers breaker if large
    assert eng_next.daily_pnl_breakdown["funding_status"] == "unknown"
    eng_next.record_funding_pnl(-105.0, now=t_day2 + 30)
    assert eng_next.daily_pnl_breakdown["funding_status"] == "confirmed"
    assert eng_next.circuit_breaker.is_reduce_only is True


# ------------------------------------------- Three hedge venues & example check

def test_all_three_hedge_venues_and_example_reconcile_sec():
    for hv in ("lighter", "lighter-rh", "tradexyz"):
        cfg = load_config(EXAMPLE, NO_ENV, symbol="SNDK", hedge_venue=hv)
        assert cfg.reconcile_sec == 15.0
        assert cfg.check_worst_case_spread is True
        assert cfg.dynamic_midline.enabled is False
        assert cfg.funding_skew.enabled is False
        assert cfg.session_filter.enabled is False
        eng = Engine(cfg, record_only=True)
        assert eng.record_only is True and eng.instance_lock is None


def test_unknown_transport_and_incomplete_success_keep_original_ids(monkeypatch):
    from types import SimpleNamespace
    from aiohttp import ServerDisconnectedError

    monkeypatch.setitem(sys.modules, "lighter", SimpleNamespace(SignerClient=SimpleNamespace(
        ORDER_TYPE_MARKET=1, ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL=0, DEFAULT_IOC_EXPIRY=0,
    )))
    cfg = make_cfg()
    venue = LighterVenue(cfg.hedge, session=None, settle_timeout_sec=1.0)

    async def submit(**kwargs):
        assert kwargs["client_order_index"] == 12345
        raise ServerDisconnectedError()

    venue.signer = SimpleNamespace(create_order=submit)
    result = asyncio.run(venue.send_taker(is_buy=True, qty=1.0, limit_px=100.0,
                                          client_order_id="12345"))
    assert result["unresolved"] and result["client_order_id"] == "12345"
    assert HLVenue._parse({"status": "ok", "response": {"data": {}}})["unresolved"]
    assert HLVenue._parse({"status": "ok", "response": {"data": {"statuses": [
        {"filled": {"totalSz": "bad", "avgPx": "100"}}
    ]}}})["unresolved"]


def _prepare_durable_pair(tmp_path):
    engine = make_engine(f'\npersistence:\n  enabled: true\n  state_dir: "{tmp_path}"\n')
    engine.entropy.reserve_order_id = lambda: "hl-id"
    engine.hedge.reserve_order_id = lambda: "lt-id"
    engine.entropy.set_book(101.00, 101.01)
    engine.hedge.set_book(99.99, 100.00)
    engine._log_csv = lambda *_: None
    plan, _ = engine._plan(engine.hedge, engine.entropy, 100.0)
    assert plan is not None
    return engine, plan


def test_order_journal_precedes_both_sends_and_recovers_interruption(tmp_path):
    engine, plan = _prepare_durable_pair(tmp_path)

    async def submit_and_wait(client_order_id, **_):
        assert client_order_id in {"hl-id", "lt-id"}
        assert set(engine.state_store.load()["pending_orders"]) == {
            "entropy:hl-id", "hedge:lt-id",
        }
        # A background position poll during submission must not reconcile the journal.
        assert not await engine._reconcile_pending_orders()
        entered.set()
        await asyncio.Future()

    async def interrupt():
        engine.hedge.send_taker = engine.entropy.send_taker = submit_and_wait
        task = asyncio.create_task(engine._execute(engine.hedge, engine.entropy, plan))
        await asyncio.wait_for(entered.wait(), 1.0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    entered = asyncio.Event()
    asyncio.run(interrupt())

    resumed, _ = _prepare_durable_pair(tmp_path)
    assert resumed.load_state()
    assert not asyncio.run(resumed._reconcile_pending_orders())
    assert resumed.has_pending_reconcile()
    resumed.entropy.order_queries["hl-id"] = {
        "status": "filled", "filled_base": plan.qty, "avg_px": 101.0, "unresolved": False,
    }
    resumed.hedge.order_queries["lt-id"] = {
        "status": "filled", "filled_base": plan.qty, "avg_px": 100.0, "unresolved": False,
    }
    resumed.entropy.chain_position = -plan.qty

    # Both orders have terminal updates, but the Lighter snapshot is still stale.
    assert not asyncio.run(resumed._reconcile_pending_orders())
    assert resumed.hedge.position == 0 and resumed.hedge.cash == 0
    assert resumed.has_pending_reconcile()
    resumed.hedge.chain_position = plan.qty
    assert asyncio.run(resumed._reconcile_pending_orders())
    assert resumed.hedge.position == plan.qty
    cash = resumed.hedge.cash
    assert asyncio.run(resumed._reconcile_pending_orders())
    assert resumed.hedge.cash == cash == -100 * plan.qty
    assert not resumed.state_store.load()["pending_orders"]


def test_failed_journal_write_blocks_both_submissions(tmp_path):
    engine, plan = _prepare_durable_pair(tmp_path)
    engine.state_store.save = lambda data: False
    assert not asyncio.run(engine._execute(engine.hedge, engine.entropy, plan))
    assert engine.halted
    assert not engine.entropy.sent_orders and not engine.hedge.sent_orders


def test_daily_funding_snapshot_deduplicates_after_restart(tmp_path):
    engine, _ = _prepare_durable_pair(tmp_path)
    ts = time.time()

    async def entropy_funding(start, end):
        assert start <= end
        return -20.0

    async def hedge_funding(start, end):
        return 7.0

    engine.entropy.fetch_funding_pnl = entropy_funding
    engine.hedge.fetch_funding_pnl = hedge_funding
    asyncio.run(engine._poll_funding_history(now=ts))
    assert engine.daily_mtm_pnl(now=ts) == -13.0
    assert engine.circuit_breaker.daily_realized_pnl_usd == -13.0
    restarted, _ = _prepare_durable_pair(tmp_path)
    restarted.entropy.fetch_funding_pnl = entropy_funding
    restarted.hedge.fetch_funding_pnl = hedge_funding
    assert restarted.load_state(now=ts)
    asyncio.run(restarted._poll_funding_history(now=ts))
    assert restarted.circuit_breaker.daily_realized_pnl_usd == -13.0
    assert restarted.daily_mtm_pnl(now=ts) == -13.0


def test_transfer_poll_does_not_count_shared_hl_twice_or_change_mtm(tmp_path):
    engine, _ = _prepare_durable_pair(tmp_path)
    engine.daily_mtm_pnl()
    engine.hedge.include_core_equity = False

    async def transfers(start, end):
        assert start <= end
        return 300.0

    async def duplicate_transfer(start, end):
        raise AssertionError("shared Hyperliquid account queried twice")

    engine.entropy.fetch_external_transfers = transfers
    engine.hedge.fetch_external_transfers = duplicate_transfer
    asyncio.run(engine._poll_transfer_history())
    assert engine.daily_pnl_breakdown["transfers_usd"] == 300.0
    assert engine.daily_pnl_breakdown["transfer_status"] == "confirmed"
    assert engine.daily_mtm_pnl() == 0.0
    assert engine.state_store.load()["daily_pnl_breakdown"]["transfers_usd"] == 300.0


def test_lighter_funding_reads_one_complete_day(monkeypatch):
    from types import SimpleNamespace

    ts = 1_800_000_000
    cfg = make_cfg()
    venue = LighterVenue(cfg.hedge, session=None, settle_timeout_sec=1.0)
    venue.conf.lighter_creds = LighterCreds(123, 0, "fixture")
    venue.market_id = 42
    venue.signer = SimpleNamespace(api_client=None,
                                   create_auth_token_with_expiry=lambda: ("fixture", None))

    class AccountApi:
        def __init__(self, _):
            pass

        async def position_funding(self, **kwargs):
            assert kwargs["market_ids"] == [42]
            if kwargs["cursor"] is None:
                return SimpleNamespace(code=200, next_cursor="next", position_fundings=[
                    SimpleNamespace(timestamp=ts * 1000, market_id=42, funding_id=1, change="-5.5"),
                    SimpleNamespace(timestamp=ts, market_id=7, funding_id=2, change="20"),
                ])
            return SimpleNamespace(code=200, next_cursor=None, position_fundings=[
                SimpleNamespace(timestamp=ts - 100, market_id=42, funding_id=3, change="1.5"),
                SimpleNamespace(timestamp=ts - 2000, market_id=42, funding_id=4, change="75"),
            ])

    monkeypatch.setitem(sys.modules, "lighter", SimpleNamespace(AccountApi=AccountApi))
    assert asyncio.run(venue.fetch_funding_pnl(ts - 1500, ts + 10)) == -4.0
