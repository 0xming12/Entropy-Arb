"""Regression guards for submit timeouts, restart, and fill accounting."""
import asyncio
import sys
from types import SimpleNamespace

import pytest

from test_live_risk_regression import LighterVenue, _prepare_durable_pair, make_cfg, make_engine


def test_submission_deadline_keeps_lighter_order_pending(monkeypatch):
    monkeypatch.setitem(sys.modules, "lighter", SimpleNamespace(SignerClient=SimpleNamespace(
        ORDER_TYPE_MARKET=1, ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL=0, DEFAULT_IOC_EXPIRY=0,
    )))
    venue = LighterVenue(make_cfg().hedge, session=None, settle_timeout_sec=0.01)

    async def probe():
        canceled = False
        unwatched = []
        pending = asyncio.get_running_loop().create_future()

        async def submit(**kwargs):
            try:
                await asyncio.Future()
            finally:
                nonlocal canceled
                canceled = True

        venue.signer = SimpleNamespace(create_order=submit)
        venue.orders_feed = SimpleNamespace(
            watch=lambda coi: pending,
            unwatch=lambda coi: (unwatched.append(coi), pending.cancel()),
        )
        outcome = await asyncio.wait_for(venue.send_taker(
            is_buy=True, qty=1.0, limit_px=100.0, client_order_id="12345"), 1.0)
        assert outcome["status"] == "send-unknown"
        assert outcome["unresolved"] and outcome["client_order_id"] == "12345"
        assert canceled and unwatched == [12345]

    asyncio.run(probe())


def test_restart_clears_only_locally_proven_unsent_orders(tmp_path):
    engine, plan = _prepare_durable_pair(tmp_path)
    ids = engine._stage_orders(
        ((engine.hedge, True, plan.buy_limit), (engine.entropy, False, plan.sell_limit)),
        qty=plan.qty, purpose="arb", reduce_only=False)
    assert ids and set(o["submission"] for o in engine.state_store.load()["pending_orders"].values()) == {"prepared"}

    async def submit(**kwargs):
        journal = engine.state_store.load()["pending_orders"]
        assert journal["hedge:lt-id"]["submission"] == "attempted"
        assert journal["entropy:hl-id"]["submission"] == "prepared"
        return {"status": "timeout", "filled_base": 0, "avg_px": None, "unresolved": True}

    engine.hedge.send_taker = submit
    asyncio.run(engine._send_staged_order(engine.hedge, ids, is_buy=True, qty=plan.qty,
                                         limit_px=plan.buy_limit, reduce_only=False))

    restarted, _ = _prepare_durable_pair(tmp_path)
    assert restarted.load_state()

    async def unexpected_lookup(_):
        raise AssertionError("prepared order must not query the venue")

    restarted.entropy.query_order = unexpected_lookup
    assert not asyncio.run(restarted._reconcile_pending_orders())
    assert restarted.has_pending_reconcile()  # Lighter lookup still says unknown.

    restarted.hedge.order_queries["lt-id"] = {
        "status": "filled", "filled_base": plan.qty, "avg_px": 100.0, "unresolved": False,
    }
    assert not asyncio.run(restarted._reconcile_pending_orders())
    assert restarted.has_pending_reconcile()  # Must verify chain before clearing either ID.
    restarted.hedge.chain_position = plan.qty
    assert asyncio.run(restarted._reconcile_pending_orders())
    assert not restarted.state_store.load()["pending_orders"]


def test_restart_without_submissions_checks_both_positions_before_clear(tmp_path):
    engine, plan = _prepare_durable_pair(tmp_path)
    engine._stage_orders(
        ((engine.hedge, True, plan.buy_limit), (engine.entropy, False, plan.sell_limit)),
        qty=plan.qty, purpose="arb", reduce_only=False)

    restarted, _ = _prepare_durable_pair(tmp_path)
    assert restarted.load_state()
    restarted.entropy.chain_position = -plan.qty
    assert not asyncio.run(restarted._reconcile_pending_orders())
    assert restarted.has_pending_reconcile()
    restarted.entropy.chain_position = 0.0
    assert asyncio.run(restarted._reconcile_pending_orders())
    assert not restarted.has_pending_reconcile()


def test_strict_startup_keeps_unmatched_saved_ledger(tmp_path):
    engine, _ = _prepare_durable_pair(tmp_path)
    engine.entropy.position = 1.0
    engine.entropy.cash = -100.0
    assert engine.save_state()

    restarted, _ = _prepare_durable_pair(tmp_path)
    assert restarted.load_state()
    previous = restarted.state_store.load()
    with pytest.raises(RuntimeError, match="saved position .* differs from exchange position"):
        asyncio.run(restarted._reconcile_positions(hedge=False, strict=True))
    assert restarted.entropy.position == 1.0 and restarted.entropy.cash == -100.0
    assert restarted.state_store.load() == previous


def test_strict_startup_rejects_position_without_saved_ledger():
    engine = make_engine()
    engine.entropy.chain_position = 0.1
    with pytest.raises(RuntimeError, match="no saved ledger"):
        asyncio.run(engine._reconcile_positions(hedge=False, strict=True))
    engine.entropy.chain_position = 0.0
    asyncio.run(engine._reconcile_positions(hedge=False, strict=True))


def test_canceled_iocs_do_not_count_as_fills(tmp_path, monkeypatch):
    engine, plan = _prepare_durable_pair(tmp_path)

    async def submit(**kwargs):
        return {"status": "canceled", "outcome": "canceled", "filled_base": 0.0,
                "avg_px": None, "unresolved": False, "err": None}

    for venue in engine.venues.values():
        monkeypatch.setattr(venue, "send_taker", submit)
    asyncio.run(engine._execute_pair(engine.hedge, engine.entropy, plan, False))
    assert engine.recent_trades[-1]["status"] == "canceled/canceled"
    assert engine.recent_trades[-1]["exp"] == 0
    assert not engine.recent_trades[-1]["ok"]
    assert engine.trades == engine.total_exp_edge == engine.total_fill_edge == 0
    assert not engine.has_pending_reconcile()
