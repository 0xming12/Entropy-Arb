import asyncio
import sys

import pytest

import main
from entropy_arb.book import OrderBook, plan_arb
from entropy_arb.config import load_config
from entropy_arb.engine import Engine
from test_live_risk_regression import make_engine, run_scan


def test_cap_uses_second_ask_level_not_best_ask_times_quantity():
    buy, sell = OrderBook(), OrderBook()
    buy.apply_hl([[{"px": "99.98", "sz": "1"}],
                  [{"px": "100", "sz": "0.04"}, {"px": "100.10", "sz": "1"}]])
    sell.apply_hl([[{"px": "99.99", "sz": "1"}], [{"px": "100", "sz": "1"}]])
    plan, reason = plan_arb(buy, sell, threshold_bps=-20, buy_fee_bps=0,
                            sell_fee_bps=0, take_fraction=0.5, cap_notional=15,
                            min_base=0.025, min_notional=10, size_step=0.001)
    assert reason == "ok" and plan.qty == pytest.approx(0.149)
    assert plan.buy_notional == pytest.approx(14.9109)
    assert plan.sell_notional <= 15


def test_entry_uses_each_venue_mark_and_rechecks_when_the_mark_moves():
    engine = make_engine(midline=0, upper=7.5, lower=7.5, leg_slip=0,
                         hedge="lighter", px_step_e=.001, px_step_h=.001)
    engine._step, engine._min_base = .001, .025
    buy, sell = engine.hedge, engine.entropy
    buy.cap_usd = sell.cap_usd = 20
    engine.cfg.max_order_notional = 16  # Isolate the position limit from the order limit.
    buy.set_book(99.99, 100)
    sell.set_book(100.320, 100.328)
    buy.position, sell.position = .05, -.05
    chosen_buy, chosen_sell, plan = run_scan(engine)
    assert (chosen_buy, chosen_sell) == (buy, sell) and plan.qty == pytest.approx(.149)
    assert (abs(sell.position) + plan.qty) * sell.book.mid() <= 20
    # Book still fills at the selected limit, but its own mid is now higher.
    sell.set_book(100.320, 101.680)
    asyncio.run(engine._execute(buy, sell, plan))
    assert buy.sent_orders[0]["qty"] == sell.sent_orders[0]["qty"] == pytest.approx(.148)
    assert abs(sell.position) * sell.book.mid() <= 20


def test_final_price_cap_shrinks_order_and_rejects_below_minimum():
    engine = make_engine(midline=0, upper=1, lower=1, leg_slip=50, hedge="lighter")
    engine.cfg.max_order_notional = 15
    engine.cfg.check_worst_case_spread = False
    engine._step, engine._min_base = .001, .025
    buy, sell = engine.hedge, engine.entropy
    buy.set_book(99.99, 100)
    sell.set_book(100.10, 100.11)
    plan, _ = engine._plan(buy, sell, 15)
    asyncio.run(engine._execute(buy, sell, plan))
    order = buy.sent_orders[0]
    assert order["qty"] == pytest.approx(.149) and order["qty"] * order["limit_px"] <= 15
    engine.cfg.max_order_notional = 10
    for venue in engine.venues.values():
        venue.position = 0
        venue.sent_orders.clear()
    asyncio.run(engine._execute(buy, sell, plan))
    assert not buy.sent_orders and not sell.sent_orders


@pytest.mark.parametrize(("mode", "record_only"), [([], True), (["--record-only"], True),
                                                  (["--live"], False)])
def test_cli_mode_is_explicit(monkeypatch, tmp_path, mode, record_only):
    config = tmp_path / "config.yaml"
    config.write_text("thresholds: {midline_bps: 0, upper_bps: 4, lower_bps: 4}")
    seen = []

    async def fake_main(cfg, **kwargs):
        seen.append(kwargs["record_only"])

    monkeypatch.setattr(main, "amain", fake_main)
    monkeypatch.setattr(main, "setup_logging", lambda *_: None)
    monkeypatch.setattr(sys, "argv", ["main.py", "--symbol", "NVDA", "--hedge", "lighter",
                                    "--config", str(config), "--env-file", "/dev/null",
                                    "--no-dashboard", *mode])
    main.main()
    assert seen == [record_only]


def test_replay_config_blocks_live_even_if_renamed(monkeypatch, tmp_path, capsys):
    config = tmp_path / "renamed.yaml"
    with open("config.nvda-replay.yaml") as original:
        config.write_text(original.read())
    monkeypatch.setattr(sys, "argv", ["main.py", "--symbol", "NVDA", "--hedge", "lighter",
                                    "--config", str(config), "--env-file", "/dev/null", "--live"])
    with pytest.raises(SystemExit, match="2"):
        main.main()
    assert "replay_only" in capsys.readouterr().err
    cfg = load_config(str(config), "/dev/null", symbol="NVDA", hedge_venue="lighter")
    with pytest.raises(RuntimeError, match="replay_only"):
        asyncio.run(Engine(cfg).run())
