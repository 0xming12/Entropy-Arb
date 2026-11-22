import argparse
import json
from zipfile import ZipFile

from tools.replay_depth import run_replay


def test_replay_reuses_live_price_bounds_and_closes_pair(tmp_path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text("""
thresholds: {midline_bps: 0.0, upper_bps: 7.5, lower_bps: 7.5}
sizing: {max_order_notional_usd: 15, min_order_notional_usd: 10}
execution: {premium_persist_sec: 2.0, cooldown_sec: 0.0, leg_slippage_bps: 50.0}
risk: {max_book_age_sec: 5}
""")
    cutoff = 1795185000

    def book(bid, ask):
        return {"bids": [[bid, 10]], "asks": [[ask, 10]],
                "fresh": True, "update_age_sec": 0.1}

    def row(i):
        return {"ts": cutoff + i,
                "entropy": book(100.21, 100.22) if i < 4 else book(99.78, 99.79),
                "hedge": book(100.00, 100.01)}

    snapshot = tmp_path / "test.zip"
    with ZipFile(snapshot, "w") as z:
        z.writestr("fixture-minutes.csv", "minute_ts,premium_mean_bps,samples\n" +
                   f"{cutoff-60},0.0,60\n" + f"{cutoff},123.0,60\n")
        z.writestr("fixture-minutes.depth-2026-11-20.jsonl",
                   "".join(json.dumps(row(i)) + "\n" for i in range(8)))
    args = argparse.Namespace(
        config=cfg, snapshot=snapshot, symbol="NVDA", hedge="lighter",
        calibration_until=cutoff, until=None, entropy_size_decimals=3,
        hedge_size_decimals=3, lighter_price_decimals=3, min_base=0.025,
        min_notional=10, entropy_fee_bps=1, hedge_fee_bps=0,
        slippage_bps=0.5, funding_per_hour_bps=1,
    )
    result = run_replay(args)
    assert result["calibrated_midline_bps"] == 0.0
    assert result["simulated_entries"] == result["simulated_exits"] == 1
    assert result["closed_pnl_quote"] > 0
