"""Config loading: example file, validation, CLI-selected markets.

Run:  python3 -m pytest tests/  (or  python3 tests/test_config.py)
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.config import ConfigError, load_config  # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), "..")
EXAMPLE = os.path.join(ROOT, "config.example.yaml")
NO_ENV = os.path.join(tempfile.gettempdir(), "entropy-arb-no-such.env")


def write_tmp(text: str) -> str:
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    f.write(text)
    f.close()
    return f.name


MINIMAL = """
thresholds:
  midline_bps: 5.0
  upper_bps: 4.0
  lower_bps: 3.0
"""


def load(yaml_text: str, symbol="SNDK", hedge="lighter-rh"):
    return load_config(write_tmp(yaml_text), NO_ENV,
                       symbol=symbol, hedge_venue=hedge)


def test_example_config_loads():
    cfg = load_config(EXAMPLE, NO_ENV,
                      symbol="SNDK", hedge_venue="lighter-rh")
    assert cfg.symbol == "SNDK"
    assert cfg.entropy.kind == "hl" and cfg.entropy.hl_dex == "io"
    assert cfg.hedge_venue == "lighter-rh"
    assert cfg.hedge.kind == "lighter"
    assert cfg.hedge.lighter_profile.chain_id == 466324
    assert cfg.entropy.symbol == "SNDK" and cfg.hedge.symbol == "SNDK"
    assert cfg.recorder_enabled and cfg.recorder_csv
    assert cfg.dashboard and cfg.log_file
    # P0/P1 + T1-T8 example fields
    assert cfg.latency_buffer_bps == 3.0
    assert cfg.risk.max_daily_loss_usd == 100.0
    assert cfg.risk.max_consecutive_losses == 5
    assert cfg.risk.max_taker_fee_bps == 2.0
    assert cfg.risk.fee_check_interval_sec == 60.0
    assert cfg.risk.reduce_only_ignore_session_buffer is True
    assert cfg.risk.max_book_age_sec == 30.0
    assert cfg.risk.max_spread_bps == 150.0
    assert cfg.risk.min_available_margin_usd == 100.0
    assert cfg.risk.reconcile_drift_reduce_only is True
    assert cfg.hedge_max_retries == 3
    assert cfg.hedge_max_slippage_bps == 50.0
    assert cfg.session_filter.enabled is False
    assert "2026-11-26" in cfg.session_filter.holidays
    assert "2026-11-27" in cfg.session_filter.early_close_dates
    assert cfg.dynamic_midline.enabled is False
    assert cfg.dynamic_midline.window_minutes == 180.0
    assert cfg.funding_skew.enabled is False
    assert cfg.funding_skew.min_net_edge_bps == 1.0
    assert cfg.persistence.enabled is True
    assert cfg.persistence.lock_enabled is True
    assert cfg.notify_throttle_sec == 60.0


def test_minimal_defaults():
    cfg = load(MINIMAL, hedge="lighter")
    assert cfg.midline_bps == 5.0 and cfg.upper_bps == 4.0 and cfg.lower_bps == 3.0
    assert cfg.latency_buffer_bps == 0.0     # backward-compatible default
    assert cfg.hedge.label == "LIGHTER"
    assert cfg.hedge.lighter_profile.chain_id == 304
    assert cfg.take_fraction == 0.5          # defaults kick in
    assert cfg.recorder_enabled is True
    assert cfg.risk.max_daily_loss_usd == 0.0
    assert cfg.risk.max_taker_fee_bps == 0.0
    assert cfg.risk.max_book_age_sec == 0.0
    assert cfg.risk.max_spread_bps == 0.0
    assert cfg.risk.min_available_margin_usd == 0.0
    assert cfg.funding_skew.min_net_edge_bps == 0.0
    assert cfg.persistence.enabled is False
    assert cfg.persistence.lock_enabled is False


def test_tradexyz_hedge():
    cfg = load(MINIMAL, hedge="tradexyz")
    assert cfg.hedge.kind == "hl" and cfg.hedge.hl_dex == "xyz"
    assert cfg.hedge.label == "XYZ"


def expect_error(yaml_text: str, needle: str, **kw):
    try:
        load(yaml_text, **kw)
    except ConfigError as e:
        assert needle in str(e), f"{needle!r} not in {e}"
        return
    raise AssertionError(f"expected ConfigError containing {needle!r}")


def test_unknown_key_rejected():
    expect_error(MINIMAL + "\nthresholdz:\n  x: 1\n",
                 "unknown config key 'thresholdz'")
    expect_error(MINIMAL + "\nsizing:\n  take_fractionn: 0.5\n",
                 "sizing.take_fractionn")
    expect_error(MINIMAL + "\nrisk:\n  unknown_key: 1\n",
                 "risk.unknown_key")


def test_p0_p1_config_validation():
    expect_error(MINIMAL + "\nrisk:\n  breaker_action: panic\n",
                 "risk.breaker_action")
    expect_error(MINIMAL + "\nrisk:\n  max_book_age_sec: -1\n",
                 "risk.max_book_age_sec")
    expect_error(MINIMAL + "\nrisk:\n  max_spread_bps: -5\n",
                 "risk.max_spread_bps")
    expect_error(MINIMAL + "\nrisk:\n  min_available_margin_usd: -10\n",
                 "risk.min_available_margin_usd")
    expect_error(MINIMAL + "\nexecution:\n  hedge_max_retries: 0\n",
                 "execution.hedge_max_retries")
    expect_error(MINIMAL + "\nsession_filter:\n  off_hours_mode: invalid\n",
                 "session_filter.off_hours_mode")
    expect_error(MINIMAL + "\nsession_filter:\n  timezone: Mars/Olympus\n",
                 "session_filter.timezone")
    expect_error(MINIMAL + "\nsession_filter:\n  holidays:\n    - '2026/11/26'\n",
                 "YYYY-MM-DD")
    expect_error(MINIMAL + "\ndynamic_midline:\n  method: kalman\n",
                 "dynamic_midline.method")
    expect_error(MINIMAL + "\nfunding_skew:\n  expected_hold_hours: -1\n",
                 "funding_skew.expected_hold_hours")
    expect_error(MINIMAL + "\nfunding_skew:\n  min_net_edge_bps: -0.5\n",
                 "funding_skew.min_net_edge_bps")
    expect_error(
        "thresholds:\n  midline_bps: 0\n  upper_bps: 4\n  lower_bps: 4\n  latency_buffer_bps: -1\n",
        "latency_buffer_bps",
    )


def test_markets_no_longer_config_keys():
    expect_error("symbol: SNDK\n" + MINIMAL, "unknown config key 'symbol'")
    expect_error("hedge_venue: tradexyz\n" + MINIMAL,
                 "unknown config key 'hedge_venue'")


def test_bad_cli_markets():
    expect_error(MINIMAL, "--hedge", hedge="binance")
    expect_error(MINIMAL, "--symbol", symbol="")


def test_missing_thresholds():
    expect_error("recorder:\n  enabled: true\n", "thresholds.")


def test_nonpositive_band():
    expect_error("thresholds:\n"
                 "  midline_bps: 5\n  upper_bps: 0\n  lower_bps: 3\n",
                 "must be > 0")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:40s} OK")
