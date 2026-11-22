import pytest

from entropy_arb.config import ConfigError, load_config


@pytest.mark.parametrize("section,field,value", [
    ("risk", "max_daily_loss_usd", ".nan"),
    ("thresholds", "upper_bps", ".inf"),
    ("execution", "settle_timeout_sec", "0"),
])
def test_invalid_numbers_cannot_silently_disable_guard(tmp_path, section, field, value):
    cfg = tmp_path / "config.yaml"
    thresholds = {"midline_bps": "0", "upper_bps": "4", "lower_bps": "4"}
    data = {"thresholds": thresholds}
    data.setdefault(section, {})[field] = value
    cfg.write_text("".join(name + ":\n" + "".join(f"  {k}: {v}\n" for k, v in values.items())
                           for name, values in data.items()))
    with pytest.raises(ConfigError, match=field):
        load_config(str(cfg), "/dev/null", symbol="NVDA", hedge_venue="lighter")
