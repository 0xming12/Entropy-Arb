from types import SimpleNamespace
import time

import pytest

from entropy_arb.book import OrderBook
from entropy_arb.config import ConfigError, load_config
from entropy_arb.engine import Engine


def _load(tmp_path, extra=""):
    p = tmp_path / "config.yaml"
    p.write_text("thresholds: {midline_bps: 0, upper_bps: 4, lower_bps: 4}\n" + extra)
    return load_config(str(p), "/dev/null", symbol="NVDA", hedge_venue="lighter")


def test_reduction_age_remains_bounded_with_entry_age_disabled(tmp_path):
    cfg = _load(tmp_path, "risk: {max_book_age_sec: 0}\n")
    assert cfg.risk.max_reduce_book_age_sec == 15
    engine = Engine(cfg)
    book = OrderBook()
    book.apply_hl([[{"px": "144", "sz": "1"}], [{"px": "145", "sz": "1"}]])
    venue = SimpleNamespace(book=book)
    now = time.monotonic()
    book.last_update_mono = now - 16
    assert engine._trade_book_fresh(venue, reduce_only=False, now_mono=now)
    assert not engine._trade_book_fresh(venue, reduce_only=True, now_mono=now)
    book.last_update_mono = now - 14
    assert engine._trade_book_fresh(venue, reduce_only=True, now_mono=now)


@pytest.mark.parametrize("limit", ["0", "-1", ".nan", ".inf"])
def test_reduction_age_cannot_be_disabled_or_nonfinite(tmp_path, limit):
    with pytest.raises(ConfigError, match="max_reduce_book_age_sec"):
        _load(tmp_path, f"risk: {{max_reduce_book_age_sec: {limit}}}\n")
