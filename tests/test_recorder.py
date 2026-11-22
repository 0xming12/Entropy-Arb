"""Minute recorder: aggregation, rollover, CSV output.

Run:  python3 -m pytest tests/  (or  python3 tests/test_recorder.py)
"""
import csv
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.book import OrderBook  # noqa: E402
from entropy_arb.recorder import HEADER, MinuteRecorder  # noqa: E402


def set_book(book, bid, ask):
    book.apply_hl([[{"px": str(bid), "sz": "10"}],
                   [{"px": str(ask), "sz": "10"}]])


def test_minute_aggregation_and_rollover():
    e_book, h_book = OrderBook(), OrderBook()
    path = os.path.join(tempfile.mkdtemp(), "minutes.csv")
    rec = MinuteRecorder(path, e_book, h_book, staleness_sec=1e9)

    t0 = 1_700_000_000.0            # 20s into a minute (boundary at ...020)
    # minute 1: entropy 10 bps rich, then 20 bps rich
    set_book(e_book, 100.09, 100.11)   # mid 100.10
    set_book(h_book, 99.99, 100.01)    # mid 100.00
    rec.sample(t0)
    set_book(e_book, 100.19, 100.21)   # mid 100.20
    rec.sample(t0 + 10)
    # next minute: back to 10 bps rich -> flushes minute 1
    set_book(e_book, 100.09, 100.11)
    rec.sample(t0 + 45)
    rec.close()                        # flushes the partial minute 2

    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert [*rows[0]] == HEADER
    assert len(rows) == 2
    m1, m2 = rows
    assert int(m1["samples"]) == 2 and int(m2["samples"]) == 1
    assert abs(float(m1["premium_open_bps"]) - 10.0) < 0.2
    assert abs(float(m1["premium_high_bps"]) - 20.0) < 0.2
    assert abs(float(m1["premium_close_bps"]) - 20.0) < 0.2
    assert abs(float(m1["premium_mean_bps"]) - 15.0) < 0.2
    # executable edges: sell = bid_e/ask_h - 1, buy = bid_h/ask_e - 1
    assert abs(float(m2["sell_edge_max_bps"])
               - ((100.09 / 100.01 - 1) * 1e4)) < 0.05
    assert abs(float(m2["buy_edge_max_bps"])
               - ((99.99 / 100.11 - 1) * 1e4)) < 0.05
    # closes carry the last books
    assert float(m2["entropy_bid"]) == 100.09
    assert float(m2["hedge_ask"]) == 100.01


def test_stale_books_are_skipped():
    e_book, h_book = OrderBook(), OrderBook()
    path = os.path.join(tempfile.mkdtemp(), "minutes.csv")
    rec = MinuteRecorder(path, e_book, h_book, staleness_sec=1e9)
    rec.sample(1_700_000_000.0)        # both books empty -> nothing recorded
    set_book(e_book, 100.0, 100.02)    # only one side fresh
    rec.sample(1_700_000_001.0)
    rec.close()
    assert rec.rows_written == 0
    assert not os.path.exists(path)    # no row, no file


def test_append_keeps_single_header():
    e_book, h_book = OrderBook(), OrderBook()
    path = os.path.join(tempfile.mkdtemp(), "minutes.csv")
    set_book(e_book, 100.0, 100.02)
    set_book(h_book, 100.0, 100.02)
    for start in (1_700_000_000.0, 1_700_000_060.0):
        rec = MinuteRecorder(path, e_book, h_book, staleness_sec=1e9)
        rec.sample(start)
        rec.close()
    with open(path) as fh:
        lines = fh.read().strip().splitlines()
    assert len(lines) == 3             # one header + two rows
    assert lines[0].startswith("minute_ts,")


def test_restart_inside_minute_merges_partial_row():
    entropy, hedge = OrderBook(), OrderBook()
    set_book(hedge, 99.99, 100.01)
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "minutes.csv"
        for bid, ask, now in ((100.09, 100.11, 1_700_000_000.0),
                              (100.19, 100.21, 1_700_000_010.0)):
            recorder = MinuteRecorder(str(path), entropy, hedge, 5.0)
            set_book(entropy, bid, ask)
            recorder.sample(now)
            recorder.close()
        with path.open() as source:
            rows = list(csv.DictReader(source))
        assert len(rows) == 1
        row = rows[0]
        assert row["samples"] == "2"
        assert row["premium_open_bps"] == "10.000"
        assert row["premium_close_bps"] == "20.000"
        assert row["premium_mean_bps"] == "15.000"
        assert row["premium_std_bps"] == "5.000"


def test_depth_output_marks_heartbeat_only_book_as_stale():
    entropy, hedge = OrderBook(), OrderBook()
    set_book(entropy, 100, 101)
    set_book(hedge, 100, 101)
    entropy.last_update_mono -= 30
    entropy.touch()
    with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"ENTROPY_RECORD_DEPTH": "1"}):
        path = Path(directory) / "minutes.csv"
        recorder = MinuteRecorder(str(path), entropy, hedge, 10, max_book_age_sec=5)
        recorder.sample(1_700_000_000.0)
        recorder.close()
        assert not path.exists()
        sample = json.loads((Path(directory) / "minutes.depth-2023-11-14.jsonl").read_text())
        assert sample["entropy"]["fresh"] is False
        assert sample["entropy"]["update_age_sec"] >= 30
        assert sample["entropy"]["bids"] == []
        assert sample["hedge"]["fresh"] is True
        assert sample["hedge"]["asks"] == [[101.0, 10.0]]


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:40s} OK")
