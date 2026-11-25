from pathlib import Path
import json

from entropy_arb.market_capture import EntropyMarketTape


def test_entropy_trade_tape_retains_trade_facts_and_deduplicates(tmp_path: Path):
    tape = EntropyMarketTape(str(tmp_path / "nvda-minutes.csv"), "io:NVDA")
    row = {"coin": "io:NVDA", "time": 1795530001001, "tid": 32, "px": "158.22",
           "sz": "0.099", "side": "B", "users": ["wallet-a", "wallet-b"]}
    message = {"channel": "trades", "data": [row, row, {**row, "tid": 33, "side": "A"},
                                             {**row, "tid": 34, "coin": "BTC"}]}
    tape.observe(message, 1795530001189123456)
    next_day = {**row, "time": 1795616401001, "tid": 34}
    tape.observe({"channel": "trades", "data": [next_day]}, 1795616401189123456)
    tape.close()
    files = sorted(tmp_path.glob("*.entropy-trades-*.jsonl"))
    assert len(files) == 2
    rows = [json.loads(line) for line in files[0].read_text().splitlines()]
    assert rows == [
        {"coin": "io:NVDA", "exchange_ts_ms": 1795530001001, "received_ts_ns": 1795530001189123456,
         "trade_id": 32, "price": "158.22", "quantity": "0.099", "side": "buy", "raw_side": "B"},
        {"coin": "io:NVDA", "exchange_ts_ms": 1795530001001, "received_ts_ns": 1795530001189123456,
         "trade_id": 33, "price": "158.22", "quantity": "0.099", "side": "sell", "raw_side": "A"},
    ]


def test_entropy_bbo_tape_keeps_exchange_and_receive_time_for_changes(tmp_path: Path):
    tape = EntropyMarketTape(str(tmp_path / "nvda-minutes.csv"), "io:NVDA")
    ts = 1795530001001

    def book(timestamp, ask_size, received):
        tape.observe({"channel": "l2Book", "data": {"coin": "io:NVDA", "time": timestamp,
            "levels": [[{"px": "158.21", "sz": "0.099"}], [{"px": "158.22", "sz": ask_size}]]}}, received)

    book(ts, "2.0", 1795530001189123456)
    book(ts + 500, "2.0", 1795530001689123456)
    book(ts + 600, "3.0", 1795530001789123456)
    book(ts + 250, "4.0", 1795530001989123456)
    tape.close()
    (path,) = tmp_path.glob("*.entropy-bbo-*.jsonl")
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert [row["exchange_ts_ms"] for row in rows] == [ts, ts + 600]
    assert [row["received_ts_ns"] for row in rows] == [1795530001189123456, 1795530001789123456]
    assert [row["ask"] for row in rows] == [["158.22", "2.0"], ["158.22", "3.0"]]
