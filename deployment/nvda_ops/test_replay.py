from pathlib import Path
import tempfile
from types import SimpleNamespace

import replay

start = replay.clock(replay.date(2026, 11, 25), 9, 30)
frames_e = []
frames_h = []
tape = []
for i in range(35):
    ts = start - 1 + 0.5 * i
    center = 101.2 if i < 15 else 98.8
    frames_e.append(replay.Frame(ts, ts + .17, ((center-.02, 3),), ((center+.02, 3),)))
    frames_h.append(replay.Frame(ts+.1, ts+.1, ((99.99, 3),), ((100.01, 3),)))
    tape.append({"exchange_ts_ms": round((ts + .25) * 1000), "received_ts_ns": round((ts + .3) * 1e9),
                 "trade_id": i, "price": str(center + .02 if i < 15 else center - .02),
                 "quantity": "5", "side": "buy" if i < 15 else "sell"})
dummy = SimpleNamespace(open=start, close=start+14, day=replay.date(2026, 11, 25),
                        e_exchange=replay.Timeline(frames_e, lambda f:f.source),
                        e_seen=replay.Timeline(frames_e, lambda f:f.seen),
                        e_signals=replay.Timeline(frames_e, lambda f:f.source+replay.IOC_QUOTE_LAG),
                        l_seen=replay.Timeline(frames_h, lambda f:f.seen),
                        tape=replay.Timeline(tape, lambda x:x["exchange_ts_ms"]/1000))
sample = Path(__file__).resolve().parents[2] / "config.live-test.example.yaml"
with tempfile.TemporaryDirectory() as temp:
    reference = Path(temp)/"config.yaml"
    reference.write_text(sample.read_text().replace("cooldown_sec: 5.0", "cooldown_sec: 2.0"))
    ioc = replay.compare(dummy, reference, 0, ({},{}))
    alo = replay.compare(dummy, reference, 0, ({},{}), .288, True)
    assert len(ioc["trips"]) > 0 and len(alo["trips"]) > 0, (ioc, alo)
    assert all(a["ts"] in dummy.e_signals.times for a in ioc["attempts"])
    assert replay.summary([ioc])["net_usd"] is not None
    assert replay.summary([alo], True)["alo_fill_rate"] > 0
q,n = replay.fill(frames_e[0],True,.025,101.21)
assert q == n == 0
q,n = replay.fill(frames_e[0],True,.025,101.23)
assert q == .025 and n > 0
assert replay.carry(dummy, ({},{}), start, start+1802, 1, True) is None
print("IOC and ALO complete synthetic round trips with execution bounds and funding-gap handling.")
