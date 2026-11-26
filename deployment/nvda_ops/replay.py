"""Single-process replay. Reads only copied market data and an isolated config."""
import argparse
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, time as wall_time
import json
import logging
import math
from pathlib import Path
from statistics import mean
import sys
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

HERE = Path(__file__).parent
REPO = HERE.resolve().parents[1]
sys.path.insert(0, str(REPO / "tools"))
from replay_depth import ReplayEngine, ReplayHLVenue, ReplayLighterVenue, SnapshotBook, load_config
from entropy_arb.book import floor_step

NY = ZoneInfo("America/New_York")
GO = 0.550
IOC_QUOTE_LAG = 0.300
ACK = 0.285
ALO_EXPIRE = 2.0
HEDGE_WIRE = 0.0  # Use the contemporaneous Lighter book, as requested.
EPS = 1e-9
PREFIX = "nvda-lighter-minutes"
EARLY_CLOSE = date(2026, 11, 27)


def clock(day, hour, minute=0):
    return datetime.combine(day, wall_time(hour, minute), NY).timestamp()


def closing_hour(day):
    return 13 if day == EARLY_CLOSE else 16


def read(path):
    with path.open("rb") as source:
        for line in source:
            if line.endswith(b"\n"):
                yield json.loads(line)


@dataclass(frozen=True)
class Frame:
    source: float
    seen: float
    bids: tuple
    asks: tuple

    @property
    def mid(self):
        return (self.bids[0][0] + self.asks[0][0]) / 2


class Timeline:
    def __init__(self, frames, key, monotonic_source=False):
        ordered = sorted(frames, key=key)
        self.frames = []
        for frame in ordered:
            if not monotonic_source or not self.frames or frame.source > self.frames[-1].source:
                self.frames.append(frame)
        self.times = [key(x) for x in self.frames]

    def at(self, when):
        index = bisect_right(self.times, when) - 1
        return self.frames[index] if index >= 0 else None

    def between(self, start, end):
        return self.frames[bisect_left(self.times, start):bisect_right(self.times, end)]


def levels(rows):
    return tuple((float(price), float(quantity)) for price, quantity in rows)


def overlay(frame, bid, ask, stamp, received):
    if frame is None or stamp - frame.source > 2:
        return Frame(stamp, received, (bid,), (ask,))
    return Frame(stamp, received,
                 (bid,) + tuple((p, q) for p, q in frame.bids if p < bid[0])[:4],
                 (ask,) + tuple((p, q) for p, q in frame.asks if p > ask[0])[:4])


class Market:
    def __init__(self, folder, day):
        opening, closing = clock(day, 9, 30), clock(day, closing_hour(day))
        series = {"entropy": {}, "hedge": {}}
        covered = set()
        for row in read(folder / f"{PREFIX}.depth-{day}.jsonl"):
            stamp = row["ts"]
            if not opening - 180 * 60 <= stamp <= closing + 5:
                continue
            paired = True
            for name, frames in series.items():
                book = row[name]
                if not book["fresh"] or book["update_age_sec"] is None or not book["bids"] or not book["asks"]:
                    paired = False
                    continue
                source = round(stamp - book["update_age_sec"], 3)
                # Lighter's captured age is measured from local receipt. Entropy's
                # is from exchange time, so the recorder timestamp is an upper bound.
                seen = stamp if name == "entropy" else source
                if source not in frames:
                    frames[source] = Frame(source, seen, levels(book["bids"]), levels(book["asks"]))
            if paired and opening <= stamp < closing:
                covered.add(int(stamp))
        if not covered or min(covered) > opening + 2 or max(covered) < closing - 3:
            raise ValueError(f"incomplete opening or closing depth on {day}")
        gaps = [b - a - 1 for a, b in zip(sorted(covered), sorted(covered)[1:])]
        if len(covered) / (closing - opening) < 0.99 or max(gaps, default=0) > 15:
            raise ValueError(f"market depth gaps exceed replay coverage limits on {day}")

        raw_ent = Timeline(series["entropy"].values(), lambda f: f.source)
        entropy_changes = []
        for row in read(folder / f"{PREFIX}.entropy-bbo-{day}.jsonl"):
            stamp = row["exchange_ts_ms"] / 1000
            received = row["received_ts_ns"] / 1e9
            if opening - 180 * 60 <= stamp <= closing + 5:
                index = bisect_right(raw_ent.times, stamp) - 1
                depth = raw_ent.frames[index] if index >= 0 else None
                bid, ask = levels([row["bid"]])[0], levels([row["ask"]])[0]
                change = overlay(depth, bid, ask, stamp, received)
                entropy_changes.append(change)
                series["entropy"][stamp] = change
        self.e_exchange = Timeline(series["entropy"].values(), lambda f: f.source)
        self.e_seen = Timeline(series["entropy"].values(), lambda f: f.seen, monotonic_source=True)
        self.e_signals = Timeline(entropy_changes, lambda f: f.source + IOC_QUOTE_LAG,
                                  monotonic_source=True)
        if not self.e_signals.times:
            raise ValueError(f"no actual Entropy BBO changes on {day}")
        self.l_seen = Timeline(series["hedge"].values(), lambda f: f.seen)
        self.covered = len(covered)
        self.max_gap = max(gaps, default=0)
        seen = {}
        for row in read(folder / f"{PREFIX}.entropy-trades-{day}.jsonl"):
            t = row["exchange_ts_ms"] / 1000
            if opening <= t <= closing + 5:
                seen[(t, row["trade_id"])] = row
        self.tape = Timeline(seen.values(), lambda row: row["exchange_ts_ms"] / 1000)
        if not seen:
            raise ValueError(f"no public trades in regular session {day}")
        self.open = opening
        self.close = closing
        self.day = day


def json_request(url, data=None):
    req = Request(url, data=json.dumps(data).encode() if data else None,
                  headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=15) as response:
        return json.load(response)


def download_funding(folder, day):
    utc_from = int(clock(day, 9))
    utc_to = int(clock(day, closing_hour(day), 2))
    hl = json_request("https://api.hyperliquid.xyz/info",
                      {"type": "fundingHistory", "coin": "io:NVDA",
                       "startTime": utc_from * 1000, "endTime": utc_to * 1000})
    query = urlencode({"market_id": 110, "resolution": "1h", "start_timestamp": utc_from,
                       "end_timestamp": utc_to, "count_back": 12})
    light = json_request("https://mainnet.zklighter.elliot.ai/api/v1/fundings?" + query)["fundings"]
    original = {"entropy": hl, "lighter": light}
    (folder / f"funding-{day}.json").write_text(json.dumps(original, indent=2) + "\n")


def funding(folder, day):
    payload = json.loads((folder / f"funding-{day}.json").read_text())
    hl, light = payload["entropy"], payload["lighter"]
    h_rates = {round(r["time"] / 3600000) * 3600: float(r["fundingRate"]) for r in hl}
    l_amount = {round(r["timestamp"] / 3600) * 3600:
                abs(float(r["value"])) * (1 if r["direction"] == "long" else -1) for r in light}
    return h_rates, l_amount


def fill(frame, buy, qty, bound):
    if frame is None:
        return 0.0, 0.0
    done, notional = 0.0, 0.0
    for price, amount in frame.asks if buy else frame.bids:
        if (buy and price > bound + EPS) or (not buy and price < bound - EPS):
            break
        portion = min(amount, qty - done)
        notional += portion * price
        done += portion
        if done >= qty - EPS:
            break
    return done, notional


def maker(market, buy, qty, price, start, stop, displayed_queue):
    frame = market.e_exchange.at(start)
    if frame is None or (buy and price >= frame.asks[0][0]) or (not buy and price <= frame.bids[0][0]):
        return [], True
    book = frame.bids if buy else frame.asks
    ahead = sum(v for p, v in book if math.isclose(p, price, rel_tol=0, abs_tol=EPS)) if displayed_queue else 0.0
    done, hits = 0.0, []
    for message in market.tape.between(start, stop):
        trade_stamp = message["exchange_ts_ms"] / 1000
        if trade_stamp <= start:
            continue
        tx = float(message["price"])
        if (message["side"] == ("sell" if buy else "buy")) and ((buy and tx <= price) or (not buy and tx >= price)):
            volume = float(message["quantity"])
            if (buy and tx < price - EPS) or (not buy and tx > price + EPS):
                # A through-price trade establishes that better queued quotes
                # were cleared, subject to the modeled order still resting.
                ahead = 0.0
            queue_consumed = min(ahead, volume)
            ahead -= queue_consumed
            qty_taken = min(qty - done, volume - queue_consumed)
            if qty_taken <= EPS:
                continue
            done += qty_taken
            hits.append((message, qty_taken))
            if done >= qty - EPS:
                break
    return hits, False


def adverse(market, buy, stamp):
    initial = market.e_exchange.at(stamp)
    later = [f for f in market.e_exchange.between(stamp, stamp + 1) if f.source > stamp]
    if initial is None or not later:
        return None
    return any(f.mid < initial.mid - EPS if buy else f.mid > initial.mid + EPS for f in later)


def engine(path, persistence, maker_fee=None):
    cfg = load_config(str(path), "/dev/null", symbol="NVDA", hedge_venue="lighter")
    cfg.premium_persist_sec = persistence
    if cfg.funding_skew.enabled:
        raise ValueError("historical funding-skew gate requires real-time indicative rate capture")
    taker_fee = cfg.entropy.fee_bps
    if maker_fee is not None:
        cfg.entropy.fee_bps = maker_fee
    bot = ReplayEngine(cfg, record_only=True)
    e = ReplayHLVenue(cfg.entropy, cfg.hl_api_url, cfg.hl_ws_url, None, cfg.settle_timeout_sec)
    l = ReplayLighterVenue(cfg.hedge, None, cfg.settle_timeout_sec)
    e.book, l.book = SnapshotBook(), SnapshotBook()
    e.size_decimals = l.size_decimals = l.price_decimals = 3
    e.min_base = l.min_base = 0.025
    e.min_quote = l.min_quote = 10.0
    bot.entropy, bot.hedge = e, l
    bot.venues = {"entropy": e, "hedge": l}
    bot._step, bot._min_base, bot._min_notional = .001, .025, max(cfg.min_order_notional, 10.0)
    return bot, taker_fee / 1e4


def observe(book, frame, now):
    if frame is None:
        book.ready = False
        return
    if book.last_update_ts != frame.source:
        book.bids = dict(frame.bids)
        book.asks = dict(frame.asks)
        book.last_update_mono = book.last_update_ts = frame.source
    book.age_at_sample = max(now - frame.source, 0.0)
    book.alive_ts = 1e12
    book.ready = True


@dataclass
class Position:
    opening: float
    buy_entropy: bool
    qty: float
    basis_per_base: float
    notional_per_base: float


def carry(market, rates, opening, closing, size, buy_entropy):
    h_rates, l_amount = rates
    carry_value = 0.0
    for hour in range((math.floor(opening / 3600) + 1) * 3600, int(closing) + 1, 3600):
        frame = market.e_exchange.at(hour)
        if hour not in h_rates or hour not in l_amount or frame is None or hour - frame.source > 3:
            return None
        e_per_unit = h_rates[hour] * frame.mid
        carry_value += (l_amount[hour] - e_per_unit) * size * (1 if buy_entropy else -1)
    return carry_value


def follow(market, bot, when, buy, desired, position):
    cfg = bot.cfg
    h_book = market.l_seen.at(when)
    if h_book is None or when - h_book.seen > cfg.risk.max_reduce_book_age_sec:
        return 0.0, 0.0
    ref = h_book.asks[0][0] if buy else h_book.bids[0][0]
    slip = bot._entropy_first_follow_slip()
    limit = bot.hedge.px_round(ref * (1 + slip if buy else 1 - slip), round_up=not buy)
    target = floor_step(min(desired, cfg.max_order_notional / max(ref, limit)), bot._step)
    full_close = position and math.isclose(target, position.qty, rel_tol=0, abs_tol=EPS)
    base_min = bot._step if full_close else bot._min_base
    notional_min = 0 if full_close else bot._min_notional
    if target < base_min or target * min(ref, limit) < notional_min:
        return 0.0, 0.0
    return fill(h_book, buy, target, limit)


def compare(market, path, cfg_persist, rates, maker_fee=None, displayed_queue=False):
    bot, e_taker_fee = engine(path, cfg_persist, maker_fee)
    cfg = bot.cfg
    is_alo = maker_fee is not None
    pos = None
    next_scan = market.open
    attempts, trips = [], []
    e_fee, l_fee = cfg.entropy.fee_bps / 1e4, cfg.hedge.fee_bps / 1e4
    if is_alo:
        times = sorted(set(market.e_seen.times + market.l_seen.times) |
                       {market.open + i / 20 for i in range(int((market.close - market.open) * 20))})
    else:
        times = market.e_signals.times
    decision_book = market.e_seen if is_alo else market.e_signals
    cleanup = 0.0
    exposure = None
    last_decision_frames = None
    for now in times:
        if now >= market.close or now < market.open - 180 * 60:
            continue
        e, l = decision_book.at(now), market.l_seen.at(now)
        if (e is None or l is None or now - l.seen > 15 or (is_alo and now - e.seen > 15)):
            bot._reset_signal()
            continue
        if now < next_scan:
            if now < market.open:
                bot.dynamic_midline.update((e.mid / l.mid - 1) * 1e4, now=now)
            continue
        observe(bot.entropy.book, e, now)
        observe(bot.hedge.book, l, now)
        if pos:
            bot.entropy.position = pos.qty * (1 if pos.buy_entropy else -1)
            bot.hedge.position = -bot.entropy.position
        else:
            bot.entropy.position = bot.hedge.position = 0.0
        if is_alo:
            # On the strategy side evaluate the planned passive Entropy price:
            # ask to sell, bid to buy. Only execution uses the future real BBO.
            mid = e.mid
            bot.dynamic_midline.update((mid / l.mid - 1) * 1e4, now=now)
            cand = passive_candidate(bot, e, l, now, pos)
            if cand is None:
                continue
            entropy_buy, qty, price = cand
            entropy_bound = price
        else:
            candidate = bot._scan(now, now_mono=now)
            if candidate is None:
                continue
            buyer, seller, plan = candidate
            entropy_buy = buyer is bot.entropy
            if pos and entropy_buy == pos.buy_entropy:
                bot._reset_signal()
                continue
            prepared = bot._prepare_pair_order(buyer, seller, plan, now=now, reduce_only=False,
                                               max_qty=pos.qty if pos else math.inf)
            if prepared is None:
                continue
            plan, buy_limit, sell_limit = prepared
            qty = plan.qty
            entropy_bound = buy_limit if entropy_buy else sell_limit

        frames_key = (e.source, l.source)
        if frames_key == last_decision_frames:
            continue
        last_decision_frames = frames_key
        bot._reset_signal()
        event = {"ts": now, "buy_entropy": entropy_buy, "order_qty": qty, "bound": entropy_bound}
        attempts.append(event)
        send_at = now + GO
        next_scan = now + cfg.cooldown_sec
        if is_alo:
            hits, reject = maker(market, entropy_buy, qty, entropy_bound, send_at,
                                 min(send_at + ALO_EXPIRE, market.close), displayed_queue)
            event["alo_rejected_crossing"] = reject
            if not hits:
                event["entropy_filled"] = 0.0
                if not reject:
                    next_scan = max(next_scan, send_at + ALO_EXPIRE)
                continue
            stamps = []
            marks = []
            e_qty = h_size = h_cost = 0.0
            for message, portion in sorted(hits, key=lambda hit: hit[0]["received_ts_ns"]):
                fill_at = message["exchange_ts_ms"] / 1000
                hedge_at = max(fill_at, message["received_ts_ns"] / 1e9) + HEDGE_WIRE
                stamps.append(fill_at)
                marks.append(adverse(market, entropy_buy, fill_at))
                e_qty += portion
                extra_qty, extra_cost = follow(market, bot, hedge_at, not entropy_buy, e_qty - h_size, pos)
                h_size += extra_qty
                h_cost += extra_cost
            e_cost = e_qty * entropy_bound
            fill_at = stamps[0]
            event["adverse_1s"] = next((mark for mark in marks if mark is not None), None)
            event["alo_partial_fills"] = len(hits)
            if e_qty + EPS < qty:
                next_scan = max(next_scan, send_at + ALO_EXPIRE)
                hedge_at = max(hedge_at, send_at + ALO_EXPIRE)
        else:
            e_qty, e_cost = fill(market.e_exchange.at(send_at), entropy_buy, qty, entropy_bound)
            if e_qty <= EPS:
                event["entropy_filled"] = 0.0
                continue
            fill_at = send_at
            hedge_at = send_at + ACK + HEDGE_WIRE
            h_size, h_cost = follow(market, bot, hedge_at, not entropy_buy, e_qty, pos)
        event["entropy_filled"] = e_qty

        event["hedge_filled"] = h_size
        next_scan = max(next_scan, hedge_at)
        matched = min(e_qty, h_size)
        net = e_qty - matched
        if net > EPS:
            # Observe the residual explicitly. Never treat it as a paired win.
            # The cleanup stress estimate is a second Entropy taker order 550ms later.
            check_at = hedge_at + GO
            unwind_book = market.e_exchange.at(check_at)
            unwind_is_buy = not entropy_buy
            unwind_size = unwind_cost = 0.0
            if unwind_book is not None:
                reference = unwind_book.asks[0][0] if unwind_is_buy else unwind_book.bids[0][0]
                slip = min(cfg.hedge_slippage_bps, cfg.hedge_max_slippage_bps) / 1e4
                bound = bot.entropy.px_round(reference * (1 + slip if unwind_is_buy else 1 - slip),
                                             round_up=not unwind_is_buy)
                unwind_size, unwind_cost = fill(unwind_book, unwind_is_buy, net, bound)
            event["single_leg_qty"] = net
            if unwind_size + EPS < net:
                exposure = {"ts": check_at, "entropy_qty": net - unwind_size}
            allocated = e_cost * unwind_size / e_qty
            cost = ((unwind_cost - allocated) if entropy_buy else (allocated - unwind_cost))
            cleanup_pnl = cost - allocated * e_fee - unwind_cost * e_taker_fee
            event["cleanup_pnl_usd"] = cleanup_pnl
            cleanup += cleanup_pnl
            next_scan = max(next_scan, check_at)
        if matched > EPS:
            entropy_cash = e_cost * matched / e_qty * (-1 if entropy_buy else 1)
            light_cash = h_cost * matched / h_size * (1 if entropy_buy else -1)
            cash = entropy_cash + light_cash - abs(entropy_cash) * e_fee - abs(light_cash) * l_fee
            if pos:
                if matched > pos.qty + EPS:
                    raise ValueError("closing quantity exceeded matched inventory")
                funds = carry(market, rates, pos.opening, fill_at, matched, pos.buy_entropy)
                pnl = pos.basis_per_base * matched + cash
                trips.append({"open": pos.opening, "close": fill_at, "qty": matched,
                              "notional": pos.notional_per_base * matched,
                              "funding": funds, "net_before_funding": pnl,
                              "net": pnl + funds if funds is not None else None,
                              "completed": pos.qty - matched <= EPS})
                pos.qty -= matched
                if pos.qty <= EPS:
                    pos = None
            else:
                pos = Position(fill_at, entropy_buy, matched, cash / matched, (abs(entropy_cash) + abs(light_cash)) / (2 * matched))
            bot.last_trade_ts = hedge_at
            bot.entropy.last_traded_ts = e.source
            bot.hedge.last_traded_ts = l.source
        if exposure is not None:
            break
    return {"attempts": attempts, "trips": trips, "unpaired_cleanup_pnl_usd": cleanup,
            "unresolved": exposure, "open_paired_qty": pos.qty if pos else 0}


def passive_candidate(bot, e, l, now, position):
    best = None
    qty_max = position.qty if position else math.inf
    if bot.session_state(now).reduce_only:
        return None
    if not all(bot._entry_book_fresh(v, reduce_only=False, now_mono=now)
               for v in (bot.entropy, bot.hedge)):
        bot._reset_signal()
        return None
    for e_buy in (True, False):
        name = "buy_entropy" if e_buy else "sell_entropy"
        if position and position.buy_entropy == e_buy:
            bot._reset_signal(name)
            continue
        a, b = (bot.entropy, bot.hedge) if e_buy else (bot.hedge, bot.entropy)
        passive = e.bids[0][0] if e_buy else e.asks[0][0]
        highest = max(passive, l.bids[0][0] if e_buy else l.asks[0][0])
        qty = floor_step(min(qty_max, bot.cfg.max_order_notional / highest,
                             bot._position_qty_headroom(a, b, bot.session_state(now).cap_multiplier)), bot._step)
        volume, hedge_value = fill(l, not e_buy, qty, 0.0 if e_buy else float("inf"))
        if volume + EPS < qty or qty < bot._min_base or passive * qty < bot._min_notional:
            bot._reset_signal(name)
            continue
        average = hedge_value / qty
        net_edge = bot._worst_case_check(a, b, qty, passive if e_buy else average,
                                         average if e_buy else passive, now=now)
        if not net_edge[0]:
            bot._reset_signal(name)
            continue
        current = bot._armed.get(name)
        if current is None:
            bot._armed[name] = now
            bot._armed_entropy_mono[name] = e.source
            continue
        if now - current < bot.cfg.premium_persist_sec:
            continue
        if (bot.cfg.require_entropy_update_on_persist and bot.cfg.premium_persist_sec > 0
                and e.source <= bot._armed_entropy_mono[name]):
            continue
        value = net_edge[2]
        if best is None or value > best[0]:
            best = (value, e_buy, qty, passive)
    return best[1:] if best else None


def summary(records, alo=False):
    all_a = [attempt for day in records for attempt in day["attempts"]]
    all_t = [trip for day in records for trip in day["trips"]]

    def aggregate(attempts, trips):
        good = [t for t in trips if t["net"] is not None]
        cycles = {}
        for trip in trips:
            cycles.setdefault(trip["open"], []).append(trip)
        completed = [rows for rows in cycles.values() if rows[-1]["completed"]]
        complete_net_bps = [10_000 * sum(x["net"] for x in rows) / sum(x["notional"] for x in rows)
                             for rows in completed if all(x["net"] is not None for x in rows)]
        cleanup = sum(a.get("cleanup_pnl_usd", 0) for a in attempts)
        return {
            "signals": len(attempts), "round_trips_closed": len(completed), "paired_close_slices": len(trips),
            "unpriced_funding": len(trips) - len(good),
            "avg_net_bps_paired": mean(complete_net_bps) if complete_net_bps else None,
            "net_usd": (sum(t["net"] for t in good) + cleanup) if len(good) == len(trips) else None,
            "funding_usd": sum(t["funding"] for t in good) if len(good) == len(trips) else None,
            "entropy_with_fill": sum(a["entropy_filled"] > EPS for a in attempts),
            "single_legs": sum(a.get("single_leg_qty", 0) > EPS for a in attempts),
            "unpaired_cleanup_pnl_usd": cleanup,
        }

    def first_30(ts):
        t = datetime.fromtimestamp(ts, NY).time()
        return wall_time(9, 30) <= t < wall_time(10)

    total = aggregate(all_a, all_t)
    total["windows"] = {name: aggregate([a for a in all_a if first_30(a["ts"]) == opened],
                                        [t for t in all_t if first_30(t["open"]) == opened])
                        for name, opened in (("first_30_min", True), ("rest", False))}
    total["open_paired_qty_by_day"] = [day["open_paired_qty"] for day in records]
    total["unresolved_by_day"] = [day["unresolved"] for day in records]
    if alo:
        posted = [x for x in all_a if not x["alo_rejected_crossing"]]
        valid = [x["adverse_1s"] for x in posted if x.get("adverse_1s") is not None]
        total["alo_posted"] = len(posted)
        total["alo_fill_rate"] = sum(x["entropy_filled"] > EPS for x in posted) / len(posted) if posted else None
        total["alo_adverse_1s_observations"] = len(valid)
        total["alo_adverse_1s_rate"] = mean(valid) if valid else None
    return total


def run():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data", type=Path, default=HERE / "input")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--entropy-maker-bps", type=float, required=True)
    p.add_argument("days", nargs=3, type=date.fromisoformat)
    p.add_argument("--include-early-close", action="store_true")
    args = p.parse_args()
    if len(set(args.days)) != 3 or EARLY_CLOSE in args.days or not math.isfinite(args.entropy_maker_bps):
        raise ValueError("supply three distinct trading days and a source-verified finite maker fee")
    logging.disable(logging.CRITICAL)
    reference = args.data / "strategy.reference.json"
    p0 = load_config(str(reference), "/dev/null", symbol="NVDA", hedge_venue="lighter").premium_persist_sec
    schedules = {"ioc_0": (0, None, False), "ioc_0.2": (.2, None, False), "ioc_0.5": (.5, None, False),
                 "alo_displayed_queue": (p0, args.entropy_maker_bps, True),
                 "alo_queue_head": (p0, args.entropy_maker_bps, False)}
    detail = {name: [] for name in schedules}
    coverage = {}
    def run_day(day):
        tape = Market(args.data, day)
        quality = {"usable_seconds": tape.covered, "max_gap_s": tape.max_gap,
                   "entropy_trades": len(tape.tape.frames), "close_hour_et": closing_hour(day)}
        rates = funding(args.data, day)
        results = {name:compare(tape,reference,gate,rates,charge,queue)
                   for name,(gate,charge,queue) in schedules.items()}
        return quality,results

    for day in sorted(args.days):
        quality, results = run_day(day)
        coverage[str(day)] = quality
        for name,result in results.items():
            detail[name].append(result)
    out = {"coverage": coverage, "ioc_signal_clock": "entropy_bbo_change_exchange_time",
           "ioc_quote_lag_sec": IOC_QUOTE_LAG, "ioc_entropy_one_way_sec": GO, "ioc_entropy_ack_after_match_sec": ACK,
           "alo_ttl_sec": ALO_EXPIRE, "alo_entropy_maker_bps": args.entropy_maker_bps,
           "summary": {key:summary(value,key.startswith("alo_")) for key,value in detail.items()}}
    args.out.mkdir(parents=True, exist_ok=True)
    if args.include_early_close:
        quality, results = run_day(EARLY_CLOSE)
        out["early_close"] = {"trading_day":str(EARLY_CLOSE),"coverage":quality,
                              "summary":{k:summary([v],k.startswith("alo_")) for k,v in results.items()}}
        (args.out / "details-early-close.json").write_text(json.dumps(results,indent=2)+"\n")
    (args.out / "details.json").write_text(json.dumps(detail, indent=2) + "\n")
    (args.out / "summary.json").write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    run()
