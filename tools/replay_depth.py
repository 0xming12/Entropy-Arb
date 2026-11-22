"""Replay a minute CSV and seconds-level JSONL export without signing or sending orders."""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zipfile import BadZipFile, ZipFile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from entropy_arb.book import OrderBook
from entropy_arb.config import TelegramCreds, load_config
from entropy_arb.engine import Engine
from entropy_arb.venue_hl import HLVenue
from entropy_arb.venue_lighter import LighterVenue


class ReplayEngine(Engine):
    def _schedule_poke(self, delay: float) -> None:
        # Historical samples drive the clock. There is no live event loop.
        pass


class SnapshotBook(OrderBook):
    age_at_sample: float | None = None

    def age_mono(self, now_mono: float | None = None) -> float | None:
        return self.age_at_sample if self.ready else None


class ReplayHLVenue(HLVenue):
    def ready_to_trade(self) -> bool:
        return True


class ReplayLighterVenue(LighterVenue):
    def ready_to_trade(self) -> bool:
        return True


@dataclass
class Position:
    buy: str
    qty: float
    opened_at: float
    cash_at_entry: float
    notional: float


def timestamp(value: str) -> float:
    try:
        result = datetime.fromisoformat(value)
        if result.tzinfo is not None:
            return result.timestamp()
    except (ValueError, OverflowError):
        pass
    raise argparse.ArgumentTypeError("provide a date and time with a timezone, e.g. 2026-11-20T09:30:00-05:00")


def nonnegative(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("must be a finite nonnegative number")
    return number


def book_at(book, sample, observed_at, wall_now) -> None:
    book.clear()
    if not sample["fresh"] or not (sample["bids"] and sample["asks"]):
        return
    age = sample.get("update_age_sec")
    if not isinstance(age, (int, float)) or not math.isfinite(age):
        return
    age = max(0.0, age)
    book.bids.update(sample["bids"])
    book.asks.update(sample["asks"])
    if not all(math.isfinite(p) and math.isfinite(q) and p > 0 and q > 0
               for side in (book.bids, book.asks) for p, q in side.items()):
        book.clear()
        return
    book.ready = True
    book.age_at_sample = age
    book.last_update_ts = wall_now - age
    book.alive_ts = wall_now
    book.last_update_mono = observed_at - age


def run_replay(args: argparse.Namespace) -> dict:
    cfg = load_config(str(args.config), "/dev/null", symbol=args.symbol, hedge_venue=args.hedge)
    cfg.entropy.fee_bps = args.entropy_fee_bps
    cfg.hedge.fee_bps = args.hedge_fee_bps
    cfg.telegram = TelegramCreds()
    engine = ReplayEngine(cfg, record_only=True)
    e = ReplayHLVenue(cfg.entropy, cfg.hl_api_url, cfg.hl_ws_url, None, cfg.settle_timeout_sec)
    if cfg.hedge.kind == "lighter":
        h = ReplayLighterVenue(cfg.hedge, None, cfg.settle_timeout_sec)
    else:
        h = ReplayHLVenue(cfg.hedge, cfg.hl_api_url, cfg.hl_ws_url, None, cfg.settle_timeout_sec)
    e.book, h.book = SnapshotBook(), SnapshotBook()
    engine.entropy, engine.hedge = e, h
    engine.venues = {e.key: e, h.key: h}
    e.size_decimals, h.size_decimals = args.entropy_size_decimals, args.hedge_size_decimals
    if isinstance(h, LighterVenue):
        h.price_decimals = args.lighter_price_decimals
    engine._step = 10 ** -min(e.size_decimals, h.size_decimals)
    engine._min_base = max(args.min_base, engine._step)
    engine._min_notional = max(args.min_notional, cfg.min_order_notional)

    # Offline this uses explicitly supplied fee assumptions and assumes margin
    # checks pass. It does not connect to venues or initialize a signer.
    if cfg.risk.max_taker_fee_bps > 0:
        engine.circuit_breaker.update_taker_fee(args.entropy_fee_bps)

    samples = entries = exits = rejected_bounds = rejected_slippage = 0
    results: list[float] = []
    hold_times: list[float] = []
    position = None
    last_ts = None
    last_usable = None
    slip = args.slippage_bps / 1e4
    hourly_funding = args.funding_per_hour_bps / 1e4

    with ZipFile(args.snapshot) as archive:
        minutes = sorted(name for name in archive.namelist() if name.endswith("minutes.csv"))
        depth = sorted(name for name in archive.namelist() if ".depth-" in name and name.endswith(".jsonl"))
        if len(minutes) != 1 or not depth:
            raise ValueError("archive needs one minute CSV and at least one seconds-level depth JSONL file")
        with archive.open(minutes[0]) as file:
            by_minute = {
                int(row["minute_ts"]): float(row["premium_mean_bps"])
                for row in csv.DictReader(io.TextIOWrapper(file, encoding="utf-8"))
                if int(row["minute_ts"]) + 60 <= args.calibration_until and int(row["samples"]) >= 50
            }
        if not by_minute or not all(math.isfinite(value) for value in by_minute.values()):
            raise ValueError("no usable calibration data wholly before the replay window")
        cfg.midline_bps = statistics.median(by_minute.values())

        for name in depth:
            with archive.open(name) as file:
                for line in file:
                    row = json.loads(line)
                    now = float(row["ts"])
                    if not math.isfinite(now):
                        raise ValueError("invalid timestamp")
                    if now < args.calibration_until or (args.until is not None and now >= args.until):
                        continue
                    if last_ts is not None and now <= last_ts:
                        raise ValueError("depth timestamps must be strictly increasing")
                    last_ts = now
                    wall_now = time.time()
                    for venue in (e, h):
                        book_at(venue.book, row[venue.key], now, wall_now)
                    if e.book.ready and h.book.ready:
                        samples += 1
                        last_usable = now
                    if now - engine.last_trade_ts < cfg.cooldown_sec:
                        continue
                    candidate = engine._scan(now, now_mono=now)
                    if candidate is None:
                        continue
                    buy, sell, plan = candidate
                    if position and (buy.key == position.buy or plan.qty + 1e-12 < position.qty):
                        continue
                    prepared = engine._prepare_pair_order(
                        buy, sell, plan, reduce_only=engine.is_reduce_only(now), now=now,
                        max_qty=position.qty if position else plan.qty,
                    )
                    if prepared is None:
                        rejected_bounds += 1
                        continue
                    plan, buy_bound, sell_bound = prepared
                    qty = plan.qty
                    if position and qty + 1e-12 < position.qty:
                        continue
                    if (plan.buy_limit * (1 + slip) > buy_bound + 1e-9
                            or plan.sell_limit * (1 - slip) < sell_bound - 1e-9):
                        rejected_slippage += 1
                        continue

                    debit = plan.buy_notional * (1 + slip) * (1 + engine.effective_fee_bps(buy) / 1e4)
                    credit = plan.sell_notional * (1 - slip) * (1 - engine.effective_fee_bps(sell) / 1e4)
                    cash_flow = credit - debit
                    buy.cash -= debit
                    sell.cash += credit
                    buy.position += qty
                    sell.position -= qty
                    engine.last_trade_ts = now
                    for key in engine._armed:
                        engine._armed[key] = None
                    if position:
                        held = now - position.opened_at
                        funding = position.notional * hourly_funding * held / 3600
                        result = position.cash_at_entry + cash_flow - funding
                        results.append(result)
                        hold_times.append(held)
                        engine.circuit_breaker.record_trade_pnl(result, now=now)
                        position = None
                        exits += 1
                    else:
                        position = Position(buy.key, qty, now, cash_flow,
                                            (plan.buy_notional + plan.sell_notional) / 2)
                        entries += 1

    if not samples:
        raise ValueError("no two-sided book snapshots in the replay window")
    return {
        "symbol": args.symbol,
        "hedge": args.hedge,
        "replay_cutoff": datetime.fromtimestamp(args.calibration_until, timezone.utc).isoformat(),
        "last_paired_book": datetime.fromtimestamp(last_usable, timezone.utc).isoformat(),
        "calibration_minutes": len(by_minute),
        "calibrated_midline_bps": round(cfg.midline_bps, 6),
        "assumptions": {
            "upper_bps": cfg.upper_bps, "lower_bps": cfg.lower_bps,
            "entropy_fee_bps": args.entropy_fee_bps, "hedge_fee_bps": args.hedge_fee_bps,
            "slippage_per_leg_bps": args.slippage_bps,
            "funding_per_hour_bps": args.funding_per_hour_bps,
        },
        "paired_book_samples": samples,
        "simulated_entries": entries,
        "simulated_exits": exits,
        "closed_pnl_quote": round(sum(results), 8),
        "median_closed_hold_sec": round(statistics.median(hold_times), 1) if hold_times else None,
        "open_qty": round(position.qty, 12) if position else 0,
        "open_hold_sec_at_last_book": round(last_usable - position.opened_at, 1) if position else None,
        "blocked_by_price_bounds": rejected_bounds,
        "blocked_by_fill_slippage": rejected_slippage,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("snapshot", type=Path)
    p.add_argument("--config", required=True, type=Path)
    p.add_argument("--symbol", required=True)
    p.add_argument("--hedge", required=True, choices=("lighter", "lighter-rh", "tradexyz"))
    p.add_argument("--calibration-until", required=True, type=timestamp)
    p.add_argument("--until", type=timestamp)
    p.add_argument("--entropy-size-decimals", required=True, type=int)
    p.add_argument("--hedge-size-decimals", required=True, type=int)
    p.add_argument("--lighter-price-decimals", type=int)
    for key in ("min-base", "min-notional", "entropy-fee-bps", "hedge-fee-bps",
                "slippage-bps", "funding-per-hour-bps"):
        p.add_argument(f"--{key}", required=True, type=nonnegative)
    args = p.parse_args()
    if args.until is not None and args.until <= args.calibration_until:
        p.error("--until must be after --calibration-until")
    if args.hedge != "tradexyz" and args.lighter_price_decimals is None:
        p.error("--lighter-price-decimals is required with Lighter")
    if any(not 0 <= n <= 12 for n in (args.entropy_size_decimals, args.hedge_size_decimals,
                                      args.lighter_price_decimals or 0)):
        p.error("price and size decimals must be between 0 and 12")
    if max(args.entropy_fee_bps, args.hedge_fee_bps, args.slippage_bps) >= 10_000:
        p.error("fee and slippage inputs must be below 10,000 bps")
    try:
        result = run_replay(args)
    except (ValueError, OSError, KeyError, BadZipFile) as exc:
        p.exit(2, f"{p.prog}: {exc}\n")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
