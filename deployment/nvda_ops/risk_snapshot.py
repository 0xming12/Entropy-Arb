"""Watch liquidation distance with public exchange APIs and report account risk."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import re
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

ROOT = Path("/opt/entropy-arb")
OPS = Path("/var/lib/entropy-nvda-ops")
STATE = ROOT / "data/live-test/NVDA_lighter_state.json"
NY = ZoneInfo("America/New_York")
def funding_start():
    raw = os.environ.get("ENTROPY_NVDA_FUNDING_START_ET", "")
    if not raw:
        return None
    when = datetime.fromisoformat(raw)
    if when.tzinfo is None:
        raise ValueError("funding start must include a UTC offset")
    return when


def tracked_lighter_size():
    raw = os.environ.get("ENTROPY_NVDA_FUNDING_POSITION_SIZE", "")
    return Decimal(raw) if raw else None


def get(url, payload=None):
    request = Request(url, data=json.dumps(payload).encode() if payload else None,
                      headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=8) as response:
        return json.load(response)


def distance(size, mark, liquidation):
    if not size or mark is None or liquidation is None:
        return None
    px, liq = Decimal(mark), Decimal(liquidation)
    if px <= 0 or liq <= 0:
        return None
    return (px - liq) * (1 if size > 0 else -1) * 100 / px


def entropy(accounts, through, since):
    url = "https://api.hyperliquid.xyz/info"
    address = re.fullmatch(r"https://api.hyperliquid.xyz/(0x[0-9a-fA-F]{40})/io:NVDA",
                           accounts["entropy"])[1]
    state = get(url, {"type": "clearinghouseState", "user": address, "dex": "io"})
    pos = next((row["position"] for row in state["assetPositions"]
                if row["position"]["coin"] == "io:NVDA"), {})
    meta, ctx = get(url, {"type": "metaAndAssetCtxs", "dex": "io"})
    quote = next(ctx[i] for i, asset in enumerate(meta["universe"]) if asset["name"] == "io:NVDA")
    amount = Decimal(pos.get("szi", 0))
    mark, liq = quote.get("markPx"), pos.get("liquidationPx")
    leverage = pos.get("leverage") or {}
    result = dict(position=amount, margin_mode=leverage.get("type"), leverage=leverage.get("value"),
                  margin_used=pos.get("marginUsed"), margin_kind="exchange_position_margin",
                  collateral_unit="USDC", mark=mark, liquidation_price=liq,
                  distance_percent=distance(amount, mark, liq))
    if through is not None and since is not None:
        try:
            fund = get(url, {"type": "userFunding", "user": address,
                             "startTime": int(since.timestamp() * 1000),
                             "endTime": int(through.timestamp() * 1000)})
            if not isinstance(fund, list):
                raise ValueError("funding response")
            settlements = {(entry["time"], entry["hash"]): Decimal(entry["delta"]["usdc"])
                           for entry in fund if entry["delta"]["coin"] == "io:NVDA"}
            result.update(funding_net=sum(settlements.values(), Decimal(0)),
                          funding_count=len(settlements), funding_source="exchange_user_funding_ledger")
        except (ValueError, OSError, KeyError, TypeError):
            result.update(funding_net=None, funding_source="exchange_funding_unavailable")
    return result


def lighter(accounts, through, since):
    url = "https://mainnet.zklighter.elliot.ai/api/v1/"
    index, market = re.fullmatch(r"https://mainnet.zklighter.elliot.ai/(\d+)/(\d+)",
                                 accounts["hedge"]).groups()
    account = get(url + "account?" + urlencode({"by":"index","value":index}))["accounts"][0]
    position = next((p for p in account["positions"] if str(p["market_id"]) == market), {})
    market_info = get(url + "orderBookDetails?" + urlencode({"market_id":market}))
    row = next(p for p in market_info["order_book_details"] if str(p["market_id"]) == market)
    amount = Decimal(position.get("position", 0)) * int(position.get("sign", 1))
    mark, liq = row.get("mark_price"), position.get("liquidation_price")
    mode = {0:"cross",1:"isolated"}.get(position.get("margin_mode"))
    margin_percent = Decimal(position.get("initial_margin_fraction", 0))
    leverage = Decimal(100) / margin_percent if margin_percent else None
    if mode == "cross":
        margin_used = (Decimal(position["position_value"]) * margin_percent / 100 if amount else None)
        margin_kind = "position_value_times_initial_margin_percent"
    else:
        margin_used, margin_kind = position.get("allocated_margin"), "exchange_allocated_margin"
    result = dict(position=amount, margin_mode=mode, leverage=leverage, margin_used=margin_used,
                  margin_kind=margin_kind, account_cross_initial_margin=account.get("cross_initial_margin_requirement"),
                  collateral_unit="USDC", mark=mark, liquidation_price=liq,
                  distance_percent=distance(amount, mark, liq),
                  funding_position_total=position.get("total_funding_paid_out"))
    if through is not None:
        # This public exchange field is cumulative for the current position.
        # Report it separately from a ledger if the original position changes.
        size_at_open = tracked_lighter_size()
        current_accum = position.get("total_funding_paid_out")
        result.update(funding_net=current_accum if amount == size_at_open else None,
                      funding_source=("exchange_current_position_accumulator" if amount == size_at_open
                                      else "original_position_changed_public_accumulator_cannot_verify_period"))
    return result


def read_risk(with_funding):
    now = datetime.now(timezone.utc)
    accounts = json.loads(STATE.read_text())["accounts"]
    since = funding_start() if with_funding else None
    answer = {"utc":now.isoformat(), "et":now.astimezone(NY).isoformat(),
              "funding_since_et":since.isoformat() if since is not None else None}
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {name:pool.submit(func,accounts,now if with_funding else None,since)
                   for name,func in (("entropy",entropy),("lighter",lighter))}
        for name, future in futures.items():
            try:
                answer[name] = future.result()
            except Exception as error:
                answer[name] = {"error":type(error).__name__}
    row=answer["lighter"]
    last=OPS/"lighter-original-position-funding.json"
    if with_funding and row.get("funding_net") is None and last.is_file():
        prior=json.loads(last.read_text())
        row["original_position_last_observed_funding"]=prior
    return answer


def save(path, payload):
    tmp = path.with_suffix(".pending")
    tmp.write_text(json.dumps(payload, default=str, ensure_ascii=False) + "\n")
    tmp.replace(path)


def alert(data):
    from ops import publish

    path = OPS / "liquidation-alert.json"
    active = json.loads(path.read_text()) if path.is_file() else {}
    for name in ("entropy", "lighter"):
        row = data[name]
        if "error" in row:
            continue
        gap = row["distance_percent"]
        if gap is not None and gap < 20:
            if not active.get(name):
                publish(f"NVDA liquidation distance under 20% on {name.title()}",
                        f"{data['et']} ET\n{row['position']} NVDA. Mark {row['mark']}, "
                        f"liquidation {row['liquidation_price']}, distance {gap:.2f}%")
                active[name] = True
                save(path, active)
        elif (gap is not None and gap >= 20) or not row["position"]:
            if active.pop(name, None):
                save(path, active)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("command", choices=("read", "watch", "snapshot"))
    cmd=parser.parse_args().command
    data=read_risk(cmd!="watch")
    if cmd=="watch":
        lig=data["lighter"]
        if lig.get("position")==tracked_lighter_size() and lig.get("funding_position_total") is not None:
            last=OPS/"lighter-original-position-funding.json"
            old=json.loads(last.read_text()) if last.is_file() else {}
            value=lig["funding_position_total"]
            if value!=old.get("funding_net"):
                save(last,{"funding_net":value,"seen_et":data["et"]})
        alert(data)
    if cmd=="snapshot":
        d=datetime.now(NY).date().isoformat()
        if d not in ("2026-11-26","2026-11-27"):
            raise SystemExit("outside snapshot dates")
        base = OPS / "position-risk"
        base.mkdir(exist_ok=True)
        save(base / f"{d}.json",data)
    if cmd!="watch":
        print(json.dumps(data, default=str, ensure_ascii=False))
    elif any("error" in data[v] for v in ("entropy","lighter")):
        print(json.dumps({v:data[v].get("error") for v in ("entropy","lighter")
                          if "error" in data[v]}))


if __name__=="__main__":
    main()
