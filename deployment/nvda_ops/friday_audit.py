"""Audit November 27's 13:00 ET cutoff and retain disk-timing snapshots."""
import json
from datetime import datetime, timezone
from decimal import Decimal
import re
import subprocess
from pathlib import Path
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from risk_snapshot import get

ROOT=Path("/opt/entropy-arb")
OPS=Path("/var/lib/entropy-nvda-ops")
NY=ZoneInfo("America/New_York")
START=datetime(2026,11,27,13,tzinfo=NY)
END=datetime(2026,11,28,tzinfo=NY)


def lighter_fills(holder, end):
    account, market=map(int,re.fullmatch(r"https://mainnet.zklighter.elliot.ai/(\d+)/(\d+)",holder).groups())
    root="https://mainnet.zklighter.elliot.ai/api/v1/"
    def current():
        data=get(root+"account?"+urlencode({"by":"index","value":account}))["accounts"][0]
        p=next((x for x in data["positions"] if x["market_id"]==market),{})
        return Decimal(p.get("position",0))*int(p.get("sign",1))
    before=current()
    rows={}
    cursor=None
    pages=set()
    fields={"account_index":account,"market_id":market,"sort_by":"timestamp","sort_dir":"desc","limit":100}
    for _ in range(25):
        req=get(root+"trades?"+urlencode({**fields,**({"cursor":cursor} if cursor else {})}))
        if req.get("code")!=200:
            raise ValueError("exchange trades unavailable")
        older=False
        for trade in req["trades"]:
            ts=float(trade["timestamp"])
            if ts>10**11:
                ts/=1000
            if ts<START.timestamp():
                older=True
                continue
            matches=[trade["ask_account_id"]==account,trade["bid_account_id"]==account]
            if sum(matches)!=1:
                raise ValueError("unexpected account trade identity")
            is_ask=matches[0]
            role="maker" if is_ask==trade["is_maker_ask"] else "taker"
            key=int(trade["trade_id"])
            rows[key]=(ts, (-1 if is_ask else 1)*Decimal(trade["size"]),
                       Decimal(trade[role+"_position_size_before"]))
        cursor=req.get("next_cursor")
        if older or not cursor or not req["trades"]:
            break
        if cursor in pages:
            raise ValueError("repeated exchange cursor")
        pages.add(cursor)
    else:
        raise ValueError("exchange history pagination limit")
    position=current()
    if position!=before:
        raise ValueError("position changed during exchange snapshot")
    count=opened=0
    for _,(ts,qty,previous_abs) in sorted(rows.items(),key=lambda row:(row[1][0],row[0]),reverse=True):
        earlier=position-qty
        if abs(abs(earlier)-abs(previous_abs))>Decimal("0.000001"):
            raise ValueError("account position chain does not match trade history")
        if ts<end.timestamp():
            count+=1
            opened+=abs(position)>abs(earlier) or position*earlier<0
        position=earlier
    return count,opened


def audit():
    date=datetime.now(timezone.utc)
    end=min(date,END)
    payload={"from_et":START.isoformat(),"asof_et":end.astimezone(NY).isoformat(),
             "full_friday_after_close":date>=END,"engine_attempts_after_close":0,
             "engine_non_reduce_only_attempts":0,"engine_non_reduce_only_filled_legs":0}
    if date < START:
        raise SystemExit("audit window has not arrived")
    path=ROOT/"logs/exec_attempts.jsonl"
    with path.open("rb") as stream:
        for line in stream:
            if not line.endswith(b"\n"):
                break
            obj=json.loads(line)
            ts=obj.get("ts")
            if obj.get("type")!="attempt" or ts is None or not START.timestamp()<=float(ts)<end.timestamp():
                continue
            payload["engine_attempts_after_close"]+=1
            if obj.get("reduce_only") is not True:
                payload["engine_non_reduce_only_attempts"]+=1
                payload["engine_non_reduce_only_filled_legs"]+=sum(
                    (row.get("filled_base") or 0)>0 for row in (obj.get("legs") or {}).values())
    try:
        s=json.loads((ROOT/"data/live-test/NVDA_lighter_state.json").read_text())
        addr=re.fullmatch(r"https://api.hyperliquid.xyz/(0x[0-9a-fA-F]{40})/io:NVDA",s["accounts"]["entropy"])[1]
        fills=get("https://api.hyperliquid.xyz/info",{"type":"userFillsByTime","user":addr,
                  "startTime":int(START.timestamp()*1000),"endTime":int(end.timestamp()*1000),
                  "aggregateByTime":False})
        if not isinstance(fills,list) or len(fills)>=2000:
            raise ValueError("unsupported or truncated exchange response")
        pair=[r for r in fills if r.get("coin")=="io:NVDA"]
        directions=(r.get("dir","").lower() for r in pair)
        opened=[d for d in directions if "open" in d or (" > " in d or " >" in d)]
        payload.update(entropy_exchange_fills=len(pair),entropy_exchange_opening_or_flip_fills=len(opened))
    except Exception as error:
        payload.update(entropy_exchange_status=f"unavailable:{type(error).__name__}")
    try:
        holder=json.loads((ROOT/"data/live-test/NVDA_lighter_state.json").read_text())["accounts"]["hedge"]
        fills,opens=lighter_fills(holder,end)
        payload.update(lighter_exchange_fills=fills,lighter_exchange_opening_or_flip_fills=opens)
    except Exception as error:
        payload.update(lighter_exchange_status=f"unavailable:{type(error).__name__}")
    payload["disk"]=json.loads(subprocess.check_output(
        ["/usr/bin/python3","/opt/entropy-nvda-ops/io_metrics.py"],text=True,timeout=18))
    p=OPS/("friday-audit-full.json" if date>=END else "friday-audit-close.json")
    tmp=p.with_suffix(".pending")
    tmp.write_text(json.dumps(payload,ensure_ascii=False)+"\n")
    tmp.replace(p)
    print(json.dumps(payload,ensure_ascii=False))


if __name__=="__main__":
    audit()
