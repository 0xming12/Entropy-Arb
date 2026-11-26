"""Fetch public Entropy io:NVDA maker and taker fee estimates for replay."""
from server import ssm

CODE = r'''
import json,re
from pathlib import Path
from urllib.request import Request,urlopen
state=json.loads(Path("/opt/entropy-arb/data/live-test/NVDA_lighter_state.json").read_text())
match=re.fullmatch(r"https://api.hyperliquid.xyz/(0x[0-9a-fA-F]{40})/io:NVDA",state["accounts"]["entropy"])
if not match: raise ValueError("saved public address unavailable")
def info(payload):
    request=Request("https://api.hyperliquid.xyz/info",json.dumps(payload).encode(),
                    headers={"Content-Type":"application/json"})
    with urlopen(request,timeout=7) as handle:
        return json.load(handle)
fees=info({"type":"userFees","user":match[1]})
meta=info({"type":"meta","dex":"io"})
asset=next(x for x in meta["universe"] if x["name"]=="io:NVDA")
deployer=next(x for x in info({"type":"perpDexs"}) if x and x["name"]=="io")
dfs=float(asset.get("deployerFeeScale", meta.get("deployerFeeScale",deployer.get("deployerFeeScale", 1.0))))
growth=.1 if asset.get("growthMode") in ("enabled",True) else 1
mult=(1+dfs if dfs<1 else 2*dfs)*growth*(1-float(fees.get("activeReferralDiscount",0)))
maker=float(fees["userAddRate"])
if maker < 0:
    raise ValueError("maker rebate requires aligned-collateral verification")
print(json.dumps({"entropy_maker_model_bps":maker*mult*10000,
                  "entropy_taker_model_bps_before_aligned_discount":float(fees["userCrossRate"])*mult*10000,
                  "source":"HL public userFees and HIP-3 meta; modeled rate, no order sent"}))
'''
if __name__ == "__main__":
    print(ssm("python3 - <<'PUBLIC_FEES'\n" + CODE + "\nPUBLIC_FEES", timeout=30), end="")
