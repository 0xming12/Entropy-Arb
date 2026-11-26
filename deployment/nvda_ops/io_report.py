"""Read Friday NVDA staging latency alongside the same seconds of gp3 telemetry."""
from server import ssm

REMOTE = r'''
from datetime import datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo
import csv,json,math

ny=ZoneInfo("America/New_York")
day="2026-11-27"
journal=Path("/opt/entropy-arb/logs/exec_attempts.jsonl")
path=Path("/run/entropy-nvda-iostat/2026-11-27.csv")
samples={}
if path.is_file():
    with path.open() as f:
        for row in csv.DictReader(f):
            try:
                if row["utc_epoch"]=="utc_epoch": continue
                at=int(row["utc_epoch"])
                samples[at]={key:float(val) for key,val in row.items() if key!="utc_epoch"}
            except (KeyError,ValueError,TypeError):
                pass
attempts=[]
for line in journal.open():
    try:
        obj=json.loads(line)
        wall=float(obj["ts"])
    except (ValueError,KeyError,TypeError): continue
    ts=datetime.fromtimestamp(wall,ny)
    if obj.get("type")!="attempt" or ts.date().isoformat()!=day or not time(9,30)<=ts.time()<time(13):
        continue
    marks=obj.get("marks_ms") or {}
    p=marks.get("prepared")
    s=marks.get("staged")
    if p is None or s is None or not (0<=p<=s):
        continue
    overlap=range(math.floor(wall+p/1000),math.floor(wall+s/1000)+3)
    nearby=[samples[t] for t in overlap if t in samples]
    measured=[x["w_await_ms"] for x in nearby if x["writes_per_sec"]>0]
    flushed=[x["f_await_ms"] for x in nearby if x["flushes_per_sec"]>0]
    attempts.append({"time_et":ts.isoformat(timespec="milliseconds"),"staging_ms":s-p,
                     "samples":len(nearby),"near_max_w_await_ms":max(measured,default=None),
                     "near_max_f_await_ms":max(flushed,default=None),
                     "near_max_queue":max((x["queue"] for x in nearby),default=None)})
def percentile(seq,p):
    if not seq:return None
    a=sorted(seq); r=(len(a)-1)*p; lo=math.floor(r); hi=math.ceil(r)
    return round(a[lo]+(a[hi]-a[lo])*(r-lo),3)
stages=[r["staging_ms"] for r in attempts]
written=[r["w_await_ms"] for r in samples.values() if r["writes_per_sec"]>0]
depth_file=Path("/opt/entropy-arb/logs/nvda-lighter-minutes.depth-2026-11-27.jsonl")
print(json.dumps({"trading_day_et":day,"staging_n":len(stages),"staging_p50_ms":percentile(stages,.5),
                  "staging_p95_ms":percentile(stages,.95),
                  "disk_1s_samples":len(samples),"disk_samples_with_writes":len(written),
                  "device_w_await_sec_interval_p95_ms":percentile(written,.95),
                  "utc_day_depth_bytes_so_far":depth_file.stat().st_size if depth_file.is_file() else None,
                  "slowest_attempts":sorted(attempts,key=lambda r:r["staging_ms"],reverse=True)[:5]}))
'''

if __name__=="__main__":
    print(ssm("nice -n 19 python3 - <<'IO_REPORT'\n"+REMOTE+"\nIO_REPORT\n",timeout=20),end="")
