"""Verify and copy NVDA public tape and five-level depth to the isolated workspace."""
import argparse
from datetime import date
import json
from pathlib import Path

from server import ssm
import transfer

HERE = Path(__file__).parent
PREFIX = "nvda-lighter-minutes"


def names(day):
    return [f"{PREFIX}.{kind}-{day}.jsonl" for kind in ("depth", "entropy-bbo", "entropy-trades")]


def inspect(days, include_early_close=False):
    request = r'''
import json
from pathlib import Path
import subprocess
from datetime import datetime,time
from zoneinfo import ZoneInfo
import yaml
root=Path("/opt/entropy-arb")
ny=ZoneInfo("America/New_York")
cfg=yaml.safe_load((root/"config.live.yaml").read_text())
sections=("thresholds","entropy","hedge","sizing","inventory","execution","risk","session_filter","dynamic_midline","funding_skew")
def replay_config(config):
    clean={}
    for section in sections:
        if section not in config:
            continue
        value=config[section]
        if not isinstance(value,dict):
            raise ValueError("unexpected configuration")
        clean[section]={k:v for k,v in value.items()
            if not any(x in k.lower() for x in ["secret","key","wallet","private","password","address","token","auth","seed","credential","account","endpoint","url"])}
    return clean

def edge(path, first):
    with path.open("rb") as f:
        stat=f.seek(0,2)
        seek=max(0,stat-128000) if not first else 0
        f.seek(seek)
        if seek: f.readline()
        rows=f.read(128000).splitlines()
    if not first:
        rows=reversed(rows)
    for line in rows:
        try:
            r=json.loads(line)
            return r["ts"] if "ts" in r else r["exchange_ts_ms"]/1000
        except (ValueError,KeyError,TypeError):
            pass
    return None

result={}
for value in dates:
    local=datetime.fromisoformat(value).date()
    if local.weekday()>=5 or value=="2026-11-26" or (value=="2026-11-27" and not include_early_close):
        raise ValueError("not a complete regular session")
    start=datetime.combine(local,time(9,30),ny)
    end=datetime.combine(local,time(13 if value=="2026-11-27" else 16),ny)
    if datetime.now(ny)<end:
        result[value]={"ready":False,"reason":"regular session still open"}
        continue
    summary={}
    for kind in ("depth","entropy-bbo","entropy-trades"):
        f=root/"logs"/f"nvda-lighter-minutes.{kind}-{value}.jsonl"
        if f.is_file():
            summary[kind]={"bytes":f.stat().st_size,"first_s":edge(f,True),"last_s":edge(f,False)}
    ready=all(kind in summary and summary[kind]["bytes"] and summary[kind]["first_s"] is not None
              for kind in ("depth","entropy-bbo","entropy-trades"))
    ready=bool(ready and summary["depth"]["first_s"] <= start.timestamp()+1
               and summary["depth"]["last_s"] >= end.timestamp()-1)
    since=start.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S UTC")
    until=end.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S UTC")
    warnings=subprocess.run(["journalctl","-u","entropy-arb-record.service","--since",since,"--until",until,
                             "-o","cat","--no-pager","-g","public capture interrupted|public trades subscribed"],
                            text=True,capture_output=True,timeout=10).stdout.splitlines()
    result[value]={"ready":ready,"summary":summary,"public_ws_connection_events":len(warnings),"early_close":value=="2026-11-27"}
print(json.dumps({"days":result,"replay_config":replay_config(cfg)}))
'''
    return json.loads(ssm("nice -n 19 python3 - <<'REPORT'\ndates=" + repr(days)
                          + "\ninclude_early_close=" + repr(include_early_close) + "\n" + request + "\nREPORT\n", timeout=40))


def download(days, folder):
    for day in days:
        missing = [name for name in names(day) if not (folder / name).is_file()]
        if missing:
            transfer.copy(missing, folder)


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("days", nargs="+", type=lambda value: date.fromisoformat(value).isoformat())
    operation=args.add_mutually_exclusive_group()
    operation.add_argument("--download",action="store_true")
    operation.add_argument("--stage",action="store_true",help="prefetch any ready input without running the replay")
    args.add_argument("--include-early-close", action="store_true")
    options = args.parse_args()
    data = inspect(options.days, options.include_early_close)
    print(json.dumps(data["days"], indent=2))
    if options.download or options.stage:
        qualified = list(dict.fromkeys(day for day in options.days if data["days"][day]["ready"] and day!="2026-11-27"))
        if options.download and len(set(qualified)) < 3:
            raise SystemExit("Require at least three closed regular sessions with public log coverage before replay download.")
        folder = HERE / "input"
        folder.mkdir(exist_ok=True, mode=0o700)
        reference = folder / "strategy.reference.json"
        if not reference.exists():
            reference.write_text(json.dumps(data["replay_config"], indent=2))
        elif json.loads(reference.read_text()) != data["replay_config"]:
            archived = folder / "strategy.observed-after-reference.json"
            archived.write_text(json.dumps(data["replay_config"], indent=2))
            print("Configuration differs from the original isolated replay reference.")
        from replay import download_funding

        selected=qualified[:3]
        if options.include_early_close and data["days"].get("2026-11-27",{}).get("ready"):
            selected.append("2026-11-27")
        for day in selected:
            download_funding(folder, date.fromisoformat(day))
        download(selected, folder)
