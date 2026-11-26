"""Archive immutable NVDA UTC-day market logs without removing their source files."""
import argparse
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
import json
from pathlib import Path
import re
import shutil
import tempfile
from zipfile import ZIP_DEFLATED, ZipFile

LOGS = Path("/opt/entropy-arb/logs")
OPS = Path("/var/lib/entropy-nvda-ops")
DAILY_LOG = re.compile(r"nvda-lighter-minutes\.(depth|entropy-trades)-(\d{4}-\d{2}-\d{2})\.jsonl$")


def archive(now, root, destination):
    groups = defaultdict(list)
    cutoff = now - timedelta(days=3)
    for path in root.glob("nvda-lighter-minutes.*.jsonl"):
        match = DAILY_LOG.fullmatch(path.name)
        if not match or path.is_symlink():
            continue
        day = date.fromisoformat(match[2])
        end = datetime.combine(day + timedelta(days=1), time(), timezone.utc)
        stat = path.stat()
        if end <= cutoff and datetime.fromtimestamp(stat.st_mtime, timezone.utc) <= cutoff:
            groups[day].append((path, stat.st_size, stat.st_mtime_ns))
    destination.mkdir(mode=0o750, exist_ok=True)
    before = shutil.disk_usage(root)
    for day, sources in sorted(groups.items()):
        target = destination / f"nvda-depth-and-trades-{day}.zip"
        if target.exists():
            # Immutable valid ZIPs are only read once while reporting. Confirm the
            # member names/lengths still match the archived source inventory.
            with ZipFile(target) as previous:
                if {item.filename: item.file_size for item in previous.infolist()} == {p.name:n for p,n,_ in sources}:
                    continue
        if before.free - sum(n for _, n, _ in sources) < 128 * 1024 * 1024:
            raise OSError("insufficient temporary space for safe ZIP")
        with tempfile.NamedTemporaryFile(dir=destination, prefix=".nvda-archive-", delete=False) as temp:
            working = Path(temp.name)
        try:
            with ZipFile(working, "w", compression=ZIP_DEFLATED, compresslevel=1) as archive_file:
                for path, size, modified in sorted(sources):
                    archive_file.write(path, path.name)
                    current = path.stat()
                    if current.st_size != size or current.st_mtime_ns != modified:
                        raise RuntimeError("source changed during archive; ZIP rejected")
            with ZipFile(working) as done:
                if done.testzip() is not None:
                    raise RuntimeError("ZIP integrity check failed")
            working.chmod(0o640)
            working.replace(target)
        finally:
            working.unlink(missing_ok=True)
    return groups


def alert_if_low(free, now, ops):
    from ops import publish
    state = ops / "disk-low.json"
    day = now.date().isoformat()
    if free >= 5_000_000_000:
        state.unlink(missing_ok=True)
    elif not state.is_file() or json.loads(state.read_text()).get("utc_day") != day:
        mid = publish("NVDA AWS disk below 5 GB",
                      f"{now.isoformat()} UTC. Disk available: {free / 1e9:.3f} GB.")
        temp=state.with_suffix(".pending")
        temp.write_text(json.dumps({"utc_day":day,"message_id":mid})+"\n")
        temp.replace(state)


def metrics(now, root, archives, groups):
    records=[]
    for day in sorted(groups):
        path=archives / f"nvda-depth-and-trades-{day}.zip"
        if not path.exists():
            continue
        with ZipFile(path) as z:
            files=z.infolist()
        records.append(dict(utc_day=str(day),members=[x.filename for x in files],
                            raw_bytes=sum(x.file_size for x in files),zip_bytes=path.stat().st_size))
    raw_total=sum(x["raw_bytes"] for x in records)
    zip_total=sum(x["zip_bytes"] for x in records)
    ratio=zip_total/raw_total if raw_total else None
    today=now.date().isoformat()
    today_raw=sum(p.stat().st_size for p in root.glob(f"nvda-lighter-minutes.*-{today}.jsonl")
                  if DAILY_LOG.fullmatch(p.name))
    elapsed=(now-datetime.combine(now.date(),time(),timezone.utc)).total_seconds()
    per_day=today_raw*86400/elapsed if today_raw and elapsed>=1800 else None
    space=shutil.disk_usage(root)
    service_growth=per_day*(1+ratio) if per_day and ratio is not None else None
    return dict(asof_utc=now.isoformat(),archives=records,source_bytes=raw_total,zip_bytes=zip_total,
                zip_to_source_ratio=ratio,free_bytes=space.free,todays_depth_and_tape_bytes=today_raw,
                raw_bytes_per_day_estimate=per_day,zip_bytes_per_day_estimate=per_day*ratio if per_day and ratio else None,
                actual_bytes_per_day_retaining_source_and_zip=service_growth,
                estimated_days_until_5gb=max(space.free-5e9,0)/service_growth if service_growth else None,
                estimated_days_until_full=space.free/service_growth if service_growth else None)


def main():
    parser=argparse.ArgumentParser()
    parser.parse_args()
    now=datetime.now(timezone.utc)
    archives=LOGS / "archives"
    alert_if_low(shutil.disk_usage(LOGS).free,now,OPS)
    groups=archive(now,LOGS,archives)
    result=metrics(now,LOGS,archives,groups)
    alert_if_low(result["free_bytes"],now,OPS)
    data=json.dumps(result,ensure_ascii=False)+"\n"
    temp=OPS/"archive-disk.pending"
    temp.write_text(data)
    temp.replace(OPS/"archive-disk.json")
    print(data,end="")


if __name__=="__main__":
    main()
