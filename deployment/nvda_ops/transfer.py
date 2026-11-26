"""Copy only allowlisted public-market files through a short-lived SSM tunnel."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
import subprocess
import shlex
import time

from remote import aws_command, cli, instance, invocation

UNIT = "entropy-nvda-depth-replay-transfer"
PORT = 47631
LOCAL = 47632
PUBLIC_MARKET_FILE = re.compile(
    r"nvda-lighter-minutes\.(depth|entropy-bbo|entropy-trades)-\d{4}-\d{2}-\d{2}\.jsonl"
)


def run_ssm(script):
    return cli("send-command", "--instance-ids", instance(), "--document-name", "AWS-RunShellScript",
               "--comment", "Copy NVDA public depth data for isolated offline replay",
               "--parameters", json.dumps({"commands":[script],"executionTimeout":["35"]}),
               "--query", "Command.CommandId", "--output", "text")


def wait_ssm(cid):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            state = invocation(cid)
        except subprocess.CalledProcessError:
            time.sleep(1)
            continue
        if state["Status"] not in {"InProgress","Pending","Delayed"}:
            if state["Status"] != "Success":
                raise RuntimeError((state["Status"],state["StandardErrorContent"][:300]))
            return state["StandardOutputContent"]
        time.sleep(1)
    raise RuntimeError("SSM did not return in time")


def credits():
    now = datetime.now(timezone.utc)
    data = json.loads(subprocess.check_output([*aws_command(),"cloudwatch","get-metric-statistics",
        "--namespace","AWS/EC2","--metric-name","CPUCreditBalance",
        "--dimensions","Name=InstanceId,Value="+instance(),"--statistics","Average","--period","300",
        "--start-time",(now-timedelta(minutes=30)).isoformat(),"--end-time",now.isoformat(),
        "--output","json"], text=True, timeout=12))
    return sorted((r["Timestamp"],r["Average"]) for r in data["Datapoints"])


def copy(names, output):
    files = list(dict.fromkeys(names))
    if not files or any(PUBLIC_MARKET_FILE.fullmatch(name) is None for name in files):
        raise ValueError("provide only public NVDA depth, BBO or trade files")
    output.mkdir(exist_ok=True, parents=True)
    baseline = credits()
    if not baseline:
        raise RuntimeError("cannot check CloudWatch CPU credits")
    print("CPU credits before",baseline[-1],flush=True)
    threshold = baseline[-1][1] - 5
    if baseline[-1][1] < baseline[0][1] - 5:
        raise RuntimeError("CPU credit balance is already materially lower")
    server = f"""
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import gzip

root=Path('/opt/entropy-arb/logs')
files={{'/'+p.name:(p,p.stat().st_size) for p in (root / n for n in {files!r})}}
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        match=files.get(self.path)
        if match is None:
            if self.path=='/ready':
                self.send_response(200); self.send_header('Content-Length','0'); self.end_headers()
            else:
                self.send_error(404)
            return
        path,nbytes=match
        self.send_response(200)
        self.send_header('Content-Encoding','gzip')
        self.send_header('X-Uncompressed-Bytes',str(nbytes))
        self.end_headers()
        with path.open('rb') as f, gzip.GzipFile(fileobj=self.wfile, mode='wb', compresslevel=1) as stream:
            remaining=nbytes
            while remaining:
                chunk=f.read(min(remaining,262144))
                if not chunk:
                    raise OSError('file shortened during copy')
                stream.write(chunk)
                remaining-=len(chunk)
    def log_message(self,*args):
        pass
HTTPServer(('127.0.0.1',{PORT}),Handler).serve_forever()
"""
    start = ("systemd-run --unit=" + UNIT + " --quiet --property=User=entropyarb "
          "--property=Group=entropyarb --property=Nice=19 --property=CPUWeight=1 "
          "--property=IOWeight=1 --property=RuntimeMaxSec=360s /usr/bin/python3 -u -c " + shlex.quote(server))
    tunnel = None
    copying = None
    started = False
    try:
        wait_ssm(run_ssm(start))
        started = True
        args = [*aws_command(),"ssm","start-session","--target",instance(),"--document-name",
                "AWS-StartPortForwardingSession","--parameters",
                json.dumps({"portNumber":[str(PORT)],"localPortNumber":[str(LOCAL)]})]
        with (output/"tunnel.log").open("wb") as log:
            tunnel = subprocess.Popen(args,stdout=log,stderr=subprocess.STDOUT)
        base = f"http://127.0.0.1:{LOCAL}/"
        for attempts in range(12):
            if subprocess.run(["curl","-fsS","--max-time","2",base+"ready"],
                              stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode == 0:
                break
            if tunnel.poll() is not None:
                raise RuntimeError("SSM tunnel did not start")
            time.sleep(1)
        else:
            raise RuntimeError("SSM port-forward did not reach private listener")
        next_poll = time.monotonic()
        for name in files:
            target = output/name
            temp = target.with_suffix(".part")
            headers = target.with_suffix(".headers")
            copying = subprocess.Popen(["curl","-fsS","--compressed","--max-time","105","--limit-rate","7M",
                                         "--dump-header",str(headers),"--output",str(temp),base+name])
            while copying.poll() is None:
                if time.monotonic() >= next_poll:
                    if credits()[-1][1] <= threshold:
                        raise RuntimeError("CPUCreditBalance dropped >=5 credits; transfer stopped")
                    next_poll = time.monotonic()+12
                time.sleep(.8)
            if copying.returncode != 0:
                raise RuntimeError("Depth transfer failed")
            sizes = re.findall(r"^x-uncompressed-bytes: (\d+)",headers.read_text(),re.I|re.M)
            headers.unlink()
            if len(sizes) != 1 or int(sizes[0]) != temp.stat().st_size:
                raise RuntimeError("public archive transfer length does not match source")
            temp.replace(target)
            print(name,target.stat().st_size,flush=True)
        print("CPU credits after",credits()[-1],flush=True)
    finally:
        if copying and copying.poll() is None:
            copying.terminate()
        if tunnel and tunnel.poll() is None:
            tunnel.terminate()
        if started:
            wait_ssm(run_ssm(f"systemctl stop {UNIT}.service || true"))
