"""Retrieve an allowlisted server-side snapshot unchanged."""
import argparse
from server import ssm

ROOT="/var/lib/entropy-nvda-ops"
FILES={
    "risk-evening":["position-risk/2026-11-26.json"],
    "risk-preopen":["position-risk/2026-11-27.json"],
    "weekend":["weekend-restart/status.json","weekend-restart/order-id-proof.json","friday-audit-full.json"],
    "friday":["friday-audit-close.json"],
    "disk":["archive-disk.json"],
}

def read_result(which):
    paths=FILES[which]
    cmd=f"cd {ROOT}\n"
    for p in paths:
        cmd+=f"if test -f {p}; then printf '%s\\n' '--- {p} ---'; cat {p}; fi\n"
    print(ssm(cmd,20),end="")

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("which",choices=FILES)
    read_result(p.parse_args().which)
