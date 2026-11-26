#!/bin/bash
set -eo pipefail
export LC_ALL=C
disk=${ENTROPY_NVDA_DISK_DEVICE:-nvme0n1}
printf 'utc_epoch,writes_per_sec,write_kib_per_sec,w_await_ms,flushes_per_sec,f_await_ms,queue,util_percent\n'
stdbuf -oL iostat -d -x -y "$disk" 1 |
    awk -v disk="$disk" '$1==disk{printf "%d,%s,%s,%s,%s,%s,%s,%s\n",systime(),$8,$9,$12,$18,$19,$20,$21;fflush()}'
