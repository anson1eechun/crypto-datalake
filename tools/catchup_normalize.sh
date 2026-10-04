#!/bin/bash
# 一次性補轉回填下來的大資料集。單工執行：normalize 每個 job 緩衝 200 萬列，
# 多工會把記憶體撞破（md-maintain 因此被 systemd-oomd 連續砍了好幾小時）。
set -u
cd /home/anson/crypto-datalake
for ds in um/trades um/bookTicker um/aggTrades spot/aggTrades; do
  echo "======== $(date '+%F %T')  $ds ========"
  nice -n 15 ionice -c2 -n7 uv run --quiet catalog/normalize.py --dataset "$ds" --jobs 1 2>&1 \
    | grep -Ev '^\s+[0-9]+/' | tail -5
done
echo "======== $(date '+%F %T')  補轉結束 ========"
