#!/bin/bash
# 全幣全期間跑牆偵測。單幣 24 小時約 280 秒、峰值 3.5 GB，所以限制平行度，
# 並用 nice 讓 recorder 優先——錄製中斷補不回來，偵測隨時可重跑。
set -u
cd /home/anson/crypto-datalake
mkdir -p research/events
xargs -a logs/symbols.txt -P 5 -I{} \
  nice -n 15 sh -c 'uv run --quiet research/flow.py --symbol {} --hours 260 \
    --chunk-hours 4 --db state/research.duckdb 2>&1 | head -1'
echo "======== $(date '+%F %T')  偵測完成 ========"
