#!/bin/bash
# 全幣聚合 1 分鐘 bar。與牆偵測並行，所以壓低優先序與執行緒數——
# 兩者都不是即時作業，recorder 才是。
set -u
cd /home/anson/crypto-datalake
nice -n 18 ionice -c2 -n7 uv run --quiet research/build_bars.py --mem 5GB
echo "======== $(date '+%F %T')  bar 聚合完成 ========"
