#!/bin/bash
# 依優先序回填。小檔多的資料集用高並行（瓶頸在往返延遲），大檔用低並行（瓶頸在頻寬/磁碟）。
cd /home/anson/crypto-datalake
run() { echo "======== $(date '+%F %T')  $1  (jobs=$2) ========";
        nice -n 10 ionice -c2 -n6 uv run --quiet backfill/vision.py fetch --dataset "$1" --jobs "$2" --rate-mbps "${MD_RATE:-6}"; }
run um/metrics 24
run um/fundingRate 16
run um/klines1m 16
run um/markPrice1m 16
run um/bookDepth 24
run spot/klines1s 8
run um/trades 4
run um/aggTrades 4
run um/bookTicker 4
run spot/aggTrades 6
# 暫時性失敗（斷線/逾時/503/校驗不符）重置後再跑一輪，避免抖動變成永久缺口
echo "======== $(date '+%F %T')  重試失敗檔 ========"
uv run --quiet backfill/vision.py retry
for ds in um/trades um/aggTrades um/bookTicker spot/aggTrades um/bookDepth; do
  run "$ds" 4
done
echo "======== $(date '+%F %T')  全部回填結束 ========"
