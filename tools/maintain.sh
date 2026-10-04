#!/bin/bash
# 每小時維護：把已封存的錄製時檔轉成 parquet，把新下載的 vision 資料轉檔，更新目錄。
set -u
cd /home/anson/crypto-datalake
export MD_ROOT=/srv/nas/marketdata
UV=/home/anson/.local/bin/uv

echo "===== $(date '+%F %T') 維護開始 ====="
FREE=$(df --output=avail -k /srv/nas | tail -1)
echo "NAS 可用: $((FREE/1024/1024)) GB"
if [ "$FREE" -lt 314572800 ]; then      # < 300 GB
  echo "!! NAS 可用空間低於 300GB，請處理後再繼續寫入"
fi

nice -n 10 $UV run --quiet catalog/normalize_recorded.py --jobs 2 2>&1 | tail -3

# 大資料集的月檔一個就幾億列，normalize 每個 job 緩衝 200 萬列；開 3 個會超過
# MemoryHigh=3G 而被 systemd-oomd 殺掉（2026-09-17 每小時都死）。大的一律單工。
for ds in um/metrics um/fundingRate um/klines1m um/markPrice1m um/bookDepth spot/klines1s; do
  nice -n 10 $UV run --quiet catalog/normalize.py --dataset "$ds" --jobs 3 2>&1 \
    | grep -Ev '^\s+[0-9]+/' | tail -2
done
for ds in um/bookTicker um/aggTrades um/trades spot/aggTrades; do
  nice -n 10 $UV run --quiet catalog/normalize.py --dataset "$ds" --jobs 1 --limit 40 2>&1 \
    | grep -Ev '^\s+[0-9]+/' | tail -2
done

$UV run --quiet catalog/db.py build
echo "===== $(date '+%F %T') 維護結束 ====="
