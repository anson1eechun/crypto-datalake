#!/bin/bash
# 每 30 秒記一次 IO/CPU/記憶體壓力與 recorder 斷線數，用來判斷回填期間的斷線是不是 IO 阻塞造成的。
# PSI 的 some avg10 = 過去 10 秒有多少比例的時間至少一個行程卡在該資源上。
set -u
cd /home/anson/crypto-datalake
OUT=logs/io_probe.tsv
[ -f "$OUT" ] || echo -e "ts\tio_some\tio_full\tcpu_some\tmem_some\trec_state\tdisc\tnas_mb" > "$OUT"

psi() { awk -v k="$1" '$1==k{sub("avg10=","",$2); print $2}' "$2"; }
# 裝置名重開機會變（本機已發生 sda->sdc），所以每次從 /srv/nas 反查
NASDEV=$(lsblk -no pkname "$(findmnt -rn -o SOURCE /srv/nas)" 2>/dev/null)
nas_kb() { awk -v d="$NASDEV" '$3==d{print $10*512/1024}' /proc/diskstats; }  # 欄10=累計寫入磁區

prev=$(nas_kb)
while :; do
  sleep 30
  pid=$(pgrep -f 'recorder/record\.py' | head -1)
  st=$([ -n "$pid" ] && awk '{print $3}' /proc/"$pid"/stat || echo "-")
  cur=$(nas_kb)
  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%.1f\n" \
    "$(date '+%F %T')" \
    "$(psi some /proc/pressure/io)" "$(psi full /proc/pressure/io)" \
    "$(psi some /proc/pressure/cpu)" "$(psi some /proc/pressure/memory)" \
    "$st" \
    "$(grep -a -o '斷線 [0-9]*' logs/recorder.log | tail -1 | tr -dc 0-9)" \
    "$(echo "($cur-$prev)/1024/30" | bc -l)" >> "$OUT"
  prev=$cur
done
