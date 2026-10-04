#!/bin/bash
# sdc2 變成 ext4 之後的收尾：改 fstab、把 recorder 的目的地從 nvme 換回 NAS、
# 搬回已錄資料、重建目錄、恢復回填。
#
#   sudo tools/post_migrate.sh fstab      # 只有這步要 root
#        tools/post_migrate.sh moveback   # 其餘用一般權限跑
#        tools/post_migrate.sh resume
set -uo pipefail
LABEL=datalake
NEW=/srv/nas/marketdata
STEP=${1:-}

case "$STEP" in
fstab)
  U=$(blkid -s UUID -o value "/dev/disk/by-label/$LABEL") || { echo "找不到 label=$LABEL"; exit 1; }
  cp /etc/fstab /etc/fstab.bak-$(date +%s)
  # 舊的那行是 ntfs3 + automount，換成 ext4 開機直接掛
  sed -i '\|[[:space:]]/srv/nas[[:space:]]|d' /etc/fstab
  echo "UUID=$U  /srv/nas  ext4  defaults,nofail  0  2" >> /etc/fstab
  grep -n '/srv/nas' /etc/fstab
  systemctl daemon-reload
  echo "fstab 已更新（備份在 /etc/fstab.bak-*）"
  ;;
moveback)
  findmnt -rn /srv/nas >/dev/null || { echo "/srv/nas 沒掛載"; exit 1; }
  mkdir -p "$NEW/recorded" "$NEW/parquet"
  # 把當初因為 NAS 掛不上而落在 nvme 的錄製資料搬回去
  L=/home/anson/crypto-datalake/local
  if [ -d "$L/recorded" ]; then
    echo "搬 $L/recorded -> $NEW/recorded"
    rsync -a --remove-source-files "$L/recorded/" "$NEW/recorded/"
    echo "rsync 結束碼 $?"
    find "$L" -type d -empty -delete
  fi
  du -sh "$NEW/recorded" 2>/dev/null; df -h /srv/nas | tail -1
  ;;
resume)
  S=/home/anson/.config/systemd/user/md-recorder.service
  sed -i 's|^Environment=MD_ROOT=.*$|Environment=MD_ROOT=/srv/nas/marketdata|' "$S"
  grep -n MD_ROOT "$S"
  systemctl --user daemon-reload && systemctl --user restart md-recorder
  systemctl --user start md-maintain.timer
  cd /home/anson/crypto-datalake
  MD_ROOT="$NEW" uv run --quiet catalog/db.py build
  echo "recorder 目的地已改回 NAS、maintain.timer 已啟動、目錄已重建。"
  echo "回填請另外起： setsid nohup ./tools/run_backfill.sh > logs/backfill.log 2>&1 &"
  ;;
*) echo "步驟要是 fstab / moveback / resume"; exit 1 ;;
esac
