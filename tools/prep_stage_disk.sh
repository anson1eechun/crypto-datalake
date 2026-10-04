#!/bin/bash
# 把 sdb2（SSD_MAIN, NTFS）改成 ext4 當 go3 的中繼站，再把 go3 搬過去。
# 全程零 ntfs3 kernel 寫入：來源唯讀掛載，目的地是 ext4。
#
# 裝置名重開機會變，所以用檔案系統 UUID 認碟；格式化後改用 label 認。
#
#   sudo tools/prep_stage_disk.sh save
#   sudo CONFIRM=format tools/prep_stage_disk.sh format
#   sudo tools/prep_stage_disk.sh copy
set -uo pipefail

NTFS_UUID=3C818ECF20E12FF5      # sdb2 (SSD_MAIN) 現在的 NTFS UUID
LABEL=stage                     # 格式化後給 ext4 的標籤
KEEP=/home/anson/nas-staging/sdb2-keep
MNT=/mnt/stage
STAGE=${1:-}

case "$STAGE" in
save)
  link=/dev/disk/by-uuid/$NTFS_UUID
  [ -e "$link" ] || { echo "找不到 NTFS UUID $NTFS_UUID"; exit 1; }
  P=$(readlink -f "$link"); echo "來源 $P"
  mkdir -p /mnt/chk "$KEEP"
  mount -t ntfs3 -o ro "$P" /mnt/chk || exit 1
  # carve 是 PhotoRec 備援碎片，復原已由 factorlab_rebuilt 完成，不保留
  rsync -a --exclude=carve --exclude='$RECYCLE.BIN' --exclude='System Volume Information' \
        /mnt/chk/ "$KEEP/"
  rc=$?
  echo "rsync 結束碼 $rc"
  echo "來源(不含 carve): $(find /mnt/chk -mindepth 1 -maxdepth 1 ! -name carve -exec du -sb {} + 2>/dev/null | awk '{s+=$1}END{print s}') bytes"
  echo "目的:             $(du -sb "$KEEP" 2>/dev/null | cut -f1) bytes"
  du -sh "$KEEP"; umount /mnt/chk
  echo "確認上面兩個數字相近後，再跑 format"
  ;;
format)
  [ "${CONFIRM:-}" = format ] || { echo "需要 CONFIRM=format"; exit 1; }
  link=/dev/disk/by-uuid/$NTFS_UUID
  [ -e "$link" ] || { echo "找不到 NTFS UUID $NTFS_UUID（已經格式化過了？）"; exit 1; }
  P=$(readlink -f "$link")
  [ -d "$KEEP" ] && [ -n "$(ls -A "$KEEP" 2>/dev/null)" ] || { echo "$KEEP 是空的，先跑 save"; exit 1; }
  findmnt -rn "$P" >/dev/null && { echo "$P 掛載中"; exit 1; }
  # 保險：目標必須是這顆 191G 的分割，不能是 10.9T 那顆
  sz=$(blockdev --getsize64 "$P")
  [ "$sz" -lt 300000000000 ] || { echo "目標 $P 大小 $sz 不像 sdb2，中止"; exit 1; }
  echo "即將格式化 $P ($(numfmt --to=iec $sz)) 為 ext4 label=$LABEL —— 不可逆"
  sleep 5
  mkfs.ext4 -m 0 -L "$LABEL" "$P" && blkid "$P"
  ;;
copy)
  P=/dev/disk/by-label/$LABEL
  [ -e "$P" ] || { echo "找不到 label=$LABEL 的分割區，先跑 format"; exit 1; }
  mkdir -p "$MNT"; findmnt -rn "$MNT" >/dev/null || mount "$P" "$MNT"
  chown anson:anson "$MNT"
  findmnt -rn /srv/nas >/dev/null || { echo "/srv/nas 沒掛載"; exit 1; }
  # 先搬主體（不含 LRV 低解析代理檔），確認空間夠再補 LRV
  for part in main lrv; do
    if [ "$part" = main ]; then EX="--exclude=LRV"; else EX=""; fi
    echo "===== $part $(date '+%T') ====="
    sudo -u anson rsync -a $EX /srv/nas/Anson/go3 "$MNT/"
    echo "rsync 結束碼 $?"; df -h "$MNT" | tail -1
  done
  echo "來源: $(find /srv/nas/Anson/go3 -type f | wc -l) 檔 $(du -sb /srv/nas/Anson/go3 | cut -f1) bytes"
  echo "目的: $(find $MNT/go3 -type f | wc -l) 檔 $(du -sb $MNT/go3 | cut -f1) bytes"
  ;;
*) echo "階段要是 save / format / copy"; exit 1 ;;
esac
