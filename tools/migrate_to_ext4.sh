#!/bin/bash
# 把 10.9T 的 sdc2 從 NTFS 改成 ext4 資料湖。
#
# 路線是整顆重來，不縮容也不改分割表（ntfsresize 因 $Bitmap 有 86542 個叢集
# 記帳不一致而拒絕執行，且無 Windows 可跑 chkdsk）。既有分割區直接 mkfs，
# 分割表原封不動，所以沒有算錯起始磁區的風險。
#
#   sudo tools/migrate_to_ext4.sh verify
#   sudo CONFIRM=format tools/migrate_to_ext4.sh format
#   sudo tools/migrate_to_ext4.sh restore
set -uo pipefail

NTFS_UUID=8ADCC067DCC04EDF          # 這顆 10.9T 現在的 NTFS UUID
LABEL=datalake
MIN_BYTES=10000000000000            # 目標必須大於 10TB，否則不是這顆
SRC=/srv/nas/Anson
NVME=/home/anson/nas-staging        # iphone-backup, iphone17 在這
STAGE=/mnt/stage                    # go3 在這（sdb2 改成的 ext4）
STEP=${1:-}

# 逐項比對「來源」與「副本」的檔案數與位元組數。任何一項對不上就不准格式化。
verify_all() {
  local ok=0
  findmnt -rn /srv/nas >/dev/null || { echo "/srv/nas 沒掛載，無法比對"; return 1; }
  for spec in "go3:$STAGE/go3" "iphone-backup:$NVME/iphone-backup" "iphone17:$NVME/iphone17"; do
    local name=${spec%%:*} dst=${spec#*:} s="$SRC/${spec%%:*}"
    if [ ! -d "$dst" ]; then echo "  $name  副本不存在: $dst   ✗"; ok=1; continue; fi
    local sn sb dn db
    sn=$(find "$s"   -type f 2>/dev/null | wc -l); sb=$(du -sb "$s"   2>/dev/null | cut -f1)
    dn=$(find "$dst" -type f 2>/dev/null | wc -l); db=$(du -sb "$dst" 2>/dev/null | cut -f1)
    if [ "$sn" = "$dn" ] && [ "$sb" = "$db" ]; then
      printf "  %-14s %7d 檔 %15s bytes   ✓\n" "$name" "$sn" "$sb"
    else
      printf "  %-14s 來源 %d 檔/%s  副本 %d 檔/%s   ✗\n" "$name" "$sn" "$sb" "$dn" "$db"
      ok=1
    fi
  done
  return $ok
}

case "$STEP" in
verify)
  echo "比對不可再生資料的副本："; verify_all
  rc=$?
  [ $rc = 0 ] && echo "全部通過。marketdata 可重下、Recovered 與 carve 使用者已確認不留。" \
              || echo "有項目未通過，不可執行 format。"
  exit $rc
  ;;
format)
  [ "${CONFIRM:-}" = format ] || { echo "需要 CONFIRM=format"; exit 1; }
  echo "格式化前重新比對："; verify_all || { echo "比對未通過，中止。"; exit 1; }
  link=/dev/disk/by-uuid/$NTFS_UUID
  [ -e "$link" ] || { echo "找不到 NTFS UUID $NTFS_UUID"; exit 1; }
  P=$(readlink -f "$link")
  sz=$(blockdev --getsize64 "$P")
  [ "$sz" -gt "$MIN_BYTES" ] || { echo "$P 只有 $(numfmt --to=iec $sz)，不是那顆 10.9T，中止"; exit 1; }
  umount /srv/nas 2>/dev/null
  findmnt -rn "$P" >/dev/null && { echo "$P 仍掛載中，中止"; exit 1; }
  echo "即將格式化 $P ($(numfmt --to=iec $sz)) 為 ext4 label=$LABEL"
  echo "Recovered 1.7T 與 marketdata 238G 會一併消失。10 秒後開始。"
  sleep 10
  mkfs.ext4 -m 0 -L "$LABEL" "$P" || exit 1
  systemctl reset-failed srv-nas.mount srv-nas.automount 2>/dev/null
  mount "/dev/disk/by-label/$LABEL" /srv/nas && chown anson:anson /srv/nas
  df -h /srv/nas; blkid "$P"
  echo "接著跑 restore，並把 fstab 的 /srv/nas 那行換成新的 UUID"
  ;;
restore)
  findmnt -rn /srv/nas >/dev/null || mount "/dev/disk/by-label/$LABEL" /srv/nas
  install -d -o anson -g anson /srv/nas/Anson
  for spec in "$STAGE/go3" "$NVME/iphone-backup" "$NVME/iphone17"; do
    [ -d "$spec" ] || { echo "跳過不存在的 $spec"; continue; }
    echo "===== $(basename "$spec") $(date '+%T') ====="
    sudo -u anson rsync -a "$spec" /srv/nas/Anson/
    echo "rsync 結束碼 $?"
    printf "  來源 %d 檔 %s bytes\n" "$(find "$spec" -type f|wc -l)" "$(du -sb "$spec"|cut -f1)"
    printf "  目的 %d 檔 %s bytes\n" "$(find "/srv/nas/Anson/$(basename "$spec")" -type f|wc -l)" \
           "$(du -sb "/srv/nas/Anson/$(basename "$spec")"|cut -f1)"
  done
  df -h /srv/nas
  echo "三項都對上之後，才可以刪掉 $NVME 與 $STAGE 上的暫存副本"
  ;;
*) echo "步驟要是 verify / format / restore"; exit 1 ;;
esac
