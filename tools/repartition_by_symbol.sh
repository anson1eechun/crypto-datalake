#!/bin/bash
# 把自錄資料重新按 symbol 分區，供研究用。
#
# 原始 parquet 按 date/hour 分區（寫入時的自然順序），研究程式是逐幣讀，
# 於是每查一個幣都要掃過全部小時檔。74 幣 × 11 個日窗 = 全量掃 814 次。
# 按 symbol 分區之後每次只碰該幣的檔案。原始分區保留不動。
set -u
cd /home/anson/crypto-datalake
OUT=/srv/nas/marketdata/parquet/by_symbol
uv run --quiet --with duckdb python - "$OUT" <<'PY'
import duckdb, sys, time, pathlib
out = pathlib.Path(sys.argv[1])
con = duckdb.connect("state/market.duckdb", read_only=True)
con.execute("SET memory_limit='8GB'"); con.execute("SET threads=4")
con.execute(f"SET temp_directory='{out.parent}/duckdb_tmp'")
for view in ("rec_depth", "rec_trade", "rec_depth_snap", "rec_book_ticker"):
    d = out / view
    if d.exists() and any(d.iterdir()):
        print(f"{view}: 已存在，略過", flush=True); continue
    t0 = time.time()
    con.execute(f"""COPY (SELECT * FROM {view}) TO '{d}'
        (FORMAT PARQUET, PARTITION_BY (symbol), COMPRESSION ZSTD,
         OVERWRITE_OR_IGNORE, FILENAME_PATTERN 'part_{{i}}')""")
    print(f"{view}: {time.time()-t0:.0f}s", flush=True)
PY
echo "======== $(date '+%F %T')  分區完成 ========"
du -sh /srv/nas/marketdata/parquet/by_symbol/*/ 2>/dev/null
