#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb"]
# ///
"""把 um/trades 的逐筆成交聚合成 1 分鐘 bar，含主動買賣量與 delta。

原始資料 74 幣 / 584 GB / 約 500 億筆，逐幣處理並各自落檔，避免一次撐爆記憶體，
也讓中斷之後可以續跑（已存在的輸出直接略過）。

攻擊方向：is_buyer_maker=True 表示掛買被主動賣單吃 → 記為主動賣。

  uv run research/build_bars.py [--symbol BTCUSDT]
"""
import argparse, time
from pathlib import Path
import duckdb

SRC = Path("/srv/nas/marketdata/parquet/um/trades")
OUT = Path("/srv/nas/marketdata/parquet/research/bars_1m")

SQL = """
COPY (
  SELECT
    '{sym}' AS symbol,
    CAST(time / 60000 AS BIGINT) * 60000        AS bar_ms,
    first(price ORDER BY id)                    AS open,
    max(price)                                  AS high,
    min(price)                                  AS low,
    last(price ORDER BY id)                     AS close,
    sum(qty)                                    AS vol,
    sum(CASE WHEN is_buyer_maker THEN 0 ELSE qty END) AS buy_vol,
    sum(CASE WHEN is_buyer_maker THEN qty ELSE 0 END) AS sell_vol,
    count(*)                                    AS n_trades
  FROM read_parquet('{src}/*.parquet')
  GROUP BY 2 ORDER BY 2
) TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol")
    ap.add_argument("--mem", default="6GB")
    a = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    dirs = ([SRC / f"symbol={a.symbol}"] if a.symbol
            else sorted(SRC.glob("symbol=*")))
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{a.mem}'")
    con.execute("SET threads=3")
    con.execute(f"SET temp_directory='{OUT.parent}/duckdb_tmp'")
    for d in dirs:
        sym = d.name.split("=", 1)[1]
        dst = OUT / f"{sym}.parquet"
        if dst.exists():
            print(f"{sym}: 已存在，略過", flush=True)
            continue
        t0 = time.time()
        tmp = dst.with_suffix(".parquet.tmp")
        try:
            con.execute(SQL.format(sym=sym, src=d, dst=tmp))
            tmp.replace(dst)
            n = con.execute(f"select count(*) from read_parquet('{dst}')").fetchone()[0]
            print(f"{sym}: {n:,} bar  {time.time()-t0:.0f}s", flush=True)
        except Exception as e:
            tmp.unlink(missing_ok=True)
            print(f"{sym}: 失敗 {type(e).__name__}: {str(e)[:90]}", flush=True)


if __name__ == "__main__":
    main()
