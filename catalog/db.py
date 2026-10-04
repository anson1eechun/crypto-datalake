#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb"]
# ///
"""建立 DuckDB 目錄：對 NAS 上的 parquet 開視圖，讓兩層資料用同一套 SQL 查。

視圖是 glob，不是快照 —— 新資料轉檔後自動出現，不必重建。
DuckDB 檔放本機 NVMe（NTFS 上的 DB 檔鎖不可靠）。

  uv run catalog/db.py build          # 建/更新視圖
  uv run catalog/db.py status         # 各表資料範圍
  uv run catalog/db.py sql "SELECT ..."
"""
import argparse, os, sys
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent.parent
BASE = Path(os.environ.get("MD_ROOT", "/srv/nas/marketdata"))
DBF = ROOT / "state/market.duckdb"

# 視圖名 -> (parquet glob, 是否 hive 分區)
VIEWS = {
    "um_metrics":      (BASE / "parquet/um/metrics/*/*.parquet", True),
    "um_klines_1m":    (BASE / "parquet/um/klines1m/*/*.parquet", True),
    "um_mark_1m":      (BASE / "parquet/um/markPrice1m/*/*.parquet", True),
    "um_trades":       (BASE / "parquet/um/trades/*/*.parquet", True),
    "um_agg_trades":   (BASE / "parquet/um/aggTrades/*/*.parquet", True),
    "um_book_ticker":  (BASE / "parquet/um/bookTicker/*/*.parquet", True),
    "um_book_depth":   (BASE / "parquet/um/bookDepth/*/*.parquet", True),
    "um_funding":      (BASE / "parquet/um/fundingRate/*/*.parquet", True),
    "spot_klines_1s":  (BASE / "parquet/spot/klines1s/*/*.parquet", True),
    "spot_agg_trades": (BASE / "parquet/spot/aggTrades/*/*.parquet", True),
    "rec_trade":       (BASE / "parquet/recorded/trade/*/*.parquet", True),
    "rec_book_ticker": (BASE / "parquet/recorded/bookTicker/*/*.parquet", True),
    "rec_depth":       (BASE / "parquet/recorded/depth/*/*.parquet", True),
    "rec_depth_snap":  (BASE / "parquet/recorded/depthSnapshot/*/*.parquet", True),
    "rec_open_interest": (BASE / "parquet/recorded/openInterest/*/*.parquet", True),
    "rec_premium_index": (BASE / "parquet/recorded/premiumIndex/*/*.parquet", True),
}
# 每個視圖用哪一欄判斷時間範圍
TCOL = {
    "um_metrics": "create_time", "um_klines_1m": "open_time", "um_mark_1m": "open_time",
    "um_trades": "time", "um_agg_trades": "transact_time",
    "um_book_ticker": "transaction_time", "um_book_depth": "timestamp",
    "um_funding": "calc_time", "spot_klines_1s": "open_time",
    "spot_agg_trades": "transact_time",
    "rec_trade": "trade_ms", "rec_book_ticker": "trans_ms", "rec_depth": "trans_ms",
    "rec_depth_snap": "recv_ns", "rec_open_interest": "time_ms",
    "rec_premium_index": "time_ms",
}


def build(con):
    made, skipped = [], []
    for name, (glob, hive) in VIEWS.items():
        if not list(Path(glob).parent.parent.glob("*/*.parquet")):
            skipped.append(name)
            continue
        con.execute(f"""CREATE OR REPLACE VIEW {name} AS
            SELECT * FROM read_parquet('{glob}', hive_partitioning={1 if hive else 0},
                                       union_by_name=true)""")
        made.append(name)
    print(f"已建立 {len(made)} 個視圖: {', '.join(made)}")
    if skipped:
        print(f"尚無資料而略過: {', '.join(skipped)}")


def _fmt(v):
    """時間欄可能是 datetime、毫秒或奈秒整數，統一顯示成可讀時間。"""
    from datetime import datetime, timezone
    if isinstance(v, int):
        div = 1e9 if v > 1e17 else 1e3
        v = datetime.fromtimestamp(v / div, timezone.utc).replace(tzinfo=None)
    return str(v)[:19]


def status(con):
    print(f"{'視圖':20s} {'列數':>14s}  {'symbols':>7s}  時間範圍")
    for name in VIEWS:
        try:
            n, s = con.execute(
                f"SELECT count(*), count(DISTINCT symbol) FROM {name}").fetchone()
        except Exception:
            continue
        tc = TCOL.get(name)
        rng = ""
        if tc and n:
            try:
                a, z = con.execute(f"SELECT min({tc}), max({tc}) FROM {name}").fetchone()
                rng = f"{_fmt(a)} → {_fmt(z)}"
            except Exception as e:
                rng = f"(範圍不明 {str(e)[:40]})"
        print(f"{name:20s} {n:>14,}  {s:>7}  {rng}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["build", "status", "sql"])
    ap.add_argument("query", nargs="?")
    a = ap.parse_args()
    DBF.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DBF))
    if a.cmd == "build":
        build(con)
    elif a.cmd == "status":
        build(con)
        status(con)
    else:
        if not a.query:
            sys.exit("需要 SQL")
        con.sql(a.query).show(max_rows=50)
