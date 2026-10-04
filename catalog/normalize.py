#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow"]
# ///
"""把 vision 的 CSV zip 轉成 hive 分區 parquet，供 DuckDB 直接查詢。

處理三個 Binance 資料的實務陷阱：
  1. 舊檔沒有標頭列、新檔有 —— 逐檔偵測。
  2. 2025 年起部分資料集時間戳從毫秒改微秒 —— 一律正規化成毫秒。
  3. 月檔可能有數 GB —— 用 batch reader 串流轉檔，不整檔進記憶體。

  uv run catalog/normalize.py --dataset um/metrics [--jobs 4] [--limit N]
"""
import argparse, os, sqlite3, zipfile
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "state/manifest.sqlite"
BASE = Path(os.environ.get("MD_ROOT", "/srv/nas/marketdata"))
SRC, OUT = BASE / "vision", BASE / "parquet"

# 各資料集的欄位（用於舊版無標頭檔）與需要正規化成毫秒的時間欄
COLS = {
    "um/metrics": (["create_time", "symbol", "sum_open_interest", "sum_open_interest_value",
                    "count_toptrader_long_short_ratio", "sum_toptrader_long_short_ratio",
                    "count_long_short_ratio", "sum_taker_long_short_vol_ratio"], []),
    "um/klines1m": (["open_time", "open", "high", "low", "close", "volume", "close_time",
                     "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume",
                     "ignore"], ["open_time", "close_time"]),
    "um/markPrice1m": (["open_time", "open", "high", "low", "close", "volume", "close_time",
                        "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume",
                        "ignore"], ["open_time", "close_time"]),
    "spot/klines1s": (["open_time", "open", "high", "low", "close", "volume", "close_time",
                       "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume",
                       "ignore"], ["open_time", "close_time"]),
    "um/trades": (["id", "price", "qty", "quote_qty", "time", "is_buyer_maker"], ["time"]),
    "um/aggTrades": (["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id",
                      "transact_time", "is_buyer_maker"], ["transact_time"]),
    "spot/aggTrades": (["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id",
                        "transact_time", "is_buyer_maker"], ["transact_time"]),
    "um/bookTicker": (["update_id", "best_bid_price", "best_bid_qty", "best_ask_price",
                       "best_ask_qty", "transaction_time", "event_time"],
                      ["transaction_time", "event_time"]),
    "um/bookDepth": (["timestamp", "percentage", "depth", "notional"], []),
    "um/fundingRate": (["calc_time", "funding_interval_hours", "last_funding_rate"], ["calc_time"]),
}
f64, i64 = pa.float64(), pa.int64()
_KLINE_T = {"open_time": i64, "open": f64, "high": f64, "low": f64, "close": f64,
            "volume": f64, "close_time": i64, "quote_volume": f64, "count": i64,
            "taker_buy_volume": f64, "taker_buy_quote_volume": f64, "ignore": f64}
TYPES = {
    "um/metrics": {"create_time": pa.timestamp("ms"), "symbol": pa.string(),
                   "sum_open_interest": f64, "sum_open_interest_value": f64,
                   "count_toptrader_long_short_ratio": f64,
                   "sum_toptrader_long_short_ratio": f64,
                   "count_long_short_ratio": f64, "sum_taker_long_short_vol_ratio": f64},
    "um/klines1m": _KLINE_T, "um/markPrice1m": _KLINE_T, "spot/klines1s": _KLINE_T,
    "um/trades": {"id": i64, "price": f64, "qty": f64, "quote_qty": f64, "time": i64,
                  "is_buyer_maker": pa.bool_()},
    "um/aggTrades": {"agg_trade_id": i64, "price": f64, "quantity": f64,
                     "first_trade_id": i64, "last_trade_id": i64, "transact_time": i64,
                     "is_buyer_maker": pa.bool_()},
    "spot/aggTrades": {"agg_trade_id": i64, "price": f64, "quantity": f64,
                       "first_trade_id": i64, "last_trade_id": i64, "transact_time": i64,
                       "is_buyer_maker": pa.bool_()},
    "um/bookTicker": {"update_id": i64, "best_bid_price": f64, "best_bid_qty": f64,
                      "best_ask_price": f64, "best_ask_qty": f64,
                      "transaction_time": i64, "event_time": i64},
    "um/bookDepth": {"timestamp": pa.timestamp("ms"), "percentage": f64,
                     "depth": f64, "notional": f64},
    "um/fundingRate": {"calc_time": i64, "funding_interval_hours": i64,
                       "last_funding_rate": f64},
}

# 微秒門檻：毫秒時間戳 < 1e13，微秒 > 1e15
US_THRESHOLD = 1e14
ROWGROUP_ROWS = 2_000_000      # 夠大的 row group 才壓得動；每來源檔各成一組會毀掉壓縮率


def to_ms(tbl, tcols):
    """把微秒時間戳統一降成毫秒；已是毫秒的原樣保留。"""
    for c in tcols:
        if c not in tbl.column_names:
            continue
        col = tbl.column(c)
        try:
            first = col[0].as_py()
        except (IndexError, AttributeError):
            continue
        if first is None or first < US_THRESHOLD:
            continue
        import pyarrow.compute as pc
        tbl = tbl.set_column(tbl.schema.get_field_index(c), c,
                             pc.divide(pc.cast(col, pa.int64()), pa.scalar(1000, pa.int64())))
    return tbl


def _batches(src, names, tcols, types):
    """串流讀一個 zip 內的 CSV，逐批 yield 已正規化時間戳的 Table。"""
    with zipfile.ZipFile(src) as z:
        inner = z.namelist()[0]
        with z.open(inner) as fh:
            head = fh.readline()
        has_header = not head.split(b",")[0].strip().replace(b"-", b"").isdigit()
        with z.open(inner) as fh:
            ropt = pacsv.ReadOptions(block_size=1 << 24,
                                     column_names=None if has_header else names)
            copt = pacsv.ConvertOptions(column_types=types)
            for batch in pacsv.open_csv(fh, read_options=ropt, convert_options=copt):
                yield to_ms(pa.Table.from_batches([batch]), tcols)


def convert_group(group):
    """一組來源 zip 合併寫成單一 parquet。小日檔按年合併，大月檔維持 1:1。"""
    dataset, symbol, label, keys = group
    dst = OUT / dataset / f"symbol={symbol}" / f"{label}.parquet"
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(".parquet.part")
    names, tcols = COLS[dataset]
    types = TYPES[dataset]
    w = None
    rows = 0
    buf, buf_rows = [], 0

    def flush(w):
        nonlocal buf, buf_rows
        if not buf:
            return w
        tbl = pa.concat_tables(buf, promote_options="permissive")
        if w is None:
            w = pq.ParquetWriter(tmp, tbl.schema, compression="zstd", compression_level=6)
        w.write_table(tbl)
        buf, buf_rows = [], 0
        return w

    try:
        for key in keys:
            src = SRC / key
            if not src.exists():
                continue
            for tbl in _batches(src, names, tcols, types):
                buf.append(tbl)
                buf_rows += tbl.num_rows
                rows += tbl.num_rows
                if buf_rows >= ROWGROUP_ROWS:
                    w = flush(w)
        w = flush(w)
        if w is None:
            return label, "empty", 0, 0
        w.close()
        os.replace(tmp, dst)
        return label, "done", dst.stat().st_size, rows
    except Exception as e:
        if w is not None:
            try:
                w.close()
            except Exception:
                pass
        Path(tmp).unlink(missing_ok=True)
        return label, f"err:{type(e).__name__}:{str(e)[:70]}", 0, 0


def _commit(con, tries=10):
    """回填器同時在寫 manifest；鎖競爭時重試而不是整輪作廢。"""
    import time as _t
    for k in range(tries):
        try:
            con.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" not in str(e) or k == tries - 1:
                raise
            _t.sleep(2 * (k + 1))


GROUPS_DDL = """CREATE TABLE IF NOT EXISTS pq_groups(
  dataset TEXT, symbol TEXT, label TEXT, n_src INTEGER, bytes INTEGER, rows INTEGER,
  ts INTEGER, PRIMARY KEY(dataset,symbol,label))"""


def build_groups(con, dataset):
    """日檔→按 symbol-年合併；月檔→維持 1:1。已建且來源數未變的跳過。"""
    rows = con.execute(
        "SELECT key,symbol,period,freq FROM files WHERE status='done' AND dataset=?"
        " ORDER BY symbol,period", (dataset,)).fetchall()
    g = {}
    for key, symbol, period, freq in rows:
        label = period[:4] if freq == "daily" else period
        g.setdefault((symbol, label), []).append(key)
    con.execute(GROUPS_DDL)
    have = {(s, l): n for s, l, n in con.execute(
        "SELECT symbol,label,n_src FROM pq_groups WHERE dataset=?", (dataset,))}
    todo = [(dataset, s, l, ks) for (s, l), ks in sorted(g.items())
            if have.get((s, l)) != len(ks)]
    return todo, len(g)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--limit", type=int)
    a = ap.parse_args()
    con = sqlite3.connect(DB, timeout=300)
    con.execute("PRAGMA busy_timeout=300000")
    todo, ntot = build_groups(con, a.dataset)
    if a.limit:
        todo = todo[:a.limit]
    src_b = sum((SRC / k).stat().st_size for _, _, _, ks in todo for k in ks
                if (SRC / k).exists())
    print(f"{a.dataset}: {ntot} 組，需重建 {len(todo)} 組，來源 {src_b/1e9:.2f} GB", flush=True)
    out_b = nerr = nrow = 0
    with ProcessPoolExecutor(a.jobs) as ex:
        for i, (label, st, sz, rows) in enumerate(
                ex.map(convert_group, todo, chunksize=1), 1):
            d, s, l, ks = todo[i - 1]
            if st.startswith("err"):
                nerr += 1
                if nerr <= 5:
                    print(f"  ! {d} {s} {l} {st}", flush=True)
            else:
                out_b += sz
                nrow += rows
                con.execute("INSERT OR REPLACE INTO pq_groups VALUES(?,?,?,?,?,?,strftime('%s','now'))",
                            (d, s, l, len(ks), sz, rows))
            # 每組就提交。大資料集一組是幾分鐘的巨型月檔，每 50 組才提交等於
            # 整整一小時持有 manifest 的寫入交易，會把同時在跑的回填鎖死。
            _commit(con)
            if i % 50 == 0 or i == len(todo):
                print(f"  {i}/{len(todo)}  parquet {out_b/1e9:.2f} GB"
                      f"  (原 zip 的 {out_b/src_b if src_b else 0:.2f} 倍)"
                      f"  {nrow/1e6:.1f}M 列  誤 {nerr}", flush=True)
    _commit(con)


if __name__ == "__main__":
    main()
