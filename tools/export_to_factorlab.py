#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb", "pandas", "pyarrow"]
# ///
"""把資料湖的衍生品指標匯出成 factor-lab 既有的 pkl 格式。

口徑刻意與 factor-lab/scripts/fetch_deriv_metrics.py 完全一致（每日末筆快照對齊收盤），
已驗證在 2022-01→2026-06 的 1619 個重疊日上與其原始 deriv_btc.pkl 逐欄相同
（僅 oi_val 有 float64 最後一位差異）。差別只在：期間更長（2020-09 起）、幣種更多（73 幣）。

  uv run tools/export_to_factorlab.py
"""
from pathlib import Path
import duckdb, pandas as pd

DB = Path(__file__).resolve().parent.parent / "state/market.duckdb"
OUT = Path("/home/anson/factor-lab/empirical_runs")
COLS = ["oi", "oi_val", "global_acct_ls", "top_acct_ls", "top_pos_ls", "taker_ls"]

Q = """
WITH r AS (
  SELECT symbol, create_time,
         sum_open_interest oi, sum_open_interest_value oi_val,
         count_long_short_ratio global_acct_ls,
         count_toptrader_long_short_ratio top_acct_ls,
         sum_toptrader_long_short_ratio top_pos_ls,
         sum_taker_long_short_vol_ratio taker_ls,
         row_number() OVER (PARTITION BY symbol, CAST(create_time AS DATE)
                            ORDER BY create_time DESC) rn
  FROM um_metrics {where})
SELECT symbol, CAST(create_time AS DATE) date, oi, oi_val, global_acct_ls,
       top_acct_ls, top_pos_ls, taker_ls
FROM r WHERE rn = 1 ORDER BY symbol, date"""


def _plain(df):
    """轉成純 numpy dtype：duckdb 回傳的 arrow-backed 欄位會讓 pkl 依賴 pyarrow，
    與 factor-lab 既有 pkl 的讀取方式不相容。"""
    out = pd.DataFrame(index=range(len(df)))
    out["date"] = pd.to_datetime(df["date"].to_numpy())
    for c in COLS:
        out[c] = pd.to_numeric(df[c], errors="coerce").to_numpy(dtype="float64")
    out["timestamp"] = out["date"].values.astype("datetime64[ms]").astype("int64")
    # pandas 3 在有 pyarrow 時連欄位名 Index 都用 arrow string，會讓 pkl 依賴 pyarrow；
    # factor-lab 的執行環境沒裝 pyarrow，故強制退回 object dtype。
    out.columns = pd.Index(list(out.columns), dtype=object)
    return out


def main():
    con = duckdb.connect(str(DB), read_only=True)

    df = _plain(con.sql(Q.format(where="WHERE symbol='BTCUSDT'")).df())
    p = OUT / "deriv_btc_ext.pkl"
    df.to_pickle(p)
    print(f"{p.name}: {len(df)} 日 {df.date.min().date()} → {df.date.max().date()}")

    allp = con.sql(Q.format(where="")).df()
    panel = {}
    for sym, g in allp.groupby("symbol"):
        panel[sym.replace("USDT", "")] = _plain(g.sort_values("date").reset_index(drop=True))
    p = OUT / "deriv_universe_1d.pkl"
    pd.to_pickle(panel, p)
    span = [(v.date.min(), v.date.max()) for v in panel.values()]
    print(f"{p.name}: {len(panel)} 幣  最早 {min(s[0] for s in span).date()} "
          f"→ 最晚 {max(s[1] for s in span).date()}  "
          f"中位日數 {int(pd.Series([len(v) for v in panel.values()]).median())}")


if __name__ == "__main__":
    main()
