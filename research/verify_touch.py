#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb", "numpy", "pyarrow"]
# ///
"""檢查偵測到的牆事件裡，價格到底有沒有走到牆的位置。

這不是新的假說檢定，是對 flow.py 的正確性檢查。93% 的事件被標成 pulled
（撤單），如果那是成交歸戶寫錯造成的，整個分類與證偽結果都不成立。

判定方式用 bookTicker 的最佳買賣價，跟 depth 是獨立的資料流：
  買方牆在中價下方，價格走到它 = 最佳買價曾下探到牆價
  賣方牆在中價上方，價格走到它 = 最佳賣價曾上探到牆價

  uv run research/verify_touch.py
"""
import glob
from pathlib import Path
import duckdb, numpy as np, pyarrow as pa, pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "state/market.duckdb"


def main():
    tabs = [t for t in (pq.read_table(f) for f in
            sorted(glob.glob(str(ROOT / "research/events/*.parquet")))) if t.num_rows]
    if not tabs:
        raise SystemExit("沒有事件檔")
    ev = pa.concat_tables(tabs).to_pydict()
    con = duckdb.connect(str(DB), read_only=True)

    sym = np.array(ev["symbol"]); side = np.array(ev["side"])
    px = np.array(ev["price"]); t0 = np.array(ev["start_ms"], dtype=np.int64)
    t1 = np.array(ev["end_ms"], dtype=np.int64)
    traded = np.array(ev["traded"]); end = np.array(ev["end_reason"])
    touched = np.zeros(len(px), dtype=bool)
    covered = np.zeros(len(px), dtype=bool)

    for s in sorted(set(ev["symbol"])):
        a = con.execute("""SELECT trans_ms, bid, ask FROM rec_book_ticker
                           WHERE symbol=? AND bid>0 AND ask>0 ORDER BY trans_ms""",
                        [s]).fetchnumpy()
        t = a["trans_ms"].astype(np.int64)
        if len(t) < 10:
            continue
        m = np.flatnonzero(sym == s)
        i0 = np.searchsorted(t, t0[m], side="left")
        i1 = np.searchsorted(t, t1[m], side="right")
        for k, lo, hi in zip(m, i0, i1):
            if hi <= lo:
                continue
            covered[k] = True
            touched[k] = (a["bid"][lo:hi].min() <= px[k]) if side[k] == "bid" \
                    else (a["ask"][lo:hi].max() >= px[k])

    c = covered
    print(f"事件 {len(px)}，其中 {c.sum()} 筆有 bookTicker 覆蓋可判定\n")
    print(f"  價格曾走到牆位   {(touched & c).sum():6d}  ({(touched & c).sum()/c.sum()*100:5.1f}%)")
    print(f"  價格從未走到     {(~touched & c).sum():6d}  ({(~touched & c).sum()/c.sum()*100:5.1f}%)\n")

    print("交叉檢查（成交量歸戶是否與觸碰一致）：")
    for lab, mask in (("曾觸碰", touched & c), ("未觸碰", ~touched & c)):
        n = mask.sum()
        if not n:
            continue
        has = (traded[mask] > 0).sum()
        print(f"  {lab}  n={n:6d}   有成交歸戶 {has:6d} ({has/n*100:5.1f}%)")
    print()
    print("未觸碰的牆理應幾乎都是 pulled（沒人碰就撤走）：")
    for lab, mask in (("曾觸碰", touched & c), ("未觸碰", ~touched & c)):
        n = mask.sum()
        if not n:
            continue
        row = "   ".join(f"{r}={ (end[mask]==r).sum()/n*100:4.1f}%"
                         for r in ("pulled", "consumed", "partial", "shrank"))
        print(f"  {lab}  {row}")


if __name__ == "__main__":
    main()
