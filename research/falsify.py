#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb", "numpy", "pyarrow"]
# ///
"""對 flow.py 偵測到的牆事件做前瞻報酬檢定，並跟隨機基準比較。

這是證偽步驟，不是回測。它只回答一個問題：牆事件發生後的價格走勢，
跟同一段時間隨機挑時點的走勢，有沒有可區分的差異？沒有的話後面就不必做了。

符號約定：signed return 定義成「往牆的那一側穿過去」為正。
  買方牆（支撐，在價格下方）被穿過 = 價格下跌，故取負號；賣方牆取正號。
影片的主張若成立：consumed / pulled 應為正（牆沒了，價格穿過去），
                  iceberg / reloading 應為負（牆吸收，價格被擋回）。

  uv run research/falsify.py
"""
import argparse, glob
from pathlib import Path
import duckdb, numpy as np, pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "state/market.duckdb"
HORIZONS = (5, 30, 120)      # 秒
BOOT = 2000                  # bootstrap 次數
BLOCK_MS = 300_000           # block bootstrap 的區塊長度（5 分鐘）
RNG = np.random.default_rng(0)


def load_mids(con, symbols):
    """每個 symbol 一組 (時間陣列, 中價陣列, 價差bps陣列)，供 asof 查詢。"""
    mids = {}
    for s in symbols:
        arr = con.execute("""
            SELECT trans_ms, bid, ask FROM rec_book_ticker
            WHERE symbol=? AND bid>0 AND ask>0 ORDER BY trans_ms""", [s]).fetchnumpy()
        t = arr["trans_ms"].astype(np.int64)
        if len(t) < 100:
            continue
        mid = (arr["bid"] + arr["ask"]) / 2.0
        spread = (arr["ask"] - arr["bid"]) / mid * 1e4
        mids[s] = (t, mid, spread)
    return mids


def fwd(t_arr, mid_arr, t0, secs):
    """t0 當下與 t0+secs 的中價，回傳 bps 報酬；資料不足回 nan。"""
    i = np.searchsorted(t_arr, t0, side="right") - 1
    j = np.searchsorted(t_arr, t0 + secs * 1000, side="right") - 1
    ok = (i >= 0) & (j > i) & (j < len(t_arr))
    out = np.full(len(t0), np.nan)
    ii, jj = i[ok], j[ok]
    out[ok] = (mid_arr[jj] / mid_arr[ii] - 1.0) * 1e4
    return out


def stats(x, blk):
    """block bootstrap 的平均值信賴區間。

    事件在時間上高度群聚，前瞻窗又互相重疊，逐筆重抽會把相依的觀測當成獨立的，
    信賴區間會假性收窄。改成以時間區塊為單位重抽，區塊內的相依結構原樣保留。
    """
    ok = ~np.isnan(x)
    x, blk = x[ok], blk[ok]
    if len(x) < 5:
        return len(x), np.nan, np.nan, (np.nan, np.nan)
    m = x.mean()
    se = x.std(ddof=1) / np.sqrt(len(x))
    keys, inv = np.unique(blk, return_inverse=True)
    order = np.argsort(inv, kind="stable")
    starts = np.searchsorted(inv[order], np.arange(len(keys)))
    ends = np.append(starts[1:], len(order))
    groups = [x[order[a:b]] for a, b in zip(starts, ends)]
    nb = len(groups)
    bs = np.empty(BOOT)
    for i in range(BOOT):
        pick = RNG.integers(0, nb, nb)
        bs[i] = np.concatenate([groups[j] for j in pick]).mean()
    bs.sort()
    return len(x), m, (m / se if se > 0 else np.nan), (bs[int(.025 * BOOT)], bs[int(.975 * BOOT)])


def main(a):
    files = sorted(glob.glob(str(ROOT / "research/events/*.parquet")))
    if not files:
        raise SystemExit("沒有事件檔，先跑 research/flow.py")
    tabs = [pq.read_table(f) for f in files]
    tabs = [t for t in tabs if t.num_rows]
    if not tabs:
        raise SystemExit("事件檔全部是空的")
    import pyarrow as pa
    ev = pa.concat_tables(tabs).to_pydict()
    syms = sorted(set(ev["symbol"]))
    con = duckdb.connect(str(DB), read_only=True)
    mids = load_mids(con, syms)
    print(f"事件 {len(ev['symbol'])} 筆 / {len(syms)} 幣，其中 {len(mids)} 幣有可用中價序列")
    print(f"信賴區間用 block bootstrap，區塊 {BLOCK_MS//1000}s")

    if a.touched_only:
        # 影片的命題是「價格走到流動性那裡之後發生什麼」。從沒被碰過的牆
        # 不在那個命題範圍內，把它們留在樣本裡只是稀釋。
        keep = [i for i, t in enumerate(ev["traded"]) if t > 0]
        ev = {k: [v[i] for i in keep] for k, v in ev.items()}
        print(f"只留有成交碰觸的事件：{len(keep)} 筆")
    sym = np.array(ev["symbol"]); side = np.array(ev["side"])
    t_end = np.array(ev["end_ms"], dtype=np.int64)
    beh = np.array(ev["behavior"]); end = np.array(ev["end_reason"])
    sign = np.where(side == "bid", -1.0, 1.0)   # 往牆那側穿過去為正

    ev_blk = t_end // BLOCK_MS
    rets = {h: np.full(len(t_end), np.nan) for h in HORIZONS}
    base = {h: [] for h in HORIZONS}
    base_blk = {h: [] for h in HORIZONS}
    med_spread = []
    for s in syms:
        if s not in mids:
            continue
        t_arr, mid_arr, sp = mids[s]
        med_spread.append(np.median(sp))
        m = sym == s
        # 隨機基準：同一段時間、同樣筆數、同樣的買賣側比例
        lo, hi = t_arr[0], t_arr[-1] - max(HORIZONS) * 1000
        if hi <= lo:
            continue
        rt = RNG.integers(lo, hi, m.sum())
        rs = RNG.permutation(sign[m])
        for h in HORIZONS:
            rets[h][m] = fwd(t_arr, mid_arr, t_end[m], h) * sign[m]
            base[h].append(fwd(t_arr, mid_arr, rt, h) * rs)
            base_blk[h].append(rt // BLOCK_MS)

    print(f"中位價差 {np.median(med_spread):.2f} bps"
          f"（單邊成本下限；報酬要大於它兩倍才有可能賺）\n")

    cells = sorted(set(zip(beh, end)), key=lambda c: -((beh == c[0]) & (end == c[1])).sum())
    for h in HORIZONS:
        b = np.concatenate(base[h])
        nb, mb, tb, cib = stats(b, np.concatenate(base_blk[h]))
        print(f"── 前瞻 {h}s ──  隨機基準 n={nb} 平均 {mb:+.2f} bps  "
              f"95%CI [{cib[0]:+.2f}, {cib[1]:+.2f}]")
        print(f"   {'behavior':12s} {'end_reason':10s} {'n':>5s} {'平均bps':>8s} "
              f"{'t':>6s} {'95%CI':>18s}  勝過基準?")
        for bh, en in cells:
            m = (beh == bh) & (end == en)
            n, mu, t, ci = stats(rets[h][m], ev_blk[m])
            if n < a.min_n:
                continue
            beats = "是" if (ci[0] > cib[1] or ci[1] < cib[0]) else "否"
            print(f"   {bh:12s} {en:10s} {n:5d} {mu:+8.2f} {t:+6.2f} "
                  f"[{ci[0]:+7.2f},{ci[1]:+7.2f}]  {beats}")
        print()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-n", type=int, default=30)
    ap.add_argument("--touched-only", action="store_true")
    main(ap.parse_args())
