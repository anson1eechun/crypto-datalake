#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb", "numpy", "pyarrow"]
# ///
"""執行 PREREGISTRATION.md 裡預先註記的 15 個檢定，不多不少。

5 個假說 × 3 個前瞻窗，Benjamini-Hochberg 控制 FDR 於 0.10。
通過標準四項缺一不可：BH 顯著、方向相符、平均報酬 > 2×中位價差、至少 3 天同號。

符號約定沿用 falsify.py：「往牆的那一側穿過去」為正。
H1/H5 預期為負（牆守住、價格被擋回），H2/H3 預期為正（牆失守、價格續行）。

H4 的實作說明：預先註記寫「H1–H3 各自再加 location 條件」但總數鎖定 15 格，
所以 H4 是**一個**檢定——把 H1–H3 的事件在 location 條件下合併，並把 H1 的符號
翻轉使三者預期方向一致，再與基準比較。這個操作化方式寫在這裡，不是事後選的。

  uv run research/context_tests.py
"""
import glob
from pathlib import Path
import duckdb, numpy as np, pyarrow as pa, pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "state/research.duckdb"   # by_symbol 分區，逐幣查詢快兩個數量級
HORIZONS = (5, 30, 120)
BOOT = 4000
BLOCK_MS = 300_000
FDR = 0.10
NEAR_EXTREME_BPS = 5.0
RNG = np.random.default_rng(20260917)


def load_events():
    tabs = [t for t in (pq.read_table(f) for f in
            sorted(glob.glob(str(ROOT / "research/events/*.parquet")))) if t.num_rows]
    if not tabs:
        raise SystemExit("沒有事件檔，先跑 research/flow.py")
    return pa.concat_tables(tabs).to_pydict()


def load_mid(con, sym):
    """單幣的中價序列。rec_book_ticker 是十億列級別，全部留在記憶體會爆，
    所以逐幣載入、用完即丟。"""
    a = con.execute("""SELECT trans_ms, bid, ask FROM rec_book_ticker
                       WHERE symbol=? AND bid>0 AND ask>0 ORDER BY trans_ms""",
                    [sym]).fetchnumpy()
    t = a["trans_ms"].astype(np.int64)
    if len(t) < 100:
        return None
    mid = (a["bid"] + a["ask"]) / 2.0
    return t, mid, (a["ask"] - a["bid"]) / mid * 1e4


def fwd(t_arr, mid_arr, t0, secs):
    i = np.searchsorted(t_arr, t0, side="right") - 1
    j = np.searchsorted(t_arr, t0 + secs * 1000, side="right") - 1
    out = np.full(len(t0), np.nan)
    ok = (i >= 0) & (j > i) & (j < len(t_arr))
    out[ok] = (mid_arr[j[ok]] / mid_arr[i[ok]] - 1.0) * 1e4
    return out


def blocks_of(x, blk):
    """把區塊壓成 (每塊總和, 每塊筆數)。

    重抽只需要這兩個量：重抽後的整體平均 = Σ被抽中塊的和 / Σ被抽中塊的筆數，
    與「串接後取平均」完全等價，但成本是 O(區塊數) 而不是 O(樣本數)。
    3024 個區塊、347 萬樣本、4000 次重抽，兩者差四個數量級。
    """
    ok = ~np.isnan(x)
    x, blk = x[ok], blk[ok]
    if len(x) == 0:
        return None
    keys, inv = np.unique(blk, return_inverse=True)
    sums = np.bincount(inv, weights=x, minlength=len(keys))
    cnts = np.bincount(inv, minlength=len(keys)).astype(np.float64)
    return sums, cnts


def boot_means(g):
    if g is None or len(g[0]) == 0:
        return np.full(BOOT, np.nan)
    sums, cnts = g
    nb = len(sums)
    pick = RNG.integers(0, nb, (BOOT, nb))
    return sums[pick].sum(axis=1) / cnts[pick].sum(axis=1)


def test_one(ev_x, ev_blk, bs_base):
    """回傳 (n, 平均, 95%CI, 相對基準的雙尾 p)。"""
    g = blocks_of(ev_x, ev_blk)
    n = int(g[1].sum()) if g is not None else 0
    if n < 30:
        return n, np.nan, (np.nan, np.nan), np.nan
    bs = np.sort(boot_means(g))
    m = float(g[0].sum() / g[1].sum())
    diff = bs - bs_base                      # 兩個重抽分佈的差
    p = 2 * min((diff <= 0).mean(), (diff >= 0).mean())
    return n, m, (bs[int(.025 * BOOT)], bs[int(.975 * BOOT)]), min(p, 1.0)


def day_signs(x, day, expect):
    """有多少個獨立交易日的平均與預期同號。"""
    ok = ~np.isnan(x)
    x, day = x[ok], day[ok]
    n = 0
    for d in np.unique(day):
        m = x[day == d]
        if len(m) >= 10 and np.sign(m.mean()) == expect:
            n += 1
    return n


def main():
    ev = load_events()
    sym = np.array(ev["symbol"]); side = np.array(ev["side"])
    t_end = np.array(ev["end_ms"], dtype=np.int64)
    beh = np.array(ev["behavior"]); end = np.array(ev["end_reason"])
    traded = np.array(ev["traded"])
    d_hi = np.array(ev["dist_hi_bps"]); d_lo = np.array(ev["dist_lo_bps"])
    trend = np.array(ev["trend_bps"])
    sign = np.where(side == "bid", -1.0, 1.0)      # 往牆那側穿過去為正
    blk = t_end // BLOCK_MS
    day = t_end // 86_400_000

    con = duckdb.connect(str(DB), read_only=True)
    syms = sorted(set(ev["symbol"]))
    print(f"事件 {len(t_end):,} 筆 / {len(syms)} 幣；涵蓋 {len(np.unique(day))} 個日曆日")

    rets, base_x, base_blk, spreads = {}, {}, {}, []
    for h in HORIZONS:
        rets[h] = np.full(len(t_end), np.nan); base_x[h] = []; base_blk[h] = []
    nmid = 0
    for s in syms:
        got = load_mid(con, s)
        if got is None:
            continue
        nmid += 1
        t_arr, mid_arr, sp = got
        spreads.append(np.median(sp))
        m = sym == s
        lo, hi = t_arr[0], t_arr[-1] - max(HORIZONS) * 1000
        if hi <= lo or not m.any():
            continue
        rt = RNG.integers(lo, hi, m.sum())
        rs = RNG.permutation(sign[m])
        for h in HORIZONS:
            rets[h][m] = fwd(t_arr, mid_arr, t_end[m], h) * sign[m]
            base_x[h].append(fwd(t_arr, mid_arr, rt, h) * rs)
            base_blk[h].append(rt // BLOCK_MS)
        del t_arr, mid_arr, sp, got
    print(f"  {nmid} 幣取得中價序列")
    med_sp = float(np.median(spreads))
    cost = 2 * med_sp
    print(f"中位價差 {med_sp:.2f} bps → 成本門檻 {cost:.2f} bps\n")

    # ── 預先註記的五個事件集合 ────────────────────────────────────
    h1 = np.isin(beh, ("iceberg", "reloading")) & (end != "consumed")
    h2 = end == "consumed"
    h3 = (end == "pulled") & (traded > 0)
    near = (np.abs(d_hi) < NEAR_EXTREME_BPS) | (d_lo < NEAR_EXTREME_BPS)
    # 牆與趨勢同向：買方牆（支撐）配上漲、賣方牆（壓力）配下跌
    with_trend = ((side == "bid") & (trend > 0)) | ((side == "ask") & (trend < 0))
    SPECS = [
        ("H1 Absorption",  h1,               -1, None),
        ("H2 Consumption", h2,               +1, None),
        ("H3 Vacuum",      h3,               +1, None),
        ("H4 Location",    (h1 | h2 | h3) & near, +1, h1),   # h1 翻正號後合併
        ("H5 順勢牆",       h1 & with_trend,  -1, None),
    ]

    results = []
    for h in HORIZONS:
        b = np.concatenate(base_x[h]); bb = np.concatenate(base_blk[h])
        bs_base = np.sort(boot_means(blocks_of(b, bb)))
        for name, mask, expect, flip in SPECS:
            x = rets[h].copy()
            if flip is not None:
                x[flip] *= -1          # H1 預期為負，翻正號才能與 H2/H3 合併
            n, mu, ci, p = test_one(x[mask], blk[mask], bs_base)
            nd = day_signs(x[mask], day[mask], expect) if n >= 30 else 0
            results.append(dict(h=h, name=name, n=n, mu=mu, ci=ci, p=p,
                                expect=expect, days=nd,
                                base=float(np.nanmean(b))))

    # ── Benjamini-Hochberg，FDR 0.10 ───────────────────────────────
    valid = [r for r in results if r["p"] == r["p"]]
    valid.sort(key=lambda r: r["p"])
    m = len(valid)
    cut = 0.0
    for i, r in enumerate(valid, 1):
        if r["p"] <= i / m * FDR:
            cut = r["p"]
    for r in results:
        r["bh"] = (r["p"] == r["p"]) and r["p"] <= cut

    print(f"{'假說':16s}{'窗':>5s}{'n':>7s}{'平均bps':>9s}{'95%CI':>19s}"
          f"{'p':>8s}  BH  方向  過成本  ≥3天")
    for h in HORIZONS:
        for r in [x for x in results if x["h"] == h]:
            if r["n"] < 30:
                print(f"{r['name']:16s}{h:5d}{r['n']:7d}   樣本不足")
                continue
            dir_ok = np.sign(r["mu"]) == r["expect"]
            cost_ok = abs(r["mu"]) > cost
            print(f"{r['name']:16s}{h:5d}{r['n']:7d}{r['mu']:+9.2f}"
                  f" [{r['ci'][0]:+7.2f},{r['ci'][1]:+7.2f}]{r['p']:8.3f}"
                  f"  {'✓' if r['bh'] else '·'}   {'✓' if dir_ok else '✗'}"
                  f"    {'✓' if cost_ok else '✗'}    {r['days']:2d}")
        print()

    passed = [r for r in results if r["n"] >= 30 and r["bh"]
              and np.sign(r["mu"]) == r["expect"] and abs(r["mu"]) > cost
              and r["days"] >= 3]
    print("═" * 72)
    if passed:
        print(f"通過四項標準的格子：{len(passed)}")
        for r in passed:
            print(f"  {r['name']} @{r['h']}s  {r['mu']:+.2f} bps  p={r['p']:.3f}  "
                  f"{r['days']} 天同號")
    else:
        print("沒有任何格子同時滿足四項通過標準。")
        near_miss = [r for r in results if r["n"] >= 30 and r["bh"]]
        if near_miss:
            print("（BH 顯著但未過其他標準的：")
            for r in near_miss:
                why = []
                if np.sign(r["mu"]) != r["expect"]: why.append("方向相反")
                if abs(r["mu"]) <= cost: why.append(f"{abs(r['mu']):.2f}<成本{cost:.2f}")
                if r["days"] < 3: why.append(f"只有{r['days']}天同號")
                print(f"    {r['name']} @{r['h']}s {r['mu']:+.2f} bps — {'、'.join(why)}")
            print("  ）")


if __name__ == "__main__":
    main()
