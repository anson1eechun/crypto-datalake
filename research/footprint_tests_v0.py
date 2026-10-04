#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb", "numpy", "pyarrow"]
# ///
"""執行 PREREGISTRATION_FOOTPRINT.md 的 12 個檢定，不多不少。

4 個假說 × 3 個前瞻窗（1/5/30 分鐘），Benjamini-Hochberg 控制 FDR 0.10，
block bootstrap 以「日」為區塊，IS = 2019-09~2024-12、OOS = 2025-01 之後。

符號一律定義成「假說預測的方向為正」，所以每個格子的平均值都該是正的才算支持。

  uv run research/footprint_tests.py
"""
import argparse, glob
from pathlib import Path
import numpy as np, pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
BARS = Path("/srv/nas/marketdata/parquet/research/bars_1m")
HORIZONS = (1, 5, 30)          # 分鐘
BOOT = 4000
FDR = 0.10
OOS_MS = 1735689600000         # 2025-01-01 UTC
LOOKBACK = 20                  # F1 的創新高窗（bar）
EXTREME_Q = 0.95               # F2 的 |delta| 分位
QUIET_Q = 0.50                 # F2 的 |ret| 分位
RNG = np.random.default_rng(20260917)


def load_symbol(path):
    t = pq.read_table(path).to_pydict()
    ms = np.array(t["bar_ms"], dtype=np.int64)
    o = np.array(t["open"]); c = np.array(t["close"])
    bv = np.array(t["buy_vol"]); sv = np.array(t["sell_vol"])
    # 只留連續的 1 分鐘序列不必要——缺口用 forward return 的時間對齊處理即可
    delta = bv - sv
    ret = np.full(len(c), np.nan)
    ret[1:] = (c[1:] / c[:-1] - 1.0) * 1e4
    day = ms // 86_400_000
    # CVD 每日重設，避免無界漂移主導一切
    cvd = np.zeros(len(delta))
    start = 0
    for i in range(1, len(day) + 1):
        if i == len(day) or day[i] != day[start]:
            cvd[start:i] = np.cumsum(delta[start:i])
            start = i
    return ms, c, delta, cvd, ret, day


def fwd_ret(ms, close, idx, minutes):
    """idx 這些 bar 往後 minutes 分鐘的報酬 bps；跨越資料缺口的回 nan。"""
    tgt = ms[idx] + minutes * 60_000
    j = np.searchsorted(ms, tgt)
    ok = (j < len(ms)) & (ms[np.minimum(j, len(ms) - 1)] == tgt)
    out = np.full(len(idx), np.nan)
    out[ok] = (close[j[ok]] / close[idx[ok]] - 1.0) * 1e4
    return out


def rolling_max(a, w):
    out = np.full(len(a), np.nan)
    for i in range(w, len(a)):
        out[i] = a[i - w:i].max()
    return out


def rolling_min(a, w):
    out = np.full(len(a), np.nan)
    for i in range(w, len(a)):
        out[i] = a[i - w:i].min()
    return out


def masks_for(close, delta, cvd, ret):
    """回傳 {假說名: (事件索引, 方向)}；方向 +1 表示「預測價格上漲」。"""
    n = len(close)
    hi = rolling_max(close, LOOKBACK); lo = rolling_min(close, LOOKBACK)
    chi = rolling_max(cvd, LOOKBACK);  clo = rolling_min(cvd, LOOKBACK)
    out = {}

    # F1 背離：價格創新高但 CVD 沒有 → 預測下跌（方向 -1），反之亦然
    bear = (close > hi) & (cvd <= chi)
    bull = (close < lo) & (cvd >= clo)
    idx = np.flatnonzero(bear | bull)
    out["F1 背離"] = (idx, np.where(bear[idx], -1.0, 1.0))

    # F2 吸收：|delta| 極端但 |ret| 平靜 → 預測往 delta 反方向
    ad = np.abs(delta); ar = np.abs(np.nan_to_num(ret))
    big = ad >= np.nanquantile(ad, EXTREME_Q)
    quiet = ar <= np.nanquantile(ar, QUIET_Q)
    idx = np.flatnonzero(big & quiet & (delta != 0))
    out["F2 吸收"] = (idx, -np.sign(delta[idx]))

    # F3 衰竭：連 3 bar 同向且 |delta| 遞減 → 預測反轉
    s = np.sign(delta)
    same = np.zeros(n, bool)
    same[2:] = (s[2:] == s[1:-1]) & (s[1:-1] == s[:-2]) & (s[2:] != 0)
    fading = np.zeros(n, bool)
    fading[2:] = (ad[2:] < ad[1:-1]) & (ad[1:-1] < ad[:-2])
    idx = np.flatnonzero(same & fading)
    out["F3 衰竭"] = (idx, -s[idx])

    # F4 Delta 動能（樸素對照組）：順著 delta
    idx = np.flatnonzero(delta != 0)
    out["F4 動能"] = (idx, np.sign(delta[idx]))
    return out


def block_stats(x, day):
    """壓成 (每日總和, 每日筆數)。重抽後的平均 = Σ和 / Σ筆數，與串接後取平均等價，
    但成本是 O(天數) 而非 O(樣本數)——六年資料差四個數量級。"""
    ok = ~np.isnan(x)
    x, day = x[ok], day[ok]
    if len(x) == 0:
        return None
    keys, inv = np.unique(day, return_inverse=True)
    return (np.bincount(inv, weights=x, minlength=len(keys)),
            np.bincount(inv, minlength=len(keys)).astype(np.float64))


def boot_from(g):
    if g is None or len(g[0]) == 0:
        return np.full(BOOT, np.nan)
    sums, cnts = g
    nb = len(sums)
    pick = RNG.integers(0, nb, (BOOT, nb))
    return sums[pick].sum(axis=1) / cnts[pick].sum(axis=1)


def boot_ci_p(x, day, bs_base):
    g = block_stats(x, day)
    n = int(g[1].sum()) if g is not None else 0
    if n < 100:
        return n, np.nan, (np.nan, np.nan), np.nan
    bs = np.sort(boot_from(g))
    diff = bs - bs_base
    p = min(2 * min((diff <= 0).mean(), (diff >= 0).mean()), 1.0)
    return n, float(g[0].sum() / g[1].sum()), (bs[int(.025 * BOOT)], bs[int(.975 * BOOT)]), p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="只跑前 N 個幣（除錯用）")
    a = ap.parse_args()
    files = sorted(glob.glob(str(BARS / "*.parquet")))[: a.limit]
    if not files:
        raise SystemExit("沒有 bar 檔，先跑 research/build_bars.py")

    names = ["F1 背離", "F2 吸收", "F3 衰竭", "F4 動能"]
    acc = {(nm, h, seg): {"x": [], "d": []}
           for nm in names for h in HORIZONS for seg in ("IS", "OOS")}
    base = {(h, seg): {"x": [], "d": []} for h in HORIZONS for seg in ("IS", "OOS")}
    nsym = 0

    for f in files:
        try:
            ms, close, delta, cvd, ret, day = load_symbol(f)
        except Exception as e:
            print(f"  {Path(f).stem}: 讀取失敗 {type(e).__name__}", flush=True)
            continue
        if len(ms) < 10_000:
            continue
        nsym += 1
        mk = masks_for(close, delta, cvd, ret)
        for h in HORIZONS:
            # 對照組：同幣隨機 bar，方向隨機，數量對齊 F4（最大的集合）
            rnd = RNG.integers(0, len(ms) - 1, min(len(ms) // 20, 200_000))
            rsgn = RNG.choice([-1.0, 1.0], len(rnd))
            rx = fwd_ret(ms, close, rnd, h) * rsgn
            for seg, sel in (("IS", ms[rnd] < OOS_MS), ("OOS", ms[rnd] >= OOS_MS)):
                base[(h, seg)]["x"].append(rx[sel])
                base[(h, seg)]["d"].append(day[rnd][sel])
            for nm, (idx, sgn) in mk.items():
                if len(idx) == 0:
                    continue
                x = fwd_ret(ms, close, idx, h) * sgn
                for seg, sel in (("IS", ms[idx] < OOS_MS), ("OOS", ms[idx] >= OOS_MS)):
                    acc[(nm, h, seg)]["x"].append(x[sel])
                    acc[(nm, h, seg)]["d"].append(day[idx][sel])
        print(f"  {Path(f).stem}: {len(ms):,} bar", flush=True)

    cat = lambda d: (np.concatenate(d["x"]) if d["x"] else np.array([]),
                     np.concatenate(d["d"]) if d["d"] else np.array([]))
    print(f"\n{nsym} 個幣。IS = 2019-09~2024-12，OOS = 2025-01 之後。")
    print("符號已對齊成「假說預測方向為正」，所以平均為正才算支持。\n")

    results = []
    for seg in ("IS", "OOS"):
        bx, bd = cat(base[(HORIZONS[0], seg)])
        for h in HORIZONS:
            bx, bd = cat(base[(h, seg)])
            bs_base = np.sort(boot_from(block_stats(bx, bd)))
            for nm in names:
                x, d = cat(acc[(nm, h, seg)])
                n, mu, ci, p = boot_ci_p(x, d, bs_base)
                results.append(dict(seg=seg, h=h, name=nm, n=n, mu=mu, ci=ci, p=p,
                                    base=float(np.nanmean(bx))))

    # BH 只對 IS 的 12 格做（預先註記：IS 沒過的不得在 OOS 再測）
    is_res = [r for r in results if r["seg"] == "IS" and r["p"] == r["p"]]
    is_res.sort(key=lambda r: r["p"])
    m, cut = len(is_res), 0.0
    for i, r in enumerate(is_res, 1):
        if r["p"] <= i / m * FDR:
            cut = r["p"]
    for r in results:
        r["bh"] = r["seg"] == "IS" and r["p"] == r["p"] and r["p"] <= cut

    for seg in ("IS", "OOS"):
        print(f"── {seg} ──")
        print(f"{'假說':12s}{'窗':>4s}{'n':>11s}{'平均bps':>9s}{'95%CI':>19s}{'p':>8s}  BH")
        for h in HORIZONS:
            for r in [x for x in results if x["seg"] == seg and x["h"] == h]:
                if r["n"] < 100:
                    print(f"{r['name']:12s}{h:4d}{r['n']:11d}   樣本不足"); continue
                print(f"{r['name']:12s}{h:4d}{r['n']:11,d}{r['mu']:+9.3f}"
                      f" [{r['ci'][0]:+7.3f},{r['ci'][1]:+7.3f}]{r['p']:8.3f}"
                      f"  {'✓' if r['bh'] else '·'}")
        print()

    print("═" * 74)
    sig = [r for r in results if r["seg"] == "IS" and r["bh"] and r["mu"] > 0]
    if not sig:
        print("IS 沒有任何格子在 BH 校正後同時顯著且方向正確 → OOS 不必再測。")
        return
    print(f"IS 通過 BH 且方向正確的格子：{len(sig)}")
    f4 = {(r['h']): r['mu'] for r in results if r['seg'] == 'IS' and r['name'] == 'F4 動能'}
    for r in sig:
        o = next((z for z in results if z["seg"] == "OOS" and z["h"] == r["h"]
                  and z["name"] == r["name"]), None)
        oos = f"OOS {o['mu']:+.3f} bps (n={o['n']:,})" if o and o["n"] >= 100 else "OOS 樣本不足"
        beat = "" if r["name"] == "F4 動能" else \
            f"  vs F4 對照組 {f4.get(r['h'], float('nan')):+.3f}"
        print(f"  {r['name']} @{r['h']}m  IS {r['mu']:+.3f} bps  p={r['p']:.3f}  {oos}{beat}")
    print("\n提醒：成本門檻另計。Binance UM taker 單邊 4 bps，來回約 8 bps 起跳；")
    print("上面任何低於該量級的平均報酬，統計上顯著也交易不了。")


if __name__ == "__main__":
    main()
