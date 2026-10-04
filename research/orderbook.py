#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb", "numpy"]
# ///
"""從自錄的 L2 增量重建訂單簿，並用「下一個獨立 REST 快照」驗證重建正確性。

這是整個 heatmap 路線的地基：如果重建對不上快照，後面所有 absorption/exhaustion
偵測都是建在錯的簿子上。所以驗證不是可選步驟。

依 Binance 期貨官方規則套用增量：
  丟棄 u < snapshot.lastUpdateId 的事件；第一筆須滿足 U <= lastUpdateId <= u；
  其後每筆的 pu 必須等於前一筆的 u，否則序列斷裂必須重新同步。

  uv run research/orderbook.py --symbol BTCUSDT
"""
import argparse, os
from pathlib import Path
import duckdb

DB = Path(__file__).resolve().parent.parent / "state/market.duckdb"


def load(con, symbol):
    snaps = con.execute("""
        SELECT recv_ns, last_update_id, bids, asks FROM rec_depth_snap
        WHERE symbol=? ORDER BY recv_ns""", [symbol]).fetchall()
    diffs = con.execute("""
        SELECT recv_ns, trans_ms, first_id, final_id, prev_final_id, bids, asks
        FROM rec_depth WHERE symbol=? ORDER BY final_id""", [symbol]).fetchall()
    return snaps, diffs


def apply_diff(book_b, book_a, bids, asks):
    for side, lvls in ((book_b, bids), (book_a, asks)):
        for px, qty in lvls or []:
            q = float(qty)
            p = float(px)
            if q == 0.0:
                side.pop(p, None)
            else:
                side[p] = q


def rebuild(snap, diffs):
    """從一個快照往後套增量，回傳 (book_bids, book_asks, 統計)。"""
    recv, luid, sb, sa = snap
    book_b = {float(p): float(q) for p, q in sb}
    book_a = {float(p): float(q) for p, q in sa}
    applied = dropped = 0
    started = False
    prev_u = None
    gaps = []
    for d in diffs:
        _, _, U, u, pu, b, a = d
        if u < luid:
            dropped += 1
            continue
        if not started:
            if U > luid + 1:
                gaps.append(("snapshot_gap", luid, U))
            started = True
        elif pu is not None and prev_u is not None and pu != prev_u:
            gaps.append(("seq_break", prev_u, pu))
        prev_u = u
        apply_diff(book_b, book_a, b, a)
        applied += 1
    return book_b, book_a, {"applied": applied, "dropped": dropped, "gaps": gaps}


def compare(book, snap_levels, label, tol=1e-9):
    """只比較落在快照價格區間內的檔位（REST 快照只有前 1000 檔）。"""
    ref = {float(p): float(q) for p, q in snap_levels}
    if not ref:
        return None
    lo, hi = min(ref), max(ref)
    mine = {p: q for p, q in book.items() if lo <= p <= hi}
    same = sum(1 for p, q in ref.items() if abs(mine.get(p, 0.0) - q) <= tol)
    only_ref = [p for p in ref if p not in mine]
    only_mine = [p for p in mine if p not in ref]
    diff_qty = [(p, ref[p], mine[p]) for p in ref
                if p in mine and abs(mine[p] - ref[p]) > tol]
    return {"label": label, "ref_levels": len(ref), "mine_in_range": len(mine),
            "exact_match": same, "match_pct": 100.0 * same / len(ref),
            "missing": len(only_ref), "extra": len(only_mine),
            "qty_mismatch": len(diff_qty), "sample_mismatch": diff_qty[:3]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    a = ap.parse_args()
    con = duckdb.connect(str(DB), read_only=True)
    snaps, diffs = load(con, a.symbol)
    print(f"{a.symbol}: {len(snaps)} 個 REST 快照, {len(diffs)} 筆增量")
    if len(snaps) < 2:
        raise SystemExit("需要至少 2 個快照才能做重建驗證")

    ok = 0
    for i in range(len(snaps) - 1):
        s0, s1 = snaps[i], snaps[i + 1]
        # 用 update id 而非接收時間切視窗：REST 快照有往返延遲，
        # 按時間切會在開頭製造出不存在的假缺口。
        window = [d for d in diffs if d[3] >= s0[1] and d[0] <= s1[0]]
        if not window:
            continue
        bb, ba, st = rebuild(s0, window)
        rb = compare(bb, s1[2], "bids")
        ra = compare(ba, s1[3], "asks")
        dt = (s1[0] - s0[0]) / 1e9
        print(f"\n── 快照 {i} → {i+1}（間隔 {dt:.0f}s，套用 {st['applied']} 筆增量，"
              f"丟棄 {st['dropped']} 筆，序列斷裂 {len(st['gaps'])} 次）")
        for r in (rb, ra):
            if r:
                print(f"   {r['label']:5s} 參考 {r['ref_levels']:4d} 檔 | "
                      f"完全吻合 {r['exact_match']:4d} ({r['match_pct']:5.1f}%) | "
                      f"缺 {r['missing']:3d} 多 {r['extra']:4d} 量不符 {r['qty_mismatch']:3d}")
                if r["sample_mismatch"]:
                    for p, refq, myq in r["sample_mismatch"]:
                        print(f"          例: 價 {p} 參考 {refq} vs 重建 {myq}")
        if rb and ra and rb["match_pct"] > 99 and ra["match_pct"] > 99:
            ok += 1
    print(f"\n通過（雙邊 >99% 吻合）的視窗: {ok}/{max(0,len(snaps)-1)}")


if __name__ == "__main__":
    main()
