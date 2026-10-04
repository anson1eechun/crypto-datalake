#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb", "pyarrow"]
# ///
"""從重建的訂單簿 + 逐筆成交，偵測影片那套系統的「牆」生命週期與互動結果。

牆用**價格分桶**認定，不用逐檔。理由是 tick size 相對價格的比例在各幣差兩個
數量級（BTC 的 25bp 有約 2750 個 tick，CHZ 只有 6 個），任何「單一檔位 vs 帶內
其他檔位」的門檻都會被 tick 密度綁死而無法跨幣比較。影片的 heatmap 本身也是
固定像素列而非逐檔，分桶才是對應的做法。

分類分兩個正交的軸，不擠成單一標籤：
  behavior   存活期間的行為  persistent / fresh / reloading / iceberg
  end_reason 消失的方式      consumed / pulled / partial / shrank

輸出事件表給 falsify.py 做前瞻報酬檢定。偵測本身不判斷賺賠。

  uv run research/flow.py --symbol BTCUSDT --hours 2
"""
import argparse
from collections import defaultdict, deque
from pathlib import Path
import duckdb, pyarrow as pa, pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "state/market.duckdb"

BAND_BPS = 25.0       # 起始帶寬；帶內有量的桶不足就往外擴（見 band_of）
BAND_MAX_BPS = 200.0  # 擴到這裡還不夠桶就放棄這個時點
BUCKET_BPS = 1.0      # 分桶粒度，相當於 heatmap 的一列
# 門檻取 3.0：實測 64 個幣的桶深度/平均桶比值，跨幣中位數是 最大 3.1x、
# p99 3.1x、p95 2.4x。原本設 5.0 比多數幣的歷史最大值還高，等於偵測不到東西。
WALL_MULT = 3.0
MIN_BUCKETS = 8       # 帶內有量的桶少於此，平均值就沒有代表性
MIN_LIFE_MS = 2_000   # 活得比這短當雜訊丟掉（掛單閃爍）
PERSIST_MS = 30_000   # 活過這個時間才叫 persistent
REFILL_DROP = 0.30    # 被吃掉這個比例以上才算一次「被打」
REFILL_BACK = 0.80    # 補回峰值的這個比例才算一次 reload
ICEBERG_MULT = 1.5    # 該桶成交量超過顯示峰值這個倍數 → 冰山
PULL_TRADED = 0.20    # 消失量中成交佔比低於此 → 撤單
EATEN_TRADED = 0.50   # 消失量中成交佔比高於此 → 被吃掉
END_FRAC = 0.30       # 剩不到峰值的這個比例就算這道牆結束
REBASE_EVERY = 20     # 每這麼多次簿更新重算一次帶內平均
# context 特徵的窗長。影片反覆強調 location 與 reaction：同一道牆在趨勢順向
# 或逆向、在區間中央或在近期高低點附近，意義完全不同。這裡只做標註不做過濾，
# 免得在同一批資料上邊看結果邊挑條件。
TREND_MS = 300_000    # 事件前這段時間的中價變化，當作趨勢
RANGE_MS = 1_800_000  # 事件前這段時間的高低點，當作關鍵價位參考


class Wall:
    __slots__ = ("side", "bkt", "px", "t0", "t1", "peak", "size",
                 "traded", "aggr", "refills", "hit")

    def __init__(self, side, bkt, px, t, size):
        self.side, self.bkt, self.px = side, bkt, px
        self.t0 = self.t1 = t
        self.peak = self.size = size
        self.traded = 0.0     # 該桶總成交量
        self.aggr = 0.0       # 其中「會吃掉這道牆」那一側的主動成交量
        self.refills = 0
        self.hit = False

    def update(self, t, size):
        self.t1 = t
        if size <= self.peak * (1 - REFILL_DROP):
            self.hit = True
        elif self.hit and size >= self.peak * REFILL_BACK:
            self.refills += 1
            self.hit = False
        self.peak = max(self.peak, size)
        self.size = size

    def behavior(self):
        """存活期間的行為。用「吃這道牆的主動量」而非總成交量：

        影片講的 absorption 是主動賣壓打進買方牆而價格不下去，關鍵在攻擊方向。
        買方牆對應主動賣出（is_buyer_maker=True，掛買被主動賣單吃），賣方牆反之。
        用總量會把牆自己成交的部分也算進來，冰山與吸收都會被高估。
        """
        if self.aggr >= self.peak * ICEBERG_MULT:
            return "iceberg"
        if self.refills >= 2:
            return "reloading"
        return "persistent" if self.t1 - self.t0 >= PERSIST_MS else "fresh"

    def end_reason(self, vanished):
        """消失的方式。跟 behavior 是兩個獨立的軸。"""
        if vanished <= 0:
            return "shrank"
        frac = self.traded / vanished
        if frac >= EATEN_TRADED:
            return "consumed"
        return "pulled" if frac < PULL_TRADED else "partial"


def load(con, symbol, hours, after_ms=None):
    snaps = con.execute("""
        SELECT last_update_id, trans_ms, bids, asks FROM rec_depth_snap
        WHERE symbol=? AND (? IS NULL OR trans_ms >= ?) ORDER BY recv_ns""",
        [symbol, after_ms, after_ms]).fetchall()
    if not snaps:
        raise NoData(f"{symbol} 沒有可用的 REST 快照")
    # REST 快照有往返延遲，可能比第一筆增量還舊而錨不上；逐一往後試。
    for i, snap in enumerate(snaps):
        if con.execute("""SELECT count(*) FROM rec_depth
                WHERE symbol=? AND first_id<=? AND ?<=final_id""",
                [symbol, snap[0], snap[0]]).fetchone()[0]:
            break
    else:
        raise NoData(f"{symbol} 的 {len(snaps)} 個快照都錨不上增量序列")
    t0 = snap[1]
    t1 = t0 + int(hours * 3600_000)
    # 串流讀取而不是 fetchall：整批物化會把 bids/asks 變成幾百萬個巢狀 Python
    # 清單，ETH 這種高頻幣單日就吃掉 17 GB 而開始換頁。
    # 必須用獨立游標：DuckDB 一個連線只保留一個結果集，接著查 trades 會把
    # 這個串流作廢（症狀是簿更新 0、事件 0，完全不報錯）。
    cur = con.cursor().execute("""
        SELECT trans_ms, first_id, final_id, prev_final_id, bids, asks
        FROM rec_depth WHERE symbol=? AND trans_ms BETWEEN ? AND ?
        ORDER BY final_id""", [symbol, t0, t1])

    def diff_stream(batch=50_000):
        while True:
            rows = cur.fetchmany(batch)
            if not rows:
                return
            yield from rows

    diffs = diff_stream()
    trades = con.execute("""
        SELECT trade_ms, price, qty, is_buyer_maker FROM rec_trade
        WHERE symbol=? AND trade_ms BETWEEN ? AND ? ORDER BY trade_ms""",
        [symbol, t0, t1]).fetchall()
    return snap, i, diffs, trades, t1


def band_depth(bids, asks, mid, bw):
    """算帶內每個價格桶的深度。帶寬會往外擴到兩側都有足夠的桶為止。

    低價幣的 25bp 只涵蓋幾個 tick，固定帶寬會讓它們永遠湊不滿桶而被靜默丟掉。
    """
    mb = round(mid / bw)
    band = BAND_BPS
    while True:
        n = int(band / BUCKET_BPS)
        depth = {"bid": defaultdict(float), "ask": defaultdict(float)}
        for side, book in (("bid", bids), ("ask", asks)):
            for p, q in book.items():
                k = round(p / bw)
                if abs(k - mb) <= n:
                    depth[side][k] += q
        if min(len(depth["bid"]), len(depth["ask"])) >= MIN_BUCKETS:
            return depth
        if band >= BAND_MAX_BPS:
            return depth
        band *= 2


def context(hist, t, mid):
    """回傳 (前 TREND_MS 的中價變化 bps, 距近期高點 bps, 距近期低點 bps)。

    距高點為負、距低點為正，都以 bps 表示；資料不足回 nan。
    """
    if not hist:
        return float("nan"), float("nan"), float("nan")
    ref = None
    hi = lo = hist[-1][1]
    for ts, m in hist:
        if ref is None and ts >= t - TREND_MS:
            ref = m
        hi = max(hi, m)
        lo = min(lo, m)
    trend = (mid / ref - 1.0) * 1e4 if ref else float("nan")
    return trend, (mid - hi) / mid * 1e4, (mid - lo) / mid * 1e4


class NoData(Exception):
    """這個時間窗沒有可錨定的資料，不是錯誤。"""


def run_window(symbol, hours, db, after_ms=None):
    con = duckdb.connect(str(db), read_only=True)
    snap, snap_i, diffs, trades, t_end = load(con, symbol, hours, after_ms)
    luid, _, sb, sa = snap
    bids = {float(p): float(q) for p, q in sb}
    asks = {float(p): float(q) for p, q in sa}
    # 桶寬用起始中價固定住，讓桶編號在整段期間代表同一個絕對價格區間
    mid0 = (max(bids) + min(asks)) / 2
    bw = mid0 * BUCKET_BPS / 1e4

    walls, events = {}, []
    hist = deque()          # (trans_ms, mid)，只留 RANGE_MS 內的
    traded_bkt = defaultdict(float)     # 桶 -> 總成交量
    aggr_bkt = {"bid": defaultdict(float), "ask": defaultdict(float)}
    ti = n_upd = breaks = 0
    n_tr = len(trades)
    started, prev_u = False, None

    for trans_ms, U, u, pu, db_, da in diffs:
        if u < luid:
            continue
        if not started:
            if not (U <= luid <= u):
                continue
            started = True
        elif pu != prev_u:
            breaks += 1        # 序列斷裂：簿子不可信，但續跑並記下來
        prev_u = u

        while ti < len(trades) and trades[ti][0] <= trans_ms:
            _, px, qty, bmaker = trades[ti]
            k = round(px / bw)
            traded_bkt[k] += qty
            # is_buyer_maker=True → 掛買被主動賣單吃 → 這筆在吃買方牆
            aggr_bkt["bid" if bmaker else "ask"][k] += qty
            ti += 1

        for book, lvls in ((bids, db_), (asks, da)):
            for p, q in lvls or []:
                p, q = float(p), float(q)
                if q == 0.0:
                    book.pop(p, None)
                else:
                    book[p] = q
        if not bids or not asks:
            continue

        mid = (max(bids) + min(asks)) / 2
        hist.append((trans_ms, mid))
        while hist and hist[0][0] < trans_ms - RANGE_MS:
            hist.popleft()
        n_upd += 1
        if n_upd % REBASE_EVERY == 1:
            depth = band_depth(bids, asks, mid, bw)

        for side in ("bid", "ask"):
            d = depth[side]
            if len(d) < MIN_BUCKETS:
                continue
            thr = sum(d.values()) / len(d) * WALL_MULT
            for k, q in d.items():
                if q >= thr and (side, k) not in walls:
                    walls[(side, k)] = Wall(side, k, k * bw, trans_ms, q)
                    traded_bkt[k] = 0.0
                    aggr_bkt[side][k] = 0.0
            for key in [x for x in walls if x[0] == side]:
                w = walls[key]
                cur = d.get(w.bkt, 0.0)
                if cur >= w.peak * END_FRAC:
                    w.update(trans_ms, cur)
                    continue
                walls.pop(key)
                w.traded = traded_bkt.get(w.bkt, 0.0)
                w.aggr = aggr_bkt[side].get(w.bkt, 0.0)
                if w.t1 - w.t0 < MIN_LIFE_MS:
                    continue
                vanished = max(w.peak - cur, 0.0)
                trend, dhi, dlo = context(hist, trans_ms, mid)
                events.append((symbol, w.side, w.px, w.t0, w.t1, w.t1 - w.t0,
                               w.peak, w.traded, w.aggr, w.refills, vanished,
                               w.behavior(), w.end_reason(vanished),
                               (w.px - mid) / mid * 1e4, trend, dhi, dlo))

    return events, n_upd, breaks, n_tr, t_end


def run(symbol, hours, out, db=DB, chunk_hours=6.0):
    """逐段處理並累積事件。

    一次把 10 天的 depth 讀進記憶體會撐爆（單幣約 480 萬列），所以切成日窗，
    每一段各自用自己的 REST 快照重新錨定——這也順便讓某天序列斷裂不會污染其他天。
    """
    events, tot_upd, tot_breaks, tot_tr, nwin = [], 0, 0, 0, 0
    after, remaining = None, hours
    while remaining > 0.5:
        h = min(chunk_hours, remaining)
        try:
            ev, nu, br, nt, t_end = run_window(symbol, h, db, after)
        except NoData:
            break
        events += ev
        tot_upd += nu; tot_breaks += br; tot_tr += nt; nwin += 1
        after, remaining = t_end, remaining - h

    schema = pa.schema([("symbol", pa.string()), ("side", pa.string()),
                        ("price", pa.float64()), ("start_ms", pa.int64()),
                        ("end_ms", pa.int64()), ("life_ms", pa.int64()),
                        ("peak_size", pa.float64()), ("traded", pa.float64()),
                        ("aggr", pa.float64()),
                        ("refills", pa.int32()), ("vanished", pa.float64()),
                        ("behavior", pa.string()), ("end_reason", pa.string()),
                        ("dist_bps_at_end", pa.float64()),
                        ("trend_bps", pa.float64()),
                        ("dist_hi_bps", pa.float64()),
                        ("dist_lo_bps", pa.float64())])
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(
        [dict(zip(schema.names, e)) for e in events], schema=schema),
        out, compression="zstd")

    print(f"{symbol}: {nwin} 個時間窗 簿更新 {tot_upd} 序列斷裂 {tot_breaks} "
          f"成交 {tot_tr} 事件 {len(events)}")
    cnt = defaultdict(int)
    for e in events:
        cnt[(e[11], e[12])] += 1
    for (beh, end), n in sorted(cnt.items(), key=lambda x: -x[1]):
        print(f"  {beh:12s} {end:10s} {n:6d}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--hours", type=float, default=2.0)
    ap.add_argument("--out", default=None)
    ap.add_argument("--db", default=str(DB))
    ap.add_argument("--chunk-hours", type=float, default=6.0)
    a = ap.parse_args()
    out = Path(a.out) if a.out else ROOT / f"research/events/{a.symbol}.parquet"
    run(a.symbol.upper(), a.hours, out, a.db, a.chunk_hours)
