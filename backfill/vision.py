#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Binance Vision 歷史資料回填器。

以 S3 listing 探索遠端「實際存在」的檔案（不猜日期、不靠 404 試錯），
寫進 manifest.sqlite，再斷點續傳下載到 NAS，逐檔用官方 .CHECKSUM 驗 SHA256。
本機路徑完全鏡射遠端 key，所以任何檔案都能回推來源與重新驗證。

  uv run backfill/vision.py index          # 探索遠端有什麼（只讀，不下載）
  uv run backfill/vision.py status         # 覆蓋率與待下載量
  uv run backfill/vision.py fetch [--jobs 8] [--dataset um/aggTrades] [--limit N]
"""
import argparse, hashlib, os, sqlite3, threading, time, urllib.parse, urllib.request, urllib.error
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# 全域下載流量上限。錄製器只需要約 1 MB/s，但下載搶滿上行/下行時會讓 WebSocket
# 掉線（實測：回填全速時 5 分鐘斷線 41 次，暫停後剩 1 次）。留足餘裕給即時串流。
class RateLimiter:
    def __init__(self, bps):
        self.bps = bps
        self.lock = threading.Lock()
        self.allow = float(bps)
        self.ts = time.monotonic()

    def take(self, n):
        if not self.bps:
            return
        while True:
            with self.lock:
                now = time.monotonic()
                self.allow = min(self.bps, self.allow + (now - self.ts) * self.bps)
                self.ts = now
                if self.allow >= n:
                    self.allow -= n
                    return
                need = (n - self.allow) / self.bps
            time.sleep(min(need, 0.5))


LIMITER = None
DB = ROOT / "state/manifest.sqlite"
DEST = Path(os.environ.get("MD_ROOT", "/srv/nas/marketdata")) / "vision"
S3 = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
CDN = "https://data.binance.vision"
UA = {"User-Agent": "crypto-datalake/1.0"}
NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"

# (名稱, 遠端路徑樣板, 頻率)。{s}=symbol。月檔優先，日檔只補月檔沒蓋到的尾巴。
DATASETS = [
    ("um/aggTrades",   "data/futures/um/{f}/aggTrades/{s}/",            "both"),
    ("um/trades",      "data/futures/um/{f}/trades/{s}/",               "both"),
    ("um/bookTicker",  "data/futures/um/{f}/bookTicker/{s}/",           "both"),
    ("um/bookDepth",   "data/futures/um/daily/bookDepth/{s}/",          "daily"),
    ("um/metrics",     "data/futures/um/daily/metrics/{s}/",            "daily"),
    ("um/klines1m",    "data/futures/um/{f}/klines/{s}/1m/",            "both"),
    ("um/markPrice1m", "data/futures/um/{f}/markPriceKlines/{s}/1m/",   "both"),
    ("um/fundingRate", "data/futures/um/monthly/fundingRate/{s}/",      "monthly"),
    ("spot/klines1s",  "data/spot/{f}/klines/{s}/1s/",                  "both"),
    ("spot/aggTrades", "data/spot/{f}/aggTrades/{s}/",                  "both"),
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS files(
  key TEXT PRIMARY KEY, dataset TEXT, symbol TEXT, freq TEXT, period TEXT,
  remote_size INTEGER, status TEXT DEFAULT 'pending', local_size INTEGER,
  sha_ok INTEGER, err TEXT, ts INTEGER);
CREATE INDEX IF NOT EXISTS i_pend ON files(status, dataset);
CREATE INDEX IF NOT EXISTS i_ds   ON files(dataset, symbol, freq, period);
CREATE TABLE IF NOT EXISTS indexed(prefix TEXT PRIMARY KEY, ts INTEGER, n INTEGER);
"""


def db():
    # 每小時的 md-maintain 會長時間持有 manifest 的寫入交易（normalize 一次跑 6-7 分鐘），
    # 原本 timeout=60 等不到就整支回填死掉。跟 normalize.py 對齊成 300 秒。
    c = sqlite3.connect(DB, timeout=300)
    c.executescript(SCHEMA)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=300000")
    return c


def retry_locked(fn, tries=40, wait=30):
    """SQLite 被鎖住時重試，而不是讓整個資料集的回填中止。"""
    for i in range(tries):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower() or i == tries - 1:
                raise
            print(f"  [db] 被鎖住，{wait}s 後重試 ({i+1}/{tries})", flush=True)
            time.sleep(wait)


def http(url, retries=4, method="GET"):
    for i in range(retries):
        try:
            r = urllib.request.Request(url, headers=UA, method=method)
            with urllib.request.urlopen(r, timeout=90) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if i == retries - 1:
                raise
        except Exception:
            if i == retries - 1:
                raise
        time.sleep(1.5 * (i + 1))
    return None


def list_keys(prefix):
    """S3 分頁列舉；回傳 [(key, size)]，只取 .zip。"""
    out, token = [], None
    while True:
        url = f"{S3}?list-type=2&prefix={urllib.parse.quote(prefix)}&max-keys=1000"
        if token:
            url += "&continuation-token=" + urllib.parse.quote(token, safe="")
        body = http(url)
        if body is None:
            return out
        root = ET.fromstring(body)
        for c in root.findall(NS + "Contents"):
            k = c.findtext(NS + "Key", "")
            if k.endswith(".zip"):
                out.append((k, int(c.findtext(NS + "Size", "0"))))
        if root.findtext(NS + "IsTruncated") != "true":
            return out
        token = root.findtext(NS + "NextContinuationToken")


def period_of(key):
    """從檔名尾端取出 YYYY-MM 或 YYYY-MM-DD。"""
    stem = key.rsplit("/", 1)[-1][:-4]
    for n in (3, 2):
        cand = "-".join(stem.split("-")[-n:])
        p = cand.split("-")
        if len(p) == n and len(p[0]) == 4 and all(x.isdigit() for x in p):
            return cand
    return stem


def cmd_index(args):
    syms = [s.strip().upper() + "USDT" for s in
            Path(args.symbols).read_text().split() if s.strip()]
    con = db()
    todo = []
    for name, tmpl, freq in DATASETS:
        if args.dataset and args.dataset != name:
            continue
        freqs = ["monthly", "daily"] if freq == "both" else [freq]
        for s in syms:
            for f in freqs:
                todo.append((name, f, s, tmpl.format(f=f, s=s)))

    done = {r[0] for r in con.execute("SELECT prefix FROM indexed")} if not args.force else set()
    todo = [t for t in todo if t[3] not in done]
    print(f"要列舉 {len(todo)} 個 prefix（已完成 {len(done)} 個，--force 可重掃）", flush=True)

    n_new = 0
    with ThreadPoolExecutor(args.jobs) as ex:
        for (name, f, s, pref), keys in zip(todo, ex.map(lambda t: list_keys(t[3]), todo)):
            rows = [(k, name, s, f, period_of(k), sz) for k, sz in keys]
            con.executemany(
                "INSERT OR IGNORE INTO files(key,dataset,symbol,freq,period,remote_size)"
                " VALUES(?,?,?,?,?,?)", rows)
            con.execute("INSERT OR REPLACE INTO indexed VALUES(?,?,?)",
                        (pref, int(time.time()), len(rows)))
            con.commit()
            n_new += len(rows)
            if n_new and len(keys):
                print(f"  {name:16s} {s:12s} {f:8s} {len(keys):5d} 檔", flush=True)
    tot = con.execute("SELECT COUNT(*), SUM(remote_size) FROM files").fetchone()
    print(f"\nmanifest 共 {tot[0]} 檔, 遠端合計 {(tot[1] or 0)/1e9:.1f} GB")


def _redundant_daily(con):
    """日檔若該月已有月檔就跳過（避免重複下載＋控制檔案數）。"""
    retry_locked(lambda: con.execute("""UPDATE files SET status='skip_monthly_covered'
        WHERE freq='daily' AND status='pending' AND EXISTS(
          SELECT 1 FROM files m WHERE m.freq='monthly' AND m.status IN ('pending','done')
          AND m.dataset=files.dataset AND m.symbol=files.symbol
          AND m.period=substr(files.period,1,7))"""))
    retry_locked(con.commit)


def fetch_one(row):
    """串流下載到 .part 再原子改名；SHA256 邊寫邊算，避免把 GB 級月檔讀進記憶體。"""
    key, size = row
    dest = DEST / key
    tmp = dest.with_suffix(dest.suffix + ".part")
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size == size:
        return key, "done", size, None, None

    want = None
    chk = http(f"{CDN}/{key}.CHECKSUM")
    if chk:
        want = chk.decode().split()[0].strip()

    h = hashlib.sha256()
    n = 0
    try:
        req = urllib.request.Request(f"{CDN}/{key}", headers=UA)
        with urllib.request.urlopen(req, timeout=120) as r, open(tmp, "wb") as f:
            while True:
                buf = r.read(1 << 18)
                if not buf:
                    break
                if LIMITER:
                    LIMITER.take(len(buf))
                f.write(buf)
                h.update(buf)
                n += len(buf)
    except urllib.error.HTTPError as e:
        tmp.unlink(missing_ok=True)
        return key, "missing" if e.code == 404 else "http_err", 0, None, str(e.code)
    except Exception as e:
        tmp.unlink(missing_ok=True)
        return key, "net_err", 0, None, f"{type(e).__name__}: {str(e)[:80]}"

    sha_ok = None
    if want is not None:
        sha_ok = 1 if h.hexdigest() == want else 0
        if sha_ok == 0:
            tmp.unlink(missing_ok=True)
            return key, "bad_checksum", n, 0, "sha256 不符"
    os.replace(tmp, dest)
    return key, "done", n, sha_ok, None


RETRYABLE = ("net_err", "http_err", "bad_checksum")


def cmd_verify(args):
    """比對 manifest 的 done 與磁碟實況，檔案不見的重設為 pending。

    manifest 在 NVMe、資料在 NAS，兩者生命週期不同：2026-09-07 把 NAS 整顆從
    NTFS 重做成 ext4 時，遷移前下載的 223.9 GB 全部消失而 manifest 仍記著 done，
    於是轉檔階段看到 0 個來源、回報「需重建 0 組」，看起來像成功。
    """
    con = db()
    q = "SELECT key, dataset FROM files WHERE status='done'"
    p_ = []
    if args.dataset:
        q += " AND dataset=?"
        p_.append(args.dataset)
    miss, per = [], {}
    for key, ds in con.execute(q, p_).fetchall():
        if not (DEST / key).exists():
            miss.append(key)
            per[ds] = per.get(ds, 0) + 1
    for ds, n in sorted(per.items()):
        print(f"  {ds:18s} 缺 {n} 檔")
    if not miss:
        print("全部對得上，沒有缺檔")
        return
    if args.dry_run:
        print(f"\n合計 {len(miss)} 檔不在磁碟上（--dry-run，未改動）")
        return
    for i in range(0, len(miss), 5000):
        chunk = miss[i:i + 5000]
        retry_locked(lambda: con.executemany(
            "UPDATE files SET status='pending', local_size=0, sha_ok=NULL, err=NULL"
            " WHERE key=?", [(k,) for k in chunk]))
    retry_locked(con.commit)
    print(f"\n已把 {len(miss)} 檔重設為 pending")


def cmd_retry(args):
    """把暫時性失敗改回 pending 再下一次。

    斷線/逾時/503/校驗不符都是可重試的（失敗時 .part 已刪，本地沒有殘留損壞檔），
    但 manifest 會把它們標成非 pending，不重置的話暫時抖動就變成資料集的永久缺口。
    """
    con = db()
    q = "UPDATE files SET status='pending', err=NULL WHERE status IN (?,?,?)"
    p = list(RETRYABLE)
    if args.dataset:
        q += " AND dataset=?"
        p.append(args.dataset)
    cur = retry_locked(lambda: con.execute(q, p))
    retry_locked(con.commit)
    print(f"已重置 {cur.rowcount} 個失敗檔為 pending"
          + (f"（{args.dataset}）" if args.dataset else ""), flush=True)


def cmd_fetch(args):
    con = db()
    _redundant_daily(con)
    q = "SELECT key, remote_size FROM files WHERE status='pending'"
    p = []
    if args.dataset:
        q += " AND dataset=?"; p.append(args.dataset)
    q += " ORDER BY dataset, symbol, period"
    if args.limit:
        q += f" LIMIT {args.limit}"
    rows = con.execute(q, p).fetchall()
    total = sum(r[1] for r in rows)
    print(f"待下載 {len(rows)} 檔 / {total/1e9:.2f} GB", flush=True)
    if not rows:
        return
    t0, got, nerr = time.time(), 0, 0
    with ThreadPoolExecutor(args.jobs) as ex:
        for i, (key, st, sz, sha, err) in enumerate(ex.map(fetch_one, rows), 1):
            retry_locked(lambda: con.execute(
                "UPDATE files SET status=?,local_size=?,sha_ok=?,err=?,ts=? WHERE key=?",
                (st, sz, sha, err, int(time.time()), key)))
            got += sz
            nerr += st not in ("done",)
            if i % 50 == 0 or i == len(rows):
                retry_locked(con.commit)
                el = time.time() - t0
                print(f"  {i}/{len(rows)}  {got/1e9:.2f} GB  {got/1e6/el:.1f} MB/s  誤 {nerr}",
                      flush=True)
    con.commit()


def cmd_status(args):
    con = db()
    _redundant_daily(con)
    print(f"{'dataset':16s} {'狀態':22s} {'檔數':>7s} {'GB':>9s}")
    for ds, st, n, b in con.execute(
            "SELECT dataset,status,COUNT(*),SUM(remote_size) FROM files"
            " GROUP BY dataset,status ORDER BY dataset,status"):
        print(f"{ds:16s} {st:22s} {n:7d} {(b or 0)/1e9:9.2f}")
    print("\n各資料集時間覆蓋（僅計已下載）")
    for ds, a, z, n in con.execute(
            "SELECT dataset,MIN(period),MAX(period),COUNT(DISTINCT symbol) FROM files"
            " WHERE status='done' GROUP BY dataset"):
        print(f"  {ds:16s} {a} → {z}   {n} symbols")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["index", "fetch", "status", "retry", "verify"])
    ap.add_argument("--symbols", default=str(ROOT / "config/um_live.txt"))
    ap.add_argument("--dataset")
    ap.add_argument("--jobs", type=int, default=8)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--rate-mbps", type=float, default=12.0,
                    help="下載總流量上限 MB/s，留頻寬給即時錄製（0 = 不限）")
    a = ap.parse_args()
    if a.rate_mbps:
        LIMITER = RateLimiter(a.rate_mbps * 1e6)
    globals()["LIMITER"] = LIMITER
    DB.parent.mkdir(parents=True, exist_ok=True)
    {"index": cmd_index, "fetch": cmd_fetch, "status": cmd_status,
     "retry": cmd_retry, "verify": cmd_verify}[a.cmd](a)
