#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["websockets", "zstandard"]
# ///
"""Binance UM 即時微結構錄製器。

錄的是「錯過就永遠拿不回來」的資料：逐筆成交、最佳報價每次變動、L2 增量深度，
外加定期 REST 深度快照（沒有快照，depth 增量無法重建成訂單簿）與資金費/OI。

寫入策略：先落本機 NVMe spool（NTFS 上的 NAS 不適合高頻寫），整點封檔後才搬上 NAS。
行格式：<接收時間 ns>\\t<原始 JSON>  — 原封不動保存，正規化留給離線階段。

  uv run recorder/record.py --minutes 5      # 有限時間（量測用）
  uv run recorder/record.py                  # 常駐
"""
import argparse, asyncio, json, os, queue, shutil, signal, threading, time, urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import websockets
import zstandard as zstd

ROOT = Path(__file__).resolve().parent.parent
SPOOL = Path(os.environ.get("MD_SPOOL", ROOT / "spool"))
NAS = Path(os.environ.get("MD_ROOT", "/srv/nas/marketdata")) / "recorded"
WS_URL = "wss://fstream.binance.com/stream"
REST = "https://fapi.binance.com"
UA = {"User-Agent": "crypto-datalake/1.0"}
STREAMS_PER_CONN = 100
QUEUE_MAX = 200_000


def sweep_spool():
    """把上次執行殘留在 spool 的檔案搬到 NAS（只搬非當前小時的）。"""
    cur = now_hour().replace("/", "T")
    n = 0
    for p in SPOOL.rglob("*.jsonl.zst"):
        if cur in p.name:
            continue
        try:
            dest = NAS / p.relative_to(SPOOL)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(p), str(dest))
            n += 1
        except Exception as e:
            print(f"[sweep] {p} 搬移失敗: {e}", flush=True)
    if n:
        print(f"[sweep] 補搬 {n} 個殘留檔到 NAS", flush=True)


def now_hour():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d/%H")


class HourlyWriter:
    """每個 shard 一組時檔；跨 UTC 整點就封檔並搬到 NAS。"""

    def __init__(self, shard):
        self.shard = shard
        self.hour = None
        self.zw = self.fh = None
        self.path = None
        self.bytes_in = 0
        self.last_flush = 0.0

    def _open(self, hour):
        d, h = hour.split("/")
        self.path = SPOOL / f"{self.shard}/date={d}/hour={h}/{self.shard}-{d}T{h}.jsonl.zst"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fh = open(self.path, "wb")
        self.zw = zstd.ZstdCompressor(level=6, threads=2).stream_writer(self.fh)
        self.hour = hour

    def _close(self):
        if self.zw is None:
            return
        self.zw.close()
        self.fh.close()
        try:
            dest = NAS / self.path.relative_to(SPOOL)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(self.path), str(dest))
            for p in (self.path.parent, self.path.parent.parent):
                try:
                    p.rmdir()
                except OSError:
                    pass
        except Exception as e:
            print(f"[writer] 搬 NAS 失敗，留在 spool: {self.path} ({e})", flush=True)
        self.zw = self.fh = None

    def write(self, recv_ns, raw):
        h = now_hour()
        if h != self.hour:
            self._close()
            self._open(h)
        line = f"{recv_ns}\t{raw}\n".encode()
        self.zw.write(line)
        self.bytes_in += len(line)
        # 每 60s 封一個完整 frame：程序被砍時只損失最後一段，前面照樣解得開
        t = time.time()
        if t - self.last_flush > 60:
            self.zw.flush(zstd.FLUSH_FRAME)
            self.last_flush = t

    def flush_close(self):
        self._close()


class AsyncWriter:
    """壓縮與磁碟寫入搬到背景執行緒。

    先前版本在接收迴圈裡直接做 zstd 壓縮＋寫檔，壓縮一卡住 event loop 就回不了
    ping，websockets 在 ping_timeout 後關連線 —— 10 小時斷線 2222 次、訂單簿重建
    只有 7/35 視窗能對上。接收端現在只做 put_nowait。
    """

    def __init__(self, shard, stats):
        self.w = HourlyWriter(shard)
        self.q = queue.Queue(maxsize=QUEUE_MAX)
        self.stats = stats
        self.stop = threading.Event()
        self.t = threading.Thread(target=self._run, daemon=True, name=f"w-{shard}")
        self.t.start()

    def _run(self):
        while not (self.stop.is_set() and self.q.empty()):
            try:
                item = self.q.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self.w.write(*item)
            except Exception as e:
                print(f"[writer] 寫入失敗: {e}", flush=True)
        self.w.flush_close()

    def put(self, recv_ns, raw):
        try:
            self.q.put_nowait((recv_ns, raw))
        except queue.Full:
            self.stats["dropped"] += 1      # 寧可記錄丟棄，也不要阻塞接收

    def close(self):
        self.stop.set()
        self.t.join(timeout=60)


async def ws_shard(idx, streams, stats, stop_at):
    w = AsyncWriter(f"um-s{idx:02d}", stats)
    try:
        while stop_at is None or time.time() < stop_at:
            try:
                async with websockets.connect(WS_URL, ping_interval=20,
                                              ping_timeout=20, max_size=None) as ws:
                    for i in range(0, len(streams), 50):
                        await ws.send(json.dumps({"method": "SUBSCRIBE",
                                                  "params": streams[i:i + 50], "id": i}))
                        await asyncio.sleep(0.3)
                    stats["conn_ok"][idx] = time.time()
                    while stop_at is None or time.time() < stop_at:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60)
                        w.put(time.time_ns(), raw)
                        j = raw.find('"stream":"')
                        if j >= 0:
                            k = raw.find('"', j + 10)
                            s = stats["stream"][raw[j + 10:k]]
                            s[0] += len(raw)
                            s[1] += 1
            except asyncio.TimeoutError:
                stats["gap"][idx] += 1
            except Exception as e:
                stats["gap"][idx] += 1
                print(f"[conn{idx}] 重連: {type(e).__name__} {str(e)[:80]}", flush=True)
                await asyncio.sleep(2)
    finally:
        w.close()


def _get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=30) as r:
        return r.read().decode()


async def rest_poller(symbols, stats, stop_at, snap_every=900, meta_every=60):
    """depth 快照（重建訂單簿的必要錨點）＋ 資金費/標記價/OI。"""
    w = HourlyWriter("um-rest")
    loop = asyncio.get_running_loop()
    next_snap = 0.0
    try:
        while stop_at is None or time.time() < stop_at:
            t = time.time()
            try:
                body = await loop.run_in_executor(None, _get, f"{REST}/fapi/v1/premiumIndex")
                w.write(time.time_ns(),
                        json.dumps({"stream": "!premiumIndex", "data": json.loads(body)},
                                   separators=(",", ":")))
                stats["rest"]["premiumIndex"] += 1
            except Exception as e:
                print(f"[rest] premiumIndex 失敗 {e}", flush=True)

            if t >= next_snap:
                next_snap = t + snap_every
                for sym in symbols:
                    try:
                        body = await loop.run_in_executor(
                            None, _get, f"{REST}/fapi/v1/depth?symbol={sym}&limit=1000")
                        w.write(time.time_ns(),
                                json.dumps({"stream": f"{sym.lower()}@depthSnapshot",
                                            "data": json.loads(body)}, separators=(",", ":")))
                        stats["rest"]["depthSnapshot"] += 1
                    except Exception as e:
                        print(f"[rest] depth {sym} 失敗 {e}", flush=True)
                    await asyncio.sleep(0.12)          # 控權重，避免觸發 REST 限流
                for sym in symbols:
                    try:
                        body = await loop.run_in_executor(
                            None, _get, f"{REST}/fapi/v1/openInterest?symbol={sym}")
                        w.write(time.time_ns(),
                                json.dumps({"stream": f"{sym.lower()}@openInterest",
                                            "data": json.loads(body)}, separators=(",", ":")))
                        stats["rest"]["openInterest"] += 1
                    except Exception:
                        pass
                    await asyncio.sleep(0.05)
            await asyncio.sleep(max(1.0, meta_every - (time.time() - t)))
    finally:
        w.flush_close()


async def reporter(stats, stop_at, interval=60):
    t0 = time.time()
    while stop_at is None or time.time() < stop_at:
        await asyncio.sleep(interval)
        el = time.time() - t0
        tot_b = sum(v[0] for v in stats["stream"].values())
        tot_m = sum(v[1] for v in stats["stream"].values())
        print(f"[stat] {el/60:5.1f} 分  訊息 {tot_m:>9d}  原始 {tot_b/1e6:8.1f} MB  "
              f"({tot_b/el*86400/1e9:5.1f} GB/日)  斷線 {sum(stats['gap'].values())}  "
              f"REST {dict(stats['rest'])}", flush=True)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default=str(ROOT / "config/um_live.txt"))
    ap.add_argument("--kinds", default="trade,bookTicker,depth@100ms")
    ap.add_argument("--minutes", type=float, help="限時執行（量測用），預設常駐")
    ap.add_argument("--no-rest", action="store_true")
    a = ap.parse_args()

    syms = [s.strip().upper() + "USDT" for s in
            Path(a.symbols).read_text().split() if s.strip()]
    kinds = [k.strip() for k in a.kinds.split(",") if k.strip()]
    streams = [f"{s.lower()}@{k}" for s in syms for k in kinds]
    stop_at = time.time() + a.minutes * 60 if a.minutes else None
    stats = {"stream": defaultdict(lambda: [0, 0]), "gap": defaultdict(int),
             "conn_ok": {}, "rest": defaultdict(int), "dropped": 0}
    shards = [streams[i:i + STREAMS_PER_CONN]
              for i in range(0, len(streams), STREAMS_PER_CONN)]
    print(f"symbols={len(syms)} kinds={kinds} streams={len(streams)} "
          f"連線={len(shards)} spool={SPOOL} nas={NAS}", flush=True)

    sweep_spool()
    stop_ev = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_ev.set)

    async def watch_stop():
        await stop_ev.wait()
        print("[main] 收到停止訊號，封檔中", flush=True)
        raise asyncio.CancelledError

    tasks = [ws_shard(i, s, stats, stop_at) for i, s in enumerate(shards)]
    tasks.append(reporter(stats, stop_at))
    if not a.no_rest:
        tasks.append(rest_poller(syms, stats, stop_at))
    runners = [asyncio.create_task(t) for t in tasks]
    stopper = asyncio.create_task(watch_stop())
    done, pending = await asyncio.wait(runners + [stopper],
                                       return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)

    if a.minutes:
        el = a.minutes * 60
        per = defaultdict(lambda: [0, 0])
        for name, (b, m) in stats["stream"].items():
            per[name.split("@", 1)[1]][0] += b
        print("\n=== 實測（原始 JSON，未壓縮）===")
        for k, v in sorted(per.items(), key=lambda x: -x[1][0]):
            print(f"  {k:14s} {v[0]/el*86400/1e9:7.2f} GB/日")
        json.dump({n: {"bytes": b, "msgs": m, "gb_day_raw": b / el * 86400 / 1e9}
                   for n, (b, m) in stats["stream"].items()},
                  open(ROOT / "logs/recorder_rates.json", "w"), indent=1)

asyncio.run(main())
