#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["websockets"]
# ///
"""量測 Binance UM 各 stream 的實際位元組率，用來決定 recorder 要收哪些 symbol。

輸出每個 (symbol, stream) 的 bytes/s 與 msg/s，換算成 GB/日（原始）與 zstd 壓縮後估計。
跑: uv run tools/pilot_measure.py --seconds 600
"""
import argparse, asyncio, json, time, collections, sys
from pathlib import Path
import websockets

WS = "wss://fstream.binance.com/stream"
ROOT = Path(__file__).resolve().parent.parent


async def one_conn(streams, stats, stop_at, cid):
    """一條連線收一批 stream，累加位元組。斷線自動重連直到 stop_at。"""
    while time.time() < stop_at:
        try:
            async with websockets.connect(WS, ping_interval=20, max_size=None) as ws:
                # 分批 SUBSCRIBE，避免單一訊息過大被拒
                for i in range(0, len(streams), 50):
                    await ws.send(json.dumps({"method": "SUBSCRIBE",
                                              "params": streams[i:i + 50], "id": i}))
                    await asyncio.sleep(0.25)
                while time.time() < stop_at:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    n = len(raw)
                    # 只 peek stream 名稱，不做完整 json parse（省 CPU）
                    j = raw.find('"stream":"')
                    if j < 0:
                        continue
                    k = raw.find('"', j + 10)
                    name = raw[j + 10:k]
                    s = stats[name]
                    s[0] += n
                    s[1] += 1
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            print(f"[conn{cid}] 重連: {type(e).__name__} {e}", file=sys.stderr)
            await asyncio.sleep(2)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=600)
    ap.add_argument("--symbols", default=str(ROOT / "config/um_live.txt"))
    ap.add_argument("--out", default=str(ROOT / "logs/pilot_rates.json"))
    a = ap.parse_args()

    syms = [s.strip().lower() for s in Path(a.symbols).read_text().split() if s.strip()]
    kinds = ["aggTrade", "bookTicker", "depth@100ms"]
    streams = [f"{s}usdt@{k}" for s in syms for k in kinds]
    print(f"symbols={len(syms)} streams={len(streams)} 量測 {a.seconds}s", flush=True)

    stats = collections.defaultdict(lambda: [0, 0])
    stop_at = time.time() + a.seconds
    # 每條連線 <=120 streams，降低單點斷線影響
    chunks = [streams[i:i + 120] for i in range(0, len(streams), 120)]
    t0 = time.time()
    await asyncio.gather(*(one_conn(c, stats, stop_at, i) for i, c in enumerate(chunks)))
    dur = time.time() - t0

    per_kind = collections.defaultdict(lambda: [0, 0])
    rows = []
    for name, (nbytes, nmsg) in stats.items():
        sym, kind = name.split("@", 1)
        per_kind[kind][0] += nbytes
        per_kind[kind][1] += nmsg
        rows.append({"stream": name, "symbol": sym, "kind": kind,
                     "bytes_per_s": nbytes / dur, "msg_per_s": nmsg / dur,
                     "gb_per_day_raw": nbytes / dur * 86400 / 1e9})

    rows.sort(key=lambda r: -r["bytes_per_s"])
    out = {"duration_s": dur, "n_symbols": len(syms), "streams": rows,
           "by_kind": {k: {"gb_per_day_raw": v[0] / dur * 86400 / 1e9,
                           "msg_per_s": v[1] / dur} for k, v in per_kind.items()}}
    Path(a.out).write_text(json.dumps(out, indent=1))

    print(f"\n實測 {dur:.0f}s ── 全 universe 合計（未壓縮）")
    tot = 0.0
    for k, v in sorted(out["by_kind"].items(), key=lambda x: -x[1]["gb_per_day_raw"]):
        tot += v["gb_per_day_raw"]
        print(f"  {k:14s} {v['gb_per_day_raw']:8.1f} GB/日  {v['msg_per_s']:9.0f} msg/s")
    print(f"  {'合計':14s} {tot:8.1f} GB/日  → zstd 約 {tot*0.18:.1f} GB/日, "
          f"{tot*0.18*365/1000:.2f} TB/年")
    print("\n最耗流量前 15 個 stream:")
    for r in rows[:15]:
        print(f"  {r['stream']:26s} {r['gb_per_day_raw']:7.2f} GB/日  {r['msg_per_s']:7.1f} msg/s")
    print(f"\n明細 -> {a.out}")

asyncio.run(main())
