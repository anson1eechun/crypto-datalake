#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow", "zstandard"]
# ///
"""把自錄的 jsonl.zst 時檔拆成各 stream 的 parquet。

原始檔一行 = <接收時間 ns>\\t<原始 JSON>，一個檔混合所有 symbol 與 stream。
這裡按 stream 種類拆表；訂單簿增量的 b/a 保留成巢狀 list（展平會爆量，DuckDB 查得動巢狀）。

  uv run catalog/normalize_recorded.py [--date 2026-09-06] [--jobs 4]
"""
import argparse, json, os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

BASE = Path(os.environ.get("MD_ROOT", "/srv/nas/marketdata"))
SRC, OUT = BASE / "recorded", BASE / "parquet/recorded"

# 每個 kind 累積到這個列數就落一次 row group，避免整小時堆在記憶體裡
FLUSH_ROWS = {"depth": 50_000, "depthSnapshot": 2_000}
DEFAULT_FLUSH = 250_000

LVL = pa.list_(pa.list_(pa.string()))
SCHEMAS = {
    "trade": pa.schema([("recv_ns", pa.int64()), ("event_ms", pa.int64()),
                        ("trade_ms", pa.int64()), ("symbol", pa.string()),
                        ("trade_id", pa.int64()), ("price", pa.float64()),
                        ("qty", pa.float64()), ("is_buyer_maker", pa.bool_()),
                        ("order_type", pa.string())]),
    "bookTicker": pa.schema([("recv_ns", pa.int64()), ("event_ms", pa.int64()),
                             ("trans_ms", pa.int64()), ("symbol", pa.string()),
                             ("update_id", pa.int64()), ("bid", pa.float64()),
                             ("bid_qty", pa.float64()), ("ask", pa.float64()),
                             ("ask_qty", pa.float64())]),
    "depth": pa.schema([("recv_ns", pa.int64()), ("event_ms", pa.int64()),
                        ("trans_ms", pa.int64()), ("symbol", pa.string()),
                        ("first_id", pa.int64()), ("final_id", pa.int64()),
                        ("prev_final_id", pa.int64()), ("bids", LVL), ("asks", LVL)]),
    "depthSnapshot": pa.schema([("recv_ns", pa.int64()), ("symbol", pa.string()),
                                ("last_update_id", pa.int64()), ("msg_ms", pa.int64()),
                                ("trans_ms", pa.int64()), ("bids", LVL), ("asks", LVL)]),
    "openInterest": pa.schema([("recv_ns", pa.int64()), ("symbol", pa.string()),
                               ("open_interest", pa.float64()), ("time_ms", pa.int64())]),
    "premiumIndex": pa.schema([("recv_ns", pa.int64()), ("symbol", pa.string()),
                               ("mark_price", pa.float64()), ("index_price", pa.float64()),
                               ("last_funding_rate", pa.float64()),
                               ("next_funding_ms", pa.int64()), ("time_ms", pa.int64())]),
}


def _f(x):
    return float(x) if x is not None else None


def parse_line(recv_ns, o, acc):
    st = o.get("stream", "")
    d = o.get("data")
    if st.endswith("@depthSnapshot"):
        acc["depthSnapshot"].append((recv_ns, st.split("@")[0].upper(),
                                     d.get("lastUpdateId"), d.get("E"), d.get("T"),
                                     d.get("bids"), d.get("asks")))
    elif st.endswith("@openInterest"):
        acc["openInterest"].append((recv_ns, d.get("symbol"),
                                    _f(d.get("openInterest")), d.get("time")))
    elif st == "!premiumIndex":
        for r in d:
            acc["premiumIndex"].append((recv_ns, r.get("symbol"), _f(r.get("markPrice")),
                                        _f(r.get("indexPrice")), _f(r.get("lastFundingRate")),
                                        r.get("nextFundingTime"), r.get("time")))
    elif st.endswith("@trade"):
        acc["trade"].append((recv_ns, d.get("E"), d.get("T"), d.get("s"), d.get("t"),
                             _f(d.get("p")), _f(d.get("q")), d.get("m"), d.get("X")))
    elif st.endswith("@bookTicker"):
        acc["bookTicker"].append((recv_ns, d.get("E"), d.get("T"), d.get("s"), d.get("u"),
                                  _f(d.get("b")), _f(d.get("B")),
                                  _f(d.get("a")), _f(d.get("A"))))
    elif "@depth" in st:
        acc["depth"].append((recv_ns, d.get("E"), d.get("T"), d.get("s"), d.get("U"),
                             d.get("u"), d.get("pu"), d.get("b"), d.get("a")))


class KindWriter:
    """單一 stream 種類的漸進式 parquet 寫入；緩衝滿了就落一個 row group。"""

    def __init__(self, kind, path):
        self.kind, self.path = kind, path
        self.schema = SCHEMAS[kind]
        self.limit = FLUSH_ROWS.get(kind, DEFAULT_FLUSH)
        self.buf = []
        self.rows = 0
        self.w = None
        self.tmp = path.with_suffix(".part")

    def add(self, row):
        self.buf.append(row)
        if len(self.buf) >= self.limit:
            self.flush()

    def flush(self):
        if not self.buf:
            return
        sch = self.schema
        tbl = pa.Table.from_arrays(
            [pa.array([r[i] for r in self.buf], type=sch.field(i).type)
             for i in range(len(sch))], schema=sch)
        if self.w is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.w = pq.ParquetWriter(self.tmp, sch, compression="zstd",
                                      compression_level=6)
        self.w.write_table(tbl)
        self.rows += tbl.num_rows
        self.buf = []

    def close(self):
        self.flush()
        if self.w is None:
            return 0
        self.w.close()
        os.replace(self.tmp, self.path)
        return self.rows

    def abort(self):
        if self.w is not None:
            try:
                self.w.close()
            except Exception:
                pass
        Path(self.tmp).unlink(missing_ok=True)


def convert_hour(job):
    date, hour, files = job
    outs = {k: OUT / k / f"date={date}" / f"hour={hour}.parquet" for k in SCHEMAS}
    if all(p.exists() for p in outs.values()):
        return date, hour, "exists", 0
    writers = {}
    acc = defaultdict(list)
    dctx = zstd.ZstdDecompressor()
    nbad = 0

    def drain():
        for kind, rows in acc.items():
            if not rows:
                continue
            w = writers.get(kind)
            if w is None:
                w = writers[kind] = KindWriter(kind, outs[kind])
            for r in rows:
                w.add(r)
            rows.clear()

    try:
        for f in files:
            with open(f, "rb") as fh, dctx.stream_reader(fh) as r:
                for i, line in enumerate(io_lines(r)):
                    try:
                        ts, _, raw = line.partition(b"\t")
                        parse_line(int(ts), json.loads(raw), acc)
                    except Exception:
                        nbad += 1
                    if (i & 0xFFFF) == 0xFFFF:     # 每 65536 行把緩衝倒進 writer
                        drain()
            drain()
        n = sum(w.close() for w in writers.values())
    except Exception as e:
        for w in writers.values():
            w.abort()
        return date, hour, f"err:{type(e).__name__}:{str(e)[:60]}", 0
    return date, hour, f"done(壞行 {nbad})" if nbad else "done", n


def io_lines(reader, chunk=1 << 22):
    buf = b""
    while True:
        b = reader.read(chunk)
        if not b:
            break
        buf += b
        *lines, buf = buf.split(b"\n")
        yield from lines
    if buf:
        yield buf


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date")
    ap.add_argument("--jobs", type=int, default=3)
    a = ap.parse_args()
    groups = defaultdict(list)
    for p in SRC.rglob("*.jsonl.zst"):
        parts = {k: v for k, v in (s.split("=", 1) for s in p.parts if "=" in s)}
        d, h = parts.get("date"), parts.get("hour")
        if d and h and (not a.date or d == a.date):
            groups[(d, h)].append(p)
    jobs = [(d, h, sorted(f)) for (d, h), f in sorted(groups.items())]
    print(f"待處理 {len(jobs)} 個小時檔組", flush=True)
    with ProcessPoolExecutor(a.jobs) as ex:
        for d, h, st, n in ex.map(convert_hour, jobs):
            print(f"  {d} {h}時  {st}  {n/1e6:.2f}M 列", flush=True)


if __name__ == "__main__":
    main()
