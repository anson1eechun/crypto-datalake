# crypto-datalake

加密貨幣「API 拿不到」的歷史與即時微結構資料庫。兩層來源、一套 SQL。

## 為什麼需要這個

Binance REST API 有硬限制（funding rate 約 67 天、OI 約 31 天），逐筆與訂單簿資料更是完全拿不到。
本專案用兩條路徑補齊：

1. **回填層** — `data.binance.vision` 官方公開 dump（免費、免 key），逐筆成交／秒級 K 線／OI／資金費最遠回到 2020-01。
2. **錄製層** — 自架 WebSocket 常駐錄製。**這層錯過的每一天都補不回來**，因為 Binance 在 2024-04 之後就停止發布 bookTicker dump，強平資料則已完全下架。

## 已驗證的來源限制（2026-09-06 實測）

| 資料 | 可得性 |
|---|---|
| `um/trades`、`um/aggTrades`、`spot/*` 逐筆 | ✅ dump 2020-01 起 |
| `spot/klines` 1s 秒級 K 線 | ✅ dump |
| `um/metrics`（OI、多空比、taker 比，5 分鐘） | ✅ dump 2020-09 起 |
| `um/bookTicker`（最佳報價逐筆） | ⚠️ dump **僅 2023-05 → 2024-04**，之後只能自錄 |
| `um/bookDepth`（每分鐘 % 距離深度） | ✅ dump，僅日檔 |
| `liquidationSnapshot`（強平） | ❌ dump 已下架；WS `!forceOrder@arr` 無推送；REST 需 API key |
| Binance UM WebSocket | ⚠️ `@aggTrade`／`@markPrice`／`@kline_*`／`!*@arr` **訂閱不報錯但零推送**；可用的只有 `@trade`、`@bookTicker`、`@depth` |
| 完整 L2 增量歷史 | ❌ 免費無來源（Tardis.dev 等付費，或自錄） |

## 架構

```
本機 NVMe                          NAS /srv/nas/marketdata（ext4，10.9T）
─────────                          ──────────────────────────────────────
spool/            ──整點封檔搬移──▶  recorded/   自錄原始 jsonl.zst
state/manifest.sqlite  下載帳本      vision/     官方 dump 原封 zip（路徑鏡射遠端 key）
state/market.duckdb    查詢目錄      parquet/    正規化層（DuckDB 視圖指向這裡）
```

高頻寫入先落 NVMe spool，整點封檔後才搬上 NAS。DuckDB 與 SQLite 放在 NVMe。

NAS 原本是 NTFS（ntfs3），大量寫入和兩次機器硬鎖死有關，2026-09 用 `tools/migrate_to_ext4.sh` 整顆改成 ext4：先逐項比對來源與副本的檔案數、位元組數，全部對上才允許格式化，磁碟一律用 UUID 認。

## 目錄

| 路徑 | 內容 |
|---|---|
| `recorder/record.py` | WebSocket 常駐錄製：`@trade`、`@bookTicker`、`@depth` 增量，每 15 分鐘 REST 深度快照，加上資金費與 OI |
| `backfill/vision.py` | 用 S3 listing 探索 data.binance.vision 實際存在的檔，斷點續傳下載並用官方 `.CHECKSUM` 驗 SHA256；全域限速避免搶走錄製器頻寬 |
| `catalog/normalize.py` | vision CSV zip → hive 分區 parquet；處理有無標頭、毫秒改微秒、數 GB 月檔串流轉檔 |
| `catalog/normalize_recorded.py` | 自錄時檔依 stream 拆成 parquet，depth 的 bids/asks 保留巢狀 list |
| `catalog/db.py` | 建 DuckDB 視圖，`status` 與 `sql` 子命令 |
| `research/` | 訂單簿重建驗證、訂單流牆偵測與證偽、footprint/CVD 檢定（見下） |
| `tools/` | 每小時維護、回填啟動、ext4 遷移、壞扇區對應檔案（`fiemap_scan.py`）、匯出給 factor-lab |
| `config/` | 錄製與回填的幣種清單 |
| `deploy/` | systemd user units：錄製器與每小時維護 timer |

## 常駐服務

```bash
systemctl --user status md-recorder     # 錄製器（Restart=always, MemoryMax=3G）
systemctl --user list-timers md-maintain # 每小時正規化 + 更新目錄
tail -f logs/recorder.log logs/backfill.log logs/maintain.log
```

## 查詢

```bash
uv run catalog/db.py status                     # 各表列數與時間範圍
uv run catalog/db.py sql "SELECT * FROM um_metrics LIMIT 5"
```

視圖是 glob 不是快照，新資料轉檔後自動出現。

| 視圖 | 內容 |
|---|---|
| `um_metrics` | OI、多空帳戶比、頂級持倉比、taker 買賣比（5 分鐘） |
| `um_klines_1m` / `um_mark_1m` / `spot_klines_1s` | K 線 |
| `um_trades` / `um_agg_trades` / `spot_agg_trades` | 逐筆成交（`trades` 為每筆，`aggTrades` 為同價同向合併）|
| `um_book_ticker` / `um_book_depth` | 歷史最佳報價、深度剖面 |
| `um_funding` | 資金費率 |
| `rec_trade` / `rec_book_ticker` | 自錄逐筆與最佳報價 |
| `rec_depth` / `rec_depth_snap` | 自錄 L2 增量與 REST 快照 |
| `rec_open_interest` / `rec_premium_index` | 自錄 OI、標記價、資金費 |

### 重建訂單簿

`rec_depth` 是**增量**，單獨無法還原訂單簿，必須用 `rec_depth_snap`（每 15 分鐘的 REST 全深度快照）當錨點：
取一筆 `last_update_id` 落在增量區間內的快照，之後依 `first_id`/`final_id`/`prev_final_id`
序列鏈往後套用。錄製時已驗證此鏈完整且快照可接續。

## 手動操作

```bash
uv run backfill/vision.py index                 # 重掃遠端有哪些檔（只讀）
uv run backfill/vision.py status                # 覆蓋率與待下載量
uv run backfill/vision.py fetch --dataset um/trades --jobs 4
uv run catalog/normalize.py --dataset um/trades --jobs 4
uv run catalog/normalize_recorded.py --date 2026-09-06
uv run tools/pilot_measure.py --seconds 300     # 重測串流頻寬
```

## 研究：訂單流牆與 footprint

`research/` 用這個資料湖檢驗一套交易影片的主張：大掛單（牆）被吃穿、被撤走、或守住之後，價格會怎麼走。流程刻意做成「先證偽、再回測」：

1. `orderbook.py`：用 L2 增量重建訂單簿，並用下一個獨立 REST 快照驗證重建正確。對不上就不往下做。
2. `flow.py`：以**價格分桶**認定牆（逐檔門檻會被各幣 tick 密度綁死，無法跨幣比較），把牆的生命週期分成兩個正交軸：行為（persistent / fresh / reloading / iceberg）與消失方式（consumed / pulled / partial / shrank）。
3. `verify_touch.py`：用 bookTicker 這條獨立資料流檢查價格是否真的走到牆，排除成交歸戶寫錯。
4. `falsify.py`：牆事件後的前瞻報酬對照同時段隨機時點。
5. `context_tests.py`：執行 [`PREREGISTRATION.md`](research/PREREGISTRATION.md) 鎖定的 15 個檢定（5 假說 × 3 前瞻窗，BH 控制 FDR 0.10）。
6. `build_bars.py` + `footprint_tests.py`：把 74 幣、約 500 億筆逐筆成交聚合成 1 分鐘 bar，執行 [`PREREGISTRATION_FOOTPRINT.md`](research/PREREGISTRATION_FOOTPRINT.md) 的 12 個檢定（IS 2019-09～2024-12、OOS 2025 起，以日為區塊 bootstrap）。

所有檢定格子都在看結果之前寫死在預註冊文件裡。

`tools/export_to_factorlab.py` 把衍生品指標匯出成 [factor-lab](https://github.com/anson1eechun/factor-lab) 的格式，口徑與 factor-lab 原本的抓取腳本逐欄一致，但期間更長（2020-09 起）、幣種更多。

## 設計取捨

- **月檔優先、日檔只補尾巴**：全部用日檔會產生上百萬個小檔；月檔約 3.5 萬個。
- **小日檔按 symbol-年合併成單一 parquet**：逐日轉 parquet 反而比 zip 大（檔尾中繼資料佔比過高）。
- **`um/trades` 與 `um/aggTrades` 都收**：兩者資訊重疊但不等價——`trades` 是每一筆成交，
  `aggTrades` 額外帶 `agg_trade_id` 與 `first/last_trade_id` 聚合區間，省去自行重算合併的成本。
- **保留原始檔不刪**：`vision/` 路徑鏡射遠端 key，任何一筆都能回推來源並用官方 CHECKSUM 重新驗證。
- **錄製原始 JSON 不預先解析**：格式如有變動，重跑正規化即可，不必重錄。
- **每 60 秒封一個 zstd frame**：程序被強制中止時只損失最後一段，前面照樣解得開。

## 不在 repo 裡的東西

`state/`（DuckDB、manifest）、`spool/`、`logs/`、`research/events/` 以及 NAS 上的所有資料都不收錄。資料路徑由環境變數 `MD_ROOT` 決定（預設 `/srv/nas/marketdata`），部分腳本仍寫死 `/home/anson/...`。

執行只需要 [uv](https://docs.astral.sh/uv/)：每支腳本開頭有 PEP 723 inline 相依宣告，`uv run <script>` 會自動裝好。
