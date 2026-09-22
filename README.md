# kalshi-natgas-bot

Data collection and research tooling for Kalshi's **KXNATGAS15M** series: 15-minute
"Natural Gas price up?" contracts that settle on Pyth's 24/7 NATGAS index.
The collector also records the other 15-minute commodity series (KXGOLD15M, KXWTI15M, KXSILVER15M,
KXCOPPER15M, KXPLATINUM15M, KXPALLADIUM15M), which use the same contract template on their own Pyth indices.

Everything here is **read-only public data**. No API keys, no order placement.

## What the contract is (verified 2026-09-22)

| | |
|---|---|
| Target | Pyth `Commodities.Index.NATGAS/USD` price at the window's open (1-min candle close) |
| Settlement | Pyth NATGAS price at the window's close, rounded to 5 decimals, `>=` target resolves YES |
| Chaining | each window's settlement is exactly the next window's target |
| Book | markets are created ~1 day ahead (`initialized`) and open empty at window start; the target is published at open |
| Schedule | Sun 18:00 ET to Sat 00:00 ET, including the CME 17:00-18:00 halt and Friday evening (synthetic index); no windows Thu 03:00-05:00 ET (Kalshi maintenance) |
| Fallbacks | missing data: last published price; corrections within 24h and delayed prints within 1 week are used |
| Settlement timing | posts ~18s after close |
| Fees | standard quadratic taker fee, multiplier 1 |

## Setup

```bash
git clone https://github.com/srinath1510/kalshi-natgas-bot && cd kalshi-natgas-bot
uv sync --extra dev        # creates .venv with httpx + pytest
uv run pytest              # offline tests, no network needed
```

## Commands

```bash
# 1. Backfill every settled window of every configured series since launch (walks back until 14 empty days)
uv run natgas-bot backfill

#    ...optionally with each window's full trade tape and ~3.4 days of Hyperliquid NATGAS 1m candles
uv run natgas-bot backfill --trades --proxy-candles

#    ...or a bounded range (UTC dates)
uv run natgas-bot backfill --since 2026-09-01 --until 2026-09-23

# 2. Run the live collector (Ctrl+C to stop). On a Mac, prevent sleep while it runs:
caffeinate -i uv run natgas-bot collect

# 3. Report: chaining, ties, gaps, session stats, proxy error. Optional per-window CSV.
uv run natgas-bot verify --csv data/windows.csv            # default --series KXNATGAS15M
uv run natgas-bot verify --series KXGOLD15M

uv run natgas-bot status   # row counts

# 4. Consistent gzipped snapshot of the DB (safe while the collector runs), keeping the newest 3
uv run natgas-bot backup --out backups --keep 3
```

## What the collector records

| Loop | Every | Table | Notes |
|---|---|---|---|
| tracker | 5s | `windows` | lists current + upcoming windows by close time; a window is active from its `open_time` (checked every books/trades tick), so books start ~1s after open |
| books | 1s | `orderbook_snapshots` | stored when the book changes, or every 15s if unchanged |
| trades | 2s | `trades` | incremental by timestamp, deduped by `trade_id` |
| settlements | 60s | `windows`, `trades` | records settlements, then fetches each closed window's full tape |
| proxy | 2s | `proxy_ticks` | Hyperliquid `xyz` proxies for every series (NATGAS, GOLD, CL, SILVER, COPPER, PLATINUM, PALLADIUM; one request) |

Every row has `recv_ts` (local receive time, epoch ms UTC), so keep the clock NTP-synced.
A failing loop logs to `heartbeats` and retries; the others keep running.
Storage is roughly 1 GB/week for NATGAS alone and ~7 GB/week for all 7 series, dominated by order-book snapshots and trades.

## Report sections (`verify`)

- **CHAINING**: target(n) == settle(n-1) for contiguous windows. Any mismatch is worth investigating.
- **TIES**: settle == target. Resolves YES; a run of them suggests a frozen feed.
- **GAPS**: missing windows outside the scheduled breaks (Saturday-Sunday, Thursday 03:00-05:00 ET maintenance); holidays and collector downtime show up here.
- **SESSIONS**: 15-minute move sizes, YES rate and volume for `day`, `eia`, `offpeak`, `cme_break`,
  `friday_evening`, `sunday_reopen` (definitions in `src/natgas_bot/sessions.py`).
- **PROXY ERROR**: Hyperliquid oracle (and candle close) minus Kalshi settlement, in cents, by session.
  This decides whether the free proxy is good enough or Pyth access is worth paying for.

## Configuration

All optional, via environment variables (see `.env.example`): DB path, Kalshi base URL, series,
Hyperliquid dexes/coins, and every poll interval. Kalshi requests are evenly spaced at
`KALSHI_MAX_RPS` (default 15/s; live polling of 7 series needs ~12/s; Kalshi returns 429s on bursts,
which are retried). Hyperliquid: 30 of 60 allowed info calls/min.

## Known unknowns

- Hyperliquid coin name is auto-discovered (`*NATGAS*` on dex `xyz`); set `HL_COINS` to pin it.
  Its oracle source is unverified; the proxy report measures it.
- NG futures feed: not wired in yet (vendor not chosen).
- If `backfill` stops earlier than expected, Kalshi may serve older markets from a separate
  historical endpoint; check `natgas-bot verify` for the date range actually covered.

## Layout

```
src/natgas_bot/
  config.py       env-driven settings
  http_util.py    retries with backoff (429/5xx/network)
  kalshi.py       public REST client + parsers (legacy cents and fixed-point dollar formats)
  hyperliquid.py  info API client (metaAndAssetCtxs, candleSnapshot)
  db.py           SQLite schema and writes (WAL)
  sessions.py     ET session classification and scheduled breaks
  backfill.py     windows / trades / proxy candles history
  collector.py    live async collector
  verify.py       checks, stats, CSV export
  cli.py          `natgas-bot` entry point
scripts/          optional laptop-side backup pull (host set in a local, untracked config file)
tests/            offline tests using real windows from 2026-09-18..22
```
