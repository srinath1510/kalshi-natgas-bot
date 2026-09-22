"""Historical backfill: settled windows, their trades, and Hyperliquid proxy candles."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from .db import DB, now_ms
from .hyperliquid import HyperliquidClient, select_coins
from .kalshi import KalshiClient, parse_market, parse_trade

log = logging.getLogger(__name__)

DAY_S = 86_400


async def backfill_windows(
    kalshi: KalshiClient,
    db: DB,
    series: str,
    since: datetime | None = None,
    until: datetime | None = None,
    chunk_days: int = 1,
    empty_days_stop: int = 14,
) -> int:
    """Walk backwards from `until` in day-sized chunks, upserting every window in the series.

    Without `since`, stops after `empty_days_stop` consecutive empty days (weekends are
    only ~1.75 days, so 14 comfortably means we've passed the series launch).
    """
    end_s = int((until or datetime.now(timezone.utc)).timestamp())
    since_s = int(since.timestamp()) if since else None
    total = 0
    empty_streak = 0
    while True:
        if since_s is not None and end_s <= since_s:
            break
        start_s = end_s - chunk_days * DAY_S
        if since_s is not None:
            start_s = max(start_s, since_s)
        n = 0
        async for m in kalshi.iter_markets(series_ticker=series, min_close_ts=start_s, max_close_ts=end_s):
            db.upsert_window(parse_market(m), m)
            n += 1
        total += n
        empty_streak = 0 if n else empty_streak + 1
        log.info(
            "windows %s -> %s: %d (total %d)",
            datetime.fromtimestamp(start_s, timezone.utc).isoformat(timespec="minutes"),
            datetime.fromtimestamp(end_s, timezone.utc).isoformat(timespec="minutes"),
            n, total,
        )
        if since_s is None and empty_streak >= empty_days_stop:
            log.info("stopping after %d consecutive empty days", empty_streak)
            break
        end_s = start_s
    return total


async def fetch_trades_for(kalshi: KalshiClient, db: DB, ticker: str, min_ts_s: int | None = None) -> int:
    batch = []
    async for raw in kalshi.iter_trades(ticker, min_ts=min_ts_s):
        batch.append((parse_trade(raw), raw))
    return db.insert_trades(batch, now_ms())


async def backfill_trades(
    kalshi: KalshiClient, db: DB, closed_after_ms: int | None = None, settle_grace_ms: int = 60_000
) -> int:
    """Fetch the full trade tape for every closed window not yet marked complete."""
    tickers = db.windows_needing_trades(now_ms() - settle_grace_ms, closed_after_ms)
    total = 0
    for i, ticker in enumerate(tickers, 1):
        total += await fetch_trades_for(kalshi, db, ticker)
        db.mark_trades_backfilled(ticker)
        if i % 50 == 0:
            log.info("trades: %d/%d windows, %d new trades", i, len(tickers), total)
    log.info("trades: done, %d windows, %d new trades", len(tickers), total)
    return total


async def backfill_proxy_candles(
    hl: HyperliquidClient, db: DB, dexes: tuple[str, ...], coins: tuple[str, ...], days: float = 3.4
) -> dict[str, int]:
    """1m trade-price candles for the Hyperliquid NATGAS perp(s). HL keeps ~5000 recent candles."""
    selected: list[str] = []
    for dex in dexes:
        pairs = await hl.meta_and_ctxs(dex)
        selected += [name for name, _ in select_coins(pairs, coins)]
    if not selected:
        log.warning("no Hyperliquid coins matched (dexes=%s, coins=%s)", dexes, coins or "auto NATGAS")
        return {}
    end = now_ms()
    start = end - int(days * DAY_S * 1000)
    out: dict[str, int] = {}
    for coin in selected:
        n = 0
        chunk_start = start
        while chunk_start < end:
            chunk_end = min(chunk_start + DAY_S * 1000, end)
            n += db.insert_candles("hyperliquid", coin, await hl.candles(coin, chunk_start, chunk_end))
            chunk_start = chunk_end
        out[coin] = n
        log.info("proxy candles %s: %d", coin, n)
    return out
