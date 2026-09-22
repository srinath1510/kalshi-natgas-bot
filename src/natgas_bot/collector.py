"""Live collector: order books, trades, settlements, and the Hyperliquid proxy, on one clock.

Every stored row carries recv_ts = local receive time (epoch ms), so keep the machine's
clock NTP-synced. Each loop is independent: an error in one is logged to `heartbeats`
and retried on the next tick without stopping the others.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable

from .backfill import fetch_trades_for
from .config import Config
from .db import DB, now_ms
from .hyperliquid import HyperliquidClient, select_coins
from .kalshi import Book, KalshiClient, Window, parse_market, parse_orderbook

log = logging.getLogger(__name__)


class Collector:
    def __init__(self, cfg: Config, db: DB, kalshi: KalshiClient, hl: HyperliquidClient | None):
        self.cfg = cfg
        self.db = db
        self.kalshi = kalshi
        self.hl = hl
        self.known: dict[str, Window] = {}  # current and upcoming windows, from the tracker
        self.active: dict[str, Window] = {}  # windows whose 15 minutes are running now
        self._last_book: dict[str, Book] = {}
        self._last_book_store: dict[str, int] = {}
        self._announced_coins: set[str] = set()

    # --- loops -----------------------------------------------------------------
    async def track_markets(self) -> None:
        """Refresh the current and upcoming windows.

        Markets are created ~1 day ahead (status "initialized"), while the status=open listing only
        shows a new window 10-45s after it opens. So list by close time and decide what is active
        from each window's own open/close times (see _refresh_active).
        """
        now_s = int(time.time())
        known: dict[str, Window] = {}
        async for m in self.kalshi.iter_markets(
            series_ticker=self.cfg.series_ticker, min_close_ts=now_s, max_close_ts=now_s + self.cfg.tracker_lookahead_s
        ):
            w = parse_market(m)
            self.db.upsert_window(w, m)
            known[w.ticker] = w
        self.known = known
        self._refresh_active()

    def _refresh_active(self) -> None:
        now = now_ms()
        active = {
            t: w for t, w in self.known.items()
            if w.open_ts is not None and w.close_ts is not None and w.open_ts <= now < w.close_ts
        }
        for ticker in active.keys() - self.active.keys():
            log.info("window open: %s target=%s", ticker, active[ticker].target)
        for ticker in self.active.keys() - active.keys():
            log.info("window closed: %s", ticker)
            self._last_book.pop(ticker, None)
            self._last_book_store.pop(ticker, None)
        self.active = active

    async def poll_books(self) -> None:
        self._refresh_active()
        for ticker in list(self.active):
            payload = await self.kalshi.get_orderbook(ticker)
            recv = now_ms()
            book = parse_orderbook(payload)
            changed = self._last_book.get(ticker) != book
            stale = recv - self._last_book_store.get(ticker, 0) >= self.cfg.orderbook_keepalive_s * 1000
            if changed or stale:
                self.db.insert_book(ticker, recv, book)
                self._last_book[ticker] = book
                self._last_book_store[ticker] = recv

    async def poll_trades(self) -> None:
        self._refresh_active()
        for ticker in list(self.active):
            last = self.db.latest_trade_ts(ticker)
            min_ts_s = (last // 1000) - 5 if last else None  # small overlap; inserts are idempotent
            await fetch_trades_for(self.kalshi, self.db, ticker, min_ts_s)

    async def sweep_settlements(self) -> None:
        """Record settlements for recently closed windows and complete their trade tapes."""
        now_s = int(time.time())
        async for m in self.kalshi.iter_markets(
            series_ticker=self.cfg.series_ticker, min_close_ts=now_s - 3 * 3600, max_close_ts=now_s
        ):
            self.db.upsert_window(parse_market(m), m)
        for ticker in self.db.windows_needing_trades(now_ms() - 60_000, now_ms() - 3 * 3600 * 1000):
            if ticker in self.active:
                continue
            await fetch_trades_for(self.kalshi, self.db, ticker)
            self.db.mark_trades_backfilled(ticker)

    async def poll_proxy(self) -> None:
        if self.hl is None:
            return
        for dex in self.cfg.hl_dexes:
            pairs = await self.hl.meta_and_ctxs(dex)
            recv = now_ms()
            selected = select_coins(pairs, self.cfg.hl_coins)
            if not selected and dex not in self._announced_coins:
                self._announced_coins.add(dex)
                log.warning("hyperliquid dex %r: no coin matched %s", dex, self.cfg.hl_coins or "*NATGAS*")
            for name, ctx in selected:
                if name not in self._announced_coins:
                    self._announced_coins.add(name)
                    log.info("hyperliquid proxy coin: %s (oraclePx=%s)", name, ctx.get("oraclePx"))
                self.db.insert_proxy("hyperliquid", name, recv, ctx)

    # --- runner ----------------------------------------------------------------
    async def _loop(self, name: str, interval: float, fn: Callable[[], Awaitable[None]], stop: asyncio.Event) -> None:
        errors = 0
        while not stop.is_set():
            started = time.monotonic()
            try:
                await fn()
                if errors:
                    self.db.heartbeat(name, "recovered", f"after {errors} errors")
                    errors = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # keep every loop alive; the heartbeat table shows gaps
                errors += 1
                log.warning("%s failed: %r", name, exc)
                self.db.heartbeat(name, "error", repr(exc))
            elapsed = time.monotonic() - started
            try:
                await asyncio.wait_for(stop.wait(), timeout=max(0.0, interval - elapsed))
            except asyncio.TimeoutError:
                pass

    async def run(self, stop: asyncio.Event) -> None:
        cfg = self.cfg
        self.db.heartbeat("collector", "start", f"series={cfg.series_ticker}")
        loops = [
            ("tracker", cfg.tracker_interval_s, self.track_markets),
            ("books", cfg.orderbook_interval_s, self.poll_books),
            ("trades", cfg.trades_interval_s, self.poll_trades),
            ("settlements", cfg.settlement_interval_s, self.sweep_settlements),
            ("proxy", cfg.hl_interval_s, self.poll_proxy),
        ]
        tasks = [asyncio.create_task(self._loop(n, i, f, stop), name=n) for n, i, f in loops]
        log.info("collector running (%s); Ctrl+C to stop", ", ".join(n for n, _, _ in loops))
        try:
            await stop.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.db.heartbeat("collector", "stop")
            log.info("collector stopped")
