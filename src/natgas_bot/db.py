"""SQLite storage. All timestamps are epoch milliseconds UTC; prices are dollars."""
from __future__ import annotations

import gzip
import json
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable

from .kalshi import Book, Trade, Window

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS windows (
    ticker TEXT PRIMARY KEY,
    event_ticker TEXT,
    open_ts INTEGER,
    close_ts INTEGER,
    target REAL,
    settle REAL,
    result TEXT,
    status TEXT,
    volume REAL,
    open_interest REAL,
    settlement_ts INTEGER,
    trades_backfilled INTEGER NOT NULL DEFAULT 0,
    raw TEXT,
    updated_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_windows_open ON windows(open_ts);
CREATE INDEX IF NOT EXISTS idx_windows_close ON windows(close_ts);

CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    recv_ts INTEGER NOT NULL,
    best_yes_bid REAL,
    yes_bid_size REAL,
    best_yes_ask REAL,
    yes_ask_size REAL,
    yes_levels TEXT,
    no_levels TEXT
);
CREATE INDEX IF NOT EXISTS idx_books_ticker_ts ON orderbook_snapshots(ticker, recv_ts);

CREATE TABLE IF NOT EXISTS trades (
    trade_id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    created_ts INTEGER,
    yes_price REAL,
    no_price REAL,
    count REAL,
    taker_side TEXT,
    recv_ts INTEGER,
    raw TEXT
);
CREATE INDEX IF NOT EXISTS idx_trades_ticker_ts ON trades(ticker, created_ts);

CREATE TABLE IF NOT EXISTS proxy_ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    coin TEXT NOT NULL,
    recv_ts INTEGER NOT NULL,
    oracle_px REAL,
    mark_px REAL,
    mid_px REAL,
    raw TEXT
);
CREATE INDEX IF NOT EXISTS idx_proxy_coin_ts ON proxy_ticks(coin, recv_ts);

CREATE TABLE IF NOT EXISTS proxy_candles (
    source TEXT NOT NULL,
    coin TEXT NOT NULL,
    open_ts INTEGER NOT NULL,
    close_ts INTEGER,
    o REAL, h REAL, l REAL, c REAL, v REAL,
    PRIMARY KEY (source, coin, open_ts)
);

CREATE TABLE IF NOT EXISTS heartbeats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recv_ts INTEGER NOT NULL,
    component TEXT NOT NULL,
    status TEXT NOT NULL,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_heartbeats_ts ON heartbeats(recv_ts);
"""


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class DB:
    def __init__(self, path: Path | str):
        path = Path(path)
        if str(path) != ":memory:":
            path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path))
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    def backup(self, out_dir: Path | str, keep: int) -> Path:
        """Write a consistent gzipped snapshot (safe while the collector writes) and keep the newest `keep`."""
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        name = time.strftime("natgas-%Y%m%dT%H%M%SZ", time.gmtime())
        raw, gz = out_dir / f"{name}.db.tmp", out_dir / f"{name}.db.gz"
        partial = gz.with_name(gz.name + ".tmp")
        try:
            dst = sqlite3.connect(str(raw))
            try:
                self.conn.backup(dst)
                result = dst.execute("PRAGMA quick_check").fetchone()[0]
                dst.execute("PRAGMA journal_mode=DELETE")  # self-contained file, no -wal sidecar
            finally:
                dst.close()
            if result != "ok":
                raise RuntimeError(f"backup failed quick_check: {result}")
            with open(raw, "rb") as f_in, gzip.open(partial, "wb", compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out, 1 << 20)
            partial.rename(gz)  # only complete backups ever carry the .db.gz name
        finally:
            raw.unlink(missing_ok=True)
            partial.unlink(missing_ok=True)
        for old in sorted(out_dir.glob("natgas-*.db.gz"))[:-keep]:
            old.unlink()
        return gz

    # --- windows -------------------------------------------------------------
    def upsert_window(self, w: Window, raw: dict[str, Any] | None = None) -> None:
        self.conn.execute(
            """
            INSERT INTO windows (ticker, event_ticker, open_ts, close_ts, target, settle, result, status,
                                 volume, open_interest, settlement_ts, raw, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(ticker) DO UPDATE SET
                event_ticker = excluded.event_ticker,
                open_ts = COALESCE(excluded.open_ts, windows.open_ts),
                close_ts = COALESCE(excluded.close_ts, windows.close_ts),
                target = COALESCE(excluded.target, windows.target),
                settle = COALESCE(excluded.settle, windows.settle),
                result = COALESCE(excluded.result, windows.result),
                status = COALESCE(excluded.status, windows.status),
                volume = COALESCE(excluded.volume, windows.volume),
                open_interest = COALESCE(excluded.open_interest, windows.open_interest),
                settlement_ts = COALESCE(excluded.settlement_ts, windows.settlement_ts),
                raw = COALESCE(excluded.raw, windows.raw),
                updated_at = excluded.updated_at
            """,
            (
                w.ticker, w.event_ticker, w.open_ts, w.close_ts, w.target, w.settle, w.result, w.status,
                w.volume, w.open_interest, w.settlement_ts,
                json.dumps(raw, separators=(",", ":")) if raw is not None else None, now_ms(),
            ),
        )
        self.conn.commit()

    def windows(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM windows WHERE open_ts IS NOT NULL ORDER BY open_ts, ticker"
        ).fetchall()

    def windows_needing_trades(self, closed_before_ms: int, closed_after_ms: int | None = None) -> list[str]:
        sql = "SELECT ticker FROM windows WHERE trades_backfilled = 0 AND close_ts IS NOT NULL AND close_ts <= ?"
        args: list[Any] = [closed_before_ms]
        if closed_after_ms is not None:
            sql += " AND close_ts >= ?"
            args.append(closed_after_ms)
        return [r[0] for r in self.conn.execute(sql + " ORDER BY close_ts", args)]

    def mark_trades_backfilled(self, ticker: str) -> None:
        self.conn.execute("UPDATE windows SET trades_backfilled = 1 WHERE ticker = ?", (ticker,))
        self.conn.commit()

    # --- order books & trades ------------------------------------------------
    def insert_book(self, ticker: str, recv_ts: int, book: Book) -> None:
        bid, ask = book.best_yes_bid, book.best_yes_ask
        self.conn.execute(
            """INSERT INTO orderbook_snapshots (ticker, recv_ts, best_yes_bid, yes_bid_size, best_yes_ask,
                                               yes_ask_size, yes_levels, no_levels)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                ticker, recv_ts,
                bid[0] if bid else None, bid[1] if bid else None,
                ask[0] if ask else None, ask[1] if ask else None,
                json.dumps(book.yes), json.dumps(book.no),
            ),
        )
        self.conn.commit()

    def insert_trades(self, trades: Iterable[tuple[Trade, dict[str, Any]]], recv_ts: int) -> int:
        rows = [
            (t.trade_id, t.ticker, t.created_ts, t.yes_price, t.no_price, t.count, t.taker_side, recv_ts,
             json.dumps(raw, separators=(",", ":")))
            for t, raw in trades
        ]
        if not rows:
            return 0
        before = self.conn.total_changes
        self.conn.executemany(
            """INSERT OR IGNORE INTO trades (trade_id, ticker, created_ts, yes_price, no_price, count,
                                            taker_side, recv_ts, raw) VALUES (?,?,?,?,?,?,?,?,?)""",
            rows,
        )
        self.conn.commit()
        return self.conn.total_changes - before

    def latest_trade_ts(self, ticker: str) -> int | None:
        row = self.conn.execute("SELECT MAX(created_ts) FROM trades WHERE ticker = ?", (ticker,)).fetchone()
        return row[0] if row else None

    # --- proxy -----------------------------------------------------------------
    def insert_proxy(self, source: str, coin: str, recv_ts: int, ctx: dict[str, Any]) -> None:
        from .kalshi import num

        self.conn.execute(
            "INSERT INTO proxy_ticks (source, coin, recv_ts, oracle_px, mark_px, mid_px, raw) VALUES (?,?,?,?,?,?,?)",
            (source, coin, recv_ts, num(ctx.get("oraclePx")), num(ctx.get("markPx")), num(ctx.get("midPx")),
             json.dumps(ctx, separators=(",", ":"))),
        )
        self.conn.commit()

    def insert_candles(self, source: str, coin: str, candles: list[dict[str, Any]]) -> int:
        from .kalshi import num

        rows = [
            (source, coin, int(c["t"]), int(c["T"]) if c.get("T") is not None else None,
             num(c.get("o")), num(c.get("h")), num(c.get("l")), num(c.get("c")), num(c.get("v")))
            for c in candles
            if c.get("t") is not None
        ]
        before = self.conn.total_changes
        self.conn.executemany(
            """INSERT OR REPLACE INTO proxy_candles (source, coin, open_ts, close_ts, o, h, l, c, v)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            rows,
        )
        self.conn.commit()
        return self.conn.total_changes - before

    def proxy_tick_at_or_before(self, coin: str, ts: int, max_lag_ms: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """SELECT * FROM proxy_ticks WHERE coin = ? AND recv_ts <= ? AND recv_ts >= ?
               ORDER BY recv_ts DESC LIMIT 1""",
            (coin, ts, ts - max_lag_ms),
        ).fetchone()

    def proxy_coins(self) -> list[str]:
        return [r[0] for r in self.conn.execute("SELECT DISTINCT coin FROM proxy_ticks ORDER BY coin")]

    def candle_coins(self) -> list[str]:
        return [r[0] for r in self.conn.execute("SELECT DISTINCT coin FROM proxy_candles ORDER BY coin")]

    def candle_ending_at(self, coin: str, close_ts: int) -> sqlite3.Row | None:
        # A 1m candle opening at close_ts - 60s closes at close_ts (matches Kalshi's candle convention).
        return self.conn.execute(
            "SELECT * FROM proxy_candles WHERE coin = ? AND open_ts = ?", (coin, close_ts - 60_000)
        ).fetchone()

    # --- ops -----------------------------------------------------------------------
    def heartbeat(self, component: str, status: str, detail: str = "") -> None:
        self.conn.execute(
            "INSERT INTO heartbeats (recv_ts, component, status, detail) VALUES (?,?,?,?)",
            (now_ms(), component, status, detail[:2000]),
        )
        self.conn.commit()

    def counts(self) -> dict[str, int]:
        tables = ["windows", "orderbook_snapshots", "trades", "proxy_ticks", "proxy_candles", "heartbeats"]
        return {t: self.conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables}
