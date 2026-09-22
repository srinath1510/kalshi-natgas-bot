import asyncio
from datetime import datetime, timezone

import httpx

from natgas_bot.backfill import backfill_proxy_candles, backfill_trades, backfill_windows
from natgas_bot.hyperliquid import HyperliquidClient
from natgas_bot.kalshi import KalshiClient

from conftest import REAL_WINDOWS
from mock_api import FakeHyperliquid, FakeKalshi, router

BASE = "https://api.elections.kalshi.com/trade-api/v2"
HL = "https://api.hyperliquid.xyz/info"


def run(coro):
    return asyncio.run(coro)


async def _backfill(fake, db, **kw):
    async with httpx.AsyncClient(transport=router(fake)) as http:
        return await backfill_windows(KalshiClient(http, BASE, base_delay=0), db, "KXNATGAS15M", **kw)


def test_backfill_windows_with_bounds_and_pagination(db, real_markets):
    fake = FakeKalshi(real_markets, page_size=5)
    n = run(_backfill(fake, db, since=datetime(2026, 9, 18, tzinfo=timezone.utc),
                      until=datetime(2026, 9, 23, tzinfo=timezone.utc)))
    rows = db.windows()
    assert len(rows) == len(REAL_WINDOWS)
    assert n >= len(REAL_WINDOWS)  # chunk boundaries may see a window twice; upsert dedupes
    assert any(p.get("cursor") for _, p in fake.calls)  # pagination exercised


def test_backfill_stops_after_empty_streak(db, real_markets):
    fake = FakeKalshi(real_markets)
    run(_backfill(fake, db, until=datetime(2026, 9, 23, tzinfo=timezone.utc), empty_days_stop=3))
    assert len(db.windows()) == len(REAL_WINDOWS)
    market_calls = [c for c in fake.calls if c[0] == "/markets"]
    assert len(market_calls) < 40  # walked back a few empty days, then stopped


def test_backfill_idempotent(db, real_markets):
    fake = FakeKalshi(real_markets)
    kw = dict(since=datetime(2026, 9, 18, tzinfo=timezone.utc), until=datetime(2026, 9, 23, tzinfo=timezone.utc))
    run(_backfill(fake, db, **kw))
    run(_backfill(fake, db, **kw))
    assert len(db.windows()) == len(REAL_WINDOWS)


def test_backfill_trades(loaded_db, real_markets):
    ticker = real_markets[0]["ticker"]
    fake = FakeKalshi(real_markets, page_size=2)
    fake.trades[ticker] = [
        {"trade_id": "t1", "ticker": ticker, "created_time": "2026-09-18T20:16:00Z", "yes_price_dollars": "0.55", "no_price_dollars": "0.45", "count_fp": "10", "taker_side": "yes"},
        {"trade_id": "t2", "ticker": ticker, "created_time": "2026-09-18T20:17:00Z", "yes_price": 60, "no_price": 40, "count": 5, "taker_side": "no"},
        {"trade_id": "t3", "ticker": ticker, "created_time": "2026-09-18T20:29:59Z", "yes_price_dollars": "0.97", "no_price_dollars": "0.03", "count_fp": "100", "taker_side": "yes"},
    ]

    async def go():
        async with httpx.AsyncClient(transport=router(fake)) as http:
            return await backfill_trades(KalshiClient(http, BASE, base_delay=0), loaded_db)

    assert run(go()) == 3
    assert loaded_db.windows_needing_trades(10**15) == []  # every window marked complete
    assert loaded_db.counts()["trades"] == 3
    assert run(go()) == 0  # nothing left to do


def test_backfill_proxy_candles(db):
    hl = FakeHyperliquid()
    now = 1_790_100_000_000
    hl.candles = [{"t": now - 120_000, "T": now - 60_001, "o": "3.1", "h": "3.2", "l": "3.0", "c": "3.15", "v": "10"}]

    async def go():
        import natgas_bot.backfill as bf
        bf.now_ms = lambda: now  # freeze clock
        async with httpx.AsyncClient(transport=router(hl=hl)) as http:
            return await backfill_proxy_candles(HyperliquidClient(http, HL, base_delay=0), db, ("xyz",), (), days=1)

    assert run(go()) == {"xyz:NATGAS": 1}
    assert db.candle_coins() == ["xyz:NATGAS"]


def test_backup_is_consistent_and_pruned(loaded_db, tmp_path, monkeypatch):
    import gzip
    import sqlite3
    import time as _time

    import natgas_bot.db as dbmod

    out = tmp_path / "backups"
    stamps = iter(range(1_790_000_000, 1_790_000_100))
    real_gmtime = _time.gmtime
    monkeypatch.setattr(dbmod.time, "gmtime", lambda *a: real_gmtime(*a) if a else real_gmtime(next(stamps)))
    paths = [loaded_db.backup(out, keep=2) for _ in range(3)]

    assert sorted(out.iterdir()) == paths[1:]  # oldest pruned, no temp files left
    restored = tmp_path / "restored.db"
    restored.write_bytes(gzip.decompress(paths[-1].read_bytes()))
    conn = sqlite3.connect(restored)
    assert conn.execute("SELECT count(*) FROM windows").fetchone()[0] == loaded_db.counts()["windows"]
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
