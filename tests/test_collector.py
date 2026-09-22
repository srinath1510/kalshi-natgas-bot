import asyncio
from datetime import datetime, timezone

import httpx

from natgas_bot.collector import Collector
from natgas_bot.config import Config
from natgas_bot.hyperliquid import HyperliquidClient, parse_meta_and_ctxs, select_coins
from natgas_bot.kalshi import KalshiClient

from mock_api import FakeHyperliquid, FakeKalshi, router

BASE = "https://api.elections.kalshi.com/trade-api/v2"
HL = "https://api.hyperliquid.xyz/info"


def _freeze(monkeypatch, utc: str) -> int:
    import natgas_bot.collector as c
    now_s = int(datetime.fromisoformat(utc).replace(tzinfo=timezone.utc).timestamp())
    monkeypatch.setattr(c.time, "time", lambda: now_s)
    monkeypatch.setattr(c, "now_ms", lambda: now_s * 1000)
    return now_s


def _collector(db, fake_k, fake_h=None, **cfg_kw):
    cfg = Config(**cfg_kw)
    http = httpx.AsyncClient(transport=router(fake_k, fake_h))
    return Collector(cfg, db, KalshiClient(http, BASE, base_delay=0), HyperliquidClient(http, HL, base_delay=0) if fake_h else None), http


def test_track_and_book_dedupe(db, real_markets, monkeypatch):
    fake = FakeKalshi(real_markets)
    open_ticker = real_markets[-1]["ticker"]
    _freeze(monkeypatch, "2026-09-22T18:50")  # inside the last fixture window (18:45-19:00Z)
    same = {"orderbook": {"yes": [[45, 100]], "no": [[52, 30]]}}
    changed = {"orderbook": {"yes": [[46, 100]], "no": [[52, 30]]}}
    fake.books[open_ticker] = [same, same, changed]

    async def go():
        col, http = _collector(db, fake, orderbook_keepalive_s=3600)
        async with http:
            await col.track_markets()
            assert set(col.active) == {open_ticker}
            for _ in range(3):
                await col.poll_books()

    asyncio.run(go())
    rows = db.conn.execute("SELECT best_yes_bid, best_yes_ask FROM orderbook_snapshots ORDER BY id").fetchall()
    assert [tuple(r) for r in rows] == [(0.45, 0.48), (0.46, 0.48)]


def test_poll_trades_incremental(db, real_markets, monkeypatch):
    fake = FakeKalshi(real_markets)
    t = real_markets[-1]["ticker"]
    _freeze(monkeypatch, "2026-09-22T18:50")
    fake.trades[t] = [{"trade_id": "a", "ticker": t, "created_time": "2026-09-22T18:46:00Z", "yes_price": 50, "no_price": 50, "count": 1, "taker_side": "yes"}]

    async def go():
        col, http = _collector(db, fake)
        async with http:
            await col.track_markets()
            await col.poll_trades()
            fake.trades[t].append({"trade_id": "b", "ticker": t, "created_time": "2026-09-22T18:47:00Z", "yes_price": 55, "no_price": 45, "count": 2, "taker_side": "yes"})
            await col.poll_trades()

    asyncio.run(go())
    assert db.counts()["trades"] == 2
    trade_calls = [p for path, p in fake.calls if path == "/markets/trades"]
    assert "min_ts" in trade_calls[-1]  # second poll is incremental


def test_upcoming_window_activates_at_open(db, real_markets, monkeypatch):
    """Pre-created windows are known before they open and become active on the clock, not the listing."""
    import natgas_bot.collector as c
    fake = FakeKalshi(real_markets)
    prev_t, next_t = real_markets[-2]["ticker"], real_markets[-1]["ticker"]  # 18:30-18:45Z, 18:45-19:00Z
    now = {"s": _freeze(monkeypatch, "2026-09-22T18:44:59")}
    monkeypatch.setattr(c.time, "time", lambda: now["s"])
    monkeypatch.setattr(c, "now_ms", lambda: now["s"] * 1000)

    async def go():
        col, http = _collector(db, fake)
        async with http:
            await col.track_markets()
            assert set(col.known) == {prev_t, next_t}
            assert set(col.active) == {prev_t}
            now["s"] += 1  # 18:45:00Z, no tracker refresh in between
            await col.poll_books()
            assert set(col.active) == {next_t}

    asyncio.run(go())
    assert [r[0] for r in db.conn.execute("SELECT DISTINCT ticker FROM orderbook_snapshots")] == [next_t]


def test_sweep_marks_closed_windows(db, real_markets, monkeypatch):
    fake = FakeKalshi(real_markets)
    import natgas_bot.collector as c
    # pretend "now" is just after the last fixture window closed
    from datetime import datetime, timezone
    now_s = int(datetime(2026, 9, 22, 19, 20, tzinfo=timezone.utc).timestamp())
    monkeypatch.setattr(c.time, "time", lambda: now_s)
    monkeypatch.setattr(c, "now_ms", lambda: now_s * 1000)

    async def go():
        col, http = _collector(db, fake)
        async with http:
            await col.sweep_settlements()

    asyncio.run(go())
    # the Tuesday windows (closed within 3h) are recorded and their trade tapes marked complete
    rows = {r["ticker"]: r for r in db.windows()}
    assert len(rows) == 3  # only windows closed in the last 3h
    assert all(rows[m["ticker"]]["trades_backfilled"] == 1 for m in real_markets[-3:])


def test_proxy_parse_select_and_poll(db):
    hl = FakeHyperliquid()
    pairs = parse_meta_and_ctxs([{"universe": hl.universe}, hl.ctxs])
    assert [n for n, _ in select_coins(pairs, ())] == ["xyz:NATGAS"]
    assert [n for n, _ in select_coins(pairs, ("xyz:CL",))] == ["xyz:CL"]

    async def go():
        col, http = _collector(db, FakeKalshi([]), hl)
        async with http:
            await col.poll_proxy()

    asyncio.run(go())
    row = db.conn.execute("SELECT coin, oracle_px, mark_px FROM proxy_ticks").fetchone()
    assert tuple(row) == ("xyz:NATGAS", 3.1372, 3.1375)
    assert hl.bodies[0] == {"type": "metaAndAssetCtxs", "dex": "xyz"}


def test_loop_survives_errors(db):
    col, http = _collector(db, FakeKalshi([]))
    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")

    async def go():
        stop = asyncio.Event()
        task = asyncio.create_task(col._loop("flaky", 0.01, flaky, stop))
        await asyncio.sleep(0.1)
        stop.set()
        await task
        await http.aclose()

    asyncio.run(go())
    assert calls["n"] > 2
    statuses = [r[0] for r in db.conn.execute("SELECT status FROM heartbeats WHERE component='flaky'")]
    assert statuses[:2] == ["error", "recovered"]


def test_run_survives_unreachable_api(db):
    """Startup with the API down must not crash; errors go to heartbeats and loops keep retrying."""
    def down(request):
        raise httpx.ConnectError("offline")

    async def go():
        cfg = Config(tracker_interval_s=0.05, orderbook_interval_s=0.05, trades_interval_s=0.05,
                     settlement_interval_s=0.05, hl_interval_s=0.05)
        async with httpx.AsyncClient(transport=httpx.MockTransport(down)) as http:
            col = Collector(cfg, db, KalshiClient(http, BASE, base_delay=0), HyperliquidClient(http, HL, base_delay=0))
            stop = asyncio.Event()
            task = asyncio.create_task(col.run(stop))
            await asyncio.sleep(0.5)
            stop.set()
            await task

    asyncio.run(go())
    statuses = {r[0] for r in db.conn.execute("SELECT DISTINCT status FROM heartbeats")}
    assert {"start", "error", "stop"} <= statuses
