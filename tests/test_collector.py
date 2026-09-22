import asyncio
from datetime import datetime, timezone

import httpx
import pytest

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


def test_tracks_and_polls_every_series(db, real_markets, gold_markets, monkeypatch):
    fake = FakeKalshi(real_markets + gold_markets)
    ng, gold = real_markets[-1]["ticker"], gold_markets[-1]["ticker"]
    _freeze(monkeypatch, "2026-09-22T18:50")
    fake.books[ng] = [{"orderbook": {"yes": [[45, 100]], "no": [[52, 30]]}}]
    fake.books[gold] = [{"orderbook": {"yes": [[60, 10]], "no": [[30, 5]]}}]
    fake.trades[gold] = [{"trade_id": "g1", "ticker": gold, "created_time": "2026-09-22T18:46:00Z",
                          "yes_price": 60, "no_price": 40, "count": 3, "taker_side": "yes"}]

    async def go():
        col, http = _collector(db, fake, series_tickers=("KXNATGAS15M", "KXGOLD15M"))
        async with http:
            await col.track_markets()
            assert set(col.active) == {ng, gold}
            await col.poll_books()
            await col.poll_trades()

    asyncio.run(go())
    books = dict(db.conn.execute("SELECT ticker, best_yes_bid FROM orderbook_snapshots").fetchall())
    assert books == {ng: 0.45, gold: 0.60}
    assert [r[0] for r in db.conn.execute("SELECT ticker FROM trades")] == [gold]
    series_listed = {p["series_ticker"] for path, p in fake.calls if path == "/markets"}
    assert series_listed == {"KXNATGAS15M", "KXGOLD15M"}


def test_one_failing_book_does_not_block_others(db, real_markets, gold_markets, monkeypatch):
    fake = FakeKalshi(real_markets + gold_markets)
    ng, gold = real_markets[-1]["ticker"], gold_markets[-1]["ticker"]
    _freeze(monkeypatch, "2026-09-22T18:50")
    fake.books[gold] = [{"orderbook": {"yes": [[60, 10]], "no": []}}]
    handle = fake.handle

    def flaky(request):
        if request.url.path.endswith(f"/{ng}/orderbook"):
            return httpx.Response(404, json={"error": "gone"})
        return handle(request)
    fake.handle = flaky

    async def go():
        col, http = _collector(db, fake, series_tickers=("KXNATGAS15M", "KXGOLD15M"))
        async with http:
            await col.track_markets()
            with pytest.raises(httpx.HTTPStatusError):
                await col.poll_books()

    asyncio.run(go())
    assert [r[0] for r in db.conn.execute("SELECT ticker FROM orderbook_snapshots")] == [gold]


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
    row = db.conn.execute("SELECT coin, oracle_px, mark_px FROM proxy_ticks WHERE coin = 'xyz:NATGAS'").fetchone()
    assert tuple(row) == ("xyz:NATGAS", 3.1372, 3.1375)
    assert {r[0] for r in db.conn.execute("SELECT coin FROM proxy_ticks")} == {"xyz:NATGAS", "xyz:GOLD", "xyz:CL"}
    assert len(hl.bodies) == 1  # all proxies come from one request per dex
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
                     settlement_interval_s=0.05, hl_interval_s=0.05, hl_ws_url="ws://127.0.0.1:9")
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
    assert db.conn.execute("SELECT COUNT(*) FROM heartbeats WHERE component = 'hl_ws' AND status = 'error'").fetchone()[0] >= 1


def test_config_series_and_coins_from_env(monkeypatch):
    assert Config.from_env().series_tickers[0] == "KXNATGAS15M"
    assert {"KXGOLD15M", "KXWTI15M"} <= set(Config.from_env().series_tickers)
    monkeypatch.setenv("KALSHI_SERIES", "KXGOLD15M, KXWTI15M")
    monkeypatch.setenv("HL_COINS", "")
    cfg = Config.from_env()
    assert cfg.series_tickers == ("KXGOLD15M", "KXWTI15M")
    assert cfg.hl_coins == ()  # explicit empty -> auto-discover *NATGAS*


def test_kalshi_rate_limit_caps_request_rate(db, real_markets):
    """30 concurrent requests at max_rps=20 are spaced 50ms apart (~1.45s), not burst."""
    import time
    fake = FakeKalshi(real_markets)

    async def go():
        async with httpx.AsyncClient(transport=router(fake)) as http:
            k = KalshiClient(http, BASE, base_delay=0, max_rps=20)
            t0 = time.monotonic()
            await asyncio.gather(*(k.get_orderbook(real_markets[0]["ticker"]) for _ in range(30)))
            return time.monotonic() - t0

    elapsed = asyncio.run(go())
    assert 1.4 <= elapsed < 2.0
    assert len(fake.calls) == 30
