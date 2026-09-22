import asyncio
import json

import websockets

import natgas_bot.hyperliquid_ws as hws
from natgas_bot.hyperliquid_ws import HyperliquidStream, parse_bbo, parse_ctx, parse_trades
from natgas_bot.verify import proxy_errors

# Shapes captured from wss://api.hyperliquid.xyz/ws on 2026-09-22 (wallet addresses replaced).
CTX = {"channel": "activeAssetCtx", "data": {"coin": "xyz:NATGAS", "ctx": {
    "funding": "0.00000625", "openInterest": "1967916.4", "prevDayPx": "2.9946", "dayNtlVlm": "8319444.56",
    "premium": "-0.00057408", "oraclePx": "3.179", "markPx": "3.1778", "midPx": "3.17705",
    "impactPxs": ["3.1769", "3.17745"], "dayBaseVlm": "2697816.7"}}}
BBO = {"channel": "bbo", "data": {"coin": "xyz:SILVER", "time": 1790119644141,
       "bbo": [{"px": "67.388", "sz": "10.08", "n": 1}, {"px": "67.389", "sz": "76.55", "n": 2}]}}
TRADES = {"channel": "trades", "data": [
    {"coin": "xyz:NATGAS", "side": "B", "px": "3.1768", "sz": "3.5", "time": 1790119150922, "hash": "0xaa",
     "tid": 644346118695407, "users": ["0xbuyer", "0xseller"]},
    {"coin": "xyz:NATGAS", "side": "A", "px": "3.1765", "sz": "4.3", "time": 1790119188493, "hash": "0xbb",
     "tid": 960840405993232, "users": ["0xbuyer2", "0xseller2"]}]}


def test_parsers():
    coin, ctx = parse_ctx(CTX["data"])
    assert coin == "xyz:NATGAS"
    assert ctx["oracle_px"] == 3.179 and ctx["mark_px"] == 3.1778 and ctx["mid_px"] == 3.17705
    assert (ctx["impact_bid_px"], ctx["impact_ask_px"]) == (3.1769, 3.17745)
    assert ctx["funding"] == 0.00000625 and ctx["open_interest"] == 1967916.4
    b = parse_bbo(BBO["data"])
    assert (b["coin"], b["time"], b["bid_px"], b["bid_n"], b["ask_px"], b["ask_sz"]) == ("xyz:SILVER", 1790119644141, 67.388, 1, 67.389, 76.55)
    one_sided = parse_bbo({"coin": "xyz:X", "time": 1, "bbo": [None, {"px": "2", "sz": "1", "n": 1}]})
    assert one_sided["bid_px"] is None and one_sided["ask_px"] == 2.0
    t = parse_trades(TRADES["data"])
    assert [(x["tid"], x["side"], x["px"]) for x in t] == [(644346118695407, "B", 3.1768), (960840405993232, "A", 3.1765)]
    assert json.loads(t[0]["users"]) == ["0xbuyer", "0xseller"]


def test_handle_dedupes_ctx_and_trades(db, monkeypatch):
    now = {"ms": 1_790_000_000_000}
    monkeypatch.setattr(hws, "now_ms", lambda: now["ms"])
    s = HyperliquidStream("ws://unused", ("xyz:NATGAS",), db, ctx_keepalive_s=15)

    assert s.handle(CTX)
    now["ms"] += 1000
    s.handle(CTX)  # unchanged within keepalive -> not stored
    changed = json.loads(json.dumps(CTX))
    changed["data"]["ctx"]["dayNtlVlm"] = "9999999"  # volume alone is not a change worth a row
    now["ms"] += 1000
    s.handle(changed)
    changed["data"]["ctx"]["oraclePx"] = "3.180"
    now["ms"] += 1000
    s.handle(changed)  # oracle moved -> stored
    now["ms"] += 15_000
    s.handle(changed)  # keepalive -> stored
    rows = db.conn.execute("SELECT recv_ts, oracle_px FROM hl_ctx ORDER BY id").fetchall()
    base = 1_790_000_000_000
    assert [tuple(r) for r in rows] == [(base, 3.179), (base + 3000, 3.18), (base + 18000, 3.18)]

    assert s.handle(BBO)
    assert s.handle(TRADES)
    assert s.handle(TRADES)  # snapshot resent after a reconnect: no duplicates
    assert db.counts()["hl_trades"] == 2 and db.counts()["hl_bbo"] == 1
    assert not s.handle({"channel": "subscriptionResponse", "data": {}})
    assert not s.handle({"channel": "pong"})


def test_stream_subscribes_pings_and_reconnects(db):
    """Against a local server: 21 subscriptions, pings, data stored, reconnect after the server drops us."""
    received: list[list[dict]] = []

    async def handler(ws):
        msgs: list[dict] = []
        received.append(msgs)
        n_subs = 0
        async for raw in ws:
            m = json.loads(raw)
            msgs.append(m)
            if m.get("method") == "subscribe":
                n_subs += 1
                if n_subs == 21:
                    await ws.send(json.dumps(CTX))
                    await ws.send(json.dumps(TRADES))
            elif m.get("method") == "ping":
                await ws.send(json.dumps({"channel": "pong"}))
                if len(received) == 1:
                    await ws.close()  # first connection: drop the client after its first ping

    async def go():
        async with websockets.serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            coins = ("xyz:NATGAS", "xyz:GOLD", "xyz:CL", "xyz:SILVER", "xyz:COPPER", "xyz:PLATINUM", "xyz:PALLADIUM")
            stream = HyperliquidStream(f"ws://127.0.0.1:{port}", coins, db, ping_s=0.2, stale_s=5, max_backoff_s=0.1)
            stop = asyncio.Event()
            task = asyncio.create_task(stream.run(stop))
            for _ in range(100):
                await asyncio.sleep(0.05)
                if len(received) >= 2 and any(m.get("method") == "ping" for m in received[1]):
                    break
            stop.set()
            await asyncio.wait_for(task, timeout=5)

    asyncio.run(go())
    assert len(received) >= 2  # reconnected
    subs = [m["subscription"] for m in received[0] if m.get("method") == "subscribe"]
    assert len(subs) == 21 and {s["type"] for s in subs} == {"activeAssetCtx", "bbo", "trades"}
    assert any(m.get("method") == "ping" for m in received[0])
    assert db.counts()["hl_ctx"] >= 1 and db.counts()["hl_trades"] == 2
    statuses = [r[0] for r in db.conn.execute("SELECT status FROM heartbeats WHERE component = 'hl_ws' ORDER BY id")]
    assert statuses.count("connected") >= 2


def test_stream_reconnects_when_feed_goes_silent(db):
    connections = []

    async def handler(ws):
        connections.append(ws)
        async for _ in ws:  # accepts subscriptions, never sends data
            pass

    async def go():
        async with websockets.serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            stream = HyperliquidStream(f"ws://127.0.0.1:{port}", ("xyz:NATGAS",), db, stale_s=0.3, max_backoff_s=0.05)
            stop = asyncio.Event()
            task = asyncio.create_task(stream.run(stop))
            for _ in range(60):
                await asyncio.sleep(0.05)
                if len(connections) >= 2:
                    break
            stop.set()
            await asyncio.wait_for(task, timeout=5)

    asyncio.run(go())
    assert len(connections) >= 2
    errors = [r[0] for r in db.conn.execute("SELECT detail FROM heartbeats WHERE component = 'hl_ws' AND status = 'error'")]
    assert any("no data" in e for e in errors)


def test_verify_reports_ws_oracle(loaded_db):
    rows = loaded_db.windows("KXNATGAS15M")
    last = [r for r in rows if r["settle"] is not None][-1]
    loaded_db.insert_hl_ctx("xyz:NATGAS", last["close_ts"] - 500, {
        "oracle_px": last["settle"] + 0.001, "mark_px": None, "mid_px": None, "impact_bid_px": None,
        "impact_ask_px": None, "premium": None, "funding": None, "open_interest": None, "day_ntl_vlm": None})
    errs = proxy_errors(loaded_db, rows, "KXNATGAS15M")
    (session_stats,) = errs["ws_oracle:xyz:NATGAS"].values()
    assert session_stats["n"] == 1 and abs(session_stats["mean_c"] - 0.1) < 1e-9
