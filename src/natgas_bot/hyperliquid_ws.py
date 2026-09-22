"""Hyperliquid WebSocket stream for the proxy coins: asset context (oracle/mark/mid, pushed ~1/s),
best bid/offer (pushed on change, exchange-timestamped) and trades.

One connection, no REST weight. Hyperliquid's per-IP limits are shared with anything else on the
host: 10 connections, 30 new connections/min, 1000 subscriptions, 2000 sent messages/min. The server
closes connections it hasn't heard from in 60s, so we ping every 30s.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Any, Callable

import websockets

from .db import DB, now_ms
from .kalshi import num

log = logging.getLogger(__name__)

CHANNELS = ("activeAssetCtx", "bbo", "trades")
# Fields that make a context row worth storing when they change (day volume moves with every trade).
CTX_KEY = ("oracle_px", "mark_px", "mid_px", "impact_bid_px", "impact_ask_px", "funding", "open_interest")


def parse_ctx(data: dict[str, Any]) -> tuple[str, dict[str, float | None]]:
    ctx = data["ctx"]
    impact = ctx.get("impactPxs") or [None, None]
    return data["coin"], {
        "oracle_px": num(ctx.get("oraclePx")),
        "mark_px": num(ctx.get("markPx")),
        "mid_px": num(ctx.get("midPx")),
        "impact_bid_px": num(impact[0]) if len(impact) > 0 else None,
        "impact_ask_px": num(impact[1]) if len(impact) > 1 else None,
        "premium": num(ctx.get("premium")),
        "funding": num(ctx.get("funding")),
        "open_interest": num(ctx.get("openInterest")),
        "day_ntl_vlm": num(ctx.get("dayNtlVlm")),
    }


def parse_bbo(data: dict[str, Any]) -> dict[str, Any]:
    bid, ask = (list(data.get("bbo") or []) + [None, None])[:2]
    bid, ask = bid or {}, ask or {}
    return {
        "coin": data["coin"], "time": data.get("time"),
        "bid_px": num(bid.get("px")), "bid_sz": num(bid.get("sz")), "bid_n": bid.get("n"),
        "ask_px": num(ask.get("px")), "ask_sz": num(ask.get("sz")), "ask_n": ask.get("n"),
    }


def parse_trades(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "coin": t["coin"], "tid": t["tid"], "time": t.get("time"), "side": t.get("side"),
            "px": num(t.get("px")), "sz": num(t.get("sz")), "hash": t.get("hash"),
            "users": json.dumps(t.get("users")) if t.get("users") is not None else None,
        }
        for t in data
    ]


class HyperliquidStream:
    def __init__(
        self,
        url: str,
        coins: tuple[str, ...],
        db: DB,
        ctx_keepalive_s: float = 15.0,
        ping_s: float = 30.0,
        stale_s: float = 30.0,
        max_backoff_s: float = 60.0,
        connect: Callable[..., Any] = websockets.connect,
    ):
        self.url = url
        self.coins = coins
        self.db = db
        self.ctx_keepalive_ms = ctx_keepalive_s * 1000
        self.ping_s = ping_s
        self.stale_s = stale_s
        self.max_backoff_s = max_backoff_s
        self._connect = connect
        self._last_ctx: dict[str, tuple] = {}
        self._last_ctx_store: dict[str, int] = {}

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                async with self._connect(self.url, max_size=2**22, ping_interval=None, open_timeout=15) as ws:
                    for channel in CHANNELS:
                        for coin in self.coins:
                            await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": channel, "coin": coin}}))
                    self.db.heartbeat("hl_ws", "connected", f"{len(self.coins)} coins x {len(CHANNELS)} channels")
                    log.info("hyperliquid ws connected: %d subscriptions", len(self.coins) * len(CHANNELS))
                    backoff = 1.0
                    await self._read(ws, stop)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # reconnect on anything; the heartbeat table shows the gap
                log.warning("hyperliquid ws: %r", exc)
                self.db.heartbeat("hl_ws", "error", repr(exc))
            if stop.is_set():
                break
            delay = backoff * random.uniform(0.5, 1.0)  # jitter; also keeps well under 30 new connections/min
            backoff = min(backoff * 2, self.max_backoff_s)
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    async def _read(self, ws: Any, stop: asyncio.Event) -> None:
        last_ping = last_data = time.monotonic()
        while not stop.is_set():
            now = time.monotonic()
            if now - last_ping >= self.ping_s:
                await ws.send(json.dumps({"method": "ping"}))
                last_ping = now
            if now - last_data >= self.stale_s:
                raise TimeoutError(f"no data for {self.stale_s:.0f}s")
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if self.handle(json.loads(raw)):
                last_data = time.monotonic()

    def handle(self, msg: dict[str, Any]) -> bool:
        """Store one message. Returns True if it carried market data."""
        channel, data = msg.get("channel"), msg.get("data")
        recv = now_ms()
        if channel == "activeAssetCtx":
            coin, ctx = parse_ctx(data)
            key = tuple(ctx[k] for k in CTX_KEY)
            if key != self._last_ctx.get(coin) or recv - self._last_ctx_store.get(coin, 0) >= self.ctx_keepalive_ms:
                self.db.insert_hl_ctx(coin, recv, ctx)
                self._last_ctx[coin] = key
                self._last_ctx_store[coin] = recv
            return True
        if channel == "bbo":
            self.db.insert_hl_bbo(recv, parse_bbo(data))
            return True
        if channel == "trades":
            self.db.insert_hl_trades(recv, parse_trades(data))
            return True
        if channel == "error":
            log.warning("hyperliquid ws error message: %s", data)
            self.db.heartbeat("hl_ws", "error", str(data)[:500])
        return False
