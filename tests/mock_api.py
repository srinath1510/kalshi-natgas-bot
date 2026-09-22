"""In-process fakes for the Kalshi and Hyperliquid HTTP APIs (httpx.MockTransport)."""
from __future__ import annotations

import json
from typing import Any

import httpx

from natgas_bot.kalshi import iso_to_ms


class FakeKalshi:
    def __init__(self, markets: list[dict], page_size: int = 5):
        self.markets = markets
        self.page_size = page_size
        self.trades: dict[str, list[dict]] = {}
        self.books: dict[str, list[dict]] = {}  # successive payloads per ticker
        self.open_tickers: set[str] = set()
        self.calls: list[tuple[str, dict]] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.split("/trade-api/v2", 1)[-1]
        params = dict(request.url.params)
        self.calls.append((path, params))
        if path == "/markets":
            items = self.markets
            if params.get("status") == "open":
                items = [m for m in items if m["ticker"] in self.open_tickers]
            if "min_close_ts" in params:
                lo, hi = int(params["min_close_ts"]) * 1000, int(params["max_close_ts"]) * 1000
                items = [m for m in items if lo <= iso_to_ms(m["close_time"]) <= hi]
            return self._page(items, "markets", params)
        if path == "/markets/trades":
            items = self.trades.get(params["ticker"], [])
            if "min_ts" in params:
                items = [t for t in items if iso_to_ms(t["created_time"]) >= int(params["min_ts"]) * 1000]
            return self._page(items, "trades", params)
        if path.endswith("/orderbook"):
            ticker = path.split("/")[2]
            seq = self.books.get(ticker) or [{"orderbook": {"yes": [], "no": []}}]
            payload = seq.pop(0) if len(seq) > 1 else seq[0]
            return httpx.Response(200, json=payload)
        return httpx.Response(404, json={"error": path})

    def _page(self, items: list, key: str, params: dict[str, Any]) -> httpx.Response:
        start = int(params.get("cursor") or 0)
        chunk = items[start:start + self.page_size]
        nxt = start + self.page_size
        return httpx.Response(200, json={key: chunk, "cursor": str(nxt) if nxt < len(items) else ""})


class FakeHyperliquid:
    def __init__(self):
        self.universe = [{"name": "xyz:GOLD"}, {"name": "xyz:NATGAS"}, {"name": "xyz:CL"}]
        self.ctxs = [{"oraclePx": "4200.1"}, {"oraclePx": "3.1372", "markPx": "3.1375", "midPx": "3.1374"}, {"oraclePx": "62.1"}]
        self.candles: list[dict] = []
        self.bodies: list[dict] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.bodies.append(body)
        if body["type"] == "metaAndAssetCtxs":
            return httpx.Response(200, json=[{"universe": self.universe}, self.ctxs])
        if body["type"] == "candleSnapshot":
            lo, hi = body["req"]["startTime"], body["req"]["endTime"]
            return httpx.Response(200, json=[c for c in self.candles if lo <= c["t"] < hi])
        return httpx.Response(400)


def router(kalshi: FakeKalshi | None = None, hl: FakeHyperliquid | None = None) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.hyperliquid.xyz" and hl:
            return hl.handle(request)
        if kalshi:
            return kalshi.handle(request)
        return httpx.Response(404)
    return httpx.MockTransport(handler)
