"""Kalshi public market-data client and parsers.

Only unauthenticated, read-only endpoints are used. Kalshi has shipped both an older
integer-cents format and a newer fixed-point dollars format (e.g. `yes_price_dollars`,
`volume_fp`), so every parser accepts either and the raw payloads are stored as well.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator

import httpx

from .http_util import request_json

_FRACTION = re.compile(r"\.(\d+)")


def iso_to_ms(value: str | None) -> int | None:
    """Parse Kalshi ISO-8601 timestamps (with Z and 0-9 fractional digits) to epoch ms."""
    if not value:
        return None
    s = value.strip().replace("Z", "+00:00")
    s = _FRACTION.sub(lambda m: "." + (m.group(1) + "000000")[:6], s, count=1)
    return int(round(datetime.fromisoformat(s).timestamp() * 1000))


def num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def first_num(d: dict[str, Any], *keys: str) -> float | None:
    for k in keys:
        v = num(d.get(k))
        if v is not None:
            return v
    return None


@dataclass(frozen=True)
class Window:
    ticker: str
    event_ticker: str | None
    open_ts: int | None
    close_ts: int | None
    target: float | None  # floor_strike: Pyth NATGAS close at window open
    settle: float | None  # expiration_value: Pyth NATGAS close at window close
    result: str | None
    status: str | None
    volume: float | None
    open_interest: float | None
    settlement_ts: int | None


def parse_market(m: dict[str, Any]) -> Window:
    return Window(
        ticker=m["ticker"],
        event_ticker=m.get("event_ticker"),
        open_ts=iso_to_ms(m.get("open_time")),
        close_ts=iso_to_ms(m.get("close_time")),
        target=num(m.get("floor_strike")),
        settle=num(m.get("expiration_value")),
        result=m.get("result") or None,
        status=m.get("status"),
        volume=first_num(m, "volume_fp", "volume"),
        open_interest=first_num(m, "open_interest_fp", "open_interest"),
        settlement_ts=iso_to_ms(m.get("settlement_ts")),
    )


Level = tuple[float, float]  # (price in dollars, size in contracts)


@dataclass(frozen=True)
class Book:
    yes: tuple[Level, ...]  # resting YES bids
    no: tuple[Level, ...]  # resting NO bids (a NO bid at p is a YES offer at 1-p)

    @property
    def best_yes_bid(self) -> Level | None:
        return max(self.yes, key=lambda lv: lv[0]) if self.yes else None

    @property
    def best_yes_ask(self) -> Level | None:
        if not self.no:
            return None
        p, q = max(self.no, key=lambda lv: lv[0])
        return (round(1.0 - p, 4), q)


def _levels(ob: dict[str, Any], side: str) -> tuple[Level, ...]:
    raw = ob.get(f"{side}_dollars")
    scale = 1.0
    if raw is None:
        raw = ob.get(side)
        scale = 0.01  # legacy integer cents
    out = []
    for entry in raw or []:
        try:
            price, size = float(entry[0]) * scale, float(entry[1])
        except (TypeError, ValueError, IndexError):
            continue
        if size > 0:
            out.append((round(price, 4), size))
    return tuple(sorted(out))


def parse_orderbook(payload: dict[str, Any]) -> Book:
    ob = payload.get("orderbook_fp") or payload.get("orderbook") or {}
    return Book(yes=_levels(ob, "yes"), no=_levels(ob, "no"))


@dataclass(frozen=True)
class Trade:
    trade_id: str
    ticker: str
    created_ts: int | None
    yes_price: float | None
    no_price: float | None
    count: float | None
    taker_side: str | None


def _price(t: dict[str, Any], side: str) -> float | None:
    v = num(t.get(f"{side}_price_dollars"))
    if v is not None:
        return v
    cents = num(t.get(f"{side}_price"))
    return cents / 100 if cents is not None else None


def parse_trade(t: dict[str, Any]) -> Trade:
    return Trade(
        trade_id=str(t["trade_id"]),
        ticker=t["ticker"],
        created_ts=iso_to_ms(t.get("created_time")),
        yes_price=_price(t, "yes"),
        no_price=_price(t, "no"),
        count=first_num(t, "count_fp", "count"),
        taker_side=t.get("taker_side"),
    )


def _clean(params: dict[str, Any] | None) -> dict[str, Any]:
    return {k: v for k, v in (params or {}).items() if v is not None}


class KalshiClient:
    def __init__(self, http: httpx.AsyncClient, base_url: str, base_delay: float = 0.5):
        self.http = http
        self.base = base_url.rstrip("/")
        self.base_delay = base_delay

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return await request_json(
            self.http, "GET", f"{self.base}{path}", params=_clean(params), base_delay=self.base_delay
        )

    async def _paginate(self, path: str, key: str, params: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
        cursor: str | None = None
        while True:
            p = dict(params)
            p.setdefault("limit", 1000)
            if cursor:
                p["cursor"] = cursor
            data = await self._get(path, p)
            items = data.get(key) or []
            for item in items:
                yield item
            cursor = data.get("cursor")
            if not cursor or not items:
                return

    def iter_markets(self, **params: Any) -> AsyncIterator[dict[str, Any]]:
        """GET /markets, following cursors. Filters: series_ticker, status, min/max_close_ts (Unix s)."""
        return self._paginate("/markets", "markets", params)

    def iter_trades(self, ticker: str, min_ts: int | None = None, max_ts: int | None = None) -> AsyncIterator[dict[str, Any]]:
        """GET /markets/trades for one market (min_ts/max_ts in Unix seconds)."""
        return self._paginate("/markets/trades", "trades", {"ticker": ticker, "min_ts": min_ts, "max_ts": max_ts})

    async def get_orderbook(self, ticker: str) -> dict[str, Any]:
        return await self._get(f"/markets/{ticker}/orderbook")
