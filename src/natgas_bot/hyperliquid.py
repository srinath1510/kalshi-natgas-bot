"""Hyperliquid info API client, used as a free real-time proxy for Pyth NATGAS.

The xyz HIP-3 natural gas perp's oracle is set by its deployer (~every 3s). Whether it is
sourced from Pyth NATGAS is unverified; `natgas-bot verify` measures how closely it tracks
Kalshi's settlement prints.
"""
from __future__ import annotations

from typing import Any

import httpx

from .http_util import request_json


def parse_meta_and_ctxs(data: Any) -> list[tuple[str, dict[str, Any]]]:
    """metaAndAssetCtxs returns [meta, ctxs]; meta.universe[i] describes ctxs[i]."""
    if not isinstance(data, list) or len(data) < 2:
        raise ValueError("unexpected metaAndAssetCtxs payload")
    meta, ctxs = data[0], data[1]
    names = [u.get("name") for u in (meta or {}).get("universe", [])]
    if len(names) != len(ctxs):
        raise ValueError(f"universe/ctx length mismatch: {len(names)} vs {len(ctxs)}")
    return [(n, c) for n, c in zip(names, ctxs) if n]


def select_coins(pairs: list[tuple[str, dict[str, Any]]], wanted: tuple[str, ...]) -> list[tuple[str, dict[str, Any]]]:
    if wanted:
        wanted_set = set(wanted)
        return [p for p in pairs if p[0] in wanted_set]
    return [p for p in pairs if "NATGAS" in p[0].upper()]


class HyperliquidClient:
    def __init__(self, http: httpx.AsyncClient, info_url: str, base_delay: float = 0.5):
        self.http = http
        self.url = info_url
        self.base_delay = base_delay

    async def _post(self, body: dict[str, Any]) -> Any:
        return await request_json(self.http, "POST", self.url, json=body, base_delay=self.base_delay)

    async def meta_and_ctxs(self, dex: str = "") -> list[tuple[str, dict[str, Any]]]:
        body: dict[str, Any] = {"type": "metaAndAssetCtxs"}
        if dex:
            body["dex"] = dex
        return parse_meta_and_ctxs(await self._post(body))

    async def candles(self, coin: str, start_ms: int, end_ms: int, interval: str = "1m") -> list[dict[str, Any]]:
        """Trade-price candles (not oracle). The API returns at most the latest ~5000 candles."""
        body = {"type": "candleSnapshot", "req": {"coin": coin, "interval": interval, "startTime": start_ms, "endTime": end_ms}}
        data = await self._post(body)
        return data if isinstance(data, list) else []
