from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import httpx

log = logging.getLogger(__name__)

RETRY_STATUS = {429, 500, 502, 503, 504}


async def request_json(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    json: Any = None,
    max_attempts: int = 5,
    base_delay: float = 0.5,
) -> Any:
    """HTTP request returning parsed JSON, retrying on network errors, 429 and 5xx."""
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = await client.request(method, url, params=params, json=json)
        except httpx.TransportError as exc:
            if attempt >= max_attempts:
                raise
            await _backoff(attempt, base_delay, f"{type(exc).__name__} on {url}")
            continue
        if resp.status_code in RETRY_STATUS and attempt < max_attempts:
            retry_after = _parse_retry_after(resp.headers.get("retry-after"))
            await _backoff(attempt, base_delay, f"HTTP {resp.status_code} on {url}", retry_after)
            continue
        resp.raise_for_status()
        return resp.json()


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


async def _backoff(attempt: int, base: float, reason: str, override: float | None = None) -> None:
    delay = override if override is not None else base * (2 ** (attempt - 1)) * (1 + random.random() * 0.25)
    delay = min(delay, 30.0)
    log.warning("retrying after %s (attempt %d, sleeping %.2fs)", reason, attempt, delay)
    await asyncio.sleep(delay)
