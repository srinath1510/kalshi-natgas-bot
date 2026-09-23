"""Archive free, delayed 1-minute NYMEX NG futures bars (Yahoo Finance chart endpoint).

During CME hours the Pyth NATGAS index equals the roll-weighted front NG futures price, so these
bars are our history of S. Yahoo keeps only ~30 days of 1-minute data: archive them daily.
Output: one gzipped CSV per UTC day, <out>/ng-1m-YYYY-MM-DD.csv.gz, all symbols in one file.
"""
from __future__ import annotations

import asyncio
import csv
import gzip
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from .http_util import request_json

log = logging.getLogger(__name__)

CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
MONTH_CODES = "FGHJKMNQUVXZ"  # Jan..Dec
FIELDS = ("symbol", "ts_ms", "open", "high", "low", "close", "volume")
CHUNK_S = 7 * 86400  # Yahoo serves 1m bars in windows of at most ~7 days


def ng_symbols(today: date, months_ahead: int = 3) -> list[str]:
    """The next `months_ahead` NG contract months after the current month, plus the continuous front."""
    syms = []
    for k in range(1, months_ahead + 1):
        m0 = today.month - 1 + k
        y, m = today.year + m0 // 12, m0 % 12 + 1
        syms.append(f"NG{MONTH_CODES[m - 1]}{y % 100:02d}.NYM")
    return syms + ["NG=F"]


def parse_chart(symbol: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    result = ((payload.get("chart") or {}).get("result") or [None])[0]
    if not result or not result.get("timestamp"):
        return []
    q = result["indicators"]["quote"][0]
    rows = []
    for i, ts in enumerate(result["timestamp"]):
        if q["close"][i] is None:
            continue
        rows.append({"symbol": symbol, "ts_ms": ts * 1000, "open": q["open"][i], "high": q["high"][i],
                     "low": q["low"][i], "close": q["close"][i], "volume": q["volume"][i]})
    return rows


async def fetch_symbol(http: httpx.AsyncClient, symbol: str, start_s: int, end_s: int, base_delay: float = 1.0) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    t = start_s
    while t < end_s:
        t2 = min(t + CHUNK_S, end_s)
        payload = await request_json(
            http, "GET", CHART_URL.format(symbol=symbol), base_delay=base_delay,
            params={"period1": t, "period2": t2, "interval": "1m", "includePrePost": "true"},
        )
        rows += parse_chart(symbol, payload)
        t = t2
    return rows


def day_file(out: Path, d: date) -> Path:
    return out / f"ng-1m-{d.isoformat()}.csv.gz"


def write_days(out: Path, rows: list[dict[str, Any]], today: date, refresh_days: int = 2) -> list[Path]:
    """Write complete UTC days (< today). Existing files are kept unless within the last `refresh_days`
    complete days, which are rewritten in case an earlier run saw a partial day."""
    out.mkdir(parents=True, exist_ok=True)
    by_day: dict[date, list[dict[str, Any]]] = {}
    for r in rows:
        d = datetime.fromtimestamp(r["ts_ms"] / 1000, tz=timezone.utc).date()
        if d < today:
            by_day.setdefault(d, []).append(r)
    written = []
    for d, day_rows in sorted(by_day.items()):
        path = day_file(out, d)
        if path.exists() and d < today - timedelta(days=refresh_days):
            continue
        day_rows.sort(key=lambda r: (r["symbol"], r["ts_ms"]))
        tmp = path.with_name(path.name + ".tmp")
        with gzip.open(tmp, "wt", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(day_rows)
        tmp.rename(path)
        written.append(path)
    return written


async def archive(http: httpx.AsyncClient, out: Path, days: int, now: datetime | None = None,
                  symbols: list[str] | None = None, base_delay: float = 1.0) -> list[Path]:
    now = now or datetime.now(timezone.utc)
    today = now.date()
    start = datetime.combine(today - timedelta(days=days), datetime.min.time(), tzinfo=timezone.utc)
    rows: list[dict[str, Any]] = []
    for sym in symbols or ng_symbols(today):
        try:
            got = await fetch_symbol(http, sym, int(start.timestamp()), int(now.timestamp()), base_delay)
            log.info("%s: %d bars", sym, len(got))
            rows += got
        except Exception as exc:  # one bad symbol (e.g. expired contract) must not stop the others
            log.warning("%s: %r", sym, exc)
    if not rows:
        raise RuntimeError("no futures bars fetched")
    return write_days(out, rows, today)


def run(out: Path, days: int) -> list[Path]:
    async def go() -> list[Path]:
        async with httpx.AsyncClient(timeout=30, headers={"User-Agent": "Mozilla/5.0"}) as http:
            return await archive(http, out, days)
    return asyncio.run(go())
