import asyncio
import csv
import gzip
from datetime import date, datetime, timezone

import httpx

from natgas_bot import futures
from natgas_bot.futures import archive, ng_symbols, parse_chart, write_days


def _chart(ts_list, closes):
    return {"chart": {"result": [{"timestamp": ts_list, "indicators": {"quote": [{
        "open": closes, "high": closes, "low": closes, "close": closes, "volume": [1] * len(closes)}]}}]}}


def _ts(s):
    return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp())


def test_symbols_roll_over_year_end():
    assert ng_symbols(date(2026, 9, 23)) == ["NGV26.NYM", "NGX26.NYM", "NGZ26.NYM", "NG=F"]
    assert ng_symbols(date(2026, 11, 2)) == ["NGZ26.NYM", "NGF27.NYM", "NGG27.NYM", "NG=F"]


def test_parse_chart_skips_empty_bars():
    rows = parse_chart("NGX26.NYM", _chart([100, 160, 220], [3.1, None, 3.2]))
    assert [(r["ts_ms"], r["close"]) for r in rows] == [(100_000, 3.1), (220_000, 3.2)]
    assert parse_chart("X", {"chart": {"result": None}}) == []


def test_write_days_complete_days_only_and_refresh(tmp_path):
    rows = [
        {"symbol": "NGX26.NYM", "ts_ms": _ts("2026-09-20T23:59:00") * 1000, "open": 1, "high": 1, "low": 1, "close": 3.0, "volume": 1},
        {"symbol": "NGX26.NYM", "ts_ms": _ts("2026-09-22T12:00:00") * 1000, "open": 1, "high": 1, "low": 1, "close": 3.1, "volume": 1},
        {"symbol": "NGX26.NYM", "ts_ms": _ts("2026-09-23T12:00:00") * 1000, "open": 1, "high": 1, "low": 1, "close": 3.2, "volume": 1},
    ]
    today = date(2026, 9, 23)
    written = write_days(tmp_path, rows, today)
    assert [p.name for p in written] == ["ng-1m-2026-09-20.csv.gz", "ng-1m-2026-09-22.csv.gz"]  # today is incomplete
    with gzip.open(written[1], "rt") as f:
        assert [r["close"] for r in csv.DictReader(f)] == ["3.1"]
    # rerun: 09-20 is older than the refresh window and kept; 09-22 (within 2 complete days) is rewritten
    again = write_days(tmp_path, rows, today)
    assert [p.name for p in again] == ["ng-1m-2026-09-22.csv.gz"]
    assert not list(tmp_path.glob("*.tmp"))


def test_archive_fetches_in_chunks_and_survives_a_bad_symbol(tmp_path):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        sym = request.url.path.rsplit("/", 1)[-1]
        p1, p2 = int(request.url.params["period1"]), int(request.url.params["period2"])
        calls.append((sym, p1, p2))
        assert p2 - p1 <= futures.CHUNK_S
        if sym == "NGV26.NYM":
            return httpx.Response(404, json={"chart": {"error": "not found"}})
        return httpx.Response(200, json=_chart([p1 + 60], [3.0]))

    async def go():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            return await archive(http, tmp_path, days=10, now=datetime(2026, 9, 23, 10, tzinfo=timezone.utc),
                                 symbols=["NGV26.NYM", "NGX26.NYM"], base_delay=0)

    written = asyncio.run(go())
    assert {c[0] for c in calls} == {"NGV26.NYM", "NGX26.NYM"}
    assert len([c for c in calls if c[0] == "NGX26.NYM"]) == 2  # 10 days + part of today -> two <=7-day chunks
    assert written and all(p.name.startswith("ng-1m-2026-09-") for p in written)
