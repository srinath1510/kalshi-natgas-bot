"""Shared fixtures built from real KXNATGAS15M windows pulled on 2026-09-22."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from natgas_bot.db import DB
from natgas_bot.kalshi import parse_market

FIXTURES = Path(__file__).parent / "fixtures"

# (open UTC, target, settle, result, volume) -- all real values
REAL_WINDOWS = [
    # Friday 2026-09-18, 16:15 ET through Fri 21:00 ET (CME closes 17:00 ET)
    ("2026-09-18T20:15", 3.02742, 3.02992, "yes", 11639.67),
    ("2026-09-18T20:30", 3.02992, 3.03335, "yes", 9629.96),
    ("2026-09-18T20:45", 3.03335, 3.03249, "no", 27144.30),
    ("2026-09-18T21:00", 3.03249, 3.03531, "yes", 27476.33),
    ("2026-09-18T21:15", 3.03531, 3.03538, "yes", 30634.77),
    ("2026-09-18T21:30", 3.03538, 3.03690, "yes", 12153.79),
    ("2026-09-18T21:45", 3.03690, 3.03776, "yes", 12358.72),
    ("2026-09-18T22:00", 3.03776, 3.03565, "no", 7216.31),
    ("2026-09-18T22:15", 3.03565, 3.03675, "yes", 7589.56),
    ("2026-09-18T22:30", 3.03675, 3.03860, "yes", 6466.75),
    ("2026-09-18T22:45", 3.03860, 3.03845, "no", 24404.84),
    ("2026-09-18T23:00", 3.03845, 3.03941, "yes", 13057.44),
    ("2026-09-18T23:15", 3.03941, 3.04090, "yes", 5273.60),
    ("2026-09-18T23:30", 3.04090, 3.04209, "yes", 17692.80),
    ("2026-09-18T23:45", 3.04209, 3.04434, "yes", 8523.15),
    ("2026-09-19T00:00", 3.04434, 3.04352, "no", 5955.12),
    ("2026-09-19T00:15", 3.04352, 3.04540, "yes", 11119.62),
    ("2026-09-19T00:30", 3.04540, 3.04700, "yes", 6621.10),
    ("2026-09-19T00:45", 3.04700, 3.04844, "yes", 7874.33),
    # last window of the week: Fri 23:45 ET -> Sat 00:00 ET (01:00-03:45Z not pulled -> one data gap)
    ("2026-09-19T03:45", 3.04712, 3.04538, "no", 17648.51),
    # Sunday 2026-09-20 reopen at 18:00 ET
    ("2026-09-20T22:00", 3.05420, 3.02636, "no", 5776.50),
    ("2026-09-20T22:15", 3.02636, 3.02564, "no", 14918.39),
    ("2026-09-20T22:30", 3.02564, 3.02592, "yes", 8363.34),
    ("2026-09-20T22:45", 3.02592, 3.02107, "no", 6815.57),
    ("2026-09-20T23:00", 3.02107, 3.02113, "yes", 14382.99),
    ("2026-09-20T23:15", 3.02113, 3.02686, "yes", 11472.94),
    ("2026-09-20T23:30", 3.02686, 3.02522, "no", 6224.51),
    ("2026-09-20T23:45", 3.02522, 3.02396, "no", 6663.92),
    # Monday 2026-09-21, 16:15-18:30 ET including the 17:00-18:00 CME halt
    ("2026-09-21T20:15", 2.98920, 2.99013, "yes", 10288.15),
    ("2026-09-21T20:30", 2.99013, 2.99068, "yes", 18108.21),
    ("2026-09-21T20:45", 2.99068, 2.98744, "no", 18420.23),
    ("2026-09-21T21:00", 2.98744, 2.99144, "yes", 4666.99),
    ("2026-09-21T21:15", 2.99144, 2.99425, "yes", 11254.13),
    ("2026-09-21T21:30", 2.99425, 2.99504, "yes", 18010.97),
    ("2026-09-21T21:45", 2.99504, 2.99495, "no", 12242.13),
    ("2026-09-21T22:00", 2.99495, 2.99000, "no", 15141.20),
    ("2026-09-21T22:15", 2.99000, 2.98977, "no", 24755.50),
    # Tuesday 2026-09-22, 14:15-15:00 ET
    ("2026-09-22T18:15", 3.09581, 3.11931, "yes", 7066.42),
    ("2026-09-22T18:30", 3.11931, 3.13377, "yes", 7339.79),
    ("2026-09-22T18:45", 3.13377, 3.13701, "yes", 12362.35),
]


def make_market(open_utc: str, target: float, settle: float | None, result: str, volume: float) -> dict:
    o = datetime.fromisoformat(open_utc).replace(tzinfo=timezone.utc)
    c = o + timedelta(minutes=15)
    et_close = c - timedelta(hours=4)  # EDT
    event = f"KXNATGAS15M-{et_close.strftime('%y%b%d%H%M').upper()}"
    return {
        "ticker": f"{event}-{et_close.strftime('%M')}",
        "event_ticker": event,
        "open_time": o.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "close_time": c.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "floor_strike": target,
        "expiration_value": f"{settle:.5f}" if settle is not None else "",
        "result": result,
        "status": "finalized",
        "strike_type": "greater_or_equal",
        "volume_fp": f"{volume:.2f}",
        "open_interest_fp": "4000.00",
        "settlement_ts": (c + timedelta(seconds=18)).strftime("%Y-%m-%dT%H:%M:%S.123456Z"),
    }


@pytest.fixture
def real_markets() -> list[dict]:
    return [make_market(*w) for w in REAL_WINDOWS]


@pytest.fixture
def db(tmp_path) -> DB:
    d = DB(tmp_path / "test.db")
    yield d
    d.close()


@pytest.fixture
def loaded_db(db, real_markets) -> DB:
    for m in real_markets:
        db.upsert_window(parse_market(m), m)
    return db


@pytest.fixture
def real_market_raw() -> dict:
    return json.loads((FIXTURES / "market_settled.json").read_text())
