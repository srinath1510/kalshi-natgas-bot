from datetime import datetime, timezone

import pytest

from natgas_bot.sessions import classify, is_maintenance_gap, is_weekend_gap


def ms(s: str) -> int:
    return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp() * 1000)


@pytest.mark.parametrize("utc,expected", [
    ("2026-09-20T22:00", "sunday_reopen"),   # Sun 18:00 ET
    ("2026-09-20T22:15", "offpeak"),         # Sun 18:15 ET
    ("2026-09-21T21:00", "cme_break"),       # Mon 17:00 ET
    ("2026-09-21T21:45", "cme_break"),       # Mon 17:45 ET
    ("2026-09-21T22:00", "offpeak"),         # Mon 18:00 ET (CME reopened)
    ("2026-09-18T21:00", "friday_evening"),  # Fri 17:00 ET
    ("2026-09-19T03:45", "friday_evening"),  # Fri 23:45 ET
    ("2026-09-17T14:15", "eia"),             # Thu 10:15 ET
    ("2026-09-17T14:30", "eia"),             # Thu 10:30 ET
    ("2026-09-17T14:45", "day"),             # Thu 10:45 ET
    ("2026-09-22T17:00", "day"),             # Tue 13:00 ET
    ("2026-09-22T18:30", "offpeak"),         # Tue 14:30 ET (after NG settlement)
    ("2026-12-15T15:00", "day"),             # Tue 10:00 EST (DST handled)
])
def test_classify(utc, expected):
    assert classify(ms(utc)) == expected


def test_weekend_gap():
    assert is_weekend_gap(ms("2026-09-19T04:00"), ms("2026-09-20T22:00"))
    assert not is_weekend_gap(ms("2026-09-19T01:00"), ms("2026-09-19T03:45"))


def test_maintenance_gap():
    # Thu 03:00 ET -> Thu 05:00 ET, EDT and EST
    assert is_maintenance_gap(ms("2026-09-17T07:00"), ms("2026-09-17T09:00"))
    assert is_maintenance_gap(ms("2026-12-17T08:00"), ms("2026-12-17T10:00"))
    assert not is_maintenance_gap(ms("2026-09-16T07:00"), ms("2026-09-16T09:00"))  # Wednesday
    assert not is_maintenance_gap(ms("2026-09-17T07:00"), ms("2026-09-17T09:15"))  # extra missing window
