"""Classify 15-minute windows by trading session (US/Eastern, DST-aware).

Sessions:
  day            09:00-14:30 ET Mon-Fri (NYMEX NG day session, settlement at 14:30)
  eia            Thursday windows opening 10:15 or 10:30 ET (storage report at 10:30)
  cme_break      Mon-Thu windows opening 17:00-17:45 ET (CME daily halt; index is synthetic)
  friday_evening Friday windows opening >= 17:00 ET (CME closed for the weekend; index is synthetic)
  sunday_reopen  the Sunday window opening at 18:00 ET (weekend gap resolves here)
  offpeak        every other window (CME open, outside the day session)

Scheduled breaks with no windows: Saturday 00:00 -> Sunday 18:00 ET, and Kalshi's weekly
maintenance, Thursday 03:00 -> 05:00 ET. Exchange holidays are not modelled.
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

SESSIONS = ("day", "eia", "offpeak", "cme_break", "friday_evening", "sunday_reopen")


def to_et(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=ET)


def classify(open_ts_ms: int) -> str:
    dt = to_et(open_ts_ms)
    wd = dt.weekday()  # Mon=0 ... Sun=6
    minutes = dt.hour * 60 + dt.minute
    if wd == 6:
        return "sunday_reopen" if minutes == 18 * 60 else "offpeak"
    if wd == 4 and minutes >= 17 * 60:
        return "friday_evening"
    if wd == 3 and minutes in (10 * 60 + 15, 10 * 60 + 30):
        return "eia"
    if 17 * 60 <= minutes < 18 * 60:
        return "cme_break"
    if 9 * 60 <= minutes < 14 * 60 + 30:
        return "day"
    return "offpeak"


def is_weekend_gap(prev_close_ms: int, next_open_ms: int) -> bool:
    """True for the scheduled gap between the last Friday-night window and the Sunday 18:00 ET open."""
    a, b = to_et(prev_close_ms), to_et(next_open_ms)
    return (
        a.weekday() == 5 and a.hour == 0 and a.minute == 0
        and b.weekday() == 6 and b.hour == 18 and b.minute == 0
        and (b - a).days <= 2
    )


def is_maintenance_gap(prev_close_ms: int, next_open_ms: int) -> bool:
    """True for Kalshi's weekly maintenance break: no windows Thursday 03:00-05:00 ET."""
    a, b = to_et(prev_close_ms), to_et(next_open_ms)
    return (
        a.weekday() == 3 and (a.hour, a.minute) == (3, 0)
        and b.weekday() == 3 and (b.hour, b.minute) == (5, 0)
        and a.date() == b.date()
    )
