"""Checks and summaries over collected data.

- chaining: each window's target equals the previous window's settlement (when contiguous)
- ties: settlement exactly equal to target (resolves YES; also a symptom of a frozen feed)
- gaps: missing windows outside the scheduled breaks (Saturday-Sunday, Thursday 03:00-05:00 ET maintenance)
- session stats: size of 15-minute moves, YES rate, volume, by session
- proxy error: Hyperliquid oracle (and candle close) vs Kalshi settlement, by session
"""
from __future__ import annotations

import csv
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from .db import DB
from .sessions import SESSIONS, classify, is_maintenance_gap, is_weekend_gap, to_et

Row = Any  # sqlite3.Row or dict with window columns

# Hyperliquid coin (after the "dex:" prefix) that proxies each series' Pyth index.
PROXY_COIN = {
    "KXNATGAS15M": "NATGAS", "KXGOLD15M": "GOLD", "KXWTI15M": "CL", "KXSILVER15M": "SILVER",
    "KXCOPPER15M": "COPPER", "KXPLATINUM15M": "PLATINUM", "KXPALLADIUM15M": "PALLADIUM",
}


def _eq5(a: float, b: float) -> bool:
    return round(a, 5) == round(b, 5)


@dataclass
class ChainResult:
    pairs_checked: int = 0
    mismatches: list[tuple[str, str, float, float]] = field(default_factory=list)


def check_chaining(rows: Sequence[Row]) -> ChainResult:
    res = ChainResult()
    for prev, cur in zip(rows, rows[1:]):
        if prev["close_ts"] != cur["open_ts"] or prev["settle"] is None or cur["target"] is None:
            continue
        res.pairs_checked += 1
        if not _eq5(prev["settle"], cur["target"]):
            res.mismatches.append((prev["ticker"], cur["ticker"], prev["settle"], cur["target"]))
    return res


def find_ties(rows: Sequence[Row]) -> list[str]:
    return [r["ticker"] for r in rows if r["settle"] is not None and r["target"] is not None and _eq5(r["settle"], r["target"])]


def find_gaps(rows: Sequence[Row]) -> list[tuple[str, str, int, int]]:
    gaps = []
    for prev, cur in zip(rows, rows[1:]):
        a, b = prev["close_ts"], cur["open_ts"]
        if a is None or b is None or a == b:
            continue
        if b > a and (is_weekend_gap(a, b) or is_maintenance_gap(a, b)):
            continue
        gaps.append((prev["ticker"], cur["ticker"], a, b))
    return gaps


def _pct(values: list[float], q: float) -> float:
    s = sorted(values)
    if not s:
        return float("nan")
    k = (len(s) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def session_stats(rows: Sequence[Row]) -> dict[str, dict[str, float]]:
    buckets: dict[str, list[Row]] = {s: [] for s in SESSIONS}
    for r in rows:
        if r["settle"] is None or r["target"] is None:
            continue
        buckets.setdefault(classify(r["open_ts"]), []).append(r)
    out = {}
    for s, rs in buckets.items():
        if not rs:
            continue
        moves = [abs(r["settle"] - r["target"]) * 100 for r in rs]
        vols = [r["volume"] for r in rs if r["volume"] is not None]
        out[s] = {
            "n": len(rs),
            "yes_rate": sum(1 for r in rs if r["settle"] >= r["target"]) / len(rs),
            "abs_move_c_median": statistics.median(moves),
            "abs_move_c_p90": _pct(moves, 0.9),
            "abs_move_c_max": max(moves),
            "volume_median": statistics.median(vols) if vols else float("nan"),
        }
    return out


def _coins_for(coins: list[str], series: str) -> list[str]:
    name = PROXY_COIN.get(series)
    return [c for c in coins if name and c.split(":")[-1] == name]


def proxy_errors(
    db: DB, rows: Sequence[Row], series: str = "KXNATGAS15M", max_lag_ms: int = 10_000
) -> dict[str, dict[str, dict[str, float]]]:
    """Error (cents) of the series' proxy vs the Kalshi settlement print, grouped by source/coin then session."""
    samples: dict[str, dict[str, list[float]]] = {}
    for coin in _coins_for(db.proxy_coins(), series):
        for r in rows:
            if r["settle"] is None or r["close_ts"] is None:
                continue
            tick = db.proxy_tick_at_or_before(coin, r["close_ts"], max_lag_ms)
            if tick is None or tick["oracle_px"] is None:
                continue
            key = f"oracle:{coin}"
            samples.setdefault(key, {}).setdefault(classify(r["open_ts"]), []).append((tick["oracle_px"] - r["settle"]) * 100)
    for coin in _coins_for(db.hl_ctx_coins(), series):
        for r in rows:
            if r["settle"] is None or r["close_ts"] is None:
                continue
            ctx = db.hl_ctx_at_or_before(coin, r["close_ts"], max_lag_ms)
            if ctx is None or ctx["oracle_px"] is None:
                continue
            key = f"ws_oracle:{coin}"
            samples.setdefault(key, {}).setdefault(classify(r["open_ts"]), []).append((ctx["oracle_px"] - r["settle"]) * 100)
    for coin in _coins_for(db.candle_coins(), series):
        for r in rows:
            if r["settle"] is None or r["close_ts"] is None:
                continue
            c = db.candle_ending_at(coin, r["close_ts"])
            if c is None or c["c"] is None:
                continue
            key = f"candle_close:{coin}"
            samples.setdefault(key, {}).setdefault(classify(r["open_ts"]), []).append((c["c"] - r["settle"]) * 100)
    out: dict[str, dict[str, dict[str, float]]] = {}
    for key, by_session in samples.items():
        out[key] = {}
        for s, errs in by_session.items():
            abs_errs = [abs(e) for e in errs]
            out[key][s] = {
                "n": len(errs),
                "mean_c": statistics.fmean(errs),
                "abs_median_c": statistics.median(abs_errs),
                "abs_p90_c": _pct(abs_errs, 0.9),
            }
    return out


def _fmt_ts(ms: int) -> str:
    return to_et(ms).strftime("%a %Y-%m-%d %H:%M ET")


def build_report(db: DB, series: str = "KXNATGAS15M", max_list: int = 15) -> str:
    rows = db.windows(series)
    settled = [r for r in rows if r["settle"] is not None]
    lines = [f"{series} windows: {len(rows)} total, {len(settled)} settled"]
    if rows:
        lines.append(f"range:   {_fmt_ts(rows[0]['open_ts'])}  ->  {_fmt_ts(rows[-1]['open_ts'])}")

    chain = check_chaining(rows)
    lines += ["", f"CHAINING  target(n) == settle(n-1): {chain.pairs_checked - len(chain.mismatches)}/{chain.pairs_checked} contiguous pairs match"]
    for p, c, s, t in chain.mismatches[:max_list]:
        lines.append(f"  MISMATCH {p} settle={s:.5f} -> {c} target={t:.5f}")

    ties = find_ties(rows)
    lines += ["", f"TIES  settle == target (resolve YES; possible frozen feed): {len(ties)}"]
    lines += [f"  {t}" for t in ties[:max_list]]

    gaps = find_gaps(rows)
    lines += ["", f"GAPS  outside the scheduled weekend and Thursday maintenance breaks: {len(gaps)}"]
    for p, c, a, b in gaps[:max_list]:
        lines.append(f"  {_fmt_ts(a)} -> {_fmt_ts(b)}  ({p} -> {c})")

    lines += ["", f"SESSIONS  (moves in cents of the {series} index over the 15-minute window)"]
    lines.append(f"  {'session':<15}{'n':>6}{'yes%':>7}{'|move| med':>12}{'p90':>8}{'max':>8}{'vol med':>10}")
    for s, st in session_stats(rows).items():
        lines.append(
            f"  {s:<15}{st['n']:>6}{st['yes_rate'] * 100:>6.1f}%{st['abs_move_c_median']:>12.2f}"
            f"{st['abs_move_c_p90']:>8.2f}{st['abs_move_c_max']:>8.2f}{st['volume_median']:>10.0f}"
        )

    perr = proxy_errors(db, rows, series)
    lines += ["", "PROXY ERROR vs Kalshi settlement (cents; + means proxy above settlement)"]
    if not perr:
        lines.append("  no proxy data yet (run `natgas-bot collect`, or `backfill --proxy-candles`)")
    for key, by_session in perr.items():
        lines.append(f"  {key}")
        for s, st in by_session.items():
            lines.append(
                f"    {s:<15} n={st['n']:<5} mean={st['mean_c']:+.2f}  |err| median={st['abs_median_c']:.2f}  p90={st['abs_p90_c']:.2f}"
            )

    lines += ["", "TABLE COUNTS  " + ", ".join(f"{k}={v}" for k, v in db.counts().items())]
    return "\n".join(lines)


def export_windows_csv(db: DB, path: Path, series: str | None = None) -> int:
    rows = db.windows(series)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ticker", "open_et", "session", "target", "settle", "move_c", "result", "volume", "open_interest"])
        for r in rows:
            move = (r["settle"] - r["target"]) * 100 if r["settle"] is not None and r["target"] is not None else None
            w.writerow([
                r["ticker"], _fmt_ts(r["open_ts"]), classify(r["open_ts"]), r["target"], r["settle"],
                f"{move:.3f}" if move is not None else "", r["result"], r["volume"], r["open_interest"],
            ])
    return len(rows)
