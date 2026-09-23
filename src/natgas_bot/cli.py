"""natgas-bot command line: backfill, collect, verify, status."""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator

import httpx

from . import __version__
from . import futures
from .backfill import backfill_proxy_candles, backfill_trades, backfill_windows
from .collector import Collector
from .config import Config
from .db import DB
from .hyperliquid import HyperliquidClient
from .kalshi import KalshiClient
from .verify import build_report, export_windows_csv

log = logging.getLogger("natgas_bot")


def _date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


@asynccontextmanager
async def _clients(cfg: Config) -> AsyncIterator[tuple[KalshiClient, HyperliquidClient]]:
    headers = {"User-Agent": f"kalshi-natgas-bot/{__version__}"}
    async with httpx.AsyncClient(timeout=cfg.http_timeout_s, headers=headers) as http:
        yield KalshiClient(http, cfg.kalshi_base_url, max_rps=cfg.kalshi_max_rps), HyperliquidClient(http, cfg.hl_info_url)


async def _backfill(cfg: Config, db: DB, args: argparse.Namespace) -> None:
    async with _clients(cfg) as (kalshi, hl):
        for series in cfg.series_tickers:
            n = await backfill_windows(kalshi, db, series, since=args.since, until=args.until)
            log.info("%s windows upserted: %d", series, n)
        if args.trades:
            await backfill_trades(kalshi, db)
        if args.proxy_candles:
            await backfill_proxy_candles(hl, db, cfg.hl_dexes, cfg.hl_coins, days=args.proxy_days)


async def _collect(cfg: Config, db: DB, args: argparse.Namespace) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            pass
    async with _clients(cfg) as (kalshi, hl):
        await Collector(cfg, db, kalshi, None if args.no_proxy else hl).run(stop)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="natgas-bot", description=__doc__)
    p.add_argument("--db", help="SQLite path (default data/natgas.db or $NATGAS_DB_PATH)")
    p.add_argument("--log-level", default="INFO")
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("backfill", help="pull historical settled windows (and optionally trades, proxy candles)")
    b.add_argument("--since", type=_date, help="YYYY-MM-DD (UTC). Default: walk back until 14 empty days")
    b.add_argument("--until", type=_date, help="YYYY-MM-DD (UTC). Default: now")
    b.add_argument("--trades", action="store_true", help="also fetch the full trade tape of every closed window")
    b.add_argument("--proxy-candles", action="store_true", help="also fetch Hyperliquid NATGAS 1m candles")
    b.add_argument("--proxy-days", type=float, default=3.4, help="days of proxy candles (HL keeps ~5000)")

    c = sub.add_parser("collect", help="run the live collector until Ctrl+C")
    c.add_argument("--no-proxy", action="store_true", help="skip the Hyperliquid proxy (REST poll and WebSocket)")

    v = sub.add_parser("verify", help="print the rules/sessions/proxy report")
    v.add_argument("--series", default="KXNATGAS15M", help="series to report on (default KXNATGAS15M)")
    v.add_argument("--csv", type=Path, help="also export one row per window of the series to this CSV")

    sub.add_parser("status", help="row counts per table")

    k = sub.add_parser("backup", help="write a consistent gzipped DB snapshot and prune old ones")
    k.add_argument("--out", type=Path, required=True, help="backup directory")
    k.add_argument("--keep", type=int, default=3, help="number of newest backups to keep")

    f = sub.add_parser("futures-bars", help="archive free delayed 1m NG futures bars (no DB needed)")
    f.add_argument("--out", type=Path, required=True, help="directory for ng-1m-YYYY-MM-DD.csv.gz files")
    f.add_argument("--days", type=int, default=10, help="days to (re)fetch; Yahoo keeps ~30 days of 1m bars")

    args = p.parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request is too noisy
    if args.cmd == "futures-bars":
        for path in futures.run(args.out, args.days):
            print(path)
        return
    cfg = Config.from_env(args.db)
    db = DB(cfg.db_path)
    try:
        if args.cmd == "backfill":
            asyncio.run(_backfill(cfg, db, args))
        elif args.cmd == "collect":
            asyncio.run(_collect(cfg, db, args))
        elif args.cmd == "verify":
            print(build_report(db, args.series))
            if args.csv:
                print(f"\nwrote {export_windows_csv(db, args.csv, args.series)} rows to {args.csv}")
        elif args.cmd == "status":
            for k, n in db.counts().items():
                print(f"{k:<22}{n:>10}")
        elif args.cmd == "backup":
            print(db.backup(args.out, args.keep))
    finally:
        db.close()


if __name__ == "__main__":
    main()
