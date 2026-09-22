from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _csv(value: str) -> tuple[str, ...]:
    return tuple(v.strip() for v in value.split(",") if v.strip())


@dataclass(frozen=True)
class Config:
    db_path: Path = Path("data/natgas.db")
    kalshi_base_url: str = "https://api.elections.kalshi.com/trade-api/v2"
    series_ticker: str = "KXNATGAS15M"
    hl_info_url: str = "https://api.hyperliquid.xyz/info"
    hl_dexes: tuple[str, ...] = ("xyz",)
    hl_coins: tuple[str, ...] = ()  # empty -> auto-discover coins whose name contains NATGAS
    orderbook_interval_s: float = 1.0
    trades_interval_s: float = 2.0
    tracker_interval_s: float = 5.0
    tracker_lookahead_s: int = 3600  # also fetch windows closing within this horizon (pre-created)
    settlement_interval_s: float = 60.0
    hl_interval_s: float = 2.0  # Hyperliquid info calls cost weight 20 of 1200/min per IP
    orderbook_keepalive_s: float = 15.0  # store an unchanged book at least this often
    http_timeout_s: float = 10.0

    @classmethod
    def from_env(cls, db_path: str | None = None) -> "Config":
        env = os.environ
        return cls(
            db_path=Path(db_path or env.get("NATGAS_DB_PATH", "data/natgas.db")),
            kalshi_base_url=env.get("KALSHI_BASE_URL", cls.kalshi_base_url),
            series_ticker=env.get("KALSHI_SERIES", cls.series_ticker),
            hl_info_url=env.get("HL_INFO_URL", cls.hl_info_url),
            hl_dexes=_csv(env.get("HL_DEXES", "xyz")),
            hl_coins=_csv(env.get("HL_COINS", "")),
            orderbook_interval_s=float(env.get("ORDERBOOK_INTERVAL_S", 1.0)),
            trades_interval_s=float(env.get("TRADES_INTERVAL_S", 2.0)),
            tracker_interval_s=float(env.get("TRACKER_INTERVAL_S", 5.0)),
            settlement_interval_s=float(env.get("SETTLEMENT_INTERVAL_S", 60.0)),
            hl_interval_s=float(env.get("HL_INTERVAL_S", 2.0)),
            orderbook_keepalive_s=float(env.get("ORDERBOOK_KEEPALIVE_S", 15.0)),
        )
