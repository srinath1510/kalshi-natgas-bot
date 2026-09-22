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
    kalshi_max_rps: float = 15.0  # evenly spaced cap; live polling of 7 series needs ~12/s, public limit ~20/s
    # All Kalshi 15-minute commodity series (Pyth-settled, same COMMODITIES contract template).
    series_tickers: tuple[str, ...] = (
        "KXNATGAS15M", "KXGOLD15M", "KXWTI15M", "KXSILVER15M", "KXCOPPER15M", "KXPLATINUM15M", "KXPALLADIUM15M",
    )
    hl_info_url: str = "https://api.hyperliquid.xyz/info"
    hl_dexes: tuple[str, ...] = ("xyz",)
    hl_coins: tuple[str, ...] = (  # empty -> auto-discover coins whose name contains NATGAS
        "xyz:NATGAS", "xyz:GOLD", "xyz:CL", "xyz:SILVER", "xyz:COPPER", "xyz:PLATINUM", "xyz:PALLADIUM",
    )
    orderbook_interval_s: float = 1.0
    trades_interval_s: float = 2.0
    tracker_interval_s: float = 5.0
    tracker_lookahead_s: int = 3600  # also fetch windows closing within this horizon (pre-created)
    settlement_interval_s: float = 60.0
    hl_ws_url: str = "wss://api.hyperliquid.xyz/ws"
    # The WebSocket carries the proxy; the REST poll is a slow fallback/cross-check. metaAndAssetCtxs
    # costs weight 20 of the 1200/min per-IP budget, which is shared with anything else on the host.
    hl_interval_s: float = 60.0
    orderbook_keepalive_s: float = 15.0  # store an unchanged book at least this often
    http_timeout_s: float = 10.0

    @classmethod
    def from_env(cls, db_path: str | None = None) -> "Config":
        env = os.environ
        return cls(
            db_path=Path(db_path or env.get("NATGAS_DB_PATH", "data/natgas.db")),
            kalshi_base_url=env.get("KALSHI_BASE_URL", cls.kalshi_base_url),
            kalshi_max_rps=float(env.get("KALSHI_MAX_RPS", cls.kalshi_max_rps)),
            series_tickers=_csv(env["KALSHI_SERIES"]) if "KALSHI_SERIES" in env else cls.series_tickers,
            hl_info_url=env.get("HL_INFO_URL", cls.hl_info_url),
            hl_dexes=_csv(env.get("HL_DEXES", "xyz")),
            hl_coins=_csv(env["HL_COINS"]) if "HL_COINS" in env else cls.hl_coins,
            orderbook_interval_s=float(env.get("ORDERBOOK_INTERVAL_S", 1.0)),
            trades_interval_s=float(env.get("TRADES_INTERVAL_S", 2.0)),
            tracker_interval_s=float(env.get("TRACKER_INTERVAL_S", 5.0)),
            settlement_interval_s=float(env.get("SETTLEMENT_INTERVAL_S", 60.0)),
            hl_ws_url=env.get("HL_WS_URL", cls.hl_ws_url),
            hl_interval_s=float(env.get("HL_INTERVAL_S", cls.hl_interval_s)),
            orderbook_keepalive_s=float(env.get("ORDERBOOK_KEEPALIVE_S", 15.0)),
        )
