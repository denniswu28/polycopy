from __future__ import annotations

import argparse
from typing import ClassVar, List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed runtime settings loaded from environment and CLI overrides."""

    private_key: str = Field(..., description="Private key for signing CLOB requests")
    api_key: Optional[str] = Field(None, description="API key for Polymarket data access")
    api_secret: Optional[str] = Field(None, description="API secret for Polymarket data access")
    api_passphrase: Optional[str] = Field(None, description="API passphrase for Polymarket data access")
    target_wallet: str = Field(..., description="Wallet address to mirror")
    trader_wallet: str = Field(..., description="Our own wallet address used for trading")

    data_api_url: str = "https://data-api.polymarket.com"
    rtds_ws_url: str = "wss://ws-live-data.polymarket.com"
    clob_rest_url: str = "https://clob.polymarket.com"
    chain_id: int = 137

    http_poll_interval: float = 1.0
    reconcile_interval: float = 30.0
    watchlist_refresh_interval: float = 30.0
    ws_heartbeat_interval: float = 15.0
    ws_backoff_seconds: float = 5.0
    queue_maxsize: int = 512

    copy_factor: float = 0.25
    max_notional_per_trade: float = 250.0
    max_notional_per_market: float = 1000.0
    max_portfolio_exposure: float = 5000.0
    min_trade_size: float = 5.0
    slippage_bps: int = 50
    blacklist_markets: List[str] = Field(default_factory=list)
    blacklist_outcomes: List[str] = Field(default_factory=list)

    kill_switch_threshold: int = 5
    dry_run: bool = False
    paper_mode: bool = False

    db_path: str = "state.sqlite3"

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_file=".env", env_prefix="", env_file_encoding="utf-8"
    )

    @field_validator("copy_factor")
    @classmethod
    def _copy_factor_range(cls, v: float) -> float:
        """Validate that the copy factor remains within the inclusive range [0, 1]."""
        if not 0 <= v <= 1:
            raise ValueError("copy_factor must be within [0,1]")
        return v


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket copy trading bot")
    parser.add_argument("--dry-run", action="store_true", help="Do not submit live orders")
    parser.add_argument("--paper", action="store_true", help="Simulate fills without submitting")
    parser.add_argument("--target-wallet", help="Override target wallet to mirror")
    parser.add_argument("--trader-wallet", help="Override our own wallet used for execution")
    parser.add_argument("--copy-factor", type=float, help="Override copy factor scaling [0,1]")
    parser.add_argument("--healthcheck", action="store_true", help="Run startup checks and exit")
    parser.add_argument("--http-poll-interval", type=float, help="Polling interval seconds")
    parser.add_argument("--reconcile-interval", type=float, help="Reconciliation interval seconds")
    return parser


def load_settings(argv: Optional[list[str]] = None) -> tuple[Settings, argparse.Namespace]:
    parser = build_parser()
    args = parser.parse_args(argv)

    overrides = {}
    if args.dry_run:
        overrides["dry_run"] = True
    if args.paper:
        overrides["paper_mode"] = True
    if args.target_wallet:
        overrides["target_wallet"] = args.target_wallet
    if args.trader_wallet:
        overrides["trader_wallet"] = args.trader_wallet
    if args.copy_factor is not None:
        overrides["copy_factor"] = args.copy_factor
    if args.http_poll_interval:
        overrides["http_poll_interval"] = args.http_poll_interval
    if args.reconcile_interval:
        overrides["reconcile_interval"] = args.reconcile_interval

    settings = Settings(**overrides)
    return settings, args
