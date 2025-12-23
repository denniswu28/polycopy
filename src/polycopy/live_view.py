from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import os
from typing import Iterable, Mapping

from .data_api import DataAPIClient
from .util import get_first


def _clear_terminal() -> None:
    print("\033[2J\033[H", end="")


def _format_ts(value: object | None) -> str:
    try:
        ts = float(value) if value is not None else None
    except (TypeError, ValueError):
        return ""
    if ts is None:
        return ""
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).strftime("%H:%M:%S")


def _render_positions(positions: Iterable[Mapping[str, object]]) -> str:
    def _position_sort_key(position: Mapping[str, object]) -> str:
        return str(get_first(position, ["market", "market_slug", "event_slug", "eventSlug", "slug"], "") or "")

    lines = ["Positions (target wallet)"]
    lines.append(f"{'Market':32} {'Outcome':8} {'Size':>10} {'AvgPx':>8}")
    for pos in sorted(positions, key=_position_sort_key):
        market = get_first(pos, ["market", "market_slug", "event_slug", "eventSlug", "slug"], "") or ""
        outcome = get_first(pos, ["outcome", "outcome_id"], "") or ""
        try:
            size_val = float(get_first(pos, ["size", "quantity"], 0) or 0)
        except (TypeError, ValueError):
            size_val = 0.0
        try:
            avg_price = float(get_first(pos, ["avg_price", "avgPrice", "price"], 0) or 0)
        except (TypeError, ValueError):
            avg_price = 0.0
        lines.append(f"{market[:32]:32} {outcome[:8]:8} {size_val:10.2f} {avg_price:8.3f}")
    return "\n".join(lines)


def _render_trades(trades: Iterable[Mapping[str, object]], seen: set[str]) -> str:
    lines = ["Recent trades (* = new since last refresh)"]
    lines.append(f"{'Time':8} {'Side':6} {'Size':>8} {'Price':>8} {'Outcome':8} {'Market':28} tx")
    sorted_trades = sorted(
        trades,
        key=lambda t: float(get_first(t, ["timestamp"], 0) or 0),
        reverse=True,
    )
    for trade in sorted_trades:
        tx_hash = str(get_first(trade, ["transactionHash", "txHash", "tx_hash"], "") or "")
        side = (get_first(trade, ["side"], "") or "").lower()
        if not side:
            if trade.get("is_buy") is True or trade.get("isBuy") is True:
                side = "buy"
            elif trade.get("is_buy") is False or trade.get("isBuy") is False:
                side = "sell"
            else:
                side = ""
        try:
            size_val = float(trade.get("size") or 0)
        except (TypeError, ValueError):
            size_val = 0.0
        try:
            price_val = float(trade.get("price") or 0)
        except (TypeError, ValueError):
            price_val = 0.0
        outcome = str(get_first(trade, ["outcome"], "") or "")[:8]
        market = str(get_first(trade, ["market", "market_slug"], "") or "")[:28]
        marker = "*" if tx_hash and tx_hash not in seen else " "
        lines.append(
            f"{_format_ts(trade.get('timestamp')):8} {side[:6]:6} {size_val:8.2f} {price_val:8.3f} "
            f"{outcome:8} {market:28} {marker}{tx_hash}"
        )
    return "\n".join(lines)


async def _render_loop(
    *,
    target_wallet: str,
    api_key: str,
    data_api_url: str,
    refresh: float,
    limit: int,
) -> None:
    async with DataAPIClient(data_api_url, api_key) as client:
        seen: set[str] = set()
        while True:
            trades = await client.fetch_trades(target_wallet, limit=limit)
            positions = await client.fetch_positions(target_wallet)
            _clear_terminal()
            now = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(f"Polycopy live view for {target_wallet} @ {now} UTC\n")
            print(_render_positions(positions))
            print()
            print(_render_trades(trades, seen))
            for trade in trades:
                tx_hash = get_first(trade, ["transactionHash", "txHash", "tx_hash"], "")
                if tx_hash:
                    seen.add(str(tx_hash))
            await asyncio.sleep(refresh)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live terminal viewer for target wallet trades/positions.")
    parser.add_argument("--target-wallet", help="Wallet address to observe (defaults to TARGET_WALLET env var)")
    parser.add_argument("--api-key", help="Polymarket data API key (defaults to API_KEY env var)")
    parser.add_argument("--data-api-url", default="https://data-api.polymarket.com", help="Data API base URL")
    parser.add_argument("--refresh", type=float, default=3.0, help="Refresh interval in seconds")
    parser.add_argument("--limit", type=int, default=20, help="Number of recent trades to display")
    return parser.parse_args(argv)


async def main_async(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    target_wallet = args.target_wallet or os.getenv("TARGET_WALLET")
    if not target_wallet:
        raise SystemExit("target wallet is required (pass --target-wallet or set TARGET_WALLET)")
    api_key = args.api_key or os.getenv("API_KEY")
    if not api_key:
        raise SystemExit("API key is required (pass --api-key or set API_KEY)")
    await _render_loop(
        target_wallet=target_wallet,
        api_key=api_key,
        data_api_url=args.data_api_url,
        refresh=args.refresh,
        limit=args.limit,
    )


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":  # pragma: no cover
    main()
