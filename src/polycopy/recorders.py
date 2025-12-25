from __future__ import annotations

import asyncio
import csv
import datetime as dt
import logging
from pathlib import Path
from typing import Iterable, Mapping, MutableSet

from .events import normalize_trade_event
from .state import Position
from .util import get_first

logger = logging.getLogger(__name__)


class TargetCsvRecorder:
    """Record target wallet trades and positions to CSV with simple dedupe."""

    trade_fields = [
        "recorded_at",
        "tx_hash",
        "asset_id",
        "market",
        "outcome",
        "side",
        "size",
        "price",
    ]
    position_fields = [
        "recorded_at",
        "asset_id",
        "market",
        "outcome",
        "size",
        "average_price",
    ]

    def __init__(self, *, trades_path: Path, positions_path: Path) -> None:
        self.trades_path = trades_path
        self.positions_path = positions_path
        self._lock = asyncio.Lock()
        self._trade_keys: MutableSet[tuple[str, str]] = set()
        self._position_keys: MutableSet[tuple[str, str, float, float]] = set()
        self._load_existing()

    def _load_existing(self) -> None:
        for path, key_fn, store in (
            (self.trades_path, self._trade_key_from_row, self._trade_keys),
            (self.positions_path, self._position_key_from_row, self._position_keys),
        ):
            if not path.exists():
                continue
            try:
                with path.open(newline="") as handle:
                    reader = csv.DictReader(handle)
                    for row in reader:
                        key = key_fn(row)
                        if key:
                            store.add(key)
            except (csv.Error, OSError) as exc:
                logger.debug("Failed to load existing CSV %s: %s", path, exc)

    @staticmethod
    def _trade_key_from_row(row: Mapping[str, str]) -> tuple[str, str] | None:
        tx_hash = (row.get("tx_hash") or "").strip()
        asset_id = (row.get("asset_id") or "").strip()
        if not tx_hash or not asset_id:
            return None
        return (tx_hash, asset_id)

    @staticmethod
    def _position_key_from_row(row: Mapping[str, str]) -> tuple[str, str, float, float] | None:
        asset_id = (row.get("asset_id") or "").strip()
        if not asset_id:
            return None
        outcome = (row.get("outcome") or "").strip()
        try:
            size = float(row.get("size", 0))
            avg_price = float(row.get("average_price", 0))
        except (TypeError, ValueError):
            return None
        return (asset_id, outcome, size, avg_price)

    def _trade_key(self, data: Mapping[str, object]) -> tuple[str, str] | None:
        tx_hash = get_first(data, ["tx_hash", "transactionHash", "txHash"])
        asset_id = get_first(data, ["asset_id", "assetId", "asset", "conditionId"])
        if not tx_hash or not asset_id:
            return None
        return (str(tx_hash), str(asset_id))

    def _position_key(self, data: Mapping[str, object]) -> tuple[str, str, float, float] | None:
        asset_id = get_first(data, ["asset_id", "assetId", "asset", "conditionId"])
        if not asset_id:
            return None
        outcome = str(get_first(data, ["outcome", "outcome_id"], "") or "")
        try:
            size = float(get_first(data, ["size", "quantity"], 0) or 0)
            avg_price = float(get_first(data, ["avg_price", "avgPrice", "average_price"], 0) or 0)
        except (TypeError, ValueError):
            return None
        return (str(asset_id), outcome, size, avg_price)

    async def record_trade(self, event: Mapping[str, object]) -> None:
        key = self._trade_key(event)
        if not key:
            return
        normalized = normalize_trade_event(event)
        side = normalized.get("side") or ""
        row = {
            "recorded_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "tx_hash": key[0],
            "asset_id": key[1],
            "market": normalized.get("market") or "",
            "outcome": normalized.get("outcome") or "",
            "side": side,
            "size": normalized.get("size"),
            "price": normalized.get("price"),
        }
        async with self._lock:
            if key in self._trade_keys:
                return
            self.trades_path.parent.mkdir(parents=True, exist_ok=True)
            exists = self.trades_path.exists() and self.trades_path.stat().st_size > 0
            with self.trades_path.open("a", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=self.trade_fields)
                if not exists:
                    writer.writeheader()
                writer.writerow(row)
            self._trade_keys.add(key)

    async def record_trades(self, events: Iterable[Mapping[str, object]]) -> None:
        for event in events:
            await self.record_trade(event)

    def _position_row(self, position: Mapping[str, object]) -> dict | None:
        key = self._position_key(position)
        if not key:
            return None
        asset_id, outcome, size, avg_price = key
        market = get_first(position, ["market", "market_slug", "event_slug", "eventSlug", "slug"], "") or ""
        return {
            "recorded_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "asset_id": asset_id,
            "market": market,
            "outcome": outcome,
            "size": size,
            "average_price": avg_price,
        }

    async def record_position(self, position: Position | Mapping[str, object]) -> None:
        data: Mapping[str, object]
        if isinstance(position, Position):
            data = {
                "asset_id": position.asset_id,
                "market": position.market,
                "outcome": position.outcome,
                "size": position.size,
                "avg_price": position.average_price,
            }
        else:
            data = position
        row = self._position_row(data)
        key = self._position_key(data)
        if not row or not key:
            return
        async with self._lock:
            if key in self._position_keys:
                return
            self.positions_path.parent.mkdir(parents=True, exist_ok=True)
            exists = self.positions_path.exists() and self.positions_path.stat().st_size > 0
            with self.positions_path.open("a", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=self.position_fields)
                if not exists:
                    writer.writeheader()
                writer.writerow(row)
            self._position_keys.add(key)

    async def record_positions(self, positions: Iterable[Position | Mapping[str, object]]) -> None:
        for position in positions:
            await self.record_position(position)
