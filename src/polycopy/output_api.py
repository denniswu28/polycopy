"""Output API: recorders, live view, logging sinks."""

from __future__ import annotations

import asyncio
import csv
import datetime as dt
import logging
import struct
import time
from collections import deque
from multiprocessing import shared_memory
from pathlib import Path
from typing import Any, Deque, Iterable, Mapping, MutableSet

import orjson

from .events import normalize_trade_event
from .state import Position, PortfolioState
from .util import get_first

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Recorders (copied from recorders.py)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Live View (copied from live_shared.py)
# ---------------------------------------------------------------------------


_HEADER = struct.Struct("<II")
DEFAULT_SHM_NAME = "polycopy_live_view"
DEFAULT_SHM_SIZE = 4 * 1_048_576  # 4MB
DEFAULT_FIFO_LIMIT = 200
_READ_RETRIES = 3
_READ_SLEEP_SECONDS = 0.001


def _value(obj: Mapping[str, Any] | Position, keys: tuple[str, ...], default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return get_first(obj, list(keys), default)
    for key in keys:
        if hasattr(obj, key):
            return getattr(obj, key)
    return default


def _position_dict(obj: Mapping[str, Any] | Position) -> dict:
    asset_id = _value(obj, ("asset_id", "assetId", "asset", "conditionId"))
    if not asset_id:
        return {}
    try:
        size = float(_value(obj, ("size", "quantity"), 0) or 0)
    except (TypeError, ValueError):
        size = 0.0
    try:
        avg_price = float(_value(obj, ("avg_price", "avgPrice", "average_price"), 0) or 0)
    except (TypeError, ValueError):
        avg_price = 0.0
    return {
        "asset_id": str(asset_id),
        "market": _value(obj, ("market", "market_slug", "event_slug", "eventSlug", "slug"), "") or "",
        "outcome": str(_value(obj, ("outcome", "outcome_id"), "") or ""),
        "size": size,
        "average_price": avg_price,
    }


def _trade_dict(event: Mapping[str, Any], *, kind: str) -> dict | None:
    normalized = normalize_trade_event(event)
    asset_id = _value(normalized, ("asset_id",))
    if not asset_id:
        return None
    try:
        size = float(normalized.get("size") or 0)
    except (TypeError, ValueError):
        size = 0.0
    try:
        price = float(normalized.get("price")) if normalized.get("price") is not None else None
    except (TypeError, ValueError):
        price = None
    side = normalized.get("side") or ""
    ts_raw = normalized.get("timestamp") or time.time()
    try:
        ts_val = float(ts_raw)
    except (TypeError, ValueError):
        ts_val = time.time()
    return {
        "kind": kind,
        "asset_id": str(asset_id),
        "market": _value(normalized, ("market",)) or "",
        "outcome": str(normalized.get("outcome") or _value(normalized, ("outcome_id",), "")),
        "side": side,
        "size": size,
        "price": price,
        "timestamp": ts_val,
    }


class _SharedBuffer:
    def __init__(self, *, name: str, size: int, create: bool) -> None:
        self._shm = shared_memory.SharedMemory(name=name, create=create, size=size if create else 0)
        self.size = self._shm.size
        self._data_capacity = max(0, self.size - _HEADER.size)
        if create:
            self._shm.buf[: self.size] = b"\x00" * self.size

    def close(self) -> None:
        self._shm.close()

    def unlink(self) -> None:
        try:
            self._shm.unlink()
        except (FileNotFoundError, OSError):
            pass

    def wipe(self) -> None:
        self._shm.buf[: self.size] = b"\x00" * self.size

    def write(self, payload: Mapping[str, Any]) -> None:
        data = orjson.dumps(payload)
        if len(data) > self._data_capacity:
            logger.warning("live view payload exceeded shared memory capacity; truncating to error message")
            data = orjson.dumps({"error": "live view payload too large"})
        version, _ = _HEADER.unpack_from(self._shm.buf, 0)
        in_progress = version + 1 if version % 2 == 0 else version
        _HEADER.pack_into(self._shm.buf, 0, in_progress, len(data))
        self._shm.buf[_HEADER.size : _HEADER.size + len(data)] = data
        _HEADER.pack_into(self._shm.buf, 0, in_progress + 1, len(data))

    def read(self) -> dict:
        for _ in range(_READ_RETRIES):
            version_first, length = _HEADER.unpack_from(self._shm.buf, 0)
            if version_first % 2 == 1:
                time.sleep(_READ_SLEEP_SECONDS)
                continue
            if length <= 0 or length > self._data_capacity:
                return {}
            raw = bytes(self._shm.buf[_HEADER.size : _HEADER.size + length])
            version_second, _ = _HEADER.unpack_from(self._shm.buf, 0)
            if version_first != version_second or version_first % 2 == 1:
                time.sleep(_READ_SLEEP_SECONDS)
                continue
            if not raw:
                return {}
            try:
                return orjson.loads(raw)
            except orjson.JSONDecodeError:
                logger.warning("live view payload decode failed", exc_info=True)
                return {}
        return {}


class LiveViewWriter:
    """Bounded shared-memory writer for live view data."""

    def __init__(
        self,
        *,
        name: str = DEFAULT_SHM_NAME,
        size: int = DEFAULT_SHM_SIZE,
        limit: int = DEFAULT_FIFO_LIMIT,
        copy_factor: float = 1.0,
    ) -> None:
        try:
            self._buffer = _SharedBuffer(name=name, size=size, create=True)
        except FileExistsError:
            self._buffer = _SharedBuffer(name=name, size=size, create=False)
            self._buffer.wipe()
        self._target_positions: list[dict] = []
        self._our_positions: list[dict] = []
        self._prices: dict[str, float] = {}
        self._target_trades: Deque[dict] = deque(maxlen=limit)
        self._orders: Deque[dict] = deque(maxlen=limit)
        self._copy_factor = copy_factor

    def close(self) -> None:
        self._buffer.close()

    def unlink(self) -> None:
        try:
            self._buffer.unlink()
        except (FileNotFoundError, OSError):
            return

    def update_positions(
        self,
        *,
        target: PortfolioState | Iterable[Mapping[str, Any]],
        ours: PortfolioState | Iterable[Mapping[str, Any]],
    ) -> None:
        if isinstance(target, PortfolioState):
            target_positions = list(target.positions.values())
        else:
            target_positions = list(target)
        if isinstance(ours, PortfolioState):
            our_positions = list(ours.positions.values())
        else:
            our_positions = list(ours)
        self._target_positions = [p for p in (_position_dict(pos) for pos in target_positions) if p]
        self._our_positions = [p for p in (_position_dict(pos) for pos in our_positions) if p]
        self._flush()

    def record_target_trade(self, event: Mapping[str, Any]) -> None:
        trade = _trade_dict(event, kind="target")
        if not trade:
            return
        if trade.get("price") is not None:
            try:
                self._prices[trade["asset_id"]] = float(trade["price"])
            except (TypeError, ValueError):
                pass
        self._target_trades.appendleft(trade)
        self._flush()

    def record_order(self, *, asset_id: str, market: str, outcome: str, side: str, size: float, price: float) -> None:
        try:
            price_val = float(price)
        except (TypeError, ValueError):
            price_val = 0.0
        order = {
            "kind": "order",
            "asset_id": asset_id,
            "market": market,
            "outcome": outcome,
            "side": side,
            "size": size,
            "price": price_val,
            "timestamp": time.time(),
        }
        if price_val:
            self._prices[asset_id] = price_val
        self._orders.appendleft(order)
        self._flush()

    def seed_trades(self, events: Iterable[Mapping[str, Any]]) -> None:
        for event in events:
            trade = _trade_dict(event, kind="target")
            if not trade:
                continue
            if trade.get("price") is not None:
                try:
                    self._prices[trade["asset_id"]] = float(trade["price"])
                except (TypeError, ValueError):
                    pass
            self._target_trades.appendleft(trade)
        self._flush()

    def _flush(self) -> None:
        payload: dict[str, Any] = {
            "updated_at": time.time(),
            "copy_factor": self._copy_factor,
            "positions": {
                "target": self._target_positions,
                "ours": self._our_positions,
            },
            "prices": self._prices,
            "trades": {
                "target": list(self._target_trades),
                "orders": list(self._orders),
            },
        }
        logger.debug(
            "live view flush positions target=%s ours=%s trades=%s orders=%s",
            len(self._target_positions),
            len(self._our_positions),
            len(self._target_trades),
            len(self._orders),
        )
        self._buffer.write(payload)


class LiveViewReader:
    """Read-only view of live data stored in shared memory."""

    def __init__(self, *, name: str = DEFAULT_SHM_NAME) -> None:
        self._buffer = _SharedBuffer(name=name, size=1, create=False)

    def read(self) -> dict:
        return self._buffer.read()

    def close(self) -> None:
        self._buffer.close()
