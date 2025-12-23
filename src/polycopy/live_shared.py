from __future__ import annotations

import struct
import time
from collections import deque
from multiprocessing import shared_memory
from typing import Any, Deque, Iterable, Mapping

import orjson

from .state import PortfolioState, Position
from .util import get_first

_HEADER = struct.Struct("<II")
DEFAULT_SHM_NAME = "polycopy_live_view"
DEFAULT_SHM_SIZE = 1_048_576
DEFAULT_FIFO_LIMIT = 200


def _value(obj: Mapping[str, Any] | Position, keys: tuple[str, ...], default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return get_first(obj, list(keys), default)
    for key in keys:
        if hasattr(obj, key):
            return getattr(obj, key)
    return default


def _position_dict(obj: Mapping[str, Any] | Position) -> dict | None:
    asset_id = _value(obj, ("asset_id", "assetId", "asset", "conditionId"))
    if not asset_id:
        return None
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
    asset_id = _value(event, ("asset_id", "assetId", "asset", "conditionId"))
    if not asset_id:
        return None
    try:
        size = float(event.get("size") or 0)
    except (TypeError, ValueError):
        size = 0.0
    try:
        price = float(event.get("price")) if event.get("price") is not None else None
    except (TypeError, ValueError):
        price = None
    side = (event.get("side") or "").lower()
    if not side:
        if event.get("is_buy") is True or event.get("isBuy") is True:
            side = "buy"
        elif event.get("is_buy") is False or event.get("isBuy") is False:
            side = "sell"
    ts_raw = event.get("timestamp") or time.time()
    try:
        ts_val = float(ts_raw)
    except (TypeError, ValueError):
        ts_val = time.time()
    return {
        "kind": kind,
        "asset_id": str(asset_id),
        "market": _value(event, ("market", "market_slug"), "") or "",
        "outcome": str(event.get("outcome") or _value(event, ("outcome_id",), "")),
        "side": side,
        "size": size,
        "price": price,
        "timestamp": ts_val,
    }


class _SharedBuffer:
    def __init__(self, *, name: str, size: int, create: bool) -> None:
        self._name = name
        self._shm = shared_memory.SharedMemory(name=name, create=create, size=size if create else 0)
        self.size = self._shm.size
        self._data_capacity = max(0, self.size - _HEADER.size)
        if create:
            self._shm.buf[: self.size] = b"\x00" * self.size

    def close(self) -> None:
        self._shm.close()

    def unlink(self) -> None:
        self._shm.unlink()

    def wipe(self) -> None:
        self._shm.buf[: self.size] = b"\x00" * self.size

    def write(self, payload: Mapping[str, Any]) -> None:
        data = orjson.dumps(payload)
        if len(data) > self._data_capacity:
            data = orjson.dumps({"error": "live view payload too large"})
        version, _ = _HEADER.unpack_from(self._shm.buf, 0)
        in_progress = version + 1 if version % 2 == 0 else version
        _HEADER.pack_into(self._shm.buf, 0, in_progress, len(data))
        self._shm.buf[_HEADER.size : _HEADER.size + len(data)] = data
        _HEADER.pack_into(self._shm.buf, 0, in_progress + 1, len(data))

    def read(self) -> dict:
        for _ in range(3):
            version_first, length = _HEADER.unpack_from(self._shm.buf, 0)
            if version_first % 2 == 1:
                time.sleep(0.001)
                continue
            if length <= 0 or length > self._data_capacity:
                return {}
            raw = bytes(self._shm.buf[_HEADER.size : _HEADER.size + length])
            version_second, _ = _HEADER.unpack_from(self._shm.buf, 0)
            if version_first != version_second or version_first % 2 == 1:
                time.sleep(0.001)
                continue
            if not raw:
                return {}
            try:
                return orjson.loads(raw)
            except Exception:  # noqa: BLE001
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

    def close(self) -> None:
        self._buffer.close()

    def unlink(self) -> None:
        try:
            self._buffer.unlink()
        except FileNotFoundError:
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
        self._buffer.write(payload)


class LiveViewReader:
    """Read-only view of live data stored in shared memory."""

    def __init__(self, *, name: str = DEFAULT_SHM_NAME) -> None:
        self._buffer = _SharedBuffer(name=name, size=1, create=False)

    def read(self) -> dict:
        return self._buffer.read()

    def close(self) -> None:
        self._buffer.close()
