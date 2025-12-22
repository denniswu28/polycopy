from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional, Tuple

import aiosqlite

from .util import get_first

logger = logging.getLogger(__name__)


def _updated_average_price(prev_size: float, prev_avg: float, trade_size: float, trade_price: float | None) -> float:
    if trade_price is None:
        return prev_avg
    new_size = prev_size + trade_size
    if prev_size == 0:
        return trade_price
    if (prev_size > 0 and trade_size > 0) or (prev_size < 0 and trade_size < 0):
        return trade_price if new_size == 0 else ((prev_avg * prev_size) + (trade_price * trade_size)) / new_size
    if (prev_size > 0 > new_size) or (prev_size < 0 < new_size):
        return trade_price
    return prev_avg


@dataclass
class Position:
    asset_id: str
    outcome: str
    size: float
    market: str = ""
    average_price: float = 0.0


@dataclass
class PortfolioState:
    positions: Dict[str, Position] = field(default_factory=dict)
    last_updated: float = field(default_factory=time.time)

    @classmethod
    def from_api(cls, items: Iterable[dict]) -> "PortfolioState":
        positions: Dict[str, Position] = {}
        for item in items:
            asset_id = get_first(item, ["asset_id", "assetId", "asset", "conditionId"])
            if not asset_id:
                continue
            positions[asset_id] = Position(
                asset_id=asset_id,
                outcome=get_first(item, ["outcome", "outcome_id"], ""),
                size=float(get_first(item, ["quantity", "size"], 0)),
                market=get_first(item, ["market", "market_slug", "event_slug", "eventSlug", "slug"], ""),
                average_price=float(get_first(item, ["avg_price", "avgPrice", "price"], 0)),
            )
        return cls(positions=positions, last_updated=time.time())

    def delta_against(self, other: "PortfolioState", scale: float = 1.0) -> Dict[str, float]:
        """Compute desired size delta (other - self) scaled."""
        deltas: Dict[str, float] = {}
        for asset_id, pos in other.positions.items():
            ours = self.positions.get(asset_id)
            diff = pos.size - (ours.size if ours else 0.0)
            scaled = diff * scale
            if math.isclose(scaled, 0.0, abs_tol=1e-6):
                continue
            deltas[asset_id] = scaled
        return deltas

    def notional(self, prices: Optional[Dict[str, float]] = None) -> float:
        total = 0.0
        for asset_id, pos in self.positions.items():
            price = 1.0
            if prices and asset_id in prices:
                price = prices[asset_id]
            total += abs(pos.size) * price
        return total

    def market_notional(self, market_map: Dict[str, str], prices: Dict[str, float]) -> Dict[str, float]:
        totals: Dict[str, float] = {}
        for asset_id, pos in self.positions.items():
            market = market_map.get(asset_id, "unknown")
            price = prices.get(asset_id, 1.0)
            totals[market] = totals.get(market, 0.0) + abs(pos.size) * price
        return totals


class PositionTracker:
    """In-memory tracker for target and local portfolio states."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.target = PortfolioState()
        self.ours = PortfolioState()

    async def refresh(self, *, target_positions: Iterable[dict], our_positions: Iterable[dict]) -> None:
        async with self._lock:
            self.target = PortfolioState.from_api(target_positions)
            self.ours = PortfolioState.from_api(our_positions)

    async def replace(self, *, target_state: PortfolioState, our_state: PortfolioState) -> None:
        async with self._lock:
            self.target = target_state
            self.ours = our_state

    async def update_target_from_trade(
        self, *, asset_id: str, outcome: str, market: str, size: float, price: float | None
    ) -> tuple[Position, Position | None, float]:
        async with self._lock:
            target_pos = self.target.positions.get(asset_id)
            if not target_pos:
                target_pos = Position(asset_id=asset_id, outcome=outcome, size=0.0, market=market)
                self.target.positions[asset_id] = target_pos
            prev_size = target_pos.size
            target_pos.size += size
            if market:
                target_pos.market = market
            if outcome:
                target_pos.outcome = outcome
            target_pos.average_price = _updated_average_price(prev_size, target_pos.average_price, size, price)
            our_pos = self.ours.positions.get(asset_id)
            portfolio_notional = self.ours.notional()
            return target_pos, our_pos, portfolio_notional

    async def apply_our_execution(
        self, *, asset_id: str, outcome: str, market: str, size: float, price: float
    ) -> Position:
        async with self._lock:
            our_pos = self.ours.positions.get(asset_id)
            if not our_pos:
                our_pos = Position(asset_id=asset_id, outcome=outcome, size=0.0, market=market)
                self.ours.positions[asset_id] = our_pos
            prev_size = our_pos.size
            our_pos.size += size
            if market:
                our_pos.market = market
            if outcome:
                our_pos.outcome = outcome
            our_pos.average_price = _updated_average_price(prev_size, our_pos.average_price, size, price)
            return our_pos

    async def snapshot(self) -> tuple[PortfolioState, PortfolioState]:
        async with self._lock:
            return self.target, self.ours


class IntentStore:
    """Idempotency and dedupe helper backed by SQLite."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._init_lock = asyncio.Lock()
        self._initialised = False

    async def _ensure(self) -> None:
        if self._initialised:
            return
        async with self._init_lock:
            if self._initialised:
                return
            os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS intents (
                        target_tx TEXT NOT NULL,
                        intent_key TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        PRIMARY KEY (target_tx, intent_key)
                    )
                    """
                )
                await db.commit()
            self._initialised = True

    async def record_intent_if_new(self, target_tx: str, intent_key: str) -> bool:
        await self._ensure()
        async with aiosqlite.connect(self.db_path) as db:
            try:
                await db.execute(
                    "INSERT INTO intents (target_tx, intent_key, created_at) VALUES (?, ?, ?)",
                    (target_tx, intent_key, time.time()),
                )
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def seen(self, target_tx: str, intent_key: str) -> bool:
        await self._ensure()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT 1 FROM intents WHERE target_tx = ? AND intent_key = ? LIMIT 1",
                (target_tx, intent_key),
            ) as cur:
                row = await cur.fetchone()
                return row is not None
