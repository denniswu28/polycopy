from __future__ import annotations

import asyncio
import logging
from typing import Callable, Optional

from .orderbook import OrderBookManager

logger = logging.getLogger(__name__)


class MarketBookClient:
    """WebSocket client for Polymarket CLOB market channel (public)."""

    def __init__(
        self,
        url: str,
        orderbook_manager: OrderBookManager,
        heartbeat_interval: float = 10.0,
        backoff_seconds: float = 5.0,
    ) -> None:
        self.orderbook_manager = orderbook_manager
        self.heartbeat_interval = heartbeat_interval
        self.backoff_seconds = backoff_seconds
        self._running = False

    async def run(self) -> None:
        self._running = True
        failures = 0
        while self._running:
            try:
                subscriptions = await self.orderbook_manager.get_subscriptions()
                if subscriptions:
                    await self.orderbook_manager.refresh_prices(subscriptions)
                    failures = 0
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as exc:
                failures += 1
                logger.warning("MarketBookClient polling failed: %s (failure %s)", exc, failures)
                await asyncio.sleep(min(self.backoff_seconds * failures, 60))

    def stop(self) -> None:
        self._running = False
