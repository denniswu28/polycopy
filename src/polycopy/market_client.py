from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from .orderbook import OrderBookManager

logger = logging.getLogger(__name__)


class MarketBookClient:
    """WebSocket client for Polymarket CLOB market channel (public)."""

    def __init__(
        self,
        url: str,
        orderbook_manager: OrderBookManager,
        heartbeat_interval: float = 15.0,
        backoff_seconds: float = 5.0,
    ) -> None:
        self.url = url
        self.orderbook_manager = orderbook_manager
        self.heartbeat_interval = heartbeat_interval
        self.backoff_seconds = backoff_seconds
        self._running = False
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

    async def run(self) -> None:
        self._running = True
        failures = 0
        while self._running:
            try:
                async with websockets.connect(self.url, ping_interval=None) as ws:
                    self._ws = ws
                    failures = 0
                    logger.info("MarketBookClient connected to %s", self.url)

                    # Start heartbeat and subscription tasks
                    heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                    sub_task = asyncio.create_task(self._subscription_loop())
                    
                    try:
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg == "PONG":
                                continue
                            try:
                                data = json.loads(msg)
                                # Feed to orderbook manager
                                if isinstance(data, list):
                                    for item in data:
                                        await self.orderbook_manager.update_from_ws(item)
                                else:
                                    await self.orderbook_manager.update_from_ws(data)
                            except json.JSONDecodeError:
                                pass
                            except Exception as e:
                                logger.warning("Error processing market WS message: %s", e)
                    finally:
                        heartbeat_task.cancel()
                        sub_task.cancel()
                        try:
                            await heartbeat_task
                            await sub_task
                        except asyncio.CancelledError:
                            pass

            except (ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
                failures += 1
                logger.warning("MarketBookClient connection lost: %s. Retry %d", exc, failures)
                await asyncio.sleep(min(self.backoff_seconds * failures, 60))
            except Exception as exc:
                logger.error("MarketBookClient unexpected error: %s", exc, exc_info=True)
                await asyncio.sleep(self.backoff_seconds)

    async def _heartbeat_loop(self) -> None:
        while self._running and self._ws:
            try:
                await self._ws.send("PING")
                await asyncio.sleep(self.heartbeat_interval)
            except Exception:
                break

    async def _subscription_loop(self) -> None:
        """Periodically check for new subscriptions needed."""
        last_subs = set()
        while self._running and self._ws:
            try:
                current_subs = set(await self.orderbook_manager.get_subscriptions())
                new_subs = current_subs - last_subs
                
                if new_subs:
                    # Subscribe to new assets
                    # We'll batch them in chunks of 50 just in case
                    new_subs_list = list(new_subs)
                    chunk_size = 50
                    for i in range(0, len(new_subs_list), chunk_size):
                        chunk = new_subs_list[i : i + chunk_size]
                        # Initial connection expects type="market"; follow-up additions
                        # use the documented dynamic subscribe format.
                        if not last_subs:
                            msg = {"assets_ids": chunk, "type": "market"}
                        else:
                            msg = {"assets_ids": chunk, "operation": "subscribe"}
                        await self._ws.send(json.dumps(msg))
                        logger.info("Subscribed to market data for %d assets via %s", len(chunk), msg.get("operation", msg.get("type")))
                    
                    last_subs = current_subs

                await asyncio.sleep(2.0)
            except Exception as e:
                logger.warning("Subscription loop error: %s", e)
                await asyncio.sleep(5.0)

    def stop(self) -> None:
        self._running = False
