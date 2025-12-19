from __future__ import annotations

import asyncio
import json
import logging
from typing import Awaitable, Callable, Optional, Set

import websockets
from websockets import WebSocketClientProtocol

logger = logging.getLogger(__name__)


class RtdsClient:
    """Best-effort RTDS WebSocket client for fast-path trade detection."""

    def __init__(
        self,
        url: str,
        target_wallet: str,
        queue: asyncio.Queue,
        heartbeat_interval: float = 15.0,
        backoff_seconds: float = 5.0,
        watchlist_provider: Optional[Callable[[], Awaitable[Set[str]]]] = None,
        failure_threshold: int = 5,
    ) -> None:
        self.url = url
        self.target_wallet = target_wallet.lower()
        self.queue = queue
        self.heartbeat_interval = heartbeat_interval
        self.backoff_seconds = backoff_seconds
        self.watchlist_provider = watchlist_provider
        self.failure_threshold = failure_threshold
        self._disabled = False
        self._failures = 0

    async def _heartbeat(self, ws: WebSocketClientProtocol) -> None:
        while True:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                await ws.ping()
            except Exception:
                return

    async def _subscribe(self, ws: WebSocketClientProtocol) -> None:
        filters = {"type": ["trades"]}
        if self.watchlist_provider:
            try:
                watchlist = await self.watchlist_provider()
                if watchlist:
                    filters["market_slug"] = list(watchlist)
            except Exception as exc:  # noqa: BLE001
                logger.debug("watchlist fetch failed: %s", exc)
        msg = {"action": "subscribe", "streams": [{"topic": "activity", "filters": filters}]}
        await ws.send(json.dumps(msg))

    async def _handle_message(self, message: str) -> None:
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        payload = data.get("payload") or data
        if not isinstance(payload, dict):
            return
        wallet = (payload.get("proxyWallet") or payload.get("wallet") or "").lower()
        if wallet != self.target_wallet:
            return
        event = {
            "type": "target_trade_event",
            "tx_hash": payload.get("txHash") or payload.get("transactionHash"),
            "market": payload.get("market_slug") or payload.get("market"),
            "asset_id": payload.get("asset_id") or payload.get("assetId"),
            "outcome": payload.get("outcome"),
            "size": payload.get("size"),
            "price": payload.get("price"),
            "timestamp": payload.get("timestamp"),
        }
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("WS queue full; dropping event %s", event)

    async def run(self) -> None:
        while not self._disabled:
            try:
                async with websockets.connect(self.url, ping_interval=None) as ws:
                    self._failures = 0
                    hb = asyncio.create_task(self._heartbeat(ws))
                    await self._subscribe(ws)
                    async for msg in ws:
                        await self._handle_message(msg)
                    hb.cancel()
            except Exception as exc:  # noqa: BLE001
                self._failures += 1
                logger.warning("WS error (%s), failure %s", exc, self._failures)
                if self._failures >= self.failure_threshold:
                    logger.error("WS circuit breaker opened; disabling fast path")
                    self._disabled = True
                    break
                await asyncio.sleep(self.backoff_seconds)

    @property
    def disabled(self) -> bool:
        return self._disabled
