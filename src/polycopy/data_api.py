from __future__ import annotations

import asyncio
import logging
from typing import Any, List, Mapping, Optional, Tuple
import json
import httpx

from .events import build_trade_event

logger = logging.getLogger(__name__)

class DataAPIClient:
    """Thin wrapper around the Polymarket public data API."""

    def __init__(self, base_url: str, api_key: str, timeout: float = 5.0) -> None:
        headers = {"X-API-Key": api_key}
        self._client = httpx.AsyncClient(base_url=base_url, headers=headers, timeout=timeout)

    async def __aenter__(self) -> "DataAPIClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def fetch_trades(self, user: str, limit: int = 50) -> List[dict[str, Any]]:
        logger.info("Fetching trades for user=%s limit=%s", user, limit)
        resp = await self._client.get("/trades", params={"user": user, "limit": limit})
        resp.raise_for_status()
        data = resp.json()
        logger.debug(
            "Fetched trades for user=%s limit=%s: %s",
            user, limit, json.dumps(data, indent=2)[:5000],
        )
        return data if isinstance(data, list) else data.get("data", [])

    async def fetch_positions(self, user: str) -> List[dict[str, Any]]:
        logger.info("Fetching positions for user=%s", user)
        resp = await self._client.get("/positions", params={"user": user})
        resp.raise_for_status()
        data = resp.json()
        logger.debug(
            "Fetched positions for user=%s: %s",
            user, json.dumps(data, indent=2)[:5000],
        )
        return data if isinstance(data, list) else data.get("data", [])

    async def close(self) -> None:
        await self._client.aclose()


class BackstopPoller:
    """HTTP polling backstop when WebSocket is unavailable."""

    def __init__(
        self,
        client: DataAPIClient,
        target_wallet: str,
        queue: asyncio.Queue,
        interval: float = 1.0,
    ) -> None:
        self.client = client
        self.target_wallet = target_wallet
        self.queue = queue
        self.interval = interval
        self._last_seen: Optional[Tuple[float, str]] = None
        self._running = False

    def _seen(self, ts: float, tx: str) -> bool:
        if self._last_seen is None:
            return False
        last_ts, last_tx = self._last_seen
        if ts < last_ts:
            return True
        if ts == last_ts and tx == last_tx:
            return True
        return False

    async def _publish(self, trade: Mapping[str, Any]) -> None:
        payload = build_trade_event(trade)
        if not payload:
            logger.debug("dropping backstop trade missing asset_id: %s", trade)
            return
        try:
            self.queue.put_nowait(payload)
        except asyncio.QueueFull:
            logger.warning("backstop queue full; dropping event %s", payload)

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                trades = await self.client.fetch_trades(self.target_wallet, limit=50)
                for trade in sorted(trades, key=lambda x: x.get("timestamp", 0)):
                    ts = float(trade.get("timestamp", 0))
                    tx = trade.get("transactionHash") or trade.get("txHash") or ""
                    if not tx:
                        continue
                    if self._seen(ts, tx):
                        continue
                    logger.info("Backstop found new trade: %s", tx)
                    await self._publish(trade)
                    self._last_seen = (ts, tx)
            except Exception as exc:  # noqa: BLE001
                logger.warning("backstop poll error: %s", exc)
            await asyncio.sleep(self.interval)

    def stop(self) -> None:
        self._running = False
