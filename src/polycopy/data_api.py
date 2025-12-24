from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple
import json
import httpx

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

    async def fetch_trades(self, user: str, limit: int = 50) -> List[Dict[str, Any]]:
        resp = await self._client.get("/trades", params={"user": user, "limit": limit})
        resp.raise_for_status()
        data = resp.json()
        logger.debug(
            "Fetched trades for user=%s limit=%s: %s",
            user, limit, json.dumps(data, indent=2)[:5000],
        )
        return data if isinstance(data, list) else data.get("data", [])

    async def fetch_positions(self, user: str) -> List[Dict[str, Any]]:
        resp = await self._client.get("/positions", params={"user": user})
        resp.raise_for_status()
        data = resp.json()
        logger.debug(
            "Fetched positions for user=%s: %s",
            user, json.dumps(data, indent=2)[:5000],
        )
        return data if isinstance(data, list) else data.get("data", [])

    async def fetch_book(self, asset_id: str) -> Dict[str, Any]:
        resp = await self._client.get(f"/book/{asset_id}")
        resp.raise_for_status()
        return resp.json()

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

    async def _publish(self, trade: Dict[str, Any]) -> None:
        payload = {
            "type": "target_trade_event",
            "tx_hash": trade.get("transactionHash") or trade.get("txHash"),
            "market": trade.get("market") or trade.get("market_slug"),
            "asset_id": trade.get("asset_id") or trade.get("assetId") or trade.get("asset"),
            "outcome": trade.get("outcome"),
            "size": trade.get("size"),
            "price": trade.get("price"),
            "is_buy": trade.get("is_buy") if "is_buy" in trade else trade.get("isBuy"),
            "side": trade.get("side"),
            "timestamp": trade.get("timestamp"),
        }
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
