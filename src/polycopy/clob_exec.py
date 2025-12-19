from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

import httpx

from .risk import RiskLimits, RiskError, validate_trade
from .state import IntentStore

logger = logging.getLogger(__name__)


class ExecutionEngine:
    """Simplified CLOB execution wrapper."""

    def __init__(
        self,
        rest_url: str,
        api_key: str,
        api_secret: str,
        private_key: str,
        intent_store: IntentStore,
        risk_limits: RiskLimits,
        wallet_address: str,
        dry_run: bool = False,
        paper: bool = False,
    ) -> None:
        self.rest_url = rest_url.rstrip("/")
        self.intent_store = intent_store
        self.risk_limits = risk_limits
        self.dry_run = dry_run
        self.paper = paper
        self._client = httpx.AsyncClient(
            base_url=self.rest_url,
            timeout=httpx.Timeout(5.0, connect=2.0, read=5.0, write=2.0),
            headers={"X-API-Key": api_key, "X-API-Secret": api_secret},
        )
        self.private_key = private_key
        self.wallet_address = wallet_address

    async def close(self) -> None:
        await self._client.aclose()

    async def _submit(self, order: Dict[str, Any]) -> Dict[str, Any]:
        resp = await self._client.post("/orders", json=order)
        resp.raise_for_status()
        return resp.json()

    async def place_order(
        self,
        *,
        asset_id: str,
        market_id: str,
        outcome: str,
        side: str,
        size: float,
        limit_price: float,
        intent_key: str,
        target_tx: str,
        current_market_exposure: float,
        current_portfolio_exposure: float,
    ) -> Optional[Dict[str, Any]]:
        notional = abs(size) * limit_price
        resulting_market_notional = current_market_exposure + notional
        resulting_portfolio = current_portfolio_exposure + notional
        validate_trade(
            market_id=market_id,
            outcome=outcome,
            notional=notional,
            resulting_market_notional=resulting_market_notional,
            resulting_portfolio_exposure=resulting_portfolio,
            limits=self.risk_limits,
        )

        fresh = await self.intent_store.record_intent_if_new(target_tx, intent_key)
        if not fresh:
            logger.info("intent already processed, skipping %s", intent_key)
            return None

        order = {
            "asset_id": asset_id,
            "side": side,
            "size": abs(size),
            "price": limit_price,
            "wallet": self.wallet_address,
            "client_order_id": intent_key,
        }

        if self.dry_run or self.paper:
            logger.info("dry-run order %s", order)
            return {"status": "dry-run", "order": order}

        try:
            result = await self._submit(order)
            logger.info("submitted order %s result=%s", intent_key, result)
            return result
        except Exception as exc:  # noqa: BLE001
            logger.error("order failed: %s", exc)
            raise
