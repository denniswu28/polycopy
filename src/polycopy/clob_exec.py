from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, Optional, Tuple
import json
import httpx

from .risk import RiskLimits, RiskError, validate_trade
from .state import IntentStore

logger = logging.getLogger(__name__)


class MarketStatusChecker:
    """Cache market active/closed state using the CLOB REST API.

    Results are cached for ``ttl_seconds``. If the API call fails or
    returns an unexpected payload, the checker defaults to treating the
    market as active so that execution is not blocked by transient errors.
    """

    def __init__(self, rest_url: str, ttl_seconds: float = 60.0) -> None:
        self._client = httpx.AsyncClient(
            base_url=rest_url.rstrip("/"),
            timeout=httpx.Timeout(5.0, connect=2.0, read=5.0, write=2.0),
        )
        self._cache: Dict[str, Tuple[bool, float]] = {}
        self._ttl = ttl_seconds

    async def is_active(self, market_id: str) -> bool:
        now = time.time()
        cached = self._cache.get(market_id)
        if cached and (now - cached[1]) < self._ttl:
            return cached[0]
        try:
            resp = await self._client.get(f"/markets/{market_id}")
            resp.raise_for_status()
            data = resp.json()
            closed = data.get("closed")
            if closed is not None:
                active = not bool(closed)
            else:
                active = bool(data.get("active", True))
            self._cache[market_id] = (active, now)
            logger.debug("market status for %s active=%s", market_id, active)
            return active
        except (httpx.HTTPError, ValueError) as exc:
            logger.debug("market status check failed for %s: %s", market_id, exc)
            return True

    async def close(self) -> None:
        await self._client.aclose()


class ExecutionEngine:
    """Simplified CLOB execution wrapper."""

    def __init__(
        self,
        rest_url: str,
        api_key: str,
        api_secret: str,
        api_passphrase: str | None,
        private_key: str,
        intent_store: IntentStore,
        risk_limits: RiskLimits,
        wallet_address: str,
        dry_run: bool = False,
        paper: bool = False,
        market_status_checker: MarketStatusChecker | None = None,
    ) -> None:
        self.rest_url = rest_url.rstrip("/")
        self.intent_store = intent_store
        self.risk_limits = risk_limits
        self.dry_run = dry_run
        self.paper = paper
        headers = {"X-API-Key": api_key, "X-API-Secret": api_secret}
        if api_passphrase:
            headers["X-API-Passphrase"] = api_passphrase
        self._client = httpx.AsyncClient(
            base_url=self.rest_url,
            timeout=httpx.Timeout(5.0, connect=2.0, read=5.0, write=2.0),
            headers=headers,
        )
        self.private_key = private_key
        self.wallet_address = wallet_address
        self.market_status_checker = market_status_checker

    async def close(self) -> None:
        await self._client.aclose()
        if self.market_status_checker:
            await self.market_status_checker.close()

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

        if self.market_status_checker:
            active = await self.market_status_checker.is_active(market_id)
            if not active:
                logger.warning("Market %s inactive/closed; skipping order %s", market_id, intent_key)
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

        logger.info(
            "Placing order: market_id=%s asset_id=%s side=%s size=%s limit_price=%s "
            "current_market_exposure=%s current_portfolio_exposure=%s intent_key=%s",
            market_id, asset_id, side, size, limit_price,
            current_market_exposure, current_portfolio_exposure, intent_key,
        )

        try:
            result = await self._submit(order)
            logger.info("submitted order %s result=%s", intent_key, result)
            return result
        except Exception as exc:  # noqa: BLE001
            logger.error("order failed: %s", exc)
            raise
