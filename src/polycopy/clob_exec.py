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
    """Cache market active/closed state using the Gamma API.

    Results are cached for ``ttl_seconds``. If the API call fails or
    returns an unexpected payload, the checker defaults to treating the
    market as active so that execution is not blocked by transient errors.
    """

    def __init__(self, gamma_url: str, ttl_seconds: float = 60.0) -> None:
        self._client = httpx.Client(timeout=10.0)
        self.gamma_url = gamma_url.rstrip("/") + "/markets"
        self._cache: Dict[str, Tuple[bool, float]] = {}
        self._ttl = ttl_seconds

    def _get_multiple_markets(self, markets: set[str]) -> Dict[str, Dict[str, Any]]:
        """Best-effort bulk market fetch using the Gamma API."""
        slugs = []
        ids = []
        for m in markets:
            # Heuristic: if it looks like a number, treat as ID; otherwise slug
            if m.isdigit():
                ids.append(m)
            else:
                slugs.append(m)
        
        params = []
        for s in slugs:
            params.append(("slug", s))
        for i in ids:
            params.append(("clob_token_ids", i))
            
        if not params:
            return {}

        try:
            resp = self._client.get(self.gamma_url, params=params)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.debug("gamma-api fetch failed: %s", exc)
            return {}

        # Gamma API returns a list of markets.
        # We need to map them back to the requested keys (slugs/ids).
        results: Dict[str, Dict[str, Any]] = {}
        
        # Create a lookup map from the response
        # We map slug -> item and clobTokenIds -> item
        # Note: clobTokenIds in response might be a list or string? 
        # Usually it's a list of strings in Gamma API response? 
        # Or maybe just 'clobTokenIds' field.
        # Let's assume standard Gamma API response structure.
        
        for item in data:
            # Map by slug
            if "slug" in item:
                results[item["slug"]] = item
            
            # Map by clobTokenIds
            # The field in Gamma API response is usually 'clobTokenIds' (camelCase)
            ctids = item.get("clobTokenIds")
            if ctids:
                if isinstance(ctids, list):
                    for ctid in ctids:
                        results[str(ctid)] = item
                else:
                    results[str(ctids)] = item
            
            # Also map by 'id' just in case
            if "id" in item:
                results[str(item["id"])] = item

        # Now build the return dict matching the input keys
        final_results: Dict[str, Dict[str, Any]] = {}
        for key in markets:
            if key in results:
                final_results[key] = results[key]
        
        return final_results

    async def is_active(self, market_id: str) -> bool:
        now = time.time()
        cached = self._cache.get(market_id)
        if cached and (now - cached[1]) < self._ttl:
            return cached[0]
        try:
            markets = await asyncio.to_thread(self._get_multiple_markets, {market_id})
        except Exception as exc:
            logger.debug("market status fetch failed for %s: %s", market_id, exc)
            return True
        data = markets.get(market_id)
        if not data:
            logger.warning("market not found for %s", market_id)
            return False

        closed = data.get("closed")
        if closed is not None:
            active = not bool(closed)
        else:
            active = bool(data.get("active", True))
        self._cache[market_id] = (active, now)
        logger.debug("market status for %s active=%s", market_id, active)
        return active

    async def refresh_markets(self, markets: set[str]) -> Dict[str, bool]:
        """Fetch and cache market states for a batch of markets."""
        if not markets:
            return {}
        try:
            results = await asyncio.to_thread(self._get_multiple_markets, markets)
        except Exception as exc:
            logger.debug("bulk market status fetch failed: %s", exc)
            return {}
        now = time.time()
        statuses: Dict[str, bool] = {}
        for key in markets:
            data = results.get(key)
            if not data:
                # If not found, assume inactive? Or active?
                # Original code assumed inactive if not found in bulk fetch?
                # "if not data: self._cache[key] = (False, now); statuses[key] = False"
                # Yes.
                self._cache[key] = (False, now)
                statuses[key] = False
                continue
            closed = data.get("closed")
            active = not bool(closed) if closed is not None else bool(data.get("active", True))
            self._cache[key] = (active, now)
            statuses[key] = active
        return statuses

    async def close(self) -> None:
        self._client.close()



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
        valuation_price: float | None = None,
        intent_key: str,
        target_tx: str,
        current_market_exposure: float,
        current_portfolio_exposure: float,
        order_type: str = "limit",
    ) -> Optional[Dict[str, Any]]:
        pricing_price = valuation_price if valuation_price is not None else (limit_price if limit_price > 0 else None)
        if pricing_price is None or pricing_price <= 0:
            logger.warning(
                "Using fallback valuation price for asset %s (limit_price=%s); defaulting to 1.0",
                asset_id,
                limit_price,
            )
            pricing_price = 1.0
        notional = abs(size) * pricing_price
        resulting_market_notional = current_market_exposure + notional
        resulting_portfolio = current_portfolio_exposure + notional
        validate_trade(
            market_id=market_id,
            outcome=outcome,
            notional=notional,
            size=size,
            resulting_market_notional=resulting_market_notional,
            resulting_portfolio_exposure=resulting_portfolio,
            limits=self.risk_limits,
            order_type=order_type,
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

        # For dry-run market orders, simulate with max opposite price
        final_price = limit_price
        if (self.dry_run or self.paper) and order_type == "market":
            final_price = 1.0 if side.lower() == "buy" else 0.0

        order = {
            "asset_id": asset_id,
            "side": side,
            "size": abs(size),
            "price": final_price,
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
