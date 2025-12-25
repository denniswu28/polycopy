"""Execution engine: order submission, reconciliation, risk, portfolio state."""

from __future__ import annotations

import asyncio
import copy
import logging
import math
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional, Set, Tuple

import aiosqlite
import httpx

from .config import SYSTEM_MIN_LIMIT_QTY, SYSTEM_MIN_MARKET_NOTIONAL
from .util import get_first

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Risk model / checks (copied from risk.py)
# ---------------------------------------------------------------------------


class RiskError(Exception):
    """Raised when a trade violates configured risk controls."""


@dataclass
class RiskLimits:
    max_notional_per_trade: float
    max_notional_per_market: float
    max_portfolio_exposure: float
    min_trade_size: float
    min_market_order_notional: float = 1.0
    blacklist_markets: Set[str] = field(default_factory=set)
    blacklist_outcomes: Set[str] = field(default_factory=set)
    slippage_bps: int = 50

    @classmethod
    def from_settings(cls, settings: "Settings") -> "RiskLimits":  # type: ignore[name-defined]
        return cls(
            max_notional_per_trade=settings.max_notional_per_trade,
            max_notional_per_market=settings.max_notional_per_market,
            max_portfolio_exposure=settings.max_portfolio_exposure,
            min_trade_size=settings.min_trade_size if settings.min_trade_size is not None else SYSTEM_MIN_LIMIT_QTY,
            min_market_order_notional=(
                settings.min_market_order_notional
                if settings.min_market_order_notional is not None
                else SYSTEM_MIN_MARKET_NOTIONAL
            ),
            blacklist_markets=set(settings.blacklist_markets),
            blacklist_outcomes=set(settings.blacklist_outcomes),
            slippage_bps=settings.slippage_bps,
        )


def validate_trade(
    *,
    market_id: str,
    outcome: str,
    notional: float,
    size: float,
    resulting_market_notional: float,
    resulting_portfolio_exposure: float,
    limits: RiskLimits,
    order_type: str = "limit",
) -> None:
    if market_id in limits.blacklist_markets:
        raise RiskError(f"market {market_id} is blacklisted")
    if outcome in limits.blacklist_outcomes:
        raise RiskError(f"outcome {outcome} is blacklisted")
    order_type_l = order_type.lower()
    if order_type_l == "market":
        if abs(notional) < limits.min_market_order_notional:
            raise RiskError(f"trade below min market notional {limits.min_market_order_notional}")
    else:
        if abs(size) < limits.min_trade_size:
            raise RiskError(f"trade below min size {limits.min_trade_size}")
    if abs(notional) > limits.max_notional_per_trade:
        raise RiskError(f"trade exceeds per-trade notional {limits.max_notional_per_trade}")
    if resulting_market_notional > limits.max_notional_per_market:
        raise RiskError("market exposure would exceed limit")
    if resulting_portfolio_exposure > limits.max_portfolio_exposure:
        raise RiskError("portfolio exposure would exceed limit")


def cumulative_notional(deltas: Iterable[float], price: float = 1.0) -> float:
    return sum(abs(d) * price for d in deltas)


# ---------------------------------------------------------------------------
# Portfolio state & tracking (copied from state.py)
# ---------------------------------------------------------------------------


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
                market=get_first(item, ["market_slug", "market", "event_slug", "eventSlug", "slug"], ""),
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
            if prices and asset_id in prices:
                price = prices[asset_id]
            elif pos.average_price:
                price = pos.average_price
            else:
                price = 1.0
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
        self._last_reconcile_ts: float | None = None
        self._last_copied_trade_ts: float | None = None

    async def refresh(self, *, target_positions: Iterable[dict], our_positions: Iterable[dict]) -> None:
        async with self._lock:
            self.target = PortfolioState.from_api(target_positions)
            self.ours = PortfolioState.from_api(our_positions)
            self._last_reconcile_ts = time.time()

    async def replace(
        self,
        *,
        target_state: PortfolioState,
        our_state: PortfolioState | None = None,
        mark_reconcile: bool = True,
        reconcile_ts: float | None = None,
    ) -> None:
        async with self._lock:
            self.target = target_state
            if our_state is not None:
                self.ours = our_state
            if mark_reconcile:
                self._last_reconcile_ts = reconcile_ts if reconcile_ts is not None else time.time()

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

    async def drop_markets(self, markets: set[str]) -> None:
        if not markets:
            return
        async with self._lock:
            self.target.positions = {
                aid: pos for aid, pos in self.target.positions.items() if pos.market not in markets
            }
            self.ours.positions = {
                aid: pos for aid, pos in self.ours.positions.items() if pos.market not in markets
            }

    async def snapshot(self) -> tuple[PortfolioState, PortfolioState]:
        async with self._lock:
            # Return deep copies to avoid concurrency issues
            return copy.deepcopy(self.target), copy.deepcopy(self.ours)

    async def mark_reconcile(self, timestamp: float | None = None) -> None:
        async with self._lock:
            self._last_reconcile_ts = timestamp if timestamp is not None else time.time()

    async def mark_trade_seen(self, timestamp: float | None) -> None:
        if timestamp is None:
            return
        async with self._lock:
            if self._last_copied_trade_ts is None or timestamp > self._last_copied_trade_ts:
                self._last_copied_trade_ts = timestamp

    async def event_watermark(self) -> float | None:
        async with self._lock:
            candidates = [ts for ts in (self._last_reconcile_ts, self._last_copied_trade_ts) if ts is not None]
            return max(candidates) if candidates else None

    async def is_event_stale(self, timestamp: float | None) -> bool:
        if timestamp is None:
            return False
        watermark = await self.event_watermark()
        return watermark is not None and timestamp <= watermark


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


# ---------------------------------------------------------------------------
# Market status checker (copied from clob_exec.py)
# ---------------------------------------------------------------------------


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
            logger.info("gamma-api fetch failed: %s", exc)
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
            logger.info("market status fetch failed for %s: %s", market_id, exc)
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
        logger.info("market status for %s active=%s", market_id, active)
        return active

    async def refresh_markets(self, markets: set[str]) -> Dict[str, bool]:
        """Fetch and cache market states for a batch of markets."""
        if not markets:
            return {}
        try:
            results = await asyncio.to_thread(self._get_multiple_markets, markets)
        except Exception as exc:
            logger.info("bulk market status fetch failed: %s", exc)
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


# ---------------------------------------------------------------------------
# Execution engine / CLOB client (copied from clob_exec.py)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Reconciliation loop / copy logic (copied from reconcile.py)
# ---------------------------------------------------------------------------


# Import these lazily to avoid circular imports at module load time
def _get_input_api_classes():
    from .input_api import DataAPIClient, OrderBookManager
    return DataAPIClient, OrderBookManager


def _get_output_api_classes():
    from .output_api import LiveViewWriter
    return LiveViewWriter


async def reconcile_once(
    *,
    data_api,  # DataAPIClient
    executor: ExecutionEngine,
    target_wallet: str,
    our_wallet: str,
    copy_factor: float,
    risk_limits: RiskLimits,
    position_tracker: PositionTracker | None = None,
    live_view = None,  # LiveViewWriter | None
    orderbook_manager = None,  # OrderBookManager | None
    dry_run: bool = False,
) -> None:
    target_positions = PortfolioState.from_api(await data_api.fetch_positions(target_wallet))
    
    if not dry_run:
        our_positions = PortfolioState.from_api(await data_api.fetch_positions(our_wallet))
    else:
        if position_tracker:
            _, our_positions = await position_tracker.snapshot()
        else:
            our_positions = PortfolioState()

    # Filter closed markets from view and reconciliation
    if executor.market_status_checker:
        async def _filter_active(state: PortfolioState) -> PortfolioState:
            filtered = PortfolioState()
            for aid, p in state.positions.items():
                mid = p.market
                # If we can't determine market, keep it (safe default)
                if not mid:
                    filtered.positions[aid] = p
                    continue
                if await executor.market_status_checker.is_active(mid):
                    filtered.positions[aid] = p
            return filtered

        target_positions = await _filter_active(target_positions)
        our_positions = await _filter_active(our_positions)
    if risk_limits.blacklist_markets:
        target_positions.positions = {
            aid: pos for aid, pos in target_positions.positions.items() if pos.market not in risk_limits.blacklist_markets
        }
        our_positions.positions = {
            aid: pos for aid, pos in our_positions.positions.items() if pos.market not in risk_limits.blacklist_markets
        }

    if position_tracker:
        # If dry_run, we don't want to overwrite our simulated positions with empty/stale data
        # unless we want to sync with something. Here we assume dry_run maintains its own state.
        await position_tracker.replace(
            target_state=target_positions,
            our_state=our_positions if not dry_run else None,
            reconcile_ts=time.time(),
        )

    if live_view:
        live_view.update_positions(target=target_positions, ours=our_positions)

    scaled_target_positions: Dict[str, float] = {
        asset_id: pos.size * copy_factor for asset_id, pos in target_positions.positions.items()
    }

    deltas: Dict[str, float] = {}
    for asset_id, desired_size in scaled_target_positions.items():
        current = our_positions.positions.get(asset_id)
        diff = desired_size - (current.size if current else 0.0)
        if abs(diff) < 1e-6:
            continue
        if abs(diff) < risk_limits.min_trade_size:
            continue
        deltas[asset_id] = diff

    for asset_id, delta in deltas.items():
        side = "buy" if delta > 0 else "sell"
        pos = target_positions.positions.get(asset_id)
        market_id = pos.market if pos and pos.market else (pos.outcome if pos else "unknown")
        outcome = pos.outcome if pos else "unknown"
        
        price_used = 1.0
        exposure_price = 1.0
        if orderbook_manager:
            await orderbook_manager.ensure_subscribed(asset_id)
            mid_price = await orderbook_manager.get_mid_price(asset_id)
            quote_price = await orderbook_manager.get_best_quote(asset_id, side)
            if quote_price is not None:
                price_used = quote_price
            elif mid_price is not None:
                price_used = mid_price
            else:
                # Try fetching explicit price
                fetched = await orderbook_manager.fetch_price(asset_id, side)
                if fetched is not None:
                    price_used = fetched
                else:
                    logger.warning("No quote found for %s %s, using fallback price 1.0", side, asset_id)

            exposure_price = mid_price if mid_price is not None else price_used
        else:
            exposure_price = price_used

        notional = abs(delta) * exposure_price
        
        try:
            await executor.place_order(
                asset_id=asset_id,
                market_id=market_id,
                outcome=outcome,
                side=side,
                size=abs(delta),
                limit_price=price_used,
                intent_key=f"reconcile-{asset_id}-{time.time_ns()}",
                target_tx="reconcile",
                current_market_exposure=notional,
                current_portfolio_exposure=notional,
                order_type="limit",
            )
            
            if position_tracker:
                await position_tracker.apply_our_execution(
                    asset_id=asset_id,
                    outcome=outcome,
                    market=market_id,
                    size=delta,
                    price=price_used,
                )
                
            if live_view:
                live_view.record_order(
                    asset_id=asset_id,
                    market=market_id,
                    outcome=outcome,
                    side=side,
                    size=abs(delta),
                    price=price_used,
                )
                if position_tracker:
                    _, current_ours = await position_tracker.snapshot()
                    live_view.update_positions(target=target_positions, ours=current_ours)
        except RiskError as exc:
            logger.warning("skipping reconcile order for %s: %s", asset_id, exc)
            continue


async def reconcile_loop(
    *,
    data_api,  # DataAPIClient
    executor: ExecutionEngine,
    target_wallet: str,
    our_wallet: str,
    copy_factor: float,
    risk_limits: RiskLimits,
    interval: float = 30.0,
    stop_event: asyncio.Event,
    position_tracker: PositionTracker | None = None,
    live_view = None,  # LiveViewWriter | None
    orderbook_manager = None,  # OrderBookManager | None
    dry_run: bool = False,
) -> None:
    while not stop_event.is_set():
        try:
            await reconcile_once(
                data_api=data_api,
                executor=executor,
                target_wallet=target_wallet,
                our_wallet=our_wallet,
                copy_factor=copy_factor,
                risk_limits=risk_limits,
                position_tracker=position_tracker,
                live_view=live_view,
                orderbook_manager=orderbook_manager,
                dry_run=dry_run,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("reconcile iteration failed: %s", exc)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue
