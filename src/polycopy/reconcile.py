from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict

from .clob_exec import ExecutionEngine
from .data_api import DataAPIClient
from .live_shared import LiveViewWriter
from .orderbook import OrderBookManager
from .risk import RiskLimits, RiskError
from .state import PortfolioState, PositionTracker

logger = logging.getLogger(__name__)


async def reconcile_once(
    *,
    data_api: DataAPIClient,
    executor: ExecutionEngine,
    target_wallet: str,
    our_wallet: str,
    copy_factor: float,
    risk_limits: RiskLimits,
    position_tracker: PositionTracker | None = None,
    live_view: LiveViewWriter | None = None,
    orderbook_manager: OrderBookManager | None = None,
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

    if position_tracker:
        # If dry_run, we don't want to overwrite our simulated positions with empty/stale data
        # unless we want to sync with something. Here we assume dry_run maintains its own state.
        await position_tracker.replace(target_state=target_positions, our_state=our_positions if not dry_run else None)

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
    data_api: DataAPIClient,
    executor: ExecutionEngine,
    target_wallet: str,
    our_wallet: str,
    copy_factor: float,
    risk_limits: RiskLimits,
    interval: float = 30.0,
    stop_event: asyncio.Event,
    position_tracker: PositionTracker | None = None,
    live_view: LiveViewWriter | None = None,
    orderbook_manager: OrderBookManager | None = None,
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
