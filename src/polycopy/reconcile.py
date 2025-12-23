from __future__ import annotations

import asyncio
import logging
from typing import Dict

from .clob_exec import ExecutionEngine
from .data_api import DataAPIClient
from .live_shared import LiveViewWriter
from .risk import RiskLimits
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
) -> None:
    target_positions = PortfolioState.from_api(await data_api.fetch_positions(target_wallet))
    our_positions = PortfolioState.from_api(await data_api.fetch_positions(our_wallet))

    if position_tracker:
        await position_tracker.replace(target_state=target_positions, our_state=our_positions)
    if live_view:
        live_view.update_positions(target=target_positions, ours=our_positions)

    scaled_target_positions: Dict[str, float] = {
        asset_id: pos.size * copy_factor for asset_id, pos in target_positions.positions.items()
    }

    deltas: Dict[str, float] = {}
    for asset_id, desired_size in scaled_target_positions.items():
        current = our_positions.positions.get(asset_id)
        diff = desired_size - (current.size if current else 0.0)
        if abs(diff) < risk_limits.min_trade_size:
            continue
        deltas[asset_id] = diff

    for asset_id, delta in deltas.items():
        side = "buy" if delta > 0 else "sell"
        # Assume unit price when computing exposure; REST book lookup would refine this.
        notional = abs(delta)
        pos = target_positions.positions.get(asset_id)
        market_id = pos.market if pos and pos.market else (pos.outcome if pos else "unknown")
        outcome = pos.outcome if pos else "unknown"
        await executor.place_order(
            asset_id=asset_id,
            market_id=market_id,
            outcome=outcome,
            side=side,
            size=abs(delta),
            limit_price=1.0,
            intent_key=f"reconcile-{asset_id}",
            target_tx="reconcile",
            current_market_exposure=notional,
            current_portfolio_exposure=notional,
        )


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
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("reconcile iteration failed: %s", exc)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue
