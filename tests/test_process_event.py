import pytest
from unittest.mock import AsyncMock

from polycopy.config import Settings
from polycopy.main import process_event
from polycopy.risk import RiskLimits
from polycopy.state import PositionTracker


@pytest.mark.asyncio
async def test_process_event_uses_cached_positions_and_updates_state():
    settings = Settings(
        private_key="0xkey",
        target_wallet="0xtarget",
        trader_wallet="0xme",
        copy_factor=1.0,
        min_trade_size=0.1,
    )
    position_tracker = PositionTracker()
    await position_tracker.refresh(
        target_positions=[{"asset_id": "asset1", "size": 1.0, "outcome": "YES", "market": "m1"}],
        our_positions=[{"asset_id": "asset1", "size": 0.25, "outcome": "YES", "market": "m1"}],
    )

    data_api = AsyncMock()
    data_api.fetch_positions = AsyncMock()
    executor = AsyncMock()
    risk_limits = RiskLimits.from_settings(settings)

    event = {
        "asset_id": "asset1",
        "size": 1.0,
        "price": 0.5,
        "outcome": "YES",
        "market": "m1",
        "tx_hash": "0xtx",
        "is_buy": True,
    }

    await process_event(
        event=event,
        settings=settings,
        data_api=data_api,
        executor=executor,
        risk_limits=risk_limits,
        position_tracker=position_tracker,
    )

    assert data_api.fetch_positions.await_count == 0
    executor.place_order.assert_awaited_once()
    _, kwargs = executor.place_order.await_args
    assert kwargs["side"] == "buy"

    target_state, our_state = await position_tracker.snapshot()
    assert target_state.positions["asset1"].size == pytest.approx(2.0)
    assert our_state.positions["asset1"].size == pytest.approx(2.0)
