import asyncio
import pytest
from unittest.mock import AsyncMock, Mock

from polycopy.config import Settings
from polycopy.main import _coalesce_events, process_event
from polycopy.risk import RiskLimits
from polycopy.state import PositionTracker


@pytest.mark.asyncio
async def test_process_event_uses_cached_positions_and_updates_state():
    settings = Settings(
        private_key="0xkey",
        target_wallet="0xtarget",
        trader_wallet="0xme",
        copy_factor=1.0,
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
    risk_limits.min_trade_size = 0.1
    risk_limits.min_market_order_notional = 0.1

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
    assert kwargs["limit_price"] == 1.0

    target_state, our_state = await position_tracker.snapshot()
    assert target_state.positions["asset1"].size == pytest.approx(2.0)
    assert our_state.positions["asset1"].size == pytest.approx(2.0)


@pytest.mark.asyncio
async def test_process_event_handles_sell_reversal():
    settings = Settings(
        private_key="0xkey",
        target_wallet="0xtarget",
        trader_wallet="0xme",
        copy_factor=1.0,
    )
    position_tracker = PositionTracker()
    await position_tracker.refresh(
        target_positions=[{"asset_id": "asset1", "size": 2.0, "outcome": "YES", "market": "m1"}],
        our_positions=[{"asset_id": "asset1", "size": 1.0, "outcome": "YES", "market": "m1"}],
    )

    data_api = AsyncMock()
    data_api.fetch_positions = AsyncMock()
    executor = AsyncMock()
    risk_limits = RiskLimits.from_settings(settings)
    risk_limits.min_trade_size = 0.1
    risk_limits.min_market_order_notional = 0.1

    event = {
        "asset_id": "asset1",
        "size": 5.0,
        "price": 0.6,
        "outcome": "YES",
        "market": "m1",
        "tx_hash": "0xtx-sell",
        "is_buy": False,
    }

    await process_event(
        event=event,
        settings=settings,
        data_api=data_api,
        executor=executor,
        risk_limits=risk_limits,
        position_tracker=position_tracker,
    )

    executor.place_order.assert_awaited()
    _, kwargs = executor.place_order.await_args
    assert kwargs["side"] == "sell"
    assert kwargs["limit_price"] == 0.0

    target_state, our_state = await position_tracker.snapshot()
    assert target_state.positions["asset1"].size == pytest.approx(-3.0)
    assert our_state.positions["asset1"].size == pytest.approx(-3.0)


@pytest.mark.asyncio
async def test_coalesce_events_merges_same_side_market():
    queue: asyncio.Queue = asyncio.Queue()
    base = {"asset_id": "asset1", "market": "m1", "size": 2.0, "is_buy": True, "side": "buy", "tx_hash": "tx1"}
    await queue.put({"asset_id": "asset1", "market": "m1", "size": 3.0, "is_buy": True, "tx_hash": "tx2"})
    await queue.put({"asset_id": "asset1", "market": "m1", "size": 1.0, "is_buy": False, "tx_hash": "tx3"})

    merged = await _coalesce_events(base, queue)
    assert merged["size"] == pytest.approx(4.0)
    assert merged["side"] == "buy"
    assert merged["is_buy"] is True
    assert "tx1" in merged["tx_hash"]
    assert "tx2" in merged["tx_hash"]
    assert "tx3" in merged["tx_hash"]
    # All matching trades (including opposite-side) should be consumed
    remaining = []
    while not queue.empty():
        remaining.append(queue.get_nowait())
    assert remaining == []


@pytest.mark.asyncio
async def test_coalesce_events_records_raw_trades():
    queue: asyncio.Queue = asyncio.Queue()
    base = {"asset_id": "asset1", "market": "m1", "size": 2.0, "is_buy": True, "side": "buy", "tx_hash": "tx1"}
    await queue.put({"asset_id": "asset1", "market": "m1", "size": 3.0, "is_buy": True, "tx_hash": "tx2"})

    recorder = AsyncMock()
    live_view = Mock()
    
    await _coalesce_events(base, queue, recorder=recorder, live_view=live_view)    # Should record base and the matching queued event
    assert recorder.record_trade.await_count == 2
    assert live_view.record_target_trade.call_count == 2
