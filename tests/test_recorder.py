import csv
import pytest
from unittest.mock import AsyncMock

from polycopy.config import Settings
from polycopy.main import process_event
from polycopy.recorders import TargetCsvRecorder
from polycopy.risk import RiskLimits
from polycopy.state import Position, PositionTracker


@pytest.mark.asyncio
async def test_recorder_dedupes_trades_and_positions(tmp_path):
    trades_path = tmp_path / "trades.csv"
    positions_path = tmp_path / "positions.csv"
    recorder = TargetCsvRecorder(trades_path=trades_path, positions_path=positions_path)

    event = {
        "tx_hash": "0xtx",
        "asset_id": "asset1",
        "size": 1.0,
        "price": 0.5,
        "outcome": "YES",
        "market": "m1",
        "is_buy": True,
    }
    await recorder.record_trade(event)
    await recorder.record_trade(event)  # duplicate

    pos = Position(asset_id="asset1", outcome="YES", size=1.0, market="m1", average_price=0.5)
    await recorder.record_position(pos)
    await recorder.record_position({"asset_id": "asset1", "size": 1.0, "avg_price": 0.5, "outcome": "YES"})

    # new size should create a second entry
    await recorder.record_position(Position(asset_id="asset1", outcome="YES", size=2.0, market="m1", average_price=0.6))

    with trades_path.open() as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["tx_hash"] == "0xtx"

    with positions_path.open() as handle:
        pos_rows = list(csv.DictReader(handle))
    assert len(pos_rows) == 2
    assert {float(r["size"]) for r in pos_rows} == {1.0, 2.0}


@pytest.mark.asyncio
async def test_process_event_records_trades_and_positions(tmp_path):
    settings = Settings(
        private_key="0xkey",
        target_wallet="0xtarget",
        trader_wallet="0xme",
        copy_factor=1.0,
        min_trade_size=0.1,
    )
    position_tracker = PositionTracker()
    await position_tracker.refresh(
        target_positions=[{"asset_id": "asset1", "size": 0.0, "outcome": "YES", "market": "m1"}],
        our_positions=[{"asset_id": "asset1", "size": 0.0, "outcome": "YES", "market": "m1"}],
    )
    recorder = TargetCsvRecorder(trades_path=tmp_path / "trades.csv", positions_path=tmp_path / "positions.csv")
    data_api = AsyncMock()
    executor = AsyncMock()
    risk_limits = RiskLimits.from_settings(settings)

    event = {
        "asset_id": "asset1",
        "size": 1.0,
        "price": 0.55,
        "outcome": "YES",
        "market": "m1",
        "tx_hash": "0xtx123",
        "is_buy": True,
    }

    await process_event(
        event=event,
        settings=settings,
        data_api=data_api,
        executor=executor,
        risk_limits=risk_limits,
        position_tracker=position_tracker,
        recorder=recorder,
    )

    executor.place_order.assert_awaited_once()
    with (tmp_path / "trades.csv").open() as handle:
        trades = list(csv.DictReader(handle))
    assert len(trades) == 1
    assert trades[0]["tx_hash"] == "0xtx123"

    with (tmp_path / "positions.csv").open() as handle:
        positions = list(csv.DictReader(handle))
    assert len(positions) == 1
    assert positions[0]["asset_id"] == "asset1"
