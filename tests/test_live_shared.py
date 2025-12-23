import time
import uuid

from polycopy.live_shared import LiveViewReader, LiveViewWriter
from polycopy.state import Position


def test_live_view_writer_shared_payload():
    name = f"pcopy-{uuid.uuid4().hex}"
    writer = LiveViewWriter(name=name, size=65536, limit=2)
    reader = LiveViewReader(name=name)
    try:
        writer.update_positions(
            target=[{"asset_id": "a1", "size": 1.0, "outcome": "YES", "market": "mkt", "average_price": 0.5}],
            ours=[Position(asset_id="a1", outcome="YES", size=0.5, market="mkt", average_price=0.45)],
        )
        writer.record_target_trade({"asset_id": "a1", "size": 1.0, "price": 0.6, "outcome": "YES", "market": "mkt", "timestamp": time.time()})
        writer.record_order(asset_id="a1", market="mkt", outcome="YES", side="buy", size=0.5, price=0.6)

        snapshot = reader.read()
        assert snapshot["positions"]["target"][0]["asset_id"] == "a1"
        assert snapshot["positions"]["ours"][0]["asset_id"] == "a1"
        assert len(snapshot["trades"]["target"]) == 1
        assert len(snapshot["trades"]["orders"]) == 1
    finally:
        reader.close()
        writer.close()
        writer.unlink()


def test_live_view_writer_fifo_limit():
    name = f"pcopy-{uuid.uuid4().hex}"
    writer = LiveViewWriter(name=name, size=65536, limit=2)
    reader = LiveViewReader(name=name)
    try:
        writer.record_target_trade({"asset_id": "a1", "size": 1.0, "price": 0.6, "outcome": "YES", "market": "m1"})
        writer.record_target_trade({"asset_id": "a1", "size": 2.0, "price": 0.61, "outcome": "YES", "market": "m1"})
        writer.record_target_trade({"asset_id": "a2", "size": 3.0, "price": 0.7, "outcome": "NO", "market": "m2"})

        snapshot = reader.read()
        assert len(snapshot["trades"]["target"]) == 2  # bounded by limit
        markets = {t["market"] for t in snapshot["trades"]["target"]}
        assert markets == {"m1", "m2"}
    finally:
        reader.close()
        writer.close()
        writer.unlink()
