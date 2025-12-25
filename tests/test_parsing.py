import asyncio
import json
from typing import cast

import pytest

from polycopy.input_api import BackstopPoller, DataAPIClient
from polycopy.rtds_client import RtdsClient
from polycopy.exec_engine import PortfolioState


def test_portfolio_state_parses_camel_case_payload():
    payload = {
        "asset": "0xasset",
        "outcome": "YES",
        "size": 12,
        "avgPrice": 0.42,
        "eventSlug": "event-slug",
    }

    state = PortfolioState.from_api([payload])

    assert "0xasset" in state.positions
    pos = state.positions["0xasset"]
    assert pos.size == 12
    assert pos.average_price == 0.42
    assert pos.market == "event-slug"
    assert pos.outcome == "YES"


@pytest.mark.asyncio
async def test_backstop_publish_uses_asset_field():
    queue: asyncio.Queue = asyncio.Queue()
    poller = BackstopPoller(client=cast(DataAPIClient, object()), target_wallet="target", queue=queue)
    trade = {"transactionHash": "0xtx", "asset": "0xasset", "price": 1.0, "size": 1, "timestamp": 1}

    await poller._publish(trade)

    event = await queue.get()
    assert event["asset_id"] == "0xasset"
    assert event["tx_hash"] == "0xtx"


@pytest.mark.asyncio
async def test_rtds_handles_asset_field():
    queue: asyncio.Queue = asyncio.Queue()
    client = RtdsClient(url="ws://example", target_wallet="0xabc", queue=queue)
    payload = {
        "proxyWallet": "0xAbC",
        "asset": "0xasset",
        "transactionHash": "0xtx",
        "eventSlug": "market",
        "outcome": "YES",
        "size": 2,
        "price": 0.5,
        "timestamp": 123,
    }
    message = json.dumps({"payload": payload})

    await client._handle_message(message)

    event = await queue.get()
    assert event["asset_id"] == "0xasset"
    assert event["tx_hash"] == "0xtx"
