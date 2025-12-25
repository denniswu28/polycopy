import pytest
from polycopy.input_api import normalize_trade_event, normalize_side
from polycopy.exec_engine import PortfolioState

def test_normalize_side():
    assert normalize_side({"side": "BUY"}) == "BUY"
    assert normalize_side({"side": "buy"}) == "BUY"
    assert normalize_side({"side": "SELL"}) == "SELL"
    assert normalize_side({"side": "sell"}) == "SELL"
    
    # Fallbacks should be removed
    assert normalize_side({"is_buy": True}) == ""
    assert normalize_side({"isBuy": True}) == ""
    assert normalize_side({"is_buy": False}) == ""
    assert normalize_side({"isBuy": False}) == ""

def test_normalize_trade_event():
    payload = {
        "eventSlug": "market-slug",
        "asset": "asset-123",
        "outcome": "YES",
        "size": "10.5",
        "price": "0.5",
        "side": "BUY",
        "timestamp": "1234567890"
    }
    normalized = normalize_trade_event(payload)
    assert normalized["market"] == "market-slug"
    assert normalized["asset_id"] == "asset-123"
    assert normalized["outcome"] == "YES"
    assert normalized["size"] == "10.5"
    assert normalized["price"] == "0.5"
    assert normalized["side"] == "BUY"
    assert normalized["is_buy"] is True

    # Test fallback for market
    payload2 = {
        "slug": "market-slug-2",
        "asset": "asset-456",
        "side": "SELL"
    }
    normalized2 = normalize_trade_event(payload2)
    assert normalized2["market"] == "market-slug-2"
    assert normalized2["side"] == "SELL"
    assert normalized2["is_buy"] is False

    # Test removed keys
    payload3 = {
        "market_slug": "should-ignore",
        "assetId": "should-ignore",
        "conditionId": "should-ignore",
        "quantity": "10",
        "side": "BUY"
    }
    normalized3 = normalize_trade_event(payload3)
    assert normalized3["market"] is None
    assert normalized3["asset_id"] is None
    assert normalized3["size"] is None

def test_portfolio_state_from_api():
    items = [
        {
            "asset": "asset-1",
            "outcome": "YES",
            "size": "100",
            "eventSlug": "market-1",
            "avgPrice": "0.6"
        },
        {
            "asset": "asset-2",
            "outcome": "NO",
            "size": "50",
            "slug": "market-2",
            "avgPrice": "0.4"
        }
    ]
    state = PortfolioState.from_api(items)
    assert len(state.positions) == 2
    
    pos1 = state.positions["asset-1"]
    assert pos1.asset_id == "asset-1"
    assert pos1.outcome == "YES"
    assert pos1.size == 100.0
    assert pos1.market == "market-1"
    assert pos1.average_price == 0.6

    pos2 = state.positions["asset-2"]
    assert pos2.asset_id == "asset-2"
    assert pos2.outcome == "NO"
    assert pos2.size == 50.0
    assert pos2.market == "market-2"
    assert pos2.average_price == 0.4

    # Test ignored keys
    items_ignored = [
        {
            "assetId": "asset-3",
            "quantity": "10",
            "market_slug": "market-3",
            "avg_price": "0.5"
        }
    ]
    state_ignored = PortfolioState.from_api(items_ignored)
    assert len(state_ignored.positions) == 0
