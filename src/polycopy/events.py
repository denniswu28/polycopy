from __future__ import annotations

from typing import Any, Mapping

from .util import get_first


def normalize_side(event: Mapping[str, Any]) -> str:
    """Normalize side values from mixed payload formats."""
    side = (event.get("side") or "").lower()
    if side:
        return side
    if event.get("is_buy") is True or event.get("isBuy") is True:
        return "buy"
    if event.get("is_buy") is False or event.get("isBuy") is False:
        return "sell"
    return ""


def normalize_trade_event(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize a trade payload into the internal event schema."""
    side = normalize_side(payload)
    is_buy = payload.get("is_buy") if "is_buy" in payload else payload.get("isBuy")
    if is_buy is None and side:
        is_buy = side == "buy"
    return {
        "type": "target_trade_event",
        "tx_hash": get_first(payload, ["tx_hash", "transactionHash", "txHash"]),
        "market": get_first(payload, ["market_slug", "market"]),
        "asset_id": get_first(payload, ["asset_id", "assetId", "asset", "conditionId"]),
        "outcome": get_first(payload, ["outcome"], ""),
        "size": payload.get("size"),
        "price": payload.get("price"),
        "is_buy": is_buy,
        "side": side,
        "timestamp": payload.get("timestamp"),
    }


def build_trade_event(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    """Normalize and validate a trade payload for queue ingestion."""
    event = normalize_trade_event(payload)
    if not event.get("asset_id"):
        return None
    return event
