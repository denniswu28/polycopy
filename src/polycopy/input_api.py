"""Input API: data API clients, market data clients, orderbook access, status checks."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Literal, Mapping, Optional, Set, Tuple
import json
import httpx
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import BookParams
from py_clob_client.exceptions import PolyException

from .util import get_first

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Event normalization / helpers (copied from events.py)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Data API client (copied from data_api.py)
# ---------------------------------------------------------------------------


class DataAPIClient:
    """Thin wrapper around the Polymarket public data API."""

    def __init__(self, base_url: str, api_key: str, timeout: float = 5.0) -> None:
        headers = {"X-API-Key": api_key}
        self._client = httpx.AsyncClient(base_url=base_url, headers=headers, timeout=timeout)

    async def __aenter__(self) -> "DataAPIClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def fetch_trades(self, user: str, limit: int = 50) -> List[dict[str, Any]]:
        logger.info("Fetching trades for user=%s limit=%s", user, limit)
        resp = await self._client.get("/trades", params={"user": user, "limit": limit})
        resp.raise_for_status()
        data = resp.json()
        logger.info(
            "Fetched trades for user=%s limit=%s: %s",
            user, limit, json.dumps(data, indent=2)[:5000],
        )
        return data if isinstance(data, list) else data.get("data", [])

    async def fetch_positions(self, user: str) -> List[dict[str, Any]]:
        logger.info("Fetching positions for user=%s", user)
        resp = await self._client.get("/positions", params={"user": user})
        resp.raise_for_status()
        data = resp.json()
        logger.info(
            "Fetched positions for user=%s: %s",
            user, json.dumps(data, indent=2)[:5000],
        )
        return data if isinstance(data, list) else data.get("data", [])

    async def close(self) -> None:
        await self._client.aclose()


class BackstopPoller:
    """HTTP polling backstop when WebSocket is unavailable."""

    def __init__(
        self,
        client: DataAPIClient,
        target_wallet: str,
        queue: asyncio.Queue,
        interval: float = 1.0,
    ) -> None:
        self.client = client
        self.target_wallet = target_wallet
        self.queue = queue
        self.interval = interval
        self._last_seen: Optional[Tuple[float, str]] = None
        self._running = False

    def _seen(self, ts: float, tx: str) -> bool:
        if self._last_seen is None:
            return False
        last_ts, last_tx = self._last_seen
        # if ts < last_ts:
        #     return True
        if ts == last_ts and tx == last_tx:
            return True
        return False

    async def _publish(self, trade: Mapping[str, Any]) -> None:
        payload = build_trade_event(trade)
        if not payload:
            logger.info("dropping backstop trade missing asset_id: %s", trade)
            return
        try:
            self.queue.put_nowait(payload)
        except asyncio.QueueFull:
            logger.warning("backstop queue full; dropping event %s", payload)

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                trades = await self.client.fetch_trades(self.target_wallet, limit=50)
                for trade in sorted(trades, key=lambda x: x.get("timestamp", 0)):
                    ts = float(trade.get("timestamp", 0))
                    tx = trade.get("transactionHash") or trade.get("txHash") or ""
                    if not tx:
                        continue
                    if self._seen(ts, tx):
                        logger.info("Backstop skipping seen trade: %s", tx)
                        continue
                    logger.info("Backstop found new trade: %s", tx)
                    await self._publish(trade)
                    self._last_seen = (ts, tx)
            except Exception as exc:  # noqa: BLE001
                logger.warning("backstop poll error: %s", exc)
            await asyncio.sleep(self.interval)

    def stop(self) -> None:
        self._running = False


# ---------------------------------------------------------------------------
# Order book manager (copied from orderbook.py)
# ---------------------------------------------------------------------------


# Forward reference for PortfolioState - will be imported at runtime
# to avoid circular import issues
def _get_portfolio_state_class():
    from .state import PortfolioState
    return PortfolioState


@dataclass
class BestQuote:
    asset_id: str
    market: str
    best_bid: float | None
    best_ask: float | None
    last_update_ms: int
    last_source: Literal["book", "price_change", "rest_book", "last_trade", "get_prices"]
    tick_size: float | None = None


class OrderBookManager:
    """Maintains best bid/ask per asset_id and provides pricing."""

    def __init__(
        self,
        data_api: DataAPIClient,
        max_staleness_s: float = 60.0,
        clob_rest_url: str = "https://clob.polymarket.com",
    ) -> None:
        self.data_api = data_api
        self.max_staleness_s = max_staleness_s
        self._quotes: Dict[str, BestQuote] = {}
        self._subscriptions: Set[str] = set()
        self._lock = asyncio.Lock()
        self._clob_client = ClobClient(host=clob_rest_url)
        self._price_fetch_ts: Dict[str, float] = {}
        self._price_inflight: Dict[str, asyncio.Task[Optional[BestQuote]]] = {}

    async def update_from_ws(self, message: Dict) -> None:
        """Process WS message to update quotes."""
        event_type = message.get("event_type")
        if not event_type:
            return

        async with self._lock:
            if event_type == "book":
                self._handle_book(message)
            elif event_type == "price_change":
                self._handle_price_change(message)
            elif event_type == "tick_size_change":
                self._handle_tick_size_change(message)

    def _handle_book(self, message: Dict) -> None:
        asset_id = message.get("asset_id")
        if not asset_id:
            return

        bids = message.get("bids", [])
        asks = message.get("asks", [])

        best_bid = self._extract_best_price(bids)
        best_ask = self._extract_best_price(asks)

        try:
            ts = int(message.get("timestamp", 0))
        except (ValueError, TypeError):
            ts = int(time.time() * 1000)

        self._quotes[asset_id] = BestQuote(
            asset_id=asset_id,
            market=message.get("market", ""),
            best_bid=best_bid,
            best_ask=best_ask,
            last_update_ms=ts,
            last_source="book",
        )
        logger.debug("updated book quote for %s bid=%s ask=%s", asset_id, best_bid, best_ask)

    def _handle_price_change(self, message: Dict) -> None:
        changes = message.get("price_changes", [])
        try:
            ts = int(message.get("timestamp", 0))
        except (ValueError, TypeError):
            ts = int(time.time() * 1000)

        for change in changes:
            asset_id = change.get("asset_id")
            if not asset_id:
                continue

            # price_change event includes best_bid/best_ask snapshot
            best_bid_raw = change.get("best_bid")
            best_ask_raw = change.get("best_ask")

            best_bid = float(best_bid_raw) if best_bid_raw else None
            best_ask = float(best_ask_raw) if best_ask_raw else None

            # If we already have a quote, update it; else create new
            if asset_id in self._quotes:
                q = self._quotes[asset_id]
                q.best_bid = best_bid
                q.best_ask = best_ask
                q.last_update_ms = ts
                q.last_source = "price_change"
            else:
                self._quotes[asset_id] = BestQuote(
                    asset_id=asset_id,
                    market=message.get("market", ""),
                    best_bid=best_bid,
                    best_ask=best_ask,
                    last_update_ms=ts,
                    last_source="price_change",
                )
            logger.debug("price_change for %s bid=%s ask=%s", asset_id, best_bid, best_ask)

    def _handle_tick_size_change(self, message: Dict) -> None:
        asset_id = message.get("asset_id")
        new_tick = message.get("new_tick_size")
        if asset_id and new_tick and asset_id in self._quotes:
            try:
                self._quotes[asset_id].tick_size = float(new_tick)
            except (ValueError, TypeError):
                pass

    @staticmethod
    def _extract_best_price(entries: List[Dict[str, Any]]) -> Optional[float]:
        for entry in entries:
            try:
                price_val = entry.get("price")
                if price_val is None:
                    continue
                return float(price_val)
            except (TypeError, ValueError, AttributeError):
                continue
        return None

    async def _refresh_from_clob(self, asset_id: str) -> Optional[BestQuote]:
        now = time.time()
        async with self._lock:
            last_fetch = self._price_fetch_ts.get(asset_id, 0)
            inflight = self._price_inflight.get(asset_id)
            if inflight:
                result = await inflight
                return result if result is not None else self._quotes.get(asset_id)
            if now - last_fetch < 1.0:
                return self._quotes.get(asset_id)
            task = asyncio.create_task(self._do_fetch_prices(asset_id))
            self._price_inflight[asset_id] = task
        try:
            result = await task
            if result is None:
                async with self._lock:
                    return self._quotes.get(asset_id)
            return result
        finally:
            async with self._lock:
                self._price_inflight.pop(asset_id, None)

    async def _do_fetch_prices(self, asset_id: str) -> Optional[BestQuote]:
        try:
            params = [
                BookParams(token_id=asset_id, side="BUY"),
                BookParams(token_id=asset_id, side="SELL"),
            ]
            resp = await asyncio.to_thread(self._clob_client.get_prices, params)
        except (PolyException, httpx.HTTPError, ValueError, TypeError) as exc:
            logger.debug("clob get_prices failed for %s: %s", asset_id, exc)
            return None

        if not isinstance(resp, dict):
            return None
        entry = resp.get(asset_id, {})
        # get_prices returns taker quotes: BUY maps to best ask, SELL maps to best bid.
        best_ask_raw = entry.get("BUY") if isinstance(entry, dict) else None
        best_bid_raw = entry.get("SELL") if isinstance(entry, dict) else None
        try:
            best_bid = float(best_bid_raw) if best_bid_raw is not None else None
        except (TypeError, ValueError):
            best_bid = None
        try:
            best_ask = float(best_ask_raw) if best_ask_raw is not None else None
        except (TypeError, ValueError):
            best_ask = None

        ts = int(time.time() * 1000)
        quote = BestQuote(
            asset_id=asset_id,
            market="",
            best_bid=best_bid,
            best_ask=best_ask,
            last_update_ms=ts,
            last_source="get_prices",
            tick_size=None,
        )
        async with self._lock:
            self._quotes[asset_id] = quote
            self._price_fetch_ts[asset_id] = time.time()
        return quote

    async def refresh_prices(self, asset_ids: list[str]) -> None:
        tokens = sorted(set(asset_ids))
        if not tokens:
            return
        params = []
        for token in tokens:
            params.append(BookParams(token_id=token, side="BUY"))
            params.append(BookParams(token_id=token, side="SELL"))
        try:
            resp = await asyncio.to_thread(self._clob_client.get_prices, params)
        except (PolyException, httpx.HTTPError, ValueError, TypeError) as exc:
            logger.debug("bulk get_prices failed for %d assets: %s", len(tokens), exc)
            return
        if not isinstance(resp, dict):
            return
        ts = int(time.time() * 1000)
        async with self._lock:
            for token in tokens:
                entry = resp.get(token, {}) if isinstance(resp, dict) else {}
                # BUY corresponds to the best ask price, SELL corresponds to the best bid.
                best_ask_raw = entry.get("BUY") if isinstance(entry, dict) else None
                best_bid_raw = entry.get("SELL") if isinstance(entry, dict) else None
                try:
                    best_bid = float(best_bid_raw) if best_bid_raw is not None else None
                except (TypeError, ValueError):
                    best_bid = None
                try:
                    best_ask = float(best_ask_raw) if best_ask_raw is not None else None
                except (TypeError, ValueError):
                    best_ask = None
                if best_bid is None and best_ask is None:
                    continue
                self._quotes[token] = BestQuote(
                    asset_id=token,
                    market="",
                    best_bid=best_bid,
                    best_ask=best_ask,
                    last_update_ms=ts,
                    last_source="get_prices",
                    tick_size=None,
                )
                self._price_fetch_ts[token] = time.time()

    def _is_stale(self, quote: BestQuote) -> bool:
        now_ms = int(time.time() * 1000)
        return (now_ms - quote.last_update_ms) > (self.max_staleness_s * 1000)

    async def get_best_quote(self, asset_id: str, side: str) -> float | None:
        """
        Get the best price for placing an order.
        For BUY, we want the lowest ASK.
        For SELL, we want the highest BID.
        """
        async with self._lock:
            quote = self._quotes.get(asset_id)
            stale = quote is None or self._is_stale(quote)
        if stale:
            quote = await self._refresh_from_clob(asset_id)
        if not quote or self._is_stale(quote):
            logger.debug("no usable quote for %s side=%s", asset_id, side)
            return None

        price = quote.best_ask if side.lower() == "buy" else quote.best_bid

        if price is None:
            return None

        # Optional: Snap to tick size if known
        if quote.tick_size:
            price = round(price / quote.tick_size) * quote.tick_size

        return price

    async def get_mid_price(self, asset_id: str) -> float | None:
        """Return a mid price for valuation where possible.

        Uses (bid+ask)/2 when both sides are present, with guards for
        obviously thin or crossed markets. Falls back to the single
        available side when only bid or ask is present. Returns None
        if the quote is stale or unusable so that callers can fall back
        to average price or other heuristics.
        """
        async with self._lock:
            quote = self._quotes.get(asset_id)
            stale = quote is None or self._is_stale(quote)
        if stale:
            quote = await self._refresh_from_clob(asset_id)
        if not quote or self._is_stale(quote):
            logger.debug("no usable mid price for %s", asset_id)
            return None

        bid = quote.best_bid
        ask = quote.best_ask

        if bid is None and ask is None:
            return None

        if bid is not None and ask is not None:
            spread = ask - bid
            if spread <= 0:
                mid = (bid + ask) / 2.0
            else:
                max_side = max(abs(bid), abs(ask), 1e-9)
                spread_pct = spread / max_side
                # Polymarket prices live in [0,1]; treat spreads wider than
                # 0.5 absolute or 50% relative as too thin to trust.
                if spread > 0.5 or spread_pct > 0.5:
                    return None
                mid = (bid + ask) / 2.0
        else:
            # Only one side available; still better than nothing.
            mid = bid if bid is not None else ask

        if quote.tick_size:
            mid = round(mid / quote.tick_size) * quote.tick_size

        return mid

    async def ensure_subscribed(self, asset_id: str) -> bool:
        """Mark asset as needing subscription. Returns True if newly added."""
        async with self._lock:
            if asset_id not in self._subscriptions:
                self._subscriptions.add(asset_id)
                return True
            return False

    async def get_subscriptions(self) -> list[str]:
        async with self._lock:
            return list(self._subscriptions)

    async def bootstrap_from_portfolios(self, target, ours) -> None:
        """Subscribe to all assets currently held."""
        async with self._lock:
            for aid, pos in target.positions.items():
                if abs(pos.size) > 0:
                    self._subscriptions.add(aid)
            for aid, pos in ours.positions.items():
                if abs(pos.size) > 0:
                    self._subscriptions.add(aid)

    async def close(self) -> None:
        close_fn = getattr(self._clob_client, "close", None)
        if callable(close_fn):
            await asyncio.to_thread(close_fn)
        else:
            logger.debug("ClobClient provides no close() hook; nothing to clean up")

    async def fetch_price(self, asset_id: str, side: str) -> Optional[float]:
        """Fetch price using CLOB get_prices."""
        try:
            params = [BookParams(token_id=asset_id, side=side.upper())]
            resp = await asyncio.to_thread(self._clob_client.get_prices, params)
            if not isinstance(resp, dict):
                return None
            entry = resp.get(asset_id, {})
            price_raw = entry.get(side.upper()) if isinstance(entry, dict) else None
            if price_raw is None:
                return None
            return float(price_raw)
        except (TypeError, ValueError, KeyError) as exc:
            logger.debug("fetch_price failed for %s %s: %s", asset_id, side, exc)
            return None


# ---------------------------------------------------------------------------
# Market websocket / book client (copied from market_client.py)
# ---------------------------------------------------------------------------


class MarketBookClient:
    """WebSocket client for Polymarket CLOB market channel (public)."""

    def __init__(
        self,
        url: str,
        orderbook_manager: OrderBookManager,
        heartbeat_interval: float = 10.0,
        backoff_seconds: float = 5.0,
    ) -> None:
        self.orderbook_manager = orderbook_manager
        self.heartbeat_interval = heartbeat_interval
        self.backoff_seconds = backoff_seconds
        self._running = False

    async def run(self) -> None:
        self._running = True
        failures = 0
        while self._running:
            try:
                subscriptions = await self.orderbook_manager.get_subscriptions()
                if subscriptions:
                    await self.orderbook_manager.refresh_prices(subscriptions)
                    failures = 0
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as exc:
                failures += 1
                logger.warning("MarketBookClient polling failed: %s (failure %s)", exc, failures)
                await asyncio.sleep(min(self.backoff_seconds * failures, 60))

    def stop(self) -> None:
        self._running = False
