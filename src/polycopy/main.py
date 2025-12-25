from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict
import json
import httpx
try:
    import uvloop
except ImportError:  # pragma: no cover
    uvloop = None

from dotenv import load_dotenv

from .clob_exec import ExecutionEngine, MarketStatusChecker
from .config import PROJECT_ROOT, Settings, load_settings, MARKET_STATUS_TTL_MULTIPLIER
from .credentials import ensure_api_credentials, require_api_credentials
from .data_api import BackstopPoller, DataAPIClient
from .events import normalize_side
from .live_shared import LiveViewWriter
from .market_client import MarketBookClient
from .orderbook import OrderBookManager
from .reconcile import reconcile_loop
from .recorders import TargetCsvRecorder
from .risk import RiskLimits, RiskError
from .rtds_client import RtdsClient
from .state import IntentStore, PositionTracker, PortfolioState
from .util import get_first
from .util import logging as log_util
from .util.time import check_clock_skew
from py_clob_client.exceptions import PolyException

logger = logging.getLogger(__name__)
# Limit initial trade CSV backfill to a manageable batch to avoid large downloads on startup.
INITIAL_TRADE_LOG_LIMIT = 200
EVENT_INTENT_PREFIX = "event"


def _normalize_timestamp(value: Any) -> float | None:
    if value is None:
        return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    if ts > 1e15:
        return ts / 1e9
    if ts > 1e12:
        return ts / 1000.0
    if ts > 1e10:
        return ts / 1000.0
    return ts


def _extract_tx_hashes(event: Dict[str, Any]) -> list[str]:
    raw = event.get("tx_hash")
    if not raw:
        return []
    if isinstance(raw, str):
        return [part for part in raw.split("|") if part]
    return []


def _event_intent_key(asset_id: str) -> str:
    return f"{EVENT_INTENT_PREFIX}-{asset_id}"


async def _should_process_event(
    event: Dict[str, Any],
    position_tracker: PositionTracker | None,
    intent_store: IntentStore | None,
    risk_limits: RiskLimits,
) -> bool:
    asset_id = event.get("asset_id")
    if not asset_id:
        return False
    market = event.get("market") or ""
    if market and market in risk_limits.blacklist_markets:
        logger.debug("discarding event for blacklisted market %s", market)
        return False
    event_ts = _normalize_timestamp(event.get("timestamp"))
    if position_tracker and await position_tracker.is_event_stale(event_ts):
        logger.debug("discarding stale event before watermark: %s", event.get("tx_hash"))
        return False
    tx_hashes = _extract_tx_hashes(event)
    if intent_store and tx_hashes:
        intent_key = _event_intent_key(asset_id)
        seen = 0
        for tx_hash in tx_hashes:
            if await intent_store.seen(tx_hash, intent_key):
                seen += 1
        if seen == len(tx_hashes):
            logger.debug("discarding already-processed event(s): %s", tx_hashes)
            return False
    return True


async def _mark_event_seen(
    event: Dict[str, Any],
    position_tracker: PositionTracker | None,
    intent_store: IntentStore | None,
) -> None:
    event_ts = _normalize_timestamp(event.get("timestamp"))
    if position_tracker:
        await position_tracker.mark_trade_seen(event_ts)
    if intent_store:
        asset_id = event.get("asset_id")
        if not asset_id:
            return
        intent_key = _event_intent_key(asset_id)
        for tx_hash in _extract_tx_hashes(event):
            await intent_store.record_intent_if_new(tx_hash, intent_key)


def _signed_size_from_event(event: Dict[str, Any], size: float) -> float | None:
    side = normalize_side(event)
    if side == "buy":
        return size
    if side == "sell":
        return -size
    if size < 0:
        # Some feeds encode sells as negative sizes even without an explicit side flag.
        return size
    return None


def _side_from_size(value: float) -> str:
    if value > 0:
        return "buy"
    if value < 0:
        return "sell"
    return ""


async def _coalesce_events(
    event: Dict[str, Any],
    queue: asyncio.Queue,
    recorder: TargetCsvRecorder | None = None,
    live_view: LiveViewWriter | None = None,
    event_guard: Callable[[Dict[str, Any]], Awaitable[bool]] | None = None,
) -> Dict[str, Any]:
    """Combine queued events on the same market/asset and side into a single payload."""
    raw_size = event.get("size")
    try:
        base_size = float(raw_size)
    except (TypeError, ValueError):
        return event
    base_signed = _signed_size_from_event(event, base_size)
    if base_signed is None:
        return event

    # Record the initial event (raw)
    if recorder:
        await recorder.record_trade(event)
    if live_view:
        live_view.record_target_trade(event)

    base_market = event.get("market") or ""
    base_asset = event.get("asset_id")
    base_side = _side_from_size(base_signed)
    total_signed = base_signed
    tx_hashes = {event.get("tx_hash")} if event.get("tx_hash") else set()
    latest_ts = event.get("timestamp")
    unmatched: list[Dict[str, Any]] = []

    while True:
        try:
            candidate = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        else:
            try:
                queue.task_done()
            except ValueError:
                pass
        if event_guard:
            try:
                if not await event_guard(candidate):
                    continue
            except Exception:  # noqa: BLE001
                logger.debug("event guard failed; keeping candidate", exc_info=True)
        cand_raw_size = candidate.get("size")
        try:
            cand_size = float(cand_raw_size)
        except (TypeError, ValueError):
            unmatched.append(candidate)
            continue
        cand_signed = _signed_size_from_event(candidate, cand_size)
        if (
            cand_signed is not None
            and (candidate.get("market") or "") == base_market
            and candidate.get("asset_id") == base_asset
        ):
            # Record the consumed candidate (raw)
            if recorder:
                await recorder.record_trade(candidate)
            if live_view:
                live_view.record_target_trade(candidate)

            total_signed += cand_signed
            if candidate.get("tx_hash"):
                tx_hashes.add(candidate.get("tx_hash"))
            try:
                c_ts = float(candidate.get("timestamp"))
                if latest_ts is None or c_ts > float(latest_ts):
                    latest_ts = c_ts
            except (TypeError, ValueError):
                pass
        else:
            unmatched.append(candidate)

    for item in unmatched:
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:
            break

    merged = dict(event)
    net_side = _side_from_size(total_signed)
    merged["size"] = abs(total_signed)
    merged["side"] = net_side
    merged["is_buy"] = net_side == "buy"
    if tx_hashes:
        merged["tx_hash"] = "|".join(sorted(tx_hashes))
    if latest_ts is not None:
        merged["timestamp"] = latest_ts
    return merged


async def live_view_update_positions(writer: LiveViewWriter, tracker: PositionTracker) -> None:
    target_state, our_state = await tracker.snapshot()
    writer.update_positions(target=target_state, ours=our_state)


async def startup_checks(settings: Settings) -> None:
    require_api_credentials(settings)
    async with DataAPIClient(settings.data_api_url, settings.api_key) as client:  # type: ignore[arg-type]
        try:
            positions = await client.fetch_positions(settings.target_wallet)
            trades = await client.fetch_trades(settings.target_wallet, limit=50)

            logger.info("Startup positions for target_wallet=%s:\n%s",
                        settings.target_wallet,
                        json.dumps(positions, indent=2)[:5000])  # truncate if huge
            logger.info("Startup trades for target_wallet=%s:\n%s",
                        settings.target_wallet,
                        json.dumps(trades, indent=2)[:5000])

            our_positions = await client.fetch_positions(settings.trader_wallet)
            logger.info("Startup positions for trader_wallet=%s:\n%s",
                        settings.trader_wallet,
                        json.dumps(our_positions, indent=2)[:5000])

        except Exception as exc:  # noqa: BLE001
            raise SystemExit(f"Startup check failed: data API unreachable ({exc})") from exc
    skew = await check_clock_skew()
    if skew is not None and abs(skew) > 5:
        raise SystemExit(f"Local clock skew too high ({skew:.2f}s)")


async def process_event(
    *,
    event: Dict[str, Any],
    settings: Settings,
    data_api: DataAPIClient,
    executor: ExecutionEngine,
    risk_limits: RiskLimits,
    position_tracker: PositionTracker,
    orderbook_manager: OrderBookManager | None = None,
    recorder: TargetCsvRecorder | None = None,
    live_view: LiveViewWriter | None = None,
) -> None:
    asset_id = event.get("asset_id")
    if not asset_id:
        return
    
    if orderbook_manager:
        await orderbook_manager.ensure_subscribed(asset_id)

    raw_size = event.get("size")
    if raw_size is None:
        return
    try:
        size = float(raw_size)
    except (TypeError, ValueError):
        return

    signed_size = _signed_size_from_event(event, size)
    if signed_size is None:
        return

    market = event.get("market") or ""
    if market and market in risk_limits.blacklist_markets:
        logger.debug("ignoring event for blacklisted market %s", market)
        return
    if market and executor.market_status_checker:
        if not await executor.market_status_checker.is_active(market):
            logger.debug("ignoring event for closed market %s", market)
            return

    outcome = event.get("outcome") or ""
    price_val = event.get("price")
    try:
        price = float(price_val) if price_val is not None else None
    except (TypeError, ValueError):
        price = None
    side = _side_from_size(signed_size)

    target_pos, current_pos, _ = await position_tracker.update_target_from_trade(
        asset_id=asset_id,
        outcome=outcome,
        market=market,
        size=signed_size,
        price=price,
    )
    if recorder:
        await recorder.record_position(target_pos)
    await _mark_event_seen(event, position_tracker, executor.intent_store)
    if live_view:
        await live_view_update_positions(live_view, position_tracker)

    desired = target_pos.size * settings.copy_factor
    current_size = current_pos.size if current_pos else 0.0
    delta = desired - current_size

    order_side = _side_from_size(delta)
    quote_price = None
    mid_price = None
    if orderbook_manager:
        quote_price = await orderbook_manager.get_best_quote(asset_id, order_side)
        mid_price = await orderbook_manager.get_mid_price(asset_id)

    valuation_price = None
    if quote_price is not None:
        valuation_price = quote_price
    elif mid_price is not None:
        valuation_price = mid_price
    elif price is not None:
        valuation_price = price
    else:
        valuation_price = target_pos.average_price or 1.0
        logger.warning("Using fallback price %s for %s (side=%s)", valuation_price, asset_id, order_side)

    # Use mid price for valuation if available; otherwise fall back to
    # the actual limit price we intend to use.
    exposure_price = mid_price if mid_price is not None else valuation_price
    limit_price = settings.buy_limit_price if order_side == "buy" else settings.sell_limit_price

    # Recompute portfolio notional using mid prices across our book.
    _, our_state = await position_tracker.snapshot()
    mid_prices: Dict[str, float] = {}
    if orderbook_manager:
        # Create a list of keys to iterate over to avoid "dictionary changed size during iteration"
        # if our_state.positions is modified concurrently (though snapshot should return a copy or be safe)
        # PositionTracker.snapshot returns deepcopy of states, so it should be safe, but let's be defensive.
        asset_ids = list(our_state.positions.keys())
        for aid in asset_ids:
            mp = await orderbook_manager.get_mid_price(aid)
            if mp is not None:
                mid_prices[aid] = mp

    portfolio_notional = our_state.notional(prices=mid_prices)

    pricing_for_exposure = exposure_price if exposure_price is not None else 1.0
    if exposure_price is None:
        logger.warning("Exposure price missing for %s; using fallback %s", asset_id, pricing_for_exposure)
    notional = abs(delta) * pricing_for_exposure
    if notional < risk_limits.min_market_order_notional:
        return

    market_id = target_pos.market if target_pos.market else target_pos.outcome
    await executor.place_order(
        asset_id=asset_id,
        market_id=market_id,
        outcome=target_pos.outcome,
        side=order_side,
        size=abs(delta),
        limit_price=limit_price,
        valuation_price=exposure_price or valuation_price,
        intent_key=f"{event.get('tx_hash')}-{asset_id}",
        target_tx=event.get("tx_hash") or "unknown",
        current_market_exposure=notional,
        current_portfolio_exposure=portfolio_notional,
        order_type="market",
    )
    await position_tracker.apply_our_execution(
        asset_id=asset_id,
        outcome=target_pos.outcome,
        market=market_id,
        size=delta,
        price=quote_price or valuation_price or limit_price,
    )
    if live_view:
        live_view.record_order(
            asset_id=asset_id,
            market=market_id,
            outcome=target_pos.outcome,
            side=order_side,
            size=abs(delta),
            price=limit_price,
        )
        await live_view_update_positions(live_view, position_tracker)


async def consume_events(
    queue: asyncio.Queue,
    settings: Settings,
    data_api: DataAPIClient,
    executor: ExecutionEngine,
    risk_limits: RiskLimits,
    stop_event: asyncio.Event,
    kill_switch_threshold: int,
    position_tracker: PositionTracker,
    orderbook_manager: OrderBookManager,
    recorder: TargetCsvRecorder | None = None,
    live_view: LiveViewWriter | None = None,
) -> None:
    failures = 0
    while not stop_event.is_set():
        try:
            event = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        try:
            # Coalesce backlog items in the consumer; this loop is single-consumer so we have a consistent snapshot.
            if not await _should_process_event(event, position_tracker, executor.intent_store, risk_limits):
                continue
            event = await _coalesce_events(
                event,
                queue,
                recorder=recorder,
                live_view=live_view,
                event_guard=lambda e: _should_process_event(e, position_tracker, executor.intent_store, risk_limits),
            )
            await process_event(
                event=event,
                settings=settings,
                data_api=data_api,
                executor=executor,
                risk_limits=risk_limits,
                position_tracker=position_tracker,
                orderbook_manager=orderbook_manager,
                recorder=recorder,
                live_view=live_view,
            )
            failures = 0
        except RiskError as exc:
            logger.warning("risk check failed: %s", exc)
            # Do not increment failures for risk errors (e.g. min trade size)
        except Exception as exc:  # noqa: BLE001
            failures += 1
            logger.warning("processing failed (%s failures): %s", failures, exc)
            if failures >= kill_switch_threshold:
                logger.error("kill switch triggered, stopping bot")
                stop_event.set()
        finally:
            queue.task_done()
async def refresh_watchlist(
    data_api: DataAPIClient,
    target_wallet: str,
    watchlist: set[str],
    interval: float,
    stop_event: asyncio.Event,
    orderbook_manager: OrderBookManager,
    risk_limits: RiskLimits,
    market_status_checker: MarketStatusChecker | None = None,
) -> None:
    while not stop_event.is_set():
        try:
            positions = await data_api.fetch_positions(target_wallet)
            watchlist.clear()
            for pos in positions:
                market = get_first(pos, ["market", "market_slug", "event_slug", "eventSlug", "slug"])
                asset_id = pos.get("asset_id")

                if market and market in risk_limits.blacklist_markets:
                    logger.debug("skipping blacklisted market %s", market)
                    continue

                if market and market_status_checker:
                    if not await market_status_checker.is_active(market):
                        logger.debug("skipping closed market %s", market)
                        continue

                if market:
                    watchlist.add(market)
                if asset_id:
                    await orderbook_manager.ensure_subscribed(asset_id)
        except Exception as exc:  # noqa: BLE001
            logger.debug("watchlist refresh failed: %s", exc)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue


async def monitor_closed_markets(
    *,
    position_tracker: PositionTracker,
    market_status_checker: MarketStatusChecker,
    risk_limits: RiskLimits,
    interval: float,
    stop_event: asyncio.Event,
    live_view: LiveViewWriter | None = None,
) -> None:
    while not stop_event.is_set():
        target_state, our_state = await position_tracker.snapshot()
        markets = {
            pos.market for pos in list(target_state.positions.values()) + list(our_state.positions.values()) if pos.market
        }
        try:
            statuses = await market_status_checker.refresh_markets(set(markets))
        except (PolyException, httpx.HTTPError, ValueError) as exc:
            logger.debug("market status refresh failed: %s", exc)
            statuses = {}
        closed_markets = {mid for mid, active in statuses.items() if not active}
        if closed_markets:
            logger.info(
                "Detected closed markets %s; blacklisting and clearing related positions",
                closed_markets,
            )
            risk_limits.blacklist_markets.update(closed_markets)
            await position_tracker.drop_markets(closed_markets)
            if live_view:
                await live_view_update_positions(live_view, position_tracker)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue


async def main_async(argv: list[str] | None = None) -> None:
    if uvloop:
        uvloop.install()
    load_dotenv(PROJECT_ROOT / ".env")
    log_util.setup_logging()
    settings, args = load_settings(argv)
    ensure_api_credentials(settings)

    if args.healthcheck:
        await startup_checks(settings)
        print("healthy")
        return

    await startup_checks(settings)

    queue: asyncio.Queue = asyncio.Queue(maxsize=settings.queue_maxsize)
    data_api = DataAPIClient(settings.data_api_url, settings.api_key)  # type: ignore[arg-type]
    risk_limits = RiskLimits.from_settings(settings)
    intent_store = IntentStore(settings.db_path)
    market_status_checker = MarketStatusChecker(
        settings.gamma_api_url,
        ttl_seconds=settings.http_poll_interval * MARKET_STATUS_TTL_MULTIPLIER,
    )
    executor = ExecutionEngine(
        rest_url=settings.clob_rest_url,
        api_key=settings.api_key,  # type: ignore[arg-type]
        api_secret=settings.api_secret,  # type: ignore[arg-type]
        api_passphrase=settings.api_passphrase,
        private_key=settings.private_key,
        intent_store=intent_store,
        risk_limits=risk_limits,
        wallet_address=settings.trader_wallet,
        dry_run=settings.dry_run,
        paper=settings.paper_mode,
        market_status_checker=market_status_checker,
    )
    recorder: TargetCsvRecorder | None = None
    live_view_writer: LiveViewWriter | None = None
    if settings.dry_run or settings.paper_mode:
        base_dir = Path(settings.db_path).resolve().parent
        recorder = TargetCsvRecorder(
            trades_path=base_dir / "target_trades.csv",
            positions_path=base_dir / "target_positions.csv",
        )
        live_view_writer = LiveViewWriter(copy_factor=settings.copy_factor)
    position_tracker = PositionTracker()
    target_positions = await data_api.fetch_positions(settings.target_wallet)
    our_positions = await data_api.fetch_positions(settings.trader_wallet)
    await position_tracker.refresh(target_positions=target_positions, our_positions=our_positions)

    orderbook_manager = OrderBookManager(data_api=data_api, clob_rest_url=settings.clob_rest_url)
    market_client = MarketBookClient(
        url="wss://ws-subscriptions-clob.polymarket.com/ws/market",
        orderbook_manager=orderbook_manager,
        heartbeat_interval=settings.http_poll_interval,
    )
    await orderbook_manager.bootstrap_from_portfolios(
        PortfolioState.from_api(target_positions),
        PortfolioState.from_api(our_positions),
    )

    initial_trades: list[dict] | None = None
    if recorder:
        await recorder.record_positions(target_positions)
        try:
            initial_trades = await data_api.fetch_trades(settings.target_wallet, limit=INITIAL_TRADE_LOG_LIMIT)
            await recorder.record_trades(initial_trades)
        except Exception:  # noqa: BLE001
            logger.debug("initial trade recording failed", exc_info=True)
    if live_view_writer:
        live_view_writer.update_positions(target=target_positions, ours=our_positions)
        if initial_trades:
            live_view_writer.seed_trades(initial_trades)

    stop_event = asyncio.Event()
    watchlist: set[str] = set()

    async def get_watchlist() -> set[str]:
        return set(watchlist)

    rtds = RtdsClient(
        url=settings.rtds_ws_url,
        target_wallet=settings.target_wallet,
        queue=queue,
        heartbeat_interval=settings.ws_heartbeat_interval,
        backoff_seconds=settings.ws_backoff_seconds,
        watchlist_provider=get_watchlist,
    )
    backstop = BackstopPoller(
        client=data_api,
        target_wallet=settings.target_wallet,
        queue=queue,
        interval=settings.http_poll_interval,
    )

    tasks = [
        asyncio.create_task(rtds.run(), name="rtds"),
        asyncio.create_task(backstop.run(), name="backstop"),
        asyncio.create_task(market_client.run(), name="market_book"),
        asyncio.create_task(
            consume_events(
                queue=queue,
                settings=settings,
                data_api=data_api,
                executor=executor,
                risk_limits=risk_limits,
                stop_event=stop_event,
                kill_switch_threshold=settings.kill_switch_threshold,
                position_tracker=position_tracker,
                orderbook_manager=orderbook_manager,
                recorder=recorder,
                live_view=live_view_writer,
            ),
            name="consumer",
        ),
        asyncio.create_task(
            refresh_watchlist(
                data_api=data_api,
                target_wallet=settings.target_wallet,
                watchlist=watchlist,
                interval=settings.watchlist_refresh_interval,
                stop_event=stop_event,
                orderbook_manager=orderbook_manager,
                risk_limits=risk_limits,
                market_status_checker=market_status_checker,
            ),
            name="watchlist",
        ),
        asyncio.create_task(
            monitor_closed_markets(
                position_tracker=position_tracker,
                market_status_checker=market_status_checker,
                risk_limits=risk_limits,
                interval=settings.http_poll_interval,
                stop_event=stop_event,
                live_view=live_view_writer,
            ),
            name="market_status_monitor",
        ),
        asyncio.create_task(
            reconcile_loop(
                data_api=data_api,
                executor=executor,
                target_wallet=settings.target_wallet,
                our_wallet=settings.trader_wallet,
                copy_factor=settings.copy_factor,
                risk_limits=risk_limits,
                interval=settings.reconcile_interval,
                stop_event=stop_event,
                position_tracker=position_tracker,
                live_view=live_view_writer,
                orderbook_manager=orderbook_manager,
                dry_run=settings.dry_run or settings.paper_mode,
            ),
            name="reconcile",
        ),
    ]

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await executor.close()
    if orderbook_manager:
        await orderbook_manager.close()
    await data_api.close()
    if live_view_writer:
        live_view_writer.close()
        live_view_writer.unlink()


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":  # pragma: no cover
    main()
