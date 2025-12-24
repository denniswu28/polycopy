from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from typing import Any, Dict
import json
try:
    import uvloop
except ImportError:  # pragma: no cover
    uvloop = None

from dotenv import load_dotenv

from .clob_exec import ExecutionEngine, MarketStatusChecker
from .config import PROJECT_ROOT, Settings, load_settings
from .credentials import ensure_api_credentials, require_api_credentials
from .data_api import BackstopPoller, DataAPIClient
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

logger = logging.getLogger(__name__)
# Limit initial trade CSV backfill to a manageable batch to avoid large downloads on startup.
INITIAL_TRADE_LOG_LIMIT = 200


def _signed_size_from_event(event: Dict[str, Any], size: float) -> float | None:
    side_field = (event.get("side") or "").lower()
    is_buy = event.get("is_buy")
    if isinstance(is_buy, bool):
        return size if is_buy else -size
    if side_field in {"buy", "sell"}:
        return size if side_field == "buy" else -size
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

    if recorder:
        await recorder.record_trade(event)

    market = event.get("market") or ""
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
    if live_view:
        live_view.record_target_trade(
            {
                "asset_id": asset_id,
                "market": market,
                "outcome": outcome,
                "size": signed_size,
                "price": price,
                "side": side,
                "timestamp": event.get("timestamp"),
            }
        )
        await live_view_update_positions(live_view, position_tracker)

    desired = target_pos.size * settings.copy_factor
    current_size = current_pos.size if current_pos else 0.0
    delta = desired - current_size
    if abs(delta) < risk_limits.min_trade_size:
        return

    order_side = _side_from_size(delta)
    if orderbook_manager:
        quote_price = await orderbook_manager.get_best_quote(asset_id, order_side)
        mid_price = await orderbook_manager.get_mid_price(asset_id)
    else:
        quote_price = None
        mid_price = None

    if quote_price is not None:
        price_used = quote_price
    elif price is not None:
        price_used = price
    else:
        price_used = target_pos.average_price or 1.0
        logger.warning("Using fallback price %s for %s (side=%s)", price_used, asset_id, order_side)

    # Use mid price for valuation if available; otherwise fall back to
    # the actual limit price we intend to use.
    exposure_price = mid_price if mid_price is not None else price_used

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

    notional = abs(delta) * exposure_price
    market_id = target_pos.market if target_pos.market else target_pos.outcome
    await executor.place_order(
        asset_id=asset_id,
        market_id=market_id,
        outcome=target_pos.outcome,
        side=order_side,
        size=abs(delta),
        limit_price=price_used,
        intent_key=f"{event.get('tx_hash')}-{asset_id}",
        target_tx=event.get("tx_hash") or "unknown",
        current_market_exposure=notional,
        current_portfolio_exposure=portfolio_notional,
    )
    await position_tracker.apply_our_execution(
        asset_id=asset_id,
        outcome=target_pos.outcome,
        market=market_id,
        size=delta,
        price=price_used,
    )
    if live_view:
        live_view.record_order(
            asset_id=asset_id,
            market=market_id,
            outcome=target_pos.outcome,
            side=order_side,
            size=abs(delta),
            price=price_used,
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
    market_status_checker: MarketStatusChecker | None = None,
) -> None:
    while not stop_event.is_set():
        try:
            positions = await data_api.fetch_positions(target_wallet)
            watchlist.clear()
            for pos in positions:
                market = get_first(pos, ["market", "market_slug", "event_slug", "eventSlug", "slug"])
                asset_id = pos.get("asset_id")

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
    market_status_checker = MarketStatusChecker(settings.gamma_api_url)
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
                market_status_checker=market_status_checker,
            ),
            name="watchlist",
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
