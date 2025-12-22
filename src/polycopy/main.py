from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any, Dict

try:
    import uvloop
except ImportError:  # pragma: no cover
    uvloop = None

from .clob_exec import ExecutionEngine
from .config import Settings, load_settings
from .credentials import ensure_api_credentials, require_api_credentials
from .data_api import BackstopPoller, DataAPIClient
from .reconcile import reconcile_loop
from .risk import RiskLimits
from .rtds_client import RtdsClient
from .state import IntentStore, PortfolioState
from .util import get_first
from .util import logging as log_util
from .util.time import check_clock_skew

logger = logging.getLogger(__name__)


async def startup_checks(settings: Settings) -> None:
    require_api_credentials(settings)
    async with DataAPIClient(settings.data_api_url, settings.api_key) as client:  # type: ignore[arg-type]
        try:
            await client.fetch_positions(settings.target_wallet)
            await client.fetch_trades(settings.target_wallet, limit=1)
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
) -> None:
    target_state = PortfolioState.from_api(await data_api.fetch_positions(settings.target_wallet))
    our_state = PortfolioState.from_api(await data_api.fetch_positions(settings.trader_wallet))

    asset_id = event.get("asset_id")
    if not asset_id:
        return
    target_pos = target_state.positions.get(asset_id)
    if not target_pos:
        return
    desired = target_pos.size * settings.copy_factor
    current = our_state.positions.get(asset_id)
    delta = desired - (current.size if current else 0.0)
    if abs(delta) < risk_limits.min_trade_size:
        return

    price = float(event.get("price") or target_pos.average_price or 1.0)
    notional = abs(delta) * price
    market_id = target_pos.market if target_pos.market else target_pos.outcome
    await executor.place_order(
        asset_id=asset_id,
        market_id=market_id,
        outcome=target_pos.outcome,
        side="buy" if delta > 0 else "sell",
        size=abs(delta),
        limit_price=price,
        intent_key=f"{event.get('tx_hash')}-{asset_id}",
        target_tx=event.get("tx_hash") or "unknown",
        current_market_exposure=notional,
        current_portfolio_exposure=our_state.notional(),
    )


async def consume_events(
    queue: asyncio.Queue,
    settings: Settings,
    data_api: DataAPIClient,
    executor: ExecutionEngine,
    risk_limits: RiskLimits,
    stop_event: asyncio.Event,
    kill_switch_threshold: int,
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
            )
            failures = 0
        except Exception as exc:  # noqa: BLE001
            failures += 1
            logger.warning("processing failed (%s failures): %s", failures, exc)
            if failures >= kill_switch_threshold:
                logger.error("kill switch triggered, stopping bot")
                stop_event.set()
        finally:
            queue.task_done()


async def refresh_watchlist(
    data_api: DataAPIClient, target_wallet: str, watchlist: set[str], interval: float, stop_event: asyncio.Event
) -> None:
    while not stop_event.is_set():
        try:
            positions = await data_api.fetch_positions(target_wallet)
            watchlist.clear()
            for pos in positions:
                market = get_first(pos, ["market", "market_slug", "event_slug", "eventSlug", "slug"])
                if market:
                    watchlist.add(market)
        except Exception as exc:  # noqa: BLE001
            logger.debug("watchlist refresh failed: %s", exc)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue


async def main_async(argv: list[str] | None = None) -> None:
    if uvloop:
        uvloop.install()
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
    )

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
        asyncio.create_task(
            consume_events(
                queue=queue,
                settings=settings,
                data_api=data_api,
                executor=executor,
                risk_limits=risk_limits,
                stop_event=stop_event,
                kill_switch_threshold=settings.kill_switch_threshold,
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
    await data_api.close()


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":  # pragma: no cover
    main()
