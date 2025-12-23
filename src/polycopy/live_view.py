from __future__ import annotations

import argparse
import asyncio
import datetime as dt
from typing import Iterable, Mapping

from .live_shared import DEFAULT_SHM_NAME, LiveViewReader


def _clear_terminal() -> None:
    print("\033[2J\033[H", end="")


def _format_ts(value: object | None) -> str:
    try:
        ts = float(value) if value is not None else None
    except (TypeError, ValueError):
        return ""
    if ts is None:
        return ""
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).strftime("%H:%M:%S")


def _coerce_float(value: object | None, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _best_price(target: Mapping[str, object] | None, ours: Mapping[str, object] | None, fallback: float = 1.0) -> float:
    if isinstance(target, Mapping):
        price = _coerce_float(target.get("average_price"), default=0.0)
        if price:
            return price
    if isinstance(ours, Mapping):
        price = _coerce_float(ours.get("average_price"), default=0.0)
        if price:
            return price
    return fallback


def _merge_positions(snapshot: Mapping[str, object]) -> list[dict]:
    positions = snapshot.get("positions") or {}
    prices = snapshot.get("prices") or {}
    target_positions: Iterable[Mapping[str, object]] = positions.get("target", []) if isinstance(positions, Mapping) else []
    our_positions: Iterable[Mapping[str, object]] = positions.get("ours", []) if isinstance(positions, Mapping) else []

    combined: dict[str, dict] = {}
    for pos in target_positions:
        if not isinstance(pos, Mapping):
            continue
        asset_id = str(pos.get("asset_id") or "")
        if not asset_id:
            continue
        combined.setdefault(asset_id, {})["target"] = pos
        combined[asset_id]["market"] = pos.get("market") or combined[asset_id].get("market", "")
        combined[asset_id]["outcome"] = pos.get("outcome") or combined[asset_id].get("outcome", "")
    for pos in our_positions:
        if not isinstance(pos, Mapping):
            continue
        asset_id = str(pos.get("asset_id") or "")
        if not asset_id:
            continue
        combined.setdefault(asset_id, {})["ours"] = pos
        combined[asset_id]["market"] = pos.get("market") or combined[asset_id].get("market", "")
        combined[asset_id]["outcome"] = pos.get("outcome") or combined[asset_id].get("outcome", "")
    for asset_id, price in (prices or {}).items():
        combined.setdefault(str(asset_id), {})["price"] = price
    return [dict({"asset_id": aid}, **data) for aid, data in combined.items()]


def _render_positions(snapshot: Mapping[str, object]) -> str:
    rows = _merge_positions(snapshot)
    if not rows:
        return "Positions (target vs simulated)\n(no data yet)"
    lines = [
        "Positions (target vs simulated)",
        f"{'Market':30} {'Outcome':6} {'TgtQty':>10} {'OurQty':>10} {'Price':>8} {'TgtNot':>10} {'OurNot':>10} {'TgtAvg':>9} {'OurAvg':>9} {'ΔQty':>9} {'ΔNot':>10}",
    ]
    for row in sorted(rows, key=lambda r: r.get("market", "")):
        target = row.get("target") or {}
        ours = row.get("ours") or {}
        price = _coerce_float(row.get("price"), default=0.0)
        if not price:
            price = _best_price(target if isinstance(target, Mapping) else None, ours if isinstance(ours, Mapping) else None)
        tgt_qty = _coerce_float(target.get("size") if isinstance(target, Mapping) else None)
        our_qty = _coerce_float(ours.get("size") if isinstance(ours, Mapping) else None)
        tgt_avg = _coerce_float(target.get("average_price") if isinstance(target, Mapping) else None)
        our_avg = _coerce_float(ours.get("average_price") if isinstance(ours, Mapping) else None)
        tgt_notional = abs(tgt_qty) * price
        our_notional = abs(our_qty) * price
        diff_qty = our_qty - tgt_qty
        diff_notional = our_notional - tgt_notional
        lines.append(
            f"{str(row.get('market',''))[:30]:30} {str(row.get('outcome',''))[:6]:6} "
            f"{tgt_qty:10.2f} {our_qty:10.2f} {price:8.3f} {tgt_notional:10.2f} {our_notional:10.2f} "
            f"{tgt_avg:9.3f} {our_avg:9.3f} {diff_qty:9.2f} {diff_notional:10.2f}"
        )
    return "\n".join(lines)


def _render_trades(snapshot: Mapping[str, object]) -> str:
    trades = []
    trade_section = snapshot.get("trades") if isinstance(snapshot.get("trades"), Mapping) else {}
    if isinstance(trade_section, Mapping):
        trades.extend([(t, "T") for t in trade_section.get("target", []) or []])
        trades.extend([(t, "O") for t in trade_section.get("orders", []) or []])
    if not trades:
        return "Trades / Orders\n(no trades yet)"
    trades.sort(key=lambda item: _coerce_float(item[0].get("timestamp"), 0.0), reverse=True)
    lines = [
        "Trades / Orders (T=target trade, O=simulated order)",
        f"{'Src':3} {'Time':8} {'Side':6} {'Qty':>9} {'Price':>9} {'Outcome':6} {'Market':30}",
    ]
    for entry, label in trades:
        side = (entry.get("side") if isinstance(entry, Mapping) else "") or ""
        qty = _coerce_float(entry.get("size") if isinstance(entry, Mapping) else None)
        price = _coerce_float(entry.get("price") if isinstance(entry, Mapping) else None)
        outcome = str(entry.get("outcome") if isinstance(entry, Mapping) else "")[:6]
        market = str(entry.get("market") if isinstance(entry, Mapping) else "")[:30]
        ts = _format_ts(entry.get("timestamp") if isinstance(entry, Mapping) else None)
        lines.append(f"{label:3} {ts:8} {side.upper():6} {qty:9.3f} {price:9.3f} {outcome:6} {market:30}")
    return "\n".join(lines)


async def _render_loop(*, shm_name: str, refresh: float) -> None:
    try:
        reader = LiveViewReader(name=shm_name)
    except FileNotFoundError as exc:  # pragma: no cover - runtime path
        raise SystemExit(
            f"Shared memory segment '{shm_name}' not found. Start python -m polycopy.main with --dry-run/--paper "
            f"and matching --shm-name (default: {DEFAULT_SHM_NAME})."
        ) from exc
    while True:
        snapshot = reader.read()
        _clear_terminal()
        now = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        print(f"Polycopy live view @ {now} UTC\n")
        print(_render_positions(snapshot))
        print()
        print(_render_trades(snapshot))
        await asyncio.sleep(refresh)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live terminal viewer for dry-run/paper shared state.")
    parser.add_argument("--shm-name", default=DEFAULT_SHM_NAME, help="Shared memory segment name to read from")
    parser.add_argument("--refresh", type=float, default=1.5, help="Refresh interval in seconds")
    return parser.parse_args(argv)


async def main_async(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    await _render_loop(shm_name=args.shm_name, refresh=args.refresh)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":  # pragma: no cover
    main()
