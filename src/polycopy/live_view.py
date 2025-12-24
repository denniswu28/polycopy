from __future__ import annotations

import argparse
import asyncio
import datetime as dt
from typing import Iterable, Mapping

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .live_shared import DEFAULT_SHM_NAME, LiveViewReader


def make_layout() -> Layout:
    layout = Layout()
    layout.split(
        Layout(name="header", size=3),
        Layout(name="main", size=10),
        Layout(name="footer", ratio=1),
    )
    layout["footer"].split_row(
        Layout(name="target_activity"),
        Layout(name="our_activity"),
    )
    return layout


def generate_header() -> Panel:
    grid = Table.grid(expand=True)
    grid.add_column(justify="center", ratio=1)
    grid.add_row(
        "[b]PolyCopy Live View[/b] | " + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    return Panel(grid, style="white on blue")


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


def generate_positions_table(snapshot: Mapping[str, object]) -> Table:
    copy_factor = _coerce_float(snapshot.get("copy_factor"), default=1.0)
    table = Table(title=f"Positions (Target vs Ours) [Copy Factor: {copy_factor:.2f}]", expand=True, border_style="blue")
    table.add_column("Market", style="cyan", no_wrap=True)
    table.add_column("Outcome", style="magenta")
    table.add_column("Tgt Qty", justify="right")
    table.add_column("Adj Tgt Qty", justify="right")
    table.add_column("Our Qty", justify="right")
    table.add_column("Price", justify="right")
    table.add_column("Tgt Notional", justify="right")
    table.add_column("Our Notional", justify="right")
    table.add_column("Diff Qty", justify="right")
    table.add_column("Diff Notional", justify="right")

    rows = _merge_positions(snapshot)
    if not rows:
        return table

    for row in sorted(rows, key=lambda r: r.get("market", "")):
        target = row.get("target") or {}
        ours = row.get("ours") or {}
        price = _coerce_float(row.get("price"), default=0.0)
        if not price:
            price = _best_price(target if isinstance(target, Mapping) else None, ours if isinstance(ours, Mapping) else None)
        
        tgt_qty = _coerce_float(target.get("size") if isinstance(target, Mapping) else None)
        adj_tgt_qty = tgt_qty * copy_factor
        our_qty = _coerce_float(ours.get("size") if isinstance(ours, Mapping) else None)
        
        tgt_notional = abs(tgt_qty) * price
        our_notional = abs(our_qty) * price
        diff_qty = our_qty - adj_tgt_qty
        diff_notional = our_notional - (abs(adj_tgt_qty) * price)

        market_name = str(row.get('market',''))[:30]
        outcome_name = str(row.get('outcome',''))[:6]
        
        diff_style = "red" if abs(diff_notional) > 1.0 else "green"

        table.add_row(
            market_name,
            outcome_name,
            f"{tgt_qty:.2f}",
            f"{adj_tgt_qty:.2f}",
            f"{our_qty:.2f}",
            f"{price:.3f}",
            f"{tgt_notional:.2f}",
            f"{our_notional:.2f}",
            f"[{diff_style}]{diff_qty:.2f}[/{diff_style}]",
            f"[{diff_style}]{diff_notional:.2f}[/{diff_style}]",
        )
    return table


def generate_target_activity_table(snapshot: Mapping[str, object], max_rows: int = 10) -> Table:
    table = Table(title="Target Activity", expand=True, border_style="green")
    table.add_column("Time", style="dim")
    table.add_column("Market")
    table.add_column("Side")
    table.add_column("Size", justify="right")
    table.add_column("Price", justify="right")

    trades_data = snapshot.get("trades") or {}
    if not isinstance(trades_data, dict):
        trades_data = {}
        
    target_trades = trades_data.get("target") or []
    
    items = []
    for t in target_trades:
        if isinstance(t, dict):
            items.append(t)
            
    # Sort by timestamp descending
    items.sort(key=lambda x: _coerce_float(x.get("timestamp"), 0), reverse=True)
    
    for item in items[:max_rows]:
        ts = _format_ts(item.get("timestamp"))
        market = str(item.get("market", ""))[:20]
        side = str(item.get("side", ""))
        size = _coerce_float(item.get("size"))
        price = _coerce_float(item.get("price"))
        
        color = "green" if side.lower() == "buy" else "red"
        
        table.add_row(
            ts,
            market,
            f"[{color}]{side}[/{color}]",
            f"{size:.2f}",
            f"{price:.3f}",
        )
        
    return table


def generate_our_activity_table(snapshot: Mapping[str, object], max_rows: int = 10) -> Table:
    table = Table(title="Our/Sim Activity", expand=True, border_style="yellow")
    table.add_column("Time", style="dim")
    table.add_column("Market")
    table.add_column("Side")
    table.add_column("Size", justify="right")
    table.add_column("Price", justify="right")

    trades_data = snapshot.get("trades") or {}
    if not isinstance(trades_data, dict):
        trades_data = {}
        
    orders = trades_data.get("orders") or []
    
    items = []
    for o in orders:
        if isinstance(o, dict):
            items.append(o)
            
    # Sort by timestamp descending
    items.sort(key=lambda x: _coerce_float(x.get("timestamp"), 0), reverse=True)
    
    for item in items[:max_rows]:
        ts = _format_ts(item.get("timestamp"))
        market = str(item.get("market", ""))[:20]
        side = str(item.get("side", ""))
        size = _coerce_float(item.get("size"))
        price = _coerce_float(item.get("price"))
        
        color = "green" if side.lower() == "buy" else "red"
        
        table.add_row(
            ts,
            market,
            f"[{color}]{side}[/{color}]",
            f"{size:.2f}",
            f"{price:.3f}",
        )
        
    return table


async def run_live_view(shm_name: str) -> None:
    reader = LiveViewReader(name=shm_name)
    layout = make_layout()
    console = Console()
    
    with Live(layout, refresh_per_second=4, screen=True, console=console) as live:
        while True:
            try:
                snapshot = reader.read()
                layout["header"].update(generate_header())
                
                positions_table = generate_positions_table(snapshot)
                layout["main"].update(positions_table)
                
                # Dynamic sizing for positions table
                # Overhead: Title/Header/Borders ~ 6 lines
                table_rows = len(positions_table.rows)
                target_height = table_rows + 6
                
                # Ensure footer has at least 8 lines
                available_height = console.height - 3 # minus header
                max_main_height = max(5, available_height - 8)
                
                layout["main"].size = min(target_height, max_main_height)
                
                footer_height = console.height - 3 - layout["main"].size
                # Estimate rows: height - 2 (borders) - 1 (header) = height - 3. 
                # Let's use height - 4 to be safe.
                activity_rows = max(0, footer_height - 4)

                layout["target_activity"].update(generate_target_activity_table(snapshot, max_rows=activity_rows))
                layout["our_activity"].update(generate_our_activity_table(snapshot, max_rows=activity_rows))
            except Exception:
                # In case of read error or empty shm, just wait
                pass
            await asyncio.sleep(0.25)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shm-name", default=DEFAULT_SHM_NAME)
    args = parser.parse_args()
    
    try:
        asyncio.run(run_live_view(args.shm_name))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
