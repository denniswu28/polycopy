from __future__ import annotations

import asyncio
import time
from typing import Optional

import httpx


def utcnow_ms() -> int:
    return int(time.time() * 1000)


def monotonic_ms() -> int:
    return int(time.monotonic() * 1000)


async def check_clock_skew(reference_url: str = "https://worldtimeapi.org/api/timezone/Etc/UTC") -> Optional[float]:
    """Return clock skew in seconds (positive if local is ahead)."""
    timeout = httpx.Timeout(5.0, read=5.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(reference_url)
            resp.raise_for_status()
            data = resp.json()
            server_ts = data.get("unixtime")
            if server_ts is None:
                return None
            return time.time() - float(server_ts)
    except Exception:
        return None


async def sleep_cancellable(delay: float) -> None:
    """Sleep with cancellation handling."""
    try:
        await asyncio.sleep(delay)
    except asyncio.CancelledError:
        raise
