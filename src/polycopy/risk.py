from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Set


class RiskError(Exception):
    """Raised when a trade violates configured risk controls."""


@dataclass
class RiskLimits:
    max_notional_per_trade: float
    max_notional_per_market: float
    max_portfolio_exposure: float
    min_trade_size: float
    blacklist_markets: Set[str] = field(default_factory=set)
    blacklist_outcomes: Set[str] = field(default_factory=set)
    slippage_bps: int = 50

    @classmethod
    def from_settings(cls, settings: "Settings") -> "RiskLimits":  # type: ignore[name-defined]
        return cls(
            max_notional_per_trade=settings.max_notional_per_trade,
            max_notional_per_market=settings.max_notional_per_market,
            max_portfolio_exposure=settings.max_portfolio_exposure,
            min_trade_size=settings.min_trade_size,
            blacklist_markets=set(settings.blacklist_markets),
            blacklist_outcomes=set(settings.blacklist_outcomes),
            slippage_bps=settings.slippage_bps,
        )


def validate_trade(
    *,
    market_id: str,
    outcome: str,
    notional: float,
    resulting_market_notional: float,
    resulting_portfolio_exposure: float,
    limits: RiskLimits,
) -> None:
    if market_id in limits.blacklist_markets:
        raise RiskError(f"market {market_id} is blacklisted")
    if outcome in limits.blacklist_outcomes:
        raise RiskError(f"outcome {outcome} is blacklisted")
    if abs(notional) < limits.min_trade_size:
        raise RiskError(f"trade below min size {limits.min_trade_size}")
    if abs(notional) > limits.max_notional_per_trade:
        raise RiskError(f"trade exceeds per-trade notional {limits.max_notional_per_trade}")
    if resulting_market_notional > limits.max_notional_per_market:
        raise RiskError("market exposure would exceed limit")
    if resulting_portfolio_exposure > limits.max_portfolio_exposure:
        raise RiskError("portfolio exposure would exceed limit")


def cumulative_notional(deltas: Iterable[float], price: float = 1.0) -> float:
    return sum(abs(d) * price for d in deltas)
