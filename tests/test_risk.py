import pytest

from polycopy.risk import RiskError, RiskLimits, validate_trade


def test_trade_limits_enforced():
    limits = RiskLimits(
        max_notional_per_trade=100.0,
        max_notional_per_market=150.0,
        max_portfolio_exposure=500.0,
        min_trade_size=10.0,
        blacklist_markets={"m1"},
        blacklist_outcomes={"bad"},
    )

    with pytest.raises(RiskError):
        validate_trade(
            market_id="m1",
            outcome="o1",
            notional=50.0,
            resulting_market_notional=50.0,
            resulting_portfolio_exposure=50.0,
            limits=limits,
        )

    with pytest.raises(RiskError):
        validate_trade(
            market_id="m2",
            outcome="bad",
            notional=50.0,
            resulting_market_notional=50.0,
            resulting_portfolio_exposure=50.0,
            limits=limits,
        )

    with pytest.raises(RiskError):
        validate_trade(
            market_id="m2",
            outcome="ok",
            notional=5.0,
            resulting_market_notional=5.0,
            resulting_portfolio_exposure=5.0,
            limits=limits,
        )

    with pytest.raises(RiskError):
        validate_trade(
            market_id="m2",
            outcome="ok",
            notional=200.0,
            resulting_market_notional=200.0,
            resulting_portfolio_exposure=200.0,
            limits=limits,
        )

    with pytest.raises(RiskError):
        validate_trade(
            market_id="m2",
            outcome="ok",
            notional=90.0,
            resulting_market_notional=160.0,
            resulting_portfolio_exposure=160.0,
            limits=limits,
        )

    # Valid trade does not raise
    validate_trade(
        market_id="m2",
        outcome="ok",
        notional=90.0,
        resulting_market_notional=90.0,
        resulting_portfolio_exposure=100.0,
        limits=limits,
    )
