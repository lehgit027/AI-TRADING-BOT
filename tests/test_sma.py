from datetime import datetime
import pytest

from ai_trading_bot.data.models import Candle, MarketData
from ai_trading_bot.indicators.sma import SimpleMovingAverage


def make_market_data(closes: list[float]) -> MarketData:
    candles = []

    for i, close in enumerate(closes):
        candles.append(
            Candle(
                timestamp=datetime(2025, 1, i + 1),
                open=close,
                high=close,
                low=close,
                close=close,
                volume=1000,
            )
        )

    return MarketData(
        symbol="TEST",
        interval="1d",
        provider="Unit Test",
        candles=candles,
    )


import pytest

def test_invalid_period_zero() -> None:
    with pytest.raises(ValueError):
        SimpleMovingAverage(period=0)


def test_invalid_period_negative() -> None:
    with pytest.raises(ValueError):
        SimpleMovingAverage(period=-5)


def test_period_larger_than_dataset() -> None:
    market_data = make_market_data([100, 101])

    sma = SimpleMovingAverage(period=5)

    result = sma.calculate(market_data)

    assert result.values == [None, None]

def test_sma_period_3() -> None:
    market_data = make_market_data([100, 101, 103, 102, 104])

    sma = SimpleMovingAverage(period=3)

    result = sma.calculate(market_data)

    expected = [
        None,
        None,
        101.33333333333333,
        102.0,
        103.0,
    ]

    assert result.name == "SMA(3)"
    assert result.values == expected

    