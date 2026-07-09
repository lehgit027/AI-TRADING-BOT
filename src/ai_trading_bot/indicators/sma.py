from dataclasses import dataclass

from ai_trading_bot.data.models import MarketData
from ai_trading_bot.indicators.base import Indicator
from ai_trading_bot.indicators.result import IndicatorResult


@dataclass(frozen=True)
class SimpleMovingAverage(Indicator):
    """Simple Moving Average (SMA)."""

    period: int

    def __post_init__(self) -> None:
        if self.period <= 0:
            raise ValueError("Period must be greater than zero.")

    def calculate(self, market_data: MarketData) -> IndicatorResult:
        closes = market_data.close_prices()

        values: list[float | None] = []

        for i in range(len(closes)):
            if i < self.period - 1:
                values.append(None)
                continue

            window = closes[i - self.period + 1 : i + 1]
            values.append(sum(window) / self.period)

        return IndicatorResult(
            name=f"SMA({self.period})",
            values=values,
        )