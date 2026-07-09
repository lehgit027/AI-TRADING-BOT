from abc import ABC, abstractmethod

from ai_trading_bot.data.models import MarketData
from ai_trading_bot.indicators.result import IndicatorResult


class Indicator(ABC):
    """Base class for all indicators."""

    @abstractmethod
    def calculate(self, market_data: MarketData) -> IndicatorResult:
        """Calculate indicator values."""
        raise NotImplementedError