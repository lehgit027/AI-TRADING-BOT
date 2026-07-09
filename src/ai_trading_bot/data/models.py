from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Candle:
    """Represents one OHLCV market candle."""

    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass(frozen=True)
class MarketData:
    """Represents a historical market dataset."""

    symbol: str
    interval: str
    provider: str
    candles: list[Candle]

    def latest(self) -> Candle:
        return self.candles[-1]

    def first(self) -> Candle:
        return self.candles[0]

    def close_prices(self) -> list[float]:
        return [c.close for c in self.candles]

    def open_prices(self) -> list[float]:
        return [c.open for c in self.candles]

    def high_prices(self) -> list[float]:
        return [c.high for c in self.candles]

    def low_prices(self) -> list[float]:
        return [c.low for c in self.candles]

    def volumes(self) -> list[int]:
        return [c.volume for c in self.candles]