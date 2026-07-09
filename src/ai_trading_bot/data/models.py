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