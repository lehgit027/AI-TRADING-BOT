from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class TradeSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class Trade:
    """Represents a completed trade."""

    symbol: str
    side: TradeSide
    timestamp: datetime
    price: float
    quantity: float