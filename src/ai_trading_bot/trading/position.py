from dataclasses import dataclass


@dataclass
class Position:
    symbol: str
    quantity: float
    average_price: float