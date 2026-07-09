from __future__ import annotations

from typing import List

import yfinance as yf

from ai_trading_bot.data.models import Candle


class MarketDataService:
    """Downloads historical market data."""

    def get_history(
        self,
        symbol: str,
        period: str = "1mo",
        interval: str = "1d",
    ) -> List[Candle]:
        ticker = yf.Ticker(symbol)
        history = ticker.history(period=period, interval=interval)

        candles: List[Candle] = []

        for timestamp, row in history.iterrows():
            candles.append(
                Candle(
                    timestamp=timestamp.to_pydatetime(),
                    open=float(row["Open"]),
                    high=float(row["High"]),
                    low=float(row["Low"]),
                    close=float(row["Close"]),
                    volume=int(row["Volume"]),
                )
            )

        return candles