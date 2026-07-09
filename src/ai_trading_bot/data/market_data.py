from __future__ import annotations

import yfinance as yf

from ai_trading_bot.data.models import Candle, MarketData


class MarketDataService:
    """Downloads historical market data."""

    def get_history(
        self,
        symbol: str,
        period: str = "1mo",
        interval: str = "1d",
    ) -> MarketData:
        """Download historical market data and return a MarketData object."""

        ticker = yf.Ticker(symbol)
        history = ticker.history(period=period, interval=interval)

        candles: list[Candle] = []

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

        return MarketData(
            symbol=symbol,
            interval=interval,
            provider="Yahoo Finance",
            candles=candles,
        )