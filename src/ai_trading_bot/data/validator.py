"""Market data validation."""

from ai_trading_bot.data.models import MarketData


class DataValidationError(ValueError):
    """Raised when market data is invalid."""


class MarketDataValidator:
    """Validates market data before it is used."""

    @staticmethod
    def validate(market_data: MarketData) -> None:
        """Validate a MarketData object."""

        if not market_data.candles:
            raise DataValidationError("No market data provided.")

        previous_timestamp = None

        for candle in market_data.candles:
            if candle.open <= 0:
                raise DataValidationError("Open price must be positive.")

            if candle.high <= 0:
                raise DataValidationError("High price must be positive.")

            if candle.low <= 0:
                raise DataValidationError("Low price must be positive.")

            if candle.close <= 0:
                raise DataValidationError("Close price must be positive.")

            if candle.volume < 0:
                raise DataValidationError("Volume cannot be negative.")

            if candle.high < max(candle.open, candle.close, candle.low):
                raise DataValidationError("High price is invalid.")

            if candle.low > min(candle.open, candle.close, candle.high):
                raise DataValidationError("Low price is invalid.")

            if (
                previous_timestamp is not None
                and candle.timestamp <= previous_timestamp
            ):
                raise DataValidationError(
                    "Candles are not in chronological order."
                )

            previous_timestamp = candle.timestamp