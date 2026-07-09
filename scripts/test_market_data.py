from ai_trading_bot.data.market_data import MarketDataService
from ai_trading_bot.data.validator import MarketDataValidator

service = MarketDataService()

candles = service.get_history("AAPL")

MarketDataValidator.validate(candles)

print("✅ Market data validated successfully.")
print(f"Downloaded {len(candles)} candles")
print(candles[0])