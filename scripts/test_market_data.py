from ai_trading_bot.data.market_data import MarketDataService
from ai_trading_bot.data.validator import MarketDataValidator

service = MarketDataService()

market_data = service.get_history("AAPL")

MarketDataValidator.validate(market_data)

print("✅ Market data validated successfully.")
print(f"Symbol: {market_data.symbol}")
print(f"Provider: {market_data.provider}")
print(f"Candles: {len(market_data.candles)}")
print(f"Latest close: {market_data.latest().close}")