from ai_trading_bot.core.config import AppConfig


def test_default_config() -> None:
    config = AppConfig()

    assert config.app_name == "AI Trading Bot"
    assert config.version == "0.1.0"