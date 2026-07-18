from scripts.trusted_data_research import trusted_data_environment, trusted_data_status_payload


def test_trusted_data_status_requires_data_only_credentials() -> None:
    payload = trusted_data_status_payload({})

    assert not payload["ready"]
    assert not payload["market_data"]["configured"]
    assert not payload["macro_data"]["configured"]
    assert "MARKET_DATA" in payload["market_data"]["detail"]


def test_trusted_data_status_accepts_market_and_macro_configuration() -> None:
    payload = trusted_data_status_payload(
        {
            "AI_TRADING_MARKET_DATA_API_KEY": "key",
            "AI_TRADING_MARKET_DATA_API_SECRET": "secret",
            "AI_TRADING_MARKET_DATA_FEED": "sip",
            "AI_TRADING_FRED_API_KEY": "fred",
            "AI_TRADING_FRED_VINTAGE_DATE": "2026-07-01",
        }
    )

    assert payload["ready"]
    assert payload["market_data"]["detail"] == "Alpaca adjusted bars using sip feed."
    assert "2026-07-01" in payload["macro_data"]["detail"]


def test_trusted_data_environment_loads_only_approved_missing_values(tmp_path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "AI_TRADING_MARKET_DATA_API_KEY='dotenv-key'\n"
        "AI_TRADING_FRED_API_KEY=fred-key\n"
        "AI_TRADING_EARNINGS_API_KEY=earnings-key\n"
        "UNRELATED=value\n"
    )

    environment = trusted_data_environment(
        {"AI_TRADING_MARKET_DATA_API_KEY": "process-key"}, dotenv
    )

    assert environment["AI_TRADING_MARKET_DATA_API_KEY"] == "process-key"
    assert environment["AI_TRADING_FRED_API_KEY"] == "fred-key"
    assert environment["AI_TRADING_EARNINGS_API_KEY"] == "earnings-key"
    assert "UNRELATED" not in environment
