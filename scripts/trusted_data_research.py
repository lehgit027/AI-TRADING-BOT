"""Shared trusted-data configuration helpers for research commands."""

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ai_trading_bot.data.trusted_sources import AlpacaMarketDataConfig, FredConfig

TRUSTED_DATA_ENVIRONMENT_NAMES = frozenset(
    {
        "AI_TRADING_MARKET_DATA_API_KEY",
        "AI_TRADING_MARKET_DATA_API_SECRET",
        "AI_TRADING_MARKET_DATA_FEED",
        "AI_TRADING_FRED_API_KEY",
        "AI_TRADING_FRED_VINTAGE_DATE",
        "AI_TRADING_EARNINGS_API_KEY",
        "AI_TRADING_EARNINGS_PROVIDER",
        "SEC_USER_AGENT_NAME",
        "SEC_USER_AGENT_EMAIL",
    }
)


def trusted_data_environment(
    environment: Mapping[str, str],
    dotenv_path: Path | None = None,
) -> dict[str, str]:
    """Load approved data-only variables from an optional .env without shell evaluation."""
    values = dict(environment)
    if dotenv_path is None or not dotenv_path.exists():
        return values
    for raw_line in dotenv_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", maxsplit=1)
        key = key.strip()
        if key not in TRUSTED_DATA_ENVIRONMENT_NAMES or values.get(key, "").strip():
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def trusted_data_status_payload(environment: Mapping[str, str]) -> dict[str, Any]:
    try:
        market_config = AlpacaMarketDataConfig.from_environment(dict(environment))
    except ValueError as exc:
        market_status = {"configured": False, "detail": str(exc)}
    else:
        market_status = {
            "configured": True,
            "detail": f"Alpaca adjusted bars using {market_config.feed} feed.",
        }

    try:
        fred_config = FredConfig.from_environment(dict(environment))
    except ValueError as exc:
        fred_status = {"configured": False, "detail": str(exc)}
    else:
        vintage = (
            "latest" if fred_config.vintage_date is None else fred_config.vintage_date.isoformat()
        )
        fred_status = {"configured": True, "detail": f"FRED macro data vintage: {vintage}."}

    return {
        "research_only": True,
        "market_data": market_status,
        "macro_data": fred_status,
        "ready": market_status["configured"] and fred_status["configured"],
        "next_action": (
            (
                "Run trusted-data research with data-only credentials; no broker or execution "
                "credentials are used."
            )
            if market_status["configured"] and fred_status["configured"]
            else "Configure the missing data-only environment variables from .env.broker.example."
        ),
    }
