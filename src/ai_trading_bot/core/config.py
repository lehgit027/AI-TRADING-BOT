"""Application configuration."""

from dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    """Application-wide configuration."""

    app_name: str = "AI Trading Bot"
    version: str = "0.1.0"