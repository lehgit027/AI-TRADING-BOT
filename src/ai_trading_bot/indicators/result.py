from dataclasses import dataclass


@dataclass(frozen=True)
class IndicatorResult:
    """Represents the output of an indicator."""

    name: str
    values: list[float | None]