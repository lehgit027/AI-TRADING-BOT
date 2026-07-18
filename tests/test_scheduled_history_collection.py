from datetime import UTC, datetime

import pytest

from scripts.run_scheduled_history_collection import should_run


def test_filing_schedule_runs_on_weekdays() -> None:
    due, reason = should_run("filings", datetime(2026, 7, 16, 12, 0, tzinfo=UTC))

    assert due is True
    assert reason == "due"


def test_filing_schedule_skips_weekends() -> None:
    due, reason = should_run("filings", datetime(2026, 7, 18, 12, 0, tzinfo=UTC))

    assert due is False
    assert reason == "weekend"


def test_daily_schedule_runs_every_day() -> None:
    due, reason = should_run("daily", datetime(2026, 7, 18, 12, 0, tzinfo=UTC))

    assert due is True
    assert reason == "due"


def test_schedule_rejects_unknown_mode() -> None:
    with pytest.raises(ValueError, match="Unsupported"):
        should_run("unknown", datetime(2026, 7, 16, 12, 0, tzinfo=UTC))
