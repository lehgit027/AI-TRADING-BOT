import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from ai_trading_bot.data.history_archive import PointInTimeArchive, load_source_policies
from scripts.collect_alpaca_market_calendar import (
    CalendarHttpCapture,
    calendar_covers,
    collect,
    load_latest_calendar,
    normalize_calendar_sessions,
)


def calendar_payload() -> bytes:
    return json.dumps(
        [
            {
                "date": "2026-07-02",
                "open": "09:30",
                "close": "16:00",
                "settlement_date": "2026-07-06",
            },
            {
                "date": "2026-07-03",
                "open": "09:30",
                "close": "13:00",
                "settlement_date": "2026-07-07",
            },
        ]
    ).encode()


def test_calendar_normalization_preserves_early_close_and_timezone() -> None:
    sessions = normalize_calendar_sessions(calendar_payload(), source_snapshot_id="snapshot")

    assert len(sessions) == 2
    assert sessions[0].open_at.isoformat() == "2026-07-02T09:30:00-04:00"
    assert sessions[1].close_at.isoformat() == "2026-07-03T13:00:00-04:00"
    assert sessions[1].settlement_date == date(2026, 7, 7)


def test_calendar_collection_archives_raw_and_loads_latest(tmp_path: Path) -> None:
    policies = load_source_policies(
        Path(__file__).resolve().parents[1] / "config" / "data_source_policies.json"
    )
    archive = PointInTimeArchive(tmp_path / "archive", policies)
    retrieved = datetime.now(UTC) - timedelta(seconds=1)
    capture = CalendarHttpCapture(
        payload=calendar_payload(),
        request_started_at=retrieved - timedelta(milliseconds=100),
        retrieved_at=retrieved,
        content_type="application/json",
        response_metadata={"http_status": 200},
    )

    output = collect(
        archive,
        api_key="secret",
        api_secret="secret",
        start=date(2026, 7, 2),
        end=date(2026, 7, 3),
        http_capture=capture,
    )
    latest = load_latest_calendar(archive)

    assert output["status"] == "captured"
    assert output["session_count"] == 2
    assert latest is not None
    assert latest.sessions[1].close_at.hour == 13
    assert calendar_covers(archive, start=date(2026, 7, 2), end=date(2026, 7, 3))
    assert output["raw_snapshot"]["request_parameters"]["api_key"] == "<redacted>"
