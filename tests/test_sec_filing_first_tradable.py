import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from ai_trading_bot.data.history_archive import (
    PointInTimeArchive,
    canonical_json_bytes,
    load_source_policies,
)
from scripts.collect_alpaca_market_calendar import CalendarHttpCapture, CalendarSession
from scripts.collect_alpaca_market_calendar import collect as collect_calendar
from scripts.resolve_sec_filing_first_tradable import (
    FilingEvent,
    QuoteHttpCapture,
    collect,
    eligible_sessions,
    load_forward_filing_events,
    load_timing_policy,
    select_first_valid_quote,
)

NY = ZoneInfo("America/New_York")


def test_eligible_sessions_use_open_after_premarket_and_next_day_after_close() -> None:
    sessions = (
        CalendarSession(
            trading_date=date(2026, 7, 16),
            open_at=datetime(2026, 7, 16, 9, 30, tzinfo=NY),
            close_at=datetime(2026, 7, 16, 16, 0, tzinfo=NY),
            settlement_date=None,
            source_snapshot_id="calendar",
        ),
        CalendarSession(
            trading_date=date(2026, 7, 17),
            open_at=datetime(2026, 7, 17, 9, 30, tzinfo=NY),
            close_at=datetime(2026, 7, 17, 16, 0, tzinfo=NY),
            settlement_date=None,
            source_snapshot_id="calendar",
        ),
    )
    premarket = FilingEvent(
        "accession-1",
        "0000000001",
        "TEST",
        datetime(2026, 7, 16, 8, 0, tzinfo=NY),
        "event",
    )
    after_close = FilingEvent(
        "accession-2",
        "0000000001",
        "TEST",
        datetime(2026, 7, 16, 17, 0, tzinfo=NY),
        "event",
    )

    assert eligible_sessions(premarket, sessions)[0].eligible_start_at.hour == 9
    assert eligible_sessions(premarket, sessions)[0].eligible_start_at.minute == 30
    assert eligible_sessions(after_close, sessions)[0].session.trading_date == date(2026, 7, 17)


def test_quote_selection_is_two_sided_after_boundary_and_preserves_nanoseconds() -> None:
    payload = json.dumps(
        {
            "symbol": "TEST",
            "quotes": [
                {
                    "t": "2026-07-16T13:29:59.999999999Z",
                    "bp": 100,
                    "ap": 100.02,
                    "bs": 1,
                    "as": 2,
                    "bx": "Q",
                    "ax": "P",
                    "c": ["R"],
                    "z": "C",
                },
                {
                    "t": "2026-07-16T13:30:00.123456789Z",
                    "bp": 100.01,
                    "ap": 100.03,
                    "bs": 3,
                    "as": 4,
                    "bx": "Q",
                    "ax": "P",
                    "c": ["R"],
                    "z": "C",
                },
            ],
            "next_page_token": None,
        }
    ).encode()

    quote = select_first_valid_quote(
        payload,
        symbol="TEST",
        earliest_at=datetime(2026, 7, 16, 13, 30, tzinfo=UTC),
        latest_at=datetime(2026, 7, 16, 13, 31, tzinfo=UTC),
    )

    assert quote is not None
    assert quote.timestamp_text == "2026-07-16T13:30:00.123456789Z"
    assert quote.bid_price == 100.01
    assert quote.ask_price == 100.03


def test_quote_selection_rejects_crossed_or_one_sided_quotes() -> None:
    payload = json.dumps(
        {
            "symbol": "TEST",
            "quotes": [
                {
                    "t": "2026-07-16T13:30:00Z",
                    "bp": 101,
                    "ap": 100,
                    "bs": 1,
                    "as": 1,
                    "bx": "Q",
                    "ax": "P",
                    "c": [],
                },
                {
                    "t": "2026-07-16T13:30:01Z",
                    "bp": 0,
                    "ap": 100,
                    "bs": 1,
                    "as": 1,
                    "bx": "Q",
                    "ax": "P",
                    "c": [],
                },
            ],
            "next_page_token": None,
        }
    ).encode()

    assert (
        select_first_valid_quote(
            payload,
            symbol="TEST",
            earliest_at=datetime(2026, 7, 16, 13, 30, tzinfo=UTC),
            latest_at=datetime(2026, 7, 16, 13, 31, tzinfo=UTC),
        )
        is None
    )


def test_resolver_archives_private_lineage_without_applying_fill(tmp_path: Path) -> None:
    policies = load_source_policies(
        Path(__file__).resolve().parents[1] / "config" / "data_source_policies.json"
    )
    archive = PointInTimeArchive(tmp_path / "archive", policies)
    decision_at = datetime(2026, 7, 14, 13, 30, tzinfo=UTC)
    sec_raw = archive.capture(
        source_id="sec_edgar",
        dataset="submissions_cik_0000320193",
        source_url="https://data.sec.gov/submissions/CIK0000320193.json",
        payload=b'{"filing":"event"}',
        request_started_at=decision_at - timedelta(milliseconds=100),
        retrieved_at=decision_at,
        decision_available_at=decision_at,
        decision_availability_basis="first_observed",
        market_timezone="UTC",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
    )
    event_payload = {
        "schema_version": "sec_filing_events_v1",
        "rows": [
            {
                "accession_number": "0000320193-26-000999",
                "cik": "0000320193",
                "symbol": "AAPL",
                "decision_available_at": decision_at.isoformat(),
                "source_snapshot_id": sec_raw.snapshot_id,
                "first_observation_class": "monitored_new_accession",
                "forward_event_eligible": True,
            }
        ],
    }
    archive.capture(
        source_id="internal_derived",
        dataset="sec_filing_events",
        source_url="internal://events/sec-filings",
        payload=canonical_json_bytes(event_payload),
        request_started_at=decision_at + timedelta(seconds=1),
        retrieved_at=decision_at + timedelta(seconds=2),
        decision_available_at=decision_at,
        decision_availability_basis="upstream_first_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        upstream_snapshot_ids=(sec_raw.snapshot_id,),
    )
    calendar_bytes = json.dumps(
        [
            {
                "date": "2026-07-14",
                "open": "09:30",
                "close": "16:00",
                "settlement_date": "2026-07-15",
            }
        ]
    ).encode()
    collect_calendar(
        archive,
        api_key="key",
        api_secret="secret",
        start=date(2026, 7, 14),
        end=date(2026, 7, 14),
        http_capture=CalendarHttpCapture(
            payload=calendar_bytes,
            request_started_at=decision_at + timedelta(seconds=3),
            retrieved_at=decision_at + timedelta(seconds=4),
            content_type="application/json",
            response_metadata={"http_status": 200},
        ),
    )
    quote_time = "2026-07-14T13:30:00.123456789Z"
    quote_bytes = json.dumps(
        {
            "symbol": "AAPL",
            "quotes": [
                {
                    "t": quote_time,
                    "bp": 200.0,
                    "ap": 200.02,
                    "bs": 10,
                    "as": 11,
                    "bx": "Q",
                    "ax": "P",
                    "c": ["R"],
                    "z": "C",
                }
            ],
            "next_page_token": None,
        }
    ).encode()
    quote_retrieved = decision_at + timedelta(minutes=30)

    output = collect(
        archive,
        api_key="key",
        api_secret="secret",
        policy=load_timing_policy(
            Path(__file__).resolve().parents[1] / "config" / "market_timing_policy.json"
        ),
        now=quote_retrieved,
        quote_captures={
            "0000320193-26-000999": (
                QuoteHttpCapture(
                    payload=quote_bytes,
                    source_url="https://data.alpaca.markets/v2/stocks/AAPL/quotes",
                    request_started_at=quote_retrieved - timedelta(milliseconds=100),
                    retrieved_at=quote_retrieved,
                    content_type="application/json",
                    response_metadata={"http_status": 200},
                    request_parameters={"feed": "sip", "api_key": "key"},
                ),
            )
        },
    )

    assert output["status"] == "captured"
    assert output["resolved_event_count"] == 1
    resolution = output["resolutions"][0]
    assert resolution["first_tradable_at"] == quote_time
    assert resolution["execution_price_selected"] is False
    assert resolution["paper_fill_applied"] is False
    assert resolution["public_gui"] == "permission_required"
    assert output["archive_audit"]["status"] == "pass"
    assert load_forward_filing_events(archive) == ()
