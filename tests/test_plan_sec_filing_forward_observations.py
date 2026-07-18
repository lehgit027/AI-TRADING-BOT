import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from ai_trading_bot.data.history_archive import (
    PointInTimeArchive,
    canonical_json_bytes,
    load_source_policies,
)
from scripts.collect_alpaca_market_calendar import CalendarHttpCapture
from scripts.collect_alpaca_market_calendar import collect as collect_calendar
from scripts.plan_sec_filing_forward_observations import (
    collect,
    load_observation_policy,
    load_planning_state,
)

ROOT = Path(__file__).resolve().parents[1]
NY = ZoneInfo("America/New_York")
ACCESSION = "0000320193-26-000999"


def build_archive(
    tmp_path: Path,
    *,
    include_resolution: bool,
    form: str = "8-K",
) -> PointInTimeArchive:
    archive = PointInTimeArchive(
        tmp_path / "archive",
        load_source_policies(ROOT / "config" / "data_source_policies.json"),
    )
    observed = datetime(2026, 7, 2, 13, 35, tzinfo=UTC)
    raw = archive.capture(
        source_id="sec_edgar",
        dataset="sec_filing_primary_document",
        source_url="https://www.sec.gov/Archives/edgar/data/320193/event.htm",
        payload=b"<html>filing</html>",
        request_started_at=observed - timedelta(milliseconds=100),
        retrieved_at=observed,
        decision_available_at=observed,
        decision_availability_basis="first_observed",
        market_timezone="UTC",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="text/html",
    )
    package_payload = {
        "schema_version": "sec_filing_document_packages_v1",
        "rows": [
            {
                "accession_number": ACCESSION,
                "cik": "0000320193",
                "symbol": "AAPL",
                "form": form,
                "data_quality_status": "pass",
                "classification": {"event_types": ["earnings_results", "guidance_or_outlook"]},
            }
        ],
    }
    package = archive.capture(
        source_id="internal_derived",
        dataset="sec_filing_document_packages",
        source_url="internal://events/sec-filing-document-packages",
        payload=canonical_json_bytes(package_payload),
        request_started_at=observed + timedelta(seconds=1),
        retrieved_at=observed + timedelta(seconds=2),
        decision_available_at=observed,
        decision_availability_basis="documents_first_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        upstream_snapshot_ids=(raw.snapshot_id,),
    )

    session_dates = business_dates(date(2026, 7, 2), 70)
    calendar_payload = json.dumps(
        [
            {
                "date": session_date.isoformat(),
                "open": "09:30",
                "close": "16:00",
                "settlement_date": None,
            }
            for session_date in session_dates
        ]
    ).encode()
    calendar_output = collect_calendar(
        archive,
        api_key="key",
        api_secret="secret",
        start=session_dates[0],
        end=session_dates[-1],
        http_capture=CalendarHttpCapture(
            payload=calendar_payload,
            request_started_at=observed - timedelta(minutes=2),
            retrieved_at=observed - timedelta(minutes=1),
            content_type="application/json",
            response_metadata={"http_status": 200},
        ),
    )
    if include_resolution:
        first_tradable = datetime(2026, 7, 2, 10, 0, tzinfo=NY)
        resolution_payload = {
            "schema_version": "sec_filing_first_tradable_resolutions_v1",
            "rows": [
                {
                    "accession_number": ACCESSION,
                    "first_tradable_at": "2026-07-02T14:00:00.123456789Z",
                    "bid_price": 200.0,
                    "ask_price": 200.02,
                    "calendar_snapshot_id": calendar_output["derived_snapshot"]["snapshot_id"],
                }
            ],
        }
        archive.capture(
            source_id="internal_derived",
            dataset="sec_filing_first_tradable_resolutions",
            source_url="internal://market-timing/sec-filing-first-tradable",
            payload=canonical_json_bytes(resolution_payload),
            request_started_at=first_tradable.astimezone(UTC) + timedelta(minutes=20),
            retrieved_at=first_tradable.astimezone(UTC) + timedelta(minutes=21),
            decision_available_at=first_tradable.astimezone(UTC) + timedelta(minutes=20),
            decision_availability_basis="quote_response_first_observed",
            market_timezone="America/New_York",
            timestamp_quality="first_seen_exact",
            research_use="forward_only",
            content_type="application/json",
            upstream_snapshot_ids=(
                raw.snapshot_id,
                str(calendar_output["derived_snapshot"]["snapshot_id"]),
                package.snapshot_id,
            ),
        )
    return archive


def business_dates(start: date, count: int) -> list[date]:
    dates: list[date] = []
    cursor = start
    while len(dates) < count:
        if cursor.weekday() < 5:
            dates.append(cursor)
        cursor += timedelta(days=1)
    return dates


def test_plans_horizons_only_after_package_and_first_tradable(tmp_path: Path) -> None:
    archive = build_archive(tmp_path, include_resolution=True)
    policy = load_observation_policy(ROOT / "config" / "sec_filing_forward_observation_policy.json")

    output = collect(archive, policy=policy)

    assert output["status"] == "captured"
    assert output["planned_event_count"] == 1
    assert output["awaiting_first_tradable_count"] == 0
    derived = archive.root / str(output["derived_snapshot"]["raw_path"])
    row = json.loads(derived.read_text())["rows"][0]
    horizons = {item["horizon"]: item for item in row["horizons"]}
    assert horizons["1h"]["target_at"] == "2026-07-02T11:00:00.123457-04:00"
    assert horizons["1h"]["target_timestamp_rounding"] == ("ceil_to_microsecond_never_earlier")
    assert horizons["1d"]["target_at"] == "2026-07-03T15:59:59-04:00"
    assert horizons["1d"]["observation_window_end_at"] == ("2026-07-03T16:00:00-04:00")
    assert horizons["1d"]["earliest_observation_retrieval_at"] == ("2026-07-03T16:16:00-04:00")
    assert horizons["60d"]["status"] == "awaiting_future_market_observation"
    assert row["benchmark_symbol"] == "VOO"
    assert row["costs_bps"] == [5, 25, 50]
    assert row["execution_price_selected"] is False
    assert row["paper_fill_applied"] is False
    assert row["candidate_created"] is False
    assert archive.audit().status == "pass"

    repeat = collect(archive, policy=policy)
    assert repeat["status"] == "completed"
    assert repeat["planned_event_count"] == 0


def test_package_waits_without_first_tradable_evidence(tmp_path: Path) -> None:
    archive = build_archive(tmp_path, include_resolution=False)
    policy = load_observation_policy(ROOT / "config" / "sec_filing_forward_observation_policy.json")

    state = load_planning_state(archive)
    output = collect(archive, policy=policy)

    assert state.ready == ()
    assert state.awaiting_first_tradable_count == 1
    assert output["status"] == "completed"
    assert output["planned_event_count"] == 0
    assert output["awaiting_first_tradable_count"] == 1


def test_clear_form4_ledger_uses_the_same_six_forward_horizons(tmp_path: Path) -> None:
    archive = build_archive(tmp_path, include_resolution=True, form="4")
    upstream_snapshot_id = json.loads(
        archive.manifest_path.read_text().splitlines()[0]
    )["snapshot_id"]
    ledger_payload = {
        "schema_version": "sec_insider_transaction_events_v1",
        "rows": [
            {
                "accession_number": ACCESSION,
                "cik": "0000320193",
                "symbol": "AAPL",
                "forward_event_eligible": True,
                "transaction_code": "P",
                "transaction_direction": "open_market_purchase",
                "primary_document_sha256": "a" * 64,
            }
        ],
    }
    archive.capture(
        source_id="internal_derived",
        dataset="sec_insider_transaction_events",
        source_url="internal://test/form4-ledger",
        payload=canonical_json_bytes(ledger_payload),
        request_started_at=datetime(2026, 7, 2, 15, 0, tzinfo=UTC),
        retrieved_at=datetime(2026, 7, 2, 15, 1, tzinfo=UTC),
        decision_available_at=datetime(2026, 7, 2, 15, 0, tzinfo=UTC),
        decision_availability_basis="verified_primary_form4_xml_first_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        upstream_snapshot_ids=(upstream_snapshot_id,),
    )

    output = collect(
        archive,
        policy=load_observation_policy(
            ROOT / "config" / "sec_filing_forward_observation_policy.json"
        ),
    )

    assert output["planned_event_count"] == 1
    row = json.loads(
        (archive.root / str(output["derived_snapshot"]["raw_path"])).read_text()
    )["rows"][0]
    assert row["event_types"] == ["insider_open_market_transaction"]
    assert {item["horizon"] for item in row["horizons"]} == {
        "1h",
        "1d",
        "3d",
        "5d",
        "20d",
        "60d",
    }
