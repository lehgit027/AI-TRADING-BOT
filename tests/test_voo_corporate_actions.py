import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from ai_trading_bot.data.history_archive import PointInTimeArchive, load_source_policies
from scripts.capture_voo_official_distribution_history import (
    OfficialDistributionHttpCapture,
    parse_official_distributions,
)
from scripts.capture_voo_official_distribution_history import (
    collect as capture_official_distribution_history,
)
from scripts.capture_voo_official_schedule import (
    OfficialScheduleHttpCapture,
    load_schedule_definition,
)
from scripts.capture_voo_official_schedule import (
    collect as capture_official_schedule,
)
from scripts.collect_voo_corporate_actions import (
    CorporateActionHttpCapture,
    OfficialDistributionDate,
    VooCorporateAction,
    VooCorporateActionLedger,
    VooOfficialSchedule,
    calculate_voo_total_return,
    collect,
    load_latest_official_schedule,
    load_total_return_policy,
    normalize_corporate_actions,
)

ROOT = Path(__file__).resolve().parents[1]


def action_payload(rate: float = 1.25) -> bytes:
    return json.dumps(
        {
            "corporate_actions": {
                "cash_dividends": [
                    {
                        "id": "action-1",
                        "symbol": "VOO",
                        "cusip": "922908363",
                        "process_date": "2026-07-20",
                        "ex_date": "2026-07-20",
                        "record_date": "2026-07-20",
                        "payable_date": "2026-07-23",
                        "rate": rate,
                        "foreign": False,
                        "special": False,
                    }
                ]
            },
            "next_page_token": None,
        }
    ).encode()


def empty_action_payload() -> bytes:
    return json.dumps({"corporate_actions": {}, "next_page_token": None}).encode()


def action_capture(payload: bytes, retrieved_at: datetime) -> CorporateActionHttpCapture:
    return CorporateActionHttpCapture(
        payload=payload,
        request_started_at=retrieved_at - timedelta(milliseconds=100),
        retrieved_at=retrieved_at,
        content_type="application/json",
        response_metadata={"http_status": 200},
        source_url="https://data.alpaca.markets/v1/corporate-actions?symbols=VOO",
        request_parameters={
            "symbols": "VOO",
            "types": "cash_dividend,stock_dividend,forward_split,reverse_split",
            "start": "2025-01-01",
            "end": "2027-12-31",
            "limit": 1000,
            "sort": "asc",
        },
    )


def test_normalize_voo_action_keeps_provider_dates_and_first_seen_separate() -> None:
    observed = datetime(2026, 7, 16, 5, 0, tzinfo=UTC)

    rows = normalize_corporate_actions(
        action_payload(),
        symbol="VOO",
        observed_at=observed,
        source_snapshot_id="snapshot",
        source_sha256="a" * 64,
        prior_observations={},
    )

    assert len(rows) == 1
    assert rows[0]["ex_date"] == "2026-07-20"
    assert rows[0]["first_seen_at"] == observed.isoformat()
    assert rows[0]["decision_available_at"] == observed.isoformat()
    assert rows[0]["availability_integrity_status"] == ("pass_first_seen_on_or_before_ex_date")
    assert rows[0]["data_quality_status"] == "pass"


def test_collect_preserves_first_seen_and_records_provider_revision(tmp_path: Path) -> None:
    archive = PointInTimeArchive(
        tmp_path / "archive",
        load_source_policies(ROOT / "config" / "data_source_policies.json"),
    )
    policy = load_total_return_policy(ROOT / "config" / "voo_total_return_policy.json")
    first_time = datetime.now(UTC) - timedelta(minutes=10)
    first = collect(
        archive,
        api_key="key",
        api_secret="secret",
        policy=policy,
        start=date(2025, 1, 1),
        end=date(2027, 12, 31),
        http_captures=(action_capture(action_payload(1.25), first_time),),
    )
    second_time = first_time + timedelta(minutes=5)
    second = collect(
        archive,
        api_key="key",
        api_secret="secret",
        policy=policy,
        start=date(2025, 1, 1),
        end=date(2027, 12, 31),
        http_captures=(action_capture(action_payload(1.30), second_time),),
    )

    assert first["corporate_action_count"] == 1
    assert second["revision_count"] == 1
    second_payload = json.loads(
        (tmp_path / "archive" / str(second["derived_snapshot"]["raw_path"])).read_text()
    )
    row = second_payload["rows"][0]
    assert row["first_seen_at"] == first_time.isoformat()
    assert row["revision_status"] == "revised_provider_state"
    assert row["rate"] == pytest.approx(1.30)
    assert archive.audit().status == "pass"


def test_calculator_passes_verified_no_action_interval() -> None:
    policy = load_total_return_policy(ROOT / "config" / "voo_total_return_policy.json")
    ledger = VooCorporateActionLedger(
        snapshot_id="ledger",
        retrieved_at=datetime(2026, 7, 18, 4, 0, tzinfo=UTC),
        query_start=date(2026, 1, 1),
        query_end=date(2026, 12, 31),
        symbol="VOO",
        actions=(),
    )
    schedule = VooOfficialSchedule(
        snapshot_id="schedule",
        retrieved_at=datetime(2026, 7, 16, 4, 0, tzinfo=UTC),
        calendar_year=2026,
        symbol="VOO",
        rows=(
            OfficialDistributionDate(
                ex_dividend_date=date(2026, 9, 28),
                record_date=date(2026, 9, 28),
                payable_date=date(2026, 9, 30),
                verified_amount_per_share=None,
            ),
        ),
    )

    result = calculate_voo_total_return(
        policy=policy,
        ledger=ledger,
        official_schedule=schedule,
        start_at=datetime(2026, 7, 16, 14, 0, tzinfo=UTC),
        end_at=datetime(2026, 7, 17, 20, 0, tzinfo=UTC),
        start_price=100.0,
        end_price=102.0,
    )

    assert result.status == "pass_verified_no_corporate_action"
    assert result.price_return == pytest.approx(0.02)
    assert result.total_return == pytest.approx(0.02)


def test_calculator_blocks_unverified_amount_then_passes_verified_amount() -> None:
    policy = load_total_return_policy(ROOT / "config" / "voo_total_return_policy.json")
    action = VooCorporateAction(
        action_id="action-1",
        action_type="cash_dividend",
        symbol="VOO",
        process_date=date(2026, 7, 20),
        ex_date=date(2026, 7, 20),
        record_date=date(2026, 7, 20),
        payable_date=date(2026, 7, 23),
        rate=1.25,
        old_rate=None,
        new_rate=None,
        first_seen_at=datetime(2026, 7, 16, 5, 0, tzinfo=UTC),
        first_observed_source_snapshot_id="raw",
        source_snapshot_id="raw",
        state_sha256="a" * 64,
        data_quality_status="pass",
    )
    ledger = VooCorporateActionLedger(
        snapshot_id="ledger",
        retrieved_at=datetime(2026, 7, 22, 4, 0, tzinfo=UTC),
        query_start=date(2026, 1, 1),
        query_end=date(2026, 12, 31),
        symbol="VOO",
        actions=(action,),
    )

    def schedule(amount: float | None) -> VooOfficialSchedule:
        return VooOfficialSchedule(
            snapshot_id="schedule",
            retrieved_at=datetime(2026, 7, 16, 4, 0, tzinfo=UTC),
            calendar_year=2026,
            symbol="VOO",
            rows=(
                OfficialDistributionDate(
                    ex_dividend_date=date(2026, 7, 20),
                    record_date=date(2026, 7, 20),
                    payable_date=date(2026, 7, 23),
                    verified_amount_per_share=amount,
                ),
            ),
        )

    blocked = calculate_voo_total_return(
        policy=policy,
        ledger=ledger,
        official_schedule=schedule(None),
        start_at=datetime(2026, 7, 16, 14, 0, tzinfo=UTC),
        end_at=datetime(2026, 7, 21, 20, 0, tzinfo=UTC),
        start_price=100.0,
        end_price=102.0,
    )
    passed = calculate_voo_total_return(
        policy=policy,
        ledger=ledger,
        official_schedule=schedule(1.25),
        start_at=datetime(2026, 7, 16, 14, 0, tzinfo=UTC),
        end_at=datetime(2026, 7, 21, 20, 0, tzinfo=UTC),
        start_price=100.0,
        end_price=102.0,
    )

    assert blocked.status == "blocked"
    assert blocked.total_return is None
    assert "not independently verified" in str(blocked.exact_block_reason)
    assert passed.status == "pass_verified_corporate_action_adjustment"
    assert passed.total_return == pytest.approx(0.0325)


def test_one_time_official_schedule_capture_is_private_and_date_only(
    tmp_path: Path,
) -> None:
    archive = PointInTimeArchive(
        tmp_path / "archive",
        load_source_policies(ROOT / "config" / "data_source_policies.json"),
    )
    schedule = load_schedule_definition(ROOT / "config" / "voo_official_schedule_2026.json")
    retrieved = datetime.now(UTC) - timedelta(seconds=1)
    capture = OfficialScheduleHttpCapture(
        payload=b"%PDF-1.7\nprivate-test-reference",
        request_started_at=retrieved - timedelta(milliseconds=100),
        retrieved_at=retrieved,
        content_type="application/pdf",
        response_metadata={"http_status": 200},
        event_publication_at=retrieved - timedelta(days=1),
    )

    output = capture_official_schedule(archive, schedule=schedule, http_capture=capture)

    assert output["status"] == "captured"
    assert output["schedule_row_count"] == 5
    assert output["verified_amount_count"] == 0
    assert str(output["raw_snapshot"]["raw_path"]).endswith(".pdf")
    assert output["redistribution"] == "prohibited_without_written_permission"
    assert archive.audit().status == "pass"


def test_one_time_distribution_history_verifies_only_exact_schedule_dates(
    tmp_path: Path,
) -> None:
    archive = PointInTimeArchive(
        tmp_path / "archive",
        load_source_policies(ROOT / "config" / "data_source_policies.json"),
    )
    schedule = load_schedule_definition(ROOT / "config" / "voo_official_schedule_2026.json")
    schedule_time = datetime.now(UTC) - timedelta(minutes=2)
    capture_official_schedule(
        archive,
        schedule=schedule,
        http_capture=OfficialScheduleHttpCapture(
            payload=b"%PDF-1.7\nprivate-test-reference",
            request_started_at=schedule_time - timedelta(milliseconds=100),
            retrieved_at=schedule_time,
            content_type="application/pdf",
            response_metadata={"http_status": 200},
            event_publication_at=schedule_time - timedelta(days=1),
        ),
    )
    history_time = schedule_time + timedelta(minutes=1)
    payload = json.dumps(
        {
            "divCapGain": {
                "item": [
                    {
                        "type": "Dividend",
                        "perShareAmount": "$1.872400",
                        "recordDate": "2026-03-27T00:00:00-04:00",
                        "reinvestmentDate": "2026-03-27T00:00:00-04:00",
                        "payableDate": "2026-03-31T00:00:00-04:00",
                    },
                    {
                        "type": "Dividend",
                        "perShareAmount": "$1.962200",
                        "recordDate": "2026-06-26T00:00:00-04:00",
                        "reinvestmentDate": "2026-06-26T00:00:00-04:00",
                        "payableDate": "2026-06-30T00:00:00-04:00",
                    },
                ]
            }
        }
    ).encode()

    output = capture_official_distribution_history(
        archive,
        http_capture=OfficialDistributionHttpCapture(
            payload=payload,
            request_started_at=history_time - timedelta(milliseconds=100),
            retrieved_at=history_time,
            content_type="application/json",
            response_metadata={"http_status": 200},
        ),
    )

    current = load_latest_official_schedule(archive)
    assert output["status"] == "captured"
    assert output["verified_amount_count"] == 2
    assert current is not None
    assert [row.verified_amount_per_share for row in current.rows[:2]] == [1.8724, 1.9622]
    assert current.rows[-1].verified_amount_per_share is None
    assert archive.audit().status == "pass"


def test_official_distribution_history_requires_explicit_dividend_type() -> None:
    payload = json.dumps(
        {
            "divCapGain": {
                "item": [
                    {
                        "type": "Capital gain",
                        "perShareAmount": "$1.00",
                        "recordDate": "2026-03-27T00:00:00-04:00",
                        "reinvestmentDate": "2026-03-27T00:00:00-04:00",
                        "payableDate": "2026-03-31T00:00:00-04:00",
                    }
                ]
            }
        }
    ).encode()

    with pytest.raises(ValueError, match="Unsupported Vanguard VOO distribution type"):
        parse_official_distributions(payload)
