import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from ai_trading_bot.data.history_archive import (
    PointInTimeArchive,
    canonical_json_bytes,
    load_source_policies,
)
from scripts.calculate_sec_filing_total_returns import collect as reconcile_total_returns
from scripts.capture_voo_official_schedule import (
    OfficialScheduleHttpCapture,
    load_schedule_definition,
)
from scripts.capture_voo_official_schedule import (
    collect as capture_official_schedule,
)
from scripts.collect_voo_corporate_actions import (
    CorporateActionHttpCapture,
    load_total_return_policy,
)
from scripts.collect_voo_corporate_actions import (
    collect as collect_actions,
)

ROOT = Path(__file__).resolve().parents[1]


def test_reconciler_certifies_total_return_when_two_sources_show_no_action(
    tmp_path: Path,
) -> None:
    archive = PointInTimeArchive(
        tmp_path / "archive",
        load_source_policies(ROOT / "config" / "data_source_policies.json"),
    )
    quote_retrieved = datetime(2026, 7, 15, 20, 5, tzinfo=UTC)
    quote_raw = archive.capture(
        source_id="sec_edgar",
        dataset="test_quote_lineage",
        source_url="https://www.sec.gov/test-only",
        payload=b"test-only-upstream",
        request_started_at=quote_retrieved - timedelta(seconds=1),
        retrieved_at=quote_retrieved,
        decision_available_at=quote_retrieved,
        decision_availability_basis="first_observed",
        market_timezone="UTC",
        timestamp_quality="first_seen_exact",
        research_use="blocked",
        content_type="text/plain",
        integrity_notes=("Injected test lineage only.",),
    )
    quote_payload = {
        "schema_version": "sec_filing_forward_observations_v1",
        "rows": [
            {
                "accession_number": "0000320193-26-000101",
                "horizon": "1d",
                "symbol": "AAPL",
                    "first_tradable_at": "2026-07-14T14:00:00+00:00",
                    "target_at": "2026-07-15T19:59:59+00:00",
                "data_quality_status": "pass_quote_price_only",
                "benchmark_start": {"midpoint": 100.0},
                "benchmark_end": {"midpoint": 102.0},
                "return_observation": {
                    "stock_midpoint_price_return": 0.03,
                    "stock_net_return_by_cost_bps": {
                        "5": 0.0295,
                        "25": 0.0275,
                        "50": 0.025,
                    },
                },
            }
        ],
    }
    archive.capture(
        source_id="internal_derived",
        dataset="sec_filing_forward_observations",
        source_url="internal://test/forward-observation",
        payload=canonical_json_bytes(quote_payload),
        request_started_at=quote_retrieved,
        retrieved_at=quote_retrieved + timedelta(microseconds=1),
        decision_available_at=quote_retrieved,
        decision_availability_basis="test_quote_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        upstream_snapshot_ids=(quote_raw.snapshot_id,),
        integrity_notes=("Injected forward quote observation.",),
    )
    policy = load_total_return_policy(ROOT / "config" / "voo_total_return_policy.json")
    ledger_retrieved = datetime.now(UTC) - timedelta(seconds=3)
    empty_payload = json.dumps({"corporate_actions": {}, "next_page_token": None}).encode()
    collect_actions(
        archive,
        api_key="key",
        api_secret="secret",
        policy=policy,
        start=date(2026, 1, 1),
        end=date(2026, 12, 31),
        http_captures=(
            CorporateActionHttpCapture(
                payload=empty_payload,
                request_started_at=ledger_retrieved - timedelta(milliseconds=100),
                retrieved_at=ledger_retrieved,
                content_type="application/json",
                response_metadata={"http_status": 200},
                source_url=("https://data.alpaca.markets/v1/corporate-actions?symbols=VOO"),
                request_parameters={
                    "symbols": "VOO",
                    "start": "2026-01-01",
                    "end": "2026-12-31",
                },
            ),
        ),
    )
    schedule_retrieved = datetime.now(UTC) - timedelta(seconds=1)
    capture_official_schedule(
        archive,
        schedule=load_schedule_definition(ROOT / "config" / "voo_official_schedule_2026.json"),
        http_capture=OfficialScheduleHttpCapture(
            payload=b"%PDF-1.7\ntest-schedule",
            request_started_at=schedule_retrieved - timedelta(milliseconds=100),
            retrieved_at=schedule_retrieved,
            content_type="application/pdf",
            response_metadata={"http_status": 200},
            event_publication_at=schedule_retrieved - timedelta(days=1),
        ),
    )

    output = reconcile_total_returns(
        archive,
        total_return_policy_path=ROOT / "config" / "voo_total_return_policy.json",
    )

    assert output["status"] == "captured"
    assert output["total_return_pass_count"] == 1
    assert output["total_return_blocked_count"] == 0
    row = output["reconciliations"][0]
    assert row["data_quality_status"] == "pass_verified_no_corporate_action"
    assert row["voo_total_return"] == pytest.approx(0.02)
    excess = row["total_return_excess_observation"]
    assert excess["gross_total_excess_return"] == pytest.approx(0.01)
    assert excess["alpha_calculated"] is False
    assert row["execution_price_selected"] is False
    assert row["paper_fill_applied"] is False
    assert archive.audit().status == "pass"
