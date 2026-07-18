from datetime import UTC, datetime, timedelta
from pathlib import Path

from ai_trading_bot.data.history_archive import (
    PointInTimeArchive,
    canonical_json_bytes,
    load_source_policies,
)
from scripts.collect_sec_insider_transactions import collect
from scripts.resolve_sec_filing_first_tradable import load_forward_filing_events

ROOT = Path(__file__).resolve().parents[1]
ACCESSION = "0000320193-26-000444"
FIRST_SEEN = datetime(2026, 7, 18, 14, 0, tzinfo=UTC)
ACCEPTANCE = FIRST_SEEN - timedelta(minutes=3)


def test_form4_ledger_keeps_only_clear_open_market_transactions(tmp_path: Path) -> None:
    archive = _archive_with_form4(tmp_path)

    # A Form 4 alone cannot reach the timing/return pipeline.
    assert load_forward_filing_events(archive) == ()

    output = collect(archive)

    assert output["status"] == "captured"
    assert output["accepted_transaction_count"] == 2
    assert output["rejected_filing_count"] == 0
    transactions = output["transactions"]
    assert [row["transaction_code"] for row in transactions] == ["P", "S"]
    assert [row["transaction_direction"] for row in transactions] == [
        "open_market_purchase",
        "open_market_sale",
    ]
    for row in transactions:
        assert row["acceptance_at"] == ACCEPTANCE.isoformat()
        assert row["first_seen_at"] == FIRST_SEEN.isoformat()
        assert row["document_hash_verified"] is True
        assert row["historical_backtest_eligible"] is False
        assert row["paper_fill_applied"] is False

    timing_events = load_forward_filing_events(archive)
    assert len(timing_events) == 1
    assert timing_events[0].accession_number == ACCESSION
    assert timing_events[0].decision_available_at == FIRST_SEEN + timedelta(seconds=1)

    # Re-running does not reinterpret or duplicate a previously observed Form 4.
    repeat = collect(archive)
    assert repeat["status"] == "completed"
    assert repeat["accepted_transaction_count"] == 0


def test_form4_without_clear_transaction_is_archived_but_not_a_forward_event(
    tmp_path: Path,
) -> None:
    archive = _archive_with_form4(tmp_path, clear_transactions=False)

    output = collect(archive)

    assert output["status"] == "completed"
    assert output["accepted_transaction_count"] == 0
    assert output["rejected_filing_count"] == 1
    assert output["rejected_filings"][0]["reason"] == (
        "no_clear_non_derivative_open_market_transaction"
    )
    assert load_forward_filing_events(archive) == ()


def test_baseline_form4_is_background_only(tmp_path: Path) -> None:
    archive = _archive_with_form4(tmp_path, first_observation_class="baseline_existing_accession")

    output = collect(archive)

    assert output["status"] == "completed"
    assert output["accepted_transaction_count"] == 0
    assert load_forward_filing_events(archive) == ()


def _archive_with_form4(
    tmp_path: Path,
    *,
    clear_transactions: bool = True,
    first_observation_class: str = "monitored_new_accession",
) -> PointInTimeArchive:
    policies = load_source_policies(ROOT / "config" / "data_source_policies.json")
    archive = PointInTimeArchive(tmp_path / "archive", policies)
    submissions = archive.capture(
        source_id="sec_edgar",
        dataset="submissions_cik_0000320193",
        source_url="https://data.sec.gov/submissions/CIK0000320193.json",
        payload=b'{"filing":"form4"}',
        request_started_at=FIRST_SEEN - timedelta(milliseconds=100),
        retrieved_at=FIRST_SEEN,
        decision_available_at=FIRST_SEEN,
        decision_availability_basis="first_observed",
        market_timezone="UTC",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
    )
    primary_payload = _form4_xml(clear_transactions)
    primary = archive.capture(
        source_id="sec_edgar",
        dataset="sec_filing_primary_document",
        source_url=(
            "https://www.sec.gov/Archives/edgar/data/320193/"
            "000032019326000444/xslF345X03/form4.xml"
        ),
        payload=primary_payload,
        request_started_at=FIRST_SEEN + timedelta(milliseconds=900),
        retrieved_at=FIRST_SEEN + timedelta(seconds=1),
        decision_available_at=FIRST_SEEN + timedelta(seconds=1),
        decision_availability_basis="first_observed",
        market_timezone="UTC",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/xml",
        request_parameters={
            "accession_number": ACCESSION,
            "cik": "0000320193",
            "symbol": "AAPL",
            "role": "primary_document",
            "file_name": "form4.xml",
        },
    )
    event_payload = {
        "schema_version": "sec_filing_events_v1",
        "rows": [
            {
                "accession_number": ACCESSION,
                "cik": "0000320193",
                "symbol": "AAPL",
                "form": "4",
                "acceptance_at": ACCEPTANCE.isoformat(),
                "first_seen_at": FIRST_SEEN.isoformat(),
                "decision_available_at": FIRST_SEEN.isoformat(),
                "source_snapshot_id": submissions.snapshot_id,
                "first_observation_class": first_observation_class,
                "forward_event_eligible": (
                    first_observation_class == "monitored_new_accession"
                ),
            }
        ],
    }
    event = archive.capture(
        source_id="internal_derived",
        dataset="sec_filing_events",
        source_url="internal://test/form4-event",
        payload=canonical_json_bytes(event_payload),
        request_started_at=FIRST_SEEN + timedelta(seconds=2),
        retrieved_at=FIRST_SEEN + timedelta(seconds=3),
        decision_available_at=FIRST_SEEN,
        decision_availability_basis="upstream_first_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        upstream_snapshot_ids=(submissions.snapshot_id,),
    )
    package_payload = {
        "schema_version": "sec_filing_document_packages_v1",
        "rows": [
            {
                "accession_number": ACCESSION,
                "cik": "0000320193",
                "symbol": "AAPL",
                "form": "4",
                "event_source_snapshot_id": event.snapshot_id,
                "data_quality_status": "pass",
                "document_records": [
                    {
                        "role": "primary_document",
                        "snapshot_id": primary.snapshot_id,
                        "raw_sha256": primary.raw_sha256,
                        "retrieved_at": primary.retrieved_at,
                    }
                ],
                "classification": {"event_types": ["insider_activity"]},
            }
        ],
    }
    archive.capture(
        source_id="internal_derived",
        dataset="sec_filing_document_packages",
        source_url="internal://test/form4-package",
        payload=canonical_json_bytes(package_payload),
        request_started_at=FIRST_SEEN + timedelta(seconds=4),
        retrieved_at=FIRST_SEEN + timedelta(seconds=5),
        decision_available_at=FIRST_SEEN + timedelta(seconds=1),
        decision_availability_basis="primary_document_first_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        upstream_snapshot_ids=(event.snapshot_id, primary.snapshot_id),
    )
    return archive


def _form4_xml(clear_transactions: bool) -> bytes:
    rows = (
        """
        <nonDerivativeTransaction><securityTitle><value>Common Stock</value></securityTitle>
        <transactionDate><value>2026-07-18</value></transactionDate><transactionCoding><transactionCode>P</transactionCode></transactionCoding>
        <transactionAmounts><transactionShares><value>100</value></transactionShares><transactionPricePerShare><value>200.50</value></transactionPricePerShare><transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode></transactionAmounts></nonDerivativeTransaction>
        <nonDerivativeTransaction><securityTitle><value>Common Stock</value></securityTitle>
        <transactionDate><value>2026-07-18</value></transactionDate><transactionCoding><transactionCode>S</transactionCode></transactionCoding>
        <transactionAmounts><transactionShares><value>50</value></transactionShares><transactionPricePerShare><value>201.00</value></transactionPricePerShare><transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode></transactionAmounts></nonDerivativeTransaction>
        <nonDerivativeTransaction><securityTitle><value>Common Stock</value></securityTitle>
        <transactionDate><value>2026-07-18</value></transactionDate><transactionCoding><transactionCode>A</transactionCode></transactionCoding>
        <transactionAmounts><transactionShares><value>10</value></transactionShares><transactionPricePerShare><value>1</value></transactionPricePerShare><transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode></transactionAmounts></nonDerivativeTransaction>
        """
        if clear_transactions
        else """
        <nonDerivativeTransaction><securityTitle><value>Common Stock</value></securityTitle>
        <transactionDate><value>2026-07-18</value></transactionDate><transactionCoding><transactionCode>A</transactionCode></transactionCoding>
        <transactionAmounts><transactionShares><value>10</value></transactionShares><transactionPricePerShare><value>1</value></transactionPricePerShare><transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode></transactionAmounts></nonDerivativeTransaction>
        """
    )
    return f"""<?xml version=\"1.0\"?>
    <ownershipDocument><issuer><issuerCik>320193</issuerCik></issuer>
    <reportingOwner><reportingOwnerId><rptOwnerCik>12345</rptOwnerCik>
    <rptOwnerName>Test Owner</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>1</isDirector><isOfficer>0</isOfficer>
    <isTenPercentOwner>0</isTenPercentOwner></reportingOwnerRelationship></reportingOwner>
    <nonDerivativeTable>{rows}</nonDerivativeTable>
    <derivativeTable><derivativeTransaction /></derivativeTable></ownershipDocument>""".encode()
