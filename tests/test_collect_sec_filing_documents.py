import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ai_trading_bot.data.history_archive import (
    PointInTimeArchive,
    canonical_json_bytes,
    load_source_policies,
)
from scripts.collect_sec_filing_documents import (
    DocumentHttpCapture,
    _resolve_primary_document_name,
    _validate_file_name,
    _validate_package,
    collect,
    load_document_policy,
    load_pending_filing_document_events,
)

ROOT = Path(__file__).resolve().parents[1]
ACCESSION = "0000320193-26-000999"
DECISION_AT = datetime(2026, 7, 16, 13, 30, tzinfo=UTC)


def archive_with_event(
    tmp_path: Path,
    *,
    first_class: str = "monitored_new_accession",
) -> PointInTimeArchive:
    archive = PointInTimeArchive(
        tmp_path / "archive",
        load_source_policies(ROOT / "config" / "data_source_policies.json"),
    )
    raw = archive.capture(
        source_id="sec_edgar",
        dataset="submissions_cik_0000320193",
        source_url="https://data.sec.gov/submissions/CIK0000320193.json",
        payload=b'{"filing":"event"}',
        request_started_at=DECISION_AT - timedelta(milliseconds=100),
        retrieved_at=DECISION_AT,
        decision_available_at=DECISION_AT,
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
                "accession_number": ACCESSION,
                "cik": "0000320193",
                "symbol": "AAPL",
                "form": "8-K",
                "items": ["2.02", "8.01"],
                "decision_available_at": DECISION_AT.isoformat(),
                "source_snapshot_id": raw.snapshot_id,
                "first_observation_class": first_class,
                "forward_event_eligible": first_class == "monitored_new_accession",
                "primary_document": "event.htm",
                "primary_document_url": (
                    "https://www.sec.gov/Archives/edgar/data/320193/000032019326000999/event.htm"
                ),
                "filing_index_url": (
                    "https://www.sec.gov/Archives/edgar/data/320193/"
                    f"000032019326000999/{ACCESSION}-index.html"
                ),
                "complete_submission_url": (
                    "https://www.sec.gov/Archives/edgar/data/320193/"
                    f"000032019326000999/{ACCESSION}.txt"
                ),
            }
        ],
    }
    archive.capture(
        source_id="internal_derived",
        dataset="sec_filing_events",
        source_url="internal://events/sec-filings",
        payload=canonical_json_bytes(event_payload),
        request_started_at=DECISION_AT + timedelta(seconds=1),
        retrieved_at=DECISION_AT + timedelta(seconds=2),
        decision_available_at=DECISION_AT,
        decision_availability_basis="upstream_first_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        upstream_snapshot_ids=(raw.snapshot_id,),
    )
    return archive


def captures() -> tuple[DocumentHttpCapture, ...]:
    index_payload = json.dumps(
        {
            "directory": {
                "item": [
                    {"name": f"{ACCESSION}.txt"},
                    {"name": "event.htm"},
                    {"name": "aapl-20260716_htm.xml"},
                ]
            }
        }
    ).encode()
    complete_payload = b"""<SEC-DOCUMENT>
<DOCUMENT>
<TYPE>8-K
<SEQUENCE>1
<FILENAME>event.htm
<DESCRIPTION>Current report
<TEXT>The company issued full-year guidance and a share repurchase program.</TEXT>
</DOCUMENT>
<DOCUMENT>
<TYPE>EX-99.1
<SEQUENCE>2
<FILENAME>earnings.htm
<DESCRIPTION>Earnings release
<TEXT>Quarterly financial results.</TEXT>
</DOCUMENT>
<DOCUMENT>
<TYPE>XML
<SEQUENCE>3
<FILENAME>aapl-20260716_htm.xml
<TEXT>&lt;xbrl&gt;facts&lt;/xbrl&gt;</TEXT>
</DOCUMENT>
</SEC-DOCUMENT>"""
    primary_payload = b"<html><body>Full-year guidance and share repurchase program.</body></html>"
    return (
        capture("index", "index.json", index_payload, 1),
        capture("complete_submission", f"{ACCESSION}.txt", complete_payload, 2),
        capture("primary_document", "event.htm", primary_payload, 3),
        capture("xbrl_asset", "aapl-20260716_htm.xml", b"<xbrl>facts</xbrl>", 4),
    )


def capture(
    role: str,
    file_name: str,
    payload: bytes,
    second: int,
) -> DocumentHttpCapture:
    retrieved = DECISION_AT + timedelta(seconds=second)
    return DocumentHttpCapture(
        role=role,
        file_name=file_name,
        source_url=(
            f"https://www.sec.gov/Archives/edgar/data/320193/000032019326000999/{file_name}"
        ),
        payload=payload,
        decoded_payload=payload,
        request_started_at=retrieved - timedelta(milliseconds=100),
        retrieved_at=retrieved,
        content_type="application/json" if role == "index" else "text/plain",
        content_encoding=None,
        response_metadata={"http_status": 200},
    )


def test_document_collection_is_forward_only_classified_and_idempotent(
    tmp_path: Path,
) -> None:
    archive = archive_with_event(tmp_path)
    policy = load_document_policy(ROOT / "config" / "sec_filing_document_policy.json")

    output = collect(
        archive,
        user_agent="test test@example.com",
        policy=policy,
        document_captures={ACCESSION: captures()},
    )

    assert output["status"] == "captured"
    assert output["captured_package_count"] == 1
    assert output["captured_document_count"] == 4
    assert output["pending_event_count"] == 0
    package = output["packages"][0]
    assert package["data_quality_status"] == "pass"
    assert package["exhibit_count"] == 1
    assert package["xbrl_asset_count"] == 1
    assert package["execution_price_selected"] is False
    assert package["paper_fill_applied"] is False
    event_types = package["classification"]["event_types"]
    assert "earnings_results" in event_types
    assert "guidance_or_outlook" in event_types
    assert "buyback_or_repurchase" in event_types
    assert package["classification"]["expected_return_inferred"] is False
    assert archive.audit().status == "pass"

    repeat = collect(
        archive,
        user_agent="test test@example.com",
        policy=policy,
        document_captures={},
    )
    assert repeat["status"] == "completed"
    assert repeat["captured_package_count"] == 0
    assert load_pending_filing_document_events(archive) == ()


def test_historical_baseline_never_requests_documents(tmp_path: Path) -> None:
    archive = archive_with_event(tmp_path, first_class="baseline_existing_accession")

    assert load_pending_filing_document_events(archive) == ()


def test_document_package_blocks_incomplete_xbrl_capture(tmp_path: Path) -> None:
    archive = archive_with_event(tmp_path)
    policy = load_document_policy(ROOT / "config" / "sec_filing_document_policy.json")

    with pytest.raises(ValueError, match="XBRL assets"):
        collect(
            archive,
            user_agent="test test@example.com",
            policy=policy,
            document_captures={ACCESSION: captures()[:-1]},
        )

    assert archive.audit().manifest_records == 2


def test_complete_submission_has_a_separate_bounded_size_limit(tmp_path: Path) -> None:
    archive = archive_with_event(tmp_path)
    policy = load_document_policy(ROOT / "config" / "sec_filing_document_policy.json")
    event = load_pending_filing_document_events(archive)[0]
    package = list(captures())
    complete_index = next(
        index
        for index, capture_item in enumerate(package)
        if capture_item.role == "complete_submission"
    )
    complete = package[complete_index]
    oversized_complete = complete.payload + b"x" * (
        policy.max_file_bytes + 1 - len(complete.payload)
    )
    package[complete_index] = replace(
        complete,
        payload=oversized_complete,
        decoded_payload=oversized_complete,
    )

    _validate_package(event, tuple(package), policy)


@pytest.mark.parametrize(
    "file_name",
    ("primary_doc.xml", "xslSCHEDULE_13G_X01/primary_doc.xml", "reports/2026/q1/data.json"),
)
def test_sec_document_filename_accepts_safe_relative_nested_paths(file_name: str) -> None:
    _validate_file_name(file_name)


@pytest.mark.parametrize(
    "file_name",
    ("../primary_doc.xml", "xsl/../primary_doc.xml", "/etc/passwd", "xsl\\doc.xml", "xsl//doc.xml"),
)
def test_sec_document_filename_rejects_unsafe_paths(file_name: str) -> None:
    with pytest.raises(ValueError, match="Unsafe SEC filing filename"):
        _validate_file_name(file_name)


def test_sec_primary_document_wrapper_path_resolves_to_unique_indexed_file() -> None:
    assert (
        _resolve_primary_document_name(
            "xslSCHEDULE_13G_X01/primary_doc.xml",
            ("0000019617-26-000246.txt", "primary_doc.xml"),
        )
        == "primary_doc.xml"
    )


def test_sec_primary_document_wrapper_path_requires_unique_indexed_file() -> None:
    with pytest.raises(ValueError, match="cannot be resolved uniquely"):
        _resolve_primary_document_name(
            "xslSCHEDULE_13G_X01/primary_doc.xml",
            ("one/primary_doc.xml", "two/primary_doc.xml"),
        )
