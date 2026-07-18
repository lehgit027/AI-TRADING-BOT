import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ai_trading_bot.data.history_archive import PointInTimeArchive, load_source_policies
from scripts.collect_sec_filing_history import (
    HttpCapture,
    WatchedIssuer,
    collect,
    load_prior_filing_observations,
    normalize_sec_filing_events,
)

ISSUER = WatchedIssuer("AAPL", "0000320193", "Apple Inc.")


def submissions_payload(accessions: list[tuple[str, str, str]]) -> bytes:
    row_count = len(accessions)
    return json.dumps(
        {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "filings": {
                "recent": {
                    "accessionNumber": [row[0] for row in accessions],
                    "filingDate": ["2026-07-16"] * row_count,
                    "reportDate": ["2026-06-30"] * row_count,
                    "acceptanceDateTime": [row[1] for row in accessions],
                    "act": ["34"] * row_count,
                    "form": [row[2] for row in accessions],
                    "fileNumber": ["001-36743"] * row_count,
                    "filmNumber": ["261234567"] * row_count,
                    "items": ["2.02,9.01"] * row_count,
                    "size": [12345] * row_count,
                    "isXBRL": [1] * row_count,
                    "isInlineXBRL": [1] * row_count,
                    "primaryDocument": ["event.htm"] * row_count,
                    "primaryDocDescription": ["Current report"] * row_count,
                },
                "files": [{"name": "CIK0000320193-submissions-001.json"}],
            },
        }
    ).encode()


def http_capture(payload: bytes, retrieved: datetime) -> HttpCapture:
    return HttpCapture(
        payload=payload,
        decoded_payload=payload,
        request_started_at=retrieved - timedelta(milliseconds=100),
        retrieved_at=retrieved,
        content_type="application/json",
        content_encoding=None,
        response_metadata={"http_status": 200},
    )


def test_normalize_sec_filing_keeps_acceptance_and_first_seen_separate() -> None:
    observed = datetime(2026, 7, 16, 14, 5, tzinfo=UTC)
    payload = submissions_payload(
        [("0000320193-26-000100", "2026-07-16T14:00:00.000Z", "8-K")]
    )

    rows = normalize_sec_filing_events(
        payload,
        issuer=ISSUER,
        observed_at=observed,
        source_snapshot_id="snapshot",
        source_sha256="a" * 64,
        prior_observations={},
        cik_has_prior_history=False,
    )

    row = rows[0]
    assert row["acceptance_at"] == "2026-07-16T14:00:00+00:00"
    assert row["publication_at"] is None
    assert row["first_seen_at"] == observed.isoformat()
    assert row["decision_available_at"] == observed.isoformat()
    assert row["first_tradable_at"] is None
    assert row["first_observation_class"] == "baseline_existing_accession"
    assert row["forward_event_eligible"] is False
    assert row["event_category"] == "earnings_results"


def test_second_collection_marks_only_new_accession_as_forward_observed(tmp_path: Path) -> None:
    policies = load_source_policies(
        Path(__file__).resolve().parents[1] / "config" / "data_source_policies.json"
    )
    archive = PointInTimeArchive(tmp_path / "archive", policies)
    first_time = datetime.now(UTC) - timedelta(minutes=10)
    first_acceptance = (first_time - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
    first_accession = ("0000320193-26-000100", first_acceptance, "8-K")
    first = collect(
        archive,
        user_agent="test test@example.com",
        issuers=(ISSUER,),
        http_captures={
            ISSUER.cik: http_capture(submissions_payload([first_accession]), first_time)
        },
    )

    second_time = first_time + timedelta(minutes=5)
    new_acceptance = (second_time - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
    new_accession = ("0000320193-26-000101", new_acceptance, "8-K")
    second = collect(
        archive,
        user_agent="test test@example.com",
        issuers=(ISSUER,),
        http_captures={
            ISSUER.cik: http_capture(
                submissions_payload([new_accession, first_accession]),
                second_time,
            )
        },
    )

    assert first["baseline_event_count"] == 1
    assert first["new_monitored_event_count"] == 0
    assert second["baseline_event_count"] == 0
    assert second["new_monitored_event_count"] == 1
    assert second["forward_event_eligible_count"] == 1
    assert second["emitted_event_count"] == 1
    assert second["reconfirmed_filing_count"] == 1
    derived_path = tmp_path / "archive" / str(second["derived_snapshot"]["raw_path"])
    rows = json.loads(derived_path.read_text())["rows"]
    by_accession = {row["accession_number"]: row for row in rows}
    assert first_accession[0] not in by_accession
    assert by_accession[new_accession[0]]["first_seen_at"] == second_time.isoformat()
    assert by_accession[new_accession[0]]["forward_event_eligible"] is True
    prior = load_prior_filing_observations(archive)
    assert prior[f"{ISSUER.cik}:{first_accession[0]}"].first_seen_at == first_time
