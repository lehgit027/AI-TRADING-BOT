import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ai_trading_bot.data.history_archive import (
    ArchivePolicyError,
    PointInTimeArchive,
    load_source_policies,
)


def policy_file(tmp_path: Path) -> Path:
    path = tmp_path / "policies.json"
    path.write_text(
        json.dumps(
            {
                "policy_schema_version": "1.0",
                "sources": [
                    {
                        "source_id": "internal_derived",
                        "official_name": "Internal",
                        "terms_url": None,
                        "terms_checked_at": "2026-07-16",
                        "automated_collection": "allowed",
                        "raw_archiving": "allowed",
                        "internal_research": "allowed",
                        "raw_redistribution": "inherit_upstream",
                        "derived_redistribution": "inherit_upstream",
                        "public_gui": "inherit_upstream",
                        "attribution_required": False,
                        "notes": "test",
                    },
                    {
                        "source_id": "allowed_source",
                        "official_name": "Allowed",
                        "terms_url": "https://example.com/terms",
                        "terms_checked_at": "2026-07-16",
                        "automated_collection": "allowed",
                        "raw_archiving": "allowed",
                        "internal_research": "allowed",
                        "raw_redistribution": "prohibited",
                        "derived_redistribution": "permission_required",
                        "public_gui": "permission_required",
                        "attribution_required": False,
                        "notes": "test",
                    },
                    {
                        "source_id": "blocked_source",
                        "official_name": "Blocked",
                        "terms_url": "https://example.com/terms",
                        "terms_checked_at": "2026-07-16",
                        "automated_collection": "prohibited",
                        "raw_archiving": "prohibited",
                        "internal_research": "permission_required",
                        "raw_redistribution": "prohibited",
                        "derived_redistribution": "prohibited",
                        "public_gui": "prohibited",
                        "attribution_required": False,
                        "notes": "test",
                    },
                ],
            }
        )
    )
    return path


def capture(archive: PointInTimeArchive, payload: bytes = b'{"value":1}'):
    started = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    retrieved = started + timedelta(seconds=1)
    return archive.capture(
        source_id="allowed_source",
        dataset="sample",
        source_url="https://example.com/data",
        payload=payload,
        request_started_at=started,
        retrieved_at=retrieved,
        decision_available_at=retrieved,
        decision_availability_basis="first_observed",
        market_timezone="UTC",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        request_parameters={"api_key": "must-not-leak", "symbol": "TEST"},
    )


def test_archive_writes_immutable_raw_event_and_hash_chain(tmp_path: Path) -> None:
    policies = load_source_policies(policy_file(tmp_path))
    archive = PointInTimeArchive(tmp_path / "archive", policies)

    first = capture(archive)
    second = capture(archive, b'{"value":2}')

    assert first.raw_path != second.raw_path
    assert Path(tmp_path / "archive" / first.raw_path).read_bytes() == b'{"value":1}'
    assert first.request_parameters["api_key"] == "<redacted>"
    assert second.previous_record_sha256 == first.record_sha256
    assert archive.audit().status == "pass"


def test_archive_rejects_prohibited_source(tmp_path: Path) -> None:
    policies = load_source_policies(policy_file(tmp_path))
    archive = PointInTimeArchive(tmp_path / "archive", policies)
    now = datetime.now(UTC)

    with pytest.raises(ArchivePolicyError, match="prohibited"):
        archive.capture(
            source_id="blocked_source",
            dataset="sample",
            source_url="https://example.com/data",
            payload=b"data",
            request_started_at=now,
            retrieved_at=now,
            decision_available_at=now,
            decision_availability_basis="first_observed",
            market_timezone="UTC",
            timestamp_quality="first_seen_exact",
            research_use="blocked",
            content_type="text/plain",
        )


def test_archive_audit_detects_raw_tampering(tmp_path: Path) -> None:
    policies = load_source_policies(policy_file(tmp_path))
    archive = PointInTimeArchive(tmp_path / "archive", policies)
    record = capture(archive)
    raw_path = tmp_path / "archive" / record.raw_path
    raw_path.chmod(0o644)
    raw_path.write_bytes(b"tampered")

    audit = archive.audit()

    assert audit.status == "fail"
    assert any("raw object hash mismatch" in issue for issue in audit.issues)


def test_first_observed_availability_must_equal_retrieval(tmp_path: Path) -> None:
    policies = load_source_policies(policy_file(tmp_path))
    archive = PointInTimeArchive(tmp_path / "archive", policies)
    now = datetime.now(UTC)

    with pytest.raises(ValueError, match="first_observed"):
        archive.capture(
            source_id="allowed_source",
            dataset="sample",
            source_url="https://example.com/data",
            payload=b"data",
            request_started_at=now,
            retrieved_at=now,
            decision_available_at=now - timedelta(seconds=1),
            decision_availability_basis="first_observed",
            market_timezone="UTC",
            timestamp_quality="first_seen_exact",
            research_use="forward_only",
            content_type="text/plain",
        )


def test_decision_availability_cannot_be_after_retrieval(tmp_path: Path) -> None:
    policies = load_source_policies(policy_file(tmp_path))
    archive = PointInTimeArchive(tmp_path / "archive", policies)
    now = datetime.now(UTC)

    with pytest.raises(ValueError, match="after retrieved_at"):
        archive.capture(
            source_id="allowed_source",
            dataset="sample",
            source_url="https://example.com/data",
            payload=b"data",
            request_started_at=now,
            retrieved_at=now,
            decision_available_at=now + timedelta(seconds=1),
            decision_availability_basis="provider_publication",
            market_timezone="UTC",
            timestamp_quality="provider_timestamp_exact",
            research_use="point_in_time",
            content_type="text/plain",
        )


def test_archive_audit_rejects_missing_derived_upstream(tmp_path: Path) -> None:
    policies = load_source_policies(policy_file(tmp_path))
    archive = PointInTimeArchive(tmp_path / "archive", policies)
    now = datetime.now(UTC)
    archive.capture(
        source_id="internal_derived",
        dataset="sample",
        source_url="internal://sample",
        payload=b'{"derived":true}',
        request_started_at=now,
        retrieved_at=now,
        decision_available_at=now,
        decision_availability_basis="upstream_first_observed",
        market_timezone="UTC",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        upstream_snapshot_ids=("missing-snapshot",),
    )

    audit = archive.audit()

    assert audit.status == "fail"
    assert any("upstream snapshot missing-snapshot" in issue for issue in audit.issues)
