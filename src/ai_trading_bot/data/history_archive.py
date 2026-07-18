"""Immutable, policy-gated point-in-time research data archive."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

ARCHIVE_SCHEMA_VERSION = "1.0"
_SAFE_COMPONENT = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")
_SENSITIVE_KEY = re.compile(
    r"(authorization|api[_-]?key|api[_-]?secret|password|secret|token|cookie)",
    re.IGNORECASE,
)
_COLLECTION_ALLOWED = {"allowed", "allowed_subject_to_endpoint_rules"}
_ARCHIVING_ALLOWED = {
    "allowed",
    "allowed_for_government_information",
    "allowed_for_internal_use",
}
_TIMESTAMP_QUALITY = {
    "first_seen_exact",
    "provider_timestamp_exact",
    "retrieval_recorded",
    "unknown",
}
_RESEARCH_USE = {"point_in_time", "forward_only", "blocked"}


class ArchivePolicyError(ValueError):
    """Raised when a source policy does not permit a requested capture."""


class ArchiveIntegrityError(ValueError):
    """Raised when immutable archive content fails an integrity check."""


@dataclass(frozen=True)
class SourcePolicy:
    source_id: str
    official_name: str
    terms_url: str | None
    terms_checked_at: str
    automated_collection: str
    raw_archiving: str
    internal_research: str
    raw_redistribution: str
    derived_redistribution: str
    public_gui: str
    attribution_required: bool
    notes: str

    @property
    def collection_is_allowed(self) -> bool:
        return self.automated_collection in _COLLECTION_ALLOWED

    @property
    def archiving_is_allowed(self) -> bool:
        return self.raw_archiving in _ARCHIVING_ALLOWED


@dataclass(frozen=True)
class SnapshotRecord:
    snapshot_id: str
    source_id: str
    dataset: str
    source_url: str
    request_started_at: str
    retrieved_at: str
    event_publication_at: str | None
    decision_available_at: str
    decision_availability_basis: str
    market_timezone: str
    timestamp_quality: str
    research_use: str
    content_type: str
    content_encoding: str | None
    raw_bytes: int
    raw_sha256: str
    raw_path: str
    event_path: str
    request_parameters: dict[str, object]
    response_metadata: dict[str, object]
    upstream_snapshot_ids: tuple[str, ...]
    integrity_notes: tuple[str, ...]
    source_policy_checked_at: str
    raw_redistribution: str
    derived_redistribution: str
    public_gui: str
    active_profile_changed: bool
    previous_record_sha256: str | None
    record_sha256: str

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["upstream_snapshot_ids"] = list(self.upstream_snapshot_ids)
        payload["integrity_notes"] = list(self.integrity_notes)
        return payload


@dataclass(frozen=True)
class ArchiveAudit:
    status: str
    manifest_records: int
    raw_objects_checked: int
    event_records_checked: int
    issues: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "manifest_records": self.manifest_records,
            "raw_objects_checked": self.raw_objects_checked,
            "event_records_checked": self.event_records_checked,
            "issues": list(self.issues),
        }


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def load_source_policies(path: Path) -> dict[str, SourcePolicy]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict) or payload.get("policy_schema_version") != "1.0":
        raise ValueError("Source-policy registry has an unsupported schema.")
    raw_sources = payload.get("sources")
    if not isinstance(raw_sources, list):
        raise ValueError("Source-policy registry must contain a sources list.")
    policies: dict[str, SourcePolicy] = {}
    for item in raw_sources:
        if not isinstance(item, dict):
            raise ValueError("Source-policy entries must be objects.")
        policy = SourcePolicy(
            source_id=str(item["source_id"]),
            official_name=str(item["official_name"]),
            terms_url=None if item.get("terms_url") is None else str(item["terms_url"]),
            terms_checked_at=str(item["terms_checked_at"]),
            automated_collection=str(item["automated_collection"]),
            raw_archiving=str(item["raw_archiving"]),
            internal_research=str(item["internal_research"]),
            raw_redistribution=str(item["raw_redistribution"]),
            derived_redistribution=str(item["derived_redistribution"]),
            public_gui=str(item["public_gui"]),
            attribution_required=bool(item["attribution_required"]),
            notes=str(item["notes"]),
        )
        _validate_component(policy.source_id, "source_id")
        if policy.source_id in policies:
            raise ValueError(f"Duplicate source policy: {policy.source_id}")
        policies[policy.source_id] = policy
    return policies


def redact_sensitive_mapping(values: dict[str, object]) -> dict[str, object]:
    redacted: dict[str, object] = {}
    for key, value in values.items():
        redacted[str(key)] = "<redacted>" if _SENSITIVE_KEY.search(str(key)) else value
    return redacted


class PointInTimeArchive:
    """Write raw responses once and append hash-chained retrieval records."""

    def __init__(self, root: Path, policies: dict[str, SourcePolicy]) -> None:
        self.root = root
        self.policies = policies
        self.manifest_path = root / "manifest" / "snapshots.jsonl"

    def capture(
        self,
        *,
        source_id: str,
        dataset: str,
        source_url: str,
        payload: bytes,
        request_started_at: datetime,
        retrieved_at: datetime,
        decision_available_at: datetime,
        decision_availability_basis: str,
        market_timezone: str,
        timestamp_quality: str,
        research_use: str,
        content_type: str,
        content_encoding: str | None = None,
        event_publication_at: datetime | None = None,
        request_parameters: dict[str, object] | None = None,
        response_metadata: dict[str, object] | None = None,
        upstream_snapshot_ids: tuple[str, ...] = (),
        integrity_notes: tuple[str, ...] = (),
    ) -> SnapshotRecord:
        _validate_component(source_id, "source_id")
        _validate_component(dataset, "dataset")
        policy = self._policy_for_capture(source_id)
        request_started = _aware_datetime(request_started_at, "request_started_at")
        retrieved = _aware_datetime(retrieved_at, "retrieved_at")
        decision_available = _aware_datetime(decision_available_at, "decision_available_at")
        publication = (
            None
            if event_publication_at is None
            else _aware_datetime(event_publication_at, "event_publication_at")
        )
        if request_started > retrieved:
            raise ValueError("request_started_at cannot be after retrieved_at.")
        if decision_available > retrieved:
            raise ValueError("decision_available_at cannot be after retrieved_at.")
        if publication is not None and decision_available < publication:
            raise ValueError("decision_available_at cannot predate event_publication_at.")
        if decision_availability_basis == "first_observed" and decision_available != retrieved:
            raise ValueError(
                "first_observed decision availability must equal the retrieval timestamp."
            )
        if timestamp_quality not in _TIMESTAMP_QUALITY:
            raise ValueError(f"Unsupported timestamp_quality: {timestamp_quality}")
        if research_use not in _RESEARCH_USE:
            raise ValueError(f"Unsupported research_use: {research_use}")
        if not market_timezone.strip():
            raise ValueError("market_timezone is required.")
        if not payload:
            raise ValueError("Cannot archive an empty response.")
        if source_id == "internal_derived" and not upstream_snapshot_ids:
            raise ValueError("Internal derived snapshots must identify upstream snapshots.")

        digest = hashlib.sha256(payload).hexdigest()
        extension = _extension_for(content_type, content_encoding)
        date_path = retrieved.strftime("%Y/%m/%d")
        raw_path = self.root / "raw" / source_id / dataset / date_path / f"{digest}{extension}"
        _write_once(raw_path, payload)

        retrieved_utc = retrieved.astimezone(UTC)
        snapshot_id = f"{retrieved_utc:%Y%m%dT%H%M%S%fZ}-{digest[:12]}-{uuid4().hex[:8]}"
        event_path = self.root / "events" / source_id / dataset / date_path / f"{snapshot_id}.json"
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with self.manifest_path.open("a+", encoding="utf-8") as manifest:
            fcntl.flock(manifest.fileno(), fcntl.LOCK_EX)
            manifest.seek(0)
            lines = [line for line in manifest.read().splitlines() if line.strip()]
            previous_hash = None
            if lines:
                previous = json.loads(lines[-1])
                previous_hash = str(previous["record_sha256"])
            record_without_hash: dict[str, object] = {
                "archive_schema_version": ARCHIVE_SCHEMA_VERSION,
                "snapshot_id": snapshot_id,
                "source_id": source_id,
                "dataset": dataset,
                "source_url": source_url,
                "request_started_at": request_started.isoformat(),
                "retrieved_at": retrieved.isoformat(),
                "event_publication_at": (None if publication is None else publication.isoformat()),
                "decision_available_at": decision_available.isoformat(),
                "decision_availability_basis": decision_availability_basis,
                "market_timezone": market_timezone,
                "timestamp_quality": timestamp_quality,
                "research_use": research_use,
                "content_type": content_type,
                "content_encoding": content_encoding,
                "raw_bytes": len(payload),
                "raw_sha256": digest,
                "raw_path": str(raw_path.relative_to(self.root)),
                "event_path": str(event_path.relative_to(self.root)),
                "request_parameters": redact_sensitive_mapping(request_parameters or {}),
                "response_metadata": redact_sensitive_mapping(response_metadata or {}),
                "upstream_snapshot_ids": list(upstream_snapshot_ids),
                "integrity_notes": list(integrity_notes),
                "source_policy_checked_at": policy.terms_checked_at,
                "raw_redistribution": policy.raw_redistribution,
                "derived_redistribution": policy.derived_redistribution,
                "public_gui": policy.public_gui,
                "active_profile_changed": False,
                "previous_record_sha256": previous_hash,
            }
            record_hash = hashlib.sha256(canonical_json_bytes(record_without_hash)).hexdigest()
            record_payload = {**record_without_hash, "record_sha256": record_hash}
            _write_once(
                event_path,
                json.dumps(record_payload, indent=2, sort_keys=True).encode("utf-8") + b"\n",
            )
            manifest.seek(0, os.SEEK_END)
            manifest.write(json.dumps(record_payload, sort_keys=True, separators=(",", ":")) + "\n")
            manifest.flush()
            os.fsync(manifest.fileno())
            fcntl.flock(manifest.fileno(), fcntl.LOCK_UN)
        return _snapshot_record(record_payload)

    def audit(self) -> ArchiveAudit:
        if not self.manifest_path.exists():
            return ArchiveAudit("blocked_empty_archive", 0, 0, 0, ("Manifest is missing.",))
        issues: list[str] = []
        raw_checked = 0
        events_checked = 0
        previous_hash: str | None = None
        seen_snapshot_ids: set[str] = set()
        lines = self.manifest_path.read_text().splitlines()
        records = [line for line in lines if line.strip()]
        for index, line in enumerate(records, start=1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                issues.append(f"Manifest line {index} is invalid JSON: {exc}")
                continue
            if not isinstance(record, dict):
                issues.append(f"Manifest line {index} is not an object.")
                continue
            actual_hash = record.get("record_sha256")
            unhashed = dict(record)
            unhashed.pop("record_sha256", None)
            expected_hash = hashlib.sha256(canonical_json_bytes(unhashed)).hexdigest()
            if actual_hash != expected_hash:
                issues.append(f"Manifest line {index} record hash mismatch.")
            if record.get("previous_record_sha256") != previous_hash:
                issues.append(f"Manifest line {index} chain link mismatch.")
            previous_hash = None if actual_hash is None else str(actual_hash)

            snapshot_id = str(record.get("snapshot_id", "")).strip()
            if not snapshot_id:
                issues.append(f"Manifest line {index} snapshot ID is missing.")
            elif snapshot_id in seen_snapshot_ids:
                issues.append(f"Manifest line {index} snapshot ID is duplicated.")
            upstream_ids = record.get("upstream_snapshot_ids")
            if not isinstance(upstream_ids, list):
                issues.append(f"Manifest line {index} upstream snapshot IDs are invalid.")
                upstream_ids = []
            if record.get("source_id") == "internal_derived" and not upstream_ids:
                issues.append(f"Manifest line {index} derived snapshot has no upstream evidence.")
            for upstream_id in upstream_ids:
                if str(upstream_id) not in seen_snapshot_ids:
                    issues.append(
                        f"Manifest line {index} upstream snapshot {upstream_id} "
                        "is missing or appears later."
                    )
            if snapshot_id:
                seen_snapshot_ids.add(snapshot_id)
            if record.get("active_profile_changed") is not False:
                issues.append(f"Manifest line {index} changed the active profile.")

            raw_path = self.root / str(record.get("raw_path", ""))
            if not raw_path.is_file():
                issues.append(f"Manifest line {index} raw object is missing.")
            else:
                raw = raw_path.read_bytes()
                raw_checked += 1
                if hashlib.sha256(raw).hexdigest() != record.get("raw_sha256"):
                    issues.append(f"Manifest line {index} raw object hash mismatch.")
                if len(raw) != record.get("raw_bytes"):
                    issues.append(f"Manifest line {index} raw object size mismatch.")

            event_path = self.root / str(record.get("event_path", ""))
            if not event_path.is_file():
                issues.append(f"Manifest line {index} immutable event record is missing.")
            else:
                events_checked += 1
                try:
                    event = json.loads(event_path.read_text())
                except json.JSONDecodeError:
                    issues.append(f"Manifest line {index} event record is invalid JSON.")
                else:
                    if event != record:
                        issues.append(f"Manifest line {index} event record mismatch.")

            source_id = str(record.get("source_id", ""))
            if source_id not in self.policies:
                issues.append(f"Manifest line {index} source policy is missing.")
        status = "pass" if not issues else "fail"
        return ArchiveAudit(status, len(records), raw_checked, events_checked, tuple(issues))

    def _policy_for_capture(self, source_id: str) -> SourcePolicy:
        try:
            policy = self.policies[source_id]
        except KeyError as exc:
            raise ArchivePolicyError(f"No source policy is registered for {source_id}.") from exc
        if not policy.collection_is_allowed:
            raise ArchivePolicyError(
                f"Automated collection for {source_id} is {policy.automated_collection}."
            )
        if not policy.archiving_is_allowed:
            raise ArchivePolicyError(f"Raw archiving for {source_id} is {policy.raw_archiving}.")
        return policy


def _snapshot_record(payload: dict[str, object]) -> SnapshotRecord:
    return SnapshotRecord(
        snapshot_id=str(payload["snapshot_id"]),
        source_id=str(payload["source_id"]),
        dataset=str(payload["dataset"]),
        source_url=str(payload["source_url"]),
        request_started_at=str(payload["request_started_at"]),
        retrieved_at=str(payload["retrieved_at"]),
        event_publication_at=(
            None
            if payload.get("event_publication_at") is None
            else str(payload["event_publication_at"])
        ),
        decision_available_at=str(payload["decision_available_at"]),
        decision_availability_basis=str(payload["decision_availability_basis"]),
        market_timezone=str(payload["market_timezone"]),
        timestamp_quality=str(payload["timestamp_quality"]),
        research_use=str(payload["research_use"]),
        content_type=str(payload["content_type"]),
        content_encoding=(
            None if payload.get("content_encoding") is None else str(payload["content_encoding"])
        ),
        raw_bytes=int(str(payload["raw_bytes"])),
        raw_sha256=str(payload["raw_sha256"]),
        raw_path=str(payload["raw_path"]),
        event_path=str(payload["event_path"]),
        request_parameters=_object_mapping(payload["request_parameters"], "request_parameters"),
        response_metadata=_object_mapping(payload["response_metadata"], "response_metadata"),
        upstream_snapshot_ids=tuple(
            str(item)
            for item in _object_list(payload["upstream_snapshot_ids"], "upstream_snapshot_ids")
        ),
        integrity_notes=tuple(
            str(item) for item in _object_list(payload["integrity_notes"], "integrity_notes")
        ),
        source_policy_checked_at=str(payload["source_policy_checked_at"]),
        raw_redistribution=str(payload["raw_redistribution"]),
        derived_redistribution=str(payload["derived_redistribution"]),
        public_gui=str(payload["public_gui"]),
        active_profile_changed=bool(payload["active_profile_changed"]),
        previous_record_sha256=(
            None
            if payload.get("previous_record_sha256") is None
            else str(payload["previous_record_sha256"])
        ),
        record_sha256=str(payload["record_sha256"]),
    )


def _validate_component(value: str, label: str) -> None:
    if not _SAFE_COMPONENT.fullmatch(value):
        raise ValueError(f"{label} must contain only lowercase safe path characters.")


def _aware_datetime(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware.")
    return value


def _extension_for(content_type: str, content_encoding: str | None) -> str:
    normalized = content_type.lower().split(";", maxsplit=1)[0].strip()
    extension = {
        "application/json": ".json",
        "text/json": ".json",
        "text/csv": ".csv",
        "application/zip": ".zip",
        "application/pdf": ".pdf",
        "text/plain": ".txt",
        "application/xml": ".xml",
        "text/xml": ".xml",
    }.get(normalized, ".bin")
    if content_encoding and content_encoding.lower() == "gzip":
        extension += ".gz"
    return extension


def _object_mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object.")
    return {str(key): item for key, item in value.items()}


def _object_list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list.")
    return value


def _write_once(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
    except FileExistsError:
        existing = path.read_bytes()
        if existing != payload:
            raise ArchiveIntegrityError(f"Immutable path collision at {path}.")
        return
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    path.chmod(0o444)
