"""Archive and classify documents for genuinely new monitored SEC filings."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import html
import json
import os
import re
import zlib
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import cast
from urllib.parse import quote
from urllib.request import Request, urlopen

from ai_trading_bot.data.history_archive import (
    ArchiveIntegrityError,
    PointInTimeArchive,
    SnapshotRecord,
    canonical_json_bytes,
    load_source_policies,
)

try:
    from scripts.trusted_data_research import trusted_data_environment
except ModuleNotFoundError:
    from trusted_data_research import (  # type: ignore[import-not-found,no-redef]
        trusted_data_environment,
    )

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARCHIVE = ROOT / "outputs" / "high_integrity_history"
DEFAULT_POLICIES = ROOT / "config" / "data_source_policies.json"
DEFAULT_DOCUMENT_POLICY = ROOT / "config" / "sec_filing_document_policy.json"

_DOCUMENT_BLOCK = re.compile(rb"<DOCUMENT>(.*?)</DOCUMENT>", re.IGNORECASE | re.DOTALL)
_DOCUMENT_FIELD = {
    "document_type": re.compile(rb"<TYPE>\s*([^\r\n<]+)", re.IGNORECASE),
    "sequence": re.compile(rb"<SEQUENCE>\s*([^\r\n<]+)", re.IGNORECASE),
    "file_name": re.compile(rb"<FILENAME>\s*([^\r\n<]+)", re.IGNORECASE),
    "description": re.compile(rb"<DESCRIPTION>\s*([^\r\n<]+)", re.IGNORECASE),
}
_TAG = re.compile(r"<[^>]+>")
_WHITESPACE = re.compile(r"\s+")
_XBRL_EXTENSIONS = {".xml", ".xsd", ".json"}


@dataclass(frozen=True)
class FilingDocumentPolicy:
    max_file_bytes: int
    max_complete_submission_bytes: int
    max_package_bytes: int
    max_xbrl_assets: int
    classification_rules_version: str


@dataclass(frozen=True)
class FilingDocumentEvent:
    accession_number: str
    cik: str
    symbol: str
    form: str
    items: tuple[str, ...]
    decision_available_at: datetime
    primary_document: str
    primary_document_url: str
    complete_submission_url: str
    filing_index_url: str
    event_source_snapshot_id: str


@dataclass(frozen=True)
class DocumentHttpCapture:
    role: str
    file_name: str
    source_url: str
    payload: bytes
    decoded_payload: bytes
    request_started_at: datetime
    retrieved_at: datetime
    content_type: str
    content_encoding: str | None
    response_metadata: dict[str, object]


def load_document_policy(path: Path) -> FilingDocumentPolicy:
    payload = json.loads(path.read_text())
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != "sec_filing_document_policy_v1"
    ):
        raise ValueError("SEC filing document policy has an unsupported schema.")
    required_true = (
        "research_only",
        "forward_events_only",
        "fetch_index_json",
        "fetch_complete_submission",
        "fetch_primary_document",
        "fetch_xbrl_assets",
    )
    if any(payload.get(key) is not True for key in required_true):
        raise ValueError("SEC filing document policy must retain all required safeguards.")
    required_false = (
        "sentiment_or_direction_inference",
        "alpha_calculation_enabled",
        "execution_price_selected",
        "paper_fill_applied",
    )
    if any(payload.get(key) is not False for key in required_false):
        raise ValueError("SEC filing document policy enables a prohibited action.")
    return FilingDocumentPolicy(
        max_file_bytes=_positive_int(payload.get("max_file_bytes"), "max_file_bytes"),
        max_complete_submission_bytes=_positive_int(
            payload.get("max_complete_submission_bytes"),
            "max_complete_submission_bytes",
        ),
        max_package_bytes=_positive_int(
            payload.get("max_package_bytes"),
            "max_package_bytes",
        ),
        max_xbrl_assets=_positive_int(
            payload.get("max_xbrl_assets"),
            "max_xbrl_assets",
        ),
        classification_rules_version=_required_string(
            payload,
            "classification_rules_version",
        ),
    )


def load_pending_filing_document_events(
    archive: PointInTimeArchive,
) -> tuple[FilingDocumentEvent, ...]:
    if not archive.manifest_path.exists():
        return ()
    events: dict[str, FilingDocumentEvent] = {}
    completed: set[str] = set()
    for record in _manifest_records(archive):
        if record.get("source_id") != "internal_derived":
            continue
        dataset = record.get("dataset")
        if dataset not in {"sec_filing_events", "sec_filing_document_packages"}:
            continue
        payload = _derived_payload(archive, record, str(dataset))
        for row in cast(list[object], payload["rows"]):
            if not isinstance(row, dict):
                raise ArchiveIntegrityError(f"Archive dataset {dataset} contains an invalid row.")
            accession = _required_string(row, "accession_number")
            if dataset == "sec_filing_document_packages":
                completed.add(accession)
                continue
            if (
                row.get("first_observation_class") != "monitored_new_accession"
                or row.get("forward_event_eligible") is not True
            ):
                continue
            event = _filing_event(row)
            events[event.accession_number] = event
    return tuple(
        sorted(
            (event for accession, event in events.items() if accession not in completed),
            key=lambda event: (event.decision_available_at, event.accession_number),
        )
    )


def fetch_sec_document(
    *,
    role: str,
    file_name: str,
    source_url: str,
    user_agent: str,
    max_file_bytes: int,
) -> DocumentHttpCapture:
    _validate_file_name(file_name)
    started = datetime.now(UTC)
    request = Request(
        source_url,
        headers={"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"},
    )
    with urlopen(request, timeout=60) as response:  # noqa: S310 - SEC URL from archived event
        declared_length = response.headers.get("Content-Length")
        if declared_length is not None and int(declared_length) > max_file_bytes:
            raise ValueError(
                f"SEC document {file_name} declared {declared_length} bytes; "
                f"limit is {max_file_bytes}."
            )
        payload = response.read(max_file_bytes + 1)
        retrieved = datetime.now(UTC)
        if len(payload) > max_file_bytes:
            raise ValueError(f"SEC document {file_name} exceeded the {max_file_bytes}-byte limit.")
        encoding = response.headers.get("Content-Encoding")
        decoded = _decode_http_payload(payload, encoding)
        if len(decoded) > max_file_bytes:
            raise ValueError(
                f"Decoded SEC document {file_name} exceeded the {max_file_bytes}-byte limit."
            )
        metadata = {
            key: response.headers[key]
            for key in ("Date", "ETag", "Last-Modified", "Content-Length")
            if response.headers.get(key) is not None
        }
        metadata["http_status"] = response.status
        metadata["latency_ms"] = round((retrieved - started).total_seconds() * 1000, 3)
        return DocumentHttpCapture(
            role=role,
            file_name=file_name,
            source_url=source_url,
            payload=payload,
            decoded_payload=decoded,
            request_started_at=started,
            retrieved_at=retrieved,
            content_type=response.headers.get_content_type(),
            content_encoding=encoding,
            response_metadata=metadata,
        )


def classify_filing_event(
    event: FilingDocumentEvent,
    *,
    complete_submission: bytes,
    primary_document: bytes,
    rules_version: str,
) -> dict[str, object]:
    text = _normalized_document_text(complete_submission + b"\n" + primary_document)
    lower_text = text.casefold()
    evidence: dict[str, set[str]] = {}

    def add(event_type: str, basis: str) -> None:
        evidence.setdefault(event_type, set()).add(basis)

    if event.form in {"10-K", "10-K/A", "10-Q", "10-Q/A", "20-F", "20-F/A"}:
        add("periodic_financial_report", f"sec_form:{event.form}")
    if event.form in {"3", "3/A", "4", "4/A", "5", "5/A"}:
        add("insider_activity", f"sec_form:{event.form}")
    if event.form.startswith(("S-", "F-", "424B")):
        add("capital_markets_filing", f"sec_form:{event.form}")

    item_rules = {
        "1.01": "material_agreement",
        "1.03": "bankruptcy_or_receivership",
        "1.05": "cybersecurity_incident",
        "2.01": "acquisition_or_disposition",
        "2.02": "earnings_results",
        "2.03": "debt_obligation",
        "3.01": "listing_status",
        "3.02": "equity_issuance_or_dilution",
        "4.02": "accounting_revision",
        "5.02": "management_change",
        "7.01": "regulation_fd_disclosure",
    }
    for item in event.items:
        event_type = item_rules.get(item)
        if event_type is not None:
            add(event_type, f"sec_item:{item}")

    term_rules = {
        "guidance_or_outlook": (
            "financial guidance",
            "full-year guidance",
            "full year guidance",
            "business outlook",
            "financial outlook",
        ),
        "buyback_or_repurchase": (
            "share repurchase program",
            "stock repurchase program",
            "share buyback",
            "stock buyback",
        ),
        "dividend_change": (
            "declared a quarterly cash dividend",
            "increase the quarterly dividend",
            "reduced the quarterly dividend",
            "suspend the dividend",
        ),
        "equity_issuance_or_dilution": (
            "at-the-market offering",
            "at the market offering",
            "registered direct offering",
            "private placement of shares",
            "public offering of common stock",
        ),
        "debt_financing": (
            "senior notes offering",
            "offering of senior notes",
            "entered into a credit agreement",
            "credit facility",
        ),
        "merger_or_acquisition": (
            "entered into a merger agreement",
            "definitive merger agreement",
            "definitive agreement to acquire",
        ),
        "workforce_restructuring": (
            "reduction in force",
            "workforce reduction",
            "restructuring plan",
        ),
    }
    for event_type, terms in term_rules.items():
        for term in terms:
            if term in lower_text:
                add(event_type, f"document_term:{term}")

    if not evidence:
        add("other_filing", f"sec_form:{event.form}")
    return {
        "classifier_version": rules_version,
        "classification_method": "deterministic_form_item_and_exact_term_rules",
        "event_types": sorted(evidence),
        "evidence": [
            {"event_type": event_type, "basis": sorted(bases)}
            for event_type, bases in sorted(evidence.items())
        ],
        "sentiment_or_direction_inferred": False,
        "expected_return_inferred": False,
        "alpha_calculated": False,
    }


def collect(
    archive: PointInTimeArchive,
    *,
    user_agent: str,
    policy: FilingDocumentPolicy,
    document_captures: Mapping[str, tuple[DocumentHttpCapture, ...]] | None = None,
) -> dict[str, object]:
    pre_audit = archive.audit()
    if pre_audit.status == "fail":
        raise ArchiveIntegrityError("Refusing document collection because archive audit failed.")
    events = load_pending_filing_document_events(archive)
    if not events:
        return _status_output("completed", (), (), archive)

    staged: list[tuple[FilingDocumentEvent, tuple[DocumentHttpCapture, ...]]] = []
    for event in events:
        captures = (
            document_captures[event.accession_number]
            if document_captures is not None
            else _fetch_package(event, user_agent=user_agent, policy=policy)
        )
        _validate_package(event, captures, policy)
        staged.append((event, captures))

    package_rows: list[dict[str, object]] = []
    all_upstream_ids: set[str] = set()
    raw_records: list[SnapshotRecord] = []
    for event, captures in staged:
        records = tuple(_archive_document(archive, event, capture) for capture in captures)
        raw_records.extend(records)
        complete = _capture_for_role(captures, "complete_submission")
        primary = _capture_for_role(captures, "primary_document")
        classification = classify_filing_event(
            event,
            complete_submission=complete.decoded_payload,
            primary_document=primary.decoded_payload,
            rules_version=policy.classification_rules_version,
        )
        inventory = _complete_submission_inventory(complete.decoded_payload)
        latest_retrieval = max(capture.retrieved_at for capture in captures)
        row = {
            "observation_type": "sec_filing_document_package",
            "accession_number": event.accession_number,
            "cik": event.cik,
            "symbol": event.symbol,
            "form": event.form,
            "items": list(event.items),
            "filing_event_decision_available_at": event.decision_available_at.isoformat(),
            "document_package_available_at": latest_retrieval.isoformat(),
            "document_availability_basis": "all_required_documents_first_observed",
            "market_timezone": "America/New_York",
            "event_source_snapshot_id": event.event_source_snapshot_id,
            "document_records": [
                {
                    "role": capture.role,
                    "file_name": capture.file_name,
                    "source_url": capture.source_url,
                    "snapshot_id": record.snapshot_id,
                    "retrieved_at": record.retrieved_at,
                    "raw_sha256": record.raw_sha256,
                    "raw_bytes": record.raw_bytes,
                    "content_type": record.content_type,
                }
                for capture, record in zip(captures, records, strict=True)
            ],
            "complete_submission_inventory": inventory,
            "exhibit_count": sum(
                str(item["document_type"]).upper().startswith("EX-") for item in inventory
            ),
            "xbrl_asset_count": sum(capture.role == "xbrl_asset" for capture in captures),
            "classification": classification,
            "data_quality_status": "pass",
            "point_in_time_scope": "forward_only_from_document_first_observation",
            "first_tradable_status": "handled_by_separate_forward_only_resolver",
            "market_observation_only": True,
            "execution_price_selected": False,
            "paper_fill_applied": False,
            "active_profile_changed": False,
            "research_only": True,
        }
        package_rows.append(row)
        all_upstream_ids.add(event.event_source_snapshot_id)
        all_upstream_ids.update(record.snapshot_id for record in records)

    derived_payload = {
        "schema_version": "sec_filing_document_packages_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "point_in_time_scope": "genuinely_new_monitored_accessions_only",
        "classification_rules_version": policy.classification_rules_version,
        "sentiment_or_direction_inference": False,
        "alpha_calculated": False,
        "execution_price_selected": False,
        "paper_fill_applied": False,
        "rows": package_rows,
    }
    latest_document_retrieval = max(
        _parse_aware_datetime(row["document_package_available_at"]) for row in package_rows
    )
    started_at = max(datetime.now(UTC), latest_document_retrieval)
    derived_retrieved_at = started_at + timedelta(microseconds=1)
    derived_record = archive.capture(
        source_id="internal_derived",
        dataset="sec_filing_document_packages",
        source_url="internal://events/sec-filing-document-packages",
        payload=canonical_json_bytes(derived_payload),
        request_started_at=started_at,
        retrieved_at=derived_retrieved_at,
        decision_available_at=latest_document_retrieval,
        decision_availability_basis="all_required_documents_first_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        request_parameters={"accession_count": len(package_rows)},
        response_metadata={
            "package_count": len(package_rows),
            "document_count": len(raw_records),
        },
        upstream_snapshot_ids=tuple(sorted(all_upstream_ids)),
        integrity_notes=(
            "Historical baseline accessions are categorically excluded.",
            "Complete submissions preserve filing document blocks and exhibits.",
            "Classification is deterministic, neutral, and not an alpha calculation.",
            "No order, execution price, paper fill, or profile change is created.",
        ),
    )
    return _status_output("captured", package_rows, raw_records, archive, derived_record)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICIES)
    parser.add_argument("--document-policy", type=Path, default=DEFAULT_DOCUMENT_POLICY)
    args = parser.parse_args()
    environment = trusted_data_environment(os.environ, ROOT / ".env")
    email = environment.get("SEC_USER_AGENT_EMAIL", "").strip()
    if not email:
        raise ValueError("Missing SEC_USER_AGENT_EMAIL.")
    name = environment.get("SEC_USER_AGENT_NAME", "AI-TRADING-BOT/0.1").strip()
    archive = PointInTimeArchive(args.archive_root, load_source_policies(args.policies))
    output = collect(
        archive,
        user_agent=f"{name} {email}",
        policy=load_document_policy(args.document_policy),
    )
    latest = args.archive_root / "latest_sec_filing_document_collection.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(
        "SEC filing document collection",
        output["status"],
        "pending",
        output["pending_event_count"],
        "captured",
        output["captured_package_count"],
    )


def _fetch_package(
    event: FilingDocumentEvent,
    *,
    user_agent: str,
    policy: FilingDocumentPolicy,
) -> tuple[DocumentHttpCapture, ...]:
    base_url = event.filing_index_url.rsplit("/", maxsplit=1)[0]
    index = fetch_sec_document(
        role="index",
        file_name="index.json",
        source_url=f"{base_url}/index.json",
        user_agent=user_agent,
        max_file_bytes=policy.max_file_bytes,
    )
    index_names = _index_file_names(index.decoded_payload)
    primary_document_name = _resolve_primary_document_name(
        event.primary_document,
        index_names,
    )
    complete_submission_name = PurePosixPath(event.complete_submission_url).name
    required_names = {primary_document_name, complete_submission_name}
    missing = required_names - set(index_names)
    if missing:
        raise ValueError(
            f"SEC filing index for {event.accession_number} is missing {sorted(missing)}."
        )
    captures = [
        index,
        fetch_sec_document(
            role="complete_submission",
            file_name=complete_submission_name,
            source_url=event.complete_submission_url,
            user_agent=user_agent,
            max_file_bytes=policy.max_complete_submission_bytes,
        ),
        fetch_sec_document(
            role="primary_document",
            file_name=primary_document_name,
            source_url=f"{base_url}/{quote(primary_document_name, safe='/._-')}",
            user_agent=user_agent,
            max_file_bytes=policy.max_file_bytes,
        ),
    ]
    xbrl_names = [
        name
        for name in index_names
        if _is_xbrl_asset(name) and name not in required_names and name != "index.json"
    ]
    if len(xbrl_names) > policy.max_xbrl_assets:
        raise ValueError(
            f"SEC filing {event.accession_number} has {len(xbrl_names)} XBRL assets; "
            f"limit is {policy.max_xbrl_assets}."
        )
    for name in xbrl_names:
        captures.append(
            fetch_sec_document(
            role="xbrl_asset",
            file_name=name,
            source_url=f"{base_url}/{quote(name, safe='/._-')}",
                user_agent=user_agent,
                max_file_bytes=policy.max_file_bytes,
            )
        )
    return tuple(captures)


def _validate_package(
    event: FilingDocumentEvent,
    captures: tuple[DocumentHttpCapture, ...],
    policy: FilingDocumentPolicy,
) -> None:
    if not captures:
        raise ValueError(f"SEC filing {event.accession_number} has no document captures.")
    roles = [capture.role for capture in captures]
    for required in ("index", "complete_submission", "primary_document"):
        if roles.count(required) != 1:
            raise ValueError(
                f"SEC filing {event.accession_number} requires exactly one {required} capture."
            )
    allowed_roles = {"index", "complete_submission", "primary_document", "xbrl_asset"}
    if any(role not in allowed_roles for role in roles):
        raise ValueError(f"SEC filing {event.accession_number} contains an unsupported role.")
    total_bytes = 0
    total_decoded_bytes = 0
    seen_files: set[tuple[str, str]] = set()
    for capture in captures:
        _validate_file_name(capture.file_name)
        _require_aware(capture.request_started_at, "request_started_at")
        _require_aware(capture.retrieved_at, "retrieved_at")
        if capture.request_started_at > capture.retrieved_at:
            raise ValueError("Document request start cannot follow retrieval.")
        if not capture.payload or not capture.decoded_payload:
            raise ValueError(f"SEC document {capture.file_name} is empty.")
        byte_limit = _capture_byte_limit(capture, policy)
        if len(capture.payload) > byte_limit:
            raise ValueError(f"SEC document {capture.file_name} exceeds the file-size limit.")
        if len(capture.decoded_payload) > byte_limit:
            raise ValueError(
                f"Decoded SEC document {capture.file_name} exceeds the file-size limit."
            )
        key = (capture.role, capture.file_name)
        if key in seen_files:
            raise ValueError(f"SEC document package repeats {capture.role}:{capture.file_name}.")
        seen_files.add(key)
        total_bytes += len(capture.payload)
        total_decoded_bytes += len(capture.decoded_payload)
    if total_bytes > policy.max_package_bytes or total_decoded_bytes > policy.max_package_bytes:
        raise ValueError(
            f"SEC filing {event.accession_number} package is {total_bytes} raw bytes and "
            f"{total_decoded_bytes} decoded bytes; "
            f"limit is {policy.max_package_bytes}."
        )
    index = _capture_for_role(captures, "index")
    names = set(_index_file_names(index.decoded_payload))
    primary_document_name = _resolve_primary_document_name(event.primary_document, names)
    primary_capture = _capture_for_role(captures, "primary_document")
    if primary_capture.file_name != primary_document_name:
        raise ValueError("Captured primary document does not match the SEC directory index.")
    complete_name = PurePosixPath(event.complete_submission_url).name
    if complete_name not in names:
        raise ValueError("Complete submission is absent from the SEC directory index.")
    expected_xbrl = {
        name
        for name in names
        if _is_xbrl_asset(name)
        and name not in {primary_document_name, complete_name, "index.json"}
    }
    captured_xbrl = {capture.file_name for capture in captures if capture.role == "xbrl_asset"}
    if expected_xbrl != captured_xbrl:
        raise ValueError(
            "Captured XBRL assets do not exactly match the SEC directory index: "
            f"missing={sorted(expected_xbrl - captured_xbrl)}, "
            f"unexpected={sorted(captured_xbrl - expected_xbrl)}."
        )
    if len(captured_xbrl) > policy.max_xbrl_assets:
        raise ValueError("SEC filing package exceeds the XBRL asset-count limit.")
    inventory = _complete_submission_inventory(
        _capture_for_role(captures, "complete_submission").decoded_payload
    )
    inventory_names = {str(item["file_name"]) for item in inventory}
    if event.primary_document not in inventory_names:
        raise ValueError("Primary filing document is absent from complete submission blocks.")


def _capture_byte_limit(
    capture: DocumentHttpCapture,
    policy: FilingDocumentPolicy,
) -> int:
    return (
        policy.max_complete_submission_bytes
        if capture.role == "complete_submission"
        else policy.max_file_bytes
    )


def _archive_document(
    archive: PointInTimeArchive,
    event: FilingDocumentEvent,
    capture: DocumentHttpCapture,
) -> SnapshotRecord:
    dataset_by_role = {
        "index": "sec_filing_document_index",
        "complete_submission": "sec_filing_complete_submission",
        "primary_document": "sec_filing_primary_document",
        "xbrl_asset": "sec_filing_xbrl_asset",
    }
    return archive.capture(
        source_id="sec_edgar",
        dataset=dataset_by_role[capture.role],
        source_url=capture.source_url,
        payload=capture.payload,
        request_started_at=capture.request_started_at,
        retrieved_at=capture.retrieved_at,
        decision_available_at=capture.retrieved_at,
        decision_availability_basis="first_observed",
        market_timezone="UTC",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type=capture.content_type,
        content_encoding=capture.content_encoding,
        request_parameters={
            "accession_number": event.accession_number,
            "cik": event.cik,
            "symbol": event.symbol,
            "role": capture.role,
            "file_name": capture.file_name,
        },
        response_metadata=capture.response_metadata,
        integrity_notes=(
            "Document retrieval is restricted to a genuinely new monitored accession.",
            "SEC acceptance and collector document-retrieval times remain separate.",
            "No document timestamp is inferred or backdated.",
        ),
    )


def _filing_event(row: Mapping[str, object]) -> FilingDocumentEvent:
    primary_document = _required_string(row, "primary_document")
    primary_url = _required_string(row, "primary_document_url")
    complete_url = _required_string(row, "complete_submission_url")
    index_url = _required_string(row, "filing_index_url")
    urls = (primary_url, complete_url, index_url)
    if any(not value.startswith("https://www.sec.gov/Archives/edgar/data/") for value in urls):
        raise ArchiveIntegrityError("SEC filing event contains an unexpected document URL.")
    _validate_file_name(primary_document)
    raw_items = row.get("items")
    if not isinstance(raw_items, list):
        raise ArchiveIntegrityError("SEC filing event items must be a list.")
    return FilingDocumentEvent(
        accession_number=_required_string(row, "accession_number"),
        cik=_required_string(row, "cik"),
        symbol=_required_string(row, "symbol").upper(),
        form=_required_string(row, "form").upper(),
        items=tuple(sorted(str(item) for item in raw_items)),
        decision_available_at=_parse_aware_datetime(row.get("decision_available_at")),
        primary_document=primary_document,
        primary_document_url=primary_url,
        complete_submission_url=complete_url,
        filing_index_url=index_url,
        event_source_snapshot_id=_required_string(row, "source_snapshot_id"),
    )


def _manifest_records(archive: PointInTimeArchive) -> tuple[dict[str, object], ...]:
    records: list[dict[str, object]] = []
    for line in archive.manifest_path.read_text().splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if not isinstance(record, dict):
            raise ArchiveIntegrityError("Archive manifest contains a non-object record.")
        records.append(record)
    return tuple(records)


def _derived_payload(
    archive: PointInTimeArchive,
    record: Mapping[str, object],
    dataset: str,
) -> dict[str, object]:
    payload = json.loads((archive.root / str(record.get("raw_path", ""))).read_text())
    if not isinstance(payload, dict) or not isinstance(payload.get("rows"), list):
        raise ArchiveIntegrityError(f"Archive dataset {dataset} has an invalid schema.")
    return payload


def _index_file_names(payload: bytes) -> tuple[str, ...]:
    parsed = json.loads(payload)
    directory = parsed.get("directory") if isinstance(parsed, dict) else None
    items = directory.get("item") if isinstance(directory, dict) else None
    if not isinstance(items, list):
        raise ValueError("SEC filing directory index has an invalid item list.")
    names: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("SEC filing directory index contains a non-object item.")
        name = _required_string(item, "name")
        _validate_file_name(name)
        names.append(name)
    if len(names) != len(set(names)):
        raise ValueError("SEC filing directory index repeats a filename.")
    return tuple(sorted(names))


def _resolve_primary_document_name(
    declared_name: str,
    index_names: Collection[str],
) -> str:
    """Resolve SEC's occasional display-wrapper path to its indexed archive name.

    SEC submissions may declare a primary document as, for example,
    ``xslSCHEDULE_13G_X01/primary_doc.xml`` while the authoritative filing
    directory index lists the actual archived file as ``primary_doc.xml``.
    The fallback is deliberately narrow: the declared name must be a safe
    nested path and its basename must occur exactly once in that index.
    """
    _validate_file_name(declared_name)
    names = set(index_names)
    if declared_name in names:
        return declared_name
    if "/" not in declared_name:
        raise ValueError("Primary filing document is absent from the SEC directory index.")
    basename = PurePosixPath(declared_name).name
    candidates = [name for name in names if PurePosixPath(name).name == basename]
    if len(candidates) != 1:
        raise ValueError(
            "SEC primary-document wrapper path cannot be resolved uniquely from the "
            f"directory index: declared={declared_name!r}, candidates={sorted(candidates)!r}."
        )
    return candidates[0]


def _complete_submission_inventory(payload: bytes) -> list[dict[str, object]]:
    inventory: list[dict[str, object]] = []
    for index, block in enumerate(_DOCUMENT_BLOCK.findall(payload), start=1):
        fields = {
            key: _matched_bytes_text(pattern, block) for key, pattern in _DOCUMENT_FIELD.items()
        }
        file_name = fields["file_name"]
        document_type = fields["document_type"]
        if file_name is None or document_type is None:
            raise ValueError("Complete submission document block lacks type or filename.")
        _validate_file_name(file_name)
        inventory.append(
            {
                "block_number": index,
                "document_type": document_type,
                "sequence": fields["sequence"],
                "file_name": file_name,
                "description": fields["description"],
                "block_bytes": len(block),
                "block_sha256": hashlib.sha256(block).hexdigest(),
            }
        )
    if not inventory:
        raise ValueError("Complete submission contains no parseable document blocks.")
    return inventory


def _normalized_document_text(payload: bytes) -> str:
    decoded = payload.decode("utf-8", errors="replace")
    without_tags = _TAG.sub(" ", decoded)
    return _WHITESPACE.sub(" ", html.unescape(without_tags)).strip()


def _capture_for_role(
    captures: tuple[DocumentHttpCapture, ...],
    role: str,
) -> DocumentHttpCapture:
    matches = [capture for capture in captures if capture.role == role]
    if len(matches) != 1:
        raise ValueError(f"Document package requires exactly one {role} capture.")
    return matches[0]


def _is_xbrl_asset(file_name: str) -> bool:
    lower = file_name.casefold()
    suffix = PurePosixPath(lower).suffix
    return suffix in _XBRL_EXTENSIONS and lower != "index.json"


def _validate_file_name(value: str) -> None:
    if not value or "\\" in value or "\x00" in value:
        raise ValueError(f"Unsafe SEC filing filename: {value}")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or str(path) != value
        or any(part in {"", ".", ".."} for part in path.parts)
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(f"Unsafe SEC filing filename: {value}")


def _decode_http_payload(payload: bytes, content_encoding: str | None) -> bytes:
    encoding = "" if content_encoding is None else content_encoding.lower().strip()
    if encoding == "gzip":
        return gzip.decompress(payload)
    if encoding == "deflate":
        return zlib.decompress(payload)
    return payload


def _matched_bytes_text(pattern: re.Pattern[bytes], payload: bytes) -> str | None:
    match = pattern.search(payload)
    if match is None:
        return None
    text = match.group(1).decode("utf-8", errors="replace").strip()
    return text or None


def _parse_aware_datetime(value: object) -> datetime:
    text = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp: {text}") from exc
    return _require_aware(parsed, "timestamp")


def _require_aware(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware.")
    return value


def _required_string(values: Mapping[str, object], key: str) -> str:
    text = str(values.get(key, "")).strip()
    if not text:
        raise ValueError(f"Missing required field: {key}")
    return text


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a positive integer.")
    parsed = int(str(value))
    if parsed <= 0:
        raise ValueError(f"{label} must be a positive integer.")
    return parsed


def _status_output(
    status: str,
    rows: tuple[()] | list[dict[str, object]],
    raw_records: tuple[()] | list[SnapshotRecord],
    archive: PointInTimeArchive,
    derived_record: SnapshotRecord | None = None,
) -> dict[str, object]:
    audit = archive.audit()
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_only": True,
        "trading_actions_enabled": False,
        "active_profile_changed": False,
        "status": status if audit.status == "pass" else "integrity_audit_failed",
        "pending_event_count": len(load_pending_filing_document_events(archive)),
        "captured_package_count": len(rows),
        "captured_document_count": len(raw_records),
        "packages": list(rows),
        "derived_snapshot": None if derived_record is None else derived_record.to_dict(),
        "archive_audit": audit.to_dict(),
        "alpha_calculated": False,
        "execution_price_selected": False,
        "paper_fill_applied": False,
    }


if __name__ == "__main__":
    main()
