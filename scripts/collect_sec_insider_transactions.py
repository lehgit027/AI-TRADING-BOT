"""Create a forward-only ledger of clear SEC Form 4 open-market transactions.

Only Form 4/4-A packages that this collector first observed through the live SEC
monitor are considered.  The ledger deliberately accepts only non-derivative
open-market purchases (P) and sales (S) with a positive share count and price.
Awards, gifts, tax withholding, exercises, conversions, derivative transactions,
and every unknown code remain background information, not forward signals.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import xml.etree.ElementTree as element_tree
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import cast

from ai_trading_bot.data.history_archive import (
    ArchiveIntegrityError,
    PointInTimeArchive,
    SnapshotRecord,
    canonical_json_bytes,
    load_source_policies,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARCHIVE = ROOT / "outputs" / "high_integrity_history"
DEFAULT_POLICIES = ROOT / "config" / "data_source_policies.json"
_FORM_4S = {"4", "4/A"}
_UNSAFE_XML_DECLARATION = re.compile(br"<!\s*(?:DOCTYPE|ENTITY)\b", re.IGNORECASE)


@dataclass(frozen=True)
class Form4Package:
    accession_number: str
    cik: str
    symbol: str
    form: str
    acceptance_at: datetime
    first_seen_at: datetime
    decision_available_at: datetime
    document_snapshot_id: str
    document_sha256: str
    document_retrieved_at: datetime
    package_snapshot_id: str


def collect(archive: PointInTimeArchive) -> dict[str, object]:
    """Extract clear transactions from newly monitored, archived Form 4 XML."""
    pre_audit = archive.audit()
    if pre_audit.status == "fail":
        raise ArchiveIntegrityError("Refusing insider extraction after archive audit failure.")
    packages = load_pending_form4_packages(archive)
    if not packages:
        return _status_output("completed", (), (), archive)

    accepted_rows: list[dict[str, object]] = []
    rejected_filings: list[dict[str, object]] = []
    upstream_ids: set[str] = set()
    latest_decision_at: datetime | None = None
    for package in packages:
        upstream_ids.update({package.document_snapshot_id, package.package_snapshot_id})
        latest_decision_at = _later(latest_decision_at, package.decision_available_at)
        try:
            raw_xml = _load_verified_xml(archive, package)
            rows, exclusions = extract_open_market_transactions(raw_xml, package=package)
        except (ArchiveIntegrityError, ValueError, element_tree.ParseError) as exc:
            rejected_filings.append(
                _rejected_filing(
                    package,
                    reason=f"blocked_{type(exc).__name__}: {exc}",
                    remediation_needed=(
                        "Retain a matching, parseable raw SEC Form 4 XML document with "
                        "verified hash and monitored-forward provenance."
                    ),
                )
            )
            continue
        if not rows:
            rejected_filings.append(
                _rejected_filing(
                    package,
                    reason="no_clear_non_derivative_open_market_transaction",
                    remediation_needed=(
                        "A future Form 4 must contain a clear non-derivative P or S "
                        "transaction with positive shares and price."
                    ),
                    exclusion_counts=_counts(exclusions),
                )
            )
            continue
        accepted_rows.extend(rows)

    derived_record: SnapshotRecord | None = None
    if accepted_rows or rejected_filings:
        now = datetime.now(UTC)
        decision_at = latest_decision_at or now
        started_at = max(now, decision_at)
        payload = {
            "schema_version": "sec_insider_transaction_events_v1",
            "generated_at": now.isoformat(),
            "point_in_time_scope": "genuinely_new_monitored_form4_accessions_only",
            "historical_form4_status": "background_only_not_forward_signal_eligible",
            "accepted_transaction_rule": (
                "non_derivative_transaction_code_P_or_S_with_matching_A_or_D_and_positive_"
                "shares_and_price"
            ),
            "rows": accepted_rows,
            "rejected_filings": rejected_filings,
            "alpha_calculated": False,
            "execution_price_selected": False,
            "paper_fill_applied": False,
            "active_profile_changed": False,
            "research_only": True,
        }
        derived_record = archive.capture(
            source_id="internal_derived",
            dataset="sec_insider_transaction_events",
            source_url="internal://events/sec-form4-open-market-transactions",
            payload=canonical_json_bytes(payload),
            request_started_at=started_at,
            retrieved_at=started_at + timedelta(microseconds=1),
            decision_available_at=decision_at,
            decision_availability_basis="verified_primary_form4_xml_first_observed",
            market_timezone="America/New_York",
            timestamp_quality="first_seen_exact",
            research_use="forward_only",
            content_type="application/json",
            request_parameters={"form4_package_count": len(packages)},
            response_metadata={
                "accepted_transaction_count": len(accepted_rows),
                "rejected_filing_count": len(rejected_filings),
            },
            upstream_snapshot_ids=tuple(sorted(upstream_ids)),
            integrity_notes=(
                "Only accessions first observed by the live monitor are eligible.",
                "Raw SEC XML, its retrieval time, and SHA-256 are retained separately.",
                "This ledger excludes grants, conversions, derivatives, and ambiguous codes.",
                "It records events only; it creates no order, fill, candidate, or alpha result.",
            ),
        )
    status = "captured" if accepted_rows else "completed"
    return _status_output(status, accepted_rows, rejected_filings, archive, derived_record)


def load_pending_form4_packages(archive: PointInTimeArchive) -> tuple[Form4Package, ...]:
    """Return only unseen, live-monitored Form 4 packages with complete lineage."""
    if not archive.manifest_path.exists():
        return ()
    filing_events: dict[str, dict[str, object]] = {}
    packages: dict[str, tuple[dict[str, object], str]] = {}
    completed: set[str] = set()
    for record in _manifest_records(archive):
        if record.get("source_id") != "internal_derived":
            continue
        dataset = str(record.get("dataset", ""))
        if dataset not in {
            "sec_filing_events",
            "sec_filing_document_packages",
            "sec_insider_transaction_events",
        }:
            continue
        payload = _derived_payload(archive, record, dataset)
        rows = cast(list[object], payload["rows"])
        if dataset == "sec_filing_events":
            for raw in rows:
                if not isinstance(raw, dict):
                    raise ArchiveIntegrityError("SEC filing event row is invalid.")
                filing_events[_required_string(raw, "accession_number")] = raw
        elif dataset == "sec_filing_document_packages":
            for raw in rows:
                if not isinstance(raw, dict):
                    raise ArchiveIntegrityError("SEC document package row is invalid.")
                packages[_required_string(raw, "accession_number")] = (
                    raw,
                    _required_string(record, "snapshot_id"),
                )
        else:
            for raw in rows:
                if not isinstance(raw, dict):
                    raise ArchiveIntegrityError("SEC insider transaction row is invalid.")
                completed.add(_required_string(raw, "accession_number"))
            rejected = payload.get("rejected_filings", [])
            if not isinstance(rejected, list):
                raise ArchiveIntegrityError("SEC insider rejection ledger is invalid.")
            for raw in rejected:
                if not isinstance(raw, dict):
                    raise ArchiveIntegrityError("SEC insider rejection row is invalid.")
                completed.add(_required_string(raw, "accession_number"))

    pending: list[Form4Package] = []
    for accession, (package, package_snapshot_id) in packages.items():
        if accession in completed or str(package.get("form", "")).upper() not in _FORM_4S:
            continue
        event = filing_events.get(accession)
        if event is None:
            continue
        if (
            event.get("first_observation_class") != "monitored_new_accession"
            or event.get("forward_event_eligible") is not True
        ):
            continue
        if package.get("data_quality_status") != "pass":
            continue
        document = _primary_document_record(package)
        pending.append(
            Form4Package(
                accession_number=accession,
                cik=_normalize_cik(_required_string(package, "cik")),
                symbol=_required_string(package, "symbol").upper(),
                form=str(package.get("form", "")).upper(),
                acceptance_at=_parse_aware_datetime(event.get("acceptance_at")),
                first_seen_at=_parse_aware_datetime(event.get("first_seen_at")),
                decision_available_at=_parse_aware_datetime(document.get("retrieved_at")),
                document_snapshot_id=_required_string(document, "snapshot_id"),
                document_sha256=_required_sha256(document.get("raw_sha256")),
                document_retrieved_at=_parse_aware_datetime(document.get("retrieved_at")),
                package_snapshot_id=package_snapshot_id,
            )
        )
    return tuple(
        sorted(pending, key=lambda item: (item.decision_available_at, item.accession_number))
    )


def extract_open_market_transactions(
    payload: bytes,
    *,
    package: Form4Package,
) -> tuple[list[dict[str, object]], list[str]]:
    if _UNSAFE_XML_DECLARATION.search(payload):
        raise ValueError("SEC Form 4 XML contains a prohibited declaration.")
    root = element_tree.fromstring(payload)
    if _local_name(root.tag) != "ownershipDocument":
        raise ValueError("primary document is not an SEC ownershipDocument XML payload.")
    issuer_cik = _first_text(root, "issuerCik")
    if issuer_cik is None or _normalize_cik(issuer_cik) != package.cik:
        raise ArchiveIntegrityError("Form 4 issuer CIK does not match the monitored filing.")

    owner = _reporting_owner(root)
    transactions: list[dict[str, object]] = []
    exclusions: list[str] = []
    for ordinal, element in enumerate(_elements(root, "nonDerivativeTransaction"), start=1):
        code = _nested_text(element, "transactionCoding", "transactionCode")
        acquired_disposed = _nested_text(
            element,
            "transactionAmounts",
            "transactionAcquiredDisposedCode",
            "value",
        )
        if code not in {"P", "S"}:
            exclusions.append(f"unsupported_transaction_code:{code or 'missing'}")
            continue
        expected = "A" if code == "P" else "D"
        if acquired_disposed != expected:
            exclusions.append("transaction_code_direction_mismatch")
            continue
        try:
            shares = _positive_decimal(
                _nested_text(element, "transactionAmounts", "transactionShares", "value"),
                "transaction shares",
            )
            price = _positive_decimal(
                _nested_text(element, "transactionAmounts", "transactionPricePerShare", "value"),
                "transaction price",
            )
        except ValueError:
            exclusions.append("missing_or_invalid_positive_shares_or_price")
            continue
        transaction_date = _nested_text(element, "transactionDate", "value")
        security_title = _nested_text(element, "securityTitle", "value")
        if transaction_date is None or security_title is None:
            exclusions.append("missing_required_transaction_field")
            continue
        try:
            datetime.strptime(transaction_date, "%Y-%m-%d")
        except ValueError:
            exclusions.append("invalid_transaction_date")
            continue
        direction = "open_market_purchase" if code == "P" else "open_market_sale"
        transaction_id = f"{package.accession_number}:non_derivative:{ordinal}"
        transactions.append(
            {
                "observation_type": "sec_form4_open_market_transaction",
                "transaction_id": transaction_id,
                "accession_number": package.accession_number,
                "cik": package.cik,
                "symbol": package.symbol,
                "form": package.form,
                "transaction_date": transaction_date,
                "transaction_code": code,
                "transaction_direction": direction,
                "security_title": security_title,
                "shares": _decimal_text(shares),
                "price_per_share": _decimal_text(price),
                "transaction_value": _decimal_text(shares * price),
                "reporting_owner": owner,
                "acceptance_at": package.acceptance_at.isoformat(),
                "first_seen_at": package.first_seen_at.isoformat(),
                "first_seen_timestamp_quality": "collector_exact",
                "document_retrieved_at": package.document_retrieved_at.isoformat(),
                "decision_available_at": package.decision_available_at.isoformat(),
                "decision_availability_basis": "verified_primary_form4_xml_first_observed",
                "first_tradable_at": None,
                "first_tradable_status": "pending_separate_forward_only_resolver",
                "primary_document_snapshot_id": package.document_snapshot_id,
                "primary_document_sha256": package.document_sha256,
                "document_hash_verified": True,
                "package_snapshot_id": package.package_snapshot_id,
                "first_observation_class": "monitored_new_accession",
                "forward_event_eligible": True,
                "historical_backtest_eligible": False,
                "historical_form4_status": "background_only",
                "market_observation_only": True,
                "execution_price_selected": False,
                "paper_fill_applied": False,
                "alpha_calculated": False,
                "active_profile_changed": False,
                "research_only": True,
            }
        )
    for _ in _elements(root, "derivativeTransaction"):
        exclusions.append("derivative_transaction_excluded")
    return transactions, exclusions


def _load_verified_xml(archive: PointInTimeArchive, package: Form4Package) -> bytes:
    matching = [
        record
        for record in _manifest_records(archive)
        if record.get("snapshot_id") == package.document_snapshot_id
    ]
    if len(matching) != 1:
        raise ArchiveIntegrityError("Form 4 primary-document snapshot is missing or ambiguous.")
    record = matching[0]
    if (
        record.get("source_id") != "sec_edgar"
        or record.get("dataset") != "sec_filing_primary_document"
    ):
        raise ArchiveIntegrityError(
            "Form 4 primary-document snapshot has the wrong source or dataset."
        )
    payload = (archive.root / _required_string(record, "raw_path")).read_bytes()
    actual = hashlib.sha256(payload).hexdigest()
    if actual != package.document_sha256 or actual != _required_sha256(
        record.get("raw_sha256")
    ):
        raise ArchiveIntegrityError(
            "Form 4 primary-document SHA-256 does not match archived bytes."
        )
    parameters = record.get("request_parameters")
    if (
        not isinstance(parameters, dict)
        or parameters.get("accession_number") != package.accession_number
    ):
        raise ArchiveIntegrityError("Form 4 primary-document accession lineage does not match.")
    return payload


def _primary_document_record(package: Mapping[str, object]) -> dict[str, object]:
    records = package.get("document_records")
    if not isinstance(records, list):
        raise ArchiveIntegrityError("Form 4 package document records are invalid.")
    primary = [
        record
        for record in records
        if isinstance(record, dict) and record.get("role") == "primary_document"
    ]
    if len(primary) != 1:
        raise ArchiveIntegrityError("Form 4 package must contain exactly one primary document.")
    return primary[0]


def _reporting_owner(root: element_tree.Element) -> dict[str, object]:
    owner = next(iter(_elements(root, "reportingOwner")), None)
    if owner is None:
        raise ValueError("Form 4 has no reporting owner.")
    return {
        "name": _nested_text(owner, "reportingOwnerId", "rptOwnerName"),
        "cik": _optional_normalized_cik(_nested_text(owner, "reportingOwnerId", "rptOwnerCik")),
        "is_director": _nested_text(owner, "reportingOwnerRelationship", "isDirector"),
        "is_officer": _nested_text(owner, "reportingOwnerRelationship", "isOfficer"),
        "is_ten_percent_owner": _nested_text(
            owner,
            "reportingOwnerRelationship",
            "isTenPercentOwner",
        ),
        "officer_title": _nested_text(owner, "reportingOwnerRelationship", "officerTitle"),
    }


def _elements(root: element_tree.Element, local_name: str) -> list[element_tree.Element]:
    return [element for element in root.iter() if _local_name(element.tag) == local_name]


def _first_text(root: element_tree.Element, local_name: str) -> str | None:
    for element in _elements(root, local_name):
        if element.text is not None and element.text.strip():
            return element.text.strip()
    return None


def _nested_text(root: element_tree.Element, *names: str) -> str | None:
    current = root
    for name in names:
        matches = [child for child in current if _local_name(child.tag) == name]
        if len(matches) != 1:
            return None
        current = matches[0]
    return None if current.text is None or not current.text.strip() else current.text.strip()


def _local_name(tag: str) -> str:
    return tag.rsplit("}", maxsplit=1)[-1]


def _positive_decimal(value: str | None, name: str) -> Decimal:
    if value is None:
        raise ValueError(f"Form 4 {name} is missing.")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"Form 4 {name} is not numeric.") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"Form 4 {name} must be positive.")
    return parsed


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _normalize_cik(value: str) -> str:
    digits = value.strip()
    if not digits.isdigit():
        raise ArchiveIntegrityError("SEC CIK is not numeric.")
    return digits.zfill(10)


def _optional_normalized_cik(value: str | None) -> str | None:
    return None if value is None else _normalize_cik(value)


def _required_sha256(value: object) -> str:
    text = str(value).strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", text):
        raise ArchiveIntegrityError("Expected a SHA-256 hex digest.")
    return text


def _required_string(mapping: Mapping[str, object], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ArchiveIntegrityError(f"Missing required {key}.")
    return value.strip()


def _parse_aware_datetime(value: object) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ArchiveIntegrityError("Missing required timestamp.")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ArchiveIntegrityError("Timestamp must include a timezone.")
    return parsed


def _derived_payload(
    archive: PointInTimeArchive,
    record: Mapping[str, object],
    dataset: str,
) -> dict[str, object]:
    payload = json.loads((archive.root / _required_string(record, "raw_path")).read_text())
    if not isinstance(payload, dict) or not isinstance(payload.get("rows"), list):
        raise ArchiveIntegrityError(f"Archive dataset {dataset} has an invalid schema.")
    return payload


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


def _counts(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _rejected_filing(
    package: Form4Package,
    *,
    reason: str,
    remediation_needed: str,
    exclusion_counts: dict[str, int] | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "accession_number": package.accession_number,
        "form": package.form,
        "acceptance_at": package.acceptance_at.isoformat(),
        "first_seen_at": package.first_seen_at.isoformat(),
        "document_retrieved_at": package.document_retrieved_at.isoformat(),
        "primary_document_snapshot_id": package.document_snapshot_id,
        "primary_document_sha256": package.document_sha256,
        "reason": reason,
        "remediation_needed": remediation_needed,
    }
    if exclusion_counts:
        row["exclusion_counts"] = exclusion_counts
    return row


def _later(left: datetime | None, right: datetime) -> datetime:
    return right if left is None or right > left else left


def _status_output(
    status: str,
    accepted_rows: list[dict[str, object]] | tuple[()],
    rejected_filings: list[dict[str, object]] | tuple[()],
    archive: PointInTimeArchive,
    derived_record: SnapshotRecord | None = None,
) -> dict[str, object]:
    audit = archive.audit()
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_only": True,
        "active_profile_changed": False,
        "status": status if audit.status == "pass" else "integrity_audit_failed",
        "accepted_transaction_count": len(accepted_rows),
        "rejected_filing_count": len(rejected_filings),
        "transactions": list(accepted_rows),
        "rejected_filings": list(rejected_filings),
        "derived_snapshot": None if derived_record is None else derived_record.to_dict(),
        "archive_audit": audit.to_dict(),
        "alpha_calculated": False,
        "execution_price_selected": False,
        "paper_fill_applied": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICIES)
    args = parser.parse_args()
    archive = PointInTimeArchive(args.archive_root, load_source_policies(args.policies))
    output = collect(archive)
    latest = args.archive_root / "latest_sec_insider_transaction_collection.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(
        "SEC Form 4 ledger",
        output["status"],
        "accepted",
        output["accepted_transaction_count"],
        "rejected",
        output["rejected_filing_count"],
    )


if __name__ == "__main__":
    main()
