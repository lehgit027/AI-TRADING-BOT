"""Collect forward-only SEC filing events with exact first-observed availability."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import time
import zlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

from ai_trading_bot.data.history_archive import (
    ArchiveIntegrityError,
    PointInTimeArchive,
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
DEFAULT_WATCHLIST = ROOT / "config" / "sec_filing_watchlist.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_ARCHIVE_ROOT = "https://www.sec.gov/Archives/edgar/data"


@dataclass(frozen=True)
class WatchedIssuer:
    symbol: str
    cik: str
    issuer_name: str


@dataclass(frozen=True)
class HttpCapture:
    payload: bytes
    decoded_payload: bytes
    request_started_at: datetime
    retrieved_at: datetime
    content_type: str
    content_encoding: str | None
    response_metadata: dict[str, object]


@dataclass(frozen=True)
class PriorFilingObservation:
    cik: str
    first_seen_at: datetime
    first_observed_source_snapshot_id: str
    first_observation_class: str


def load_watchlist(path: Path) -> tuple[WatchedIssuer, ...]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict) or payload.get("schema_version") != "sec_filing_watchlist_v1":
        raise ValueError("SEC filing watchlist has an unsupported schema.")
    raw_issuers = payload.get("issuers")
    if not isinstance(raw_issuers, list) or not raw_issuers:
        raise ValueError("SEC filing watchlist must contain at least one issuer.")
    issuers: list[WatchedIssuer] = []
    symbols: set[str] = set()
    ciks: set[str] = set()
    for raw in raw_issuers:
        if not isinstance(raw, dict):
            raise ValueError("SEC filing watchlist entries must be objects.")
        symbol = str(raw.get("symbol", "")).strip().upper()
        cik = _normalize_cik(raw.get("cik"))
        issuer_name = str(raw.get("issuer_name", "")).strip()
        if not symbol or not issuer_name:
            raise ValueError("SEC filing watchlist entry is missing symbol or issuer name.")
        if symbol in symbols or cik in ciks:
            raise ValueError("SEC filing watchlist contains a duplicate symbol or CIK.")
        symbols.add(symbol)
        ciks.add(cik)
        issuers.append(WatchedIssuer(symbol=symbol, cik=cik, issuer_name=issuer_name))
    return tuple(issuers)


def fetch_sec_submissions(cik: str, user_agent: str) -> HttpCapture:
    normalized_cik = _normalize_cik(cik)
    url = SEC_SUBMISSIONS_URL.format(cik=normalized_cik)
    started = datetime.now(UTC)
    request = Request(
        url,
        headers={"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"},
    )
    with urlopen(request, timeout=45) as response:  # noqa: S310 - fixed SEC endpoint
        payload = response.read()
        retrieved = datetime.now(UTC)
        content_encoding = response.headers.get("Content-Encoding")
        decoded_payload = _decode_http_payload(payload, content_encoding)
        selected_headers = {
            key: response.headers[key]
            for key in ("Date", "ETag", "Last-Modified", "Content-Length")
            if response.headers.get(key) is not None
        }
        selected_headers["http_status"] = response.status
        selected_headers["latency_ms"] = round((retrieved - started).total_seconds() * 1000, 3)
        return HttpCapture(
            payload=payload,
            decoded_payload=decoded_payload,
            request_started_at=started,
            retrieved_at=retrieved,
            content_type=response.headers.get_content_type(),
            content_encoding=content_encoding,
            response_metadata=selected_headers,
        )


def load_prior_filing_observations(
    archive: PointInTimeArchive,
) -> dict[str, PriorFilingObservation]:
    if not archive.manifest_path.exists():
        return {}
    observations: dict[str, PriorFilingObservation] = {}
    for line in archive.manifest_path.read_text().splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if not isinstance(record, dict):
            raise ArchiveIntegrityError("Archive manifest contains a non-object record.")
        if (
            record.get("source_id") != "internal_derived"
            or record.get("dataset") != "sec_filing_events"
        ):
            continue
        raw_path = archive.root / str(record.get("raw_path", ""))
        payload = json.loads(raw_path.read_text())
        if not isinstance(payload, dict) or not isinstance(payload.get("rows"), list):
            raise ArchiveIntegrityError("Prior SEC filing event snapshot has an invalid schema.")
        for raw_row in payload["rows"]:
            if not isinstance(raw_row, dict):
                raise ArchiveIntegrityError("Prior SEC filing event row is invalid.")
            cik = _normalize_cik(raw_row.get("cik"))
            accession = _required_text(raw_row, "accession_number")
            first_seen_at = _parse_aware_datetime(raw_row.get("first_seen_at"))
            source_snapshot_id = _required_text(
                raw_row,
                "first_observed_source_snapshot_id",
            )
            first_class = _required_text(raw_row, "first_observation_class")
            key = _filing_key(cik, accession)
            candidate = PriorFilingObservation(
                cik=cik,
                first_seen_at=first_seen_at,
                first_observed_source_snapshot_id=source_snapshot_id,
                first_observation_class=first_class,
            )
            current = observations.get(key)
            if current is None or candidate.first_seen_at < current.first_seen_at:
                observations[key] = candidate
    return observations


def normalize_sec_filing_events(
    decoded_payload: bytes,
    *,
    issuer: WatchedIssuer,
    observed_at: datetime,
    source_snapshot_id: str,
    source_sha256: str,
    prior_observations: Mapping[str, PriorFilingObservation],
    cik_has_prior_history: bool,
) -> list[dict[str, object]]:
    payload = json.loads(decoded_payload)
    if not isinstance(payload, dict):
        raise ValueError("SEC submissions response must be a JSON object.")
    response_cik = _normalize_cik(payload.get("cik"))
    if response_cik != issuer.cik:
        raise ValueError(
            f"SEC submissions CIK mismatch: expected {issuer.cik}, received {response_cik}."
        )
    filings = payload.get("filings")
    if not isinstance(filings, dict):
        raise ValueError("SEC submissions response is missing filings.")
    recent = filings.get("recent")
    if not isinstance(recent, dict):
        raise ValueError("SEC submissions response is missing recent filing columns.")
    columns = _validated_columns(recent)
    accessions = columns.get("accessionNumber")
    if accessions is None:
        raise ValueError("SEC submissions response is missing accessionNumber.")

    rows: list[dict[str, object]] = []
    seen_accessions: set[str] = set()
    for index in range(len(accessions)):
        accession = _required_column_text(columns, "accessionNumber", index)
        if accession in seen_accessions:
            raise ValueError(f"SEC submissions response repeats accession {accession}.")
        seen_accessions.add(accession)
        form = _required_column_text(columns, "form", index).upper()
        acceptance_at = _parse_aware_datetime(_column_value(columns, "acceptanceDateTime", index))
        filing_date = _parse_date_text(_column_value(columns, "filingDate", index), "filingDate")
        report_date = _parse_date_text(_column_value(columns, "reportDate", index), "reportDate")
        primary_document = _optional_text(_column_value(columns, "primaryDocument", index))
        items = _parse_items(_column_value(columns, "items", index))
        key = _filing_key(issuer.cik, accession)
        previous = prior_observations.get(key)
        if previous is None:
            first_seen_at = observed_at
            first_source_snapshot_id = source_snapshot_id
            first_observation_class = (
                "monitored_new_accession"
                if cik_has_prior_history
                else "baseline_existing_accession"
            )
            current_observation_class = "first_observation"
        else:
            first_seen_at = previous.first_seen_at
            first_source_snapshot_id = previous.first_observed_source_snapshot_id
            first_observation_class = previous.first_observation_class
            current_observation_class = "reconfirmed"

        timestamp_integrity = (
            "pass"
            if acceptance_at <= first_seen_at
            else "blocked_acceptance_after_first_observation"
        )
        forward_event_eligible = (
            first_observation_class == "monitored_new_accession"
            and timestamp_integrity == "pass"
        )
        event_trigger_status = (
            "forward_observable_pending_first_tradable"
            if forward_event_eligible
            else "blocked_historical_baseline"
        )
        accession_without_dashes = accession.replace("-", "")
        filing_directory = (
            f"{SEC_ARCHIVE_ROOT}/{int(issuer.cik)}/{accession_without_dashes}"
        )
        primary_document_url = (
            None
            if primary_document is None
            else f"{filing_directory}/{quote(primary_document, safe='._-')}"
        )
        event_age_seconds = (first_seen_at - acceptance_at).total_seconds()
        tags = _event_tags(form, items)
        rows.append(
            {
                "observation_type": "sec_filing_event",
                "issuer_id": f"sec-cik:{issuer.cik}",
                "symbol": issuer.symbol,
                "watchlist_issuer_name": issuer.issuer_name,
                "sec_entity_name": _optional_text(payload.get("name")),
                "cik": issuer.cik,
                "accession_number": accession,
                "form": form,
                "event_category": _event_category(form, tags),
                "event_tags": tags,
                "items": items,
                "reporting_period_end": report_date,
                "filing_date": filing_date,
                "acceptance_at": acceptance_at.isoformat(),
                "acceptance_timestamp_quality": "sec_provider_exact",
                "publication_at": None,
                "publication_timestamp_quality": "unavailable_from_sec",
                "first_seen_at": first_seen_at.isoformat(),
                "first_seen_timestamp_quality": "collector_exact",
                "decision_available_at": first_seen_at.isoformat(),
                "decision_availability_basis": "collector_first_observed",
                "first_tradable_at": None,
                "first_tradable_status": (
                    "pending_separate_calendar_and_quote_resolution"
                ),
                "market_timezone": "America/New_York",
                "acceptance_to_first_seen_seconds": event_age_seconds,
                "timestamp_integrity_status": timestamp_integrity,
                "first_observation_class": first_observation_class,
                "current_observation_class": current_observation_class,
                "event_trigger_research_status": event_trigger_status,
                "forward_event_eligible": forward_event_eligible,
                "historical_backtest_eligible_from_acceptance": False,
                "may_not_use_before": first_seen_at.isoformat(),
                "primary_document": primary_document,
                "primary_document_description": _optional_text(
                    _column_value(columns, "primaryDocDescription", index)
                ),
                "primary_document_url": primary_document_url,
                "filing_index_url": f"{filing_directory}/{accession}-index.html",
                "complete_submission_url": f"{filing_directory}/{accession}.txt",
                "file_number": _optional_text(_column_value(columns, "fileNumber", index)),
                "film_number": _optional_text(_column_value(columns, "filmNumber", index)),
                "size_bytes": _optional_int(_column_value(columns, "size", index)),
                "is_xbrl": _optional_bool(_column_value(columns, "isXBRL", index)),
                "is_inline_xbrl": _optional_bool(
                    _column_value(columns, "isInlineXBRL", index)
                ),
                "act": _optional_text(_column_value(columns, "act", index)),
                "source_snapshot_id": source_snapshot_id,
                "source_sha256": source_sha256,
                "first_observed_source_snapshot_id": first_source_snapshot_id,
                "research_only": True,
                "active_profile_changed": False,
            }
        )
    return sorted(rows, key=lambda row: (str(row["acceptance_at"]), str(row["accession_number"])))


def collect(
    archive: PointInTimeArchive,
    *,
    user_agent: str,
    issuers: tuple[WatchedIssuer, ...],
    http_captures: Mapping[str, HttpCapture] | None = None,
    request_interval_seconds: float = 0.2,
) -> dict[str, object]:
    pre_audit = archive.audit()
    if pre_audit.status == "fail":
        raise ArchiveIntegrityError("Refusing SEC collection because the archive audit failed.")
    prior_observations = load_prior_filing_observations(archive)
    prior_ciks = {observation.cik for observation in prior_observations.values()}
    raw_records = []
    rows: list[dict[str, object]] = []
    coverage: list[dict[str, object]] = []

    for index, issuer in enumerate(issuers):
        capture = (
            http_captures[issuer.cik]
            if http_captures is not None
            else fetch_sec_submissions(issuer.cik, user_agent)
        )
        raw_record = archive.capture(
            source_id="sec_edgar",
            dataset=f"submissions_cik_{issuer.cik}",
            source_url=SEC_SUBMISSIONS_URL.format(cik=issuer.cik),
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
            request_parameters={"cik": issuer.cik, "symbol": issuer.symbol},
            response_metadata=capture.response_metadata,
            integrity_notes=(
                "SEC acceptance time is preserved but is not treated as public availability.",
                "SEC states that no timestamp identifies first availability on sec.gov.",
                "Research decisions may begin only at this collector's first observation.",
            ),
        )
        raw_records.append(raw_record)
        issuer_rows = normalize_sec_filing_events(
            capture.decoded_payload,
            issuer=issuer,
            observed_at=capture.retrieved_at,
            source_snapshot_id=raw_record.snapshot_id,
            source_sha256=raw_record.raw_sha256,
            prior_observations=prior_observations,
            cik_has_prior_history=issuer.cik in prior_ciks,
        )
        rows.extend(issuer_rows)
        coverage.append(_coverage_metadata(capture.decoded_payload, issuer, len(issuer_rows)))
        if http_captures is None and index + 1 < len(issuers):
            time.sleep(max(0.0, request_interval_seconds))

    duplicate_keys = _duplicate_filing_keys(rows)
    if duplicate_keys:
        raise ValueError(f"Duplicate filing keys across watchlist: {sorted(duplicate_keys)}")
    emitted_rows = [
        row for row in rows if row["current_observation_class"] == "first_observation"
    ]
    latest_retrieval = max(_parse_aware_datetime(record.retrieved_at) for record in raw_records)
    derived_payload = {
        "schema_version": "sec_filing_events_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "point_in_time_scope": "forward_only_from_collector_first_observation",
        "event_storage_mode": "append_only_first_observations",
        "acceptance_time_is_decision_time": False,
        "first_tradable_calculation_status": "handled_by_separate_forward_only_resolver",
        "current_response_filing_count": len(rows),
        "emitted_first_observation_count": len(emitted_rows),
        "reconfirmed_filing_count": len(rows) - len(emitted_rows),
        "watchlist": [
            {"symbol": issuer.symbol, "cik": issuer.cik, "issuer_name": issuer.issuer_name}
            for issuer in issuers
        ],
        "coverage": coverage,
        "rows": emitted_rows,
    }
    derived_started_at = datetime.now(UTC)
    derived_record = archive.capture(
        source_id="internal_derived",
        dataset="sec_filing_events",
        source_url="internal://events/sec-filings",
        payload=canonical_json_bytes(derived_payload),
        request_started_at=derived_started_at,
        retrieved_at=datetime.now(UTC),
        decision_available_at=latest_retrieval,
        decision_availability_basis="all_upstream_snapshots_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        request_parameters={"watchlist_size": len(issuers)},
        response_metadata={
            "current_response_filing_count": len(rows),
            "emitted_first_observation_count": len(emitted_rows),
            "baseline_event_count": sum(
                row["first_observation_class"] == "baseline_existing_accession"
                for row in emitted_rows
            ),
            "monitored_new_event_count": sum(
                row["first_observation_class"] == "monitored_new_accession"
                for row in emitted_rows
            ),
        },
        upstream_snapshot_ids=tuple(record.snapshot_id for record in raw_records),
        integrity_notes=(
            "Historical filing rows are baseline context and cannot be backdated into alpha.",
            "Only accessions first observed after monitoring began can become forward events.",
            "Reconfirmed accessions are not duplicated in the derived append-only event ledger.",
            "First tradable remains pending until the separate calendar/quote resolver succeeds.",
        ),
    )
    audit = archive.audit()
    new_event_count = sum(
        row["first_observation_class"] == "monitored_new_accession"
        for row in emitted_rows
    )
    baseline_count = sum(
        row["first_observation_class"] == "baseline_existing_accession"
        for row in emitted_rows
    )
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_only": True,
        "active_profile_changed": False,
        "status": "captured" if audit.status == "pass" else "integrity_audit_failed",
        "raw_snapshots": [record.to_dict() for record in raw_records],
        "derived_snapshot": derived_record.to_dict(),
        "issuer_count": len(issuers),
        "filing_event_count": len(rows),
        "emitted_event_count": len(emitted_rows),
        "reconfirmed_filing_count": len(rows) - len(emitted_rows),
        "baseline_event_count": baseline_count,
        "new_monitored_event_count": new_event_count,
        "forward_event_eligible_count": sum(
            bool(row["forward_event_eligible"]) for row in emitted_rows
        ),
        "first_tradable_status": "handled_by_separate_forward_only_resolver",
        "archive_audit": audit.to_dict(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICIES)
    parser.add_argument("--watchlist", type=Path, default=DEFAULT_WATCHLIST)
    parser.add_argument(
        "--request-interval-seconds",
        type=float,
        default=0.2,
        help="Pause between SEC requests; default remains below the SEC's stated limit.",
    )
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
        issuers=load_watchlist(args.watchlist),
        request_interval_seconds=args.request_interval_seconds,
    )
    latest = args.archive_root / "latest_sec_filing_collection.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(f"High-integrity SEC filing history captured in {args.archive_root}")
    print(
        "status",
        output["status"],
        "events",
        output["filing_event_count"],
        "new_monitored",
        output["new_monitored_event_count"],
    )


def _validated_columns(recent: dict[str, object]) -> dict[str, list[object]]:
    columns: dict[str, list[object]] = {}
    expected_length: int | None = None
    for name, values in recent.items():
        if not isinstance(values, list):
            raise ValueError(f"SEC recent filing column {name} is not an array.")
        if expected_length is None:
            expected_length = len(values)
        elif len(values) != expected_length:
            raise ValueError("SEC recent filing columns have inconsistent lengths.")
        columns[str(name)] = values
    return columns


def _column_value(columns: Mapping[str, list[object]], name: str, index: int) -> object:
    values = columns.get(name)
    return None if values is None else values[index]


def _required_column_text(
    columns: Mapping[str, list[object]],
    name: str,
    index: int,
) -> str:
    value = _optional_text(_column_value(columns, name, index))
    if value is None:
        raise ValueError(f"SEC recent filing row is missing {name}.")
    return value


def _required_text(row: Mapping[str, object], key: str) -> str:
    value = _optional_text(row.get(key))
    if value is None:
        raise ArchiveIntegrityError(f"Prior filing event row is missing {key}.")
    return value


def _parse_aware_datetime(value: object) -> datetime:
    text = _optional_text(value)
    if text is None:
        raise ValueError("SEC filing event is missing a required timestamp.")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp: {text}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"Timestamp is not timezone-aware: {text}")
    return parsed


def _parse_date_text(value: object, label: str) -> str | None:
    text = _optional_text(value)
    if text is None:
        return None
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(f"Invalid {label}: {text}") from exc


def _normalize_cik(value: object) -> str:
    if isinstance(value, bool):
        raise ValueError("CIK must be numeric.")
    text = str(value).strip()
    if not text.isdigit() or len(text) > 10:
        raise ValueError(f"Invalid CIK: {text}")
    return text.zfill(10)


def _decode_http_payload(payload: bytes, content_encoding: str | None) -> bytes:
    encoding = "" if content_encoding is None else content_encoding.lower().strip()
    if encoding == "gzip":
        return gzip.decompress(payload)
    if encoding == "deflate":
        return zlib.decompress(payload)
    return payload


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("SEC integer field has an invalid boolean value.")
    try:
        return int(str(value))
    except ValueError as exc:
        raise ValueError(f"SEC integer field is invalid: {value}") from exc


def _optional_bool(value: object) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if value in (0, 1, "0", "1"):
        return bool(int(str(value)))
    raise ValueError(f"SEC boolean field is invalid: {value}")


def _parse_items(value: object) -> list[str]:
    text = _optional_text(value)
    if text is None:
        return []
    return sorted({item for item in re.split(r"[\s,]+", text) if item})


def _event_tags(form: str, items: list[str]) -> list[str]:
    tags: set[str] = set()
    if form in {"10-K", "10-K/A", "10-Q", "10-Q/A", "20-F", "20-F/A"}:
        tags.add("periodic_financial_report")
    if form in {"3", "3/A", "4", "4/A", "5", "5/A"}:
        tags.add("insider_ownership")
    if form.startswith("SC 13D") or form.startswith("SC 13G"):
        tags.add("beneficial_ownership")
    if form.startswith(("S-", "F-")) or form.startswith("424B"):
        tags.add("capital_markets")
    if form in {"DEF 14A", "DEFA14A", "PRE 14A"}:
        tags.add("governance")
    if form.startswith("8-K"):
        tags.add("current_report")
        item_tags = {
            "2.02": "earnings_results",
            "3.01": "listing_status",
            "3.02": "equity_issuance",
            "4.02": "accounting_revision",
            "5.02": "management_change",
            "7.01": "regulation_fd",
            "8.01": "other_material_event",
        }
        for item in items:
            tag = item_tags.get(item)
            if tag is not None:
                tags.add(tag)
    if not tags:
        tags.add("other_filing")
    return sorted(tags)


def _event_category(form: str, tags: list[str]) -> str:
    priority = (
        "earnings_results",
        "periodic_financial_report",
        "accounting_revision",
        "equity_issuance",
        "listing_status",
        "management_change",
        "insider_ownership",
        "beneficial_ownership",
        "capital_markets",
        "governance",
        "regulation_fd",
        "current_report",
        "other_filing",
    )
    for category in priority:
        if category in tags:
            return category
    return f"other_{form.lower()}"


def _filing_key(cik: str, accession: str) -> str:
    return f"{cik}:{accession}"


def _duplicate_filing_keys(rows: list[dict[str, object]]) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for row in rows:
        key = _filing_key(str(row["cik"]), str(row["accession_number"]))
        if key in seen:
            duplicates.add(key)
        seen.add(key)
    return duplicates


def _coverage_metadata(
    decoded_payload: bytes,
    issuer: WatchedIssuer,
    recent_count: int,
) -> dict[str, object]:
    payload = json.loads(decoded_payload)
    filings = payload.get("filings", {}) if isinstance(payload, dict) else {}
    older_files = filings.get("files", []) if isinstance(filings, dict) else []
    if not isinstance(older_files, list):
        raise ValueError("SEC submissions older-files metadata is invalid.")
    return {
        "symbol": issuer.symbol,
        "cik": issuer.cik,
        "recent_filing_count": recent_count,
        "older_submission_file_count": len(older_files),
        "older_submission_files_fetched": False,
        "coverage_limit": "SEC recent submissions response only",
    }


if __name__ == "__main__":
    main()
