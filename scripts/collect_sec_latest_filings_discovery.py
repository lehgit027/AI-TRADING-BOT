"""Collect SEC Latest Filings and promote only forward, confirmed liquid-universe events."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import zlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from ai_trading_bot.data.history_archive import (
    ArchiveIntegrityError,
    PointInTimeArchive,
    SnapshotRecord,
    canonical_json_bytes,
    load_source_policies,
)

try:
    from scripts.collect_sec_filing_history import (
        HttpCapture as SubmissionsHttpCapture,
    )
    from scripts.collect_sec_filing_history import (
        PriorFilingObservation,
        WatchedIssuer,
        fetch_sec_submissions,
        load_prior_filing_observations,
        normalize_sec_filing_events,
    )
    from scripts.trusted_data_research import trusted_data_environment
except ModuleNotFoundError:
    from collect_sec_filing_history import (  # type: ignore[import-not-found,no-redef]
        HttpCapture as SubmissionsHttpCapture,
    )
    from collect_sec_filing_history import (  # type: ignore[no-redef]
        PriorFilingObservation,
        WatchedIssuer,
        fetch_sec_submissions,
        load_prior_filing_observations,
        normalize_sec_filing_events,
    )
    from trusted_data_research import (  # type: ignore[import-not-found,no-redef]
        trusted_data_environment,
    )

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARCHIVE = ROOT / "outputs" / "high_integrity_history"
DEFAULT_POLICIES = ROOT / "config" / "data_source_policies.json"
DEFAULT_DISCOVERY_POLICY = ROOT / "config" / "sec_latest_filings_discovery_policy.json"
SEC_LATEST_FILINGS_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
ATOM_NAMESPACE = "http://www.w3.org/2005/Atom"
_ACCESSION_ID = re.compile(r"accession-number=(\d{10}-\d{2}-\d{6})$")
_TITLE_CIK = re.compile(r"\((\d{1,10})\)(?:\s+\([^)]*\))*$")
_SUMMARY_FILED = re.compile(r"Filed:</b>\s*(\d{4}-\d{2}-\d{2})", re.IGNORECASE)


@dataclass(frozen=True)
class LatestFilingsDiscoveryPolicy:
    feed_count: int
    ownership_filter: str
    recommended_interval_seconds: int
    continuity_warning_seconds: int
    allowed_forms: tuple[str, ...]
    curated_liquid_symbols: tuple[str, ...]


@dataclass(frozen=True)
class FeedHttpCapture:
    payload: bytes
    decoded_payload: bytes
    request_started_at: datetime
    retrieved_at: datetime
    content_type: str
    content_encoding: str | None
    response_metadata: dict[str, object]
    source_url: str
    request_parameters: dict[str, object]


@dataclass(frozen=True)
class LatestFilingEntry:
    accession_number: str
    cik: str
    form: str
    entity_name: str
    filing_role: str
    title: str
    filing_date: date
    feed_updated_at: datetime
    filing_index_url: str


@dataclass(frozen=True)
class PriorDiscoveryObservation:
    first_seen_at: datetime
    first_observed_source_snapshot_id: str
    first_observation_class: str


@dataclass(frozen=True)
class DiscoveryUniverse:
    by_cik: dict[str, WatchedIssuer]
    security_master_snapshot_id: str | None
    alpaca_assets_snapshot_id: str | None
    configured_symbol_count: int
    eligible_symbol_count: int
    missing_sec_mapping_symbols: tuple[str, ...]
    inactive_or_untradable_symbols: tuple[str, ...]


def load_discovery_policy(path: Path) -> LatestFilingsDiscoveryPolicy:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict) or payload.get("schema_version") != (
        "sec_latest_filings_discovery_policy_v1"
    ):
        raise ValueError("SEC Latest Filings discovery policy has an unsupported schema.")
    if payload.get("research_only") is not True:
        raise ValueError("SEC Latest Filings discovery must remain research-only.")
    for key in ("execution_price_selected", "paper_fill_applied", "candidate_created"):
        if payload.get(key) is not False:
            raise ValueError(f"SEC discovery policy enables prohibited action: {key}")
    raw_forms = payload.get("allowed_forms")
    raw_symbols = payload.get("curated_liquid_symbols")
    if not isinstance(raw_forms, list) or not raw_forms:
        raise ValueError("SEC discovery policy must contain allowed forms.")
    if not isinstance(raw_symbols, list) or not raw_symbols:
        raise ValueError("SEC discovery policy must contain a curated symbol universe.")
    forms = tuple(
        sorted({_required_list_text(item, "allowed_forms").upper() for item in raw_forms})
    )
    symbols = tuple(
        sorted(
            {_required_list_text(item, "curated_liquid_symbols").upper() for item in raw_symbols}
        )
    )
    ownership_filter = _required_text(payload, "ownership_filter").lower()
    if ownership_filter not in {"include", "exclude", "only"}:
        raise ValueError("SEC discovery ownership filter is invalid.")
    return LatestFilingsDiscoveryPolicy(
        feed_count=_positive_int(payload.get("feed_count"), "feed_count"),
        ownership_filter=ownership_filter,
        recommended_interval_seconds=_positive_int(
            payload.get("recommended_collection_interval_seconds"),
            "recommended_collection_interval_seconds",
        ),
        continuity_warning_seconds=_positive_int(
            payload.get("continuity_warning_seconds"), "continuity_warning_seconds"
        ),
        allowed_forms=forms,
        curated_liquid_symbols=symbols,
    )


def fetch_latest_filings(
    user_agent: str,
    *,
    policy: LatestFilingsDiscoveryPolicy,
) -> FeedHttpCapture:
    parameters: dict[str, object] = {
        "action": "getcurrent",
        "output": "atom",
        "count": policy.feed_count,
        "owner": policy.ownership_filter,
    }
    source_url = f"{SEC_LATEST_FILINGS_URL}?{urlencode(parameters)}"
    started = datetime.now(UTC)
    request = Request(
        source_url,
        headers={
            "User-Agent": user_agent,
            "Accept": "application/atom+xml",
            "Accept-Encoding": "gzip, deflate",
        },
    )
    with urlopen(request, timeout=45) as response:  # noqa: S310 - fixed SEC endpoint
        payload = response.read()
        retrieved = datetime.now(UTC)
        encoding = response.headers.get("Content-Encoding")
        decoded = _decode_http_payload(payload, encoding)
        metadata = {
            key: response.headers[key]
            for key in ("Date", "ETag", "Last-Modified", "Content-Length")
            if response.headers.get(key) is not None
        }
        metadata["http_status"] = response.status
        metadata["latency_ms"] = round((retrieved - started).total_seconds() * 1000, 3)
        return FeedHttpCapture(
            payload=payload,
            decoded_payload=decoded,
            request_started_at=started,
            retrieved_at=retrieved,
            content_type=response.headers.get_content_type(),
            content_encoding=encoding,
            response_metadata=metadata,
            source_url=source_url,
            request_parameters=parameters,
        )


def parse_latest_filings_feed(decoded_payload: bytes) -> tuple[LatestFilingEntry, ...]:
    try:
        root = ElementTree.fromstring(decoded_payload)
    except ElementTree.ParseError as exc:
        raise ValueError("SEC Latest Filings response is invalid Atom XML.") from exc
    if root.tag != f"{{{ATOM_NAMESPACE}}}feed":
        raise ValueError("SEC Latest Filings response is not an Atom feed.")
    entries: list[LatestFilingEntry] = []
    seen: set[tuple[str, str, str]] = set()
    for raw_entry in root.findall(f"{{{ATOM_NAMESPACE}}}entry"):
        title = _element_text(raw_entry, "title")
        entry_id = _element_text(raw_entry, "id")
        accession_match = _ACCESSION_ID.search(entry_id)
        if accession_match is None:
            raise ValueError(f"SEC Atom entry has invalid accession ID: {entry_id}")
        accession = accession_match.group(1)
        category = raw_entry.find(f"{{{ATOM_NAMESPACE}}}category")
        if category is None:
            raise ValueError(f"SEC Atom entry {accession} lacks a form category.")
        raw_form = str(category.attrib.get("term", "")).strip().upper()
        if not raw_form:
            raise ValueError(f"SEC Atom entry {accession} has an empty form.")
        form = _normalize_sec_atom_form(raw_form)
        cik_match = _TITLE_CIK.search(title)
        if cik_match is None:
            raise ValueError(f"SEC Atom entry title lacks a terminal CIK: {title}")
        cik = cik_match.group(1).zfill(10)
        prefix = f"{raw_form} - "
        entity_part = title[len(prefix) :] if title.startswith(prefix) else title
        entity_name = entity_part[
            : cik_match.start() - (len(prefix) if title.startswith(prefix) else 0)
        ]
        entity_name = entity_name.strip().rstrip("-").strip()
        if not entity_name:
            raise ValueError(f"SEC Atom entry {accession} lacks an entity name.")
        role_match = re.search(r"\(([^()]*)\)\s*$", title)
        filing_role = "unknown" if role_match is None else role_match.group(1).strip().lower()
        summary = _element_text(raw_entry, "summary")
        filing_match = _SUMMARY_FILED.search(summary)
        if filing_match is None:
            raise ValueError(f"SEC Atom entry {accession} lacks a filing date.")
        link = raw_entry.find(f"{{{ATOM_NAMESPACE}}}link")
        if link is None:
            raise ValueError(f"SEC Atom entry {accession} lacks an index link.")
        index_url = str(link.attrib.get("href", "")).strip()
        if not index_url.startswith("https://www.sec.gov/Archives/edgar/data/"):
            raise ValueError(f"SEC Atom entry {accession} has an unsafe index URL.")
        identity = (accession, cik, title)
        if identity in seen:
            continue
        seen.add(identity)
        entries.append(
            LatestFilingEntry(
                accession_number=accession,
                cik=cik,
                form=form,
                entity_name=entity_name,
                filing_role=filing_role,
                title=title,
                filing_date=date.fromisoformat(filing_match.group(1)),
                feed_updated_at=_parse_aware_datetime(_element_text(raw_entry, "updated")),
                filing_index_url=index_url,
            )
        )
    if not entries:
        raise ValueError("SEC Latest Filings feed contains no entries.")
    return tuple(sorted(entries, key=lambda entry: (entry.feed_updated_at, entry.accession_number)))


def load_prior_discoveries(
    archive: PointInTimeArchive,
) -> tuple[dict[str, PriorDiscoveryObservation], datetime | None]:
    if not archive.manifest_path.exists():
        return {}, None
    observations: dict[str, PriorDiscoveryObservation] = {}
    latest_retrieval: datetime | None = None
    for record in _manifest_records(archive):
        if record.get("source_id") != "internal_derived" or record.get("dataset") != (
            "sec_latest_filing_discoveries"
        ):
            continue
        payload = _derived_payload(archive, record, "sec_latest_filing_discoveries")
        retrieval = _parse_aware_datetime(payload.get("feed_retrieved_at"))
        latest_retrieval = (
            retrieval if latest_retrieval is None else max(latest_retrieval, retrieval)
        )
        for raw in cast(list[object], payload["rows"]):
            if not isinstance(raw, dict):
                raise ArchiveIntegrityError("SEC discovery ledger contains an invalid row.")
            accession = _required_text(raw, "accession_number")
            candidate = PriorDiscoveryObservation(
                first_seen_at=_parse_aware_datetime(raw.get("first_seen_at")),
                first_observed_source_snapshot_id=_required_text(
                    raw, "first_observed_source_snapshot_id"
                ),
                first_observation_class=_required_text(raw, "first_observation_class"),
            )
            current = observations.get(accession)
            if current is None or candidate.first_seen_at < current.first_seen_at:
                observations[accession] = candidate
    return observations, latest_retrieval


def load_discovery_universe(
    archive: PointInTimeArchive,
    *,
    policy: LatestFilingsDiscoveryPolicy,
) -> DiscoveryUniverse:
    sec_snapshot_id, sec_rows = _latest_rows(archive, "security_master_observations")
    asset_snapshot_id, asset_rows = _latest_rows(archive, "alpaca_asset_observations")
    configured = set(policy.curated_liquid_symbols)
    sec_by_symbol: dict[str, tuple[str, str]] = {}
    for row in sec_rows:
        symbol = str(row.get("symbol", "")).upper()
        if symbol in configured:
            sec_by_symbol[symbol] = (
                str(row.get("cik", "")).zfill(10),
                str(row.get("issuer_name", "")).strip(),
            )
    active_tradable = {
        str(row.get("symbol", "")).upper()
        for row in asset_rows
        if row.get("status") == "active" and row.get("tradable") is True
    }
    missing_sec = tuple(sorted(configured - set(sec_by_symbol)))
    inactive = tuple(sorted(set(sec_by_symbol) - active_tradable))
    by_cik: dict[str, WatchedIssuer] = {}
    for symbol in sorted(set(sec_by_symbol) & active_tradable):
        cik, issuer_name = sec_by_symbol[symbol]
        if not cik.isdigit() or not issuer_name:
            continue
        issuer = WatchedIssuer(symbol=symbol, cik=cik, issuer_name=issuer_name)
        current = by_cik.get(cik)
        if current is not None and current.symbol != symbol:
            raise ArchiveIntegrityError(f"Curated SEC universe maps CIK {cik} to two symbols.")
        by_cik[cik] = issuer
    return DiscoveryUniverse(
        by_cik=by_cik,
        security_master_snapshot_id=sec_snapshot_id,
        alpaca_assets_snapshot_id=asset_snapshot_id,
        configured_symbol_count=len(configured),
        eligible_symbol_count=len(by_cik),
        missing_sec_mapping_symbols=missing_sec,
        inactive_or_untradable_symbols=inactive,
    )


def collect(
    archive: PointInTimeArchive,
    *,
    user_agent: str,
    policy: LatestFilingsDiscoveryPolicy,
    feed_capture: FeedHttpCapture | None = None,
    submission_captures: Mapping[str, SubmissionsHttpCapture] | None = None,
) -> dict[str, object]:
    pre_audit = archive.audit()
    if pre_audit.status == "fail":
        raise ArchiveIntegrityError("Refusing SEC discovery because archive audit failed.")
    capture = feed_capture or fetch_latest_filings(user_agent, policy=policy)
    raw_entries = parse_latest_filings_feed(capture.decoded_payload)
    raw_feed_record = archive.capture(
        source_id="sec_edgar",
        dataset="latest_filings_atom_company",
        source_url=capture.source_url,
        payload=capture.payload,
        request_started_at=capture.request_started_at,
        retrieved_at=capture.retrieved_at,
        decision_available_at=capture.retrieved_at,
        decision_availability_basis="first_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type=capture.content_type,
        content_encoding=capture.content_encoding,
        request_parameters=capture.request_parameters,
        response_metadata=capture.response_metadata,
        integrity_notes=(
            (
                "SEC feed updated time is preserved but never substituted for collector "
                "first-seen time."
            ),
            "The first captured feed page is a historical baseline and cannot trigger research.",
            "Ownership filings are excluded because their filing CIK may identify the reporter.",
        ),
    )
    prior_discoveries, prior_feed_retrieval = load_prior_discoveries(archive)
    prior_filings = load_prior_filing_observations(archive)
    universe = load_discovery_universe(archive, policy=policy)
    entries, ambiguous_accessions = _select_accession_entries(raw_entries, universe)
    initial_baseline = not prior_discoveries
    continuity_status = _continuity_status(
        capture.retrieved_at,
        prior_feed_retrieval,
        raw_entries,
        policy,
    )
    discovery_rows: list[dict[str, object]] = []
    eligible_entries: list[LatestFilingEntry] = []
    reconfirmed = 0
    for entry in entries:
        prior = prior_discoveries.get(entry.accession_number)
        if prior is not None:
            reconfirmed += 1
            continue
        first_class = (
            "baseline_existing_feed_entry" if initial_baseline else "monitored_new_feed_entry"
        )
        first_seen = capture.retrieved_at
        eligibility, reason = _entry_eligibility(
            entry,
            first_class=first_class,
            policy=policy,
            universe=universe,
            prior_filings=prior_filings,
        )
        if eligibility == "pending_submissions_confirmation":
            eligible_entries.append(entry)
        discovery_rows.append(
            {
                "observation_type": "sec_latest_filing_discovery",
                "accession_number": entry.accession_number,
                "cik": entry.cik,
                "form": entry.form,
                "entity_name": entry.entity_name,
                "filing_role": entry.filing_role,
                "feed_title": entry.title,
                "filing_date": entry.filing_date.isoformat(),
                "feed_updated_at": entry.feed_updated_at.isoformat(),
                "feed_updated_timestamp_quality": "sec_provider_timestamp_not_publication_time",
                "publication_at": None,
                "publication_timestamp_quality": "unavailable_from_sec",
                "filing_index_url": entry.filing_index_url,
                "first_seen_at": first_seen.isoformat(),
                "decision_available_at": first_seen.isoformat(),
                "decision_availability_basis": "collector_first_observed_in_latest_filings_feed",
                "first_observation_class": first_class,
                "current_observation_class": "first_observation",
                "discovery_eligibility_status": eligibility,
                "exact_eligibility_reason": reason,
                "feed_continuity_status": continuity_status,
                "first_observed_source_snapshot_id": raw_feed_record.snapshot_id,
                "source_snapshot_id": raw_feed_record.snapshot_id,
                "source_sha256": raw_feed_record.raw_sha256,
                "research_only": True,
                "active_profile_changed": False,
            }
        )
        if entry.accession_number in ambiguous_accessions:
            discovery_rows[-1]["discovery_eligibility_status"] = (
                "blocked_multiple_integrity_cleared_filing_parties"
            )
            discovery_rows[-1]["exact_eligibility_reason"] = (
                "Multiple filing parties map to the curated universe; no issuer was inferred."
            )
            if entry in eligible_entries:
                eligible_entries.remove(entry)
    submission_records: dict[str, SnapshotRecord] = {}
    event_rows: list[dict[str, object]] = []
    row_by_accession = {str(row["accession_number"]): row for row in discovery_rows}
    for cik in sorted({entry.cik for entry in eligible_entries}):
        issuer = universe.by_cik[cik]
        submission_capture = (
            submission_captures[cik]
            if submission_captures is not None
            else fetch_sec_submissions(cik, user_agent)
        )
        submission_record = archive.capture(
            source_id="sec_edgar",
            dataset=f"submissions_cik_{cik}",
            source_url=f"https://data.sec.gov/submissions/CIK{cik}.json",
            payload=submission_capture.payload,
            request_started_at=submission_capture.request_started_at,
            retrieved_at=submission_capture.retrieved_at,
            decision_available_at=submission_capture.retrieved_at,
            decision_availability_basis="first_observed",
            market_timezone="UTC",
            timestamp_quality="first_seen_exact",
            research_use="forward_only",
            content_type=submission_capture.content_type,
            content_encoding=submission_capture.content_encoding,
            request_parameters={"cik": cik, "symbol": issuer.symbol, "trigger": "latest_filings"},
            response_metadata=submission_capture.response_metadata,
            integrity_notes=(
                "This submissions response was requested only for a new eligible feed accession.",
                "The exact accession must be present before the discovery becomes a filing event.",
                (
                    "Decision availability remains the earlier feed first-seen time, never "
                    "acceptance time."
                ),
            ),
        )
        submission_records[cik] = submission_record
        normalized = normalize_sec_filing_events(
            submission_capture.decoded_payload,
            issuer=issuer,
            observed_at=capture.retrieved_at,
            source_snapshot_id=submission_record.snapshot_id,
            source_sha256=submission_record.raw_sha256,
            prior_observations=prior_filings,
            cik_has_prior_history=True,
        )
        normalized_by_accession = {str(row["accession_number"]): row for row in normalized}
        for entry in (item for item in eligible_entries if item.cik == cik):
            discovery = row_by_accession[entry.accession_number]
            event = normalized_by_accession.get(entry.accession_number)
            if event is None:
                discovery["discovery_eligibility_status"] = (
                    "blocked_accession_missing_from_submissions"
                )
                discovery["exact_eligibility_reason"] = (
                    "The exact feed accession was not present in the independently fetched "
                    "SEC submissions response."
                )
                continue
            acceptance = _parse_aware_datetime(event.get("acceptance_at"))
            if acceptance > capture.retrieved_at:
                discovery["discovery_eligibility_status"] = "blocked_acceptance_after_first_seen"
                discovery["exact_eligibility_reason"] = (
                    "SEC acceptance time is after collector first-seen time."
                )
                continue
            event.update(
                {
                    "first_seen_at": capture.retrieved_at.isoformat(),
                    "decision_available_at": capture.retrieved_at.isoformat(),
                    "decision_availability_basis": (
                        "collector_first_observed_in_latest_filings_feed"
                    ),
                    "first_observation_class": "monitored_new_accession",
                    "current_observation_class": "first_observation",
                    "event_trigger_research_status": ("forward_observable_pending_first_tradable"),
                    "forward_event_eligible": True,
                    "may_not_use_before": capture.retrieved_at.isoformat(),
                    "first_observed_source_snapshot_id": raw_feed_record.snapshot_id,
                    "sec_latest_feed_source_snapshot_id": raw_feed_record.snapshot_id,
                    "sec_submissions_confirmation_snapshot_id": submission_record.snapshot_id,
                    "feed_updated_at": entry.feed_updated_at.isoformat(),
                    "feed_continuity_status": continuity_status,
                }
            )
            discovery["discovery_eligibility_status"] = "pass_confirmed_forward_event"
            discovery["exact_eligibility_reason"] = None
            discovery["sec_submissions_confirmation_snapshot_id"] = submission_record.snapshot_id
            event_rows.append(event)
    discovery_upstream = [raw_feed_record.snapshot_id]
    discovery_upstream.extend(record.snapshot_id for record in submission_records.values())
    if universe.security_master_snapshot_id is not None:
        discovery_upstream.append(universe.security_master_snapshot_id)
    if universe.alpaca_assets_snapshot_id is not None:
        discovery_upstream.append(universe.alpaca_assets_snapshot_id)
    latest_retrieval = max(
        [capture.retrieved_at]
        + [_parse_aware_datetime(record.retrieved_at) for record in submission_records.values()]
    )
    discovery_payload = {
        "schema_version": "sec_latest_filing_discoveries_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "feed_retrieved_at": capture.retrieved_at.isoformat(),
        "point_in_time_scope": "forward_only_from_feed_first_observation",
        "initial_feed_baseline": initial_baseline,
        "feed_continuity_status": continuity_status,
        "feed_entry_count": len(raw_entries),
        "unique_accession_count": len(entries),
        "emitted_first_observation_count": len(discovery_rows),
        "reconfirmed_entry_count": reconfirmed,
        "ownership_filter": policy.ownership_filter,
        "allowed_forms": list(policy.allowed_forms),
        "universe": {
            "configured_symbol_count": universe.configured_symbol_count,
            "eligible_symbol_count": universe.eligible_symbol_count,
            "missing_sec_mapping_symbols": list(universe.missing_sec_mapping_symbols),
            "inactive_or_untradable_symbols": list(universe.inactive_or_untradable_symbols),
            "security_master_snapshot_id": universe.security_master_snapshot_id,
            "alpaca_assets_snapshot_id": universe.alpaca_assets_snapshot_id,
        },
        "rows": discovery_rows,
    }
    discovery_started = datetime.now(UTC)
    discovery_record = archive.capture(
        source_id="internal_derived",
        dataset="sec_latest_filing_discoveries",
        source_url="internal://events/sec-latest-filings-discovery",
        payload=canonical_json_bytes(discovery_payload),
        request_started_at=discovery_started,
        retrieved_at=max(datetime.now(UTC), latest_retrieval + timedelta(microseconds=1)),
        decision_available_at=latest_retrieval,
        decision_availability_basis="feed_and_required_confirmation_snapshots_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        request_parameters={
            "feed_count": policy.feed_count,
            "ownership_filter": policy.ownership_filter,
        },
        response_metadata={
            "feed_entry_count": len(raw_entries),
            "unique_accession_count": len(entries),
            "new_entry_count": len(discovery_rows),
            "confirmed_event_count": len(event_rows),
            "blocked_or_rejected_count": sum(
                row["discovery_eligibility_status"] != "pass_confirmed_forward_event"
                for row in discovery_rows
            ),
        },
        upstream_snapshot_ids=tuple(sorted(set(discovery_upstream))),
        integrity_notes=(
            "Baseline feed entries cannot trigger research or retrospective alpha.",
            "New entries require form, identity, active/tradable asset, and accession checks.",
            (
                "A continuity gap delays decision availability to current first-seen time; "
                "it is never backfilled."
            ),
        ),
    )
    event_record: SnapshotRecord | None = None
    if event_rows:
        event_payload = {
            "schema_version": "sec_filing_events_v1",
            "generated_at": datetime.now(UTC).isoformat(),
            "point_in_time_scope": "forward_only_from_latest_filings_feed_first_observation",
            "event_storage_mode": "append_only_first_observations",
            "acceptance_time_is_decision_time": False,
            "first_tradable_calculation_status": "handled_by_separate_forward_only_resolver",
            "current_response_filing_count": len(event_rows),
            "emitted_first_observation_count": len(event_rows),
            "reconfirmed_filing_count": 0,
            "discovery_snapshot_id": discovery_record.snapshot_id,
            "watchlist": [
                {"symbol": issuer.symbol, "cik": issuer.cik, "issuer_name": issuer.issuer_name}
                for issuer in sorted(
                    {universe.by_cik[str(row["cik"])] for row in event_rows},
                    key=lambda item: item.symbol,
                )
            ],
            "coverage": [],
            "rows": event_rows,
        }
        event_started = datetime.now(UTC)
        event_record = archive.capture(
            source_id="internal_derived",
            dataset="sec_filing_events",
            source_url="internal://events/sec-latest-filings-confirmed-events",
            payload=canonical_json_bytes(event_payload),
            request_started_at=event_started,
            retrieved_at=max(datetime.now(UTC), latest_retrieval + timedelta(microseconds=2)),
            decision_available_at=capture.retrieved_at,
            decision_availability_basis="latest_filings_feed_first_observed_and_confirmed",
            market_timezone="America/New_York",
            timestamp_quality="first_seen_exact",
            research_use="forward_only",
            content_type="application/json",
            request_parameters={"confirmed_event_count": len(event_rows)},
            response_metadata={
                "monitored_new_event_count": len(event_rows),
                "forward_event_eligible_count": len(event_rows),
            },
            upstream_snapshot_ids=tuple(
                sorted(
                    {
                        discovery_record.snapshot_id,
                        raw_feed_record.snapshot_id,
                        *(record.snapshot_id for record in submission_records.values()),
                    }
                )
            ),
            integrity_notes=(
                (
                    "Only feed entries first observed after baseline and confirmed in submissions "
                    "appear here."
                ),
                (
                    "Decision availability is feed first-seen time; SEC acceptance is never "
                    "backdated into it."
                ),
                (
                    "First tradable, documents, observations, and any research remain separate "
                    "gated steps."
                ),
            ),
        )
    audit = archive.audit()
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_only": True,
        "trading_actions_enabled": False,
        "active_profile_changed": False,
        "status": "captured" if audit.status == "pass" else "integrity_audit_failed",
        "feed_entry_count": len(raw_entries),
        "unique_accession_count": len(entries),
        "discovery_emitted_count": len(discovery_rows),
        "reconfirmed_discovery_count": reconfirmed,
        "baseline_discovery_count": sum(
            row["first_observation_class"] == "baseline_existing_feed_entry"
            for row in discovery_rows
        ),
        "new_feed_entry_count": sum(
            row["first_observation_class"] == "monitored_new_feed_entry" for row in discovery_rows
        ),
        "confirmed_forward_event_count": len(event_rows),
        "blocked_or_filtered_discovery_count": sum(
            row["discovery_eligibility_status"] != "pass_confirmed_forward_event"
            for row in discovery_rows
        ),
        "eligible_universe_symbol_count": universe.eligible_symbol_count,
        "feed_continuity_status": continuity_status,
        "raw_feed_snapshot": raw_feed_record.to_dict(),
        "raw_submission_snapshots": [record.to_dict() for record in submission_records.values()],
        "discovery_snapshot": discovery_record.to_dict(),
        "filing_event_snapshot": None if event_record is None else event_record.to_dict(),
        "archive_audit": audit.to_dict(),
        "execution_price_selected": False,
        "paper_fill_applied": False,
        "candidate_created": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICIES)
    parser.add_argument("--discovery-policy", type=Path, default=DEFAULT_DISCOVERY_POLICY)
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
        policy=load_discovery_policy(args.discovery_policy),
    )
    latest = args.archive_root / "latest_sec_latest_filings_discovery.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(
        "SEC Latest Filings discovery",
        output["status"],
        "new",
        output["new_feed_entry_count"],
        "confirmed",
        output["confirmed_forward_event_count"],
    )


def _select_accession_entries(
    entries: tuple[LatestFilingEntry, ...],
    universe: DiscoveryUniverse,
) -> tuple[tuple[LatestFilingEntry, ...], frozenset[str]]:
    grouped: dict[str, list[LatestFilingEntry]] = {}
    for entry in entries:
        grouped.setdefault(entry.accession_number, []).append(entry)
    selected: list[LatestFilingEntry] = []
    ambiguous: set[str] = set()
    for accession, candidates in grouped.items():
        mapped_by_cik: dict[str, LatestFilingEntry] = {}
        for candidate in sorted(candidates, key=_entry_party_priority):
            if candidate.cik in universe.by_cik:
                mapped_by_cik.setdefault(candidate.cik, candidate)
        if len(mapped_by_cik) == 1:
            selected.append(next(iter(mapped_by_cik.values())))
            continue
        if len(mapped_by_cik) > 1:
            ambiguous.add(accession)
            selected.append(sorted(mapped_by_cik.values(), key=_entry_party_priority)[0])
            continue
        selected.append(sorted(candidates, key=_entry_party_priority)[0])
    return (
        tuple(
            sorted(
                selected,
                key=lambda entry: (entry.feed_updated_at, entry.accession_number),
            )
        ),
        frozenset(ambiguous),
    )


def _entry_party_priority(entry: LatestFilingEntry) -> tuple[int, str, str]:
    role_priority = {
        "subject": 0,
        "filer": 1,
        "issuer": 2,
        "filed by": 3,
    }
    return (
        role_priority.get(entry.filing_role, 9),
        entry.cik,
        entry.entity_name,
    )


def _normalize_sec_atom_form(form: str) -> str:
    """Align SEC Atom labels with the canonical forms used by submissions JSON."""
    if form.startswith("SCHEDULE 13"):
        return f"SC 13{form.removeprefix('SCHEDULE 13')}"
    return form


def _entry_eligibility(
    entry: LatestFilingEntry,
    *,
    first_class: str,
    policy: LatestFilingsDiscoveryPolicy,
    universe: DiscoveryUniverse,
    prior_filings: Mapping[str, PriorFilingObservation],
) -> tuple[str, str | None]:
    if first_class != "monitored_new_feed_entry":
        return "blocked_historical_feed_baseline", "Entry was present in the initial feed baseline."
    if f"{entry.cik}:{entry.accession_number}" in prior_filings:
        return (
            "already_observed_by_existing_filing_monitor",
            "The accession already exists in the forward filing ledger.",
        )
    if entry.form not in policy.allowed_forms:
        return "filtered_form_not_allowed", f"Form {entry.form} is outside the discovery policy."
    if entry.cik not in universe.by_cik:
        return (
            "filtered_outside_integrity_cleared_universe",
            "Filing CIK is not mapped to an active, tradable curated symbol.",
        )
    return "pending_submissions_confirmation", None


def _continuity_status(
    retrieved_at: datetime,
    prior_retrieval: datetime | None,
    entries: tuple[LatestFilingEntry, ...],
    policy: LatestFilingsDiscoveryPolicy,
) -> str:
    if prior_retrieval is None:
        return "baseline_initialized"
    gap = (retrieved_at - prior_retrieval).total_seconds()
    oldest_updated = min(entry.feed_updated_at for entry in entries)
    if oldest_updated > prior_retrieval:
        return "warning_feed_window_does_not_reach_prior_retrieval"
    if gap > policy.continuity_warning_seconds:
        return "warning_collection_gap_but_feed_window_overlaps"
    return "pass_continuous_feed_window"


def _latest_rows(
    archive: PointInTimeArchive,
    dataset: str,
) -> tuple[str | None, list[dict[str, object]]]:
    for record in reversed(_manifest_records(archive)):
        if record.get("source_id") == "internal_derived" and record.get("dataset") == dataset:
            payload = _derived_payload(archive, record, dataset)
            rows: list[dict[str, object]] = []
            for raw in cast(list[object], payload["rows"]):
                if not isinstance(raw, dict):
                    raise ArchiveIntegrityError(f"Archive dataset {dataset} has an invalid row.")
                rows.append({str(key): value for key, value in raw.items()})
            return _required_text(record, "snapshot_id"), rows
    return None, []


def _manifest_records(archive: PointInTimeArchive) -> tuple[dict[str, object], ...]:
    if not archive.manifest_path.exists():
        return ()
    records: list[dict[str, object]] = []
    for line in archive.manifest_path.read_text().splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if not isinstance(record, dict):
            raise ArchiveIntegrityError("Archive manifest contains a non-object record.")
        records.append({str(key): value for key, value in record.items()})
    return tuple(records)


def _derived_payload(
    archive: PointInTimeArchive,
    record: Mapping[str, object],
    dataset: str,
) -> dict[str, object]:
    payload = json.loads((archive.root / str(record.get("raw_path", ""))).read_text())
    if not isinstance(payload, dict) or not isinstance(payload.get("rows"), list):
        raise ArchiveIntegrityError(f"Archive dataset {dataset} has an invalid schema.")
    return {str(key): value for key, value in payload.items()}


def _element_text(entry: ElementTree.Element, name: str) -> str:
    element = entry.find(f"{{{ATOM_NAMESPACE}}}{name}")
    text = "" if element is None or element.text is None else element.text.strip()
    if not text:
        raise ValueError(f"SEC Atom entry lacks {name}.")
    return text


def _decode_http_payload(payload: bytes, content_encoding: str | None) -> bytes:
    encoding = "" if content_encoding is None else content_encoding.lower().strip()
    if encoding == "gzip":
        return gzip.decompress(payload)
    if encoding == "deflate":
        return zlib.decompress(payload)
    return payload


def _parse_aware_datetime(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid SEC discovery timestamp: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"SEC discovery timestamp lacks a timezone: {value}")
    return parsed


def _required_text(values: Mapping[str, object], key: str) -> str:
    text = str(values.get(key, "")).strip()
    if not text:
        raise ValueError(f"SEC discovery is missing {key}.")
    return text


def _required_list_text(value: object, label: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"SEC discovery {label} contains an empty value.")
    return text


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a positive integer.")
    parsed = int(str(value))
    if parsed <= 0:
        raise ValueError(f"{label} must be a positive integer.")
    return parsed


if __name__ == "__main__":
    main()
