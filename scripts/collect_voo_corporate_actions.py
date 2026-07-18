"""Collect a private point-in-time VOO corporate-action ledger and calculate total return."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

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
DEFAULT_TOTAL_RETURN_POLICY = ROOT / "config" / "voo_total_return_policy.json"
ALPACA_CORPORATE_ACTIONS_URL = "https://data.alpaca.markets/v1/corporate-actions"
MARKET_TIMEZONE = ZoneInfo("America/New_York")
_GROUP_TO_TYPE = {
    "cash_dividends": "cash_dividend",
    "stock_dividends": "stock_dividend",
    "forward_splits": "forward_split",
    "reverse_splits": "reverse_split",
}


@dataclass(frozen=True)
class VooTotalReturnPolicy:
    symbol: str
    market_timezone: str
    corporate_action_types: tuple[str, ...]
    lookback_days: int
    future_days: int
    page_limit: int
    max_pages: int
    total_return_convention: str
    require_ledger_after_horizon: bool
    require_official_schedule: bool


@dataclass(frozen=True)
class CorporateActionHttpCapture:
    payload: bytes
    request_started_at: datetime
    retrieved_at: datetime
    content_type: str
    response_metadata: dict[str, object]
    source_url: str
    request_parameters: dict[str, object]


@dataclass(frozen=True)
class PriorActionObservation:
    first_seen_at: datetime
    first_observed_source_snapshot_id: str
    latest_state_sha256: str
    latest_ledger_snapshot_id: str


@dataclass(frozen=True)
class VooCorporateAction:
    action_id: str
    action_type: str
    symbol: str
    process_date: date
    ex_date: date
    record_date: date | None
    payable_date: date | None
    rate: float | None
    old_rate: float | None
    new_rate: float | None
    first_seen_at: datetime
    first_observed_source_snapshot_id: str
    source_snapshot_id: str
    state_sha256: str
    data_quality_status: str


@dataclass(frozen=True)
class VooCorporateActionLedger:
    snapshot_id: str
    retrieved_at: datetime
    query_start: date
    query_end: date
    symbol: str
    actions: tuple[VooCorporateAction, ...]


@dataclass(frozen=True)
class OfficialDistributionDate:
    ex_dividend_date: date
    record_date: date
    payable_date: date
    verified_amount_per_share: float | None


@dataclass(frozen=True)
class VooOfficialSchedule:
    snapshot_id: str
    retrieved_at: datetime
    calendar_year: int
    symbol: str
    rows: tuple[OfficialDistributionDate, ...]


@dataclass(frozen=True)
class VooTotalReturnResult:
    status: str
    exact_block_reason: str | None
    price_return: float
    total_return: float | None
    distribution_cash_per_initial_share: float | None
    ending_shares_per_initial_share: float | None
    action_ids: tuple[str, ...]
    ledger_snapshot_id: str | None
    official_schedule_snapshot_id: str | None
    convention: str

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["action_ids"] = list(self.action_ids)
        return payload


def load_total_return_policy(path: Path) -> VooTotalReturnPolicy:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict) or payload.get("schema_version") != (
        "voo_total_return_policy_v1"
    ):
        raise ValueError("VOO total-return policy has an unsupported schema.")
    if payload.get("research_only") is not True:
        raise ValueError("VOO total-return policy must remain research-only.")
    for key in ("execution_price_selected", "paper_fill_applied", "candidate_created"):
        if payload.get(key) is not False:
            raise ValueError(f"VOO total-return policy enables prohibited action: {key}")
    raw_types = payload.get("corporate_action_types")
    if not isinstance(raw_types, list) or not raw_types:
        raise ValueError("VOO policy must list corporate-action types.")
    action_types = tuple(str(item).strip() for item in raw_types)
    if set(action_types) - set(_GROUP_TO_TYPE.values()):
        raise ValueError("VOO policy contains an unsupported corporate-action type.")
    return VooTotalReturnPolicy(
        symbol=_required_text(payload, "symbol").upper(),
        market_timezone=_required_text(payload, "market_timezone"),
        corporate_action_types=action_types,
        lookback_days=_positive_int(payload.get("lookback_days"), "lookback_days"),
        future_days=_positive_int(payload.get("future_days"), "future_days"),
        page_limit=_positive_int(payload.get("page_limit"), "page_limit"),
        max_pages=_positive_int(payload.get("max_pages"), "max_pages"),
        total_return_convention=_required_text(payload, "total_return_convention"),
        require_ledger_after_horizon=bool(
            payload.get("require_ledger_retrieved_at_or_after_horizon")
        ),
        require_official_schedule=bool(payload.get("require_independent_official_schedule")),
    )


def fetch_corporate_action_page(
    api_key: str,
    api_secret: str,
    *,
    symbol: str,
    action_types: tuple[str, ...],
    start: date,
    end: date,
    limit: int,
    page_token: str | None = None,
) -> CorporateActionHttpCapture:
    parameters: dict[str, object] = {
        "symbols": symbol,
        "types": ",".join(action_types),
        "region": "us",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "limit": limit,
        "sort": "asc",
    }
    if page_token is not None:
        parameters["page_token"] = page_token
    source_url = f"{ALPACA_CORPORATE_ACTIONS_URL}?{urlencode(parameters)}"
    started = datetime.now(UTC)
    request = Request(
        source_url,
        headers={
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret,
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=60) as response:  # noqa: S310 - fixed Alpaca endpoint
        payload = response.read()
        retrieved = datetime.now(UTC)
        metadata = {
            key: response.headers[key]
            for key in (
                "Date",
                "ETag",
                "Last-Modified",
                "Content-Length",
                "X-RateLimit-Limit",
                "X-RateLimit-Remaining",
                "X-RateLimit-Reset",
            )
            if response.headers.get(key) is not None
        }
        metadata["http_status"] = response.status
        metadata["latency_ms"] = round((retrieved - started).total_seconds() * 1000, 3)
        return CorporateActionHttpCapture(
            payload=payload,
            request_started_at=started,
            retrieved_at=retrieved,
            content_type=response.headers.get_content_type(),
            response_metadata=metadata,
            source_url=source_url,
            request_parameters=parameters,
        )


def fetch_corporate_action_pages(
    api_key: str,
    api_secret: str,
    *,
    policy: VooTotalReturnPolicy,
    start: date,
    end: date,
) -> tuple[CorporateActionHttpCapture, ...]:
    captures: list[CorporateActionHttpCapture] = []
    token: str | None = None
    seen_tokens: set[str] = set()
    for _ in range(policy.max_pages):
        capture = fetch_corporate_action_page(
            api_key,
            api_secret,
            symbol=policy.symbol,
            action_types=policy.corporate_action_types,
            start=start,
            end=end,
            limit=policy.page_limit,
            page_token=token,
        )
        captures.append(capture)
        parsed = _response_payload(capture.payload)
        next_token = _optional_text(parsed.get("next_page_token"))
        if next_token is None:
            return tuple(captures)
        if next_token in seen_tokens:
            raise ValueError("Alpaca corporate-action pagination repeated a token.")
        seen_tokens.add(next_token)
        token = next_token
    raise ValueError("Alpaca corporate-action response exceeded the configured page limit.")


def load_prior_action_observations(
    archive: PointInTimeArchive,
) -> dict[str, PriorActionObservation]:
    if not archive.manifest_path.exists():
        return {}
    observations: dict[str, PriorActionObservation] = {}
    for record in _manifest_records(archive):
        if record.get("source_id") != "internal_derived" or record.get("dataset") != (
            "voo_corporate_action_ledger"
        ):
            continue
        for row in _derived_rows(archive, record, "voo_corporate_action_ledger"):
            action_id = _required_text(row, "action_id")
            first_seen = _parse_aware_datetime(row.get("first_seen_at"))
            candidate = PriorActionObservation(
                first_seen_at=first_seen,
                first_observed_source_snapshot_id=_required_text(
                    row, "first_observed_source_snapshot_id"
                ),
                latest_state_sha256=_required_text(row, "state_sha256"),
                latest_ledger_snapshot_id=_required_text(record, "snapshot_id"),
            )
            current = observations.get(action_id)
            if current is None or _parse_aware_datetime(record.get("retrieved_at")) >= (
                _ledger_retrieved_at(archive, current.latest_ledger_snapshot_id)
            ):
                observations[action_id] = candidate
    return observations


def normalize_corporate_actions(
    payload: bytes,
    *,
    symbol: str,
    observed_at: datetime,
    source_snapshot_id: str,
    source_sha256: str,
    prior_observations: Mapping[str, PriorActionObservation],
) -> list[dict[str, object]]:
    parsed = _response_payload(payload)
    grouped = parsed.get("corporate_actions")
    if not isinstance(grouped, dict):
        raise ValueError("Alpaca corporate-actions response lacks the action object.")
    unknown_groups = set(str(key) for key in grouped) - set(_GROUP_TO_TYPE)
    if unknown_groups:
        raise ValueError(f"Unexpected corporate-action groups: {sorted(unknown_groups)}")
    rows: list[dict[str, object]] = []
    for group_name, action_type in _GROUP_TO_TYPE.items():
        raw_rows = grouped.get(group_name, [])
        if not isinstance(raw_rows, list):
            raise ValueError(f"Corporate-action group {group_name} is not an array.")
        for raw in cast(list[object], raw_rows):
            if not isinstance(raw, dict):
                raise ValueError(f"Corporate-action group {group_name} contains a non-object.")
            action_id = _required_text(raw, "id")
            row_symbol = _required_text(raw, "symbol").upper()
            if row_symbol != symbol.upper():
                raise ValueError(
                    f"Corporate-action symbol mismatch: expected {symbol}, received {row_symbol}."
                )
            process_date = _required_date(raw.get("process_date"), "process_date")
            ex_date = _required_date(raw.get("ex_date"), "ex_date")
            rate = _optional_positive_float(raw.get("rate"), "rate")
            old_rate = _optional_positive_float(raw.get("old_rate"), "old_rate")
            new_rate = _optional_positive_float(raw.get("new_rate"), "new_rate")
            quality = _action_quality(action_type, rate, old_rate, new_rate)
            core = {
                "action_id": action_id,
                "action_type": action_type,
                "symbol": row_symbol,
                "cusip": _optional_text(raw.get("cusip")),
                "isin": _optional_text(raw.get("isin")),
                "currency": _optional_text(raw.get("currency")),
                "process_date": process_date.isoformat(),
                "ex_date": ex_date.isoformat(),
                "record_date": _optional_date_text(raw.get("record_date"), "record_date"),
                "payable_date": _optional_date_text(raw.get("payable_date"), "payable_date"),
                "due_bill_redemption_date": _optional_date_text(
                    raw.get("due_bill_redemption_date"), "due_bill_redemption_date"
                ),
                "rate": rate,
                "old_rate": old_rate,
                "new_rate": new_rate,
                "foreign": _optional_bool(raw.get("foreign")),
                "special": _optional_bool(raw.get("special")),
                "sub_type": _optional_text(raw.get("sub_type")),
            }
            state_sha256 = hashlib.sha256(canonical_json_bytes(core)).hexdigest()
            prior = prior_observations.get(action_id)
            if prior is None:
                first_seen = observed_at
                first_source = source_snapshot_id
                revision_status = "first_observation"
                previous_ledger = None
            else:
                first_seen = prior.first_seen_at
                first_source = prior.first_observed_source_snapshot_id
                revision_status = (
                    "reconfirmed_unchanged"
                    if prior.latest_state_sha256 == state_sha256
                    else "revised_provider_state"
                )
                previous_ledger = prior.latest_ledger_snapshot_id
            first_seen_date = first_seen.astimezone(MARKET_TIMEZONE).date()
            if first_seen_date <= ex_date:
                availability_status = "pass_first_seen_on_or_before_ex_date"
            else:
                availability_status = "blocked_late_provider_observation"
            rows.append(
                {
                    **core,
                    "observation_type": "voo_corporate_action_state",
                    "first_seen_at": first_seen.isoformat(),
                    "decision_available_at": first_seen.isoformat(),
                    "decision_availability_basis": "collector_first_observed",
                    "first_observed_source_snapshot_id": first_source,
                    "current_source_snapshot_id": source_snapshot_id,
                    "current_source_sha256": source_sha256,
                    "state_sha256": state_sha256,
                    "revision_status": revision_status,
                    "previous_ledger_snapshot_id": previous_ledger,
                    "availability_integrity_status": availability_status,
                    "data_quality_status": quality,
                    "historical_test_only": ex_date < observed_at.astimezone(MARKET_TIMEZONE).date()
                    and prior is None,
                    "redistribution_allowed": False,
                    "research_only": True,
                    "active_profile_changed": False,
                }
            )
    ids = [str(row["action_id"]) for row in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("Alpaca corporate-actions response repeats an action ID.")
    return sorted(
        rows,
        key=lambda row: (str(row["ex_date"]), str(row["action_type"]), str(row["action_id"])),
    )


def collect(
    archive: PointInTimeArchive,
    *,
    api_key: str,
    api_secret: str,
    policy: VooTotalReturnPolicy,
    start: date,
    end: date,
    http_captures: tuple[CorporateActionHttpCapture, ...] | None = None,
) -> dict[str, object]:
    if end < start:
        raise ValueError("Corporate-action query end cannot precede start.")
    pre_audit = archive.audit()
    if pre_audit.status == "fail":
        raise ArchiveIntegrityError("Refusing corporate-action collection after audit failure.")
    captures = http_captures or fetch_corporate_action_pages(
        api_key,
        api_secret,
        policy=policy,
        start=start,
        end=end,
    )
    if not captures:
        raise ValueError("Corporate-action collection requires at least one response page.")
    prior = load_prior_action_observations(archive)
    raw_records: list[SnapshotRecord] = []
    rows: list[dict[str, object]] = []
    for capture in captures:
        raw_record = archive.capture(
            source_id="alpaca_market_data",
            dataset="corporate_actions_voo",
            source_url=capture.source_url,
            payload=capture.payload,
            request_started_at=capture.request_started_at,
            retrieved_at=capture.retrieved_at,
            decision_available_at=capture.retrieved_at,
            decision_availability_basis="first_observed",
            market_timezone=policy.market_timezone,
            timestamp_quality="first_seen_exact",
            research_use="forward_only",
            content_type=capture.content_type,
            request_parameters={
                **capture.request_parameters,
                "api_key": api_key,
                "api_secret": api_secret,
            },
            response_metadata=capture.response_metadata,
            integrity_notes=(
                "Provider action dates remain distinct from collector first-seen availability.",
                "Alpaca warns that corporate actions may be created or delivered late.",
                "Raw and reversible action data remains private under provider policy.",
            ),
        )
        raw_records.append(raw_record)
        rows.extend(
            normalize_corporate_actions(
                capture.payload,
                symbol=policy.symbol,
                observed_at=capture.retrieved_at,
                source_snapshot_id=raw_record.snapshot_id,
                source_sha256=raw_record.raw_sha256,
                prior_observations=prior,
            )
        )
    ids = [str(row["action_id"]) for row in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("Paginated corporate-action responses repeat an action ID.")
    latest_retrieval = max(capture.retrieved_at for capture in captures)
    derived_payload = {
        "schema_version": "voo_corporate_action_ledger_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "symbol": policy.symbol,
        "query_start": start.isoformat(),
        "query_end": end.isoformat(),
        "retrieved_at": latest_retrieval.isoformat(),
        "action_types_requested": list(policy.corporate_action_types),
        "page_count": len(captures),
        "full_state_snapshot": True,
        "provider_creation_time_guaranteed": False,
        "redistribution_allowed": False,
        "rows": rows,
    }
    derived_started = datetime.now(UTC)
    derived_record = archive.capture(
        source_id="internal_derived",
        dataset="voo_corporate_action_ledger",
        source_url="internal://benchmarks/voo-corporate-action-ledger",
        payload=canonical_json_bytes(derived_payload),
        request_started_at=derived_started,
        retrieved_at=max(datetime.now(UTC), latest_retrieval + timedelta(microseconds=1)),
        decision_available_at=latest_retrieval,
        decision_availability_basis="all_corporate_action_pages_first_observed",
        market_timezone=policy.market_timezone,
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        request_parameters={
            "symbol": policy.symbol,
            "query_start": start.isoformat(),
            "query_end": end.isoformat(),
        },
        response_metadata={
            "action_count": len(rows),
            "page_count": len(captures),
            "revision_count": sum(
                row["revision_status"] == "revised_provider_state" for row in rows
            ),
            "late_observation_count": sum(
                row["availability_integrity_status"] == "blocked_late_provider_observation"
                for row in rows
            ),
        },
        upstream_snapshot_ids=tuple(record.snapshot_id for record in raw_records),
        integrity_notes=(
            "This full-state ledger preserves first-seen and revision lineage per action ID.",
            "No absence or per-share amount is independently verified by this provider alone.",
            "The upstream Alpaca redistribution restriction applies to this derived snapshot.",
        ),
    )
    audit = archive.audit()
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_only": True,
        "trading_actions_enabled": False,
        "active_profile_changed": False,
        "status": "captured" if audit.status == "pass" else "integrity_audit_failed",
        "symbol": policy.symbol,
        "query_start": start.isoformat(),
        "query_end": end.isoformat(),
        "corporate_action_count": len(rows),
        "cash_dividend_count": sum(row["action_type"] == "cash_dividend" for row in rows),
        "split_count": sum(
            row["action_type"] in {"forward_split", "reverse_split"} for row in rows
        ),
        "revision_count": sum(row["revision_status"] == "revised_provider_state" for row in rows),
        "raw_snapshot_count": len(raw_records),
        "raw_snapshots": [record.to_dict() for record in raw_records],
        "derived_snapshot": derived_record.to_dict(),
        "archive_audit": audit.to_dict(),
        "redistribution": "blocked_without_written_permission",
    }


def load_latest_corporate_action_ledger(
    archive: PointInTimeArchive,
) -> VooCorporateActionLedger | None:
    if not archive.manifest_path.exists():
        return None
    for record in reversed(_manifest_records(archive)):
        if record.get("source_id") != "internal_derived" or record.get("dataset") != (
            "voo_corporate_action_ledger"
        ):
            continue
        payload = _derived_payload(archive, record, "voo_corporate_action_ledger")
        actions: list[VooCorporateAction] = []
        for row in cast(list[object], payload["rows"]):
            if not isinstance(row, dict):
                raise ArchiveIntegrityError("VOO corporate-action ledger contains an invalid row.")
            actions.append(
                VooCorporateAction(
                    action_id=_required_text(row, "action_id"),
                    action_type=_required_text(row, "action_type"),
                    symbol=_required_text(row, "symbol").upper(),
                    process_date=_required_date(row.get("process_date"), "process_date"),
                    ex_date=_required_date(row.get("ex_date"), "ex_date"),
                    record_date=_optional_date(row.get("record_date"), "record_date"),
                    payable_date=_optional_date(row.get("payable_date"), "payable_date"),
                    rate=_optional_positive_float(row.get("rate"), "rate"),
                    old_rate=_optional_positive_float(row.get("old_rate"), "old_rate"),
                    new_rate=_optional_positive_float(row.get("new_rate"), "new_rate"),
                    first_seen_at=_parse_aware_datetime(row.get("first_seen_at")),
                    first_observed_source_snapshot_id=_required_text(
                        row, "first_observed_source_snapshot_id"
                    ),
                    source_snapshot_id=_required_text(row, "current_source_snapshot_id"),
                    state_sha256=_required_text(row, "state_sha256"),
                    data_quality_status=_required_text(row, "data_quality_status"),
                )
            )
        return VooCorporateActionLedger(
            snapshot_id=_required_text(record, "snapshot_id"),
            retrieved_at=_parse_aware_datetime(payload.get("retrieved_at")),
            query_start=_required_date(payload.get("query_start"), "query_start"),
            query_end=_required_date(payload.get("query_end"), "query_end"),
            symbol=_required_text(payload, "symbol").upper(),
            actions=tuple(actions),
        )
    return None


def load_latest_official_schedule(archive: PointInTimeArchive) -> VooOfficialSchedule | None:
    if not archive.manifest_path.exists():
        return None
    for record in reversed(_manifest_records(archive)):
        if record.get("source_id") != "internal_derived" or record.get("dataset") != (
            "voo_official_distribution_schedule"
        ):
            continue
        payload = _derived_payload(archive, record, "voo_official_distribution_schedule")
        rows: list[OfficialDistributionDate] = []
        for raw in cast(list[object], payload["rows"]):
            if not isinstance(raw, dict):
                raise ArchiveIntegrityError("Official VOO schedule contains an invalid row.")
            rows.append(
                OfficialDistributionDate(
                    ex_dividend_date=_required_date(
                        raw.get("ex_dividend_date"), "ex_dividend_date"
                    ),
                    record_date=_required_date(raw.get("record_date"), "record_date"),
                    payable_date=_required_date(raw.get("payable_date"), "payable_date"),
                    verified_amount_per_share=_optional_positive_float(
                        raw.get("verified_amount_per_share"), "verified_amount_per_share"
                    ),
                )
            )
        return VooOfficialSchedule(
            snapshot_id=_required_text(record, "snapshot_id"),
            retrieved_at=_parse_aware_datetime(record.get("retrieved_at")),
            calendar_year=int(str(payload.get("calendar_year"))),
            symbol=_required_text(payload, "symbol").upper(),
            rows=tuple(rows),
        )
    return None


def calculate_voo_total_return(
    *,
    policy: VooTotalReturnPolicy,
    ledger: VooCorporateActionLedger | None,
    official_schedule: VooOfficialSchedule | None,
    start_at: datetime,
    end_at: datetime,
    start_price: float,
    end_price: float,
) -> VooTotalReturnResult:
    start = _require_aware(start_at, "start_at")
    end = _require_aware(end_at, "end_at")
    if end <= start:
        raise ValueError("Total-return end must be after start.")
    if start_price <= 0 or end_price <= 0:
        raise ValueError("Total-return prices must be positive.")
    price_return = end_price / start_price - 1
    ledger_snapshot_id = None if ledger is None else ledger.snapshot_id
    official_schedule_snapshot_id = (
        None if official_schedule is None else official_schedule.snapshot_id
    )

    def blocked(reason: str, actions: tuple[str, ...] = ()) -> VooTotalReturnResult:
        return VooTotalReturnResult(
            status="blocked",
            exact_block_reason=reason,
            total_return=None,
            distribution_cash_per_initial_share=None,
            ending_shares_per_initial_share=None,
            price_return=price_return,
            action_ids=actions,
            ledger_snapshot_id=ledger_snapshot_id,
            official_schedule_snapshot_id=official_schedule_snapshot_id,
            convention=policy.total_return_convention,
        )

    if ledger is None:
        return blocked("No archived VOO corporate-action ledger is available.")
    if ledger.symbol != policy.symbol:
        return blocked("Corporate-action ledger symbol does not match VOO policy.")
    start_date = start.astimezone(MARKET_TIMEZONE).date()
    end_date = end.astimezone(MARKET_TIMEZONE).date()
    if ledger.query_start > start_date or ledger.query_end < end_date:
        return blocked("Corporate-action ledger query dates do not cover the observation interval.")
    if policy.require_ledger_after_horizon and ledger.retrieved_at < end:
        return blocked(
            "Corporate-action ledger was retrieved before the observation horizon matured."
        )
    if policy.require_official_schedule and official_schedule is None:
        return blocked("No independently archived official VOO distribution schedule is available.")
    if official_schedule is None:
        official_rows: tuple[OfficialDistributionDate, ...] = ()
    else:
        if official_schedule.symbol != policy.symbol:
            return blocked("Official distribution schedule symbol does not match VOO policy.")
        if start_date.year != official_schedule.calendar_year or end_date.year != (
            official_schedule.calendar_year
        ):
            return blocked("Official VOO schedule does not cover the full observation interval.")
        official_rows = tuple(
            row for row in official_schedule.rows if start_date < row.ex_dividend_date <= end_date
        )
    actions = tuple(action for action in ledger.actions if start_date < action.ex_date <= end_date)
    action_ids = tuple(action.action_id for action in actions)
    bad_actions = tuple(
        action.action_id for action in actions if action.data_quality_status != "pass"
    )
    if bad_actions:
        return blocked(
            f"VOO corporate actions failed schema/amount integrity: {list(bad_actions)}.",
            action_ids,
        )
    official_dates = {row.ex_dividend_date for row in official_rows}
    cash_dates = {action.ex_date for action in actions if action.action_type == "cash_dividend"}
    if official_dates != cash_dates:
        return blocked(
            "Alpaca action dates disagree with the archived official VOO schedule.",
            action_ids,
        )
    unsupported = tuple(
        action.action_id
        for action in actions
        if action.action_type not in {"cash_dividend", "forward_split", "reverse_split"}
    )
    if unsupported:
        return blocked(f"Unsupported VOO corporate actions: {list(unsupported)}.", action_ids)
    split_actions = tuple(
        action.action_id
        for action in actions
        if action.action_type in {"forward_split", "reverse_split"}
    )
    if split_actions:
        return blocked(
            f"VOO split lacks an independent official action cross-check: {list(split_actions)}.",
            action_ids,
        )
    if not actions and not official_rows:
        return VooTotalReturnResult(
            status="pass_verified_no_corporate_action",
            exact_block_reason=None,
            total_return=price_return,
            distribution_cash_per_initial_share=0.0,
            ending_shares_per_initial_share=1.0,
            price_return=price_return,
            action_ids=(),
            ledger_snapshot_id=ledger_snapshot_id,
            official_schedule_snapshot_id=official_schedule_snapshot_id,
            convention=policy.total_return_convention,
        )
    official_by_date = {row.ex_dividend_date: row for row in official_rows}
    unverified_amounts = tuple(
        action.action_id
        for action in actions
        if action.action_type == "cash_dividend"
        and (
            official_by_date[action.ex_date].verified_amount_per_share is None
            or action.rate is None
            or abs(
                action.rate
                - cast(float, official_by_date[action.ex_date].verified_amount_per_share)
            )
            > 1e-9
        )
    )
    if unverified_amounts:
        return blocked(
            "VOO distribution date is cross-checked, but the per-share amount is not "
            f"independently verified for {list(unverified_amounts)}.",
            action_ids,
        )
    shares = 1.0
    cash = 0.0
    for action in sorted(actions, key=lambda item: (item.ex_date, item.action_id)):
        if action.action_type == "cash_dividend":
            cash += shares * cast(float, action.rate)
        elif action.action_type in {"forward_split", "reverse_split"}:
            shares *= cast(float, action.new_rate) / cast(float, action.old_rate)
    total_return = (shares * end_price + cash) / start_price - 1
    return VooTotalReturnResult(
        status="pass_verified_corporate_action_adjustment",
        exact_block_reason=None,
        total_return=total_return,
        distribution_cash_per_initial_share=cash,
        ending_shares_per_initial_share=shares,
        price_return=price_return,
        action_ids=action_ids,
        ledger_snapshot_id=ledger_snapshot_id,
        official_schedule_snapshot_id=official_schedule_snapshot_id,
        convention=policy.total_return_convention,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICIES)
    parser.add_argument("--total-return-policy", type=Path, default=DEFAULT_TOTAL_RETURN_POLICY)
    args = parser.parse_args()
    environment = trusted_data_environment(os.environ, ROOT / ".env")
    api_key = environment.get("AI_TRADING_MARKET_DATA_API_KEY", "").strip()
    api_secret = environment.get("AI_TRADING_MARKET_DATA_API_SECRET", "").strip()
    if not api_key or not api_secret:
        raise ValueError("Missing Alpaca data-only credentials.")
    policy = load_total_return_policy(args.total_return_policy)
    today = datetime.now(MARKET_TIMEZONE).date()
    archive = PointInTimeArchive(args.archive_root, load_source_policies(args.policies))
    output = collect(
        archive,
        api_key=api_key,
        api_secret=api_secret,
        policy=policy,
        start=today - timedelta(days=policy.lookback_days),
        end=today + timedelta(days=policy.future_days),
    )
    latest = args.archive_root / "latest_voo_corporate_action_collection.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(
        "VOO corporate-action ledger",
        output["status"],
        "actions",
        output["corporate_action_count"],
        "revisions",
        output["revision_count"],
    )


def _response_payload(payload: bytes) -> dict[str, object]:
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise ValueError("Alpaca corporate-actions response must be a JSON object.")
    return {str(key): value for key, value in parsed.items()}


def _action_quality(
    action_type: str,
    rate: float | None,
    old_rate: float | None,
    new_rate: float | None,
) -> str:
    if action_type == "cash_dividend":
        return "pass" if rate is not None else "blocked_missing_cash_rate"
    if action_type in {"forward_split", "reverse_split"}:
        return (
            "pass"
            if old_rate is not None and new_rate is not None
            else "blocked_missing_split_ratio"
        )
    if action_type == "stock_dividend":
        return "blocked_stock_dividend_calculation_not_supported"
    return "blocked_unsupported_action_type"


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


def _derived_rows(
    archive: PointInTimeArchive,
    record: Mapping[str, object],
    dataset: str,
) -> list[dict[str, object]]:
    payload = _derived_payload(archive, record, dataset)
    rows: list[dict[str, object]] = []
    for raw in cast(list[object], payload["rows"]):
        if not isinstance(raw, dict):
            raise ArchiveIntegrityError(f"Archive dataset {dataset} contains an invalid row.")
        rows.append({str(key): value for key, value in raw.items()})
    return rows


def _ledger_retrieved_at(archive: PointInTimeArchive, snapshot_id: str) -> datetime:
    for record in _manifest_records(archive):
        if record.get("snapshot_id") == snapshot_id:
            return _parse_aware_datetime(record.get("retrieved_at"))
    raise ArchiveIntegrityError(f"Prior ledger snapshot is missing: {snapshot_id}")


def _required_text(values: Mapping[str, object], key: str) -> str:
    text = _optional_text(values.get(key))
    if text is None:
        raise ValueError(f"Missing required field: {key}")
    return text


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a positive integer.")
    parsed = int(str(value))
    if parsed <= 0:
        raise ValueError(f"{label} must be a positive integer.")
    return parsed


def _optional_positive_float(value: object, label: str) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"{label} must be positive.")
    parsed = float(str(value))
    if parsed <= 0:
        raise ValueError(f"{label} must be positive.")
    return parsed


def _optional_bool(value: object) -> bool | None:
    if value is None or value == "":
        return None
    if not isinstance(value, bool):
        raise ValueError("Corporate-action boolean field has an invalid type.")
    return value


def _required_date(value: object, label: str) -> date:
    text = _optional_text(value)
    if text is None:
        raise ValueError(f"Missing corporate-action date: {label}")
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"Invalid corporate-action date {label}: {value}") from exc


def _optional_date(value: object, label: str) -> date | None:
    text = _optional_text(value)
    return None if text is None else _required_date(text, label)


def _optional_date_text(value: object, label: str) -> str | None:
    parsed = _optional_date(value, label)
    return None if parsed is None else parsed.isoformat()


def _parse_aware_datetime(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp: {value}") from exc
    return _require_aware(parsed, "timestamp")


def _require_aware(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware.")
    return value


if __name__ == "__main__":
    main()
