"""Plan forward-only filing observations after first-tradable evidence exists."""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

from ai_trading_bot.data.history_archive import (
    ArchiveIntegrityError,
    PointInTimeArchive,
    SnapshotRecord,
    canonical_json_bytes,
    load_source_policies,
)

try:
    from scripts.collect_alpaca_market_calendar import CalendarSession
except ModuleNotFoundError:
    from collect_alpaca_market_calendar import (  # type: ignore[import-not-found,no-redef]
        CalendarSession,
    )

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARCHIVE = ROOT / "outputs" / "high_integrity_history"
DEFAULT_POLICIES = ROOT / "config" / "data_source_policies.json"
DEFAULT_OBSERVATION_POLICY = ROOT / "config" / "sec_filing_forward_observation_policy.json"


@dataclass(frozen=True)
class HorizonSpec:
    horizon_id: str
    kind: str
    value: int


@dataclass(frozen=True)
class ForwardObservationPolicy:
    benchmark_symbol: str
    quote_feed: str
    quote_page_limit: int
    delay_buffer_seconds: int
    observation_window_seconds: int
    costs_bps: tuple[int, ...]
    horizons: tuple[HorizonSpec, ...]


@dataclass(frozen=True)
class ReadyFilingEvent:
    accession_number: str
    cik: str
    symbol: str
    event_types: tuple[str, ...]
    first_tradable_at_text: str
    first_tradable_at: datetime
    start_bid_price: float
    start_ask_price: float
    package_snapshot_id: str
    resolution_snapshot_id: str
    calendar_snapshot_id: str


@dataclass(frozen=True)
class PlanningState:
    ready: tuple[ReadyFilingEvent, ...]
    awaiting_first_tradable_count: int


def load_observation_policy(path: Path) -> ForwardObservationPolicy:
    payload = json.loads(path.read_text())
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != "sec_filing_forward_observation_policy_v1"
    ):
        raise ValueError("SEC filing observation policy has an unsupported schema.")
    if payload.get("research_only") is not True:
        raise ValueError("SEC filing observation policy must remain research-only.")
    if payload.get("session_scope") != "regular_hours_only":
        raise ValueError("SEC filing observations require regular-hours sessions.")
    for prohibited in (
        "execution_price_selected",
        "paper_fill_applied",
        "candidate_created",
    ):
        if payload.get(prohibited) is not False:
            raise ValueError(f"SEC filing observation policy enables {prohibited}.")
    if payload.get("return_measure") != "midpoint_observation_only":
        raise ValueError("SEC filing observation policy has an unsupported return measure.")
    raw_horizons = payload.get("horizons")
    if not isinstance(raw_horizons, list) or not raw_horizons:
        raise ValueError("SEC filing observation policy requires horizons.")
    horizons: list[HorizonSpec] = []
    seen_ids: set[str] = set()
    for raw in raw_horizons:
        if not isinstance(raw, dict):
            raise ValueError("SEC filing observation horizon must be an object.")
        horizon_id = _required_string(raw, "id")
        kind = _required_string(raw, "kind")
        if kind not in {"trading_minutes", "full_sessions"}:
            raise ValueError(f"Unsupported observation horizon kind: {kind}")
        if horizon_id in seen_ids:
            raise ValueError(f"Duplicate observation horizon: {horizon_id}")
        seen_ids.add(horizon_id)
        horizons.append(
            HorizonSpec(
                horizon_id=horizon_id,
                kind=kind,
                value=_positive_int(raw.get("value"), f"horizon {horizon_id} value"),
            )
        )
    raw_costs = payload.get("costs_bps")
    if not isinstance(raw_costs, list) or not raw_costs:
        raise ValueError("SEC filing observation policy requires cost stresses.")
    costs = tuple(_positive_int(value, "costs_bps") for value in raw_costs)
    if costs != (5, 25, 50):
        raise ValueError("SEC filing observation costs must remain 5/25/50 bps.")
    quote_feed = _required_string(payload, "quote_feed").lower()
    if quote_feed != "sip":
        raise ValueError("SEC filing observations require the consolidated SIP feed.")
    return ForwardObservationPolicy(
        benchmark_symbol=_required_string(payload, "benchmark_symbol").upper(),
        quote_feed=quote_feed,
        quote_page_limit=_positive_int(
            payload.get("quote_page_limit"),
            "quote_page_limit",
        ),
        delay_buffer_seconds=_positive_int(
            payload.get("historical_quote_delay_buffer_seconds"),
            "historical_quote_delay_buffer_seconds",
        ),
        observation_window_seconds=_positive_int(
            payload.get("observation_window_seconds"),
            "observation_window_seconds",
        ),
        costs_bps=costs,
        horizons=tuple(horizons),
    )


def load_planning_state(archive: PointInTimeArchive) -> PlanningState:
    if not archive.manifest_path.exists():
        return PlanningState((), 0)
    packages: dict[str, tuple[dict[str, object], str]] = {}
    resolutions: dict[str, tuple[dict[str, object], str]] = {}
    planned: set[str] = set()
    eligible_insider_accessions: dict[str, str] = {}
    for record in _manifest_records(archive):
        if record.get("source_id") != "internal_derived":
            continue
        dataset = str(record.get("dataset", ""))
        if dataset not in {
            "sec_filing_document_packages",
            "sec_insider_transaction_events",
            "sec_filing_first_tradable_resolutions",
            "sec_filing_forward_observation_plans",
        }:
            continue
        rows = _derived_rows(archive, record, dataset)
        for row in rows:
            accession = _required_string(row, "accession_number")
            if dataset == "sec_filing_document_packages":
                if row.get("data_quality_status") != "pass":
                    continue
                packages[accession] = (row, _required_string(record, "snapshot_id"))
            elif dataset == "sec_insider_transaction_events":
                if row.get("forward_event_eligible") is True:
                    eligible_insider_accessions[accession] = _required_string(
                        record,
                        "snapshot_id",
                    )
            elif dataset == "sec_filing_first_tradable_resolutions":
                resolutions[accession] = (row, _required_string(record, "snapshot_id"))
            else:
                planned.add(accession)

    ready: list[ReadyFilingEvent] = []
    awaiting = 0
    for accession, (package, package_snapshot_id) in packages.items():
        if accession in planned:
            continue
        form = str(package.get("form", "")).upper()
        insider_snapshot_id = eligible_insider_accessions.get(accession)
        if form in {"4", "4/A"} and insider_snapshot_id is None:
            continue
        resolved = resolutions.get(accession)
        if resolved is None:
            awaiting += 1
            continue
        resolution, resolution_snapshot_id = resolved
        classification = package.get("classification")
        if not isinstance(classification, dict):
            raise ArchiveIntegrityError("Filing document package classification is invalid.")
        event_types_raw = classification.get("event_types")
        if not isinstance(event_types_raw, list) or not event_types_raw:
            raise ArchiveIntegrityError("Filing document package has no event types.")
        first_tradable_text = _required_string(resolution, "first_tradable_at")
        ready.append(
            ReadyFilingEvent(
                accession_number=accession,
                cik=_required_string(package, "cik"),
                symbol=_required_string(package, "symbol").upper(),
                event_types=(
                    ("insider_open_market_transaction",)
                    if insider_snapshot_id is not None
                    else tuple(sorted(str(item) for item in event_types_raw))
                ),
                first_tradable_at_text=first_tradable_text,
                first_tradable_at=_parse_timestamp_ceil_microsecond(first_tradable_text),
                start_bid_price=_positive_float(resolution.get("bid_price"), "bid_price"),
                start_ask_price=_positive_float(resolution.get("ask_price"), "ask_price"),
                package_snapshot_id=(
                    insider_snapshot_id
                    if insider_snapshot_id is not None
                    else package_snapshot_id
                ),
                resolution_snapshot_id=resolution_snapshot_id,
                calendar_snapshot_id=_required_string(resolution, "calendar_snapshot_id"),
            )
        )
    return PlanningState(
        tuple(sorted(ready, key=lambda event: (event.first_tradable_at, event.accession_number))),
        awaiting,
    )


def plan_event_horizons(
    event: ReadyFilingEvent,
    *,
    sessions: tuple[CalendarSession, ...],
    policy: ForwardObservationPolicy,
) -> list[dict[str, object]]:
    start_index = _session_index(event.first_tradable_at, sessions)
    rows: list[dict[str, object]] = []
    for horizon in policy.horizons:
        if horizon.kind == "trading_minutes":
            target_at = _advance_trading_minutes(
                event.first_tradable_at,
                horizon.value,
                sessions[start_index:],
            )
            target_basis = f"{horizon.value}_regular_session_minutes_after_first_tradable"
            observation_window_end = target_at + timedelta(
                seconds=policy.observation_window_seconds
            )
        else:
            target_index = start_index + horizon.value
            if target_index >= len(sessions):
                raise ValueError(
                    f"Calendar coverage cannot plan {horizon.horizon_id} for "
                    f"{event.accession_number}."
                )
            target_at = sessions[target_index].close_at - timedelta(seconds=1)
            target_basis = f"one_second_before_close_after_{horizon.value}_full_sessions"
            observation_window_end = sessions[target_index].close_at
        target_at = target_at.astimezone(sessions[start_index].open_at.tzinfo)
        observation_window_end = observation_window_end.astimezone(
            sessions[start_index].open_at.tzinfo
        )
        rows.append(
            {
                "horizon": horizon.horizon_id,
                "horizon_kind": horizon.kind,
                "horizon_value": horizon.value,
                "target_at": target_at.isoformat(),
                "target_basis": target_basis,
                "target_timestamp_rounding": "ceil_to_microsecond_never_earlier",
                "earliest_observation_retrieval_at": (
                    observation_window_end + timedelta(seconds=policy.delay_buffer_seconds)
                ).isoformat(),
                "observation_window_end_at": observation_window_end.isoformat(),
                "status": "awaiting_future_market_observation",
            }
        )
    return rows


def collect(
    archive: PointInTimeArchive,
    *,
    policy: ForwardObservationPolicy,
) -> dict[str, object]:
    pre_audit = archive.audit()
    if pre_audit.status == "fail":
        raise ArchiveIntegrityError("Refusing observation planning after audit failure.")
    state = load_planning_state(archive)
    if not state.ready:
        return _status_output("completed", 0, state.awaiting_first_tradable_count, archive)
    rows: list[dict[str, object]] = []
    upstream_ids: set[str] = set()
    for event in state.ready:
        sessions = _load_calendar_sessions(archive, event.calendar_snapshot_id)
        horizons = plan_event_horizons(event, sessions=sessions, policy=policy)
        start_session = sessions[_session_index(event.first_tradable_at, sessions)]
        rows.append(
            {
                "observation_type": "sec_filing_forward_observation_plan",
                "accession_number": event.accession_number,
                "cik": event.cik,
                "symbol": event.symbol,
                "event_types": list(event.event_types),
                "first_tradable_at": event.first_tradable_at_text,
                "first_tradable_bid": event.start_bid_price,
                "first_tradable_ask": event.start_ask_price,
                "first_tradable_reference_only": True,
                "first_tradable_session_open_at": start_session.open_at.isoformat(),
                "first_tradable_session_close_at": start_session.close_at.isoformat(),
                "benchmark_symbol": policy.benchmark_symbol,
                "quote_feed": policy.quote_feed,
                "costs_bps": list(policy.costs_bps),
                "return_measure": "midpoint_observation_only",
                "horizons": horizons,
                "package_snapshot_id": event.package_snapshot_id,
                "resolution_snapshot_id": event.resolution_snapshot_id,
                "calendar_snapshot_id": event.calendar_snapshot_id,
                "market_observation_only": True,
                "execution_price_selected": False,
                "paper_fill_applied": False,
                "candidate_created": False,
                "alpha_calculated": False,
                "active_profile_changed": False,
                "research_only": True,
            }
        )
        upstream_ids.update(
            {
                event.package_snapshot_id,
                event.resolution_snapshot_id,
                event.calendar_snapshot_id,
            }
        )

    now = datetime.now(UTC)
    derived_payload = {
        "schema_version": "sec_filing_forward_observation_plans_v1",
        "generated_at": now.isoformat(),
        "benchmark_symbol": policy.benchmark_symbol,
        "costs_bps": list(policy.costs_bps),
        "quote_delay_buffer_seconds": policy.delay_buffer_seconds,
        "execution_price_selected": False,
        "paper_fill_applied": False,
        "candidate_created": False,
        "alpha_calculated": False,
        "rows": rows,
    }
    derived_record = archive.capture(
        source_id="internal_derived",
        dataset="sec_filing_forward_observation_plans",
        source_url="internal://observations/sec-filing-forward-plans",
        payload=canonical_json_bytes(derived_payload),
        request_started_at=now,
        retrieved_at=datetime.now(UTC),
        decision_available_at=now,
        decision_availability_basis="document_package_and_first_tradable_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        request_parameters={"event_count": len(rows)},
        response_metadata={
            "event_count": len(rows),
            "horizon_count": sum(len(cast(list[object], row["horizons"])) for row in rows),
        },
        upstream_snapshot_ids=tuple(sorted(upstream_ids)),
        integrity_notes=(
            "Plans require both an integrity-cleared filing package and first-tradable evidence.",
            "Targets use recorded regular-session boundaries and never impute missing quotes.",
            "The plan creates no position, order, execution price, paper fill, or candidate.",
        ),
    )
    return _status_output(
        "captured",
        len(rows),
        state.awaiting_first_tradable_count,
        archive,
        derived_record,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICIES)
    parser.add_argument("--observation-policy", type=Path, default=DEFAULT_OBSERVATION_POLICY)
    args = parser.parse_args()
    archive = PointInTimeArchive(args.archive_root, load_source_policies(args.policies))
    output = collect(
        archive,
        policy=load_observation_policy(args.observation_policy),
    )
    latest = args.archive_root / "latest_sec_filing_forward_observation_plan.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(
        "SEC filing forward observation planning",
        output["status"],
        "planned",
        output["planned_event_count"],
        "awaiting_first_tradable",
        output["awaiting_first_tradable_count"],
    )


def _session_index(value: datetime, sessions: tuple[CalendarSession, ...]) -> int:
    for index, session in enumerate(sessions):
        if session.open_at <= value < session.close_at:
            return index
    raise ValueError(f"First-tradable timestamp {value.isoformat()} is outside the calendar.")


def _advance_trading_minutes(
    start: datetime,
    minutes: int,
    sessions: tuple[CalendarSession, ...],
) -> datetime:
    remaining = timedelta(minutes=minutes)
    current = start
    for session in sessions:
        segment_start = max(current, session.open_at)
        if segment_start >= session.close_at:
            continue
        available = session.close_at - segment_start
        if remaining < available:
            return segment_start + remaining
        remaining -= available
        current = session.close_at
    raise ValueError(f"Calendar coverage cannot advance {minutes} trading minutes.")


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


def _load_calendar_sessions(
    archive: PointInTimeArchive,
    snapshot_id: str,
) -> tuple[CalendarSession, ...]:
    for record in _manifest_records(archive):
        if record.get("snapshot_id") != snapshot_id:
            continue
        if (
            record.get("source_id") != "internal_derived"
            or record.get("dataset") != "market_calendar_sessions"
        ):
            raise ArchiveIntegrityError(
                f"Referenced calendar snapshot {snapshot_id} has the wrong dataset."
            )
        rows = _derived_rows(archive, record, "market_calendar_sessions")
        sessions: list[CalendarSession] = []
        for row in rows:
            settlement = row.get("settlement_date")
            sessions.append(
                CalendarSession(
                    trading_date=date.fromisoformat(_required_string(row, "trading_date")),
                    open_at=_parse_aware_datetime(row.get("session_open_at")),
                    close_at=_parse_aware_datetime(row.get("session_close_at")),
                    settlement_date=(
                        None if settlement is None else date.fromisoformat(str(settlement))
                    ),
                    source_snapshot_id=_required_string(row, "source_snapshot_id"),
                )
            )
        return tuple(sorted(sessions, key=lambda session: session.trading_date))
    raise ArchiveIntegrityError(f"Referenced calendar snapshot {snapshot_id} is missing.")


def _derived_rows(
    archive: PointInTimeArchive,
    record: Mapping[str, object],
    dataset: str,
) -> list[dict[str, object]]:
    payload = json.loads((archive.root / str(record.get("raw_path", ""))).read_text())
    if not isinstance(payload, dict) or not isinstance(payload.get("rows"), list):
        raise ArchiveIntegrityError(f"Archive dataset {dataset} has an invalid schema.")
    rows: list[dict[str, object]] = []
    for row in cast(list[object], payload["rows"]):
        if not isinstance(row, dict):
            raise ArchiveIntegrityError(f"Archive dataset {dataset} contains an invalid row.")
        rows.append({str(key): value for key, value in row.items()})
    return rows


def _parse_aware_datetime(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"Timestamp is not timezone-aware: {value}")
    return parsed


def _parse_timestamp_ceil_microsecond(value: object) -> datetime:
    text = str(value)
    parsed = _parse_aware_datetime(text)
    fraction = re.search(r"\.(\d+)(?=Z$|[+-]\d{2}:\d{2}$)", text)
    if fraction is not None:
        digits = fraction.group(1)
        if len(digits) > 6 and any(digit != "0" for digit in digits[6:]):
            parsed += timedelta(microseconds=1)
    return parsed


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


def _positive_float(value: object, label: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be positive.")
    parsed = float(str(value))
    if parsed <= 0:
        raise ValueError(f"{label} must be positive.")
    return parsed


def _status_output(
    status: str,
    planned_count: int,
    awaiting_count: int,
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
        "planned_event_count": planned_count,
        "awaiting_first_tradable_count": awaiting_count,
        "derived_snapshot": None if derived_record is None else derived_record.to_dict(),
        "archive_audit": audit.to_dict(),
        "alpha_calculated": False,
        "execution_price_selected": False,
        "paper_fill_applied": False,
        "candidate_created": False,
    }


if __name__ == "__main__":
    main()
