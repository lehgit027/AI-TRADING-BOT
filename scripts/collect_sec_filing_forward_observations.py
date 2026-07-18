"""Collect due forward market observations for integrity-cleared SEC filing events."""

from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

from ai_trading_bot.data.history_archive import (
    ArchiveIntegrityError,
    PointInTimeArchive,
    SnapshotRecord,
    canonical_json_bytes,
    load_source_policies,
)

try:
    from scripts.plan_sec_filing_forward_observations import (
        ForwardObservationPolicy,
        load_observation_policy,
    )
    from scripts.resolve_sec_filing_first_tradable import (
        QuoteHttpCapture,
        QuoteObservation,
        fetch_quote_page,
        select_first_valid_quote,
    )
    from scripts.trusted_data_research import trusted_data_environment
except ModuleNotFoundError:
    from plan_sec_filing_forward_observations import (  # type: ignore[import-not-found,no-redef]
        ForwardObservationPolicy,
        load_observation_policy,
    )
    from resolve_sec_filing_first_tradable import (  # type: ignore[import-not-found,no-redef]
        QuoteHttpCapture,
        QuoteObservation,
        fetch_quote_page,
        select_first_valid_quote,
    )
    from trusted_data_research import (  # type: ignore[import-not-found,no-redef]
        trusted_data_environment,
    )

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARCHIVE = ROOT / "outputs" / "high_integrity_history"
DEFAULT_POLICIES = ROOT / "config" / "data_source_policies.json"
DEFAULT_OBSERVATION_POLICY = ROOT / "config" / "sec_filing_forward_observation_policy.json"
MARKET_TIMEZONE = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class DueForwardObservation:
    accession_number: str
    cik: str
    symbol: str
    event_types: tuple[str, ...]
    first_tradable_at_text: str
    first_tradable_at: datetime
    first_session_close_at: datetime
    start_bid_price: float
    start_ask_price: float
    benchmark_symbol: str
    costs_bps: tuple[int, ...]
    horizon: str
    target_at: datetime
    window_end_at: datetime
    earliest_retrieval_at: datetime
    plan_snapshot_id: str
    resolution_snapshot_id: str


@dataclass(frozen=True)
class DueState:
    due: tuple[DueForwardObservation, ...]
    awaiting_future_count: int


@dataclass(frozen=True)
class StagedObservation:
    due: DueForwardObservation
    captures: tuple[tuple[str, str, QuoteHttpCapture], ...]
    quotes: dict[str, QuoteObservation | None]


def load_due_observations(
    archive: PointInTimeArchive,
    *,
    now: datetime,
) -> DueState:
    effective_now = _require_aware(now, "now")
    if not archive.manifest_path.exists():
        return DueState((), 0)
    plans: list[tuple[dict[str, object], str]] = []
    completed: set[str] = set()
    for record in _manifest_records(archive):
        if record.get("source_id") != "internal_derived":
            continue
        dataset = str(record.get("dataset", ""))
        if dataset not in {
            "sec_filing_forward_observation_plans",
            "sec_filing_forward_observations",
        }:
            continue
        rows = _derived_rows(archive, record, dataset)
        if dataset == "sec_filing_forward_observations":
            completed.update(
                _observation_key(
                    _required_string(row, "accession_number"),
                    _required_string(row, "horizon"),
                )
                for row in rows
            )
        else:
            plans.extend((row, _required_string(record, "snapshot_id")) for row in rows)

    due: list[DueForwardObservation] = []
    awaiting = 0
    for plan, plan_snapshot_id in plans:
        raw_horizons = plan.get("horizons")
        if not isinstance(raw_horizons, list):
            raise ArchiveIntegrityError("Forward observation plan horizons are invalid.")
        accession = _required_string(plan, "accession_number")
        raw_event_types = plan.get("event_types")
        raw_costs = plan.get("costs_bps")
        if not isinstance(raw_event_types, list) or not isinstance(raw_costs, list):
            raise ArchiveIntegrityError("Forward observation plan metadata is invalid.")
        first_text = _required_string(plan, "first_tradable_at")
        for raw_horizon in cast(list[object], raw_horizons):
            if not isinstance(raw_horizon, dict):
                raise ArchiveIntegrityError("Forward observation horizon is not an object.")
            horizon = _required_string(raw_horizon, "horizon")
            if _observation_key(accession, horizon) in completed:
                continue
            earliest = _parse_aware_datetime(raw_horizon.get("earliest_observation_retrieval_at"))
            if effective_now < earliest:
                awaiting += 1
                continue
            due.append(
                DueForwardObservation(
                    accession_number=accession,
                    cik=_required_string(plan, "cik"),
                    symbol=_required_string(plan, "symbol").upper(),
                    event_types=tuple(sorted(str(item) for item in raw_event_types)),
                    first_tradable_at_text=first_text,
                    first_tradable_at=_parse_timestamp_ceil_microsecond(first_text),
                    first_session_close_at=_parse_aware_datetime(
                        plan.get("first_tradable_session_close_at")
                    ),
                    start_bid_price=_positive_float(
                        plan.get("first_tradable_bid"),
                        "first_tradable_bid",
                    ),
                    start_ask_price=_positive_float(
                        plan.get("first_tradable_ask"),
                        "first_tradable_ask",
                    ),
                    benchmark_symbol=_required_string(plan, "benchmark_symbol").upper(),
                    costs_bps=tuple(_positive_int(value, "costs_bps") for value in raw_costs),
                    horizon=horizon,
                    target_at=_parse_aware_datetime(raw_horizon.get("target_at")),
                    window_end_at=_parse_aware_datetime(
                        raw_horizon.get("observation_window_end_at")
                    ),
                    earliest_retrieval_at=earliest,
                    plan_snapshot_id=plan_snapshot_id,
                    resolution_snapshot_id=_required_string(
                        plan,
                        "resolution_snapshot_id",
                    ),
                )
            )
    return DueState(
        tuple(sorted(due, key=lambda item: (item.target_at, item.accession_number))),
        awaiting,
    )


def collect(
    archive: PointInTimeArchive,
    *,
    api_key: str,
    api_secret: str,
    policy: ForwardObservationPolicy,
    now: datetime | None = None,
    quote_captures: Mapping[str, QuoteHttpCapture] | None = None,
) -> dict[str, object]:
    pre_audit = archive.audit()
    if pre_audit.status == "fail":
        raise ArchiveIntegrityError("Refusing forward observations after audit failure.")
    effective_now = datetime.now(UTC) if now is None else _require_aware(now, "now")
    state = load_due_observations(archive, now=effective_now)
    if not state.due:
        return _status_output("completed", (), state.awaiting_future_count, archive)

    staged: list[StagedObservation] = []
    for due in state.due:
        if due.benchmark_symbol != policy.benchmark_symbol:
            raise ValueError("Observation plan benchmark does not match current policy.")
        if due.costs_bps != policy.costs_bps:
            raise ValueError("Observation plan costs do not match current policy.")
        start_window_end = min(
            due.first_tradable_at + timedelta(seconds=policy.observation_window_seconds),
            due.first_session_close_at,
        )
        captures = (
            (
                "benchmark_start",
                due.benchmark_symbol,
                _capture_for(
                    due,
                    "benchmark_start",
                    quote_captures,
                    api_key=api_key,
                    api_secret=api_secret,
                    symbol=due.benchmark_symbol,
                    start=due.first_tradable_at,
                    end=start_window_end,
                    policy=policy,
                ),
            ),
            (
                "stock_end",
                due.symbol,
                _capture_for(
                    due,
                    "stock_end",
                    quote_captures,
                    api_key=api_key,
                    api_secret=api_secret,
                    symbol=due.symbol,
                    start=due.target_at,
                    end=due.window_end_at,
                    policy=policy,
                ),
            ),
            (
                "benchmark_end",
                due.benchmark_symbol,
                _capture_for(
                    due,
                    "benchmark_end",
                    quote_captures,
                    api_key=api_key,
                    api_secret=api_secret,
                    symbol=due.benchmark_symbol,
                    start=due.target_at,
                    end=due.window_end_at,
                    policy=policy,
                ),
            ),
        )
        quotes = {
            role: select_first_valid_quote(
                capture.payload,
                symbol=symbol,
                earliest_at=(due.first_tradable_at if role == "benchmark_start" else due.target_at),
                latest_at=(start_window_end if role == "benchmark_start" else due.window_end_at),
            )
            for role, symbol, capture in captures
        }
        staged.append(StagedObservation(due=due, captures=captures, quotes=quotes))

    rows: list[dict[str, object]] = []
    raw_records: list[SnapshotRecord] = []
    upstream_ids: set[str] = set()
    latest_retrieval: datetime | None = None
    for item in staged:
        evidence: dict[str, SnapshotRecord] = {}
        for role, symbol, capture in item.captures:
            record = _archive_quote(archive, item.due, role, symbol, capture)
            evidence[role] = record
            raw_records.append(record)
            retrieved = _parse_aware_datetime(record.retrieved_at)
            latest_retrieval = (
                retrieved if latest_retrieval is None else max(latest_retrieval, retrieved)
            )
        row = _observation_row(item, evidence)
        rows.append(row)
        upstream_ids.update(record.snapshot_id for record in evidence.values())
        upstream_ids.update({item.due.plan_snapshot_id, item.due.resolution_snapshot_id})

    if latest_retrieval is None:
        raise ArchiveIntegrityError("Due observations produced no quote evidence.")
    generated_at = max(datetime.now(UTC), latest_retrieval)
    derived_retrieved_at = generated_at + timedelta(microseconds=1)
    derived_payload = {
        "schema_version": "sec_filing_forward_observations_v1",
        "generated_at": generated_at.isoformat(),
        "benchmark_symbol": policy.benchmark_symbol,
        "benchmark_return_type": "unadjusted_quote_price_return",
        "voo_total_return_status": "pending_separate_integrity_reconciliation",
        "costs_bps": list(policy.costs_bps),
        "execution_price_selected": False,
        "paper_fill_applied": False,
        "candidate_created": False,
        "alpha_calculated": False,
        "rows": rows,
    }
    derived_record = archive.capture(
        source_id="internal_derived",
        dataset="sec_filing_forward_observations",
        source_url="internal://observations/sec-filing-forward-returns",
        payload=canonical_json_bytes(derived_payload),
        request_started_at=generated_at,
        retrieved_at=derived_retrieved_at,
        decision_available_at=latest_retrieval,
        decision_availability_basis="all_horizon_quote_responses_first_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        request_parameters={"observation_count": len(rows)},
        response_metadata={
            "observation_count": len(rows),
            "pass_count": sum(str(row["data_quality_status"]).startswith("pass") for row in rows),
            "blocked_count": sum(
                str(row["data_quality_status"]).startswith("blocked") for row in rows
            ),
        },
        upstream_snapshot_ids=tuple(sorted(upstream_ids)),
        integrity_notes=(
            "Every row is a future market observation tied to a genuinely new filing.",
            "Midpoints and fixed cost stresses are measurements, not execution prices or fills.",
            "No missing quote is imputed; incomplete rows remain blocked.",
            (
                "A separate reconciler joins the quote observation to immutable VOO action "
                "and official schedule evidence."
            ),
        ),
    )
    return _status_output(
        "captured",
        rows,
        state.awaiting_future_count,
        archive,
        derived_record,
        len(raw_records),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICIES)
    parser.add_argument("--observation-policy", type=Path, default=DEFAULT_OBSERVATION_POLICY)
    args = parser.parse_args()
    environment = trusted_data_environment(os.environ, ROOT / ".env")
    key = environment.get("AI_TRADING_MARKET_DATA_API_KEY", "").strip()
    secret = environment.get("AI_TRADING_MARKET_DATA_API_SECRET", "").strip()
    if not key or not secret:
        raise ValueError("Missing Alpaca data-only credentials.")
    archive = PointInTimeArchive(args.archive_root, load_source_policies(args.policies))
    output = collect(
        archive,
        api_key=key,
        api_secret=secret,
        policy=load_observation_policy(args.observation_policy),
    )
    latest = args.archive_root / "latest_sec_filing_forward_observations.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(
        "SEC filing forward observations",
        output["status"],
        "observed",
        output["observed_horizon_count"],
        "blocked",
        output["blocked_horizon_count"],
        "future",
        output["awaiting_future_horizon_count"],
    )


def _capture_for(
    due: DueForwardObservation,
    role: str,
    captures: Mapping[str, QuoteHttpCapture] | None,
    *,
    api_key: str,
    api_secret: str,
    symbol: str,
    start: datetime,
    end: datetime,
    policy: ForwardObservationPolicy,
) -> QuoteHttpCapture:
    if end <= start:
        raise ValueError(f"Observation window is empty for {due.accession_number}:{due.horizon}.")
    key = _capture_key(due.accession_number, due.horizon, role)
    if captures is not None:
        return captures[key]
    return fetch_quote_page(
        api_key,
        api_secret,
        symbol=symbol,
        start=start,
        end=end,
        asof=start.astimezone(MARKET_TIMEZONE).date(),
        feed=policy.quote_feed,
        limit=policy.quote_page_limit,
    )


def _archive_quote(
    archive: PointInTimeArchive,
    due: DueForwardObservation,
    role: str,
    symbol: str,
    capture: QuoteHttpCapture,
) -> SnapshotRecord:
    return archive.capture(
        source_id="alpaca_market_data",
        dataset=f"sip_quotes_{symbol.lower()}",
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
        request_parameters={
            **capture.request_parameters,
            "accession_number": due.accession_number,
            "horizon": due.horizon,
            "observation_role": role,
        },
        response_metadata=capture.response_metadata,
        integrity_notes=(
            "Quote was requested only because a genuinely new filing horizon matured.",
            "Quote event time and response retrieval time remain separate.",
            "Raw quote data remains private under provider policy.",
            "No execution price or paper fill is created.",
        ),
    )


def _observation_row(
    item: StagedObservation,
    evidence: Mapping[str, SnapshotRecord],
) -> dict[str, object]:
    due = item.due
    missing = sorted(role for role, quote in item.quotes.items() if quote is None)
    evidence_rows = [
        {
            "role": role,
            "symbol": symbol,
            "snapshot_id": evidence[role].snapshot_id,
            "retrieved_at": evidence[role].retrieved_at,
            "raw_sha256": evidence[role].raw_sha256,
            "raw_bytes": evidence[role].raw_bytes,
        }
        for role, symbol, _capture in item.captures
    ]
    base: dict[str, object] = {
        "observation_type": "sec_filing_forward_market_observation",
        "accession_number": due.accession_number,
        "cik": due.cik,
        "symbol": due.symbol,
        "event_types": list(due.event_types),
        "horizon": due.horizon,
        "target_at": due.target_at.isoformat(),
        "observation_window_end_at": due.window_end_at.isoformat(),
        "earliest_retrieval_at": due.earliest_retrieval_at.isoformat(),
        "first_tradable_at": due.first_tradable_at_text,
        "stock_start_bid": due.start_bid_price,
        "stock_start_ask": due.start_ask_price,
        "stock_start_midpoint": _midpoint(due.start_bid_price, due.start_ask_price),
        "stock_start_source": "first_tradable_resolution",
        "benchmark_symbol": due.benchmark_symbol,
        "quote_evidence": evidence_rows,
        "plan_snapshot_id": due.plan_snapshot_id,
        "resolution_snapshot_id": due.resolution_snapshot_id,
        "benchmark_return_type": "unadjusted_quote_price_return",
        "voo_total_return_status": "pending_separate_integrity_reconciliation",
        "market_observation_only": True,
        "execution_price_selected": False,
        "paper_fill_applied": False,
        "candidate_created": False,
        "alpha_calculated": False,
        "active_profile_changed": False,
        "research_only": True,
    }
    if missing:
        return {
            **base,
            "data_quality_status": "blocked_no_valid_quote",
            "exact_block_reason": f"No valid positive two-sided SIP quote for {missing}.",
            "missing_quote_roles": missing,
            "return_observation": None,
        }
    benchmark_start = _required_quote(item.quotes, "benchmark_start")
    stock_end = _required_quote(item.quotes, "stock_end")
    benchmark_end = _required_quote(item.quotes, "benchmark_end")
    stock_start_mid = _midpoint(due.start_bid_price, due.start_ask_price)
    stock_end_mid = _midpoint(stock_end.bid_price, stock_end.ask_price)
    benchmark_start_mid = _midpoint(
        benchmark_start.bid_price,
        benchmark_start.ask_price,
    )
    benchmark_end_mid = _midpoint(benchmark_end.bid_price, benchmark_end.ask_price)
    stock_return = stock_end_mid / stock_start_mid - 1
    benchmark_return = benchmark_end_mid / benchmark_start_mid - 1
    return {
        **base,
        "data_quality_status": "pass_quote_price_only",
        "quote_timestamp_quality": "provider_nanosecond_exact",
        "benchmark_start": _quote_fields(benchmark_start),
        "stock_end": _quote_fields(stock_end),
        "benchmark_end": _quote_fields(benchmark_end),
        "return_observation": {
            "stock_midpoint_price_return": stock_return,
            "voo_midpoint_price_return": benchmark_return,
            "gross_price_excess_return": stock_return - benchmark_return,
            "stock_net_return_by_cost_bps": {
                str(cost): stock_return - cost / 10_000 for cost in due.costs_bps
            },
            "price_excess_return_by_cost_bps": {
                str(cost): stock_return - cost / 10_000 - benchmark_return for cost in due.costs_bps
            },
            "cost_application": "fixed_round_trip_stress_on_stock_price_return_only",
            "not_execution_or_fill": True,
        },
    }


def _quote_fields(quote: QuoteObservation) -> dict[str, object]:
    return {
        "quote_at": quote.timestamp_text,
        "bid_price": quote.bid_price,
        "ask_price": quote.ask_price,
        "bid_size": quote.bid_size,
        "ask_size": quote.ask_size,
        "bid_exchange": quote.bid_exchange,
        "ask_exchange": quote.ask_exchange,
        "conditions": list(quote.conditions),
        "tape": quote.tape,
        "midpoint": _midpoint(quote.bid_price, quote.ask_price),
        "spread_bps": (
            (quote.ask_price - quote.bid_price)
            / _midpoint(quote.bid_price, quote.ask_price)
            * 10_000
        ),
    }


def _required_quote(
    quotes: Mapping[str, QuoteObservation | None],
    role: str,
) -> QuoteObservation:
    quote = quotes.get(role)
    if quote is None:
        raise ArchiveIntegrityError(f"Missing required quote after validation: {role}")
    return quote


def _midpoint(bid: float, ask: float) -> float:
    if bid <= 0 or ask <= 0 or ask < bid:
        raise ValueError("Cannot calculate midpoint from invalid bid/ask.")
    return (bid + ask) / 2


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


def _capture_key(accession: str, horizon: str, role: str) -> str:
    return f"{accession}:{horizon}:{role}"


def _observation_key(accession: str, horizon: str) -> str:
    return f"{accession}:{horizon}"


def _parse_aware_datetime(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp: {value}") from exc
    return _require_aware(parsed, "timestamp")


def _parse_timestamp_ceil_microsecond(value: object) -> datetime:
    text = str(value)
    parsed = _parse_aware_datetime(text)
    fraction = re.search(r"\.(\d+)(?=Z$|[+-]\d{2}:\d{2}$)", text)
    if fraction is not None:
        digits = fraction.group(1)
        if len(digits) > 6 and any(digit != "0" for digit in digits[6:]):
            parsed += timedelta(microseconds=1)
    return parsed


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


def _positive_float(value: object, label: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be positive.")
    parsed = float(str(value))
    if parsed <= 0:
        raise ValueError(f"{label} must be positive.")
    return parsed


def _status_output(
    status: str,
    rows: tuple[()] | list[dict[str, object]],
    awaiting_count: int,
    archive: PointInTimeArchive,
    derived_record: SnapshotRecord | None = None,
    raw_record_count: int = 0,
) -> dict[str, object]:
    audit = archive.audit()
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_only": True,
        "trading_actions_enabled": False,
        "active_profile_changed": False,
        "status": status if audit.status == "pass" else "integrity_audit_failed",
        "observed_horizon_count": sum(
            row.get("data_quality_status") == "pass_quote_price_only" for row in rows
        ),
        "blocked_horizon_count": sum(
            str(row.get("data_quality_status", "")).startswith("blocked") for row in rows
        ),
        "awaiting_future_horizon_count": awaiting_count,
        "raw_quote_snapshot_count": raw_record_count,
        "observations": list(rows),
        "derived_snapshot": None if derived_record is None else derived_record.to_dict(),
        "archive_audit": audit.to_dict(),
        "voo_total_return_status": "pending_separate_integrity_reconciliation",
        "alpha_calculated": False,
        "execution_price_selected": False,
        "paper_fill_applied": False,
        "candidate_created": False,
    }


if __name__ == "__main__":
    main()
