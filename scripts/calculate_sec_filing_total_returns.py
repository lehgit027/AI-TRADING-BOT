"""Reconcile SEC filing quote observations to an integrity-cleared VOO total return."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
    from scripts.collect_voo_corporate_actions import (
        calculate_voo_total_return,
        load_latest_corporate_action_ledger,
        load_latest_official_schedule,
        load_total_return_policy,
    )
except ModuleNotFoundError:
    from collect_voo_corporate_actions import (  # type: ignore[import-not-found,no-redef]
        calculate_voo_total_return,
        load_latest_corporate_action_ledger,
        load_latest_official_schedule,
        load_total_return_policy,
    )

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARCHIVE = ROOT / "outputs" / "high_integrity_history"
DEFAULT_POLICIES = ROOT / "config" / "data_source_policies.json"
DEFAULT_TOTAL_RETURN_POLICY = ROOT / "config" / "voo_total_return_policy.json"


@dataclass(frozen=True)
class QuoteObservationForReconciliation:
    accession_number: str
    horizon: str
    symbol: str
    first_tradable_at: datetime
    target_at: datetime
    stock_return: float
    voo_start_midpoint: float
    voo_end_midpoint: float
    stock_net_return_by_cost_bps: dict[str, float]
    quote_observation_snapshot_id: str
    quote_observation_retrieved_at: datetime


@dataclass(frozen=True)
class ReconciliationState:
    observations: tuple[QuoteObservationForReconciliation, ...]
    awaiting_new_evidence_count: int
    completed_count: int


def load_reconciliation_state(
    archive: PointInTimeArchive,
    *,
    current_ledger_snapshot_id: str | None,
    current_schedule_snapshot_id: str | None,
) -> ReconciliationState:
    if not archive.manifest_path.exists():
        return ReconciliationState((), 0, 0)
    quote_rows: dict[str, QuoteObservationForReconciliation] = {}
    latest_reconciliations: dict[str, dict[str, object]] = {}
    for record in _manifest_records(archive):
        if record.get("source_id") != "internal_derived":
            continue
        dataset = str(record.get("dataset", ""))
        if dataset not in {
            "sec_filing_forward_observations",
            "sec_filing_total_return_observations",
        }:
            continue
        for row in _derived_rows(archive, record, dataset):
            key = _key(_required_text(row, "accession_number"), _required_text(row, "horizon"))
            if dataset == "sec_filing_total_return_observations":
                latest_reconciliations[key] = row
                continue
            if row.get("data_quality_status") != "pass_quote_price_only":
                continue
            benchmark_start = _required_mapping(row.get("benchmark_start"), "benchmark_start")
            benchmark_end = _required_mapping(row.get("benchmark_end"), "benchmark_end")
            returns = _required_mapping(row.get("return_observation"), "return_observation")
            cost_returns = _required_mapping(
                returns.get("stock_net_return_by_cost_bps"),
                "stock_net_return_by_cost_bps",
            )
            quote_rows[key] = QuoteObservationForReconciliation(
                accession_number=_required_text(row, "accession_number"),
                horizon=_required_text(row, "horizon"),
                symbol=_required_text(row, "symbol").upper(),
                first_tradable_at=_parse_aware_datetime(row.get("first_tradable_at")),
                target_at=_parse_aware_datetime(row.get("target_at")),
                stock_return=_required_float(
                    returns.get("stock_midpoint_price_return"),
                    "stock_midpoint_price_return",
                ),
                voo_start_midpoint=_positive_float(
                    benchmark_start.get("midpoint"), "benchmark_start.midpoint"
                ),
                voo_end_midpoint=_positive_float(
                    benchmark_end.get("midpoint"), "benchmark_end.midpoint"
                ),
                stock_net_return_by_cost_bps={
                    str(cost): _required_float(value, f"cost return {cost}")
                    for cost, value in cost_returns.items()
                },
                quote_observation_snapshot_id=_required_text(record, "snapshot_id"),
                quote_observation_retrieved_at=_parse_aware_datetime(record.get("retrieved_at")),
            )
    pending: list[QuoteObservationForReconciliation] = []
    awaiting = 0
    completed = 0
    for key, observation in quote_rows.items():
        prior = latest_reconciliations.get(key)
        if prior is not None and str(prior.get("data_quality_status", "")).startswith("pass"):
            completed += 1
            continue
        if prior is not None and (
            prior.get("ledger_snapshot_id") == current_ledger_snapshot_id
            and prior.get("official_schedule_snapshot_id") == current_schedule_snapshot_id
        ):
            awaiting += 1
            continue
        pending.append(observation)
    return ReconciliationState(
        tuple(sorted(pending, key=lambda item: (item.target_at, item.accession_number))),
        awaiting,
        completed,
    )


def collect(
    archive: PointInTimeArchive,
    *,
    total_return_policy_path: Path = DEFAULT_TOTAL_RETURN_POLICY,
) -> dict[str, object]:
    pre_audit = archive.audit()
    if pre_audit.status == "fail":
        raise ArchiveIntegrityError("Refusing total-return reconciliation after audit failure.")
    policy = load_total_return_policy(total_return_policy_path)
    ledger = load_latest_corporate_action_ledger(archive)
    official_schedule = load_latest_official_schedule(archive)
    state = load_reconciliation_state(
        archive,
        current_ledger_snapshot_id=None if ledger is None else ledger.snapshot_id,
        current_schedule_snapshot_id=(
            None if official_schedule is None else official_schedule.snapshot_id
        ),
    )
    if not state.observations:
        return _status_output(
            status="completed",
            rows=(),
            archive=archive,
            awaiting_new_evidence_count=state.awaiting_new_evidence_count,
            completed_count=state.completed_count,
        )
    rows: list[dict[str, object]] = []
    upstream_ids: set[str] = set()
    latest_evidence_at: datetime | None = None
    if ledger is not None:
        upstream_ids.add(ledger.snapshot_id)
        latest_evidence_at = ledger.retrieved_at
    if official_schedule is not None:
        upstream_ids.add(official_schedule.snapshot_id)
        latest_evidence_at = (
            official_schedule.retrieved_at
            if latest_evidence_at is None
            else max(latest_evidence_at, official_schedule.retrieved_at)
        )
    for observation in state.observations:
        result = calculate_voo_total_return(
            policy=policy,
            ledger=ledger,
            official_schedule=official_schedule,
            start_at=observation.first_tradable_at,
            end_at=observation.target_at,
            start_price=observation.voo_start_midpoint,
            end_price=observation.voo_end_midpoint,
        )
        upstream_ids.add(observation.quote_observation_snapshot_id)
        latest_evidence_at = (
            observation.quote_observation_retrieved_at
            if latest_evidence_at is None
            else max(latest_evidence_at, observation.quote_observation_retrieved_at)
        )
        base: dict[str, object] = {
            "observation_type": "sec_filing_voo_total_return_reconciliation",
            "accession_number": observation.accession_number,
            "horizon": observation.horizon,
            "symbol": observation.symbol,
            "benchmark_symbol": policy.symbol,
            "first_tradable_at": observation.first_tradable_at.isoformat(),
            "target_at": observation.target_at.isoformat(),
            "quote_observation_snapshot_id": observation.quote_observation_snapshot_id,
            "ledger_snapshot_id": result.ledger_snapshot_id,
            "official_schedule_snapshot_id": result.official_schedule_snapshot_id,
            "benchmark_total_return_convention": result.convention,
            "benchmark_price_return": result.price_return,
            "action_ids": list(result.action_ids),
            "market_observation_only": True,
            "execution_price_selected": False,
            "paper_fill_applied": False,
            "candidate_created": False,
            "alpha_calculated": False,
            "active_profile_changed": False,
            "research_only": True,
        }
        if result.total_return is None:
            rows.append(
                {
                    **base,
                    "data_quality_status": "blocked_total_return_integrity",
                    "exact_block_reason": result.exact_block_reason,
                    "voo_total_return": None,
                    "total_return_excess_observation": None,
                }
            )
            continue
        gross_excess = observation.stock_return - result.total_return
        rows.append(
            {
                **base,
                "data_quality_status": result.status,
                "exact_block_reason": None,
                "voo_total_return": result.total_return,
                "distribution_cash_per_initial_share": (result.distribution_cash_per_initial_share),
                "ending_shares_per_initial_share": result.ending_shares_per_initial_share,
                "total_return_excess_observation": {
                    "stock_midpoint_price_return": observation.stock_return,
                    "voo_holding_period_total_return": result.total_return,
                    "gross_total_excess_return": gross_excess,
                    "total_excess_return_by_cost_bps": {
                        cost: net_return - result.total_return
                        for cost, net_return in observation.stock_net_return_by_cost_bps.items()
                    },
                    "excess_return_calculated": True,
                    "alpha_calculated": False,
                    "not_execution_or_fill": True,
                },
            }
        )
    if latest_evidence_at is None:
        raise ArchiveIntegrityError("Total-return reconciliation has no upstream evidence time.")
    generated_at = max(datetime.now(UTC), latest_evidence_at)
    derived_payload = {
        "schema_version": "sec_filing_total_return_observations_v1",
        "generated_at": generated_at.isoformat(),
        "benchmark_symbol": policy.symbol,
        "benchmark_total_return_convention": policy.total_return_convention,
        "execution_price_selected": False,
        "paper_fill_applied": False,
        "candidate_created": False,
        "alpha_calculated": False,
        "rows": rows,
    }
    derived_record = archive.capture(
        source_id="internal_derived",
        dataset="sec_filing_total_return_observations",
        source_url="internal://observations/sec-filing-voo-total-return",
        payload=canonical_json_bytes(derived_payload),
        request_started_at=generated_at,
        retrieved_at=generated_at + timedelta(microseconds=1),
        decision_available_at=latest_evidence_at,
        decision_availability_basis="quote_action_and_official_schedule_evidence_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        request_parameters={"observation_count": len(rows)},
        response_metadata={
            "pass_count": sum(str(row["data_quality_status"]).startswith("pass") for row in rows),
            "blocked_count": sum(
                str(row["data_quality_status"]).startswith("blocked") for row in rows
            ),
        },
        upstream_snapshot_ids=tuple(sorted(upstream_ids)),
        integrity_notes=(
            (
                "Total-return excess is emitted only when independent schedule and action "
                "evidence pass."
            ),
            "Blocked reconciliations retain their exact reason and may retry after new evidence.",
            "Excess return is an observation, not validated alpha, an order, or a fill.",
        ),
    )
    return _status_output(
        status="captured",
        rows=rows,
        archive=archive,
        awaiting_new_evidence_count=state.awaiting_new_evidence_count,
        completed_count=state.completed_count,
        derived_record=derived_record,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICIES)
    parser.add_argument("--total-return-policy", type=Path, default=DEFAULT_TOTAL_RETURN_POLICY)
    args = parser.parse_args()
    archive = PointInTimeArchive(args.archive_root, load_source_policies(args.policies))
    output = collect(archive, total_return_policy_path=args.total_return_policy)
    latest = args.archive_root / "latest_sec_filing_total_return_reconciliation.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(
        "SEC filing VOO total-return reconciliation",
        output["status"],
        "passed",
        output["total_return_pass_count"],
        "blocked",
        output["total_return_blocked_count"],
    )


def _status_output(
    *,
    status: str,
    rows: tuple[()] | list[dict[str, object]],
    archive: PointInTimeArchive,
    awaiting_new_evidence_count: int,
    completed_count: int,
    derived_record: SnapshotRecord | None = None,
) -> dict[str, object]:
    audit = archive.audit()
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_only": True,
        "trading_actions_enabled": False,
        "active_profile_changed": False,
        "status": status if audit.status == "pass" else "integrity_audit_failed",
        "total_return_pass_count": sum(
            str(row.get("data_quality_status", "")).startswith("pass") for row in rows
        ),
        "total_return_blocked_count": sum(
            str(row.get("data_quality_status", "")).startswith("blocked") for row in rows
        ),
        "awaiting_new_evidence_count": awaiting_new_evidence_count,
        "previously_completed_count": completed_count,
        "reconciliations": list(rows),
        "derived_snapshot": None if derived_record is None else derived_record.to_dict(),
        "archive_audit": audit.to_dict(),
        "execution_price_selected": False,
        "paper_fill_applied": False,
        "candidate_created": False,
        "alpha_calculated": False,
    }


def _manifest_records(archive: PointInTimeArchive) -> tuple[dict[str, object], ...]:
    records: list[dict[str, object]] = []
    for line in archive.manifest_path.read_text().splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if not isinstance(record, dict):
            raise ArchiveIntegrityError("Archive manifest contains a non-object record.")
        records.append({str(key): value for key, value in record.items()})
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
    for raw in cast(list[object], payload["rows"]):
        if not isinstance(raw, dict):
            raise ArchiveIntegrityError(f"Archive dataset {dataset} contains an invalid row.")
        rows.append({str(key): value for key, value in raw.items()})
    return rows


def _required_mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ArchiveIntegrityError(f"Forward observation {label} is invalid.")
    return {str(key): item for key, item in value.items()}


def _required_text(values: Mapping[str, object], key: str) -> str:
    text = str(values.get(key, "")).strip()
    if not text:
        raise ArchiveIntegrityError(f"Forward observation is missing {key}.")
    return text


def _required_float(value: object, label: str) -> float:
    if value is None or isinstance(value, bool):
        raise ArchiveIntegrityError(f"Forward observation {label} is invalid.")
    return float(str(value))


def _positive_float(value: object, label: str) -> float:
    parsed = _required_float(value, label)
    if parsed <= 0:
        raise ArchiveIntegrityError(f"Forward observation {label} must be positive.")
    return parsed


def _parse_aware_datetime(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ArchiveIntegrityError(f"Invalid forward-observation timestamp: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ArchiveIntegrityError(f"Forward-observation timestamp lacks a timezone: {value}")
    return parsed


def _key(accession: str, horizon: str) -> str:
    return f"{accession}:{horizon}"


if __name__ == "__main__":
    main()
