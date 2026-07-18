"""Run the research-only high-integrity history collectors and final audit."""

from __future__ import annotations

import argparse
import json
import os
import socket
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import URLError
from zoneinfo import ZoneInfo

from ai_trading_bot.data.history_archive import PointInTimeArchive, load_source_policies

try:
    from scripts.calculate_sec_filing_total_returns import (
        collect as reconcile_sec_filing_total_returns,
    )
    from scripts.collect_alpaca_asset_history import collect as collect_alpaca_assets
    from scripts.collect_alpaca_market_calendar import (
        calendar_covers,
    )
    from scripts.collect_alpaca_market_calendar import (
        collect as collect_market_calendar,
    )
    from scripts.collect_sec_filing_documents import (
        collect as collect_sec_filing_documents,
    )
    from scripts.collect_sec_filing_documents import load_document_policy
    from scripts.collect_sec_filing_forward_observations import (
        collect as collect_forward_observations,
    )
    from scripts.collect_sec_filing_history import (
        WatchedIssuer,
        load_watchlist,
    )
    from scripts.collect_sec_filing_history import (
        collect as collect_sec_filings,
    )
    from scripts.collect_sec_insider_transactions import (
        collect as collect_sec_insider_transactions,
    )
    from scripts.collect_sec_latest_filings_discovery import (
        collect as collect_sec_latest_filings,
    )
    from scripts.collect_sec_latest_filings_discovery import load_discovery_policy
    from scripts.collect_security_master_history import collect as collect_security_master
    from scripts.collect_voo_corporate_actions import (
        collect as collect_voo_corporate_actions,
    )
    from scripts.collect_voo_corporate_actions import load_total_return_policy
    from scripts.plan_sec_filing_forward_observations import (
        collect as plan_forward_observations,
    )
    from scripts.plan_sec_filing_forward_observations import load_observation_policy
    from scripts.resolve_sec_filing_first_tradable import (
        collect as resolve_first_tradable,
    )
    from scripts.resolve_sec_filing_first_tradable import (
        load_timing_policy,
    )
    from scripts.trusted_data_research import trusted_data_environment
except ModuleNotFoundError:
    from calculate_sec_filing_total_returns import (  # type: ignore[import-not-found,no-redef]
        collect as reconcile_sec_filing_total_returns,
    )
    from collect_alpaca_asset_history import (  # type: ignore[import-not-found,no-redef]
        collect as collect_alpaca_assets,
    )
    from collect_alpaca_market_calendar import (  # type: ignore[import-not-found,no-redef]
        calendar_covers,
    )
    from collect_alpaca_market_calendar import (  # type: ignore[no-redef]
        collect as collect_market_calendar,
    )
    from collect_sec_filing_documents import (  # type: ignore[import-not-found,no-redef]
        collect as collect_sec_filing_documents,
    )
    from collect_sec_filing_documents import (  # type: ignore[no-redef]
        load_document_policy,
    )
    from collect_sec_filing_forward_observations import (  # type: ignore[import-not-found,no-redef]
        collect as collect_forward_observations,
    )
    from collect_sec_filing_history import (  # type: ignore[import-not-found,no-redef]
        WatchedIssuer,
        load_watchlist,
    )
    from collect_sec_filing_history import (  # type: ignore[no-redef]
        collect as collect_sec_filings,
    )
    from collect_sec_insider_transactions import (  # type: ignore[import-not-found,no-redef]
        collect as collect_sec_insider_transactions,
    )
    from collect_sec_latest_filings_discovery import (  # type: ignore[import-not-found,no-redef]
        collect as collect_sec_latest_filings,
    )
    from collect_sec_latest_filings_discovery import (  # type: ignore[no-redef]
        load_discovery_policy,
    )
    from collect_security_master_history import (  # type: ignore[import-not-found,no-redef]
        collect as collect_security_master,
    )
    from collect_voo_corporate_actions import (  # type: ignore[import-not-found,no-redef]
        collect as collect_voo_corporate_actions,
    )
    from collect_voo_corporate_actions import (  # type: ignore[no-redef]
        load_total_return_policy,
    )
    from plan_sec_filing_forward_observations import (  # type: ignore[import-not-found,no-redef]
        collect as plan_forward_observations,
    )
    from plan_sec_filing_forward_observations import (  # type: ignore[no-redef]
        load_observation_policy,
    )
    from resolve_sec_filing_first_tradable import (  # type: ignore[import-not-found,no-redef]
        collect as resolve_first_tradable,
    )
    from resolve_sec_filing_first_tradable import (  # type: ignore[no-redef]
        load_timing_policy,
    )
    from trusted_data_research import (  # type: ignore[import-not-found,no-redef]
        trusted_data_environment,
    )

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARCHIVE = ROOT / "outputs" / "high_integrity_history"
DEFAULT_POLICIES = ROOT / "config" / "data_source_policies.json"
DEFAULT_WATCHLIST = ROOT / "config" / "sec_filing_watchlist.json"
DEFAULT_TIMING_POLICY = ROOT / "config" / "market_timing_policy.json"
DEFAULT_DOCUMENT_POLICY = ROOT / "config" / "sec_filing_document_policy.json"
DEFAULT_OBSERVATION_POLICY = ROOT / "config" / "sec_filing_forward_observation_policy.json"
DEFAULT_TOTAL_RETURN_POLICY = ROOT / "config" / "voo_total_return_policy.json"
DEFAULT_DISCOVERY_POLICY = ROOT / "config" / "sec_latest_filings_discovery_policy.json"
MARKET_TIMEZONE = ZoneInfo("America/New_York")
TRANSIENT_NETWORK_RETRY_DELAYS_SECONDS = (2, 4)


def run_cycle(
    archive: PointInTimeArchive,
    *,
    environment: dict[str, str],
    issuers: tuple[WatchedIssuer, ...],
    include_security_master: bool = True,
    include_alpaca: bool = True,
    include_filings: bool = True,
    timing_policy_path: Path = DEFAULT_TIMING_POLICY,
    document_policy_path: Path = DEFAULT_DOCUMENT_POLICY,
    observation_policy_path: Path = DEFAULT_OBSERVATION_POLICY,
    total_return_policy_path: Path = DEFAULT_TOTAL_RETURN_POLICY,
    discovery_policy_path: Path = DEFAULT_DISCOVERY_POLICY,
) -> dict[str, object]:
    started_at = datetime.now(UTC)
    pre_audit = archive.audit()
    if pre_audit.status == "fail":
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "started_at": started_at.isoformat(),
            "research_only": True,
            "trading_actions_enabled": False,
            "active_profile_changed": False,
            "status": "integrity_audit_failed_before_collection",
            "steps": [],
            "archive_audit": pre_audit.to_dict(),
        }

    email = environment.get("SEC_USER_AGENT_EMAIL", "").strip()
    name = environment.get("SEC_USER_AGENT_NAME", "AI-TRADING-BOT/0.1").strip()
    user_agent = f"{name} {email}".strip()
    api_key = environment.get("AI_TRADING_MARKET_DATA_API_KEY", "").strip()
    api_secret = environment.get("AI_TRADING_MARKET_DATA_API_SECRET", "").strip()
    steps: list[dict[str, object]] = []
    today = datetime.now(MARKET_TIMEZONE).date()
    calendar_start = today - timedelta(days=7)
    calendar_end = today + timedelta(days=400)

    if include_security_master:
        if email:
            steps.append(
                _run_step(
                    "sec_security_master",
                    lambda: collect_security_master(archive, user_agent=user_agent),
                )
            )
        else:
            steps.append(_blocked_step("sec_security_master", "missing SEC_USER_AGENT_EMAIL"))
    if include_alpaca:
        if api_key and api_secret:
            steps.append(
                _run_step(
                    "alpaca_asset_history",
                    lambda: collect_alpaca_assets(
                        archive,
                        api_key=api_key,
                        api_secret=api_secret,
                    ),
                )
            )
            voo_policy = load_total_return_policy(total_return_policy_path)
            steps.append(
                _run_step(
                    "voo_corporate_actions",
                    lambda: collect_voo_corporate_actions(
                        archive,
                        api_key=api_key,
                        api_secret=api_secret,
                        policy=voo_policy,
                        start=today - timedelta(days=voo_policy.lookback_days),
                        end=today + timedelta(days=voo_policy.future_days),
                    ),
                )
            )
        else:
            steps.append(
                _blocked_step(
                    "alpaca_asset_history",
                    "missing data-only Alpaca credentials",
                )
            )
            steps.append(
                _blocked_step(
                    "voo_corporate_actions",
                    "missing data-only Alpaca credentials",
                )
            )
    if include_filings and api_key and api_secret:
        needs_calendar_capture = include_security_master or not calendar_covers(
            archive,
            start=today,
            end=today + timedelta(days=30),
        )
        if needs_calendar_capture:
            steps.append(
                _run_step(
                    "alpaca_market_calendar",
                    lambda: collect_market_calendar(
                        archive,
                        api_key=api_key,
                        api_secret=api_secret,
                        start=calendar_start,
                        end=calendar_end,
                    ),
                )
            )
        else:
            steps.append(_completed_step("alpaca_market_calendar", "current archive coverage"))
    if include_filings:
        if email:
            steps.append(
                _run_step(
                    "sec_latest_filings_discovery",
                    lambda: collect_sec_latest_filings(
                        archive,
                        user_agent=user_agent,
                        policy=load_discovery_policy(discovery_policy_path),
                    ),
                )
            )
            steps.append(
                _run_step(
                    "sec_filing_history",
                    lambda: collect_sec_filings(
                        archive,
                        user_agent=user_agent,
                        issuers=issuers,
                    ),
                )
            )
        else:
            steps.append(
                _blocked_step(
                    "sec_latest_filings_discovery",
                    "missing SEC_USER_AGENT_EMAIL",
                )
            )
            steps.append(_blocked_step("sec_filing_history", "missing SEC_USER_AGENT_EMAIL"))
        if email:
            steps.append(
                _run_step(
                    "sec_filing_documents",
                    lambda: collect_sec_filing_documents(
                        archive,
                        user_agent=user_agent,
                        policy=load_document_policy(document_policy_path),
                    ),
                )
            )
        else:
            steps.append(_blocked_step("sec_filing_documents", "missing SEC_USER_AGENT_EMAIL"))
        steps.append(
            _run_step(
                "sec_form4_open_market_transactions",
                lambda: collect_sec_insider_transactions(archive),
            )
        )
        if api_key and api_secret:
            steps.append(
                _run_step(
                    "sec_filing_first_tradable",
                    lambda: resolve_first_tradable(
                        archive,
                        api_key=api_key,
                        api_secret=api_secret,
                        policy=load_timing_policy(timing_policy_path),
                    ),
                )
            )
        else:
            steps.append(
                _blocked_step(
                    "sec_filing_first_tradable",
                    "missing data-only Alpaca credentials",
                )
            )
        steps.append(
            _run_step(
                "sec_filing_forward_observation_plan",
                lambda: plan_forward_observations(
                    archive,
                    policy=load_observation_policy(observation_policy_path),
                ),
            )
        )
        if api_key and api_secret:
            steps.append(
                _run_step(
                    "sec_filing_forward_observations",
                    lambda: collect_forward_observations(
                        archive,
                        api_key=api_key,
                        api_secret=api_secret,
                        policy=load_observation_policy(observation_policy_path),
                    ),
                )
            )
        else:
            steps.append(
                _blocked_step(
                    "sec_filing_forward_observations",
                    "missing data-only Alpaca credentials",
                )
            )
        steps.append(
            _run_step(
                "sec_filing_total_return_reconciliation",
                lambda: reconcile_sec_filing_total_returns(
                    archive,
                    total_return_policy_path=total_return_policy_path,
                ),
            )
        )

    final_audit = archive.audit()
    failed_steps = [step for step in steps if step.get("status") not in {"captured", "completed"}]
    if final_audit.status != "pass":
        status = "integrity_audit_failed"
    elif failed_steps:
        status = "partial_failure"
    else:
        status = "completed"
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "started_at": started_at.isoformat(),
        "research_only": True,
        "trading_actions_enabled": False,
        "active_profile_changed": False,
        "status": status,
        "steps": steps,
        "archive_audit": final_audit.to_dict(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICIES)
    parser.add_argument("--watchlist", type=Path, default=DEFAULT_WATCHLIST)
    parser.add_argument("--timing-policy", type=Path, default=DEFAULT_TIMING_POLICY)
    parser.add_argument("--document-policy", type=Path, default=DEFAULT_DOCUMENT_POLICY)
    parser.add_argument(
        "--observation-policy",
        type=Path,
        default=DEFAULT_OBSERVATION_POLICY,
    )
    parser.add_argument(
        "--total-return-policy",
        type=Path,
        default=DEFAULT_TOTAL_RETURN_POLICY,
    )
    parser.add_argument(
        "--discovery-policy",
        type=Path,
        default=DEFAULT_DISCOVERY_POLICY,
    )
    parser.add_argument(
        "--filings-only",
        action="store_true",
        help="Collect only SEC filing events; suitable for a more frequent schedule.",
    )
    parser.add_argument("--skip-alpaca", action="store_true")
    args = parser.parse_args()
    archive = PointInTimeArchive(args.archive_root, load_source_policies(args.policies))
    environment = trusted_data_environment(os.environ, ROOT / ".env")
    output = run_cycle(
        archive,
        environment=environment,
        issuers=load_watchlist(args.watchlist),
        include_security_master=not args.filings_only,
        include_alpaca=not args.filings_only and not args.skip_alpaca,
        include_filings=True,
        timing_policy_path=args.timing_policy,
        document_policy_path=args.document_policy,
        observation_policy_path=args.observation_policy,
        total_return_policy_path=args.total_return_policy,
        discovery_policy_path=args.discovery_policy,
    )
    latest = args.archive_root / "latest_collection_cycle.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(f"High-integrity collection cycle finished with status {output['status']}")
    print(json.dumps(output["archive_audit"], indent=2, sort_keys=True))
    if output["status"] not in {"completed"}:
        raise SystemExit(1)


def _run_step(name: str, operation: Callable[[], dict[str, object]]) -> dict[str, object]:
    started_at = datetime.now(UTC)
    transient_errors: list[str] = []
    for attempt in range(len(TRANSIENT_NETWORK_RETRY_DELAYS_SECONDS) + 1):
        try:
            result = operation()
        except Exception as exc:  # noqa: BLE001 - cycle must record independent collector failures
            if _is_transient_network_error(exc) and attempt < len(
                TRANSIENT_NETWORK_RETRY_DELAYS_SECONDS
            ):
                transient_errors.append(f"{type(exc).__name__}: {exc}")
                time.sleep(TRANSIENT_NETWORK_RETRY_DELAYS_SECONDS[attempt])
                continue
            failure: dict[str, object] = {
                "name": name,
                "started_at": started_at.isoformat(),
                "finished_at": datetime.now(UTC).isoformat(),
                "status": "failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "attempt_count": attempt + 1,
            }
            if transient_errors:
                failure["transient_retry_errors"] = transient_errors
            return failure
        step: dict[str, object] = {
            "name": name,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "status": str(result.get("status", "completed")),
            "summary": _step_summary(result),
            "attempt_count": attempt + 1,
        }
        if transient_errors:
            step["transient_retry_errors"] = transient_errors
        return step
    raise AssertionError("Network retry loop exhausted without a result.")


def _is_transient_network_error(exc: Exception) -> bool:
    if not isinstance(exc, URLError):
        return False
    reason = exc.reason
    if isinstance(reason, socket.gaierror):
        return True
    text = str(reason).casefold()
    return any(
        marker in text
        for marker in (
            "temporarily unavailable",
            "timed out",
            "connection reset",
            "connection refused",
            "network is unreachable",
            "nodename nor servname",
        )
    )


def _blocked_step(name: str, reason: str) -> dict[str, object]:
    now = datetime.now(UTC).isoformat()
    return {
        "name": name,
        "started_at": now,
        "finished_at": now,
        "status": "blocked",
        "reason": reason,
    }


def _completed_step(name: str, reason: str) -> dict[str, object]:
    now = datetime.now(UTC).isoformat()
    return {
        "name": name,
        "started_at": now,
        "finished_at": now,
        "status": "completed",
        "reason": reason,
    }


def _step_summary(result: dict[str, object]) -> dict[str, object]:
    summary_keys = (
        "security_observation_count",
        "asset_observation_count",
        "active_asset_count",
        "borrow_observation_count",
        "filing_event_count",
        "emitted_event_count",
        "reconfirmed_filing_count",
        "baseline_event_count",
        "new_monitored_event_count",
        "forward_event_eligible_count",
        "session_count",
        "pending_event_count",
        "resolved_event_count",
        "captured_package_count",
        "captured_document_count",
        "accepted_transaction_count",
        "rejected_filing_count",
        "planned_event_count",
        "awaiting_first_tradable_count",
        "observed_horizon_count",
        "blocked_horizon_count",
        "awaiting_future_horizon_count",
        "raw_quote_snapshot_count",
        "corporate_action_count",
        "cash_dividend_count",
        "split_count",
        "revision_count",
        "feed_entry_count",
        "discovery_emitted_count",
        "new_feed_entry_count",
        "confirmed_forward_event_count",
        "eligible_universe_symbol_count",
        "total_return_pass_count",
        "total_return_blocked_count",
        "awaiting_new_evidence_count",
    )
    return {key: result[key] for key in summary_keys if key in result}


if __name__ == "__main__":
    main()
