"""Run a locked high-integrity collection cycle for a local scheduler."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from ai_trading_bot.data.history_archive import PointInTimeArchive, load_source_policies

try:
    from scripts.collect_sec_filing_history import load_watchlist
    from scripts.run_high_integrity_history_cycle import (
        DEFAULT_ARCHIVE,
        DEFAULT_POLICIES,
        DEFAULT_TIMING_POLICY,
        DEFAULT_WATCHLIST,
        run_cycle,
    )
    from scripts.trusted_data_research import trusted_data_environment
except ModuleNotFoundError:
    from collect_sec_filing_history import (  # type: ignore[import-not-found,no-redef]
        load_watchlist,
    )
    from run_high_integrity_history_cycle import (  # type: ignore[import-not-found,no-redef]
        DEFAULT_ARCHIVE,
        DEFAULT_POLICIES,
        DEFAULT_TIMING_POLICY,
        DEFAULT_WATCHLIST,
        run_cycle,
    )
    from trusted_data_research import (  # type: ignore[import-not-found,no-redef]
        trusted_data_environment,
    )

ROOT = Path(__file__).resolve().parents[1]
MARKET_TIMEZONE = ZoneInfo("America/New_York")
SCHEDULE_MODES = {"filings", "daily"}


def should_run(mode: str, now: datetime) -> tuple[bool, str]:
    if mode not in SCHEDULE_MODES:
        raise ValueError(f"Unsupported schedule mode: {mode}")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Scheduler time must be timezone-aware.")
    local_now = now.astimezone(MARKET_TIMEZONE)
    if mode == "filings" and local_now.weekday() >= 5:
        return False, "weekend"
    return True, "due"


def run_scheduled(
    *,
    mode: str,
    archive_root: Path,
    policies_path: Path,
    watchlist_path: Path,
    timing_policy_path: Path,
    environment: dict[str, str],
    now: datetime | None = None,
) -> dict[str, object]:
    effective_now = datetime.now(UTC) if now is None else now
    due, reason = should_run(mode, effective_now)
    started_at = datetime.now(UTC)
    scheduler_root = archive_root / "scheduler"
    scheduler_root.mkdir(parents=True, exist_ok=True)
    if not due:
        output = _scheduler_output(
            mode=mode,
            started_at=started_at,
            status="skipped",
            reason=reason,
            cycle=None,
        )
        _write_scheduler_status(scheduler_root, mode, output)
        return output

    lock_path = scheduler_root / "collection.lock"
    with lock_path.open("a+", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            output = _scheduler_output(
                mode=mode,
                started_at=started_at,
                status="skipped",
                reason="collection_already_running",
                cycle=None,
            )
            _write_scheduler_status(scheduler_root, mode, output)
            return output

        try:
            archive = PointInTimeArchive(
                archive_root,
                load_source_policies(policies_path),
            )
            cycle = run_cycle(
                archive,
                environment=environment,
                issuers=load_watchlist(watchlist_path),
                include_security_master=mode == "daily",
                include_alpaca=mode == "daily",
                include_filings=True,
                timing_policy_path=timing_policy_path,
            )
            cycle_path = archive_root / "latest_collection_cycle.json"
            cycle_path.write_text(json.dumps(cycle, indent=2, sort_keys=True) + "\n")
            status = "completed" if cycle.get("status") == "completed" else "failed"
            output = _scheduler_output(
                mode=mode,
                started_at=started_at,
                status=status,
                reason=None,
                cycle=cycle,
            )
        except Exception as exc:  # noqa: BLE001 - persist scheduler failures for diagnosis
            output = _scheduler_output(
                mode=mode,
                started_at=started_at,
                status="failed",
                reason=f"{type(exc).__name__}: {exc}",
                cycle=None,
            )
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    _write_scheduler_status(scheduler_root, mode, output)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=sorted(SCHEDULE_MODES), required=True)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICIES)
    parser.add_argument("--watchlist", type=Path, default=DEFAULT_WATCHLIST)
    parser.add_argument("--timing-policy", type=Path, default=DEFAULT_TIMING_POLICY)
    args = parser.parse_args()
    environment = trusted_data_environment(os.environ, ROOT / ".env")
    output = run_scheduled(
        mode=args.mode,
        archive_root=args.archive_root,
        policies_path=args.policies,
        watchlist_path=args.watchlist,
        timing_policy_path=args.timing_policy,
        environment=environment,
    )
    print(json.dumps(output, indent=2, sort_keys=True))
    if output["status"] == "failed":
        raise SystemExit(1)


def _scheduler_output(
    *,
    mode: str,
    started_at: datetime,
    status: str,
    reason: str | None,
    cycle: dict[str, object] | None,
) -> dict[str, object]:
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "started_at": started_at.isoformat(),
        "mode": mode,
        "research_only": True,
        "trading_actions_enabled": False,
        "active_profile_changed": False,
        "status": status,
        "reason": reason,
        "cycle": cycle,
    }


def _write_scheduler_status(
    scheduler_root: Path,
    mode: str,
    output: dict[str, object],
) -> None:
    path = scheduler_root / f"latest_{mode}_schedule.json"
    path.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
