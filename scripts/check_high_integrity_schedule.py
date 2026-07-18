"""Check local collection schedules, archive integrity, and weekly research status."""

from __future__ import annotations

import json
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from ai_trading_bot.data.history_archive import PointInTimeArchive, load_source_policies

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = ROOT / "outputs" / "high_integrity_history"
POLICIES = ROOT / "config" / "data_source_policies.json"
AUTOMATION = (
    Path.home()
    / ".codex"
    / "automations"
    / "weekly-edge-strategy-research"
    / "automation.toml"
)
MARKET_TIMEZONE = ZoneInfo("America/New_York")


def main() -> None:
    now = datetime.now(UTC)
    archive = PointInTimeArchive(ARCHIVE, load_source_policies(POLICIES))
    audit = archive.audit()
    filing = _scheduled_status("filings", now)
    daily = _scheduled_status("daily", now)
    weekly = _weekly_status()
    checks = {
        "archive": {
            "status": audit.status,
            "manifest_records": audit.manifest_records,
            "issues": list(audit.issues),
        },
        "filing_monitor": filing,
        "daily_snapshot": daily,
        "weekly_research": weekly,
    }
    healthy = (
        audit.status == "pass"
        and filing["healthy"] is True
        and daily["healthy"] is True
        and weekly["healthy"] is True
    )
    output = {
        "generated_at": now.isoformat(),
        "research_only": True,
        "trading_actions_enabled": False,
        "active_profile_changed": False,
        "status": "healthy" if healthy else "attention_required",
        "checks": checks,
    }
    print(json.dumps(output, indent=2, sort_keys=True))
    if not healthy:
        raise SystemExit(1)


def _scheduled_status(mode: str, now: datetime) -> dict[str, object]:
    path = ARCHIVE / "scheduler" / f"latest_{mode}_schedule.json"
    if not path.exists():
        return {"healthy": False, "status": "missing", "path": str(path)}
    payload = json.loads(path.read_text())
    generated_at = _parse_datetime(payload.get("generated_at"))
    age = now - generated_at.astimezone(UTC)
    if mode == "filings" and now.astimezone(MARKET_TIMEZONE).weekday() >= 5:
        healthy = payload.get("status") in {"completed", "skipped"} and age <= timedelta(days=3)
        due_status = "not_due_weekend"
    else:
        maximum_age = timedelta(minutes=15) if mode == "filings" else timedelta(hours=36)
        healthy = payload.get("status") == "completed" and age <= maximum_age
        due_status = "due"
    return {
        "healthy": healthy,
        "status": payload.get("status"),
        "due_status": due_status,
        "generated_at": generated_at.isoformat(),
        "age_seconds": age.total_seconds(),
        "path": str(path),
    }


def _weekly_status() -> dict[str, object]:
    if not AUTOMATION.exists():
        return {"healthy": False, "status": "missing", "path": str(AUTOMATION)}
    payload = tomllib.loads(AUTOMATION.read_text())
    active = payload.get("status") == "ACTIVE"
    weekly = str(payload.get("rrule", "")).startswith("FREQ=WEEKLY;")
    return {
        "healthy": active and weekly,
        "status": payload.get("status"),
        "rrule": payload.get("rrule"),
        "path": str(AUTOMATION),
    }


def _parse_datetime(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid scheduler timestamp: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"Scheduler timestamp is not timezone-aware: {value}")
    return parsed


if __name__ == "__main__":
    main()
