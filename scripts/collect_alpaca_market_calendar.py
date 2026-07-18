"""Archive Alpaca's holiday- and early-close-aware US equity calendar."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

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
ALPACA_CALENDAR_URL = "https://paper-api.alpaca.markets/v2/calendar"
MARKET_TIMEZONE = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class CalendarHttpCapture:
    payload: bytes
    request_started_at: datetime
    retrieved_at: datetime
    content_type: str
    response_metadata: dict[str, object]


@dataclass(frozen=True)
class CalendarSession:
    trading_date: date
    open_at: datetime
    close_at: datetime
    settlement_date: date | None
    source_snapshot_id: str


@dataclass(frozen=True)
class CalendarArchive:
    snapshot_id: str
    retrieved_at: datetime
    requested_start: date
    requested_end: date
    sessions: tuple[CalendarSession, ...]


def fetch_market_calendar(
    api_key: str,
    api_secret: str,
    *,
    start: date,
    end: date,
) -> CalendarHttpCapture:
    query = urlencode({"start": start.isoformat(), "end": end.isoformat()})
    started = datetime.now(UTC)
    request = Request(
        f"{ALPACA_CALENDAR_URL}?{query}",
        headers={
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret,
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=45) as response:  # noqa: S310 - fixed Alpaca endpoint
        payload = response.read()
        retrieved = datetime.now(UTC)
        metadata = {
            key: response.headers[key]
            for key in ("Date", "ETag", "Last-Modified", "Content-Length")
            if response.headers.get(key) is not None
        }
        metadata["http_status"] = response.status
        metadata["latency_ms"] = round((retrieved - started).total_seconds() * 1000, 3)
        return CalendarHttpCapture(
            payload=payload,
            request_started_at=started,
            retrieved_at=retrieved,
            content_type=response.headers.get_content_type(),
            response_metadata=metadata,
        )


def normalize_calendar_sessions(
    payload: bytes,
    *,
    source_snapshot_id: str,
) -> tuple[CalendarSession, ...]:
    parsed = json.loads(payload)
    if not isinstance(parsed, list):
        raise ValueError("Alpaca calendar response must be a JSON array.")
    sessions: list[CalendarSession] = []
    seen_dates: set[date] = set()
    for raw in parsed:
        if not isinstance(raw, dict):
            raise ValueError("Alpaca calendar response contains a non-object row.")
        trading_date = _parse_date(raw.get("date"), "date")
        if trading_date in seen_dates:
            raise ValueError(f"Alpaca calendar repeats trading date {trading_date}.")
        seen_dates.add(trading_date)
        open_at = datetime.combine(
            trading_date,
            _parse_clock(raw.get("open"), "open"),
            tzinfo=MARKET_TIMEZONE,
        )
        close_at = datetime.combine(
            trading_date,
            _parse_clock(raw.get("close"), "close"),
            tzinfo=MARKET_TIMEZONE,
        )
        if close_at <= open_at:
            raise ValueError(f"Alpaca calendar session {trading_date} closes before it opens.")
        settlement_raw = raw.get("settlement_date")
        settlement_date = (
            None
            if settlement_raw in (None, "")
            else _parse_date(settlement_raw, "settlement_date")
        )
        sessions.append(
            CalendarSession(
                trading_date=trading_date,
                open_at=open_at,
                close_at=close_at,
                settlement_date=settlement_date,
                source_snapshot_id=source_snapshot_id,
            )
        )
    return tuple(sorted(sessions, key=lambda session: session.trading_date))


def collect(
    archive: PointInTimeArchive,
    *,
    api_key: str,
    api_secret: str,
    start: date,
    end: date,
    http_capture: CalendarHttpCapture | None = None,
) -> dict[str, object]:
    if end < start:
        raise ValueError("Calendar end date cannot precede start date.")
    pre_audit = archive.audit()
    if pre_audit.status == "fail":
        raise ArchiveIntegrityError("Refusing calendar collection because archive audit failed.")
    capture = http_capture or fetch_market_calendar(
        api_key,
        api_secret,
        start=start,
        end=end,
    )
    query = urlencode({"start": start.isoformat(), "end": end.isoformat()})
    raw_record = archive.capture(
        source_id="alpaca_market_data",
        dataset="us_equity_trading_calendar",
        source_url=f"{ALPACA_CALENDAR_URL}?{query}",
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
            "start": start.isoformat(),
            "end": end.isoformat(),
            "api_key": api_key,
            "api_secret": api_secret,
        },
        response_metadata=capture.response_metadata,
        integrity_notes=(
            "Calendar includes provider-observed holidays and early closes.",
            "Raw and reversible calendar data remains private under Alpaca policy.",
            "Regular-hours sessions only; extended and overnight sessions are excluded.",
        ),
    )
    sessions = normalize_calendar_sessions(
        capture.payload,
        source_snapshot_id=raw_record.snapshot_id,
    )
    derived_payload = {
        "schema_version": "market_calendar_sessions_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "market_timezone": "America/New_York",
        "session_scope": "regular_hours_only",
        "source_snapshot_id": raw_record.snapshot_id,
        "source_sha256": raw_record.raw_sha256,
        "redistribution_allowed": False,
        "public_gui": "permission_required",
        "rows": [
            {
                "trading_date": session.trading_date.isoformat(),
                "session_open_at": session.open_at.isoformat(),
                "session_close_at": session.close_at.isoformat(),
                "settlement_date": (
                    None
                    if session.settlement_date is None
                    else session.settlement_date.isoformat()
                ),
                "source_snapshot_id": session.source_snapshot_id,
            }
            for session in sessions
        ],
    }
    derived_started_at = datetime.now(UTC)
    derived_record = archive.capture(
        source_id="internal_derived",
        dataset="market_calendar_sessions",
        source_url="internal://market-timing/alpaca-calendar",
        payload=canonical_json_bytes(derived_payload),
        request_started_at=derived_started_at,
        retrieved_at=datetime.now(UTC),
        decision_available_at=capture.retrieved_at,
        decision_availability_basis="upstream_first_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        request_parameters={"parent_snapshot_id": raw_record.snapshot_id},
        response_metadata={"session_count": len(sessions)},
        upstream_snapshot_ids=(raw_record.snapshot_id,),
        integrity_notes=(
            "Session boundaries are timezone-aware and preserve early closes.",
            "The upstream Alpaca redistribution restriction applies to this snapshot.",
        ),
    )
    audit = archive.audit()
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_only": True,
        "active_profile_changed": False,
        "status": "captured" if audit.status == "pass" else "integrity_audit_failed",
        "raw_snapshot": raw_record.to_dict(),
        "derived_snapshot": derived_record.to_dict(),
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "session_count": len(sessions),
        "archive_audit": audit.to_dict(),
        "redistribution": "blocked_without_written_permission",
    }


def load_latest_calendar(archive: PointInTimeArchive) -> CalendarArchive | None:
    if not archive.manifest_path.exists():
        return None
    for line in reversed(archive.manifest_path.read_text().splitlines()):
        if not line.strip():
            continue
        record = json.loads(line)
        if not isinstance(record, dict):
            raise ArchiveIntegrityError("Archive manifest contains a non-object record.")
        if (
            record.get("source_id") != "internal_derived"
            or record.get("dataset") != "market_calendar_sessions"
        ):
            continue
        raw_path = archive.root / str(record.get("raw_path", ""))
        payload = json.loads(raw_path.read_text())
        if not isinstance(payload, dict) or not isinstance(payload.get("rows"), list):
            raise ArchiveIntegrityError("Market calendar snapshot has an invalid schema.")
        sessions: list[CalendarSession] = []
        for row in payload["rows"]:
            if not isinstance(row, dict):
                raise ArchiveIntegrityError("Market calendar snapshot contains an invalid row.")
            sessions.append(
                CalendarSession(
                    trading_date=_parse_date(row.get("trading_date"), "trading_date"),
                    open_at=_parse_aware_datetime(row.get("session_open_at")),
                    close_at=_parse_aware_datetime(row.get("session_close_at")),
                    settlement_date=(
                        None
                        if row.get("settlement_date") is None
                        else _parse_date(row.get("settlement_date"), "settlement_date")
                    ),
                    source_snapshot_id=str(row.get("source_snapshot_id", "")),
                )
            )
        return CalendarArchive(
            snapshot_id=str(record["snapshot_id"]),
            retrieved_at=_parse_aware_datetime(record.get("retrieved_at")),
            requested_start=_parse_date(payload.get("requested_start"), "requested_start"),
            requested_end=_parse_date(payload.get("requested_end"), "requested_end"),
            sessions=tuple(sessions),
        )
    return None


def calendar_covers(archive: PointInTimeArchive, *, start: date, end: date) -> bool:
    calendar = load_latest_calendar(archive)
    return (
        calendar is not None
        and calendar.requested_start <= start
        and calendar.requested_end >= end
    )


def main() -> None:
    today = datetime.now(MARKET_TIMEZONE).date()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICIES)
    parser.add_argument("--start", type=date.fromisoformat, default=today - timedelta(days=7))
    parser.add_argument("--end", type=date.fromisoformat, default=today + timedelta(days=400))
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
        start=args.start,
        end=args.end,
    )
    latest = args.archive_root / "latest_market_calendar_collection.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(f"High-integrity market calendar captured in {args.archive_root}")
    print("status", output["status"], "sessions", output["session_count"])


def _parse_date(value: object, label: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"Invalid calendar {label}: {value}") from exc


def _parse_clock(value: object, label: str) -> time:
    try:
        return time.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise ValueError(f"Invalid calendar {label}: {value}") from exc


def _parse_aware_datetime(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid aware timestamp: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"Timestamp is not timezone-aware: {value}")
    return parsed


if __name__ == "__main__":
    main()
