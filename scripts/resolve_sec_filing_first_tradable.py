"""Resolve forward SEC events to the first valid regular-session SIP quote."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ai_trading_bot.data.history_archive import (
    ArchiveIntegrityError,
    PointInTimeArchive,
    canonical_json_bytes,
    load_source_policies,
)

try:
    from scripts.collect_alpaca_market_calendar import (
        CalendarSession,
        load_latest_calendar,
    )
    from scripts.trusted_data_research import trusted_data_environment
except ModuleNotFoundError:
    from collect_alpaca_market_calendar import (  # type: ignore[import-not-found,no-redef]
        CalendarSession,
        load_latest_calendar,
    )
    from trusted_data_research import (  # type: ignore[import-not-found,no-redef]
        trusted_data_environment,
    )

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARCHIVE = ROOT / "outputs" / "high_integrity_history"
DEFAULT_POLICIES = ROOT / "config" / "data_source_policies.json"
DEFAULT_TIMING_POLICY = ROOT / "config" / "market_timing_policy.json"
ALPACA_QUOTES_ROOT = "https://data.alpaca.markets/v2/stocks"


@dataclass(frozen=True)
class MarketTimingPolicy:
    quote_feed: str
    delay_buffer_seconds: int
    quote_page_limit: int
    max_quote_pages_per_session: int
    max_sessions_per_event: int


@dataclass(frozen=True)
class FilingEvent:
    accession_number: str
    cik: str
    symbol: str
    decision_available_at: datetime
    event_source_snapshot_id: str


@dataclass(frozen=True)
class EligibleSession:
    session: CalendarSession
    eligible_start_at: datetime


@dataclass(frozen=True)
class QuoteHttpCapture:
    payload: bytes
    source_url: str
    request_started_at: datetime
    retrieved_at: datetime
    content_type: str
    response_metadata: dict[str, object]
    request_parameters: dict[str, object]


@dataclass(frozen=True)
class QuoteObservation:
    timestamp_text: str
    timestamp: datetime
    bid_price: float
    ask_price: float
    bid_size: int
    ask_size: int
    bid_exchange: str
    ask_exchange: str
    conditions: tuple[str, ...]
    tape: str | None


def load_timing_policy(path: Path) -> MarketTimingPolicy:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict) or payload.get("schema_version") != "market_timing_policy_v1":
        raise ValueError("Market timing policy has an unsupported schema.")
    if payload.get("session_scope") != "regular_hours_only":
        raise ValueError("Only the regular-hours timing policy is supported.")
    if payload.get("quote_feed") != "sip":
        raise ValueError("First-tradable resolution requires the consolidated SIP feed.")
    if payload.get("execution_price_selected") is not False:
        raise ValueError("Market timing policy must not select an execution price.")
    if payload.get("paper_fill_applied") is not False:
        raise ValueError("Market timing policy must not apply paper fills.")
    return MarketTimingPolicy(
        quote_feed="sip",
        delay_buffer_seconds=_positive_int(
            payload.get("historical_quote_delay_buffer_seconds"),
            "historical_quote_delay_buffer_seconds",
        ),
        quote_page_limit=_positive_int(payload.get("quote_page_limit"), "quote_page_limit"),
        max_quote_pages_per_session=_positive_int(
            payload.get("max_quote_pages_per_session"),
            "max_quote_pages_per_session",
        ),
        max_sessions_per_event=_positive_int(
            payload.get("max_sessions_per_event"),
            "max_sessions_per_event",
        ),
    )


def load_forward_filing_events(archive: PointInTimeArchive) -> tuple[FilingEvent, ...]:
    if not archive.manifest_path.exists():
        return ()
    events: dict[str, FilingEvent] = {}
    insider_events: dict[str, FilingEvent] = {}
    resolved: set[str] = set()
    for line in archive.manifest_path.read_text().splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if not isinstance(record, dict):
            raise ArchiveIntegrityError("Archive manifest contains a non-object record.")
        dataset = record.get("dataset")
        if record.get("source_id") != "internal_derived" or dataset not in {
            "sec_filing_events",
            "sec_insider_transaction_events",
            "sec_filing_first_tradable_resolutions",
        }:
            continue
        payload = json.loads((archive.root / str(record.get("raw_path", ""))).read_text())
        if not isinstance(payload, dict) or not isinstance(payload.get("rows"), list):
            raise ArchiveIntegrityError(f"Archive dataset {dataset} has an invalid schema.")
        for row in payload["rows"]:
            if not isinstance(row, dict):
                raise ArchiveIntegrityError(f"Archive dataset {dataset} contains an invalid row.")
            accession = str(row.get("accession_number", "")).strip()
            if not accession:
                raise ArchiveIntegrityError(f"Archive dataset {dataset} row lacks accession.")
            if dataset == "sec_filing_first_tradable_resolutions":
                resolved.add(accession)
                continue
            if dataset == "sec_insider_transaction_events":
                if row.get("forward_event_eligible") is not True:
                    continue
                decision_available_at = _parse_aware_datetime(row.get("decision_available_at"))
                insider_events[accession] = FilingEvent(
                    accession_number=accession,
                    cik=str(row.get("cik", "")),
                    symbol=str(row.get("symbol", "")).upper(),
                    decision_available_at=decision_available_at,
                    event_source_snapshot_id=str(record.get("snapshot_id", "")),
                )
                continue
            if (
                row.get("first_observation_class") != "monitored_new_accession"
                or row.get("forward_event_eligible") is not True
            ):
                continue
            if str(row.get("form", "")).upper() in {"4", "4/A"}:
                # Form 4 is eligible only when the conservative XML ledger has
                # confirmed at least one clear open-market P/S transaction.
                continue
            events[accession] = FilingEvent(
                accession_number=accession,
                cik=str(row.get("cik", "")),
                symbol=str(row.get("symbol", "")).upper(),
                decision_available_at=_parse_aware_datetime(row.get("decision_available_at")),
                event_source_snapshot_id=str(row.get("source_snapshot_id", "")),
            )
    events.update(insider_events)
    return tuple(
        sorted(
            (event for accession, event in events.items() if accession not in resolved),
            key=lambda event: (event.decision_available_at, event.accession_number),
        )
    )


def eligible_sessions(
    event: FilingEvent,
    sessions: tuple[CalendarSession, ...],
) -> tuple[EligibleSession, ...]:
    eligible: list[EligibleSession] = []
    for session in sessions:
        if session.close_at <= event.decision_available_at:
            continue
        start = max(session.open_at, event.decision_available_at)
        if start < session.close_at:
            eligible.append(EligibleSession(session=session, eligible_start_at=start))
    return tuple(eligible)


def select_first_valid_quote(
    payload: bytes,
    *,
    symbol: str,
    earliest_at: datetime,
    latest_at: datetime,
) -> QuoteObservation | None:
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise ValueError("Alpaca quotes response must be a JSON object.")
    response_symbol = str(parsed.get("symbol", "")).upper()
    if response_symbol != symbol.upper():
        raise ValueError(
            f"Alpaca quote symbol mismatch: expected {symbol.upper()}, got {response_symbol}."
        )
    quotes = parsed.get("quotes")
    if not isinstance(quotes, list):
        raise ValueError("Alpaca quotes response is missing a quotes list.")
    candidates: list[QuoteObservation] = []
    for raw in quotes:
        if not isinstance(raw, dict):
            raise ValueError("Alpaca quotes response contains a non-object quote.")
        observation = _quote_observation(raw)
        if observation is None:
            continue
        if earliest_at <= observation.timestamp <= latest_at:
            candidates.append(observation)
    if not candidates:
        return None
    return min(candidates, key=lambda quote: quote.timestamp)


def fetch_quote_page(
    api_key: str,
    api_secret: str,
    *,
    symbol: str,
    start: datetime,
    end: datetime,
    asof: date,
    feed: str,
    limit: int,
    page_token: str | None = None,
) -> QuoteHttpCapture:
    query: dict[str, str] = {
        "start": _rfc3339(start),
        "end": _rfc3339(end),
        "feed": feed,
        "limit": str(limit),
        "sort": "asc",
        "asof": asof.isoformat(),
    }
    if page_token is not None:
        query["page_token"] = page_token
    source_url = f"{ALPACA_QUOTES_ROOT}/{symbol}/quotes?{urlencode(query)}"
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
            )
            if response.headers.get(key) is not None
        }
        metadata["http_status"] = response.status
        metadata["latency_ms"] = round((retrieved - started).total_seconds() * 1000, 3)
        return QuoteHttpCapture(
            payload=payload,
            source_url=source_url,
            request_started_at=started,
            retrieved_at=retrieved,
            content_type=response.headers.get_content_type(),
            response_metadata=metadata,
            request_parameters={**query, "api_key": api_key, "api_secret": api_secret},
        )


def collect(
    archive: PointInTimeArchive,
    *,
    api_key: str,
    api_secret: str,
    policy: MarketTimingPolicy,
    now: datetime | None = None,
    quote_captures: Mapping[str, tuple[QuoteHttpCapture, ...]] | None = None,
) -> dict[str, object]:
    pre_audit = archive.audit()
    if pre_audit.status == "fail":
        raise ArchiveIntegrityError("Refusing first-tradable resolution after audit failure.")
    calendar = load_latest_calendar(archive)
    if calendar is None:
        return _status_output("blocked_missing_market_calendar", 0, [], archive)
    events = load_forward_filing_events(archive)
    if not events:
        return _status_output("completed", 0, [], archive)
    effective_now = datetime.now(UTC) if now is None else _require_aware(now, "now")
    observable_cutoff = effective_now - timedelta(seconds=policy.delay_buffer_seconds)
    resolutions: list[dict[str, object]] = []
    pending: list[dict[str, object]] = []
    all_upstream_ids: set[str] = {calendar.snapshot_id}

    for event in events:
        sessions = eligible_sessions(event, calendar.sessions)[: policy.max_sessions_per_event]
        if not sessions:
            pending.append(_pending(event, "calendar_has_no_session_after_decision"))
            continue
        evidence_snapshot_ids: list[str] = []
        resolved = False
        injected_pages = (
            iter(quote_captures.get(event.accession_number, ()))
            if quote_captures is not None
            else None
        )
        for eligible in sessions:
            query_end = min(eligible.session.close_at, observable_cutoff)
            if query_end <= eligible.eligible_start_at:
                pending.append(_pending(event, "waiting_for_sip_delay_or_session_open"))
                break
            page_token: str | None = None
            for _page_number in range(policy.max_quote_pages_per_session):
                if injected_pages is None:
                    capture = fetch_quote_page(
                        api_key,
                        api_secret,
                        symbol=event.symbol,
                        start=eligible.eligible_start_at,
                        end=query_end,
                        asof=eligible.session.trading_date,
                        feed=policy.quote_feed,
                        limit=policy.quote_page_limit,
                        page_token=page_token,
                    )
                else:
                    try:
                        capture = next(injected_pages)
                    except StopIteration:
                        break
                raw_record = archive.capture(
                    source_id="alpaca_market_data",
                    dataset=f"sip_quotes_{event.symbol.lower()}",
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
                    request_parameters=capture.request_parameters,
                    response_metadata=capture.response_metadata,
                    integrity_notes=(
                        "Historical SIP quote response is retained as private timing evidence.",
                        (
                            "Quote event timestamps and collector retrieval timestamps remain "
                            "separate."
                        ),
                        "No order or paper fill is created from this observation.",
                    ),
                )
                evidence_snapshot_ids.append(raw_record.snapshot_id)
                all_upstream_ids.add(raw_record.snapshot_id)
                quote = select_first_valid_quote(
                    capture.payload,
                    symbol=event.symbol,
                    earliest_at=eligible.eligible_start_at,
                    latest_at=query_end,
                )
                if quote is not None:
                    quote_delay_seconds = (
                        capture.retrieved_at - quote.timestamp.astimezone(UTC)
                    ).total_seconds()
                    resolutions.append(
                        {
                            "observation_type": "sec_filing_first_tradable_resolution",
                            "accession_number": event.accession_number,
                            "cik": event.cik,
                            "symbol": event.symbol,
                            "decision_available_at": event.decision_available_at.isoformat(),
                            "calendar_session_date": eligible.session.trading_date.isoformat(),
                            "session_open_at": eligible.session.open_at.isoformat(),
                            "session_close_at": eligible.session.close_at.isoformat(),
                            "eligible_quote_start_at": eligible.eligible_start_at.isoformat(),
                            "first_tradable_at": quote.timestamp_text,
                            "first_tradable_basis": (
                                "first_valid_positive_two_sided_sip_quote_at_or_after_eligible_start"
                            ),
                            "quote_event_timestamp_quality": "provider_nanosecond_exact",
                            "quote_retrieved_at": capture.retrieved_at.isoformat(),
                            "quote_availability_delay_seconds": quote_delay_seconds,
                            "quote_feed": policy.quote_feed,
                            "bid_price": quote.bid_price,
                            "ask_price": quote.ask_price,
                            "bid_size": quote.bid_size,
                            "ask_size": quote.ask_size,
                            "bid_exchange": quote.bid_exchange,
                            "ask_exchange": quote.ask_exchange,
                            "quote_conditions": list(quote.conditions),
                            "tape": quote.tape,
                            "spread_bps": (
                                (quote.ask_price - quote.bid_price)
                                / ((quote.ask_price + quote.bid_price) / 2)
                                * 10_000
                            ),
                            "event_source_snapshot_id": event.event_source_snapshot_id,
                            "calendar_snapshot_id": calendar.snapshot_id,
                            "quote_evidence_snapshot_ids": list(evidence_snapshot_ids),
                            "market_observation_only": True,
                            "execution_price_selected": False,
                            "paper_fill_applied": False,
                            "raw_redistribution_allowed": False,
                            "public_gui": "permission_required",
                            "active_profile_changed": False,
                        }
                    )
                    all_upstream_ids.add(event.event_source_snapshot_id)
                    resolved = True
                    break
                page_token = _next_page_token(capture.payload)
                if page_token is None or injected_pages is not None:
                    break
            if resolved:
                break
            if observable_cutoff < eligible.session.close_at:
                pending.append(_pending(event, "no_valid_quote_yet_in_mature_window"))
                break
        if not resolved and not any(
            item["accession_number"] == event.accession_number for item in pending
        ):
            pending.append(_pending(event, "no_valid_quote_in_checked_sessions"))

    derived_record = None
    if resolutions:
        derived_payload = {
            "schema_version": "sec_filing_first_tradable_resolutions_v1",
            "generated_at": datetime.now(UTC).isoformat(),
            "session_scope": "regular_hours_only",
            "quote_feed": policy.quote_feed,
            "quote_delay_buffer_seconds": policy.delay_buffer_seconds,
            "raw_redistribution_allowed": False,
            "public_gui": "permission_required",
            "rows": resolutions,
        }
        started_at = datetime.now(UTC)
        derived_record = archive.capture(
            source_id="internal_derived",
            dataset="sec_filing_first_tradable_resolutions",
            source_url="internal://market-timing/sec-filing-first-tradable",
            payload=canonical_json_bytes(derived_payload),
            request_started_at=started_at,
            retrieved_at=datetime.now(UTC),
            decision_available_at=max(
                _parse_aware_datetime(row["quote_retrieved_at"]) for row in resolutions
            ),
            decision_availability_basis="quote_response_first_observed",
            market_timezone="America/New_York",
            timestamp_quality="first_seen_exact",
            research_use="forward_only",
            content_type="application/json",
            request_parameters={"event_count": len(resolutions)},
            response_metadata={"resolution_count": len(resolutions)},
            upstream_snapshot_ids=tuple(sorted(all_upstream_ids)),
            integrity_notes=(
                "First tradable is a market-timing observation, not an order or fill.",
                "Quote prices and reversible timing evidence inherit Alpaca restrictions.",
                "Baseline SEC events are categorically excluded from resolution.",
            ),
        )
    audit = archive.audit()
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_only": True,
        "trading_actions_enabled": False,
        "active_profile_changed": False,
        "status": "captured" if resolutions and audit.status == "pass" else "completed",
        "pending_event_count": len(pending),
        "resolved_event_count": len(resolutions),
        "pending": pending,
        "resolutions": resolutions,
        "derived_snapshot": None if derived_record is None else derived_record.to_dict(),
        "archive_audit": audit.to_dict(),
        "redistribution": "blocked_without_written_permission",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICIES)
    parser.add_argument("--timing-policy", type=Path, default=DEFAULT_TIMING_POLICY)
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
        policy=load_timing_policy(args.timing_policy),
    )
    latest = args.archive_root / "latest_first_tradable_resolution.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(
        "First-tradable resolver",
        output["status"],
        "resolved",
        output["resolved_event_count"],
        "pending",
        output["pending_event_count"],
    )


def _quote_observation(raw: dict[str, object]) -> QuoteObservation | None:
    timestamp_text = str(raw.get("t", "")).strip()
    if not timestamp_text:
        return None
    timestamp = _parse_aware_datetime(timestamp_text)
    try:
        bid_price = _float_value(raw["bp"])
        ask_price = _float_value(raw["ap"])
        bid_size = _int_value(raw["bs"])
        ask_size = _int_value(raw["as"])
    except (KeyError, TypeError, ValueError):
        return None
    bid_exchange = str(raw.get("bx", "")).strip()
    ask_exchange = str(raw.get("ax", "")).strip()
    if (
        bid_price <= 0
        or ask_price <= 0
        or ask_price < bid_price
        or bid_size < 0
        or ask_size < 0
        or not bid_exchange
        or not ask_exchange
    ):
        return None
    conditions_raw = raw.get("c", [])
    if not isinstance(conditions_raw, list):
        return None
    return QuoteObservation(
        timestamp_text=timestamp_text,
        timestamp=timestamp,
        bid_price=bid_price,
        ask_price=ask_price,
        bid_size=bid_size,
        ask_size=ask_size,
        bid_exchange=bid_exchange,
        ask_exchange=ask_exchange,
        conditions=tuple(str(item) for item in conditions_raw),
        tape=None if raw.get("z") is None else str(raw.get("z")),
    )


def _next_page_token(payload: bytes) -> str | None:
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise ValueError("Alpaca quotes response must be a JSON object.")
    token = parsed.get("next_page_token")
    if token is None:
        return None
    text = str(token).strip()
    return text or None


def _pending(event: FilingEvent, reason: str) -> dict[str, object]:
    return {
        "accession_number": event.accession_number,
        "symbol": event.symbol,
        "decision_available_at": event.decision_available_at.isoformat(),
        "status": "pending",
        "reason": reason,
    }


def _status_output(
    status: str,
    resolved_count: int,
    pending: list[dict[str, object]],
    archive: PointInTimeArchive,
) -> dict[str, object]:
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_only": True,
        "trading_actions_enabled": False,
        "active_profile_changed": False,
        "status": status,
        "pending_event_count": len(pending),
        "resolved_event_count": resolved_count,
        "pending": pending,
        "resolutions": [],
        "derived_snapshot": None,
        "archive_audit": archive.audit().to_dict(),
        "redistribution": "blocked_without_written_permission",
    }


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a positive integer.")
    try:
        parsed = int(str(value))
    except ValueError as exc:
        raise ValueError(f"{label} must be a positive integer.") from exc
    if parsed <= 0:
        raise ValueError(f"{label} must be a positive integer.")
    return parsed


def _float_value(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise ValueError("Quote numeric value has an invalid type.")
    return float(value)


def _int_value(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise ValueError("Quote integer value has an invalid type.")
    return int(value)


def _parse_aware_datetime(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"Timestamp is not timezone-aware: {value}")
    return parsed


def _require_aware(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware.")
    return value


def _rfc3339(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    main()
