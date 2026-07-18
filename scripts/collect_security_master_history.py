"""Collect a daily SEC identity snapshot into the high-integrity history archive."""

from __future__ import annotations

import argparse
import gzip
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.request import Request, urlopen

from ai_trading_bot.data.history_archive import (
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
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


@dataclass(frozen=True)
class HttpCapture:
    payload: bytes
    decoded_payload: bytes
    request_started_at: datetime
    retrieved_at: datetime
    content_type: str
    content_encoding: str | None
    response_metadata: dict[str, object]
    event_publication_at: datetime | None


def fetch_sec_tickers(user_agent: str) -> HttpCapture:
    started = datetime.now(UTC)
    request = Request(
        SEC_TICKERS_URL,
        headers={"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"},
    )
    with urlopen(request, timeout=45) as response:  # noqa: S310 - fixed SEC endpoint
        payload = response.read()
        retrieved = datetime.now(UTC)
        content_encoding = response.headers.get("Content-Encoding")
        decoded = gzip.decompress(payload) if content_encoding == "gzip" else payload
        selected_headers = {
            key: response.headers[key]
            for key in ("Date", "ETag", "Last-Modified", "Content-Length")
            if response.headers.get(key) is not None
        }
        selected_headers["http_status"] = response.status
        selected_headers["latency_ms"] = round((retrieved - started).total_seconds() * 1000, 3)
        publication_at = _http_datetime(response.headers.get("Last-Modified"))
        return HttpCapture(
            payload=payload,
            decoded_payload=decoded,
            request_started_at=started,
            retrieved_at=retrieved,
            content_type=response.headers.get_content_type(),
            content_encoding=content_encoding,
            response_metadata=selected_headers,
            event_publication_at=publication_at,
        )


def normalize_sec_company_tickers(
    decoded_payload: bytes,
    *,
    observed_at: datetime,
    source_snapshot_id: str,
    source_sha256: str,
) -> list[dict[str, object]]:
    payload = json.loads(decoded_payload)
    if not isinstance(payload, dict):
        raise ValueError("SEC company ticker response must be a JSON object.")
    rows: list[dict[str, object]] = []
    for raw in payload.values():
        if not isinstance(raw, dict):
            raise ValueError("SEC company ticker response contains a non-object row.")
        cik_raw = raw.get("cik_str")
        ticker_raw = raw.get("ticker")
        title_raw = raw.get("title")
        if isinstance(cik_raw, bool) or not isinstance(cik_raw, int | str):
            raise ValueError("SEC company ticker row has an invalid CIK.")
        ticker = str(ticker_raw).strip().upper()
        issuer_name = str(title_raw).strip()
        if not ticker or not issuer_name:
            raise ValueError("SEC company ticker row is missing ticker or issuer name.")
        try:
            cik = str(int(cik_raw)).zfill(10)
        except (TypeError, ValueError) as exc:
            raise ValueError("SEC company ticker row has a non-numeric CIK.") from exc
        rows.append(
            {
                "observation_type": "current_sec_ticker_association",
                "issuer_id": f"sec-cik:{cik}",
                "security_id": None,
                "cik": cik,
                "symbol": ticker,
                "issuer_name": issuer_name,
                "exchange": None,
                "asset_type": None,
                "listing_status": "observed_current",
                "valid_from": observed_at.isoformat(),
                "valid_to": None,
                "decision_available_at": observed_at.isoformat(),
                "source_snapshot_id": source_snapshot_id,
                "source_sha256": source_sha256,
                "point_in_time_complete": False,
                "survivorship_free": False,
            }
        )
    return sorted(rows, key=lambda item: (str(item["cik"]), str(item["symbol"])))


def collect(
    archive: PointInTimeArchive,
    *,
    user_agent: str,
    http_capture: HttpCapture | None = None,
) -> dict[str, object]:
    capture = http_capture or fetch_sec_tickers(user_agent)
    raw_record = archive.capture(
        source_id="sec_edgar",
        dataset="company_tickers",
        source_url=SEC_TICKERS_URL,
        payload=capture.payload,
        request_started_at=capture.request_started_at,
        retrieved_at=capture.retrieved_at,
        event_publication_at=capture.event_publication_at,
        decision_available_at=capture.retrieved_at,
        decision_availability_basis="first_observed",
        market_timezone="UTC",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type=capture.content_type,
        content_encoding=capture.content_encoding,
        request_parameters={"dataset": "company_tickers"},
        response_metadata=capture.response_metadata,
        integrity_notes=(
            "SEC does not guarantee ticker-map accuracy or scope.",
            "This observation is valid from first retrieval forward and must not be backfilled.",
        ),
    )
    rows = normalize_sec_company_tickers(
        capture.decoded_payload,
        observed_at=capture.retrieved_at,
        source_snapshot_id=raw_record.snapshot_id,
        source_sha256=raw_record.raw_sha256,
    )
    derived_payload = {
        "schema_version": "security_master_observations_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "decision_available_at": capture.retrieved_at.isoformat(),
        "source_snapshot_id": raw_record.snapshot_id,
        "source_sha256": raw_record.raw_sha256,
        "point_in_time_scope": "forward_only_from_first_observation",
        "survivorship_free": False,
        "rows": rows,
    }
    derived_started_at = datetime.now(UTC)
    derived_record = archive.capture(
        source_id="internal_derived",
        dataset="security_master_observations",
        source_url="internal://security-master/sec-company-tickers",
        payload=canonical_json_bytes(derived_payload),
        request_started_at=derived_started_at,
        retrieved_at=datetime.now(UTC),
        decision_available_at=capture.retrieved_at,
        decision_availability_basis="upstream_first_observed",
        market_timezone="UTC",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        request_parameters={"parent_snapshot_id": raw_record.snapshot_id},
        response_metadata={"record_count": len(rows)},
        upstream_snapshot_ids=(raw_record.snapshot_id,),
        integrity_notes=(
            (
                "No exchange, listing start, delisting, or security-level permanent identifier "
                "is inferred."
            ),
            "Absence from a later snapshot is a change candidate, not proof of delisting.",
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
        "security_observation_count": len(rows),
        "archive_audit": audit.to_dict(),
        "next_integrity_limit": (
            "History begins at this first observation. Do not infer earlier listing dates, "
            "delistings, exchange membership, or survivorship-free status."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICIES)
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="Verify the existing archive without making a network request.",
    )
    args = parser.parse_args()
    policies = load_source_policies(args.policies)
    archive = PointInTimeArchive(args.archive_root, policies)
    if args.audit_only:
        print(json.dumps(archive.audit().to_dict(), indent=2, sort_keys=True))
        return
    environment = trusted_data_environment(os.environ, ROOT / ".env")
    email = environment.get("SEC_USER_AGENT_EMAIL", "").strip()
    if not email:
        raise ValueError("Missing SEC_USER_AGENT_EMAIL.")
    name = environment.get("SEC_USER_AGENT_NAME", "AI-TRADING-BOT/0.1").strip()
    output = collect(archive, user_agent=f"{name} {email}")
    latest = args.archive_root / "latest_security_master_collection.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(f"High-integrity security history captured in {args.archive_root}")
    print("status", output["status"], "observations", output["security_observation_count"])


def _http_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = parsedate_to_datetime(value)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


if __name__ == "__main__":
    main()
