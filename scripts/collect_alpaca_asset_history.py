"""Collect internal-only Alpaca asset, listing, and borrow-status observations."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode
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
ALPACA_ASSETS_URL = "https://paper-api.alpaca.markets/v2/assets"


@dataclass(frozen=True)
class HttpCapture:
    payload: bytes
    request_started_at: datetime
    retrieved_at: datetime
    content_type: str
    response_metadata: dict[str, object]


def fetch_alpaca_assets(api_key: str, api_secret: str) -> HttpCapture:
    query = urlencode({"asset_class": "us_equity"})
    url = f"{ALPACA_ASSETS_URL}?{query}"
    started = datetime.now(UTC)
    request = Request(
        url,
        headers={
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret,
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=60) as response:  # noqa: S310 - fixed Alpaca endpoint
        payload = response.read()
        retrieved = datetime.now(UTC)
        selected_headers = {
            key: response.headers[key]
            for key in ("Date", "ETag", "Last-Modified", "Content-Length")
            if response.headers.get(key) is not None
        }
        selected_headers["http_status"] = response.status
        selected_headers["latency_ms"] = round((retrieved - started).total_seconds() * 1000, 3)
        return HttpCapture(
            payload=payload,
            request_started_at=started,
            retrieved_at=retrieved,
            content_type=response.headers.get_content_type(),
            response_metadata=selected_headers,
        )


def normalize_alpaca_assets(
    payload: bytes,
    *,
    observed_at: datetime,
    source_snapshot_id: str,
    source_sha256: str,
) -> list[dict[str, object]]:
    parsed = json.loads(payload)
    if not isinstance(parsed, list):
        raise ValueError("Alpaca assets response must be a JSON array.")
    rows: list[dict[str, object]] = []
    for raw in parsed:
        if not isinstance(raw, dict):
            raise ValueError("Alpaca assets response contains a non-object row.")
        asset_id = _required_text(raw, "id")
        symbol = _required_text(raw, "symbol").upper()
        status = _required_text(raw, "status").lower()
        attributes = raw.get("attributes", [])
        if attributes is None:
            attributes = []
        if not isinstance(attributes, list):
            raise ValueError(f"Alpaca asset {symbol} has invalid attributes.")
        rows.append(
            {
                "observation_type": "alpaca_asset_state",
                "security_id": f"alpaca-asset:{asset_id}",
                "alpaca_asset_id": asset_id,
                "symbol": symbol,
                "name": _optional_text(raw.get("name")),
                "asset_class": _optional_text(raw.get("class")),
                "exchange": _optional_text(raw.get("exchange")),
                "status": status,
                "tradable": _optional_bool(raw.get("tradable")),
                "marginable": _optional_bool(raw.get("marginable")),
                "shortable": _optional_bool(raw.get("shortable")),
                "borrow_status": _optional_text(raw.get("borrow_status")),
                "easy_to_borrow_deprecated": _optional_bool(raw.get("easy_to_borrow")),
                "fractionable": _optional_bool(raw.get("fractionable")),
                "maintenance_margin_requirement": raw.get("maintenance_margin_requirement"),
                "attributes": sorted(str(item) for item in attributes),
                "valid_from": observed_at.isoformat(),
                "valid_to": None,
                "decision_available_at": observed_at.isoformat(),
                "source_snapshot_id": source_snapshot_id,
                "source_sha256": source_sha256,
                "point_in_time_complete": False,
                "survivorship_free": False,
                "redistribution_allowed": False,
            }
        )
    return sorted(
        rows,
        key=lambda item: (
            str(item["symbol"]),
            str(item["exchange"]),
            str(item["alpaca_asset_id"]),
        ),
    )


def collect(
    archive: PointInTimeArchive,
    *,
    api_key: str,
    api_secret: str,
    http_capture: HttpCapture | None = None,
) -> dict[str, object]:
    capture = http_capture or fetch_alpaca_assets(api_key, api_secret)
    raw_record = archive.capture(
        source_id="alpaca_market_data",
        dataset="us_equity_assets",
        source_url=f"{ALPACA_ASSETS_URL}?asset_class=us_equity",
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
            "asset_class": "us_equity",
            "api_key": api_key,
            "api_secret": api_secret,
        },
        response_metadata=capture.response_metadata,
        integrity_notes=(
            "This is Alpaca platform asset state, not an official exchange security master.",
            "Raw and reversible data must remain private under the registered source policy.",
        ),
    )
    rows = normalize_alpaca_assets(
        capture.payload,
        observed_at=capture.retrieved_at,
        source_snapshot_id=raw_record.snapshot_id,
        source_sha256=raw_record.raw_sha256,
    )
    derived_payload = {
        "schema_version": "alpaca_asset_observations_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "decision_available_at": capture.retrieved_at.isoformat(),
        "source_snapshot_id": raw_record.snapshot_id,
        "source_sha256": raw_record.raw_sha256,
        "point_in_time_scope": "forward_only_from_first_observation",
        "redistribution_allowed": False,
        "survivorship_free": False,
        "rows": rows,
    }
    derived_started_at = datetime.now(UTC)
    derived_record = archive.capture(
        source_id="internal_derived",
        dataset="alpaca_asset_observations",
        source_url="internal://security-master/alpaca-assets",
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
        response_metadata={"record_count": len(rows)},
        upstream_snapshot_ids=(raw_record.snapshot_id,),
        integrity_notes=(
            "Ticker matches to SEC CIKs remain provisional until independently reconciled.",
            "Borrow status is a platform observation, not historical borrow fee or inventory.",
            "The upstream Alpaca redistribution restriction applies to this derived snapshot.",
        ),
    )
    audit = archive.audit()
    active_count = sum(row["status"] == "active" for row in rows)
    borrow_observed = sum(
        row["borrow_status"] is not None or row["easy_to_borrow_deprecated"] is not None
        for row in rows
    )
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_only": True,
        "active_profile_changed": False,
        "status": "captured" if audit.status == "pass" else "integrity_audit_failed",
        "raw_snapshot": raw_record.to_dict(),
        "derived_snapshot": derived_record.to_dict(),
        "asset_observation_count": len(rows),
        "active_asset_count": active_count,
        "borrow_observation_count": borrow_observed,
        "archive_audit": audit.to_dict(),
        "redistribution": "blocked_without_written_permission",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICIES)
    args = parser.parse_args()
    environment = trusted_data_environment(os.environ, ROOT / ".env")
    key = environment.get("AI_TRADING_MARKET_DATA_API_KEY", "").strip()
    secret = environment.get("AI_TRADING_MARKET_DATA_API_SECRET", "").strip()
    if not key or not secret:
        raise ValueError("Missing Alpaca data-only credentials.")
    archive = PointInTimeArchive(args.archive_root, load_source_policies(args.policies))
    output = collect(archive, api_key=key, api_secret=secret)
    latest = args.archive_root / "latest_alpaca_asset_collection.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(f"High-integrity Alpaca asset history captured in {args.archive_root}")
    print(
        "status",
        output["status"],
        "assets",
        output["asset_observation_count"],
        "borrow_observations",
        output["borrow_observation_count"],
    )


def _required_text(row: dict[str, object], key: str) -> str:
    value = _optional_text(row.get(key))
    if value is None:
        raise ValueError(f"Alpaca asset row is missing {key}.")
    return value


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_bool(value: object) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ValueError("Alpaca asset boolean field has an invalid type.")
    return value


if __name__ == "__main__":
    main()
