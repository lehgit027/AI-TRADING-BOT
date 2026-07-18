"""Record forward-only Norgate availability provenance inside the licensed VM.

Raw Norgate responses remain in a private Windows-only cache.  The public-facing
summary contains only timestamps, hashes, counts, and integrity status.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SYMBOLS = ("SPY", "QQQ", "IWM", "VOO")
SCHEMA_VERSION = "2.0"
ADJUSTMENT_MODE = "TOTALRETURN"


def _json_value(value: object) -> object:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _rows_to_json(rows: Any) -> list[dict[str, object]]:
    names = getattr(getattr(rows, "dtype", None), "names", None) or ()
    return [{str(name): _json_value(row[str(name)]) for name in names} for row in rows]


def _atomic_private_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as error:
        raise RuntimeError(f"Refusing to overwrite immutable private snapshot: {path}") from error
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _canonical_json(payload: object) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _last_manifest_hash(path: Path) -> str | None:
    if not path.exists():
        return None
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        return None
    return str(json.loads(lines[-1])["record_sha256"])


def record(norgate: Any, root: Path, now: datetime) -> dict[str, object]:
    if not norgate.status():
        raise RuntimeError("Norgate Data Updater is not running")
    request_started_at = now.astimezone().isoformat()
    response: dict[str, list[dict[str, object]]] = {}
    for symbol in SYMBOLS:
        rows = norgate.price_timeseries(
            symbol,
            limit=2,
            timeseriesformat="numpy-recarray",
            stock_price_adjustment_setting=norgate.StockPriceAdjustmentType.TOTALRETURN,
            padding_setting=norgate.PaddingType.NONE,
        )
        response[symbol] = _rows_to_json(rows)
    retrieved_at = datetime.now(UTC)
    source_updated_at = norgate.last_database_update_time("us").isoformat()
    content_hash = hashlib.sha256(_canonical_json(response)).hexdigest()
    raw_payload = _canonical_json(
        {
            "schema_version": SCHEMA_VERSION,
            "source": "Norgate Data private Windows worker",
            "retrieved_at": retrieved_at.isoformat(),
            "request_started_at": request_started_at,
            "source_updated_at": source_updated_at,
            "adjustment_mode": ADJUSTMENT_MODE,
            "content_sha256": content_hash,
            "response": response,
        }
    )
    snapshot_hash = hashlib.sha256(raw_payload).hexdigest()
    snapshot_id_time = retrieved_at.astimezone(UTC)
    snapshot_id = f"{snapshot_id_time.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:12]}"
    relative_raw_path = (
        Path("raw") / retrieved_at.strftime("%Y/%m/%d") / f"{snapshot_id}-{snapshot_hash}.json"
    )
    raw_path = root / relative_raw_path
    _atomic_private_write(raw_path, raw_payload)

    manifest_path = root / "manifest" / "forward_provenance.jsonl"
    previous_hash = _last_manifest_hash(manifest_path)
    record = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "source": "Norgate Data private Windows worker",
        "source_license_boundary": "Raw snapshots remain inside this licensed Windows worker.",
        "retrieved_at": retrieved_at.isoformat(),
        "request_started_at": request_started_at,
        "source_updated_at": source_updated_at,
        "market_timezone": "America/New_York",
        "adjustment_mode": ADJUSTMENT_MODE,
        "decision_availability": "forward observation only; no historical availability is inferred",
        "content_sha256": content_hash,
        "snapshot_sha256": snapshot_hash,
        "response_hash": snapshot_hash,
        "raw_path": str(relative_raw_path),
        "symbol_response_counts": {symbol: len(rows) for symbol, rows in response.items()},
        "ndu_us_database_updated_at_local": source_updated_at,
        "package_version": str(getattr(norgate, "__version__", "unknown")),
        "previous_record_sha256": previous_hash,
        "research_only": True,
        "active_profile_changed": False,
        "alpha_calculated": False,
        "paper_fill_applied": False,
        "raw_data_exported": False,
    }
    record_hash = hashlib.sha256(_canonical_json(record)).hexdigest()
    record["record_sha256"] = record_hash
    immutable_record_path = (
        root
        / "manifest"
        / "records"
        / retrieved_at.strftime("%Y/%m/%d")
        / f"{snapshot_id}-{record_hash}.json"
    )
    _atomic_private_write(immutable_record_path, _canonical_json(record))
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())

    return {
        "generated_at": retrieved_at.isoformat(),
        "status": "forward_provenance_recorded",
        "data_quality_verdict": "forward_only_pass_historical_backtest_still_blocked",
        "snapshot_id": snapshot_id,
        "retrieved_at": retrieved_at.isoformat(),
        "source_updated_at": source_updated_at,
        "adjustment_mode": ADJUSTMENT_MODE,
        "content_sha256": content_hash,
        "snapshot_sha256": snapshot_hash,
        "response_hash": snapshot_hash,
        "immutable_manifest_record": str(immutable_record_path.relative_to(root)),
        "symbol_response_counts": record["symbol_response_counts"],
        "ndu_us_database_updated_at_local": record["ndu_us_database_updated_at_local"],
        "runner": {"platform": platform.platform(), "package_version": record["package_version"]},
        "research_only": True,
        "active_profile_changed": False,
        "alpha_calculated": False,
        "paper_fill_applied": False,
        "raw_data_exported": False,
        "historical_backtest_status": "blocked_missing_historical_delivery_provenance",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.home() / "norgate_private_history",
        help="Private VM-local root only. Never use a shared or cloud path.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path.home() / "norgate_private_reports" / "forward_provenance_latest.json",
    )
    args = parser.parse_args()
    now = datetime.now().astimezone()
    try:
        import norgatedata  # type: ignore[import-not-found]

        summary = record(norgatedata, args.root.expanduser(), now)
    except Exception as error:  # noqa: BLE001 - fail closed and retain diagnostic only
        summary = {
            "generated_at": now.isoformat(),
            "status": "blocked_forward_provenance_error",
            "data_quality_verdict": "blocked",
            "error": f"{type(error).__name__}: {error}",
            "research_only": True,
            "active_profile_changed": False,
            "alpha_calculated": False,
            "paper_fill_applied": False,
            "raw_data_exported": False,
        }
    output = args.output.expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {"status": summary["status"], "data_quality_verdict": summary["data_quality_verdict"]}
        )
    )


if __name__ == "__main__":
    main()
