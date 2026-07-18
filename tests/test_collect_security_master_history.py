import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ai_trading_bot.data.history_archive import PointInTimeArchive, load_source_policies
from scripts.collect_security_master_history import (
    HttpCapture,
    collect,
    normalize_sec_company_tickers,
)


def sec_payload() -> bytes:
    return json.dumps(
        {
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
            "1": {"cik_str": "789019", "ticker": "msft", "title": "Microsoft Corp."},
        }
    ).encode()


def test_normalize_sec_company_tickers_preserves_forward_only_limits() -> None:
    observed = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)

    rows = normalize_sec_company_tickers(
        sec_payload(),
        observed_at=observed,
        source_snapshot_id="snapshot",
        source_sha256="a" * 64,
    )

    assert rows[0]["cik"] == "0000320193"
    assert rows[1]["symbol"] == "MSFT"
    assert rows[0]["valid_from"] == observed.isoformat()
    assert rows[0]["exchange"] is None
    assert rows[0]["survivorship_free"] is False


def test_collect_creates_raw_and_derived_snapshots(tmp_path: Path) -> None:
    policies = load_source_policies(
        Path(__file__).resolve().parents[1] / "config" / "data_source_policies.json"
    )
    archive = PointInTimeArchive(tmp_path / "archive", policies)
    started = datetime.now(UTC) - timedelta(seconds=1)
    retrieved = started + timedelta(milliseconds=250)
    capture = HttpCapture(
        payload=sec_payload(),
        decoded_payload=sec_payload(),
        request_started_at=started,
        retrieved_at=retrieved,
        content_type="application/json",
        content_encoding=None,
        response_metadata={"http_status": 200},
        event_publication_at=None,
    )

    output = collect(archive, user_agent="test test@example.com", http_capture=capture)

    assert output["status"] == "captured"
    assert output["security_observation_count"] == 2
    assert output["active_profile_changed"] is False
    assert output["archive_audit"]["status"] == "pass"
    manifest = (tmp_path / "archive" / "manifest" / "snapshots.jsonl").read_text()
    assert manifest.count("\n") == 2
