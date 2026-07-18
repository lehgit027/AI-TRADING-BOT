import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ai_trading_bot.data.history_archive import PointInTimeArchive, load_source_policies
from scripts.collect_alpaca_asset_history import HttpCapture, collect, normalize_alpaca_assets


def assets_payload() -> bytes:
    return json.dumps(
        [
            {
                "id": "asset-aapl",
                "class": "us_equity",
                "exchange": "NASDAQ",
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "status": "active",
                "tradable": True,
                "marginable": True,
                "shortable": True,
                "borrow_status": "easy_to_borrow",
                "easy_to_borrow": True,
                "fractionable": True,
                "attributes": ["has_options"],
            },
            {
                "id": "asset-old",
                "class": "us_equity",
                "exchange": "OTC",
                "symbol": "OLDQ",
                "name": "Old Company",
                "status": "inactive",
                "tradable": False,
                "marginable": False,
                "shortable": False,
                "easy_to_borrow": False,
                "fractionable": False,
                "attributes": [],
            },
        ]
    ).encode()


def test_normalize_alpaca_assets_keeps_inactive_and_borrow_state() -> None:
    observed = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)

    rows = normalize_alpaca_assets(
        assets_payload(),
        observed_at=observed,
        source_snapshot_id="snapshot",
        source_sha256="a" * 64,
    )

    assert len(rows) == 2
    assert rows[0]["borrow_status"] == "easy_to_borrow"
    assert rows[1]["status"] == "inactive"
    assert rows[1]["tradable"] is False
    assert rows[1]["redistribution_allowed"] is False


def test_collect_alpaca_assets_redacts_credentials_and_audits(tmp_path: Path) -> None:
    policies = load_source_policies(
        Path(__file__).resolve().parents[1] / "config" / "data_source_policies.json"
    )
    archive = PointInTimeArchive(tmp_path / "archive", policies)
    started = datetime.now(UTC) - timedelta(seconds=1)
    retrieved = started + timedelta(milliseconds=250)
    capture = HttpCapture(
        payload=assets_payload(),
        request_started_at=started,
        retrieved_at=retrieved,
        content_type="application/json",
        response_metadata={"http_status": 200},
    )

    output = collect(
        archive,
        api_key="must-not-leak",
        api_secret="must-not-leak",
        http_capture=capture,
    )

    assert output["status"] == "captured"
    assert output["asset_observation_count"] == 2
    assert output["active_asset_count"] == 1
    assert output["archive_audit"]["status"] == "pass"
    raw = output["raw_snapshot"]
    assert raw["request_parameters"]["api_key"] == "<redacted>"
    assert raw["request_parameters"]["api_secret"] == "<redacted>"
