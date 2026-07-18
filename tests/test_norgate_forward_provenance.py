import hashlib
import json
import sys
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.record_norgate_forward_provenance import record


class FakeRows:
    class dtype:
        names = ("Date", "Close", "Volume")

    def __iter__(self):
        return iter(
            [
                {"Date": "2026-07-17", "Close": 100.0, "Volume": 1_000},
                {"Date": "2026-07-16", "Close": 99.0, "Volume": 900},
            ]
        )


class FakeNorgate:
    class StockPriceAdjustmentType:
        TOTALRETURN = "total_return"

    class PaddingType:
        NONE = "none"

    __version__ = "test"

    def status(self) -> bool:
        return True

    def price_timeseries(self, symbol, **kwargs):
        assert kwargs["stock_price_adjustment_setting"] == self.StockPriceAdjustmentType.TOTALRETURN
        return FakeRows()

    def last_database_update_time(self, database: str) -> datetime:
        assert database == "us"
        return datetime(2026, 7, 17, tzinfo=UTC)


def test_forward_provenance_writes_private_raw_and_hashed_manifest(tmp_path) -> None:
    summary = record(FakeNorgate(), tmp_path, datetime(2026, 7, 17, tzinfo=UTC))

    assert summary["status"] == "forward_provenance_recorded"
    assert summary["historical_backtest_status"] == "blocked_missing_historical_delivery_provenance"
    assert summary["adjustment_mode"] == "TOTALRETURN"
    assert summary["source_updated_at"] == "2026-07-17T00:00:00+00:00"
    assert summary["retrieved_at"]

    raw_paths = list((tmp_path / "raw").rglob("*.json"))
    assert len(raw_paths) == 1
    raw_bytes = raw_paths[0].read_bytes()
    raw = json.loads(raw_bytes)
    assert raw["adjustment_mode"] == "TOTALRETURN"
    assert raw["source_updated_at"] == summary["source_updated_at"]
    assert raw["content_sha256"] == summary["content_sha256"]
    assert hashlib.sha256(raw_bytes).hexdigest() == summary["snapshot_sha256"]

    manifest = (tmp_path / "manifest" / "forward_provenance.jsonl").read_text(encoding="utf-8")
    assert "record_sha256" in manifest
    immutable_records = list((tmp_path / "manifest" / "records").rglob("*.json"))
    assert len(immutable_records) == 1
    immutable_record = json.loads(immutable_records[0].read_text(encoding="utf-8"))
    record_hash = immutable_record.pop("record_sha256")
    canonical = json.dumps(immutable_record, sort_keys=True, separators=(",", ":")).encode()
    assert hashlib.sha256(canonical).hexdigest() == record_hash


def test_forward_provenance_chains_immutable_records(tmp_path) -> None:
    first = record(FakeNorgate(), tmp_path, datetime(2026, 7, 17, tzinfo=UTC))
    second = record(FakeNorgate(), tmp_path, datetime(2026, 7, 18, tzinfo=UTC))

    records = [
        json.loads(line)
        for line in (tmp_path / "manifest" / "forward_provenance.jsonl").read_text().splitlines()
    ]
    assert records[0]["record_sha256"]
    assert records[1]["previous_record_sha256"] == records[0]["record_sha256"]
    assert first["snapshot_id"] != second["snapshot_id"]
    assert len(list((tmp_path / "raw").rglob("*.json"))) == 2
    assert len(list((tmp_path / "manifest" / "records").rglob("*.json"))) == 2


def test_forward_provenance_snapshot_id_uses_utc(monkeypatch, tmp_path) -> None:
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = datetime(2026, 7, 18, 1, 2, 3, 4, tzinfo=timezone(timedelta(hours=-7)))
            return value.astimezone(tz) if tz is not None else value.replace(tzinfo=None)

    monkeypatch.setattr("scripts.record_norgate_forward_provenance.datetime", FixedDateTime)

    summary = record(FakeNorgate(), tmp_path, datetime(2026, 7, 17, tzinfo=UTC))

    assert summary["snapshot_id"].startswith("20260718T080203000004Z-")
