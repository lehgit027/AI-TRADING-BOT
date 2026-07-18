import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ai_trading_bot.data.history_archive import (
    PointInTimeArchive,
    canonical_json_bytes,
    load_source_policies,
)
from scripts.collect_sec_filing_forward_observations import (
    collect,
    load_due_observations,
)
from scripts.plan_sec_filing_forward_observations import load_observation_policy
from scripts.resolve_sec_filing_first_tradable import QuoteHttpCapture

ROOT = Path(__file__).resolve().parents[1]
ACCESSION = "0000320193-26-000999"
FIRST_TRADABLE = datetime(2026, 7, 14, 14, 0, tzinfo=UTC)
TARGET = datetime(2026, 7, 14, 15, 0, tzinfo=UTC)
WINDOW_END = TARGET + timedelta(minutes=1)
EARLIEST = WINDOW_END + timedelta(minutes=16)


def build_archive(tmp_path: Path) -> PointInTimeArchive:
    archive = PointInTimeArchive(
        tmp_path / "archive",
        load_source_policies(ROOT / "config" / "data_source_policies.json"),
    )
    raw = archive.capture(
        source_id="sec_edgar",
        dataset="sec_filing_primary_document",
        source_url="https://www.sec.gov/Archives/edgar/data/320193/event.htm",
        payload=b"<html>filing</html>",
        request_started_at=FIRST_TRADABLE - timedelta(minutes=31),
        retrieved_at=FIRST_TRADABLE - timedelta(minutes=30),
        decision_available_at=FIRST_TRADABLE - timedelta(minutes=30),
        decision_availability_basis="first_observed",
        market_timezone="UTC",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="text/html",
    )
    plan_payload = {
        "schema_version": "sec_filing_forward_observation_plans_v1",
        "rows": [
            {
                "accession_number": ACCESSION,
                "cik": "0000320193",
                "symbol": "AAPL",
                "event_types": ["earnings_results"],
                "first_tradable_at": FIRST_TRADABLE.isoformat(),
                "first_tradable_session_close_at": "2026-07-14T16:00:00-04:00",
                "first_tradable_bid": 100.0,
                "first_tradable_ask": 100.02,
                "benchmark_symbol": "VOO",
                "costs_bps": [5, 25, 50],
                "resolution_snapshot_id": raw.snapshot_id,
                "horizons": [
                    {
                        "horizon": "1h",
                        "target_at": TARGET.isoformat(),
                        "observation_window_end_at": WINDOW_END.isoformat(),
                        "earliest_observation_retrieval_at": EARLIEST.isoformat(),
                    }
                ],
            }
        ],
    }
    archive.capture(
        source_id="internal_derived",
        dataset="sec_filing_forward_observation_plans",
        source_url="internal://observations/sec-filing-forward-plans",
        payload=canonical_json_bytes(plan_payload),
        request_started_at=FIRST_TRADABLE + timedelta(minutes=20),
        retrieved_at=FIRST_TRADABLE + timedelta(minutes=21),
        decision_available_at=FIRST_TRADABLE + timedelta(minutes=20),
        decision_availability_basis="first_tradable_observed",
        market_timezone="America/New_York",
        timestamp_quality="first_seen_exact",
        research_use="forward_only",
        content_type="application/json",
        upstream_snapshot_ids=(raw.snapshot_id,),
    )
    return archive


def quote_capture(
    symbol: str,
    quote_at: datetime,
    bid: float,
    ask: float,
    *,
    empty: bool = False,
) -> QuoteHttpCapture:
    quotes = []
    if not empty:
        quotes.append(
            {
                "t": quote_at.isoformat().replace("+00:00", "Z"),
                "bp": bid,
                "ap": ask,
                "bs": 10,
                "as": 11,
                "bx": "Q",
                "ax": "P",
                "c": ["R"],
                "z": "C",
            }
        )
    payload = json.dumps({"symbol": symbol, "quotes": quotes, "next_page_token": None}).encode()
    retrieved = EARLIEST + timedelta(minutes=1)
    return QuoteHttpCapture(
        payload=payload,
        source_url=f"https://data.alpaca.markets/v2/stocks/{symbol}/quotes",
        request_started_at=retrieved - timedelta(milliseconds=100),
        retrieved_at=retrieved,
        content_type="application/json",
        response_metadata={"http_status": 200},
        request_parameters={"feed": "sip", "api_key": "key", "api_secret": "secret"},
    )


def captures(*, empty_benchmark_end: bool = False) -> dict[str, QuoteHttpCapture]:
    prefix = f"{ACCESSION}:1h"
    return {
        f"{prefix}:benchmark_start": quote_capture("VOO", FIRST_TRADABLE, 500.0, 500.02),
        f"{prefix}:stock_end": quote_capture("AAPL", TARGET, 102.0, 102.02),
        f"{prefix}:benchmark_end": quote_capture(
            "VOO",
            TARGET,
            501.0,
            501.02,
            empty=empty_benchmark_end,
        ),
    }


def test_collects_due_quote_price_observation_without_fill(tmp_path: Path) -> None:
    archive = build_archive(tmp_path)
    policy = load_observation_policy(ROOT / "config" / "sec_filing_forward_observation_policy.json")

    output = collect(
        archive,
        api_key="key",
        api_secret="secret",
        policy=policy,
        now=EARLIEST + timedelta(minutes=2),
        quote_captures=captures(),
    )

    assert output["status"] == "captured"
    assert output["observed_horizon_count"] == 1
    assert output["blocked_horizon_count"] == 0
    assert output["raw_quote_snapshot_count"] == 3
    observation = output["observations"][0]
    assert observation["data_quality_status"] == "pass_quote_price_only"
    returns = observation["return_observation"]
    expected_stock = 102.01 / 100.01 - 1
    expected_voo = 501.01 / 500.01 - 1
    assert returns["stock_midpoint_price_return"] == pytest.approx(expected_stock)
    assert returns["voo_midpoint_price_return"] == pytest.approx(expected_voo)
    assert returns["price_excess_return_by_cost_bps"]["25"] == pytest.approx(
        expected_stock - 0.0025 - expected_voo
    )
    assert observation["voo_total_return_status"] == ("pending_separate_integrity_reconciliation")
    assert observation["execution_price_selected"] is False
    assert observation["paper_fill_applied"] is False
    assert observation["candidate_created"] is False
    assert archive.audit().status == "pass"

    repeat = collect(
        archive,
        api_key="key",
        api_secret="secret",
        policy=policy,
        now=EARLIEST + timedelta(minutes=3),
        quote_captures={},
    )
    assert repeat["status"] == "completed"
    assert repeat["observed_horizon_count"] == 0


def test_missing_quote_is_blocked_and_never_imputed(tmp_path: Path) -> None:
    archive = build_archive(tmp_path)
    policy = load_observation_policy(ROOT / "config" / "sec_filing_forward_observation_policy.json")

    output = collect(
        archive,
        api_key="key",
        api_secret="secret",
        policy=policy,
        now=EARLIEST + timedelta(minutes=2),
        quote_captures=captures(empty_benchmark_end=True),
    )

    assert output["observed_horizon_count"] == 0
    assert output["blocked_horizon_count"] == 1
    observation = output["observations"][0]
    assert observation["data_quality_status"] == "blocked_no_valid_quote"
    assert observation["missing_quote_roles"] == ["benchmark_end"]
    assert observation["return_observation"] is None


def test_future_horizon_waits_without_requesting_quotes(tmp_path: Path) -> None:
    archive = build_archive(tmp_path)
    policy = load_observation_policy(ROOT / "config" / "sec_filing_forward_observation_policy.json")

    state = load_due_observations(archive, now=EARLIEST - timedelta(seconds=1))
    output = collect(
        archive,
        api_key="key",
        api_secret="secret",
        policy=policy,
        now=EARLIEST - timedelta(seconds=1),
        quote_captures={},
    )

    assert state.due == ()
    assert state.awaiting_future_count == 1
    assert output["status"] == "completed"
    assert output["awaiting_future_horizon_count"] == 1
    assert output["raw_quote_snapshot_count"] == 0
