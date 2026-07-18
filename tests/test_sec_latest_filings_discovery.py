import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ai_trading_bot.data.history_archive import PointInTimeArchive, load_source_policies
from scripts.collect_alpaca_asset_history import (
    HttpCapture as AssetHttpCapture,
)
from scripts.collect_alpaca_asset_history import collect as collect_assets
from scripts.collect_sec_filing_history import HttpCapture as SubmissionsHttpCapture
from scripts.collect_sec_latest_filings_discovery import (
    FeedHttpCapture,
    collect,
    load_discovery_policy,
    parse_latest_filings_feed,
)
from scripts.collect_security_master_history import (
    HttpCapture as SecurityMasterHttpCapture,
)
from scripts.collect_security_master_history import collect as collect_security_master

ROOT = Path(__file__).resolve().parents[1]


def atom_feed(entries: list[tuple[str, str, str, str, str]]) -> bytes:
    rendered = []
    for accession, cik, form, updated, filed in entries:
        directory = accession.replace("-", "")
        index_url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{directory}/{accession}-index.htm"
        )
        summary = (
            f"&lt;b&gt;Filed:&lt;/b&gt; {filed} &lt;b&gt;AccNo:&lt;/b&gt; {accession} "
            "&lt;b&gt;Size:&lt;/b&gt; 10 KB"
        )
        rendered.append(
            f"""
            <entry>
              <title>{form} - Apple Inc. ({cik}) (Filer)</title>
              <link rel="alternate" type="text/html"
                    href="{index_url}" />
              <summary type="html">{summary}</summary>
              <updated>{updated}</updated>
              <category scheme="https://www.sec.gov/" label="form type" term="{form}" />
              <id>urn:tag:sec.gov,2008:accession-number={accession}</id>
            </entry>
            """
        )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<feed xmlns="http://www.w3.org/2005/Atom">' + "".join(rendered) + "</feed>"
    ).encode()


def feed_capture(payload: bytes, retrieved_at: datetime) -> FeedHttpCapture:
    return FeedHttpCapture(
        payload=payload,
        decoded_payload=payload,
        request_started_at=retrieved_at - timedelta(milliseconds=100),
        retrieved_at=retrieved_at,
        content_type="application/atom+xml",
        content_encoding=None,
        response_metadata={"http_status": 200},
        source_url=(
            "https://www.sec.gov/cgi-bin/browse-edgar?"
            "action=getcurrent&output=atom&count=100&owner=exclude"
        ),
        request_parameters={
            "action": "getcurrent",
            "output": "atom",
            "count": 100,
            "owner": "exclude",
        },
    )


def submissions_payload(accession: str, acceptance_at: str) -> bytes:
    return json.dumps(
        {
            "cik": "0000320193",
            "name": "Apple Inc.",
            "filings": {
                "recent": {
                    "accessionNumber": [accession],
                    "filingDate": ["2026-07-16"],
                    "reportDate": ["2026-06-30"],
                    "acceptanceDateTime": [acceptance_at],
                    "act": ["34"],
                    "form": ["8-K"],
                    "fileNumber": ["001-36743"],
                    "filmNumber": ["261234567"],
                    "items": ["2.02,9.01"],
                    "size": [12345],
                    "isXBRL": [1],
                    "isInlineXBRL": [1],
                    "primaryDocument": ["event.htm"],
                    "primaryDocDescription": ["Current report"],
                },
                "files": [],
            },
        }
    ).encode()


def submission_capture(payload: bytes, retrieved_at: datetime) -> SubmissionsHttpCapture:
    return SubmissionsHttpCapture(
        payload=payload,
        decoded_payload=payload,
        request_started_at=retrieved_at - timedelta(milliseconds=100),
        retrieved_at=retrieved_at,
        content_type="application/json",
        content_encoding=None,
        response_metadata={"http_status": 200},
    )


def seed_discovery_universe(archive: PointInTimeArchive, observed_at: datetime) -> None:
    sec_payload = json.dumps(
        {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}
    ).encode()
    collect_security_master(
        archive,
        user_agent="test test@example.com",
        http_capture=SecurityMasterHttpCapture(
            payload=sec_payload,
            decoded_payload=sec_payload,
            request_started_at=observed_at - timedelta(milliseconds=200),
            retrieved_at=observed_at - timedelta(milliseconds=100),
            content_type="application/json",
            content_encoding=None,
            response_metadata={"http_status": 200},
            event_publication_at=None,
        ),
    )
    assets_payload = json.dumps(
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
                "attributes": [],
            }
        ]
    ).encode()
    collect_assets(
        archive,
        api_key="key",
        api_secret="secret",
        http_capture=AssetHttpCapture(
            payload=assets_payload,
            request_started_at=observed_at,
            retrieved_at=observed_at + timedelta(milliseconds=100),
            content_type="application/json",
            response_metadata={"http_status": 200},
        ),
    )


def test_parse_latest_filings_atom_preserves_updated_but_not_as_decision_time() -> None:
    payload = atom_feed(
        [
            (
                "0000320193-26-000100",
                "0000320193",
                "8-K",
                "2026-07-16T09:01:02-04:00",
                "2026-07-16",
            )
        ]
    )

    entries = parse_latest_filings_feed(payload)

    assert len(entries) == 1
    assert entries[0].accession_number == "0000320193-26-000100"
    assert entries[0].cik == "0000320193"
    assert entries[0].form == "8-K"
    assert entries[0].feed_updated_at.isoformat() == "2026-07-16T09:01:02-04:00"


def test_parse_latest_filings_allows_multiple_parties_for_one_accession() -> None:
    accession = "0001193125-26-304969"
    payload = atom_feed(
        [
            (
                accession,
                "0001419828",
                "424B2",
                "2026-07-16T09:01:02-04:00",
                "2026-07-16",
            ),
            (
                accession,
                "0000886982",
                "424B2",
                "2026-07-16T09:01:02-04:00",
                "2026-07-16",
            ),
        ]
    )

    entries = parse_latest_filings_feed(payload)

    assert len(entries) == 2
    assert {entry.cik for entry in entries} == {"0001419828", "0000886982"}


def test_parse_latest_filings_normalizes_schedule_13_forms() -> None:
    payload = atom_feed(
        [
            (
                "0001214659-26-008613",
                "0000320193",
                "SCHEDULE 13G/A",
                "2026-07-16T09:01:02-04:00",
                "2026-07-16",
            )
        ]
    )

    entries = parse_latest_filings_feed(payload)

    assert entries[0].form == "SC 13G/A"


def test_first_feed_is_baseline_and_next_new_entry_enters_forward_pipeline(
    tmp_path: Path,
) -> None:
    archive = PointInTimeArchive(
        tmp_path / "archive",
        load_source_policies(ROOT / "config" / "data_source_policies.json"),
    )
    policy = load_discovery_policy(ROOT / "config" / "sec_latest_filings_discovery_policy.json")
    first_time = datetime.now(UTC) - timedelta(minutes=10)
    seed_discovery_universe(archive, first_time - timedelta(minutes=1))
    old_accession = "0000320193-26-000100"
    old_entry = (
        old_accession,
        "0000320193",
        "8-K",
        (first_time - timedelta(minutes=1)).isoformat(),
        "2026-07-16",
    )
    first = collect(
        archive,
        user_agent="test test@example.com",
        policy=policy,
        feed_capture=feed_capture(atom_feed([old_entry]), first_time),
        submission_captures={},
    )

    second_time = first_time + timedelta(minutes=5)
    new_accession = "0000320193-26-000101"
    new_entry = (
        new_accession,
        "0000320193",
        "8-K",
        (second_time - timedelta(seconds=30)).isoformat(),
        "2026-07-16",
    )
    acceptance = (second_time - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
    second = collect(
        archive,
        user_agent="test test@example.com",
        policy=policy,
        feed_capture=feed_capture(atom_feed([old_entry, new_entry]), second_time),
        submission_captures={
            "0000320193": submission_capture(
                submissions_payload(new_accession, acceptance),
                second_time + timedelta(milliseconds=100),
            )
        },
    )

    assert first["baseline_discovery_count"] == 1
    assert first["confirmed_forward_event_count"] == 0
    assert second["new_feed_entry_count"] == 1
    assert second["reconfirmed_discovery_count"] == 1
    assert second["confirmed_forward_event_count"] == 1
    assert second["eligible_universe_symbol_count"] == 1
    assert second["filing_event_snapshot"] is not None
    event_payload = json.loads(
        (tmp_path / "archive" / str(second["filing_event_snapshot"]["raw_path"])).read_text()
    )
    row = event_payload["rows"][0]
    assert row["accession_number"] == new_accession
    assert row["first_observation_class"] == "monitored_new_accession"
    assert row["forward_event_eligible"] is True
    assert row["decision_available_at"] == second_time.isoformat()
    assert row["decision_available_at"] != row["acceptance_at"]
    assert row["primary_document"] == "event.htm"
    assert archive.audit().status == "pass"
