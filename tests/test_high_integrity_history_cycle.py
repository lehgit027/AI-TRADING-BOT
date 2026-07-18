import socket
from pathlib import Path
from urllib.error import URLError

from ai_trading_bot.data.history_archive import (
    ArchiveAudit,
    PointInTimeArchive,
    load_source_policies,
)
from scripts import run_high_integrity_history_cycle as cycle
from scripts.collect_sec_filing_history import WatchedIssuer


def test_cycle_is_research_only_and_runs_independent_collectors(
    tmp_path: Path,
    monkeypatch,
) -> None:
    policies = load_source_policies(
        Path(__file__).resolve().parents[1] / "config" / "data_source_policies.json"
    )
    archive = PointInTimeArchive(tmp_path / "archive", policies)
    calls: list[str] = []

    monkeypatch.setattr(
        archive,
        "audit",
        lambda: ArchiveAudit("pass", 0, 0, 0, ()),
    )
    monkeypatch.setattr(
        cycle,
        "collect_security_master",
        lambda archive, user_agent: calls.append("security") or {"status": "captured"},
    )
    monkeypatch.setattr(
        cycle,
        "collect_alpaca_assets",
        lambda archive, api_key, api_secret: calls.append("alpaca") or {"status": "captured"},
    )
    monkeypatch.setattr(
        cycle,
        "load_total_return_policy",
        lambda path: type("Policy", (), {"lookback_days": 30, "future_days": 30})(),
    )
    monkeypatch.setattr(
        cycle,
        "collect_voo_corporate_actions",
        lambda archive, api_key, api_secret, policy, start, end: (
            calls.append("voo_actions") or {"status": "captured"}
        ),
    )
    monkeypatch.setattr(cycle, "load_discovery_policy", lambda path: object())
    monkeypatch.setattr(
        cycle,
        "collect_sec_latest_filings",
        lambda archive, user_agent, policy: (
            calls.append("latest_filings") or {"status": "captured"}
        ),
    )
    monkeypatch.setattr(
        cycle,
        "collect_sec_filings",
        lambda archive, user_agent, issuers: calls.append("filings") or {"status": "captured"},
    )
    monkeypatch.setattr(cycle, "load_document_policy", lambda path: object())
    monkeypatch.setattr(
        cycle,
        "collect_sec_filing_documents",
        lambda archive, user_agent, policy: calls.append("documents") or {"status": "completed"},
    )
    monkeypatch.setattr(
        cycle,
        "collect_sec_insider_transactions",
        lambda archive: calls.append("insider_transactions") or {"status": "completed"},
    )
    monkeypatch.setattr(cycle, "calendar_covers", lambda archive, start, end: False)
    monkeypatch.setattr(
        cycle,
        "collect_market_calendar",
        lambda archive, api_key, api_secret, start, end: (
            calls.append("calendar") or {"status": "captured"}
        ),
    )
    monkeypatch.setattr(cycle, "load_timing_policy", lambda path: object())
    monkeypatch.setattr(
        cycle,
        "resolve_first_tradable",
        lambda archive, api_key, api_secret, policy: (
            calls.append("first_tradable") or {"status": "completed"}
        ),
    )
    monkeypatch.setattr(cycle, "load_observation_policy", lambda path: object())
    monkeypatch.setattr(
        cycle,
        "plan_forward_observations",
        lambda archive, policy: calls.append("observation_plan") or {"status": "completed"},
    )
    monkeypatch.setattr(
        cycle,
        "collect_forward_observations",
        lambda archive, api_key, api_secret, policy: (
            calls.append("forward_observations") or {"status": "completed"}
        ),
    )
    monkeypatch.setattr(
        cycle,
        "reconcile_sec_filing_total_returns",
        lambda archive, total_return_policy_path: (
            calls.append("total_returns") or {"status": "completed"}
        ),
    )

    output = cycle.run_cycle(
        archive,
        environment={
            "SEC_USER_AGENT_EMAIL": "test@example.com",
            "SEC_USER_AGENT_NAME": "test",
            "AI_TRADING_MARKET_DATA_API_KEY": "key",
            "AI_TRADING_MARKET_DATA_API_SECRET": "secret",
        },
        issuers=(WatchedIssuer("AAPL", "0000320193", "Apple Inc."),),
    )

    assert output["status"] == "completed"
    assert output["research_only"] is True
    assert output["trading_actions_enabled"] is False
    assert output["active_profile_changed"] is False
    assert calls == [
        "security",
        "alpaca",
        "voo_actions",
        "calendar",
        "latest_filings",
        "filings",
        "documents",
        "insider_transactions",
        "first_tradable",
        "observation_plan",
        "forward_observations",
        "total_returns",
    ]


def test_cycle_step_retries_transient_dns_failure(monkeypatch) -> None:
    attempts = 0
    delays: list[int] = []

    def operation() -> dict[str, object]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise URLError(socket.gaierror(8, "nodename nor servname provided"))
        return {"status": "captured"}

    monkeypatch.setattr(cycle.time, "sleep", lambda delay: delays.append(delay))

    output = cycle._run_step("networked_collector", operation)

    assert output["status"] == "captured"
    assert output["attempt_count"] == 2
    assert output["transient_retry_errors"]
    assert delays == [2]
