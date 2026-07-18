"""Verify the raw hashes, immutable event files, and manifest chain."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ai_trading_bot.data.history_archive import PointInTimeArchive, load_source_policies

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=ROOT / "outputs" / "high_integrity_history",
    )
    parser.add_argument(
        "--policies",
        type=Path,
        default=ROOT / "config" / "data_source_policies.json",
    )
    args = parser.parse_args()
    archive = PointInTimeArchive(args.archive_root, load_source_policies(args.policies))
    audit = archive.audit()
    print(json.dumps(audit.to_dict(), indent=2, sort_keys=True))
    if audit.status != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
