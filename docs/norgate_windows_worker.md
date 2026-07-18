# Norgate private Windows worker

Norgate Data Updater (NDU), its proprietary database, and the `norgatedata`
Python package run only inside the licensed Windows machine or Windows VM. This
repository must not receive or mirror the Norgate database, price rows, symbols,
asset IDs, constituent rows, or export files.

The first worker action is a metadata-only entitlement check. It verifies NDU is
running, the US Equities / US Equities Delisted / US Indices databases are present,
stable identity round-trips work, the historical S&P 500 constituent function returns
observations, and unadjusted plus total-return price modes are available. The report
contains only booleans, counts, software versions, and the local NDU update time.
It does not calculate alpha or make a strategy eligible.

## Run inside Windows

Keep NDU open and copy only `scripts/run_norgate_platinum_validation.py` to a
private local Windows folder (code only; never the Norgate database). For example,
after saving it as `%USERPROFILE%\\norgate-worker\\run_norgate_platinum_validation.py`,
run:

```bat
py %USERPROFILE%\norgate-worker\run_norgate_platinum_validation.py
```

The report is written by default to:

```text
%USERPROFILE%\norgate_private_reports\platinum_validation_latest.json
```

Keep this report and all Norgate-related files private to the licensed Windows worker.
The trial verifies integration only: its limited history cannot clear the project's
survivorship-free historical-research or point-in-time fundamental-data gates.

## Automate the VM-local health check

The VM can check its Norgate entitlement automatically without exposing its database
to the Mac. Copy these two **code files only** into the same private Windows folder:

- `run_norgate_platinum_validation.py`
- `install_norgate_private_worker_task.cmd`

Set the Windows VM timezone to `America/New_York`, keep the VM powered on and the
Windows user logged in, then open **Command Prompt** in that folder and run:

```bat
C:\Users\lzein\norgate-worker\install_norgate_private_worker_task.cmd
```

This installs the `AI-TRADING-BOT Norgate Validation` task for 6:45 AM every
weekday. It runs under the current non-administrator Windows account and writes only
the existing metadata-only report to `%USERPROFILE%\norgate_private_reports`.
It never mounts a shared raw-data directory, starts a network service, or gives the
Mac remote access to NDU.

To run the just-installed task once immediately, use the Windows built-in command:

```bat
schtasks /run /tn "AI-TRADING-BOT Norgate Validation"
```

The task is intentionally a local health check, not an automated historical
backtest. A separate, vetted Windows-only research runner is required before
strategies can use licensed history; its output must remain non-reversible and
license-reviewed.

## Private historical early screen

`run_norgate_liquidity_reversal_early_screen.py` is the first research runner.
It evaluates a distinct non-momentum mechanism: abnormal-volume liquidity-shock
reversal. It uses exactly three symbols (SPY, QQQ, IWM), three chronological folds,
1/5/20-trading-day holding periods, 5/25/50 bps round-trip costs, and nine
predeclared parameter configurations per family. Trades use a full next-day delayed
entry and never overlap.

Copy the file into the same private VM-local folder and run it there:

```bat
py C:\Users\lzein\norgate-worker\run_norgate_liquidity_reversal_early_screen.py
```

It writes a report and append-only ledger only to `%USERPROFILE%\norgate_private_reports`.
The report includes VOO total-return benchmark data, exact gate/rejection reasons,
and data-quality verdicts, but no raw Norgate records. Because verifiable historical
vendor-delivery/revision timestamps are not yet archived, it will correctly classify
the outcome as `blocked` and will not calculate or report alpha or run a deeper
backtest. This is a required integrity gate, not a strategy judgment.

## Required availability audit

Run `run_norgate_historical_availability_audit.py` before a deeper Norgate
backtest. The Norgate API exposes the local time of the latest database/price update,
but not an auditable historical delivery timestamp for every old bar. The audit fails
closed until a dated vendor statement covers the historical period, edition schedule
and timezone, revision policy, and per-record delivery/revision coverage.

```bat
py C:\Users\lzein\norgate-worker\run_norgate_historical_availability_audit.py
```

Ask Norgate support: “For US Equities historical daily price and volume data, please
provide the historical EOD edition publication schedule/timezone, revision policy,
and whether NDU/API exposes or can supply historical delivery timestamps and revision
history for each date in the research period.” Do not treat a current update timestamp
as evidence of what was available years ago.

## Forward provenance from today

`record_norgate_forward_provenance.py` creates a private immutable cache of the
latest two daily records for SPY, QQQ, IWM, and VOO after the NDU update. Each raw
snapshot remains in `%USERPROFILE%\norgate_private_history`; its append-only manifest
chains hashes and the summary report contains only counts, timestamps, and hashes.
Every snapshot records the retrieval timestamp, NDU source-update timestamp,
`TOTALRETURN` adjustment mode, a SHA-256 hash of the canonical response content,
and a SHA-256 hash of the complete snapshot envelope. Raw snapshots and individual
manifest records are created with write-once semantics; an existing path is never
overwritten. The JSONL manifest is only a convenient index—the separately stored,
hash-chained manifest records are the immutable audit evidence.
It establishes genuine availability evidence **from today forward**, but never clears
the historical-backtest blocker for dates before recording began.

Use `run_norgate_daily_private_cycle.cmd` to run validation and this recorder in the
existing scheduled task. Copy both files to the same private VM folder, then update
the task action to point to the CMD file. Do not put the private history folder in a
VM shared folder or cloud drive.

## Optional Mac metadata relay

`read_norgate_vm_safe_reports.py` reads only the two approved JSON summaries through
SSH, removes symbol-level counts and other unapproved fields again, and writes its
own local health-only summary to `outputs/norgate_vm_reports/latest.json`. It never
requests a Norgate database, private cache, manifest, price row, asset ID, symbol
list, or constituent data.

Set the private VM's current bridged address in
`config/norgate_vm_report_reader.json`. When that address changes, update only the
`host` value. A Mac `launchd` job can lack the Terminal app's local-network
permission, so start the weekday relay from the user's normal **Mac Terminal** with:

```bash
cd /Users/louieeissa/AI-TRADING-BOT
bash scripts/start_norgate_vm_report_reader_terminal_daemon.sh
```

It fetches once immediately when started, then retries every 15 minutes from 10:35
PM Eastern on weekdays until it obtains that day's report. This is 15 minutes after
the VM's 7:20 PM Pacific collection window. The Mac and VM must be awake and Windows
must remain logged in. Restart it after a Mac reboot. Check only the sanitized result
with:

```bash
cat outputs/norgate_vm_reports/latest.json
```
