# Preserve absent-key conflict boundaries across snapshot refresh

A READ COMMITTED transaction that reads a new key as absent records version zero.
If another transaction inserts that key, the first transaction must fail with a
retryable conflict when it tries to commit a competing write. Previously, a later
snapshot refresh could silently authorize that overwrite: `TxWrite.statement_snapshot`
used zero both for a real absence observation and for an unspecified boundary, and
commit validation substituted the newly advanced applied watermark.

This refresh occurs at ordinary protocol statement boundaries and after independently
committed work such as sequence allocation or signed request-context nonce handling.
The defect is therefore a transaction-integrity issue, independent of Carrier source.

The fix preserves a genuine zero observation using the existing transaction observation
map when validating a zero-valued write. A blind optimistic insert starting at snapshot
zero also records that boundary. Nonzero explicit statement boundaries and the legacy
untracked fallback retain their behavior. No write/WAL wire fields, independent commit
semantics, authorization rules or replay-retention requirements change.

The base is `b40ac848a4fddf617f5849359142d3f15af0df36`; tested fix is `c6effac`.
All heavy commands ran on the bounded remote host, serialized with the shared Cargo
lock, four build jobs, one test thread, debug information and incremental compilation
disabled. Active memory/disk guards stopped only task-owned processes if required;
no resource interruption occurred.

| Regression | Before | After |
| --- | --- | --- |
| Refresh after absent-key write | Incorrectly overwrites winner | Retryable conflict |
| Refresh between absent read and write | Incorrectly overwrites winner | Retryable conflict |
| Blind optimistic insert at genuine snapshot zero | Incorrectly overwrites winner | Retryable conflict |
| New uncontested key + independent write + reopen | Pass | Pass |
| SQL `nextval` commits independently before parent COMMIT | Parent incorrectly commits | Parent reports SQLSTATE 40001; sequence allocation survives |

The four core regressions pass, the SQL regression passes, and twenty focused existing
checks pass: isolation (5), rollback marks (2), transaction receipts including WAL append
failure handling (9), default/experimental durability isolation (1), and floored snapshot
and delta repair checks (3). These are focused correctness gates, not a full workspace or
application acceptance claim. Retained application gates are run independently against
the combined engine build.

[`results.json`](results.json) records commands, flags and complete log hashes. The text
files retain test result/failure excerpts; unchanged compiler warnings are omitted from
passing excerpts. The initially passing SQL Cargo run's mutable shell wrapper later had
an orchestration parse error; its unchanged test was rerun with an immutable wrapper and
clean exit before the final SQL result recorded here. No acceptance tests were edited.
