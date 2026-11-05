# Preserve the connection runtime across exclusive transaction retry

Base: `be5e9701b194f5c5e084cf02fed00103a9134cab`, including upstream November 5
persistent BLOB changes from `ca96174088ca527eaa2ef2acb8757bad219192b0`.

The EHR's life-factor and practice-session writes consume a signed one-use
permission in a BEFORE trigger. Their AFTER trigger writes a mobile-sync row
with an identity sequence. Sequence allocation requires exclusive database
access. The wire server correctly rolls back the first attempt and retries
exclusively, but previously constructed the retry session without its connection
runtime. `pg_backend_pid()` and `pg_locks` then lost the connection's real
identity and advisory binding, so the otherwise valid permission was refused.

The retry now uses the existing connection-session constructor, then restores
the statement's current GUC/catalog/transaction state. It preserves the runtime
and snapshot floor without bypassing or weakening any permission check.

`exclusive_transaction_retry_preserves_backend_identity_and_advisory_locks`
reproduces this over real Parse/Bind/Describe/Execute messages. It binds a
one-use permission to the backend, transaction and two-key advisory lock, then
inserts into a BEFORE/AFTER-triggered table whose change feed has a serial key.
It verifies one item, one feed row, a consumed permission, successful commit,
and refusal to reuse authority for a second item.

- `negative.log`: unchanged retry code fails with SQLSTATE 42501, missing
  one-use operation permission (exit 101).
- `positive.log`: all 180 pgwire protocol tests pass, including the regression.
- `rustfmt --check` and `git diff --check` pass.

The task-owned build uses two jobs, no incremental compilation, stripped test
debug information, and active RAM/disk monitoring. No production data was used.
The actual hosted companion workflow still needs a rebuilt binary and rerun;
this result alone is not EHR acceptance or production cutover.
