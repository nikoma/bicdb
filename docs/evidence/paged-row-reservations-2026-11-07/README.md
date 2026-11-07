# Cold paged-row reservations: source qualified, EHR release still pending

The exact frozen `ef12dec` runtime passed the current 168-migration registry,
restricted SQL identity and patient creation, then failed the real patient meal
API: the sync trigger reported `pocket_sync_changes_version_seq` missing. The
retained failing API report is entirely synthetic. No clinical data or original
keys entered this build host; no production cutover occurred.

The sequence existed on durable pages. READ COMMITTED reservation consulted
only the resident registry, which omits untouched cold paged rows. Update and
row-lock paths shared that assumption. The fix resolves an untouched durable
baseline as version zero while resident mutation/tombstone chains remain
authoritative. It keeps the row locks, privilege checks, snapshot fences and
missing-row behavior. Updated/deleted rows still rematerialize; fixed snapshots
still reject changed rows.

On source `032c7c1df192e9fc1812867e6d787bc6059d5cb4`, the three new core
regressions pass. Before the fix, the cold-baseline case fails while its two
safety cases pass. The SQL identity/trigger/cold-reopen regression fails before
the fix at the second insert after reopen with the same missing-sequence error;
it passes after the fix, including subsequent locking/update and another reopen.
Compilation errors are excluded by the controller and never count as causal proof.

Broad qualification passes 452 core library tests (three explicit benchmarks
ignored), 762 SQL library tests (four existing ignored cases), 25 transaction,
mutation-grant, rollback and paged-concurrency cases across four targets,
187 protocol tests and 47 application-host cases across seven targets. The one
root/fixture-UID isolation case is ignored in this invocation and is not claimed
as newly executed. The core library's nested child probe is not counted twice.
Complete outputs, source-manifest hash and counts are retained in `summary.json`.

The core fix was merged and pushed immediately after these gates. The separate
offline SQL CLI persisted-mode fix is still being verified. A new runtime must
still pass the actual EHR API, full hosted applications, monitoring, browsers,
capacity, clinical import/recovery and physical three-host HA before admission.
A source regression pass is not a production deployment.

The owner now requires owned servers instead of new DigitalOcean hosts. Physical
preparation is tracked as infrastructure EHR-HA-001 on S1/S3/S4. The earlier DO
account refusal remains historical evidence, not a dependency of that plan.
