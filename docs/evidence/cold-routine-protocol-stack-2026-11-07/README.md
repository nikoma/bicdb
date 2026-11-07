# Cold monitoring compilation on the native protocol worker

The complete public WalkNorth monitoring validator crashed the ordinary native
protocol worker on source `93e40a750804eac369f2739e68bdea8d164a789d`.
The optimized engine and the development supervisor both reproduced the failure.
Only public source and synthetic fixtures were used; production remained on
PostgreSQL and no clinical data was copied to the build host.

Routine IR caches are thread-local. The earlier validator test created and
executed declarations on the same thread, warming helper compilation before
nested evaluation. A cold SQL-session test still passes without this patch;
that control is not causal evidence. The new extended-protocol regression uses
persisted declarations, a new ordinary **2 MiB** worker, a JSONB parameter and
the actual application validation expression. Without the fix it aborts with
a stack overflow. Its stack size and application assertions are unchanged.

The first trace reached a synthetic CREATE TABLE used to parse a routine type.
Parsing that type directly removes unnecessary statement/column/constraint
parser frames and rejects trailing non-type input. That change alone still
overflowed later in expression compilation; the failed run and second trace
are retained. Structural interpreter dispatch now calls a separate, non-inlined
leaf-statement function. Nested conditions and loops no longer retain unrelated
SQL-result/exception-message temporaries while cold helpers compile. Statement
authority, rollback, control flow, cancellation and clinical validation remain
on their existing paths.

The complete patch passes:

- The focused cold protocol regression on its unchanged 2 MiB thread.
- The SQL library: **763 passed**, zero failures, four existing ignored tests.
- Ten routine/domain integration targets: **33 passed**, zero failures.
- Native protocol integration: **188 passed**, zero failures, plus the actual
  procedural migration protocol test.
- Protocol library: **140 passed**, zero failures.
- `cargo fmt --all -- --check` and `git diff --check`.

The verification receipt checks all 1,408 original build/test/fixture inputs
against the immutable base and the five-file patch, including its one new
integration file. Each Cargo invocation used one job, disabled incremental
compilation and the active 12 GiB RAM / 8 GiB disk headroom guard. Raw failed and
passing evidence is retained here. Ignored tests were not run.

These are source regression results. A new frozen native runtime, complete EHR
workflows, clinical transfer/recovery and production cutover remain separate
requirements. Production remains on PostgreSQL; HA is deferred by the owner.
