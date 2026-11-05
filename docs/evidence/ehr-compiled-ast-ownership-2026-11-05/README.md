# Compiled AST ownership for SQL caches

Qualification base: 3975f15c594357d84a8a405d21251fe9ba5c7d95, fetched after
including today's Oracle foreign-key assignment, writer-fence and authoritative
parent point-read updates. The real EHR's repeated signed goal reads still fail
on the frozen 93a6cda candidate even after the earlier argument-borrowing repair.
A new reduced regression reproduces a numeric/JSONB stale-type result inside a
real compiled routine scope without the EHR or patient data.

Address-based caches now require membership in the active compiled AST. Each
routine compilation and deep clone has a fresh identity and a lazily populated
node inventory. Temporary expressions, functions, queries and rewrites remain
uncached; owned expressions retain memoization. Row typing, CASE validation,
result typing, access plans, function dispatch and binary-operand caches use
that ownership proof. Cache identity no longer depends on a freed IR address.

The parsed-statement cache also owns stable nodes. Cached CALL arguments retain
that owner's scope while the callee owns its body/defaults. Rebinding literals
between integer, bigint and numeric classes starts a fresh statement identity.
Row-column typing avoids a database-wide lookup for ordinary column names;
the context-free typer still recognizes reserved identity/system names before
row relations are consulted. This preserves the existing CALL typing test
without retaining cloned aggregate argument addresses or weakening its assertion.
The existing test additionally checks integer/decimal/bigint argument changes.

Evidence (terminal blank lines normalized in captured logs):

- negative.log: the new ownership regression fails on 93a6cda, returning numeric
  for a temporary node overwritten with a JSONB expression. The positive test
  also checks row typing, preserved memo reuse, routine clones and temporary
  function dispatch changing between two stored functions.
- incomplete-performance.log: the first broader run fails the existing CALL
  typing performance assertion (699 pass, one fail, three ignored). A partial
  cached-statement repair still performs one unnecessary catalog lookup on a
  cloned aggregate column. Test-only tracing identifies it and is removed from
  the final source. These are failed intermediate repairs, not accepted gates.
- call-performance.log: the unchanged performance assertion and the additional
  numeric-class value checks pass after the complete repair.
- unit.log: all 707 SQL unit tests pass; three existing benchmarks are ignored.
- focused-a.log: 63 Carrier, JSON SRF, RLS-depth, row-lock and type-contract tests
  pass. focused-b.log: 32 foreign-key, aggregate, stack and routine-identity
  tests pass. Total: 95 focused SQL tests.
- protocol.log: all 181 wire-protocol tests pass.
- Targeted rustfmt and git diff --check pass.

Builds use bounded concurrency, no incremental compilation and an active guard
for 12 GiB available RAM / 8 GiB target-filesystem headroom. A focused build is
interrupted by the filesystem guard (exit 99), then resumed successfully.
Cleanup removes only task-owned interrupted archives, orphaned cache entries
and test debug information; reusable dependencies, active executables and the
current frozen native candidate are preserved. Separate cleanup batches reclaim
1,345,697,478 interrupted bytes, 6,473,947,045 orphaned cache bytes and
11,166,901,992 test debug bytes. The retained RAM cache is about 31.4 GB.

Rebuilt real EHR workflow qualification remains open. No production data or services have changed. The fix
is not evidence that all EHR clinical workflows or the migration are complete.
