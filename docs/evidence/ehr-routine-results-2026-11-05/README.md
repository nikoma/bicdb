# EHR routine evaluation and result contracts

Qualification base: a46ab7ad9e26737e379e11e2e6473473abdc2f25. This includes the
November 5 Oracle foreign-key migration and constraint catalog updates fetched
before the final validation runs. Before merging, upstream c4fa9a7 was also
integrated; its additional changes are Oracle example tooling and documentation,
with no Rust engine changes. The prior frozen b90bbb0 EHR run passed 30 of
32 hosted checks, with five failed clinical assertions in two contracts.

Three reduced regressions reproduce the remaining SQL roots independently of
patient data:

- Mixed stored and builtin calls were partially executed by the hoisting fast
  path before it fell back to the generic evaluator. Warm calls could repeat
  writes, producing duplicate goals and false version conflicts. Preflight the
  entire expression before running any call, and refuse a missing routine after
  execution has begun instead of replaying side effects.
- SQL functions with OUT/INOUT parameters can supply one row in FROM without
  RETURNS SETOF. Preserve declared output names and types, take the first body
  row, and produce one null row for an empty body. Existing SQL-language,
  EXECUTE privilege and definer-identity checks remain in place.
- Row expression casts discarded declared routine parameter types, so date + 1
  was mistaken for integer before a timestamp cast. Carry slot type information
  into the cached row binding scope and consult that bound type for casts.
  Column resolution still takes precedence over a same-named variable, and
  unsupported integer-to-timestamp casts still fail with SQLSTATE 42846.

Evidence (terminal blank lines normalized in captured logs):

- negative.log: all three new tests fail before the fixes on 9b49994. The warm
  command returns 3 for call 2, the scalar OUT source fails with 0A000 and the
  date arithmetic fails with 42846.
- positive.log: all three pass with the fixes, including repeated exact write
  counts, named/aliased typed outputs, empty and multirow bodies, INOUT values,
  EXECUTE denial/grant and the invalid integer cast refusal.
- unit.log: 713 SQL unit tests pass; four existing benchmarks are ignored.
- focused-a.log: 66 Carrier, JSON SRF, domain, type and new regression tests pass.
- focused-b.log: 37 FK, aggregate, stack, row-lock, RLS and owner-identity tests pass.
- typed.log: 23 composite, base-type and routine-privilege tests pass.
- protocol.log: all 181 wire-protocol tests pass.
- Targeted rustfmt and git diff --check pass.

An initial latest-source run failed because the new upstream catalog test module
had not been copied to the build host (incomplete-source-delivery.log). A full
source sync corrected delivery; unit.log is the completed latest-source run.
A focused batch was interrupted by the active filesystem guard (exit 99,
interrupted-focused.log), not counted as a passing gate. Bounded batches resumed
successfully after removing 4,104,608 interrupted bytes and 14,883,211,368 bytes
of task-owned test debug data. Reusable dependency artifacts and frozen native
candidates were preserved; the retained cache was 26,439,941,943 bytes.
Builds use at most two compiler jobs, no incremental compilation and an active
12 GiB available RAM / 8 GiB target-filesystem headroom guard.

The fixes still require a rebuilt native EHR candidate and a fresh full hosted
clinical run. Production remains on PostgreSQL; no clinical migration or cutover
is claimed by these engine tests.
