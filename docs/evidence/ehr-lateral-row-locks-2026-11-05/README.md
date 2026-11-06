# Base-table row locks alongside lateral functions

Qualification base: ebf3bc194982dd7fe35f57e15ef08c5447eb886c. The latest Oracle
snapshot-shape and resumed verification tooling is included. Its Rust engine
sources are identical to the prior fc55e81 revision before this repair.

The actual EHR care-plan worker fails with 0A000 on a candidate CTE joining
journey_node_runs, journey_assignments and LATERAL jsonb_array_elements, using
FOR UPDATE SKIP LOCKED without OF. PostgreSQL 18.4 accepts that query shape.
The native locking planner incorrectly treats the JSON function as a base table
that must itself be locked.

Unqualified locking now skips FROM-function inputs while preserving every base
relation's privilege, RLS, nullable-side, primary-key, row-lock and contention
checks. Explicit OF against the function remains a 0A000 refusal. Derived table
and view restrictions are unchanged; this does not make them silently unlocked.

Evidence (terminal whitespace normalized):
- postgresql-reference.log: a temporary table in a rolled-back transaction shows
  the real PostgreSQL lateral/default-lock query succeeds and explicit function
  locking is refused. No application records or schema objects are changed.
- negative.log: both new behavior tests fail without the repair: default locking
  rejects the function source and explicit OF reports the wrong SQLSTATE.
- positive.log: all six row-lock tests pass. The new contention test holds the
  first job, verifies SKIP LOCKED applies before LIMIT, and proves both remaining
  base-table locks block another session until rollback. The other new test
  preserves SELECT-only role refusal and explicit function-target denial.
- unit.log: 713 SQL unit tests pass; four existing benchmarks are ignored.
- focused.log: 57 Carrier, JSON SRF, RLS-depth and row-lock tests pass.
- protocol.log: all 181 wire-protocol tests pass.
- Targeted rustfmt and git diff --check pass.

incomplete-test-fixture.log is an initial test compile failure (a Json test value
needed Arc conversion); it is not a causal negative test. negative.log is the
corrected, executing negative control. Builds use two compiler jobs, no
incremental compilation and active 12 GiB RAM / 8 GiB filesystem headroom guards.
Only task-owned frozen mutable binary aliases and obsolete test debug data are
retired; immutable candidates and reusable dependencies remain available.
Cleanup reclaims 4,674,423,000 bytes of obsolete test debug information and retires
1,056,058,104 bytes of mutable native binary aliases after verifying their frozen
copies. The retained reusable RAM cache is 26,439,941,943 bytes.

The engine fix still requires a native rebuild and actual worker replay. Other
EHR application repairs, full hosted/browser qualification, production transfer
and cutover remain separate gates. Production is still PostgreSQL.

Validation correction (November 5): subsequent artifact inspection found that
our debug stripping advanced executable timestamps, allowing Cargo to reuse
some older linked integration executables. The focused integration counts above
are retained as historical output but are withdrawn as revision qualification.
They are superseded by the explicitly relinked 130 focused and 181 wire tests in
../ehr-lateral-stored-functions-2026-11-05/. The 713-test SQL unit binary was
freshly compiled. Causal regression controls and hashed native candidate runs
remain separate evidence. Cleanup now restores original executable timestamps;
the replacement gates audit every linked binary for the new implementation.
