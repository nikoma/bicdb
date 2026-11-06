# Correlated SQL table functions in native EHR workers

Qualification base b984572, with a39bc42 integrated before commit. The latter
adds Oracle export tooling and documentation; no Rust engine changes. This fix
supports explicit and implicit lateral stored SQL table functions through the
existing correlated query executor. Authored AST arguments are parsed once per
join; row values are bound, never interpolated into SQL. Declared output types,
aliases, LEFT/INNER behavior, SECURITY DEFINER identity and EXECUTE checks are
preserved. Function bodies receive declared arguments rather than caller slots.

The frozen f270765 EHR worker advanced past its prior row-lock failure, then
failed with 0A000 at LEFT JOIN LATERAL
carrier_private.walknorth_delivery_configuration(r.org_id). This is the reduced
regression's real application origin. A native rebuild and actual worker replay
remain required; these engine tests do not establish production admission.

Evidence:

- negative.log: both new executing regressions fail with 0A000 without the fix.
- positive.log: both pass after the fix. The final fresh-a run also includes
  the added post-call assertions that the caller role is restored and direct
  access to the protected table remains forbidden.
- unit.log: 713 SQL unit tests pass; four existing benchmarks are ignored.
- ehr-bicdb-lateral-routine-fresh-a.log: 68 focused tests pass.
- ehr-bicdb-lateral-routine-fresh-b.log: 34 focused tests pass.
- ehr-bicdb-lateral-routine-fresh-c.log: 28 focused tests pass.
- ehr-bicdb-lateral-routine-protocol.log: 181 wire tests pass.
- The four artifact JSONL records audit the actual newly linked executables,
  SHA256 hashes and the unique current implementation constant before stripping.
- rustfmt --check --edition 2021 on both changed engine files and the new test,
  and git diff --check pass. Captured log trailing whitespace is normalized.

An initial fixture attempted duplicate short routine names in separate schemas,
which BicDB currently refuses. negative.log is the corrected executing control;
this change does not claim to fix that separate catalog limitation.

## Validation correction

Artifact cleanup had advanced executable timestamps when stripping debug data.
Cargo consequently reused some older linked integration executables. Those
earlier focused runs are discarded as revision evidence, including the focused
counts recorded in the two preceding EHR engine evidence directories. Their
original logs are retained and READMEs annotated. The SQL unit executable was
freshly compiled and contains the current implementation.

All 29 task-owned focused/wire executables were retired before the final gates.
That removed 10,199,516,552 logical bytes and 3,810,622,584 orphan-cache bytes;
22,629,319,359 reusable dependency bytes were retained. The replacement 130
focused and 181 wire tests were freshly linked and individually audited.
Cleanup now restores original atime/mtime after debug stripping and protects
active executable inodes. Builds use at most two jobs, incremental compilation
disabled, and active 12 GiB available-RAM / 8 GiB filesystem headroom guards.

Actual EHR application failures, signed-in browser qualification, the combined
workload's writer-admission timeout, full production data rehearsal, recovery
and cutover remain open. Production remains PostgreSQL.
