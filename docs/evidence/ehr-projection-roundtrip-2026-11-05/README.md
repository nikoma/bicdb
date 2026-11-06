# EHR projection names and lossless array text casts

Status: engine regressions verified; rebuilt EHR insight and rollback workflows
remain open. Production is PostgreSQL. No production admission is claimed.

Qualification base: 38130c7, including the latest Oracle snapshot-copy and stored
source export updates. The intervening Oracle commits since 6176c47 change
example tooling and documentation; the Rust engine base is unchanged.

The real provider /patients/{id}/changes request fails with 42703 because an
unaliased string literal becomes a column name. A dot inside the value is then
mistaken for a binding qualifier during a CTE/UNION wildcard expansion. Both
row and record projection paths now use the same authored-expression names.
Literal contents remain values; stored SQL functions retain declared outputs.

A real reverse-transfer rehearsal separately exposes 22P02 when a NULL array is
cast to text. Record, row and aggregate fast paths now preserve SQL NULL before
array formatting. SQL casts also retain nondefault array subscripts, including
multidimensional bounds. The wire codec retains its existing body/header split,
so bound prefixes are not duplicated. Source type checks remain unchanged.

Evidence:

- negative.log: all four final regressions execute and fail before the repairs:
  two CTE/routine projections, NULL-array text and lost nondefault bounds.
- negative-artifacts.json: exact test source, executable and CLI-graph SQL/core
  library SHA256 hashes. The control was linked directly with pinned rustc
  1.96.0 and the completed 6176c47 CLI's libraries/native search paths. It did not
  use the newer working-tree source or mutate the Cargo target.
- positive.log: all four pass through Cargo after the repairs, including repeated
  calls, empty sources, joined and aggregate NULL arrays, and multiple dimensions.
- unit.log: 713 SQL unit tests pass; four existing benchmarks are ignored.
- focused.log: 134 focused tests pass, including the four new regressions and
  Carrier, JSON SRFs, FK timing, locking, RLS/owner identity, types and routines.
- protocol.log: all 181 wire-protocol tests pass.
- fresh-artifacts.jsonl: each focused/wire executable and linked SQL library is
  hashed before debug stripping. All replaced old test executables were retired;
  the already-current new-regression executable may be reused. Cleanup preserves
  timestamps, protects active executables and retains reusable dependencies.
- Targeted rustfmt --check --edition 2021 and git diff --check pass. Captured log
  trailing whitespace is normalized. Focused/protocol files contain the exact
  test-output portions of the full build logs retained under /root on the build
  host; repeated compiler dependency JSON and warnings are omitted from excerpts.

The first partial repair passed one of three tests, but the record projection
path still used the literal value as its name. An intermediate assertion also
overconstrained optional internal SqlResult column_types; that assertion was
removed, while actual wire and PostgreSQL round-trip gates remain required.
The final four-test control and positive run use the committed test source.
Initial direct control compile attempts used the host's newer default rustc,
then lacked Lua's native link search path. These compile failures are not causal
controls; negative.log follows a successful pinned compile.

The final native rebuild must include these repairs. An earlier CLI-only
optimized 6176c47 diagnostic already proves the prior lateral-function repair
lets the actual scheduled care-plan worker finish, but it predates these fixes.
The separate and then unified optimized supervisor attempts were intentionally
interrupted to include the newly reproduced defects; neither is counted as a
successful native candidate. Active build guards preserve at least 12 GiB
available RAM and 8 GiB target-filesystem headroom. Subsequent cleanup removed
899,466,720 interrupted object bytes, preserving the immutable CLI diagnostic
and 22,629,319,359 reusable dependency bytes.

Combined workload writer admission, complete clinical/browser qualification,
full production data reconciliation, replay conversion and recovery/cutover
remain separate acceptance gates.
