# Native before/after intervention regression

Status: engine verified; complete hosted EHR and production cutover remain open.
Latest qualification base: 7948bd1, including today's Oracle installation evidence.
The intervening Oracle evidence commits do not change the Rust engine or lockfile.

The actual EHR intervention_effect SQL function fails with 0A000 when at least
two readings make its regr_slope expression execute. BicDB did not recognize or
implement that aggregate. It now computes the two-input least-squares slope
using centered accumulators across record, row and grouped/bound paths. Only
non-null pairs participate; empty sets and constant X yield SQL NULL. DISTINCT
applies to float8 pairs, FILTER applies before accumulation, qualified names and
numeric input checks are supported. Large epoch axes avoid uncentered sum-of-
squares cancellation. Nonfinite inputs match the tested PostgreSQL behavior;
finite overflow produces an error rather than a fabricated clinical trend.
Window forms remain explicitly unsupported, as before.

The semantics were checked against PostgreSQL's primary reference:
https://www.postgresql.org/docs/current/functions-aggregate.html#FUNCTIONS-AGGREGATE-STATISTICS-TABLE
The attached constant-only reference uses an isolated PostgreSQL 16.15 container,
without clinical data or an external mail provider.

- negative.log: all four final SQL regressions fail before the aggregate repair.
  The control is directly linked with pinned rustc 1.96.0 against retained,
  hashed 6176c47 CLI libraries. It does not compile the new engine source.
- positive.log: all four pass, including direct and joined sources, grouped
  casts, NULL pairs, DISTINCT, FILTER, invalid/empty typed inputs, arity,
  nonfinite numbers and the actual grouped-CTE/stored-function query structure.
- gates.log: all 713 SQL unit tests pass (four existing benchmarks ignored),
  36 focused tests pass and all 182 wire-protocol tests pass. The new wire test
  checks prepared Describe float8 metadata and binary numeric/NULL results.
- current-artifacts.json and retired-artifacts.jsonl retain executable and linked
  library hashes. Completed focused binaries were checked for the new aggregate
  marker before retirement; active executables were protected. Stripping keeps
  original timestamps. Cleanup reclaimed 2,519,612,296 completed-test bytes and
  retains 22,629,319,359 reusable dependency bytes.
- Targeted rustfmt --check and git diff --check pass.

An initial stored-function assertion expected numeric JSON fields to be JSON
numbers. The existing EHR contract accepts exact numeric strings and converts
these with Number; the final regression checks their numeric meaning without
requiring a particular representation. Generic composite numeric JSON conversion
is not repaired or claimed PostgreSQL-identical here. The first partial run's
2/3 result is not counted as a successful gate.

Rebuilt native binaries, all seven hosted clinical contracts, real browser
controls, successful scheduled workers, production data reconciliation, replay
conversion with owner-selected retention, capacity and recovery remain required
before the PostgreSQL production cutover.
