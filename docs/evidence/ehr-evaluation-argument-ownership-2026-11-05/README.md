# Argument ownership in expression evaluation and JSON construction

Base: `54a35a1`. Repeated signed goal-list/snapshot reads on the actual hosted
EHR fixture eventually return type errors, including interpreting the ordinary
status `pending_acceptance` as JSON. The first few calls on a fresh connection
can succeed. The frozen `18c604a` workflow also reports a JSONB/numeric common-
type mismatch during a warmed snapshot.

Additional evaluators still cloned ordinary function arguments and then ran
address-keyed type inference on those temporary nodes inside the routine's
longer-lived memo scope. JSON numeric normalization did this independently of
the primary expression evaluator. The new ownership regressions catch both
constant and virtual-row evaluation retaining those addresses.

Those paths now borrow ordinary arguments through the existing guarded argument
list. This includes generic/constant/target-record evaluation, JSON numeric
normalization, session configuration, sequence arguments and the procedural-call
fallback. The SQL-standard JSON argument rewrite retains its existing scope
guard. The no-FROM compatibility helper explicitly suspends type memoization
while it uses synthetic function wrappers. Lazy COALESCE behavior, JSON numeric
values, session identity and operation-authority guards are preserved.

- `negative.log`: both new ownership regressions fail before the fix, with
  `evaluation retained a temporary argument address in the type memo`.
- `incomplete-repair.log`: an intermediate repair still fails both regressions;
  676 existing tests pass. This is not a passing validation. Test-only tracing
  then identifies the separate JSON numeric-normalization clone; the tracing is
  removed from the final source.
- `unit.log`: all 678 SQL unit tests pass; one existing benchmark is ignored.
  Both new regressions also check exact text/object/numeric values across 64
  repeated evaluations and restoration of the surrounding memo scope.
- `focused-a.log`, `focused-b.log`, `focused-c.log`: all 66 focused tests pass
  across Carrier workflows, JSON SRFs, type contracts, row locks, RLS depth and
  both PostgreSQL/Oracle correlated-query and nested-routine stack cases.
- Targeted rustfmt and `git diff --check` pass.
- `protocol.log`: all 181 wire-protocol tests pass.

A focused build was interrupted by the 8 GiB disk-headroom guard (exit 99);
the resumed batch supplies `focused-b.log`. Cleanup is confined to the owned
build/cache paths and preserves active executables. Duplicate unfrozen `18c604a`
build outputs were hash-checked against the retained frozen binaries before
removal, reclaiming 1,024,533,208 bytes.

The rebuilt real EHR workflow still needs qualification. Group enrollment also
exposes an independent compiler authority descriptor containing another CTE's
record fields; that compiler repair is separate. Full clinical acceptance and
production cutover remain open. Production data and services were not changed.
