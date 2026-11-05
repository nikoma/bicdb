# Routine-expression stack footprint

Base: `6ba4a4a`. The immediately preceding Oracle receipt/shipment update is
documentation and validation records only; Rust/build source is identical to
the frozen `18c604a` candidate used for the actual EHR failure.

The actual consented coach invitation aborts the native server. Its backtrace
contains ordinary nested SQL/PLpgsql identity helpers and `eval_session_expr`,
ending in common-type validation. This is separate from the earlier row-policy
boolean traversal. The old development evaluator reserves approximately 79 KiB
on each call, including temporaries for unrelated expression branches.

Builtin/stored-function dispatch is now a separate non-inlined helper, keeping
those temporaries out of other recursive expression frames. Parentheses are
unwrapped iteratively. The extracted dispatch is unchanged after whitespace
normalization. No worker stack, routine-security behavior, privileges, signed
authority guard or consent rule has been relaxed.

- `negative.log`: the reduced nested identity/consent helper test aborts with
  stack overflow on an explicit 2 MiB thread before the fix.
- `positive.log`: all 62 focused SQL tests pass, including that regression, both
  consent/identity refusal cases, the row-policy depth test, existing PostgreSQL
  and Oracle correlated-query stack tests, Carrier queries, JSON and type contracts.
- `unit.log`: all 676 SQL unit tests pass; one existing benchmark is ignored.
- `protocol.log`: all 181 wire-protocol tests pass.
- Targeted rustfmt and `git diff --check` pass.

The first positive build was interrupted at the task's 8 GiB disk-headroom
floor (exit 99), not reported as a pass. Its resumed run supplies `positive.log`.
Task-owned orphan caches and superseded artifacts were cleaned while preserving
other processes. The unused `9bee2c6` candidate binaries were hash-verified and
removed (1,024,523,720 bytes); their metadata remains in the protected evidence
export. Frozen `4dc286e` and `18c604a` binaries remain available. Core dumps are
disabled for the task's intentional stack-overflow regression.

The rebuilt actual coach workflow still needs qualification. A warmed EHR goal
read also exposes remaining temporary-argument type-cache ownership failures,
and group enrollment fails its existing operation-authority guard. Full hosted
acceptance, production topology, reconciliation and cutover remain open.
Production data and services were not changed.
