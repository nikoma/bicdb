# Repeated summary queries retain their actual expression types

Base: BicDB main `4dc286eb05635b51b00c1c90e63e483fe754dd31`.

After a successful hosted practice write, repeated companion reads intermittently
failed with `NULLIF types text and int4 cannot be matched`, or attempted to cast
an integer to a date. A cold standalone call could succeed, making a single-read
check insufficient. A reduced stored routine reproduces the failure on its
second call, alternating two patients with a grouped timezone/date summary.

The type/plan memos key stable routine expressions by address. Query rewrites
for FETCH, ordinal sorting, row types and constant folding had been executing
temporary cloned trees under that persistent identity. Generic row-function
argument evaluation also cloned arguments before type inference. Freed addresses
could then inherit a different expression's cached type.

All session query rewrites now use the existing scoped suspension mechanism,
including row-count binding. Row-function evaluators use borrowed arguments,
with the existing scoped owned representation for SQL-standard JSON argument
rewrites. The whole-row JSON helper accepts that argument representation too.

- `negative.log`: the new repeated-summary test fails before the fix, attempting
  to cast an integer to a date (exit 101).
- `positive.log`: all 54 tests pass across `carrier_hub_compat`,
  `locking_projection`, and `type_consistency`. The regression verifies 100
  calls, including exact dates, patient-specific minutes and JSON structure.
- `protocol.log`: all 180 pgwire protocol tests pass.
- `rustfmt --check` and `git diff --check` pass.

Validation used the task-owned host with two jobs, incremental compilation
disabled, stripped test debug information, and active resource monitoring.
This is engine regression evidence. A rebuilt binary and the actual hosted
companion workflow remain required. Empty JSON fallback coercion, native program
creation, earlier duplicate goals, and production cutover remain open.
