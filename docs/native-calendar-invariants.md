# Native calendar invariant contract

The canonical ABI admits exactly typed `time.add(Date, days: Int)` and
`time.diff(Date, Date, unit: "days")` inside application invariants. The latter is
signed first-Date minus second-Date, matching the existing ordinary-program API.
The host lowers these calls to real core `DateAddDays` and `DateDifferenceDays`
expressions. Validation checks actual subject/binding field types as well as
signed call metadata, exact argument names/counts, result types and scalar shape.
Time/zone operands, fractional offsets and other units are not admitted.

Operations use checked integer Gregorian calendar arithmetic over the existing
`chrono::NaiveDate` domain (`-262143-01-01` through `+262142-12-31`), including year
zero. They do not use timestamps, timezones, elapsed milliseconds or floating
point. Invalid operands and overflow produce commit-validation errors and leave
no partial batch. `i64::MIN` offsets are handled without signed negation overflow.
There is no caller-provided calendar ordinal or calendar proof row.

Native invariant Date comparisons and ordinary-program comparisons whose two
operands are typed Date are chronological across this domain. Null equality and
presence guards retain their existing behavior; malformed non-null Dates cannot
make a false NotEqual predicate succeed. String and UUID comparisons are not
coerced to Date. The arithmetic API's argument order and Date formatting are
unchanged.

Persisted Date SQL/index predicate semantics are **not** changed here. A separate
admission/generation/migration plan is required for their BCE/extended-year
behavior. Core invariant validation scans each complete dependency relation plus
the transaction's writes; it does not prune candidates using lexical Date
indices. This distinction is part of the compatibility contract.

Older hosts reject calendar builtin calls in invariants as outside their enforced
corpus. They must never drop the condition or grant SQL to accept the package.
Ordinary application-host acceptance does not qualify a Cell: Cell profile,
manifest/volume binding, local authorization and actual execution remain separate
gates. A capability/preflight acceptance is not runtime evidence.

Financial applications should derive last-day boundaries from their authoritative
period records in native predicates. New guards must preserve immutable historical
evidence and controlled withdrawal/remediation for old unexecuted malformed
proposals, while preventing them from acquiring posting authority. They must not
rewrite dates or replace native arithmetic with stored caller ordinals.
