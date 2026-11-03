# Preview text patterns and collection function execution

This checkpoint implements bounded Oracle TRIM, REGEXP_SUBSTR and REGEXP_REPLACE
semantics in `oracle-preview-v1`. The stored routine compiler is version 20.
Complete application operation remains unqualified.

The runtime source is `97d5c169e90ad2d3627fe5c6107cd299cc05cbb2`. The tested development binary
has SHA-256 `6894a9d82967ed2bb22876cebe41956542694ad0a8ff23f924c5e2b317152d81` and is retained locally at
`/tmp/bicdb-text-pattern-qualified-kpxww5lg/oracle_server`. It is unoptimized, with debug information and incremental
compilation disabled and one compiler job. This is a development artifact,
not a published release or a verified application deployment.

## Text rules

TRIM supports its FROM grammar and BOTH, LEADING and TRAILING directions. It
requires one trim character, preserves the Oracle empty-string/NULL contract and
reports ORA-30001 for multi-character trim expressions. A NULL source takes
precedence over trim-character validation. Native PostgreSQL TRIM retains its
character-set behavior and empty-string result.

REGEXP_SUBSTR supports character positions, occurrences and capture selection.
REGEXP_REPLACE defaults to replacing every occurrence and permits an omitted or
NULL replacement. Position arguments truncate fractional values. An explicitly
NULL position or occurrence produces NULL; a NULL match parameter selects the
defaults. The qualified match parameters are i, c, m, n and x, with the final i/c
setting taking precedence. Searches retain anchors against the original input.
An initial position beyond the final character does not search the empty suffix.
Oracle's empty matches immediately following nonempty matches are preserved.

Typed character arguments reuse the existing Oracle NUMBER/logical DATE
conversion helpers. PostgreSQL prepared execution preserves numeric, text and
NULL parameter types. A general inference fix recognizes native concatenation's
text result during later typed passes. Qualified and quoted application function
names retain their resolution.

The execution contract uses binary, case-sensitive matching by default. Pattern
size is limited to 512 UTF-8 bytes, compilation is bounded, and replacement output
has an 8 MiB platform byte limit checked before growth. Pattern backreferences,
national-character UTF16 matching, linguistic/NLS matching, inline pattern modes,
Rust character-class set operations and other alphabetic escape families are outside
this contract. Unsupported dialect constructs are rejected rather than given
Rust-specific semantics. LOB result types, full Oracle limits and complete regex
dialect/error parity remain open.

## Version and native controls

Version-19 routines keep their first-match legacy behavior, including embedded
SQL. Actual prior-reader checks preserve collection execution and refuse
version-20 routines on the previous binary. Language OIDs remain 999418 for
version 19 and 999419 for version 20. Capability checks consult the finite language
registry; unknown future compiler names remain rejected. This also fixes
schema-qualified function cleanup after introducing a new registered version.
Independent tests protect native routine schema checks.

The public [Oracle fixture](../../../examples/oracle/reference/text_patterns.py)
executes read-only scalar queries against an Oracle reference and BicDB through
PostgreSQL prepared execution. It covers 89 source-qualified cases. The
[independent PostgreSQL controls](../../../examples/oracle/reference/postgres_text_patterns.py)
cover eight native behaviors, including first-match replacement, the g flag,
strict NULL flags, character-set trimming and empty strings.

## Application status

On a disposable application copy, the unchanged original collection-splitting
function matches Oracle on five calls: ordinary CSV, NULL input, empty input,
quoted commas and empty fields. Its source collection type is installed after an
explicit fresh-create adaptation of editioned DDL; editioning and replacement
semantics are not implemented by that adaptation. This is a bounded functional
comparison, not application migration or deployment acceptance.

The next original function still needs MULTISET INTERSECT and IS NOT EMPTY.
Eleven source calls establish its baseline; target execution remains 0/11.
The 62/62 definition inventory, complete schema objects, dictionary behavior,
constraints, startup, business workflows, recovery and performance remain
separate requirements. The retained application listener has not been upgraded.

## Validation

The [validation record](validation.json) records 1,381 passing Rust tests, three
existing ignores and 58 passing Python tests. Formatting, license,
repository-boundary and whitespace checks pass. Fifty retained reference reports
pass on the same binary, together with the collection method, result, transport
and reader contracts. The separately tracked dictionary and NVL/conversion gaps
remain 0/11 and 2/7; they are not counted as passing capabilities.

Reference runs exposed two regressions during development: prepared text
inference lost concatenation's return type, and a duplicated version list omitted
new-language routine cleanup. Both were fixed and protected by independent
regressions before the final runs. Earlier interrupted full attempts are not
counted as completed validation. All final monitored batches completed normally,
with one compiler job and an 8 GiB required host-memory reserve. Validation is
local; GitHub CI and complete application deployment are separate states.
