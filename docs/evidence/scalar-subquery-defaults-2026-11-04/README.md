# Scalar-subquery defaults and logical types

Oracle expression lowering did not resolve scalar-subquery operand types for
NVL. Calls could remain unlowered and fail as unsupported, even when the selected
column had a declared character or number type. Transport metadata also represents
both Oracle DATE and TIMESTAMP as timestamps, so generic inference alone cannot
choose DATE arithmetic and conversion rules correctly.

The resolver now obtains a scalar subquery's type from its query scope. Temporal
provenance follows its single selected expression before falling back to generic
metadata. The change does not infer types from returned values or add any
application-specific runtime conditions.

[Independent results](reference-comparisons.json), using
[scalar_subquery_defaults.py](../../../examples/oracle/reference/scalar_subquery_defaults.py),
record seven explicit expectations on each reference:

| Reference | Reference expectations | Target before | Target after |
| --- | --- | --- | --- |
| Oracle Free 23.26.3 | 7/7 | 1/7 | 7/7 |
| PostgreSQL 18.4 | 7/7 | 7/7 | 7/7 |

The cases distinguish matched nulls from missing rows, nested defaults, character
concatenation, exact numbers, DATE arithmetic and timestamp fractions. Oracle
NVL's eager evaluation and PostgreSQL COALESCE's lazy evaluation have separate
expectations, as do their different null-concatenation rules. All nonce tables
are removed, using PURGE on the Oracle reference.

[Validation](validation.json) records 597 passing SQL unit tests with three
existing ignored tests, 33 passing driver integration tests, 25 Python tests and
formatting checks. The focused regression exercises both SQL profiles. Builds
use one job, no incremental compilation or debug information, with monitored
14 GiB start and 8 GiB minimum available-memory gates. Cleanup reclaimed
695,926,176 bytes of completed test executables; runnable and compatibility
binaries were preserved.

The candidate uses source `4cabab0627cc4f93f1a989941c3db92dae528ec1`, SHA-256
`c9a5d283973fbeefbe71364d8d03014ec7f0f9f1bdfd72af4377aa58831e138b`.
It has not yet been deployed to the running application. Numeric format pictures
remain a separate lookup gap. Arbitrary scalar queries, national character types,
complete application coverage and general Oracle compatibility are not qualified
by these cases. This is local qualification, not a published release or production
deployment.
