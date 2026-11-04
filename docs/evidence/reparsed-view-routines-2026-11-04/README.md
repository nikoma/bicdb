# Repeated routine reads of reparsed views

Repeated calls to a routine reading a view could fail with datatype mismatches
despite identical SQL and unchanged rows. Type-inference caches keyed by expression
addresses incorrectly treated newly parsed view nodes as belonging to the
caller's persistent routine IR. Reused addresses could retrieve unrelated types.

View execution now suspends the caller's expression-type memo scope and creates
a context without its routine-IR ownership. Transaction state, parameters,
session identity and definer/invoker policy selection are preserved. Nested
compiled routine calls retain their own cache scopes. The previous scope is
restored on both success and error. Storage and compiler formats are unchanged.

The general regression uses a view containing numeric and character CASE
expressions, a NULL cast and UNION ALL. It checks exact decimal results across
60 rounds of three function calls in each SQL profile. The unfixed engine fails
this regression in native PostgreSQL mode; the fixed engine passes all 360 calls.
There are no application-specific runtime conditions.

The executable independent-reference fixture is
[reparsed_view_routines.py](../../../examples/oracle/reference/reparsed_view_routines.py).
[Reference results](reference-comparisons.json) retain every call, including
the baseline failures:

| Reference | Reference expectations | Target before | Target after |
| --- | --- | --- | --- |
| Oracle Free 23.26.3 | 180/180 | 155/180 | 180/180 |
| PostgreSQL 18.4 | 180/180 | 167/180 | 180/180 |

All baseline mismatches are SQLSTATE 42804 datatype errors. Both reference and
target fixtures remove their nonce objects, purging Oracle fixture tables.
The allocator-dependent baseline counts describe these runs, not a fixed rate.

[Original calculation checks](original-invoice-calculations.json) reload all 62
original iDempiere routines and 160 views on a disposable copy of the stopped
test database. Routine and view query bodies are unchanged. All 11,983 column
labels and transport type families match the Oracle metadata, including 1,104
Oracle DATE columns exposed as PostgreSQL timestamps. This does not qualify
numeric precision, scale, string widths or the values of every view.

Oracle supplies expectations for five invoice-balance calls and a dashboard
aggregate. Twenty repetitions of that sequence on BicDB pass all 120 checks.
Exception handlers remain unchanged; diagnostic rethrow definitions are not
used. No business statuses, quantities or amounts are repaired through SQL.
These checks do not qualify shipment, payment or accounting workflows, or the
entire application. Recorded query timings are observations, not benchmarks.

[Validation](validation.json) records 1,526 passing Rust tests across 25 batches,
three existing ignored tests, 25 passing Python tests and formatting checks.
The batches include view ownership, authorization, row-level security,
PostgreSQL protocol, transactions and locking. Retained joined-scalar,
Describe and wildcard fixtures pass 38/38 checks against each reference.

Builds use one job, no incremental compilation and no debug information, with
14 GiB start and 8 GiB minimum available-memory gates. Completed test executable
cleanup reclaimed 6,311,323,136 bytes; the remaining build cache was 7.5 GiB.
Frozen comparison and runnable binaries were preserved.

The candidate comes from source commit
`83797087c8f9b9fcf7a529417ef782a4cc02c608`, with SHA-256
`282463584cfbf90f90a05e9b32f00cc5f2b8af6f6f96845203962b69e72fcdb6`.
This evidence records a built candidate and local qualification. It does not
claim a published release, production deployment or complete Oracle compatibility.
