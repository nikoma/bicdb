# Logical primary keys in extrema and range queries

The primary-key MIN/MAX and range fallback paths now read logical values for
typed record identities. An encoded storage locator is not a numeric SQL
literal: parsing it caused a numeric-input error for a bare MIN or MAX on a
numeric primary key, even though ordinary row projection worked. The same
assumption affected composite-key range candidates.

Typed keys use the normal record decoder and transaction-visible record lookup.
Comparisons and prefix/suffix filters use declared column types. Lossless plain
record identities retain their existing decoding path; the committed readable
primary-key index optimization remains available. Range planning uses the
existing prefix locator helper, including its typed-prefix and pending-write
handling. Policy checks remain in the existing execution paths; the aggregate
optimization remains disabled for security contexts and row-level security.

No persisted schema, storage identity or data migration is changed. No
application, table or schema name selects this behavior. This is a correctness
fix; typed primary-key scans are not a qualification of indexed performance,
all key types, collations or concurrent transaction schedules.

## Independent references

The [fixture](../../../examples/oracle/reference/primary_key_extrema.py) has 20
explicit expectations in each profile. It checks exact 30-digit integers,
fractional composite keys, numeric-looking text suffixes, empty aggregates and
ranges, prepared prefixes, pending inserts/key updates/deletes and rollback.
Its numeric canonicalization preserves precision beyond the default Decimal
context. Failed query controls use savepoints inside explicit transactions so
one failure cannot invalidate later controls. Nonce tables and disposable
reference containers are removed.

The [baseline](baseline.json), [candidate comparison](comparison.json) and
[validation record](validation.json) retain the reference expectations, exact
observations and source/binary hashes. Oracle and PostgreSQL are checked
independently. Retained controls cover [matched-update MERGE](retained-merge-comparison.json),
[BETWEEN evaluation](retained-evaluation-comparison.json),
[typed boundaries](retained-typed-comparison.json) and
[RAW conversion](retained-raw-comparison.json).

## Local validation

The library regression also exercises database reopen and transaction-visible
key changes. Builds use one Cargo job, disabled incremental compilation and
memory guards requiring 14 GiB available to start with an 8 GiB floor. The
validation record distinguishes executed tests from existing ignores. These
checks do not establish a release, complete Oracle compatibility or full
application qualification.

The previous binary passes 3/20 controls in each profile; the candidate passes
20/20 against Oracle 23.26.3.0.0 and 20/20 against PostgreSQL 18.4. Retained
MERGE controls pass 15/15 each; evaluation controls pass 19 Oracle and 20
PostgreSQL; typed boundaries remain 16/16 each and RAW controls 13/13 each.

All 613 SQL library tests and 364 driver/protocol tests pass. Three existing
benchmark ignores and two external interoperability ignores remain recorded.
Formatting and whitespace checks pass. No release is published by this
checkpoint; application deployment is recorded separately.
