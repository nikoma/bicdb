# Pending transaction visibility through typed indexes

Pending transaction candidates now use the same canonical index-key projection
as committed records. The previous SQL helper converted a record back into
logical SQL values before reconstructing its key; that lost typed storage-key
encodings and could filter updated/inserted NUMERIC-key rows out of a secondary
index read. A subsequent aggregate or correlated parent update could therefore
return zero despite the transaction's own matching row.

`IndexDefinition::key_for_record` exposes the existing core projection, including
its field transforms and typed key encoding. Equality and range candidate merges
reuse it; the duplicate SQL reconstruction helper is removed. This adds no table,
schema, application or NUMERIC-specific exception. It neither rebuilds indexes
nor changes their persistent representation, routine compiler 25 or metadata
format 5. Index predicates and ordinary row/security filtering retain their
separate execution stages.

## Independent comparisons

The [executable fixture](../../../examples/oracle/reference/pending_index_visibility.py)
runs each workload with and without a secondary index. Prepared mutations and
reads cover equality/range sums, a correlated parent update, pending inserts,
key movement, deletion, savepoint restoration, full rollback and post-commit
reads. Every expected value must first pass on the actual reference.

The exact development binary matches [24/24 Oracle cases](oracle-comparison.json)
and [24/24 native PostgreSQL controls](postgres-controls.json). Oracle is
23.26.3.0.0; PostgreSQL is 18.4. Both [prior Oracle](oracle-baseline.json) and
[prior PostgreSQL](postgres-baseline.json) target baselines match 16/24 and retain
the eight indexed own-write mismatches. All fixture objects were removed. The
[Oracle](numeric-oracle-controls.json) and [PostgreSQL](numeric-postgres-controls.json)
scalar result-type controls also pass 12/12 on this binary, including prepared
text/binary decoding and empty results.

The Rust regression exercises both SQL profiles and requires index lookup,
zero full scans and at most two matching output-materialized rows for its bounded
lookup. This prevents replacing the correction with an unconditional table scan.
Existing pending-delta and uniqueness regressions remain enabled. The core stored
key/record projection comparison also passes.

Local validation passes **1,445 Rust tests**, with three existing ignores, and
**58 Python tests**. Formatting, diff, license and open-core checks pass. CI was
not run in this local qualification. Source identity, binary digest, batches and
memory guard observations are in [validation.json](validation.json).

## Application and wider scope

This regression reproduces the application's zero-total pattern, but passing the
generic fixture alone does not prove that the actual application's order save is
correct. The application must be restarted on this exact binary and its saved
line, tax state and parent totals verified independently through SQL and a fresh
browser. The canonical migration database remains separate from those disposable
qualification copies.

Own-write visibility in this bounded NUMERIC-key workload is qualified; complete
transaction isolation, distributed transactions, failover and the remaining
Oracle programme are not. Order processing, inventory effects, invoicing,
accounting and other application flows remain open. This is merged-source and
development-binary evidence, not a published release, production deployment or
complete iDempiere qualification.
