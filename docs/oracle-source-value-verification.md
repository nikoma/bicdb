# Oracle exported-value verification

The application's `verify` import phase compares each exported table's complete
row multiset with the target over the PostgreSQL driver. It verifies data
separately from schema installation, target-value preservation during DDL,
orphan counts and application workflows.

## Run before application writers

Keep target application, importer and schema writers stopped. This phase uses
separate queries and does not acquire a writer lease or claim an atomic
cross-table snapshot under concurrent writes. Retain the immutable source
export, its manifest and the migration backup.

Keep the PostgreSQL DSN in `BICDB_PG_DSN`. From the repository root:

```sh
python examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/export --phase verify \
  --report /private/source-values.json
```

The server must confirm `oracle-preview-v1`. Character declaration and nullable
unique-key metadata checks also run for the exported tables. Logical type
reconciliation must precede verification where required by the source schema.
This phase issues no repair SQL or schema probes.

## Source and target contracts

Before querying a table's target values, the verifier reads its source file once
and checks the exact bytes against the manifest digest, requires an array of the
declared width for every row, and requires the declared row count. Empty or
duplicate projection columns, invalid counts, unsupported value encodings and
files outside the snapshot directory are refused. An empty table still has a
declared projection and an authenticated empty row file.

Row digests retain NULLs, binary content, decimal precision, character padding
and duplicate multiplicity. Heap enumeration order can differ. The verifier
does not trim strings, coerce floats into exact decimals or round numbers using
the caller's Decimal context. The final digest streams sorted row hashes instead
of allocating another concatenated copy. Per-table row hashes are still retained
in memory for sorting; this is not a constant-memory external-sort verifier.

`--resume` requires the same manifest, target endpoint, profile and completed
table prefix. Every completed verification table is read and compared again,
including its source file and saved value digest. Row count alone cannot
re-certify an earlier result. Missing/mismatched saved digests or same-count
target value changes fail verification; there is no automatic baseline reset.

Use private report paths. Verification reports include table names, value
digests and target connection metadata, without passwords or row values. A
successful phase leaves `application_qualified` false. It does not qualify
constraints, sequences, routines, views, recovery, cutover or the entire
application merely because table values match.

## Application qualification data

Run source-value acceptance on a pristine migration target before business
workflows modify it. Then use a separate disposable copy for workflows. Orders,
shipments, postings, login state and sequence allocation can legitimately change
data. A before/after target fingerprint proves preservation relative to that
target baseline; it does not prove equality with an earlier Oracle export.

When a previously exercised target differs, investigate aggregate counts and
changed column names without exposing row values. Keep source-data equality
unqualified until a pristine comparison or an explicitly reviewed delta is
available. Do not weaken the comparison or silently repair a workflow database
to obtain a passing migration report.

The [strict verification checkpoint](evidence/oracle-source-values-2026-11-05/README.md)
records bounded tests and separates source-export integrity from target equality.
