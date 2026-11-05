# Reconciling legacy Oracle BLOB columns

Use the exported source manifest to distinguish BLOB columns from ordinary
PostgreSQL BYTEA. Binary storage, a column name or equal payload bytes do not
recover that declaration. The application snapshot importer provides an explicit
`blob-types` phase for its export format; it accepts the manifest's schema and
identifiers without application-specific table rules.

Retain a pre-upgrade backup, use an isolated migration target, and stop its
writers and schema changes. The conversion is currently bounded to
unpartitioned tables outside active transactions. Each column has its own native
commit; the entire migration is not atomic. This phase does not coordinate
writers or acquire a distributed migration lease.

Set `BICDB_PG_DSN` privately as for the other importer phases. Run a read-only
preflight with a separate report:

```sh
python3 examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/oracle-snapshot --phase blob-types \
  --check-only --report /private/blob-preflight.json
```

Preflight inventories explicit source BLOB declarations on exported base tables,
checks the server timezone status before row decoding, exact schema/table/column
mappings, physical BYTEA storage and any known
logical declaration, and fingerprints all columns of each affected target table.
It performs no probe DDL or ALTER. Its success does not establish that a server
supports persistent locators; the report leaves capability verification false.
View projections, virtual BLOB columns, duplicate declarations and incompatible
target types are not silently adapted.

Apply with an explicit confirmation that writers are stopped:

```sh
python3 examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/oracle-snapshot --phase blob-types \
  --writers-quiesced --report /private/blob-types.json
```

After preflight, apply checks a real stored `DBMS_LOB.APPEND` using a temporary
probe table before converting source columns. Apply therefore needs probe-table
CREATE/DELETE/DROP privileges in addition to reading and altering the affected
tables. It deletes the probe row before
dropping the table. The probe itself can advance the durable reader-version
fence; dropping it does not restore compatibility with older binaries. Returning
to an older reader requires the pre-upgrade backup. Every unjournaled source BLOB
receives an explicit
`ALTER TABLE ... MODIFY (... BLOB)`, even if the existing public logical label
already says BLOB: that label alone cannot distinguish older storage generations.
Native BYTEA columns absent from the source BLOB declarations stay BYTEA.

After each column conversion, complete target table fingerprints must match
their pre-conversion values, including NULLs, empty binary values, other columns
and duplicate multiplicity. The journal records a column only after those
checks pass. It contains hashes and counts rather than row contents or
connection credentials. The target endpoint is hashed to scope resume checks;
this is not a durable database identity or security boundary.

Resume with the same manifest, target and journal:

```sh
python3 examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/oracle-snapshot --phase blob-types \
  --writers-quiesced --resume --report /private/blob-types.json
```

Resume refuses changed source/target identity, malformed completion prefixes,
missing completed declarations and changed target row fingerprints. A journal
identity mismatch leaves the prior journal untouched. Completed columns are
revalidated and explicitly rewritten too: an endpoint and BLOB label alone
cannot certify that a replacement database retained the required storage.
An interrupted column can therefore be checked and replayed without inferring
success from a label or an incomplete journal.

These fingerprints prove preservation of the target's saved values, not equality
with the exported source rows. Source row-count differences are reported, and
`data_compared_to_source` remains false. Use the separate `verify` phase for
source-versus-target value equality; migrated applications that have accepted
new writes need a corresponding consistent source/cutover policy.

```sh
python3 examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/oracle-snapshot --phase verify \
  --report /private/values-after-blobs.json
```

The [BLOB storage contract](oracle-blob-storage.md) describes locator semantics,
reader-version fences and remaining DDL/package boundaries. This migration phase
is not complete LOB compatibility, a backup service, or full application and
production qualification.
