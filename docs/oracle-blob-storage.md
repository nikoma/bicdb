# Identity-bearing Oracle BLOB storage

Oracle preview table declarations distinguish a logical BLOB from PostgreSQL
BYTEA. New BLOB columns use the `oracle_blob_v2` schema modifier and durable
compatibility metadata format 7. Routine BLOB signatures retain their existing
logical modifier. An older Oracle-profile reader must reject the newer metadata
rather than interpret identity-bearing cells as ordinary binary values.

Selected persistent PL/SQL operations now bind static SELECT INTO and DML
RETURNING values to transaction-aware histories. APPEND supports temporary and
stored BLOB operands, with current owner, database instance, UPDATE privilege,
row-policy and held-lock checks. Persistent writes called from SQL are refused.
This is not complete DBMS_LOB, unchanged Oracle JDBC support, or full application
acceptance.

## Value identity and writes

A non-null stored BLOB contains a canonical, non-nil UUID and its binary payload.
SQL clients receive the payload through PostgreSQL BYTEA transport; the UUID is
not exported as a locator capability. NULL remains distinct from a non-null
empty binary value. Native BYTEA columns keep their existing representation.

The SQL write path updates a private UUID-to-owner collection in the same native
transaction as its row changes. An owner identifies a collection, column and row.
Current-owner lookup checks the actual row's UUID before returning a descriptor;
a stale metadata entry cannot establish that a value still exists. Neither a
descriptor nor an owner lookup grants permission to mutate a row.

- A new value or SQL copy receives a new identity, even for equal bytes.
- Self-assignment and changes to other columns preserve the value identity.
- Replacing a BLOB removes the previous owner mapping.
- Deleting a row removes its BLOB owner mappings.
- A primary-key move preserves identities of unchanged BLOB columns and updates
  their owner row IDs. A replacement made during that move remains a new value.
- Native rollback and savepoint rollback cover both row and owner changes.

BLOB writes use a statement transaction and the record write path. Stored-form
DML optimizations are bypassed for these tables until they can maintain the same
identity contract. SQL cannot address the private owner or compatibility-profile
collections, including through a superuser connection. Trusted native access
still requires its own application boundary.

## Migration and remaining integration

No column-name heuristic turns legacy BYTEA into a BLOB. An explicit
`ALTER TABLE ... MODIFY (... BLOB)` requests binary-column reconciliation on an
unpartitioned table outside an active transaction. Schema, converted rows and
owner mappings share one native commit; the monotonic reader fence can advance
before it. Binary conversion back to BYTEA removes the previous owner mappings.
This bounded operation is not Oracle-equivalent transactional DDL or general
partitioned-table conversion. Compare a source manifest and binary values before
accepting a migration. Reopening a format-6 database does not silently upgrade
its logical columns. Application migration qualification remains required.

The owner registry is not yet a general DDL lifecycle service. Rename, recycle,
truncate, drop, flashback and schema conversion need dedicated identity/registry
acceptance for persistent locator lifecycle behavior. In particular,
lookup validation rejects stale owners; it does not establish registry garbage
collection or preservation across all DDL operations. APPEND uses native writes and existing SQL validation without ordinary UPDATE
triggers. General functional-index, OPEN/CLOSE, deferred-effect and extension
behavior still requires source comparisons and acceptance. Cursor/dynamic-SQL
acquisition and broader package APIs remain separate integration work.

See [read-version tracking](oracle-lob-read-versions.md) for the separate runtime
history layer and its native Oracle acquisition/ownership observations. Storage
tests and PostgreSQL transport tests establish only their recorded scope, not
Oracle conformance, production qualification or a deployed release.

The [execution record](evidence/oracle-blob-storage-2026-11-05/README.md) separates
storage and driver regression evidence from the remaining runtime and deployment
work.

[Selected persistent execution evidence](evidence/oracle-persistent-blobs-2026-11-05/README.md)
covers the runtime follow-up. Quiesce writers before binary-column reconciliation;
this operation does not implement distributed DDL coordination.
