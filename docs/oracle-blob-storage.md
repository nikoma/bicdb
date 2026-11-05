# Identity-bearing Oracle BLOB storage

Oracle preview table declarations distinguish a logical BLOB from PostgreSQL
BYTEA. New BLOB columns use the `oracle_blob_v2` schema modifier and durable
compatibility metadata format 7. Routine BLOB signatures retain their existing
logical modifier. An older Oracle-profile reader must reject the newer metadata
rather than interpret identity-bearing cells as ordinary binary values.

This is storage infrastructure for persistent locators. It does **not** deliver
persistent `DBMS_LOB.APPEND`, stored-locator PL/SQL assignment, cross-transaction
locator rebinding, or full application acceptance. Those runtime paths remain
unsupported until read-version binding and mutation authorization are connected.

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
`ALTER TABLE ... MODIFY (... BLOB)` requests logical conversion; compare source
and target binary values before accepting it. Existing logical format-6 columns
are not silently reclassified as identity-bearing columns merely by reopening a
database. A source-manifest-driven reconciliation and application migration
qualification remain required.

The owner registry is not yet a general DDL lifecycle service. Rename, recycle,
truncate, drop, flashback and schema conversion need dedicated identity/registry
acceptance before persistent locator writes can rely on them. In particular,
lookup validation rejects stale owners; it does not establish registry garbage
collection or preservation across all DDL operations. General index, trigger,
constraint and functional-index behavior during future locator mutation also
requires source comparisons and acceptance.

See [read-version tracking](oracle-lob-read-versions.md) for the separate runtime
history layer and its native Oracle acquisition/ownership observations. Storage
tests and PostgreSQL transport tests establish only their recorded scope, not
Oracle conformance, production qualification or a deployed release.

The [execution record](evidence/oracle-blob-storage-2026-11-05/README.md) separates
storage and driver regression evidence from the remaining runtime and deployment
work.
