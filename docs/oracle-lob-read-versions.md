# Persistent BLOB read-version infrastructure

This internal layer tracks the read versions of an Oracle BLOB value using
native transaction outcome/write receipts. It is carried through
`SqlSessionCatalogCache`, so transaction state can survive the PostgreSQL
server's short-lived SQL sessions. It is a prerequisite for persistent locators,
used by the selected static acquisition and APPEND paths described in the
[BLOB storage contract](oracle-blob-storage.md). It does not establish full LOB
API or application qualification.

## Origin-local behavior

A read version records its acquisition mode, immutable read bytes and optional
writer/acquisition receipt. Copies initially share a history; changing a
particular destination creates a new read version without refreshing its
unwritten aliases. Within an origin transaction:

- Ordinary snapshots keep their acquisition bytes through other writes.
- An append advances its destination; another alias can still append from its
  own earlier snapshot rather than from the latest row bytes.
- A discarded writer follows the retained write prefix. Monotonic receipt
  numbers prevent a new write at a reused buffer position from reviving an old
  version.
- A later FOR UPDATE acquisition follows rollback of the write it observed.
  An ordinary read taken at that same point retains its acquisition snapshot.
- Origin commit advances locked acquisitions and actual writers to the final
  surviving version. An unwritten ordinary snapshot remains independent.
- Committing owner deletion/replacement invalidates the observed locked
  acquisitions with ORA-22922; ordinary snapshots retain their old readable
  bytes in the corresponding source observations.
- Rolling back an inserted value can remove its row while leaving acquired
  bytes readable. Readability does not establish a live writable owner.

`Present`, `Deleted` and `Replaced` owner observations remain distinct. Reads do
not grant writes. The future mutation path must resolve the durable owner,
check current SQL authorization and the required row lock, and apply the
appropriate operation/context error. An unresolved core commit outcome is
refused rather than interpreted as an abort.

## Lifetime and resource handling

Histories contain no database pointer. Registry keys separate value UUID,
collection, column and actual transaction identity; transaction numbers alone
are not identities across databases. Cloning the registry before its first use
establishes shared lazy state. Nested transaction contexts retain separate
histories rather than resetting their parent's registry.

The registry holds weak references and prunes expired entries. Rolled-back
changes are removed from history bookkeeping. A read after commit retains only
the final surviving change; an aborted origin releases its change list. Baseline
bytes and snapshots still held by live values remain available. Ordinary
sessions do not allocate registry state unless it is used or cloned.

## Evidence and integration limits

The [acquisition probe](../examples/oracle/reference/persistent_blob_acquisition.py)
records native ordinary/locked read differences, rollback, writer commit and
reuse of old locators across later transactions. Its
[observations and validation record](evidence/oracle-lob-read-versions-2026-11-05/README.md)
separate source results from native core/session tests. The earlier
[ownership probe](evidence/oracle-persistent-blob-ownership-2026-11-05/README.md)
records operation-specific copy/APPEND errors.

[Identity-bearing BLOB storage](oracle-blob-storage.md) connects selected static
SELECT/RETURNING acquisitions and APPEND to this layer. Reusing an old snapshot
in a later transaction preserves its append base, while rollback uses that
transaction's current owner baseline. Native write annotations distinguish
pending deletion from replacement even across short-lived SQL sessions. Actual
RETURNING acquisitions read the normalized buffered row, rather than a
provisional pre-write UUID. Broader acquisition paths, DDL lifecycle, complete
LOB APIs, migration qualification and full application acceptance remain open.
