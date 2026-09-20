# ADR-004: `storage_mode` is durable database metadata

Date: 2026-07-24 · Status: accepted · Scope: `bicdb-core` on-disk format

## Context

`docs/server-paged-storage-todo.md` proposes a second storage engine,
`server_paged`, for databases much larger than RAM, living behind the same SQL,
transaction, index, vector, backup, and replication contracts as today's
`embedded_memory` engine.

Two engines reading the same directory layout is the failure mode that matters.
A database whose durable state is a page file is not a database whose durable
state is a segment log, and a binary that reads one as the other does not fail
cleanly — it misparses, and then it writes. Everything else in the roadmap
(Phases 1–9) can be built incrementally and reverted; a database corrupted by an
engine that should have refused it cannot.

This ADR fixes the compatibility contract *before* any page-store code exists,
so that the fence is already deployed in the field by the time the first
server-paged database is written anywhere.

## Decision

**The storage engine that owns a database is durable metadata recorded in that
database, and a binary that cannot run the recorded engine must refuse to open
it without mutating a single byte.**

### The field

`storage_mode` is a new field in `format.json` (`FormatMetadata`), serialized as
a lowercase string:

- `embedded_memory` — the current engine, and the default.
- `server_paged` — **runnable as of 2026-07-24, in a staged form.** Records are
  stored durably in the page engine (`bicdb-page`) with its own WAL, checkpoints,
  crash recovery, MVCC, and bounded buffer pool. SQL, transactions, indexes, and
  vector search all work: the cross-mode conformance suite and
  `bicdb-sql/tests/server_paged_sql.rs` pass identically in both modes.

  **What it does NOT yet give: bounded resident memory.** The engine currently
  writes records to pages *in addition to* keeping the resident in-memory
  projection, because MVCC version chains, secondary indexes, and the vector
  store are still built from that projection. So `server_paged` today buys
  durability and crash recovery on the page engine, not a database larger than
  RAM.

  That gap is stated here rather than in a commit message because the mode's
  name promises otherwise. Until resident memory is bounded, `server_paged` must
  not be presented as the multi-terabyte capability — the roadmap's Definition of
  Done is the gate for that, and it is not met.

The field is `#[serde(default)]`. A database written before it existed, or with
no `format.json` at all, is unambiguously `embedded_memory` — there was no other
engine it could have been written by.

### Unknown modes parse, then get rejected

An unrecognized mode string deserializes to `StorageMode::Unknown(name)` rather
than failing the parse. This is deliberate. A serde failure on `format.json`
surfaces to an operator as a malformed-metadata error, which invites exactly the
wrong response (delete it, regenerate it, "repair" it). Preserving the name lets
the error say what is actually true:

```
database storage_mode `columnar_lakehouse` cannot be opened by this binary:
this binary does not recognize that storage mode; a newer BicDB binary wrote it;
open refused without mutating data
```

`Unknown` also round-trips verbatim, so no code path can normalize away a mode
name it did not understand.

### The fence is read-only and ordered

`format::ensure_storage_mode_compatible` runs before anything in `open` can
write to the directory — including the existing format-version migration, which
does write. Its order is part of the contract:

1. the **requested** mode must be one this build implements — so asking for an
   unavailable engine never creates the directory;
2. the **persisted** mode must be one this build implements — checked before the
   mismatch test, so that meeting a server-paged database reports "this binary
   cannot run that engine" rather than blaming the caller's default config;
3. only once both are individually runnable must they agree.

Step 2 preceding step 3 is not cosmetic. Every caller that opens with default
config is requesting `embedded_memory`; if the mismatch test ran first, every
encounter with a future database would produce a misleading "you asked for the
wrong mode" error naming a mode the caller never chose.

### Modes are never converted implicitly

Opening an existing database under a mode other than its recorded one is an
error, not a conversion. Migration between modes is an explicit, verified,
operator-initiated step with its own backup and rollback requirements
(Phase 8 of the roadmap). No BicDB binary converts a database because it was
started with different configuration.

Correspondingly, `persist_current` — which runs on ordinary flush paths — reads
the mode back from disk and preserves it, and fails loudly if it is a mode this
build cannot run. A routine metadata rewrite must never be the operation that
silently downgrades a server-paged database to `embedded_memory`, and a format
*version* migration must never be the operation that changes which engine owns
the data.

### The fence works retroactively

A `server_paged` database also carries the `server_paged_storage` feature flag.
Binaries predating this ADR ignore `storage_mode` entirely — but every shipped
BicDB binary already rejects unrecognized entries in `feature_flags`. Writing the
flag is therefore what makes the fence hold for binaries **already deployed**,
not merely for future ones. This is the reason to land the contract now rather
than alongside the first page-store code: the population of binaries that will
correctly refuse a server-paged database is the population shipped from this
point on, plus, via the flag, everything shipped before it.

## Consequences

- `DbConfig::storage_mode` selects the engine at open time and defaults to
  `embedded_memory`. A *new* database may be created in either mode; an existing
  one keeps the mode written down for it.
- Backup restore inherits the fence: `check_backup_restore_compatible` validates
  the mode through the same path, so a server-paged backup cannot be restored by
  an embedded-only binary.
- Adding a third mode later costs one enum variant and one `is_supported` arm.
  Removing `Unknown` is not permitted — it is the mechanism by which old binaries
  fail safely against new ones.
- `embedded_memory` databases gain no feature flag, so databases written today
  remain openable by binaries predating this change. The compatibility cost of
  this ADR for existing deployments is zero.

## Verification

`crates/bicdb-core/tests/storage_mode.rs` asserts the contract, including the
properties that are easy to regress silently:

- a refused open leaves `format.json` byte-identical and creates no `segments/`
  directory and no migration state file;
- a refused *requested* mode does not create the database directory at all;
- a format-version migration preserves the recorded mode;
- `persist_current` on a server-paged database preserves its mode rather than
  defaulting it away;
- creating a new database in `server_paged` persists the mode and its feature
  flag, while an existing *legacy* database (no metadata, but segments present)
  is never reinterpreted as paged;
- an unknown mode name survives a serde round trip verbatim.
