# Local physical namespace identity

The opt-in `local_namespace_identity_v1` format capability assigns a durable UUID
to each physical collection backing in this database. Storage/journal code can use
it to check that a loaded namespace is the backing it previously observed, across
name changes and reopen. A hard drop and backing recreation produces a new UUID.

Activate it through the trusted Core API before creating any journal or recovery
intent which requires these identities:

```rust,ignore
let assigned = db.activate_local_namespace_identity_v1()?;
let identity = db.local_namespace_identity("invoices")?;
```

Activation requires a writable, healthy database with no outstanding Core
transactions or row locks. The trusted caller must also establish that SQL or
application catalogue journals are quiescent before entering this maintenance or
startup boundary; Core's transaction check does not inspect those external
journals. Do not invoke migration to bypass an existing pending rename or
compaction recovery gate. The API fsyncs the sticky
format fence before assigning missing IDs and persisting the catalogue. Existing
IDs survive retries. The catalogue and subsequent format replacements force fsync
even if ordinary row writes use `fsync=false`; row durability settings otherwise
retain their existing meaning.

`local_namespace_identity_feature_active()` reports format activation.
`local_namespace_identity(name)` returns `None` for legacy or incomplete migration.
Code requiring a trusted identity must refuse that outcome. A flag-only interrupted
migration stays readable; explicitly rerunning activation fills only missing IDs.
Unfenced, nil or duplicate stored identities are rejected.

Rename and metadata restoration on the same backing preserve its identity.
Recreation, including recreation during logical DDL compensation, receives a new
one. Logical snapshot and commit-frame imports assign missing target backings their
own local IDs and preserve existing target IDs. Portable schema fingerprints,
snapshot-chain checksums and outbound logical metadata exclude the local UUID;
strict local metadata equality includes it. Whole-database byte copies retain the
copied physical history, so this UUID alone is no global instance/authentication
identifier.

This API does not provide a donor-to-recipient identity mapping for logical
replication. A journal which refers to the donor's UUID cannot use it directly as
the recipient's physical backing identity. Such a consumer needs a separate,
durable and authenticated binding from the logical origin and backing generation
to the actual recipient UUID, scoped to the appropriate database incarnation and
restore history. Names, SQL OIDs and retired aliases do not establish that binding.
Until it exists and is qualified, local journal replay evidence does not establish
safe replicated journal replay or promotion.

The capability is optional. Ordinary create/open paths preserve the legacy `None`
encoding, omitted from serialized metadata. Activation changes the persistent
format: older binaries refuse it before modifying database files. A compatibility
test explicitly exercises the pinned source-463 CLI as that negative control.

The API provides a local physical identity fact. Image content, a committed range
prefix, current epoch/authority, transaction visibility and read consistency need
independent proof. Existing pending rename/compaction or recovery gates remain in
force. SQL OIDs, retired aliases and caller-provided UUIDs do not replace the loaded
Core identity. Partition routing and tenant locality retain their existing contract.

The public implementation deliberately continues to reject
`range_image_install_v1` as an unknown format feature. The experimental image,
native-ready quorum and activation work has separate evidence and qualification;
this namespace API does not enable those deployment modes.

Source package versions, published binaries and verified deployments are separate.
The [validation receipt](evidence/local-namespace-identity-2026-11-11/README.md)
records the exact tested source and checks; no physical HA, scale-out throughput
or production admission follows from this feature.
