# BicDB Phase-2 Cryptographic Cell Runtime

Introduced in BicDB 1.0.317-beta, this is an **industry-agnostic**, callable
cryptographic-cell boundary. It strengthens `bicdb cell serve` and
`bicdb-cell serve`; it does not make a regulated-workload production claim.
Every runtime and rotation report continues to return:

```text
regulated_data_admitted = false
```

The normative architecture and remaining admission gates are in
[`bicdb-cell-application-architecture.md`](bicdb-cell-application-architecture.md).

## Construction boundary

A coherent Phase-2 manifest selects all three Phase-2 profiles together:

```text
storage.encryption_profile = cell-bound-xchacha20poly1305-v2
runtime.isolation_profile  = phase2-process-isolated
policy.security_profile    = phase2-cryptographic-deny-regulated
keys.authority             = kms+attested://...
```

Mixing Phase-1 and Phase-2 profiles is refused. A Phase-2 runtime also refuses
the development file-key provider. It can receive key material only through a
one-shot signed lease stream inherited from its launcher. On Linux, the CLI
accepts a pipe descriptor and rejects regular files, terminals, and standard
input/output/error descriptors.

The runtime consumes the lease only after the signed manifest, volume
identity, local monotonic manifest state, runtime binary, guest image, and
application artifacts have passed their read-only checks.

## Exact signed key lease

The independent KMS/HSM authority signs deterministic CBOR covering:

- attestation nonce and a maximum five-minute validity window;
- `CellId`, manifest digest and manifest generation;
- volume and lineage identities;
- exact BicDB binary and guest-image digests;
- encryption and security profiles;
- key authority, KEK identity, key epoch, and key fingerprint;
- an external anti-rollback predecessor and successor counter; and
- exactly one 32-byte cell root key.

Every field is compared to the already verified workload request. A valid
signature for another cell, volume, lineage, manifest, executable, image,
profile, authority, KEK, or key epoch is insufficient. The lease stream is
consumed once and key buffers are zeroized on drop.

The rollback counter must advance by exactly one. The lease must name the
durable local witness as its exact predecessor; a missing witness accepts only
predecessor zero. During key rotation, the next lease must name the retiring
lease counter. This makes a restored missing or older witness detectable when
the external authority preserves its monotonic state. The external authority,
its availability protocol, and data-commit-level rollback roots remain part of
the later fleet/KMS admission work.

Manifest lineage is equally contiguous: an unbound volume accepts only its
signed generation-1 root, and every subsequent activation must advance by
exactly one generation while naming the currently active manifest digest.
Missing, dangling, substituted, or multiply linked state/witness files fail
closed before storage or key use.

Before consuming a Phase-2 key lease, startup also walks the bounded existing
database tree without following links. Nested symbolic links, hard-linked
files, sockets/devices, excessive depth, and excessive object counts are
refused, closing parent-directory substitution paths in addition to the strict
per-file opens.

## Cell-bound encryption hierarchy

For bound stores, the root key never directly encrypts an object. BicDB derives
independent purpose keys and then path-specific object keys with HKDF-SHA-256:

```text
cell root
  -> purpose key
       -> normalized database-relative object key
```

The AEAD context includes the security domain (`CellId`), storage profile, key
epoch, purpose, normalized final object path, and the format-specific caller
context. Moving valid ciphertext to another cell, epoch, purpose, or object
path therefore fails authentication.

The constructed Phase-2 database graph uses separate purposes for:

| Object class | Protection |
| --- | --- |
| record segments | `data` |
| transaction log/WAL | `wal` |
| synchronization/replication log | `replication` |
| SQL external-sort runs | `temporary` |
| large-value chunks | `blob` |
| collection/index/policy catalogs and graph projections | `index` |
| vector structures and semantic-index job identities | `search` |
| event and record-audit streams | `audit` |
| mesh signing secret | `identity-secret` |

Immutable index/search sidecars are single strict encrypted frames. Tamper,
truncation, wrong purpose, wrong key, or path replay fails closed. Large values
stream through bounded encrypted chunks; SQL spills use an opaque database
cipher and never receive the root key.

The server-paged engine remains unavailable to an encrypted cell because its
page files are not yet encrypted. The runtime refuses that construction before
creating encryption metadata. Phase-2 cell capabilities also expose no backup
writer, general replication controller, arbitrary filesystem API, pgwire
listener, database selector, or cross-cell handle. Those absent capabilities
cannot create an unprotected path; cell-scoped backup and HA are implemented
and certified in their roadmap phases.

## Crash-safe key rotation

Rotation is offline and requires two independently verified one-shot leases
plus an exact signed manifest transition. Only the generation linkage and key
manifest may change; application, runtime, policy, storage identity, and every
other authority remain pinned.

The storage transition is:

```text
authenticate every old object
  -> build a complete sibling tree under the new epoch
  -> persist new encryption metadata last
  -> fsync and hash the complete staged tree
  -> durable PREPARED journal
  -> atomically rename source to retired
  -> durable SOURCE_RETIRED journal
  -> atomically rename staged to active
  -> verify active binding and tree digest
  -> persist rollback witness and manifest state
```

Reinvoking after either rename resumes deterministically. No mixed-key tree is
published. The retired ciphertext tree is deliberately retained until an
external recovery/KMS authority authorizes old-key destruction.

The CLI surfaces are:

```bash
bicdb cell rotate-key --help
bicdb-cell rotate-key --help
```

Both require current and next signed manifests, exact cell/volume/image
identity, independent current and next lease descriptors/nonces, trusted
manifest roots, and trusted KMS lease-signing roots.

## Evidence and limits

The release tests cover exact lease-scope substitution, invalid signatures and
time windows, one-shot consumption, inherited-pipe enforcement, wrong
cell/profile/epoch/path/purpose keys, metadata symlink/empty-file substitution,
manifest-generation gaps, external predecessor rollback, ciphertext-only
whole-tree canaries, nested storage-tree links, encrypted SQL spills, blobs,
indexes/search structures, tamper/truncation, and rotation crashes at both
namespace boundaries.

This phase does **not** claim attested microVM enforcement, a production KMS
deployment, cell-scoped HA/backup restore, device replicas, cross-cell grants,
fleet release governance, independent cryptographic review, or regulated-data
admission. Those remain explicit later phases rather than implied properties of
the environment flag or the database cipher.
