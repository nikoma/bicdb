# Phase 7 cross-Cell object grants

Introduced in BicDB 1.0.324-beta, Phase 7 adds an industry-neutral protocol for
moving explicitly granted objects between independently isolated Cells. It
inherits the Phase-6 application, fleet, HA, recovery, and device boundary. It
deliberately does not admit regulated production data.

## Boundary

The recipient never opens the source Cell and never receives its Cell key. The
source creates a deterministic, signed, bounded, non-executable CBOR package.
Each object has an independent random 256-bit DEK, an XChaCha20-Poly1305
payload, and an RFC 9180 X25519/HKDF-SHA256/ChaCha20-Poly1305 envelope to one
certified recipient key.

The wire document can contain object identifiers, cryptographic metadata, and
ciphertext. It cannot contain SQL, WASM, migrations, provider handles,
filesystem paths, network targets, or a source database capability. Imported
bytes remain quarantined evidence; an industry-specific application decides
how, or whether, to interpret and materialize them.

## Independent authority roles

`GrantTrustPolicy` requires disjoint Ed25519 keys and a threshold of at least
two for each role:

- Issue approves the exact grant.
- Revocation stops future packages for the active grant epoch.
- RecipientKey certifies the recipient HPKE key, application, schema, key
  epoch, lifetime, and hardware-attestation digest.
- ImportAcceptance certifies content-policy and scanner evidence for one exact
  package before decryption is persisted.

Export signing keys and recipient encryption keys cannot reuse authority key
ids or Ed25519 material. Production recipient implementations are expected to
implement `RecipientPrivateKey` with an HSM or attested workload; the shipped
software key is a reference implementation and does not satisfy admission.

## Mutual trust and exact scope

Each Cell pins the remote Cell's trust-anchor digest. The anchor covers every
local security decision while excluding only outbound remote pins, avoiding a
recursive digest cycle. Both directions are checked for every grant.

A grant binds:

- source and recipient Cell, full policy digest, application digest, and
  schema generation;
- one certified recipient key;
- exact namespace, object id, source-version digest, category, and media type;
- one allowlisted purpose and an explicit redisclosure rule;
- issuance, validity, object/package byte limits, object count, package count,
  grant epoch, predecessor, and anti-replay nonce.

There is no wildcard or caller-defined filter language.

## Delivery and import

Packages form a contiguous signed predecessor chain. Source and recipient
ledgers reject skipped, repeated, stale, substituted, expired, future-dated,
or over-limit packages. Ledger transitions serialize concurrent contenders,
persist to the encrypted Cell database, flush, and only then publish the new
in-memory state.

The recipient verifies both policies, the grant quorum, recipient certificate,
export signature, package chain, ciphertext accounting, and threshold import
review before opening HPKE envelopes. Imported bytes and immutable provenance
are committed atomically with the recipient ledger. Those collections inherit
the Phase-5 commit fence, replication, backup, and recovery path.

The deterministic decoder rejects trailing or alternate encodings and has a
64 MiB absolute document bound. Policy limits cap a plaintext object at 32 MiB
and a package at 60 MiB; the encoded package is checked against the parser
bound as well.

## Revocation semantics

Threshold revocation prevents future delivery and import advancement for the
active grant. It does not erase objects already decrypted by a recipient. A
new disclosure by that recipient requires another explicit grant when policy
allows it; BicDB never treats revocation as retroactive plaintext destruction.

## Construction interfaces

The reduced Cell binary accepts:

```text
--grant-trust-policy /run/bicdb/grant-policy.cbor
--grant-exporter-key-id grant-export-a
--grant-exporter-signing-key /run/bicdb/grant-exporter.key
--grant-recipient-key-id grant-recipient-a
--grant-recipient-private-key /run/bicdb/grant-recipient.key
```

All five options are required together for the cumulative Phase-7 profile.
The manifest pins the exact grant-policy file digest before key release.
Exporter and recipient private keys must match keys pinned inside that policy.
Standby/recovery replicas verify the documents before key release but do not
construct grant authority.

With `BICDB_RUNTIME_MODE=cell`, the launcher maps:

```text
BICDB_CELL_GRANT_TRUST_POLICY
BICDB_CELL_GRANT_EXPORTER_KEY_ID
BICDB_CELL_GRANT_EXPORTER_SIGNING_KEY
BICDB_CELL_GRANT_RECIPIENT_KEY_ID
BICDB_CELL_GRANT_RECIPIENT_PRIVATE_KEY
```

The callable Rust surface activates and accepts grants, exports and imports
packages, records revocation, exposes the pinned trust policy, and returns
immutable imported evidence. It never returns a remote Cell handle.

## Admission status

`regulated_data_admitted` remains `false`. The repository supplies protocol
types, cryptographic bindings, encrypted durable ledgers, runtime construction,
and adversarial reference tests. The `hpke` dependency states that it has not
received a paid third-party audit. Independent cryptographic review, complete
protocol fuzzing, production HSM/attestation, opaque relay evidence, attested
microVM enforcement, and all remaining fleet admission gates are still
required.
