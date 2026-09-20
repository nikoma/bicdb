# BicDB Cell runtime: Phase 1 operator contract

`bicdb-cell` is the capability-reduced, one-cell construction path introduced
in BicDB 1.0.314-beta. It is callable and tested, but it is intentionally **not
approved for regulated sensitive data**. Successful startup reports
`regulated_data_admitted = false` and exposes no database or network listener.

BicDB 1.0.316-beta adds an optional, still non-admitted, industry-agnostic
signed application host on top of this boundary. Its separate policy and
listener contract is documented in
[`cell-application-host-preview.md`](cell-application-host-preview.md). The
Phase-1 ceremony below remains the no-application, non-serving construction
path.

Use the separate `bicdb-cell` binary for the smallest dependency and command
surface. `bicdb cell ...` exists as a packaging-compatible wrapper, and
`BICDB_RUNTIME_MODE=cell` makes the general launcher select that same typed
path while refusing every general BicDB command.

## 1. Create release and cell key files

The Phase-1 key provider is development-only. It accepts paths to mode-0600
files, never key bytes in arguments or environment variables.

```bash
openssl rand 32 > release-signing.seed
openssl rand 32 > cell.key
chmod 600 release-signing.seed cell.key

bicdb-cell key-public \
  --signing-key release-signing.seed \
  --output release-signing.pub
```

An attested, one-cell KMS/HSM provider must replace `cell.key` before regulated-data
admission can be considered.

## 2. Prepare the immutable inputs

Create an empty volume and an artifact directory. Calculate the exact binary,
guest-image, and cell-key digests:

```bash
mkdir cell-volume artifacts
bicdb-cell digest /absolute/path/to/bicdb-cell
bicdb-cell digest /absolute/path/to/guest-image
bicdb-cell digest cell.key
```

Write `cell-manifest.json`, substituting the three digests. Digest text is
strict lowercase `sha256:` followed by 64 hexadecimal characters.

```json
{
  "format": "bicdb.cell-manifest/v1",
  "cell_id": "018f7b30-4f4d-7b5c-a1f6-a183663e1240",
  "manifest_generation": 1,
  "previous_manifest_digest": null,
  "jurisdiction": "IN",
  "storage": {
    "volume_id": "vol-7f92",
    "lineage_id": "lineage-7f92",
    "database_relative_path": "db",
    "database_format": 2,
    "encryption_profile": "phase1-xchacha20poly1305"
  },
  "runtime": {
    "bicdb_binary_digest": "sha256:...",
    "guest_image_digest": "sha256:...",
    "isolation_profile": "phase1-process-isolated"
  },
  "applications": [],
  "keys": {
    "authority": "file://development-fixed",
    "cell_kek_id": "kek-7f92",
    "key_epoch": 1,
    "key_fingerprint": "sha256:..."
  },
  "replication": {
    "group_id": "rg-7f92",
    "replica_id": "replica-primary",
    "writer_epoch": 1
  },
  "policy": {
    "security_profile": "phase1-development-deny-regulated",
    "egress_policy_digest": "sha256:...",
    "identity_policy_digest": "sha256:...",
    "trusted_release_policy_digest": "sha256:..."
  }
}
```

The three policy digests are mandatory signed placeholders in the no-application
Phase-1 path. In 1.0.316-beta they become exact content pins when the optional
cell application host is constructed; see the preview contract.

## 3. Sign, verify, and bind once

```bash
bicdb-cell manifest-sign \
  --input cell-manifest.json \
  --output cell-manifest.cbor \
  --signer-key-id release-2026-08 \
  --signing-key release-signing.seed

bicdb-cell verify \
  --manifest cell-manifest.cbor \
  --trusted-key release-2026-08=release-signing.pub

bicdb-cell volume-bind \
  --manifest cell-manifest.cbor \
  --volume cell-volume \
  --trusted-key release-2026-08=release-signing.pub \
  --signer-key-id release-2026-08 \
  --signing-key release-signing.seed
```

Binding uses create-new semantics and refuses a volume whose database path or
identity already exists. The signed volume identity permanently records the
initial manifest digest, CellId, volume, lineage, and database path.

## 4. Run the startup ceremony

```bash
bicdb-cell serve \
  --manifest cell-manifest.cbor \
  --volume cell-volume \
  --expected-cell-id 018f7b30-4f4d-7b5c-a1f6-a183663e1240 \
  --expected-volume-id vol-7f92 \
  --guest-image-digest sha256:... \
  --trusted-key release-2026-08=release-signing.pub \
  --key-file cell.key \
  --artifact-root artifacts \
  --check \
  --json
```

The runtime digest is always measured from the executable that is actually
running (`/proc/self/exe` on Linux). There is deliberately no flag or
environment variable that can redirect this check to a different file.

Remove `--check` to hold the verified cell open until an interrupt signal. This
still starts no listener. Application pins, when present, are files named
`<digest-without-sha256-prefix>.bicdb-app` inside `--artifact-root`; every byte
must match the signed digest before the database opens.

## 5. Environment-selected launcher

The general `bicdb` launcher accepts the following deployment translation:

```text
BICDB_RUNTIME_MODE=cell
BICDB_CELL_MANIFEST=/absolute/path/cell-manifest.cbor
BICDB_CELL_VOLUME=/absolute/path/cell-volume
BICDB_CELL_ID=018f7b30-4f4d-7b5c-a1f6-a183663e1240
BICDB_CELL_VOLUME_ID=vol-7f92
BICDB_CELL_GUEST_IMAGE_DIGEST=sha256:...
BICDB_CELL_TRUSTED_KEYS=release-2026-08=/absolute/path/release-signing.pub
BICDB_CELL_KEY_FILE=/absolute/path/cell.key
BICDB_CELL_ARTIFACT_ROOT=/absolute/path/artifacts
BICDB_CELL_CHECK=true
BICDB_CELL_JSON=true
```

Multiple trusted keys are separated with `;`. Key bytes are never accepted in
an environment value. When `BICDB_RUNTIME_MODE=cell` is set, `bicdb init`,
`serve`, cluster, app installer, RESP, sync, TUI, and all other general paths
fail before touching storage.

## Enforced now and admission gates still closed

Phase 1 enforces strict bounded deterministic-CBOR parsing, domain-separated
Ed25519 signatures over both signer identity and payload,
non-symlink regular files, immutable volume lineage, expected cell/volume/guest
identity, current database format, runtime and application digests, a fixed
cell-scoped key request and fingerprint, encrypted single-database open,
monotonic local manifest transitions, and key zeroization on drop.

Regulated-data admission remains closed until the architecture specification's later
gates exist: standards-profiled COSE and threshold release policy, attested
workload/KMS release, complete page/WAL/temp/index/backup encryption, external
anti-rollback state, independently enforced process or microVM isolation,
cell-native application execution and identity, egress enforcement, and
cell-scoped replication fencing and recovery certification.

See [BicDB Cell/Application Architecture](bicdb-cell-application-architecture.md)
for the normative target and threat model.
