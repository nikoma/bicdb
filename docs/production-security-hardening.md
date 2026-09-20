# Production Security Hardening

This guide is the BicDB production security review for issue #67. It records
deployment hardening evidence for greenfield transactional deployments that intentionally
use BicDB and PostgreSQL-compatible clients. It is not a regulatory compliance
certification.

This is a hardening guide for the current general server, not admission to the
target regulated-cell architecture. It MUST NOT be used to infer that today's
release is ready to hold a global sensitive-data fleet. The target architecture, callable
non-regulated Phase-1 boundary, known current gaps, and stronger regulated-data admission
gates are specified in
[`bicdb-cell-application-architecture.md`](bicdb-cell-application-architecture.md).
In particular, `regulated-data` below is an evidence profile for existing
single-instance evaluation; it does not provide per-cell runtime, workload
identity, key, storage, replication, or supply-chain containment.

## Production Gate

Run the deployment gate before publishing a production release:

```bash
BICDB_PROTECTED_FIELD_ENCRYPTION_KEY=<64-hex-characters-from-openssl-rand-hex-32> \
BICDB_PROTECTED_LOOKUP_HMAC_KEY=<a-different-64-hex-random-key> \
BICDB_BACKUP_KEY=... \
BICDB_DB_KEY=... \
cargo run -p bicdb-cli -- security production-gate /data/bicdb-production \
  --profile regulated-data \
  --host 0.0.0.0 \
  --require-auth \
  --auth-method scram-sha-256 \
  --require-tls \
  --tls-cert /etc/bicdb/tls/server.pem \
  --tls-key /etc/bicdb/tls/server-key.pem \
  --db-key-env BICDB_DB_KEY \
  --backup-key-env BICDB_BACKUP_KEY \
  --protected-data-field-key-env BICDB_PROTECTED_FIELD_ENCRYPTION_KEY \
  --protected-data-hmac-key-env BICDB_PROTECTED_LOOKUP_HMAC_KEY \
  --audit-retention-days 365 \
  --audit-tamper-evidence \
  --protected-data-evidence reports/protected-data-release-gate.json \
  --dependency-evidence reports/dependency-audit.json \
  --json
```

The gate rejects remote no-auth, missing required TLS, cleartext auth on
production profiles, unsupported client certificate configuration, weak/default
secret values, missing env-backed secret names, group/world-writable database
paths, group/world-readable TLS private keys, missing audit retention or
tamper-evidence settings, and missing or stale protected-data release evidence
security work in issues #39-#41.

Generate dependency evidence with:

```bash
cargo run -p bicdb-cli -- security supply-chain-audit --json \
  > reports/dependency-audit.json
```

The command uses `cargo audit --deny warnings` when available. If the
`cargo-audit` subcommand is not installed, it falls back to `cargo tree
--locked` as a practical locked dependency inventory, and the release owner
must record the stronger audit result before regulated data deployment.

## Hardening Profiles

| Profile | Intended use | Required gate posture |
| --- | --- | --- |
| `local-dev` | Single-developer laptop, loopback only, disposable data. | Local no-auth is allowed. Production evidence is not required. Do not use real sensitive data. |
| `shared-lan` | Shared private network with known clients. | Require auth, SCRAM-SHA-256, TLS, env-backed database and backup keys, audit retention, tamper evidence, protected-data/security evidence, and dependency evidence. |
| `production-server` | Internet-routed or shared server deployment. | Same as shared LAN, with no remote no-auth override and private file permissions for database and TLS key paths. |
| `regulated-data` | Production deployment handling protected data. | Production-server posture plus env-backed protected-data field-encryption and blind-index HMAC keys. |

## Threat Model

| Surface | Threats | Required mitigations | Evidence |
| --- | --- | --- | --- |
| Embedded mode | Direct process access bypasses pgwire auth; unsafe callers may use legacy APIs against protected data. | Protected collections fail closed without `SecurityContext`; admin bypass requires a non-empty reason and records a security event. | `crates/bicdb-core/tests/core.rs` admin bypass and tenant policy tests; issue #39. |
| Server mode | Remote no-auth, weak auth method, resource exhaustion, slow query leakage. | Non-local no-auth is refused unless explicitly allowed; production gate rejects it. Use SCRAM-SHA-256, query/request limits, slow-query redaction. | `crates/bicdb-pgwire/tests/protocol.rs` auth and server config tests; `security production-gate`. |
| Pgwire clients | Plaintext password exposure, unsupported client certificate assumptions, TLS downgrade. | Require TLS for production profiles; SCRAM-SHA-256 for production profiles; `--tls-client-ca` fails startup because client certificates are unsupported. | `require_tls_rejects_plain_startup_and_closes_connection`, `scram_auth_accepts_valid_user`, `client_certificate_configuration_is_explicitly_unsupported`. |
| Backups/restores | Backup key disclosure, stale or untested restore paths, missing PITR audit events. | Encrypted backup keys come from env/secret store; backup drill evidence is kept with release records; audit events are enabled before PITR use. | `docs/backup-recovery.md`; `crates/bicdb-core/tests/backup.rs`; `security production-gate --backup-key-env`. |
| Sync | Cross-tenant export/import, stale plaintext in synced protected records. | Secure APIs enforce tenant context before export/import; protected-data backfill compacts protected segments before release. | `protected-data-release-gate` tenant isolation and backfill evidence; `crates/bicdb-core/tests/sync_mesh.rs`. |
| Spatial/vector/analytics | Derived indexes or sidecars could expose protected data through metadata or vectors. | Protected-data controls reject sensitive vectors unless explicitly marked non-sensitive; encrypted fields cannot be normal indexes; analytics sidecars are derived and verified. | `docs/security.md`; vector/spatial tests; issue #40 evidence. |
| Admin commands | Bypass, compaction, restore, rotation, or HA promotion without audit trail. | Admin bypass requires reason and audit event; release runbooks require evidence records for backup/restore, key rotation, and promotion. | `bicdb.security` events; backup and HA CLI tests; this guide. |
| Protected-data and tenant controls | Plaintext protected data, direct SQL search over encrypted fields, tenant leakage across SQL/pgwire/Rust APIs. | Field encryption, blind indexes, raw SQL audit, tenant isolation gate, and fail-closed policy checks. | `security protected-data-release-gate`; `docs/security.md`; issues #39-#41. |
| Supply chain | Vulnerable or unexpected dependency updates enter release. | Run `security supply-chain-audit`; keep JSON evidence with release artifacts; prefer `cargo audit` for release blocking. | `reports/dependency-audit.json`; this issue #67 gate. |

## Key Management Runbook

The current general-server CLI obtains field encryption keys, blind-index HMAC
keys, backup keys, and database storage keys through named environment entries.
Those values must be generated outside source control and supplied by the
deployment platform or a managed secret store. Do not pass raw production
secrets on command lines, commit them to manifests, include them in support
bundles, or log them.

This mechanism is not the target regulated-cell key design. `bicdb cell serve`
MUST obtain only its own cell capability through an attested workload identity
and KMS/HSM provider; raw regulated-data keys MUST NOT be stored in environment
variables. The existing environment-key path is a compatibility mechanism, not
a future regulated-data security boundary.

For field encryption rotation:

1. Inventory protected collections and current `key_ref` values.
2. Run the protected-data release gate and capture a passing baseline.
3. Stage the replacement secret in the managed secret store.
4. Run the dry-run rotation path and verify old-key and new-key reads.
5. Rotate, persist the new `key_ref`, and archive the rotation report.
6. Rerun `security protected-data-release-gate` and `security production-gate`.

For blind-index HMAC rotation, create a new namespace such as
`patient-email:v2:`, backfill the new blind-index values, deploy readers that
query the new namespace, then retire the old namespace after retention and
rollback windows close.

For backup key rotation, take a new full encrypted backup with the replacement
key, verify it, run a restore drill, then retire old-key backup material only
after the retention policy no longer requires restoring old backup chains.

For incident response, freeze secret changes, preserve audit logs and backup
manifests, rotate exposed keys, rebuild blind indexes when HMAC keys are
exposed, run restore drills for affected backup chains, and attach fresh
production-gate evidence to the incident record.

## Audit Retention And Tamper Evidence

Production profiles require at least 365 days of security-sensitive audit
retention. Regulated-data deployments should use the longer retention period
mandated by their own policy. Audit records for admin bypass, security context
failures, backup/restore, key rotation, tenant isolation failures, migration
gate failures, and HA promotion must be retained with release evidence.

Tamper evidence means one of:

- BicDB audit streams plus hash-chain validation in release evidence;
- an immutable external log sink with write-once retention;
- signed release artifacts that include audit checksums and backup manifests.

Audit events must not include plaintext protected data, raw secrets, HMAC inputs, derived
keys, or decrypted values.

## Release Evidence Checklist

Before a production application release, archive:

- `security protected-data-release-gate --json` output from the same branch;
- `security production-gate --json` output from the same branch;
- dependency audit evidence;
- backup verify and restore drill reports;
- migration status and integrity check output;
- key rotation reports when keys changed;
- threat-model review sign-off for changed surfaces.
