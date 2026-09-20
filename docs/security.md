# BicDB Security Context and Tenant Policy Boundary

BicDB core is the enforcement boundary for tenant visibility on protected
collections. Application adapters may provide policy metadata and current user
context, but protected records must not rely on generated code, SQL rewriting,
or client-side filtering for isolation.

## Core Types

Protected collections store policy metadata in `CollectionMeta.policy`:

- `CollectionPolicy.tenant_field` names the metadata field that must equal the
  caller tenant.
- `read_roles`, `write_roles`, and `delete_roles` define the roles accepted for
  each operation. Empty role sets allow any authenticated context for that
  operation.
- `columns` is reserved policy metadata for column-level controls. Unsupported
  secure search or encrypted-field behavior must return an error rather than
  falling back to plaintext exposure.
- `columns` may define protected-data field security metadata keyed by metadata field name:
  `encrypted`, `pii_category`, `key_ref`, `blind_index`, `redaction`, and
  `decrypt_roles`. Setting encrypted column metadata marks the collection as a
  protected-data collection.

Callers provide `SecurityContext { user_id, tenant_id, roles, bypass_policy }`.
Protected access fails closed when context is missing, `user_id` or `tenant_id`
is empty, the required role is absent, the policy is missing, or a record is
missing the tenant field.

## PostgreSQL RLS Catalog Metadata

BicDB accepts PostgreSQL row-level security migration DDL as catalog metadata:

```sql
ALTER TABLE records ENABLE ROW LEVEL SECURITY;
ALTER TABLE records FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS records_select_policy ON records;
CREATE POLICY records_select_policy ON records
  FOR SELECT
  USING (current_setting('bicdb.current_tenant', true) = org_id::text);
```

The RLS enabled/forced flags and raw `USING` / `WITH CHECK` predicate text are
persisted with table schema metadata and survive reopen. They are exposed
through `pg_catalog.pg_class`, `pg_catalog.pg_policy`, and `pg_policies`.
`pg_policy` and `pg_policies` also include BicDB-specific `bicdb_enforced`
columns. New SQL policies are marked `true` when BicDB will evaluate them.

BicDB enforces the generated-application policy expression subset used by generated
application migrations:

- trusted session attributes: `bicdb.current_tenant`,
  `bicdb.current_workspace`, `bicdb.current_user`,
  `bicdb.current_client`, `bicdb.current_roles`,
  `bicdb.current_scopes`, `bicdb.current_session`, and
  `bicdb.authentication_strength` are installed by the authenticated host and
  are readable through `current_setting(name, missing_ok)` for policy
  compatibility. `SET`, `SET LOCAL`, `RESET`, and `set_config` cannot change
  them. Other dotted application GUCs remain mutable and transaction-scoped in
  the usual way. `current_trusted_tenant()` exposes the bound tenant directly
  and returns `NULL` when the host did not authorize tenant authority.
- expressions: `coalesce`, `position(... in ...)`, `replace`, `||`,
  `::text`, equality/inequality, `AND`, `OR`, parentheses, `IS NULL`, and
  `IS NOT NULL`
- commands: `SELECT USING`, `INSERT WITH CHECK`, `UPDATE USING` plus
  `WITH CHECK`, and `DELETE USING`

RLS applies in SQL CLI sessions, pgwire simple queries, and pgwire prepared
statement execution because the policy check runs in the shared SQL execution
path. `FORCE ROW LEVEL SECURITY` is persisted and normal SQL sessions do not
bypass RLS. The explicit administrative bypass is a `SecurityContext` with
`bypass_policy` set; callers should reserve that for maintenance tooling and
record a reason.

Unsupported PostgreSQL RLS features, such as restrictive policy combination,
role-targeted policy clauses, and arbitrary SQL functions inside policy
expressions, are outside this compatibility subset. If an unsupported
expression is used on an RLS-enabled table, protected reads or writes fail
instead of silently ignoring the predicate.

## API Usage

Use `db.secure(&ctx)` for protected collections:

```rust
let policy = CollectionPolicy::tenant_field("tenant_id")
    .with_read_roles(["reader"])
    .with_write_roles(["writer"])
    .with_delete_roles(["deleter"]);
db.create_collection_with_policy("records", CollectionMode::Standard, policy)?;

let ctx = SecurityContext::new("user-1", "tenant-a").with_roles(["reader", "writer"]);
db.secure(&ctx).insert(
    "records",
    Record::new("p-1").with_metadata(serde_json::json!({"tenant_id": "tenant-a"})),
)?;
```

Legacy `BicDb::insert`, `get`, `scan_collection`, `delete`, vector search,
time-series reads, and raw graph projection builds remain available for
unprotected collections. When a collection has a policy, those legacy calls
return authorization errors.

SQL sessions that need protected collections must be created with
`SqlSession::new_secure(&mut db, ctx)`. An authenticated pgwire server loads the
operator-assigned identity from its user catalog only after password or SCRAM
verification and creates a fresh `SecurityContext` and session id for each
connection. `PgWireConfig.security_context` is reserved for explicit local
no-auth/internal hosts; it is ignored as a shared identity template when
pgwire authentication is enabled. Without trusted tenant authority, protected
table access fails closed in core.

Create an authenticated login with its security assignment in one operation:

```sh
bicdb user create alice \
  --password "$ALICE_PASSWORD" \
  --path /srv/bicdb \
  --user-id alice \
  --tenant tenant-a \
  --workspace workspace-1 \
  --role operator \
  --scope records:read
```

Existing password users authenticate without tenant authority after upgrade.
Bind their identity without rotating the password before allowing protected
access:

```sh
bicdb user bind-identity alice \
  --path /srv/bicdb \
  --user-id alice \
  --tenant tenant-a \
  --workspace workspace-1 \
  --role operator \
  --scope records:read
```

The client may request a database and ordinary connection settings, but cannot
request or replace the resulting user, tenant, workspace, roles, or scopes.
Prepared statements, transactions, savepoints, session reset, and worker-thread
execution all rebind the connection's immutable context rather than persisting
security authority in reusable GUC state.

## Tenant Write Rules

For tenant-owned collections, inserts and updates must include the configured
tenant field in record metadata and it must match `SecurityContext.tenant_id`.
Cross-tenant writes and missing-tenant writes are rejected before the record is
persisted, indexed, exported through graph projections, or made visible to SQL.
Deletes require both tenant visibility and delete permission.

## Admin Bypass

Admin bypass is unavailable by default. A caller must set
`SecurityContext.bypass_policy` with a non-empty reason. BicDB records a
`bicdb.security` / `PolicyBypass` event containing user, tenant, collection,
operation, and reason. Do not place secrets, plaintext protected data, HMAC inputs,
derived keys, or decrypted values in the reason or any audit metadata.

This is an internal host API, not a SQL setting. Assigning `platform_admin` to
`bicdb.current_roles`, changing `session_authorization`, or passing a startup
parameter cannot create bypass authority. Cross-tenant administration must
construct a separately authorized, short-lived `SecurityContext` with an
explicit reason; ending that invocation destroys the override.

## Adapter Responsibilities

Application adapters should pass collection policy metadata into BicDB
when creating protected collections, then pass the current authenticated user
context into `db.secure(&ctx)` or `SqlSession::new_secure`. Adapters may add
their own logging and request validation, but those checks are defense in depth;
BicDB core remains responsible for refusing unsafe protected access.

This boundary is necessary for sensitive-data and tenant isolation, but it does not by
itself constitute a complete regulatory compliance program.

## Protected-data column encryption

BicDB protected-data encryption is enforced in core above storage-frame encryption. For a
protected collection, secure writes encrypt configured metadata fields before
records are persisted, indexed, synced, or emitted to audit streams. The field
envelope uses XChaCha20-Poly1305 with AAD containing the database path,
collection, record id, tenant id, schema version, key version, and field name.
Moving ciphertext to a different tenant, collection, record id, schema version,
or key version fails authentication.

Configure protected-data columns through `CollectionPolicy::with_column_security`:

```rust
let policy = CollectionPolicy::tenant_field("tenant_id")
    .with_read_roles(["reader"])
    .with_write_roles(["writer"])
    .with_column_security(
        "email",
        ColumnSecurity::encrypted("contact-email")
            .with_key_ref("protected-field:v1")
            .with_blind_index("contact-email:v1:")
            .with_redaction(RedactionPolicy::Fixed("[redacted]".to_string()))
            .with_decrypt_roles(["protected-data-reader"]),
    );
```

Protected-data writes require `BICDB_PROTECTED_FIELD_ENCRYPTION_KEY`. Fields with blind
indexes also require `BICDB_PROTECTED_LOOKUP_HMAC_KEY`. Each value must be 32 bytes of
random key material encoded as 64 hexadecimal characters (generate one with
`openssl rand -hex 32`); passphrases are rejected rather than fed to a fast,
unsalted hash. Missing, malformed, or empty key material fails
closed with `EncryptionKeyRequired`.

Production deployments must load these values from a managed secret store or
KMS-backed envelope secret before opening protected collections. Do not commit,
print, trace, or include the raw values in release artifacts. Operators should
track key references such as `protected-field:v1` and blind-index namespaces such as
`contact-email:v1:` in change control so key rotation and lookup-index
backfills can be reproduced.

## Redaction And Decryption

Callers must use `db.secure(&SecurityContext)` or
`BicDb::get_with_context`/`scan_collection_with_context`. Read authorization is
checked first. If the caller has collection read permission but lacks a field's
`decrypt_roles`, BicDB returns the configured redaction (`null` by default or a
fixed string). Admin bypass with a non-empty reason returns ciphertext envelopes
for diagnostics and records a `bicdb.security` event.

Do not log plaintext protected data, HMAC inputs, key material, ciphertext envelopes, or
decrypted values. Error messages intentionally identify fields and policies
without including protected values.

## Blind Index Lookup

Exact lookup for encrypted protected data must use HMAC-SHA256 blind indexes. Namespaces
must be field-class/version prefixes such as `contact-email:v1:`,
`contact-phone:v1:`, or `account-birth-date:v1:`. The namespace is part of the
HMAC input, so the same plaintext hashes differently for different field
classes.

Use `BicDb::blind_index_filter(collection, field, value)` to build the approved
lookup filter. Do not hash values in adapters. Direct filters over encrypted
fields are rejected; exact lookup means equality on the approved blind-index
field only. Prefix, partial, fuzzy, plaintext full-text, and vector metadata
search over encrypted protected data are unsupported.

## Encrypted-Field Constraints

Encrypted fields cannot be primary keys, normal unique keys, normal indexes, or
searchable fields. BicDB core rejects direct `IndexField::MetadataPath` indexes
over encrypted fields and rejects direct `JsonFilter` predicates over encrypted
fields on secure vector search. Approved blind-index metadata fields may be
indexed for equality lookup.

Routing-safe plaintext is limited to record id, tenant id, timestamps, and
approved blind indexes. Vectors are rejected on protected-data writes unless the
policy explicitly sets `vector_non_sensitive` through
`CollectionPolicy::with_non_sensitive_vectors(true)`. Normal legacy
`BicDb::insert`, `get`, `scan_collection`, transaction writes, and search APIs
refuse protected collections; callers must use the secure API with a
`SecurityContext`.

Raw SQL or adapter code touching encrypted fields must use explicit safe helper
paths that call core encryption/blind-index functions, or an auditable
security-approved bypass with a documented compensating control. BicDB does not
use generated application code, pgwire SQL rewriting, or client behavior as the
security boundary.

## Protected-data backfill and release evidence

Legacy plaintext records must be backfilled before release. The release gate
uses `BicDb::backfill_protected_data_security_evidence` to scan protected collections,
encrypt legacy plaintext fields, fill missing blind indexes, verify ciphertext
sampling, and compact changed collection segments so stale plaintext append
frames are removed from protected record storage. The backfill report includes
`scanned_rows`, `encrypted_rows`, `blind_index_rows_written`, `skipped_rows`,
`errors`, and `verification_status`; rerunning the backfill is expected to be
idempotent with no newly encrypted rows or blind-index writes once the database
is correct.

Run the release-blocking evidence suite locally and in CI with:

```bash
BICDB_PROTECTED_FIELD_ENCRYPTION_KEY=<64-hex-random-key> \
BICDB_PROTECTED_LOOKUP_HMAC_KEY=<different-64-hex-random-key> \
cargo run -p bicdb-cli -- security protected-data-release-gate /path/to/db --source-root . --json
```

Expected successful output is a JSON `ProtectedDataSecurityEvidenceReport` with
`passed: true`, `backfill.verification_status: true`,
`blind_index_coverage.passed: true`, `ciphertext_sampling.passed: true`,
`tenant_isolation.passed: true`, and `raw_sql_audit.passed: true`. Any false
value, missing key, backfill error, plaintext finding, tenant leak, missing
blind index, wrong namespace, plaintext search path, or raw SQL violation is a
release blocker.

The gate records evidence for:

- backfill counts and verification status;
- blind-index coverage for every approved exact-match lookup field;
- ciphertext sampling proving protected fields are stored as envelopes and
  unauthorized contexts cannot read plaintext;
- tenant isolation across list, read, create, update, delete, report, export,
  cached/dashboard reads, vector search, graph projection, sync export/import,
  SQL, pgwire, and direct Rust API surfaces;
- raw SQL scans for encrypted-field touches that lack `encrypt(...)`,
  `blind_index_filter`, a `security-approved` marker, or a documented
  `compensating-control`.

Raw SQL negative fixtures should be tested in temporary test data or clearly
marked as security-approved negative tests. Do not leave intentionally unsafe
fixtures in release-scanned source paths without an explicit marker and a test
asserting the audit catches the unsafe form.

For production transactional deployments, layer the operational hardening gate in
[Production Security Hardening](production-security-hardening.md) on top of this
protected-data evidence:

```bash
cargo run -p bicdb-cli -- security production-gate /path/to/db \
  --profile production-server \
  --host 0.0.0.0 \
  --require-auth \
  --auth-method scram-sha-256 \
  --require-tls \
  --tls-cert /etc/bicdb/tls/server.pem \
  --tls-key /etc/bicdb/tls/server-key.pem \
  --db-key-env BICDB_DB_KEY \
  --backup-key-env BICDB_BACKUP_KEY \
  --audit-retention-days 365 \
  --audit-tamper-evidence \
  --protected-data-evidence reports/protected-data-release-gate.json \
  --dependency-evidence reports/dependency-audit.json \
  --json
```

That gate rejects unsafe local-development defaults in shared deployments and
fails when protected-data/security evidence is missing or stale. It is
production hardening evidence, not a compliance certification.

## Protected-data key rotation

`BicDb::rotate_protected_data_field_encryption_key` provides the rotation foundation for
field envelopes:

1. Inventory collections with encrypted protected data metadata.
2. Verify old-key decryption for each encrypted field.
3. In non-dry-run mode, write a segment backup beside the original segment.
4. Re-encrypt field envelopes with the replacement key and `new_key_ref`.
5. Verify replacement-key reads before replacing the segment.
6. Persist updated policy metadata and mark the old key retired in the report.

Dry-run mode performs inventory, old-key verification, replacement-key
verification, and count reporting without writing backups or replacing
segments. Rotation currently covers BicDB core field envelopes; external KMS
wrapping and multi-key provider persistence are extension points, not a
regulatory compliance claim.

After rotation, run the protected-data release gate again. Rotation changes must include
the new `key_ref`, operator approval for managed-secret rollout, and a fresh
release evidence report in the same branch. No deferred follow-up issue is
recorded in the security audit; future intentionally deferred protected-data/security gate gaps
must be filed as explicit GitHub issues instead of being hidden in prose.
