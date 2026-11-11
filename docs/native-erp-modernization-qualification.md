# Native ERP modernization qualification

The application host can add a resource's first optimistic version counter during an additive
upgrade when all of these signed conditions hold: the field is new, non-nullable `Int64`, has
the literal default `1`, is excluded from writable create/update fields, and has a signed
literal-one backfill transformation. Existing writable fields cannot be reinterpreted as
counters. Renaming or removing an active counter remains refused. The other persisted mutation,
tenant, workspace, role, audit and encryption-policy compatibility checks remain in force.

Native HTTP TLS selects its AWS-LC provider per listener. This avoids Rustls's ambiguous-global-
provider panic when another enabled integration also brings in the ring feature. Non-loopback
HTTP still requires TLS; certificate verification is not disabled.

OIDC resource authentication accepts the standard space-separated `scope` claim and preserves
the existing `scopes` array contract. Invalid scope-token characters and duplicate aliases fail
closed; their authorities are not combined. Standard `sid` and `org_id` are aliases for the
existing session and organization fields. Issuer, audience, signature, lifetime and application
authorization checks still apply.

Run the native process regressions with Node and an explicit compiler/binary pair:

```sh
BICDB_BINARY=/path/to/bicdb CARRIER_BINARY=/path/to/carrier \
  node scripts/qualify-optimistic-upgrade.mjs
BICDB_BINARY=/path/to/bicdb CARRIER_BINARY=/path/to/carrier \
  node scripts/qualify-native-oidc.mjs
```

The first probe seeds an ordinary record, upgrades/backfills it, rejects stale writes, reopens
the store, rejects counter removal/rename/reinterpretation, and checks offline operator rollback
without losing the edited data. The second uses an isolated fixture issuer and native TLS
listener to test standard/legacy claims, denied scopes/roles/audiences/issuers, expiry and tenant
isolation. It also races eight editors against one version (one success, seven conflicts),
kills and reopens the native server, and restores a cold copy while the original store is
offline. The acknowledged data and signed application catalog must survive. Both create private
temporary stores and save machine-readable evidence and logs.
Neither runs Rust tests, edits compiler output, nor touches a customer database.

These are native platform regressions. They do not establish real-issuer Hub acceptance,
financial correctness, production deployment qualification or safe rollback after new customer
financial transactions. Those require the product's coherent package graph, authorized identity,
financial fixtures, writer/backup boundaries and recovery evidence.

## Executed evidence

On November 11, 2026, both Node scripts passed against the locked 1.0.472-beta native CLI
(candidate SHA-256 `fd2e0fe673aa65fd7ea50b1fd1cf56ec9fc3b9eeea8681c2fd23c40f39b432aa`)
and Carrier 2.3.83 (`ecfc9e030a7526449caa20af6527a642dab109ac`). The earlier
1.0.471-beta binary reproduced both the counter-upgrade refusal and native TLS provider
panic; a loopback-only probe additionally reproduced standard OAuth scope denial. No Rust
tests were compiled or run. The candidate build used `cargo build --locked -p bicdb-cli
--release`, two jobs, incremental disabled and release LTO/debug disabled. Formatting after
this run does not change the recorded binary identity; release consumers must pin the
separately recorded artifact and source manifest.
