# Database identity and connection pooling

RLS and concurrent writes do not inherently require a connection pool per end
user. Pooling depends on how the database obtains the identity used by policies.

## BicDB over pgwire

After password/SCRAM authentication, BicDB constructs a trusted security context
from the login's bound user, tenant, workspace, client, roles, and scopes. That
context belongs to the physical connection and cannot be replaced by ordinary
SQL session settings. Rebinding a login changes future connections; existing
connections keep their original context until disconnected.

See `connection_security_context` in
[pgwire authentication](../crates/bicdb-pgwire/src/validation.rs) and
`create_user_with_identity` / `set_user_security_identity` in
[the pgwire API](../crates/bicdb-pgwire/src/lib.rs).

Multiple logins with distinct bindings can serve multiple users. A deployment
whose only application login is bound to Alice cannot reuse that identity-bound
connection to execute Bob's requests as Bob.

## Carrier's default integration

Carrier's Node and Rust pgwire integrations compare the verified request's
identity with the connection's trusted identity. A mismatch is rejected. The
Node error code `database_identity_mismatch` is emitted by Carrier, not evidence
of a BicDB single-user capacity limit.

Carrier's generated Node service currently creates its application pools from
one `DATABASE_URL`. If that login is bound to one end user, another end user's
database requests cannot pass the identity check. More connections using the
same credentials do not fix this. Switching to Carrier's standalone Rust target
also does not remove the binding requirement.

In Carrier's PostgreSQL path, the connection authenticates the application and
Carrier establishes a signed request context for policy evaluation. That permits
a physical connection to serve different users after the previous context is
cleared. BicDB's default Carrier pgwire path instead uses the connection-bound
identity. Its signed operation envelope does not replace that identity.

Trusted identity establishes the caller; it does not replace mutation
permission. Bound and transaction-delegated callers still execute authored
trigger bodies, including operation-authority guards. No function name exempts
a trigger from execution. If a guard requires a verified operation envelope,
that envelope must be supplied through the application's supported authorization
path; a trusted login alone cannot satisfy it. Deployments that previously
relied on skipped guard bodies will now receive the guard's rejection until
their operation authorization is established.

Carrier source reference names (in the separate Carrier repository):

- `crates/carrier-codegen-node/src/lib.rs`: `render_db_mjs`,
  `assertCarrierBicDbIdentity`, `primeCarrierDbSession`, `acquireCarrierClient`.
- `crates/carrier-runtime/src/lib.rs`: `validate_bicdb_connection_identity`.

## Integration choices, not shipped promises

One approach is to route each verified identity to matching provisioned login
credentials. Connections should be created on demand with an aggregate limit,
idle eviction, bounded waiting, and invalidation when credentials or permissions
change. This must include tenant/workspace/client and role/scope context, not
only user ID. It does not require permanently opening connections for every
registered user. Provisioning must also supply the required restricted SQL
grants. Idempotency bookkeeping and handler work need budgets that avoid a
request holding one connection while waiting indefinitely for another.

BicDB also supports opt-in [verified transaction-scoped delegation](transaction-delegation.md).
An operator authorizes the application login and its tenant scope. Each explicit
transaction installs a signed, short-lived identity bound to a fresh database
challenge. RLS uses that identity until commit or rollback restores the original
connection context. This permits a shared application pool without one login per
end user. It requires a delegation-aware adapter; removing Carrier's identity
check or setting a user-ID variable is not sufficient.

BicDB's native application runtime constructs security context from a host actor
(see `raw_sql_security_context` in
[the native host](../crates/bicdb-app-runtime/src/host.rs)). That is a different
execution topology, not an automatic fix for an external pgwire application.

Verify the deployed binary and adapter versions when diagnosing an application.
A login mismatch establishes an integration/configuration problem; it says
nothing by itself about the number of registered users or concurrent writes the
system can sustain. Those require workload-specific tests.
