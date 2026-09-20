# Verified transaction-scoped delegation over pgwire

An operator can authorize an application SQL login to delegate verified end-user
identity. The same bounded pool can then serve different users in successive
transactions. Ordinary SQL settings cannot install this identity.

## Provisioning

Create a restricted application login and grant only the SQL privileges required
by the application. Use a separate owner login for migrations. With a randomly
generated secret of at least 32 bytes supplied through the process environment:

```sh
bicdb user delegate carrier_app --path /srv/bicdb/data \
  --signing-key-env CARRIER_BICDB_DELEGATION_KEY --tenant tenant-a
```

Repeat `--tenant` to authorize additional tenants. `--tenant '*'` explicitly
trusts this application for every tenant; use it only for an application whose
verified authentication and membership checks cover those tenants. Authorization
belongs to the application login, not to a SQL role an ordinary user can acquire.
The server must require authentication (`--require-auth`). Use TLS when the
application and database communicate across an untrusted network.

The operator policy is stored in `server_delegation.json`, written atomically
with owner-only permissions on Unix. Concurrent operator updates are serialized.
Include this file in protected operator backups; it contains signing secrets.
Never distribute the delegation key to browsers or end users.

## Protocol

1. Authenticate the pooled connection as the application and execute `BEGIN`.
2. Execute `SELECT bicdb_delegation_challenge()`. The text result is a JSON object
   with `version: 1`, `audience` (the SQL login) and a fresh `challenge`.
3. Add `issued_at` and `expires_at` as Unix seconds, `user_id`, `tenant_id`, and
   optional `client_id`, `workspace_id`, `roles`, and `scopes`. These must come
   from verified authentication and authorization, never unverified request
   headers or a user-supplied identity object.
4. Serialize the complete JSON object once. Compute HMAC-SHA256 over those exact
   UTF-8 bytes with the operator-provisioned key, encoded as hexadecimal.
5. Execute `SELECT bicdb_delegate($1, $2)` with the JSON text and signature.
6. Execute application queries. `COMMIT` or `ROLLBACK` ends delegation before
   the connection is returned to its pool.

Prepared statements are supported. Both host functions must be direct,
unmodified `SELECT` calls. Delegation must precede writes, DDL and savepoints.
The identity cannot change within the transaction, including after rollback to
a savepoint. `COMMIT AND CHAIN` and `ROLLBACK AND CHAIN` start a new transaction
without the previous delegated identity.

Claims have a maximum lifetime of 60 seconds. Issuance may be no more than 30
seconds old or five seconds ahead of the database clock. Keep clocks synchronized.
The JSON payload is limited to 16 KiB; unknown fields are rejected. Roles and
scopes cannot contain commas, NUL characters, or surrounding whitespace. The server verifies signatures
in constant time and checks audience, challenge, expiry and tenant authority.
A captured token cannot be reused on another connection or transaction.

Delegation installs a trusted authenticated identity for RLS; it does not grant
SQL privileges, superuser authority, an internal service identity, or RLS bypass.
The application must still verify users and their tenant membership. Compromise
of both application credentials and its delegation key permits impersonation
within the operator-authorized tenant set.

## Revocation and cleanup

```sh
bicdb user delegate carrier_app --path /srv/bicdb/data --revoke
```

Changing the policy, rotating its key, or revoking it invalidates active delegated
transactions at their next query or commit. Expiry is checked there as well,
including extended-protocol portal execution and COPY completion. This does not
interrupt an already-running statement midway. Rollback remains available.

Transaction completion restores the original connection identity and invalidates
identity-sensitive catalog caches and delegated portals. Application adapters
must roll back on errors and cancellation; a failed cleanup must discard the
connection. They must never report a write successful before commit succeeds.

## Validation

The pgwire delegation suite covers signed claims, forged signatures, wrong
audience/tenant/challenge, replay, expiry, key rotation, revocation, savepoints,
transaction-ending aliases, read/write RLS, prepared calls, pool reuse, and
concurrent private policy updates:

```sh
CARGO_BUILD_JOBS=2 cargo test -p bicdb-pgwire --lib delegation::tests --locked -- --test-threads=1
```

This establishes identity isolation behavior. Application compatibility and
capacity require separate end-to-end and workload tests; registered user count
does not equal simultaneous database connections.

## Hub SQL compatibility validation

The Hub deployment acceptance run applies its real migrations and restricted
application-role grants before exercising HTTP requests. Passing delegation
protocol tests alone does not establish that a Hub release is deployable.

The SQL session supports PL/pgSQL `RETURNS TABLE` functions using `RETURN QUERY`
in a single FROM item and correlated inner or left joins, including SELECT CTEs
used by an UPDATE. Calls retain the statement transaction and existing function
EXECUTE checks; SECURITY DEFINER changes SQL role authority through the ordinary
routine execution path and does not replace the delegated identity. An error in
the outer statement rolls back function writes in an implicit transaction.
Multiple `RETURN QUERY` statements append rows; plain `RETURN` ends execution.
`RETURN NEXT`, dynamic `RETURN QUERY EXECUTE`, WITH ORDINALITY, and anonymous
record results without named output columns remain unsupported. This is not a
claim of complete PostgreSQL procedural-language compatibility.
