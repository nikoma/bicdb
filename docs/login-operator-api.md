# Online pgwire login management

The optional host-operator API manages pgwire credentials and their trusted
identities while the database continues serving queries. It is independent of
SQL roles: `SUPERUSER`, role membership, database passwords, and identity scopes
do not authorize operator requests. SQL grants still control SQL privileges;
creating a login through this API does not create SQL roles or grant SQL access.

## Start the service

Provision a randomly generated bearer token containing 32–256 non-whitespace
ASCII bytes in a regular file owned by the server process user, with mode 0600
or stricter on Unix. Do not put the token in command-line arguments. An optional
trailing newline is accepted. Assign a stable operator actor name for the log.

```sh
bicdb serve /srv/bicdb --require-auth --auth-method scram-sha-256 \
  --operator-listen 127.0.0.1:5440 \
  --operator-token-file /run/secrets/bicdb-operator \
  --operator-actor provisioning-service
```

The same flags work with `serve-pg` and `--cluster`. A cluster has one operator
listener and uses the shared cluster authentication catalog. The API is disabled
unless explicitly configured. Its token is read once at startup; change it by
restarting the service. Each listener currently has one operator credential and
actor; do not share that credential with application users.

HTTP without TLS is restricted to an explicit loopback address. A non-loopback
listener requires the server's `--tls-cert` and `--tls-key`. When configured, the
operator endpoint always uses HTTPS with that same loaded certificate/key and
client-certificate policy. Configure clients to verify its certificate. An
operator API also requires pgwire `--require-auth`. For a loopback endpoint
behind an authenticated TLS proxy, restrict access to the proxy host and never
log Authorization headers or request bodies in the proxy.

Starting the operator API permanently marks its authentication directory with
`.server_operator_only`. Thereafter SQL `CREATE/ALTER ROLE ... PASSWORD` cannot
create or replace physical login credentials, and SQL `PASSWORD NULL` / `DROP
ROLE` cannot remove an existing physical login, even for SQL superusers. Use the
operator API or local `bicdb user` commands for those changes. Ordinary SQL-only
roles, memberships, and grants remain available. The boundary applies to every
server sharing this directory and survives restarts without the operator flags;
back up and restore the marker with the authentication catalog. Deployments that
have never enabled this API retain legacy SQL credential mirroring.

## Request format

Send `POST /v1/logins`, `Authorization: Bearer <operator token>`, and a JSON body.
Supply credentials through your HTTP client's protected secret input, never URL
parameters, SQL text, shell history, or process arguments. Responses carry
`Cache-Control: no-store`. No operation returns passwords, salts, password
hashes, SCRAM keys, or delegation signing keys.

| `operation` | Other JSON fields | Effect |
| --- | --- | --- |
| `create` | `username`, `password`, `identity` | Atomically creates credentials and an identity. Existing names return 409; they are never silently overwritten. |
| `identity` | `username`, `identity` | Replaces all identity fields, preserving credentials and enabled state. |
| `password` | `username`, `password` | Rotates credentials, preserving identity and enabled state. |
| `enabled` | `username`, `enabled` (boolean) | Disables or re-enables new authentication attempts. |
| `revoke` | `username` | Removes the login and its delegation policy. Missing names return 404. |
| `list` | Optional `after`, `limit` | Returns login metadata in username order; default limit 100, maximum 1000. Pass `next_after` from the response as `after` for the next page. |

For example, this body binds a new physical login to one trusted identity:

```json
{
  "operation": "create",
  "username": "application_pool",
  "password": "<supply securely at request time>",
  "identity": {
    "user_id": "principal-17",
    "tenant_id": "organization-4",
    "workspace_id": "workspace-2",
    "client_id": "application-1",
    "roles": ["member"],
    "scopes": ["records:read"]
  }
}
```

`user_id` is required and nonempty. `tenant_id` defaults to empty for an unbound
service identity. Optional client/workspace fields default to absent; roles and
scopes default to empty sets. Identity replacement is complete, not a partial
patch. These are operator-authorized attributes, not claims accepted from an
end-user request. Verify onboarding authority before assigning them. Identity
binding and signed transaction delegation remain separate mechanisms; this API
does not issue delegation policies or tokens.

Usernames contain 1–255 ASCII letters, digits, hyphens, or underscores. Passwords
contain 1–4096 UTF-8 bytes without NUL. Identity strings are bounded to 1024 bytes
and cannot contain control characters; roles and scopes each have at most 128
entries. Requests are limited to 32 KiB with a five-second body-read deadline and
two concurrent requests. Malformed or oversized requests return 400; missing or
incorrect operator credentials return 401; overload returns 429. Callers should
use bounded retries with backoff for overload. Unknown operations or top-level
fields are rejected. Identity metadata is returned only after operator
verification. Transport-level errors and requests refused before admission do
not have an operation-log entry.

Successful mutations return `login` metadata (`null` for revoke), plus
`request_id`. Lists return `logins`, `next_after`, and `request_id`. A disabled
login remains visible with `enabled: false`. Pagination is a sequence of current
snapshots, not a frozen list across concurrent changes.

## Consistency and session lifetime

All catalog writers, including the local `bicdb user` commands and SQL password
updates, serialize through the same cross-process file lock. Each update writes
and syncs a private temporary snapshot, atomically replaces the catalog, and
syncs its directory on Unix. Reads see an entire old or new record. Local
password rotation cannot clear the identity or re-enable a disabled login.
Use these tools instead of manually editing the catalog, and run the same
updated version for every writer; older binaries do not participate in the lock.
Upgrade every server that shares the catalog before relying on disable/revoke
semantics: older servers do not enforce the new disabled flag, handshake check,
or operator-only credential policy. The policy is checked under the same writer
lock at credential mutation, including a query that began before API activation.
If activation races a SQL transaction already in progress, SQL role metadata may
commit, but the protected physical credentials cannot be changed by its legacy
mirror. SQL metadata and the host credential catalog are separate stores.

Authentication reads the live catalog without a database restart. Attempts begun
after a successful rotation must use the new password. Attempts begun after a
successful disable or revoke fail. An in-progress authentication attempt is rejected if its login record changes
before its trusted context is established. This prevents an old password proof
from inheriting a newly bound principal, including revocation followed by reuse
of the same name. Clients may retry a rejected handshake using current credentials.

Established sessions retain their immutable authenticated identity until they
disconnect, including after identity replacement, rotation, disable, or revoke.
These operations do not terminate sessions or cancel their transactions. Drain
or disconnect pools when immediate removal of existing session access is needed.
A revoked login's delegation policy is removed. Existing delegated transactions
then fail their next protected query or commit under the existing per-statement
policy check; rollback remains available. This is stricter than the lifetime of
an ordinary login-bound session. Recreating a name never inherits the removed
login's delegation authority.

The catalog and delegation policy are separate files. Revocation removes policy
first, then the login. A crash between those steps can leave a login without its
former delegation authority; it cannot leave a deleted login's policy available
to a future replacement. Retry or inspect metadata after recovery.

## Operation records and failure handling

The authentication directory contains owner-only `server_operator.jsonl`.
Records contain the configured actor (or `unauthenticated`), validated target
login, operation, outcome, request ID, and time. They exclude request bodies,
identity contents, credentials, verifiers, and arbitrary error text. Forward and
rotate this file with your host's log management; retain its owner-only
permissions. Each event append is serialized and synced. Mutations first record
`started`, then `succeeded`, `rejected`, or `indeterminate`. Authentication and
input failures record `unauthorized` or `invalid`.

If the initial log write fails, the API does not apply the operation. If a
storage or final log write fails after an update, the API returns 503 with an
explicit potentially indeterminate outcome. The event log and catalog are not
one transactional store: after 503, disconnection, or a crash, inspect login
metadata and, for credential rotation, test the intended credentials before
retrying. A `started` record without a final outcome requires the same check.
The API never pretends it rolled back an already-persisted credential change.
