# Replicated application state

`bic serve --state-config FILE` runs applications against operator-configured
native state groups. Each allocated application has its own database and
replication listener on each host. A group contains 3, 5, 7 or 9 nodes, with one
writer, majority elections and synchronous acknowledgements from half the other
nodes. A missing quorum refuses writes; this mode never degrades to asynchronous
acknowledgement.

The database stores immutable release packages, desired deployment state,
migration receipts, transaction receipts, SQL data and job delivery state.
Local unpacked files and `deployment.json` are verified caches. A successor
recovers the active and previous packages from its database without another
upload. Activation publishes an inactive deployment before migrations and an
enabled deployment after runtime readiness. An unsuccessful activation leaves
the route disabled.

## Local development group

Copy [the n1 configuration](examples/platform-state-n1.json) to three files.
Keep the group membership and `initial_primary` identical. Set the local
`node_id` to `n1`, `n2` and `n3` respectively. This example explicitly enables
plaintext transport on loopback; it is for a single development machine.

Create one owner-only management token and use it on all three hosts:

```sh
umask 077
openssl rand -hex 32 > state-management.token
chmod 600 state-n1.json state-n2.json state-n3.json

# Run these in separate terminals.
bic --data ./state-n1 --token-file state-management.token serve \
  --listen 127.0.0.1:8788 --state-config state-n1.json
bic --data ./state-n2 --token-file state-management.token serve \
  --listen 127.0.0.1:8789 --state-config state-n2.json
bic --data ./state-n3 --token-file state-management.token serve \
  --listen 127.0.0.1:8790 --state-config state-n3.json

bic --token-file state-management.token init ./counter --name counter
bic --token-file state-management.token deploy ./counter/bic.json
```

Applications in this mode need a declared state allocation before deployment.
An unallocated application is refused; it does not silently become standalone.
Existing applications in the host directory must all have allocations. A host
without `--state-config` retains the standalone deployment workflow.

`bic list` reports the local node, known primary and live writer authority for
each allocated application, including groups awaiting their first deployment.
Only the writer starts application code or consumes jobs. Native promotion,
demotion and snapshot bootstrap reopen the database in process, then reconcile
the deployment cache and execution.

Application and application-specific management requests sent to a standby
forward once to the known primary's operator-pinned HTTP origin. A second hop
is refused during a transition. Forwarded responses stream, including recovery
images. A forwarding request has a 120-second deadline; an interruption is
reported without automatic replay. The application runtime gateway retains its
existing response limits. Internal database capabilities are accepted only at
the active writer, with a new capability for each execution.

## Multiple hosts

Disable `dev_localhost_plaintext`. Configure `tls` on each group:

```json
"tls": {
  "certificate": "node.crt",
  "key": "node.key",
  "ca": "state-ca.crt",
  "server_name": "state.internal"
}
```

Paths resolve relative to the configuration file. Native peers authenticate
with mutual TLS. Every node entry must include `certificate_sha256`, the
64-character SHA-256 hash of that node's leaf certificate. The common peer
server name must appear in the peers' certificates. Replication addresses are
explicit IP addresses and ports. Each application needs a distinct cluster ID
and local replication listener.

HTTP `endpoint` values are separate, unique HTTPS origins reachable by clients
and compute workers. Operators provide HTTPS termination and certificates
trusted by those clients; the native replication CA is not an HTTP trust-store
setting. Keep management credentials the same across state hosts, and provision
each host's own TLS key and certificate pins. Protect configuration and secret
files with owner-only permissions.

The default failure timeout is five seconds, with writer authority expiring
after half that interval without enough peer confirmations. Configure
`failover_timeout_ms` between 3000 and 60000 and
`synchronous_timeout_ms` between 1 and 60000. Allow for migration, storage and
network latency when choosing these deadlines. Private schema initialization
releases the engine and confirms replication between independent DDL operations;
each application migration preserves its own transaction boundary. A migration
that outlasts writer authority fails safely and can require a deployment retry.

After a replication acknowledgement timeout, the operation can already be
durable. Retry application effects with their idempotency key. Jobs remain at
least once; external services need their own idempotency mechanism.

## Remote execution

The existing [worker fleet configuration](platform-worker-fleet.md) still selects
compute placement. Add every permitted successor state origin to each worker:

```sh
bic --data ./worker --token-file worker-management.token worker \
  --state-endpoint https://state-n1.example \
  --state-peer https://state-n2.example \
  --state-peer https://state-n3.example
```

Each execution selects an origin from this operator-provisioned list. A release
cannot add an arbitrary database destination. Lease renewal observes the state
writer's authority without taking a database lock. Authority loss revokes local
database capabilities and stops compute renewal even when SQL is busy. A worker
that cannot receive retirement still stops execution when its five-second lease
expires. A successor issues a fresh execution capability and chooses its own
pinned state origin. This applies to process and microVM worker tiers.

## Enrollment, recovery and boundaries

Before enrolling an existing standalone application, run the current standalone
host once to migrate historical local-broker jobs, then stop and fence it.
Start the state group and let replicas bootstrap before relying on failover.
First enrollment checkpoints the original database so empty tables and old WAL
update patches enter a complete snapshot. The original node can seed the
deployment record from its local catalog; other hosts recover it from native
replication. Application directories retain their cluster/node binding and
refuse ordinary standalone startup or a different replica identity.

Encrypted recovery images remain an explicit, inactive restore workflow. Restore
also disables the database-owned deployment record so later native enrollment
cannot reactivate the saved writer automatically. Fence the previous writer
before activating a restored copy.

Selective signed edge synchronization is currently refused for applications in
native state groups. Native row replication does not yet carry those original
signed journals. Use standalone edge applications for that workflow until the
journal integration is implemented. State groups do not provide active/active
SQL or migrate in-flight processes. They recover committed state and start new
execution, preserving the documented transaction and job idempotency contracts.

This general native HA path is separate from the hardened Cell HA capability
boundary described in [the high-availability guide](high-availability.md).
