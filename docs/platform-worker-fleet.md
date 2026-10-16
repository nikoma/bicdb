# Application worker fleets

An application can execute on a pool of compute workers while its SQL database,
transaction receipts, outbox and durable jobs remain at its authoritative state
service. A worker failure replaces the computation without moving or restoring
the database. Edge databases with selective synchronization are a separate
deployment model; they do not become authoritative replicas of this service.

The state service remains a single authority. This mode provides compute
failover, not automatic database or control-plane failover. If the state service
is unavailable, workers stop when their leases expire. Recover that service from
its durable storage or an encrypted application recovery image. Do not start
independent state services against copied application catalogs and treat them as
an HA pair.

## Configure workers

Each worker belongs to one state service. Start it with a separate data directory
and a dedicated management token. The command creates an owner-only token file
if it is absent:

```sh
bic --data /var/lib/bic-worker worker \
  --listen 127.0.0.1:8789 \
  --state-endpoint https://state.example.org \
  --capacity 16
```

Expose remote listeners through operator-managed HTTPS termination. Worker and
state endpoints require HTTPS except for loopback development. The worker's
state destination is operator-pinned; application packages cannot change it.
Both services must be reachable over the configured origins. Avoid caching or
retrying execution-management requests at the reverse proxy.

Copy each worker's management token into an owner-only secret file available to
the state service. Create an owner-only fleet file:

```json
{
  "format": 1,
  "workers": {
    "west-a": {
      "endpoint": "https://worker-a.example.org",
      "token_file": "secrets/worker-a.token",
      "region": "west",
      "capacity": 16,
      "isolation": "process"
    },
    "west-b": {
      "endpoint": "https://worker-b.example.org",
      "token_file": "secrets/worker-b.token",
      "region": "west",
      "capacity": 16,
      "isolation": "process"
    }
  }
}
```

Relative secret paths resolve from the fleet file's directory. Use a distinct
token for each worker and retain the ordinary separate state-management token.
Restart the state service to apply operator configuration changes:

```sh
bic --data /var/lib/bic-state serve --fleet-config /etc/bic/fleet.json
```

Add `"placement": {"region": "west"}` to the ordinary application manifest,
then run `bic deploy`. Placement chooses an available matching worker with the
lowest proportion of assigned slots, with stable name ordering for ties. Failed
activations place the worker on a short retry delay and try another candidate.
A region or isolation requirement never falls back to local execution.
`bic list` includes the worker name. Capacity counts applications, not requests
or aggregate memory; operators must size hosts for the declared workloads.

For VM workers, provide the worker's `--microvm-config` using the
[microVM operator procedure](platform-microvm.md), and set the corresponding
fleet entry's `isolation` to `microvm`. Each eligible VM worker needs its own
operator allocation for that application name. The guest's scoped API listener
proxies only its application's requests to the pinned state service. The state
service itself does not need KVM or guest images for remote execution.

## Failure and delivery contract

The state service renews each execution independently. A worker grants a
five-second monotonic lease and checks it at admission and during periodic
cleanup. An expired execution is killed and cannot be renewed. Losing the state
service, including suspension of its entire process, therefore stops compute
without requiring a stop message. Scheduling and host overload can delay cleanup;
this is not a real-time deadline guarantee.

Replacement rotates the application's database capability. The state service
rechecks it after database admission, so an old queued request cannot gain
authority after replacement. Failed and indeterminate activation attempts use
different capabilities. A late timeout from an old request can retire only that
request's execution. Best-effort stop messages accelerate termination; leases
handle a lost message. Public request forwarding and job dispatch require the
execution capability, separate from worker-management access.

HTTP requests in flight can fail during replacement. The platform does not
silently replay them. Retry transactions with a stable application idempotency
key. The receipt and SQL effects remain together at the state service even if a
worker disappears before returning its response. Durable job deliveries may
repeat; database receipts can deduplicate their effects. External side effects
still need their own idempotency keys.

JavaScript memory, process-local files and VM temporary files do not move between
workers. Bundle application dependencies in the release and keep durable data in
BicDB. Process workers execute trusted code with the worker user's OS authority;
microVM workers provide the documented jailed compute boundary. Worker operators
and the state service remain trusted.

## Validation

`cargo test -p bicdb-app-host --test fleet` runs separate state and worker
processes on loopback. It exercises a worker crash, placement on another worker,
capacity admission, remote log access, transaction and job deduplication,
credential revocation, and lease expiry while the state process is suspended.
With the operator assets described in the VM guide, also run:

```sh
BICDB_TEST_MICROVM_CONFIG=/etc/bic-microvm-test.json \
  cargo test -p bicdb-app-host --test fleet -- --include-ignored
```

That test runs a real jailed VM on a worker, kills the worker supervisor,
restarts it, fences its orphaned VMM and resumes against the state service without
storing an authoritative database on the worker. These are failure tests on one host,
not evidence of geographically distributed performance or a network SLA.
