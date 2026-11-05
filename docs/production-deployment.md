# Running BicDB in production

BicDB is a native executable; a container is optional. Qualify the exact binary,
workload and recovery plan before adopting a release. Native hosting, container
packaging and Kubernetes orchestration are different deployment choices, not
separate SQL compatibility guarantees.

## Native hosts

Run `bicdb serve --config /etc/bicdb/bicdb.toml` under a dedicated unprivileged
account. Use persistent local storage with tested flush behavior, separate
backup storage, explicit file permissions and enough capacity for data, WAL,
checkpoints, temporary queries and restore operations. `storage_sync = "durable"`
is the server durability setting; avoid relaxed durability for authoritative data.

Start with the [commented configuration](examples/bicdb.toml) and
[configuration-based systemd unit](examples/bicdb-config.service). Provision the directories, TLS files,
authenticated SQL administrator and application roles first. The unit validates
the file before starting. Validate as that service user, inspect effective
settings and restart after changes. A successful validation is not a successful
restore test. This general service template does not run the separate production
security/evidence gates. Existing hardened deployments should preserve their
[profile-gated service](examples/bicdb.service) and
[security qualification](production-security-hardening.md) when migrating settings. Do not treat SIGTERM as reload.

Set authentication and verified TLS before listening on public interfaces.
Keep private keys/passwords in restricted files or a deployment secret manager.
Configure admission limits, query/temp budgets and shutdown timeouts from load
tests. Monitor RSS, disk space, queue rejection, WAL/replication progress and
query latency; an OS memory limit is not a workload admission policy.

A second native writer opening the same database directory is rejected by the
directory lock. This does not qualify shared network filesystems or make a
single data directory safe for multiple replicas. Offline administration tools
may also be refused while the server owns the directory; use SQL/status
interfaces for live inspection.

## Containers

The [Compose example](examples/container-compose.yml) requires an image you have
built or independently verified, pinned by digest. It is a packaging example,
not evidence of a published/certified official image. Mount durable data and
logs separately from a read-only configuration and secrets. Ensure the runtime
UID can write the volumes. Preserve a writable temporary directory and allow
the configured grace period when stopping the container. Container restart
alone does not provide database HA or backups.

## Kubernetes

A native BicDB process can be packaged in a container and scheduled on
Kubernetes-like systems. This repository does not establish a qualified
first-party operator or Helm chart. Validate your deployment's security
context, persistent storage/flush semantics, startup time, termination behavior,
resource limits and rolling-upgrade process.

Give every HA member its **own** persistent volume; never scale replicas over
one database directory. Database replication, membership and fencing must be
configured through BicDB's HA/distributed protocols. StatefulSet replica count
is not quorum configuration. Use network policies and verified transport TLS.
Keep ConfigMaps limited to non-secret configuration and mount credentials
separately. Design readiness around actual SQL availability and the intended
read/write role; liveness should tolerate recovery and avoid restart loops.
Do not run an offline command opening the live directory as a probe.

## Recovery and acceptance

Exercise backup creation and restore into an isolated directory, interrupted
startup, planned shutdown and node failover before cutover. Record recovery
time and allowed data loss for the actual acknowledgement policy. Compare
restored data and application workflows, not just process health. Preserve
rollback access to the old system until acceptance is complete.

Read [backup and recovery](backup-recovery.md), [HA](high-availability.md),
[distributed cluster operations](distributed-cluster-operations.md),
[observability](production-observability.md) and
[security hardening](production-security-hardening.md) for their specific
capabilities and qualification limits.
