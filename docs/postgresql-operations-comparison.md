# PostgreSQL operations comparison

This is an operator-oriented comparison with PostgreSQL 18. PostgreSQL wire
compatibility does not imply identical storage, isolation, extension support or
operational procedures. BicDB remains a developing system; qualify the exact
version against your application. Rows below describe capabilities and limits,
not certification of PostgreSQL parity.

## Reading support and evidence

The labels below answer three separate questions:

- **Implemented:** the relevant code path exists, within the stated limits.
- **Verified:** specified checks actually ran against an identified source version
  and environment. A test file by itself is a reproducible check, not an execution
  record. Passing a few cases does not establish an entire feature's compatibility.
- **Production-qualified:** the exact build, configuration, workload, failure
  model and deployment have passed recorded operational acceptance, including
  recovery and agreed RPO/RTO. Unit/integration tests and a deployment example
  cannot establish this status.

The [configuration and regression execution record](evidence/server-configuration-2026-11-05.json)
pins the local Linux test runs behind the verified entries below. Its broad
regression run precedes an upstream SQL merge; the final combined source has
separate configuration integration and workspace-check results. This matrix
claims **no blanket production qualification** for PostgreSQL workloads.
Release-specific differential SQL evidence has its own narrower scope in the
[PostgreSQL compatibility report](../POSTGRES_COMPATIBILITY.md).

## Operational comparison

| Area | PostgreSQL 18 | BicDB |
| --- | --- | --- |
| Databases and schemas | Multiple databases per instance, schemas within databases | Standalone database or `serve --cluster` database routing; SQL schemas/catalog compatibility is a subset. Databases sharing a server share its process/failure domain. |
| Configuration | Commented configuration, scoped settings and reload/restart distinctions | Versioned TOML, environment/CLI precedence, validation, effective JSON and running drift inspection; file changes require restart. See [configuration](server-configuration.md). |
| Transactions | READ COMMITTED, REPEATABLE READ and SERIALIZABLE with documented semantics | PostgreSQL frontend supports READ COMMITTED and explicit READ ONLY REPEATABLE READ. READ WRITE REPEATABLE READ and PostgreSQL SERIALIZABLE are rejected. Native engine/Oracle profile isolation capabilities do not establish PostgreSQL SSI parity. |
| Locks | Row/table/advisory locks, wait policies and deadlock handling | Row locks, NOWAIT/SKIP LOCKED and advisory interfaces exist. Row lock strengths and some wait paths differ; ordinary native contention can return a transaction conflict. Do not assume the full PostgreSQL table-lock matrix or retry behavior. |
| HA | Physical WAL streaming, synchronous/asynchronous policies and external failover tooling | Native ordered replication, acknowledgement policies, election/fencing and recovery mechanisms. Not PostgreSQL physical WAL replication; RPO depends on configured acknowledgements and failure policy. See [HA](high-availability.md). |
| Horizontal placement | Core PostgreSQL generally needs external sharding tooling | Distributed membership/range placement exists. Certified single-range quorum writes do not imply arbitrary distributed SQL or durable cross-range transaction parity. See [cluster operations](distributed-cluster-operations.md). |
| Internal authentication | Password/SCRAM and local/network methods | SQL users and SCRAM, including TLS channel binding. Configure server-wide authentication explicitly; PostgreSQL pg_hba.conf rule parity is not claimed. |
| External authentication | Certificate, GSSAPI, LDAP, PAM and other documented methods | Do not assume these PostgreSQL authentication integrations. TLS client-certificate authentication is currently rejected; HTTP/provider identity features are separate from pgwire login. |
| Authorization | Roles, memberships, grants and row security | Role/grant/RLS support with compatibility limits; test policies and ownership explicitly. Trusted embedded access requires its own security context. Protect filesystem access independently. |
| Plugins/extensions | Native extension ABI and ecosystem | Selected compatible SQL functions/extensions and BicDB's own WASM interfaces. PostgreSQL C extension binaries cannot be loaded as compatible plugins. |
| Same-directory protection | Server instance/data-directory ownership controls | Native directory lock rejects concurrent writers, including a second process. This is not shared-disk/RAC or network-filesystem qualification. |
| Backups and migration | Native physical backup/WAL plus logical exports | Native BicDB backup/restore; physical PostgreSQL backups and WAL are incompatible. Logical migration requires compatibility review and reconciliation. |
| Client protocol | Native PostgreSQL protocol, COPY and catalogs | PostgreSQL drivers work within implemented SQL/protocol/catalog coverage. Text/CSV COPY is available; binary COPY is not supported. An unmodified arbitrary pg_dump restore is not guaranteed. |
| Deployment | Native packages, containers and Kubernetes ecosystems | Native executable, container examples; Kubernetes packaging requires operator validation. No qualified first-party Kubernetes operator is claimed here. |

## Verification and qualification by area

“Not established” is a qualification limit, not a prediction of whether a
particular deployment will work. Tests named without an execution record remain
checks to run for your build and environment.

| Area | Implemented scope/status | Verification evidence and limits | Production qualification |
| --- | --- | --- | --- |
| Databases and schemas | Implemented subset | Executed [protocol checks](../crates/bicdb-pgwire/tests/protocol.rs): multiple persistent databases, transactional shared roles, database-local types and schema-qualified COPY/functions. Explicit key/index names still share a native global namespace; reuse across schemas is not qualified. No complete catalog/schema parity claim. | Not established for arbitrary PostgreSQL applications. |
| Configuration | Implemented TOML and inspection commands | Executed [resolver tests](../crates/bicdb-cli/src/server_config.rs) and [live CLI tests](../crates/bicdb-cli/tests/server_configuration_cli.rs): precedence, types, paths, unopened validation, frozen drift snapshots, TLS and hostname rejection. See pinned execution record above. | Workload/host resource sizing and rollout acceptance remain deployment-specific. |
| Transactions | Implemented READ COMMITTED and restricted repeatable-read form | Executed `pgwire_transaction_isolation_commands_accept_read_only_repeatable_read` and protocol transaction regressions. These do not prove PostgreSQL SERIALIZABLE/SSI behavior. | Not established beyond individually qualified isolation/retry workloads. |
| Locks | Implemented subset | Executed protocol row/advisory lock checks, including `tokio_postgres_row_locks_survive_protocol_messages_and_skip_before_limit`. Full PostgreSQL table-lock strengths, fairness and deadlock parity are not verified. | Application contention and retry behavior require qualification. |
| HA | Implemented native replication/failover | Executed [CLI failover checks](../crates/bicdb-cli/tests/ha_failover_cli.rs). These selected failure cases do not qualify every partition, topology or timeout policy. [HA guarantees and limits](high-availability.md) define the contract. | Required RPO/RTO and fencing must be demonstrated in the target topology. |
| Horizontal placement | Implemented membership/range-placement paths | [Cluster operations](distributed-cluster-operations.md) separates retained certified single-range behavior from remaining distributed SQL work. The configuration test run does not qualify placement. | No general cross-range SQL qualification claimed. |
| Authentication and TLS | Implemented internal login/SCRAM/channel binding; external methods limited | Executed password/SCRAM protocol cases, [TLS channel-binding tests](../crates/bicdb-pgwire/tests/tls_channel_binding.rs), explicit libpq interoperability check and authenticated configuration inspection. Client-certificate rejection is checked; LDAP/PAM/GSSAPI support is not inferred. | Identity provisioning, rotation and trust configuration require deployment acceptance. |
| Authorization/security | Implemented roles/grants/RLS subset and host protections | Executed protocol ACL/RLS/ownership cases and configuration snapshot denial after role/session-authorization changes. Selected cases are not an independent security audit. See [security contracts](production-security-hardening.md). | No blanket security certification; profile/evidence gates and workload policies require qualification. |
| Same-directory protection | Implemented native writer claim | [Directory-lock tests](../crates/bicdb-core/tests/single_writer_directory_lock.rs) are reproducible checks; they were **not rerun in the linked configuration batch**. Same-process tests alone do not qualify multi-host/shared-volume operation. | Network/shared filesystem behavior not established. |
| Backups/recovery | Implemented native backup/restore mechanisms | [Backup contracts and drills](backup-recovery.md) and [online backup tests](../crates/bicdb-pgwire/tests/online_backups.rs) describe separate checks. The configuration batch did **not** execute full online-backup/restore drills. | Requires restoration and recovery-time evidence for the exact deployment. |
| Migration/client protocol | Implemented PostgreSQL transport and text/CSV COPY subset | Executed protocol driver, catalog, COPY and binary-COPY rejection cases. No arbitrary pg_dump/pg_restore or production migration qualification follows. Use the [migration acceptance steps](postgresql-migration.md). | Requires source/target reconciliation, application acceptance and rollback rehearsal. |
| Extensions/plugins | Selected built-ins/WASM interfaces; native PostgreSQL extension ABI unsupported | The linked configuration batch does not establish third-party extension compatibility. Inventory and test each dependency; native PostgreSQL shared libraries are not compatible plugins. | Not established for arbitrary extension-dependent workloads. |
| Deployment | Native executable; container/systemd examples; Kubernetes requirements documented | Live CLI tests run as native Linux processes. Compose/Kubernetes production behavior was not qualified by that batch. See [deployment guide](production-deployment.md). | No qualified first-party Kubernetes operator or universal container deployment claim. |

Transaction access-mode parsing does not establish PostgreSQL-equivalent
read-only write enforcement; use tested roles/grants for authorization.

For security-sensitive decisions inspect [security hardening](production-security-hardening.md),
[transaction behavior](postgresql-transactions.md), [production deployment](production-deployment.md)
and [migration](postgresql-migration.md). Verify actual SQL error/retry behavior
and restore/failover results under your workload.

PostgreSQL baseline references: [configuration](https://www.postgresql.org/docs/18/config-setting.html),
[schemas](https://www.postgresql.org/docs/18/ddl-schemas.html),
[isolation](https://www.postgresql.org/docs/18/transaction-iso.html),
[locks](https://www.postgresql.org/docs/18/explicit-locking.html),
[authentication](https://www.postgresql.org/docs/18/auth-methods.html),
[standbys](https://www.postgresql.org/docs/18/warm-standby.html) and
[C functions/extensions](https://www.postgresql.org/docs/18/xfunc-c.html).
