# PostgreSQL operations comparison

This is an operator-oriented comparison with PostgreSQL 18. PostgreSQL wire
compatibility does not imply identical storage, isolation, extension support or
operational procedures. BicDB remains a developing system; qualify the exact
version against your application. Rows below describe capabilities and limits,
not certification of PostgreSQL parity.

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
