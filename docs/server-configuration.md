# Server configuration

BicDB accepts a commented TOML file for `serve` and `serve-pg`. The same
resolver powers server startup and the configuration inspection commands.
`version = 1` identifies the file format, independently of the binary version.

```toml
version = 1

[server]
path = "./data"
host = "127.0.0.1"
port = 5433
storage_sync = "durable"
max_connections = 100
query_timeout_ms = 30000
redact_field = ["ssn", "tax_id"]

[engine]
query_work_memory_bytes = 16777216
query_temp_space_bytes = 1073741824
```

```sh
bicdb config validate --file bicdb.toml
bicdb config show --effective --file bicdb.toml
bicdb serve --config bicdb.toml
bicdb config diff --running --file bicdb.toml
```

`validate` checks parsing, types, option dependencies, engine bounds and
referenced configuration/TLS material using the startup builder. It does not
open a database, bind listeners, connect to peers or prove that a deployment
has sufficient resources. Run it as the service account to check file access.
Use `--json` for machine-readable successful validation.

`show --effective` prints JSON containing defaults, resolved values, their
origins and a fingerprint. This is a desired configuration report; operational
validation is explicitly marked `not_performed`. It does not read secret
contents or TLS keys. Startup aliases `serve --check-config` and
`serve --print-effective-config` perform startup validation before returning.

`diff --running` reads the immutable startup snapshot over the PostgreSQL
protocol with `SELECT * FROM bicdb_server_configuration`. Only a SQL superuser
may read its rows. Embedded hosts must explicitly publish a redacted snapshot;
older servers cannot supply one. Exit codes are **0** for equal values, **1**
for drift and **2** for an error. Origin-only changes appear separately and do
not count as value drift. Changes are marked as requiring a restart. The
command never applies changes; there is no file watcher or SIGHUP reload.

## Precedence and paths

For server options, precedence is built-in default, TOML file, scoped environment
variable, explicit CLI option. Select the file with `--config FILE` or
`BICDB_CONFIG`; the explicit option wins. Inspection commands accept `--file`
and otherwise use `BICDB_CONFIG`, then `./bicdb.toml`. Ordinary server startup
does not automatically discover a file in the current directory.

Every server option shown by `bicdb serve --help` is available in `[server]`:
replace hyphens in its **long option name** with underscores. The positional
database directory is `path`. Control options such as `config`, `check_config`
and `print_effective_config` are not file settings. Unknown fields/sections,
unsupported versions, incorrect TOML types and files larger than 1 MiB fail.
The only root fields are `version`, `server` and optional `engine`.

Booleans are TOML booleans, counts are integers, other scalar options are
strings. Repeated options are arrays. Lists replace lower-precedence lists;
`[]` clears a list. Existing bare boolean CLI flags remain valid. Use
`--require-auth=false`, for example, for an explicit false override.

`BICDB_SERVER_PORT=5434` overrides `server.port`. Repeated environment options
use JSON arrays, for example `BICDB_SERVER_REDACT_FIELD='["ssn","tax_id"]'`.
Unknown `BICDB_SERVER_` and `BICDB_ENGINE_` variables fail to catch typos.
TOML path options are relative to the physical configuration file's parent
(after symlink resolution); environment and CLI paths are relative to the
working directory. Values are literal: no shell expansion, variable
interpolation or include directives occur. Referenced JSON files keep their
existing formats and semantics.

## Engine tuning

`[engine]` supports these process-local settings, with matching
`BICDB_ENGINE_<UPPERCASE_NAME>` overrides:

| Settings | Type |
| --- | --- |
| `paged_buffer_pool_bytes`, `paged_read_ahead_queue_pages`, `paged_extent_bytes` | Integer |
| `paged_value_compression`, `snapshot`, `sync_outbox`, `audit_events` | Boolean |
| `fts_query_partitions`, `fts_build_memory_bytes`, `fts_build_workers` | Integer |
| `fts_block_cache_bytes`, `fts_prefetch_blocks`, `fts_packed_segments` | Integer, integer, boolean |
| `query_work_memory_bytes`, `query_temp_space_bytes`, `query_server_temp_space_bytes`, `query_merge_fan_in` | Integer |

Omitted settings retain the engine's existing defaults, including documented
legacy engine environment defaults. File and scoped engine variables override
those defaults. `fts_query_partitions = 0` selects adaptive width;
`paged_extent_bytes = 0` selects a monolithic page file for newly created stores.
Existing stores retain their recorded layout. Server durability remains controlled by `server.storage_sync`;
replication authority, persistent formats and database-owned topology are not
arbitrary engine overrides. A server configuration file is not a database
backup or a cluster membership manifest.

## Secrets and remote inspection

Use existing `*_env` references, operator token files and TLS key files rather
than storing secret values in TOML. Restrict configuration/secret permissions
to the service account. Reports redact secret-valued arguments and never
resolve secret references into their contents. Paths and environment variable
names remain visible to administrators. Secret rotations with unchanged
references are **not compared** by configuration drift detection. Referenced
JSON/TLS/token file contents are not compared either; their configured paths
are compared. Validate and qualify those external inputs separately. Reports cover
startup server/engine options, not SQL session settings, dynamic HA roles or
all database metadata.

```sh
bicdb config diff --running --file bicdb.toml \
  --server db.example.org:5433 --user operator --dbname bicdb \
  --password-env BICDB_OPERATOR_PASSWORD --tls-ca /etc/bicdb/ca.pem
```

If the desired listener address/port has changed, pass `--server` with the
currently running endpoint.

Remote connections require verified TLS; the OS trust store is used when no CA
file is supplied. Plaintext is permitted only for loopback inspection when the
desired configuration does not require TLS. For an IP endpoint/SSH forward,
`--server-name db.example.org` verifies the certificate against that name.
There is no insecure certificate bypass.

See the [commented deployment example](examples/bicdb.toml),
[production guide](production-deployment.md) and
[PostgreSQL operations comparison](postgresql-operations-comparison.md).
