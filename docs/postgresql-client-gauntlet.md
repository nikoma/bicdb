# PostgreSQL Client Gauntlet

BicDB keeps a practical client compatibility gauntlet for common PostgreSQL
drivers, ORMs, and GUI tools. The cross-target script runs seven declared
clients against BicDB and the shared PostgreSQL 18.4 comparison target.
tokio-postgres is a separate in-process BicDB regression. The generated
[client matrix](postgresql-client-matrix.md) is authoritative for versions,
formats, and whether coverage is cross-target or BicDB-only.

## Automated Clients

Start from the repository root:

```bash
./scripts/client-gauntlet.sh
```

The script builds required Rust binaries before starting a temporary local
BicDB pgwire server, starts the shared
PostgreSQL 18.4 Docker harness through `scripts/pg18-up.sh`, installs pinned
temporary client dependencies, and runs the same type assertions against both
servers. Missing client prerequisites fail the run instead of silently reducing
coverage.

Before starting either server, the gauntlet also runs the registry-exhaustive
prepared/binary scalar and array protocol matrices. This couples every declared
client pass to byte-level coverage for every registered binary codec, including
structured types and arrays. Driver-native assertions remain limited to codecs
the driver itself supports; psql exposes text results only, and node-postgres
has binary decoding limitations that reproduce against stock PostgreSQL.

| Client | Version pin | Coverage |
| --- | --- | --- |
| psql | local stock executable | text-query scalar/OID values; psql does not expose a binary row-result switch |
| node-postgres | `pg@8.16.3` | simple-query text plus extended-query binary mode, DDL, CRUD, rollback, and named prepared statements |
| psycopg | `psycopg[binary]==3.2.9` | explicit text and binary cursors, DDL, CRUD, rollback, prepared execution, and non-trivial typed migrations |
| SQLAlchemy | `SQLAlchemy==2.0.41` with psycopg | engine connect, session flow, DDL, insert, update, select, rollback, bound parameters |
| SQLx | `sqlx=0.8.6` | textual canonical values and typed binary decoding through the PostgreSQL extended protocol |
| tokio-postgres | `tokio-postgres=0.7.18` | separate in-process BicDB regression for simple-query text plus extended-query binary parameters/results |
| PostgreSQL JDBC | `postgresql-42.7.7.jar` by checked SHA-256 | `binaryTransfer=false` and forced binary transfer with typed getters |
| libpq | installed stock shared library | `PQexecParams` with result format `0` and `1`, including raw network-byte assertions |

Useful environment overrides:

```bash
BICDB_DATABASE_URL=postgres://bicdb@127.0.0.1:5433/bicdb ./scripts/client-gauntlet.sh
BICDB_GAUNTLET_START_PG18=0 PG18_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/compat ./scripts/client-gauntlet.sh
```

The gauntlet pins the downloaded JDBC artifact by SHA-256 and writes a compact
pass report to `target/postgresql-client-gauntlet.json`. It also requires
`psql`, `curl`, `sha256sum`, `gcc`, `javac`, and `java`; the libpq shared-library
path can be supplied with `BICDB_LIBPQ_LIBRARY` when it is not discoverable by
`ldconfig`.

node-postgres 8.16.3 misdecodes several binary scalar values in the same way
against stock PostgreSQL 18, including exact numeric. The Node matrix therefore
asserts every scalar and exact decimal in text mode, and only the binary codecs
that the stock driver decodes correctly. BicDB's raw pgwire matrix separately
asserts every supported binary codec byte-for-byte.

## Rust Clients

`tokio-postgres` and SQLx are also covered by the Rust pgwire test suite:

```bash
cargo test -p bicdb-pgwire tokio_postgres_basic_orm_client_gauntlet -- --nocapture
cargo test -p bicdb-pgwire sqlx_text_and_binary_type_client_gauntlet -- --nocapture
```

Those tests start an in-process BicDB pgwire listener and cover text and binary
type decoding. The tokio-postgres case also covers DDL, CRUD, transaction
rollback, and prepared statements.

## Large Result Fetches

BicDB supports pgwire portal suspension for extended-query `Execute` calls with
a positive row count. Drivers that implement client-side cursors or named
portals can fetch large reports in batches that exceed `max_result_rows`.
`COPY TO STDOUT` is the preferred path for full CSV/text exports.

See [Large Result Streaming](large-results.md) for recommended large-result client
patterns and the `bicdb_server_stats` streaming metrics.

## Prisma Introspection

Prisma introspection is documented as a reproducible manual script because
`prisma db pull` currently reaches BicDB but fails on a parameterized
introspection statement before it can produce a schema.

```bash
DATABASE_URL=postgres://bicdb@127.0.0.1:5433/bicdb ./scripts/prisma-introspection-smoke.sh
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/compat ./scripts/prisma-introspection-smoke.sh
```

The script installs `prisma@6.10.1` into a temporary npm project and runs
`prisma db pull` with a generated schema file. As of the 2026-06-20 smoke run,
PostgreSQL 18.4 is the expected passing comparison target; BicDB exposes the
remaining Prisma catalog/prepared-statement gap with:

```text
Incorrect number of parameters given to a statement. Expected 0: got: 1.
```

## GUI Smoke Scripts

Use [scripts/manual-gui-client-smoke.sql](../scripts/manual-gui-client-smoke.sql)
in DBeaver, DataGrip, and TablePlus.

Connection defaults for a local BicDB server:

| Field | Value |
| --- | --- |
| Host | `127.0.0.1` |
| Port | `5433` |
| Database | `bicdb` |
| User | `bicdb` |
| Password | empty unless `--require-auth` is enabled |
| SSL | disabled unless the server was started with `--tls-cert` and `--tls-key` |

Manual flow for each GUI client:

1. Start BicDB: `cargo run -p bicdb-cli -- serve ./tmp/gui-smoke --host 127.0.0.1 --port 5433`.
2. Create a PostgreSQL connection with the fields above.
3. Confirm the schema/table browser opens without connection errors.
4. Run `scripts/manual-gui-client-smoke.sql`.
5. Confirm the first result set returns `created from GUI client, 2`, the rollback count is `0`, and catalog queries return `gui_client_smoke` metadata.

For PostgreSQL 18.4 comparison, start `./scripts/pg18-up.sh`, connect the GUI to
`127.0.0.1:55432` with database/user/password
`compat` / `postgres` / `postgres`, and run the same SQL file.
