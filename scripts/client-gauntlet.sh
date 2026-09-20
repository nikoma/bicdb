#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/bicdb-client-gauntlet.XXXXXX")"
bicdb_port="${BICDB_GAUNTLET_PORT:-}"
bicdb_pid=""
client_report="${BICDB_CLIENT_GAUNTLET_REPORT:-$repo_root/target/postgresql-client-gauntlet.json}"
target_dir="${CARGO_TARGET_DIR:-$repo_root/target}"
build_jobs="${CARGO_BUILD_JOBS:-10}"

if [[ "$target_dir" != /* ]]; then
  target_dir="$repo_root/$target_dir"
fi
if [[ ! "$build_jobs" =~ ^[0-9]+$ ]] || (( build_jobs < 1 || build_jobs > 10 )); then
  printf '%s\n' 'CARGO_BUILD_JOBS must be an integer from 1 through 10' >&2
  exit 1
fi
export CARGO_BUILD_JOBS="$build_jobs"
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"

# Prove the complete registered scalar/array binary protocol surface before
# launching external clients. These tests are exhaustive over the canonical
# registry and make the driver gauntlet fail when a newly registered codec has
# no prepared/binary round-trip evidence.
(cd "$repo_root" && cargo test --locked -q -p bicdb-pgwire --test protocol \
  extended_query_binary_parameters_and_results_cover_structured_types -- \
  --exact --test-threads=1)
(cd "$repo_root" && cargo test --locked -q -p bicdb-pgwire --test protocol \
  extended_query_binary_array_matrix_covers_every_registered_scalar_codec -- \
  --exact --test-threads=1)

# Build before starting the long-lived server so later client checks never
# overlap a Cargo process with the server child held by `cargo run`.
(cd "$repo_root" && cargo build --locked -q -p bicdb-pgwire --example sqlx_type_formats)

cleanup() {
  if [[ -n "$bicdb_pid" ]]; then
    kill "$bicdb_pid" >/dev/null 2>&1 || true
    wait "$bicdb_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

choose_port() {
  python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local name="$3"
  for _ in $(seq 1 120); do
    if python3 - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket
import sys
with socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=0.25):
    pass
PY
    then
      return 0
    fi
    sleep 0.25
  done
  printf '%s\n' "timed out waiting for $name at $host:$port" >&2
  return 1
}

if [[ -z "${BICDB_DATABASE_URL:-}" ]]; then
  if [[ -z "$bicdb_port" ]]; then
    bicdb_port="$(choose_port)"
  fi
  bicdb_path="$work_dir/bicdb"
  (cd "$repo_root" && cargo build --locked -q -p bicdb-cli)
  (
    "$target_dir/debug/bicdb" serve "$bicdb_path" --host 127.0.0.1 --port "$bicdb_port"
  ) >"$work_dir/bicdb.log" 2>&1 &
  bicdb_pid="$!"
  wait_for_tcp 127.0.0.1 "$bicdb_port" BicDB
  BICDB_DATABASE_URL="postgres://bicdb@127.0.0.1:${bicdb_port}/bicdb"
fi

if [[ "${BICDB_GAUNTLET_START_PG18:-1}" == "1" ]]; then
  "$repo_root/scripts/pg18-up.sh"
fi

PG18_DATABASE_URL="${PG18_DATABASE_URL:-postgres://postgres:postgres@${PG18_HOST:-127.0.0.1}:${PG18_HOST_PORT:-55432}/${PG18_DB:-compat}}"

cat >"$work_dir/python_gauntlet.py" <<'PY'
import sys
from urllib.parse import urlparse

client_name, url = sys.argv[1], sys.argv[2]
table = f"gauntlet_{client_name.replace('-', '_')}"

def expect(value, expected, label):
    if value != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {value!r}")

if client_name == "psycopg":
    import psycopg
    from decimal import Decimal
    with psycopg.connect(url) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute(f"CREATE TABLE {table} (id TEXT PRIMARY KEY, label TEXT NOT NULL, seen INT NOT NULL)")
            cur.execute(f"INSERT INTO {table} (id, label, seen) VALUES (%s, %s, %s)", ("p1", "Ada", 1))
            cur.execute(f"UPDATE {table} SET seen = %s WHERE id = %s", (2, "p1"))
            cur.execute(f"SELECT label, seen FROM {table} WHERE id = %s", ("p1",))
            expect(cur.fetchone(), ("Ada", 2), "psycopg CRUD")
            conn.commit()
            cur.execute("BEGIN")
            cur.execute(f"INSERT INTO {table} (id, label, seen) VALUES (%s, %s, %s)", ("rolled", "Rollback", 9))
            cur.execute("ROLLBACK")
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE id = %s", ("rolled",))
            expect(cur.fetchone()[0], 0, "psycopg rollback")
            cur.execute(f"SELECT label FROM {table} WHERE id = %s", ("p1",))
            expect(cur.fetchone()[0], "Ada", "psycopg prepared select")
            typed_table = f"{table}_types"
            cur.execute(f"DROP TABLE IF EXISTS {typed_table}")
            cur.execute(
                f"CREATE TABLE {typed_table} ("
                "id TEXT PRIMARY KEY, "
                "amount NUMERIC, "
                "day DATE, "
                "at_time TIME, "
                "span INTERVAL, "
                "starts_at TIMESTAMP WITH TIME ZONE)"
            )
            cur.execute(
                f"INSERT INTO {typed_table} (id, amount, day, at_time, span, starts_at) "
                "VALUES ('typed-1', '12.30'::numeric, '2024-01-02'::date, "
                "'02:03:04'::time, '1 day'::interval, '2024-01-02 03:04:05+00'::timestamptz)"
            )
            cur.execute(
                f"SELECT amount::text, day::text, at_time::text, span::text, starts_at::text "
                f"FROM {typed_table} WHERE id = %s",
                ("typed-1",),
            )
            expect(
                cur.fetchone(),
                ("12.30", "2024-01-02", "02:03:04", "1 day", "2024-01-02 03:04:05+00"),
                "psycopg non-trivial type migration",
            )
        typed_query = (
            "SELECT TRUE::bool, (-12345)::int2, 123456789::int4, "
            "9007199254740993::int8, 1.5::float4, (-2.25)::float8, "
            "12345678901234567890.125::numeric, 'client-text'::text, "
            "'\\x00ff10'::bytea"
        )
        expected = (
            True,
            -12345,
            123456789,
            9007199254740993,
            1.5,
            -2.25,
            Decimal("12345678901234567890.125"),
            "client-text",
            b"\x00\xff\x10",
        )
        with conn.cursor(binary=False) as text_cur:
            text_cur.execute(typed_query)
            expect(text_cur.fetchone(), expected, "psycopg text result formats")
            expect(
                {column.type_code for column in text_cur.description},
                {16, 21, 23, 20, 700, 701, 1700, 25, 17},
                "psycopg text result OIDs",
            )
        with conn.cursor(binary=True) as binary_cur:
            binary_cur.execute(typed_query)
            expect(binary_cur.fetchone(), expected, "psycopg binary result formats")
elif client_name == "sqlalchemy":
    import sqlalchemy as sa
    from sqlalchemy.orm import Session
    sa_url = url.replace("postgres://", "postgresql+psycopg://", 1)
    engine = sa.create_engine(sa_url, future=True, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {table}")
        conn.exec_driver_sql(f"CREATE TABLE {table} (id TEXT PRIMARY KEY, label TEXT NOT NULL, seen INT NOT NULL)")
    with Session(engine) as session:
        conn = session.connection()
        conn.exec_driver_sql(f"INSERT INTO {table} (id, label, seen) VALUES (%s, %s, %s)", ("s1", "Grace", 1))
        conn.exec_driver_sql(f"UPDATE {table} SET seen = %s WHERE id = %s", (3, "s1"))
        row = conn.exec_driver_sql(f"SELECT label, seen FROM {table} WHERE id = %s", ("s1",)).one()
        expect(tuple(row), ("Grace", 3), "SQLAlchemy CRUD/session")
        session.commit()
    with engine.connect() as conn:
        conn.exec_driver_sql("BEGIN")
        conn.exec_driver_sql(f"INSERT INTO {table} (id, label, seen) VALUES (%s, %s, %s)", ("rolled", "Rollback", 9))
        conn.exec_driver_sql("ROLLBACK")
        count = conn.exec_driver_sql(f"SELECT COUNT(*) FROM {table} WHERE id = %s", ("rolled",)).scalar_one()
        expect(count, 0, "SQLAlchemy rollback")
else:
    raise SystemExit(f"unknown Python client {client_name}")

print(f"{client_name} gauntlet passed for {urlparse(url).hostname}:{urlparse(url).port}")
PY

cat >"$work_dir/node_gauntlet.js" <<'JS'
const pg = require("pg");

const typedQuery = `
  SELECT TRUE::bool AS flag, (-12345)::int2 AS i2, 123456789::int4 AS i4,
         9007199254740993::int8 AS i8, 1.5::float4 AS f4, (-2.25)::float8 AS f8,
         12345678901234567890.125::numeric AS exact_value,
         'client-text'::text AS label, '\\x00ff10'::bytea AS bytes
`;

function assertTypedOids(result, mode) {
  const row = result.rows[0];
  const expectedOids = [16, 21, 23, 20, 700, 701, 1700, 25, 17];
  const actualOids = result.fields.map((field) => field.dataTypeID);
  if (JSON.stringify(actualOids) !== JSON.stringify(expectedOids)) {
    throw new Error(`node-postgres ${mode} OIDs mismatch: ${JSON.stringify(actualOids)}`);
  }
  return row;
}

function assertTypedTextRow(result) {
  const row = assertTypedOids(result, "text");
  if (row.flag !== true || row.i2 !== -12345 || row.i4 !== 123456789 ||
      row.i8 !== "9007199254740993" || row.f4 !== 1.5 || row.f8 !== -2.25 ||
      row.exact_value !== "12345678901234567890.125" || row.label !== "client-text" ||
      !Buffer.isBuffer(row.bytes) || row.bytes.toString("hex") !== "00ff10") {
    throw new Error(`node-postgres text typed row mismatch: ${JSON.stringify(row)}`);
  }
}

async function main() {
  const url = process.argv[2];
  const target = process.argv[3];
  const table = `gauntlet_node_${target}`;
  const client = new pg.Client({ connectionString: url });
  await client.connect();
  try {
    await client.query(`DROP TABLE IF EXISTS ${table}`);
    await client.query(`CREATE TABLE ${table} (id TEXT PRIMARY KEY, label TEXT NOT NULL, seen INT NOT NULL)`);
    await client.query({
      name: `${table}_insert`,
      text: `INSERT INTO ${table} (id, label, seen) VALUES ($1, $2, $3)`,
      values: ["n1", "Lin", 1],
    });
    await client.query(`UPDATE ${table} SET seen = $1 WHERE id = $2`, [4, "n1"]);
    const crud = await client.query({
      name: `${table}_select`,
      text: `SELECT label, seen FROM ${table} WHERE id = $1`,
      values: ["n1"],
    });
    if (crud.rows[0].label !== "Lin" || Number(crud.rows[0].seen) !== 4) {
      throw new Error(`node-postgres CRUD mismatch: ${JSON.stringify(crud.rows)}`);
    }
    await client.query("BEGIN");
    await client.query(`INSERT INTO ${table} (id, label, seen) VALUES ($1, $2, $3)`, ["rolled", "Rollback", 9]);
    await client.query("ROLLBACK");
    const rolled = await client.query(`SELECT COUNT(*) AS count FROM ${table} WHERE id = $1`, ["rolled"]);
    if (Number(rolled.rows[0].count) !== 0) {
      throw new Error("node-postgres rollback did not hide row");
    }
    const textResult = await client.query({ text: typedQuery, queryMode: "simple" });
    assertTypedTextRow(textResult);
    const binaryResult = await client.query({
      text: typedQuery,
      values: [],
      binary: true,
      queryMode: "extended",
    });
    const binaryRow = assertTypedOids(binaryResult, "binary");
    // pg@8.16.3 misdecodes several binary scalar codecs against PostgreSQL
    // itself. Assert the codecs it handles exactly and retain the complete
    // binary matrix in the raw protocol tests.
    if (binaryRow.flag !== true || binaryRow.i8 !== "9007199254740993" ||
        binaryRow.label !== "client-text") {
      throw new Error(`node-postgres binary row mismatch: ${JSON.stringify(binaryRow)}`);
    }
    console.log(`node-postgres gauntlet passed for ${target}`);
  } finally {
    await client.end();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
JS

cat >"$work_dir/package.json" <<'JSON'
{
  "private": true,
  "dependencies": {
    "pg": "8.16.3"
  },
  "devDependencies": {}
}
JSON

if python3 -m venv "$work_dir/venv" >/dev/null 2>&1; then
  python_exe="$work_dir/venv/bin/python"
  "$python_exe" -m pip install -q --upgrade pip
  "$python_exe" -m pip install -q "psycopg[binary]==3.2.9" "SQLAlchemy==2.0.41"
  run_python() {
    "$python_exe" "$@"
  }
else
  pydeps="$work_dir/python-deps"
  mkdir -p "$pydeps"
  python3 -m pip install -q --target "$pydeps" "psycopg[binary]==3.2.9" "SQLAlchemy==2.0.41"
  run_python() {
    PYTHONPATH="$pydeps${PYTHONPATH:+:$PYTHONPATH}" python3 "$@"
  }
fi

(cd "$work_dir" && npm install --silent)

for command in psql curl sha256sum gcc javac java; do
  if ! command -v "$command" >/dev/null 2>&1; then
    printf 'required client-gauntlet command is missing: %s\n' "$command" >&2
    exit 1
  fi
done

jdbc_version="42.7.7"
jdbc_jar="$work_dir/postgresql-${jdbc_version}.jar"
curl -fsSL \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/${jdbc_version}/postgresql-${jdbc_version}.jar" \
  -o "$jdbc_jar"
printf '%s  %s\n' \
  "157963d60ae66d607e09466e8c0cdf8087e9cb20d0159899ffca96bca2528460" \
  "$jdbc_jar" | sha256sum --check --status
javac -d "$work_dir" "$repo_root/tests/client-gauntlet/TypeFormats.java"

libpq_library="${BICDB_LIBPQ_LIBRARY:-}"
if [[ -z "$libpq_library" ]]; then
  libpq_library="$(ldconfig -p 2>/dev/null | awk '/libpq\.so(\.|$)/ { print $NF; exit }')"
fi
if [[ -z "$libpq_library" || ! -f "$libpq_library" ]]; then
  printf '%s\n' "libpq shared library not found; set BICDB_LIBPQ_LIBRARY" >&2
  exit 1
fi
gcc -std=c11 -Wall -Wextra -Werror \
  "$repo_root/tests/client-gauntlet/libpq_type_formats.c" \
  "$libpq_library" -o "$work_dir/libpq_type_formats"

for target in bicdb pg18; do
  if [[ "$target" == "bicdb" ]]; then
    url="$BICDB_DATABASE_URL"
  else
    url="$PG18_DATABASE_URL"
  fi
  psql "$url" -v ON_ERROR_STOP=1 -Atc \
    "SELECT TRUE::text, (-12345)::int2::text, 123456789::int4::text, 9007199254740993::int8::text, 12345678901234567890.125::numeric::text, '\\x00ff10'::bytea::text;" \
    | grep -Fx 'true|-12345|123456789|9007199254740993|12345678901234567890.125|\x00ff10' >/dev/null
  printf '%s\n' "psql text gauntlet passed for $target"
  run_python "$work_dir/python_gauntlet.py" psycopg "$url"
  run_python "$work_dir/python_gauntlet.py" sqlalchemy "$url"
  (cd "$work_dir" && node node_gauntlet.js "$url" "$target")
  "$work_dir/libpq_type_formats" "$url" "$target"
  java -cp "$work_dir:$jdbc_jar" TypeFormats "$url" "$target"
  "$target_dir/debug/examples/sqlx_type_formats" "$url" "$target"
done

mkdir -p "$(dirname "$client_report")"
node - "$repo_root/fixtures/postgresql-18/client-matrix.json" "$client_report" <<'NODE'
const fs = require("node:fs");
const [matrixPath, outputPath] = process.argv.slice(2);
const matrix = JSON.parse(fs.readFileSync(matrixPath, "utf8"));
const clients = matrix.automated
  .filter(({ scope }) => scope === "cross_target")
  .map(({ id, name, version }) => ({ id, name, version, status: "passed" }));
const report = {
  schema_version: 1,
  target_version: matrix.target_version,
  status: "passed",
  targets: ["bicdb", "postgresql_18_4"],
  protocol_coverage: {
    prepared_binary_registered_scalars: "passed",
    prepared_binary_registered_arrays: "passed",
    evidence_tests: [
      "extended_query_binary_parameters_and_results_cover_structured_types",
      "extended_query_binary_array_matrix_covers_every_registered_scalar_codec",
    ],
  },
  clients,
};
fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`);
NODE
printf 'Client gauntlet report: %s\n' "$client_report"
printf '%s\n' "Prisma introspection is documented separately: run scripts/prisma-introspection-smoke.sh"
