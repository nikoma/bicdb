#!/usr/bin/env bash
# Differential RLS parity check: run scripts/rls_parity_battery.sql against
# both a BicDB pgwire server and the postgres:18.4 oracle (bicdb-pg18-compat),
# normalize the output, and diff.
#
# Usage:
#   BICDB_HOST=127.0.0.1 BICDB_PORT=5439 BICDB_USER=admin BICDB_PASSWORD=adminpw \
#   PG_CONTAINER=bicdb-pg18-compat scripts/rls-pg18-diff.sh
#
# The BicDB connection user must map to a SUPERUSER SQL role so SET ROLE works
# like the oracle's postgres superuser; the script creates it if missing.
set -u

BICDB_HOST="${BICDB_HOST:-127.0.0.1}"
BICDB_PORT="${BICDB_PORT:-5439}"
BICDB_USER="${BICDB_USER:-admin}"
BICDB_PASSWORD="${BICDB_PASSWORD:-adminpw}"
PG_CONTAINER="${PG_CONTAINER:-bicdb-pg18-compat}"
BATTERY="$(dirname "$0")/rls_parity_battery.sql"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Give the BicDB session-user a superuser SQL role, mirroring the oracle's
# superuser connection.
PGPASSWORD="$BICDB_PASSWORD" psql -h "$BICDB_HOST" -p "$BICDB_PORT" -U "$BICDB_USER" -d bicdb \
    -c "CREATE ROLE ${BICDB_USER} SUPERUSER LOGIN" >/dev/null 2>&1 || true

normalize() {
    # Strip BicDB's error prefix and PG's DETAIL/HINT/LINE noise so only the
    # primary behavior is compared.
    sed -E \
        -e 's/^psql:[^:]+:[0-9]+: //' \
        -e 's/^ERROR:  authorization denied: /ERROR:  /' \
        -e 's/^ERROR:  unsupported SQL: /ERROR:  /' \
        -e 's/^ERROR:  invalid SQL: /ERROR:  /' \
        -e '/^(DETAIL|HINT|LINE|NOTICE):/d' \
        -e '/^ *\^ *$/d' \
        -e 's/ +$//'
}

PGPASSWORD="$BICDB_PASSWORD" psql -h "$BICDB_HOST" -p "$BICDB_PORT" -U "$BICDB_USER" -d bicdb \
    -v ON_ERROR_STOP=0 -P pager=off -f "$BATTERY" 2>&1 | normalize > "$WORK/bicdb.out"

docker exec -i "$PG_CONTAINER" psql -U postgres -d compat \
    -v ON_ERROR_STOP=0 -P pager=off < "$BATTERY" 2>&1 | normalize > "$WORK/pg18.out"

if diff -u "$WORK/pg18.out" "$WORK/bicdb.out"; then
    echo "RLS parity battery: no differences against PostgreSQL 18"
else
    echo
    echo "RLS parity battery: differences found (left = postgres:18.4, right = bicdb)"
    exit 1
fi
