#!/usr/bin/env sh
set -eu

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
oracle_image="${PG18_ORACLE_IMAGE:-postgres:18.4}"
output="${PG18_TYPE_INVENTORY_OUTPUT:-$repo_root/fixtures/postgresql-18/type-inventory.json}"
query="$repo_root/scripts/sql/pg18-type-inventory.sql"
container="bicdb-pg18-type-inventory-$$"
temporary="$output.tmp"

cleanup() {
  rm -f "$temporary"
  docker rm -f "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

if ! command -v docker >/dev/null 2>&1; then
  printf '%s\n' 'docker is required to generate the PostgreSQL 18 type inventory' >&2
  exit 1
fi

mkdir -p "$(dirname -- "$output")"

docker run --detach --rm \
  --name "$container" \
  --env POSTGRES_HOST_AUTH_METHOD=trust \
  "$oracle_image" >/dev/null

attempt=0
until docker exec "$container" pg_isready -U postgres -d postgres >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    printf '%s\n' 'PostgreSQL 18 oracle did not become ready' >&2
    exit 1
  fi
  sleep 1
done

docker exec -i "$container" \
  psql -X -A -t -q -v ON_ERROR_STOP=1 -v "oracle_image=$oracle_image" \
  -U postgres -d postgres <"$query" >"$temporary"

# psql terminates its single JSON value with a newline. Validate the artifact
# before replacing the checked-in fixture.
node -e 'JSON.parse(require("fs").readFileSync(process.argv[1], "utf8"))' "$temporary"
mv "$temporary" "$output"
printf 'Wrote %s from %s\n' "$output" "$oracle_image"
