#!/usr/bin/env sh
set -eu

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
compose_file="${BICDB_PG18_COMPOSE_FILE:-$repo_root/compose.pg18.yml}"
project_name="${PG18_COMPOSE_PROJECT:-bicdb-pg18-compat}"
service_name="${PG18_SERVICE:-pg18}"
container_name="${PG18_CONTAINER_NAME:-bicdb-pg18-compat}"

if docker container inspect "$container_name" >/dev/null 2>&1; then
  exec docker exec -i \
    -e "PGPASSWORD=${PG18_PASSWORD:-postgres}" \
    "$container_name" \
    psql \
    -h 127.0.0.1 \
    -p 5432 \
    -U "${PG18_USER:-postgres}" \
    -d "${PG18_DB:-compat}" \
    "$@"
elif docker compose version >/dev/null 2>&1; then
  exec docker compose -p "$project_name" -f "$compose_file" exec -T \
    -e "PGPASSWORD=${PG18_PASSWORD:-postgres}" \
    "$service_name" \
    psql \
    -h 127.0.0.1 \
    -p 5432 \
    -U "${PG18_USER:-postgres}" \
    -d "${PG18_DB:-compat}" \
    "$@"
elif command -v docker-compose >/dev/null 2>&1; then
  exec docker-compose -p "$project_name" -f "$compose_file" exec -T \
    -e "PGPASSWORD=${PG18_PASSWORD:-postgres}" \
    "$service_name" \
    psql \
    -h 127.0.0.1 \
    -p 5432 \
    -U "${PG18_USER:-postgres}" \
    -d "${PG18_DB:-compat}" \
    "$@"
else
  printf '%s\n' "docker compose or docker-compose is required" >&2
  exit 1
fi
