#!/usr/bin/env sh
set -eu

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
compose_file="${BICDB_PG18_COMPOSE_FILE:-$repo_root/compose.pg18.yml}"
project_name="${PG18_COMPOSE_PROJECT:-bicdb-pg18-compat}"

if docker compose version >/dev/null 2>&1; then
  docker compose -p "$project_name" -f "$compose_file" down "$@"
elif command -v docker-compose >/dev/null 2>&1; then
  docker-compose -p "$project_name" -f "$compose_file" down "$@"
else
  printf '%s\n' "docker compose or docker-compose is required" >&2
  exit 1
fi
