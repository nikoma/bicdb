#!/usr/bin/env sh
set -eu

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
compose_file="${BICDB_PG18_COMPOSE_FILE:-$repo_root/compose.pg18.yml}"
project_name="${PG18_COMPOSE_PROJECT:-bicdb-pg18-compat}"
service_name="${PG18_SERVICE:-pg18}"
container_name="${PG18_CONTAINER_NAME:-bicdb-pg18-compat}"

docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose -p "$project_name" -f "$compose_file" "$@"
  elif command -v docker-compose >/dev/null 2>&1; then
    docker-compose -p "$project_name" -f "$compose_file" "$@"
  else
    printf '%s\n' "docker compose or docker-compose is required" >&2
    exit 1
  fi
}

if docker container inspect "$container_name" >/dev/null 2>&1; then
  docker start "$container_name" >/dev/null
else
  docker_compose up -d "$service_name"
fi

attempts="${PG18_READY_ATTEMPTS:-60}"
sleep_seconds="${PG18_READY_SLEEP_SECONDS:-1}"
i=0

while [ "$i" -lt "$attempts" ]; do
  if docker exec "$container_name" pg_isready \
    -h 127.0.0.1 \
    -p 5432 \
    -U "${PG18_USER:-postgres}" \
    -d "${PG18_DB:-compat}" >/dev/null 2>&1; then
    printf 'PostgreSQL 18.4 harness is ready at %s:%s/%s\n' \
      "${PG18_BIND_HOST:-127.0.0.1}" \
      "${PG18_HOST_PORT:-55432}" \
      "${PG18_DB:-compat}"
    exit 0
  fi

  i=$((i + 1))
  sleep "$sleep_seconds"
done

printf '%s\n' "PostgreSQL 18.4 harness did not become ready" >&2
docker ps --filter "name=^/${container_name}$" >&2 || true
exit 1
