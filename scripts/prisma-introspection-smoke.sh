#!/usr/bin/env bash
set -euo pipefail

database_url="${DATABASE_URL:-postgres://bicdb@127.0.0.1:5433/bicdb}"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/bicdb-prisma-introspection.XXXXXX")"

cleanup() {
  rm -rf "$work_dir"
}
trap cleanup EXIT

cat >"$work_dir/package.json" <<'JSON'
{
  "private": true,
  "dependencies": {
    "prisma": "6.10.1"
  },
  "devDependencies": {}
}
JSON

cat >"$work_dir/schema.prisma" <<'PRISMA'
datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

generator client {
  provider = "prisma-client-js"
}
PRISMA

(
  cd "$work_dir"
  npm install --silent
  DATABASE_URL="$database_url" ./node_modules/.bin/prisma db pull --schema schema.prisma
)

# Some engine/CLI failure paths exit successfully without changing the input
# schema. A passing introspection smoke must actually produce model definitions.
if ! rg -q '^model[[:space:]]+[^[:space:]]+[[:space:]]*\{' "$work_dir/schema.prisma"; then
  echo "Prisma exited without generating any model definitions" >&2
  exit 1
fi
rg '^model[[:space:]]' "$work_dir/schema.prisma"
