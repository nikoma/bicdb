#!/usr/bin/env sh
set -eu

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
source_file="$repo_root/fixtures/postgresql-18/type-capabilities.json"
roadmap="$repo_root/docs/postgresql-18-data-type-parity-todo.md"
output_dir="$repo_root/fixtures/postgres-compat"

command -v jq >/dev/null 2>&1 || {
  printf '%s\n' 'jq is required to generate PostgreSQL type capability fixtures' >&2
  exit 1
}

rm -f "$output_dir/026-expected-difference-unsupported-types.json"
rm -f "$output_dir/027-expected-difference-enum-types.json"
rm -f "$output_dir/028-expected-difference-domain-types.json"

jq -c '.capabilities[]' "$source_file" | while IFS= read -r capability; do
  output="$(printf '%s' "$capability" | jq -r '.output')"
  roadmap_items="$(printf '%s' "$capability" | jq -r '.roadmap_items[]')"
  status_json='{}'
  for item in $roadmap_items; do
    line="$(sed -n "/^- \[[ x]\] ${item} /p" "$roadmap")"
    if [ -z "$line" ]; then
      printf 'unknown roadmap item %s in %s\n' "$item" "$source_file" >&2
      exit 1
    fi
    case "$line" in
      '- [x]'*) state=complete ;;
      *) state=pending ;;
    esac
    status_json="$(printf '%s' "$status_json" | jq --arg item "$item" --arg state "$state" '. + {($item): $state}')"
  done
  printf '%s' "$capability" | jq --argjson statuses "$status_json" \
    '.fixture + {roadmap_items: .roadmap_items, roadmap_status: $statuses}' \
    > "$output_dir/$output"
done

printf 'Generated PostgreSQL type capability fixtures in %s\n' "$output_dir"
