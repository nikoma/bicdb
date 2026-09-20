#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

./scripts/check-open-core-boundary.sh

# These are focused admission tests for the capabilities that must remain in a
# standalone community checkout. The complete workspace suite is still run by
# the ordinary CI matrix.
cargo test -p bicdb-core --locked --test encryption
cargo test -p bicdb-core --locked --test backup --test backup_paged
cargo test -p bicdb-core --locked --test core \
  native_replication_stream_replays_and_resumes_to_standby
cargo test -p bicdb-core --locked --test core \
  operational_metrics_cover_storage_backup_compaction_and_replication
cargo test -p bicdb-pgwire --locked --lib \
  pgwire_host_services_are_explicit_and_start_once
cargo test -p bicdb-sql --locked --test authorization_canary
cargo test -p bicdb-sql --locked --test streaming_rls_guard
cargo test -p bicdb-cell --locked --lib
cargo test -p bicdb-cell-admission --locked --lib
cargo test -p bicdb-cell-grant --locked --lib
cargo test -p bicdb-cell-ha --locked --lib \
  failover_routes_only_after_fence_durable_selection_and_activation
cargo test -p bicdb-sync --locked

