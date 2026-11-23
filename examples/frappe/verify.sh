#!/usr/bin/env bash
# Qualify the Frappe Framework (PostgreSQL mode) against a BicDB server:
# bench init, bench new-site --db-type postgres, an HTTP smoke test
# (login, ToDo CRUD, a custom DocType created through the API) and a slice
# of Frappe's own test suite.
#
# Requires: BICDB_BIN (a bicdb binary), python3.14 with `pip install
# frappe-bench`, node 24 + yarn, psql, the MariaDB client headers (Frappe
# builds mysqlclient even for PostgreSQL sites) and a Redis server for
# Frappe's cache/queue (REDIS_URL).
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
: "${BICDB_BIN:?set BICDB_BIN to the bicdb binary}"
BICDB_BIN=$(realpath "$BICDB_BIN")
WORK=${WORK_DIR:-$(mktemp -d)}
mkdir -p "$WORK"
WORK=$(realpath "$WORK")
FRAPPE_REF=${FRAPPE_REF:-v16.60.2}
PYTHON=${FRAPPE_PYTHON:-python3.14}
PG_PORT=${BICDB_PORT:-55432}
WEB_PORT=${FRAPPE_WEB_PORT:-58000}
REDIS_URL=${REDIS_URL:-redis://127.0.0.1:6379}
SITE=${FRAPPE_SITE:-bicdb.test}
ROOT_PASSWORD=${BICDB_ROOT_PASSWORD:-bicdb}
# Frappe test modules to run. Every failure must be listed in
# known-failures.txt (see docs/frappe-compatibility.md, "Known gaps").
TESTS=${FRAPPE_TESTS:-"frappe.tests.test_db frappe.tests.test_document frappe.tests.test_query_builder frappe.tests.test_model_utils frappe.tests.test_permissions frappe.tests.test_db_query frappe.tests.test_naming frappe.tests.test_child_table frappe.tests.test_defaults frappe.tests.test_sequence frappe.tests.test_docstatus"}

log() { printf '[frappe-verify %s] %s\n' "$(date +%T)" "$*"; }
PIDS=()
cleanup() {
	for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
	wait 2>/dev/null || true
}
trap cleanup EXIT

log "starting BicDB on 127.0.0.1:$PG_PORT (data in $WORK/cluster)"
# Password authentication, as Frappe expects: new-site creates the site's
# role WITH PASSWORD and the site then logs in with it.
export PGPASSWORD=$ROOT_PASSWORD
"$BICDB_BIN" user create bicdb --password "$ROOT_PASSWORD" --path "$WORK/cluster" --role superuser >/dev/null
"$BICDB_BIN" serve "$WORK/cluster" --cluster --default-database bicdb \
	--host 127.0.0.1 --port "$PG_PORT" --max-connections-per-ip 200 --max-connections 250 \
	--require-auth --auth-method scram-sha-256 \
	--slow-query-log "$WORK/slow-queries.log" --slow-query-threshold-ms 250 \
	--storage-sync buffered >"$WORK/bicdb.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 300); do
	psql -X -h 127.0.0.1 -p "$PG_PORT" -U bicdb -d bicdb -Atc "select 1" >/dev/null 2>&1 && break
	sleep 0.2
done
psql -X -h 127.0.0.1 -p "$PG_PORT" -U bicdb -d bicdb -Atc "select version()"

if [ ! -d "$WORK/bench/apps/frappe" ]; then
	log "bench init (Frappe $FRAPPE_REF)"
	(cd "$WORK" && bench init bench --frappe-branch "$FRAPPE_REF" --python "$PYTHON" \
		--skip-redis-config-generation --skip-assets --no-backups --no-procfile) >"$WORK/bench-init.log" 2>&1 \
		|| { tail -50 "$WORK/bench-init.log"; exit 1; }
	# Test-only dependencies (freezegun, unittest-xml-reporting, ...).
	(cd "$WORK/bench" && bench setup requirements --dev) >>"$WORK/bench-init.log" 2>&1 \
		|| { tail -50 "$WORK/bench-init.log"; exit 1; }
fi
cd "$WORK/bench"
bench set-config -g redis_cache "$REDIS_URL/0" >/dev/null
bench set-config -g redis_queue "$REDIS_URL/1" >/dev/null
bench set-config -g redis_socketio "$REDIS_URL/2" >/dev/null

log "bench new-site $SITE --db-type postgres"
rm -rf "sites/$SITE"
started=$SECONDS
bench new-site "$SITE" --db-type postgres --db-host 127.0.0.1 --db-port "$PG_PORT" \
	--db-root-username bicdb --db-root-password "$ROOT_PASSWORD" --admin-password admin >"$WORK/new-site.log" 2>&1 \
	|| { tail -80 "$WORK/new-site.log"; exit 1; }
log "new-site finished in $((SECONDS - started))s"

log "HTTP smoke test"
bench --site "$SITE" serve --port "$WEB_PORT" --noreload --nothreading >"$WORK/web.log" 2>&1 &
WEB_PID=$!
PIDS+=($WEB_PID)
for _ in $(seq 1 120); do
	curl -fs -o /dev/null "http://127.0.0.1:$WEB_PORT/api/method/ping" && break
	sleep 0.5
done
"$PYTHON" "$HERE/smoke.py" "http://127.0.0.1:$WEB_PORT" admin | tee "$WORK/smoke.log"
kill "$WEB_PID" 2>/dev/null || true

log "Frappe test slice"
bench --site "$SITE" set-config allow_tests true >/dev/null
mkdir -p "$WORK/junit"
for module in $TESTS; do
	name=${module##*.}
	if bench --site "$SITE" run-tests --module "$module" --junit-xml-output "$WORK/junit/$name.xml" \
		>"$WORK/test-$name.log" 2>&1; then
		log "$module: passed"
	else
		log "$module: failures (checked against known-failures.txt below)"
	fi
done
# shellcheck disable=SC2086
"$PYTHON" "$HERE/junit_summary.py" "$WORK/junit" "$HERE/known-failures.txt" $TESTS | tee "$WORK/test-summary.txt"
exit "${PIPESTATUS[0]}"
