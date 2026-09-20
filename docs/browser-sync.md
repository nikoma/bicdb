# Browser sync: working sets over HTTP

How the browser cache (`web/bicdb-client`, `crates/bicdb-wasm`) stays in
sync with the server (`bicdb sync-serve`). Phase 2 of
`docs/wasm-browser-cache-todo.md`; interop decision in
`docs/decisions/ADR-002-browser-wasi-opfs-pool.md`.

## Model

Every user (or other cache unit) gets a **scope**: a normal BicDB database
on the server at `<root>/<scope>/db`, which is the authoritative twin of
that user's browser cache. The server is itself a sync node in bicdb's
event-bundle mesh (`sync_mesh.rs`, `bicdb-sync`):

- **push** — the browser exports its local record-audit events as a
  `SyncBundle`; the server imports it into the scope database immediately.
  Bundles never accumulate on disk.
- **pull** — the server exports the scope database's events since the
  client's last recorded server checkpoint. A brand-new device pulls from
  offset 0 and receives the whole working set (schema included — SQL
  schema rows live in `__bicdb_pg_schema` and travel like any record).
  The puller's own events are filtered out server-side so they don't echo.
- **checkpoints** — stored server-side per client node id
  (`<root>/<scope>/checkpoints/<node>.json`), same layout as
  `FileSyncEndpoint`. The browser's node id persists in the database's
  `sync_state.json`, so a device keeps its identity across sessions.

The JS `SyncManager` (`web/bicdb-client/src/sync.js`) mirrors
`bicdb_sync::SyncCoordinator::sync_once` exactly: load checkpoint → push →
pull/import → re-export to advance `local_export` past imports (prevents
echo on the next push) → save checkpoint.

Record-audit events are the sync substrate. The wasm `bicdb_open` enables
`audit_events` by default; a database opened with `audit_events: false`
does not sync.

## Conflict policy

- **Events** are content-addressed (event id + payload hash) and
  deduplicated on import — replaying a bundle, or pulling your own writes
  back, is harmless.
- **Records** resolve **last-writer-wins per record**, ordered by
  `(timestamp, sequence, node_id)` (`AuditOrder` in core). The tuple makes
  ties deterministic on every node; timestamps come from the writer's
  clock (browser clocks feed `clock_time_get`, server uses system time).
- **Deletes** participate the same way: a newer delete beats an older
  update and vice versa.
- Practical reading for Hub:
  - *Cache-shaped data* (courses, boards, announcements, appointments):
    written only by the server → server is authoritative by construction.
  - *Drafts / client-first rows*: written only by that user's devices →
    the newest device write wins; push eagerly (call `syncNow()` after
    saving) to shrink the window where two of the user's devices diverge.
  - Fields edited from both sides concurrently resolve whole-record LWW —
    don't design shared-mutable rows that need field-level merge; split
    them into per-writer rows instead.

## Server

```sh
bicdb sync-serve <root> --host 0.0.0.0 --port 8787 \
  --token <client-bearer> --admin-token <backend-bearer> [--fsync]
```

Routes (JSON bodies; `Authorization: Bearer <token>` unless noted):

| Route | Purpose |
|---|---|
| `GET /healthz` | liveness (no auth) |
| `GET /v1/<scope>/checkpoint/<node>` | load client checkpoint |
| `PUT /v1/<scope>/checkpoint/<node>` | save client checkpoint |
| `POST /v1/<scope>/push` | body `SyncBundle` → import, returns `PushBundleReport` |
| `POST /v1/<scope>/pull` | body `{node_id, checkpoint}` → `{server_node_id, bundles}` |
| `POST /v1/<scope>/sql` | body `{sql}` → `{result}` — **admin token only** |

**Working-set composition** happens through `POST /v1/<scope>/sql`: the Hub
backend decides what a user should have (their courses, chats, today's
appointments) and writes those rows into the scope database; connected
devices receive them on the next pull. Never open a scope directory with a
second process while the server runs — go through the endpoint. `sql` may
be an array of statements run in one session for transactional administration.
It cannot set `bicdb.current_*` identity. In RLS composition mode the trusted
host derives the user and tenant from the configured scope mapping and opens a
secure SQL session internally.

## Multi-tenant mode: RLS-driven composition (`--rls-compose`)

Instead of hand-composing scopes, declare visibility ONCE as PostgreSQL
row-level-security policies on a single **master** database, and the server
maintains each user's scope as a materialized RLS view:

```sh
bicdb sync-serve <root> --token ... --admin-token ... --rls-compose compose.json
```

```json
{
  "master_scope": "master",
  "user_scope_prefix": "user-",
  "tables": [ { "table": "announcements", "pk": "id" },
              { "table": "docs", "pk": "id" } ],
  "interval_seconds": 5
}
```

Seed the master through the admin endpoint: create tables, `ENABLE/FORCE
ROW LEVEL SECURITY`, and `CREATE POLICY ... USING
(current_setting('bicdb.current_user', true) = owner)`. A table without
policies is visible to everyone (announcements, catalogs). Devices sync
scope `user-<id>` exactly as in per-scope mode. Each composer tick, per
user:

1. **Reverse pass** — client-originated events in the scope replay onto the
   master *as that user* (session GUCs set), so `WITH CHECK`/`USING`
   authorize every device write. A rejected write (e.g. a forged
   `owner='someone-else'` row) is skipped, and the forward pass reverts it
   on the offending device — authorization violations are visibly undone.
2. **Forward pass** — `SELECT *` per table as the user (RLS filters),
   diffed into the scope. Grants appear as inserts, revocations as deletes;
   visibility changes need no checkpoint surgery because the scope is
   state, not a filtered stream.

Verified end to end in `npm run test:rls`: two users' browsers, per-user
visibility, device write-back under policy, forgery rejection + revert, and
an ownership transfer propagating as revoke+grant.

Cost model and limits: each user's *visible* rows are duplicated into their
scope (bounded by working-set design); the composer is O(users ×
visible rows) per tick — right for working sets, not for huge tenants; the
policy expression subset is the application-policy set documented in
`docs/security.md`; and the static `--token` still lets any client claim
any scope name — per-session tokens (the known auth gap) matter even more
in this mode. The eventual alternative — native per-user *filtered event
streams* from one database with no scope copies — is specced as Phase 5 in
the TODO.

Deployment cautions (deliberate scope limits of this iteration):

- The listener is plaintext HTTP/1.1 — put TLS termination in front.
- `--token` is a static bearer shared by clients; replace with per-session
  Hub tokens (the auth check is one function in
  `crates/bicdb-cli/src/sync_server.rs`) before real deployments.
- One OS thread per request, one mutex per scope database: right-sized for
  per-user working sets, not for thousands of concurrent pullers of one
  scope.

## Browser

```js
import { BicDbClient, HttpSyncEndpoint, SyncManager } from "@bicdb/client";

const db = await BicDbClient.open({ database: "hub", wasmUrl });
const sync = new SyncManager(
  db,
  new HttpSyncEndpoint({ url: "https://sync.example", scope: userId, token }),
  { intervalMs: 30_000, onReport: console.log, onError: console.warn },
);
sync.start();            // sync now, then every 30 s; backoff (max 8x)
                         // while offline; immediate run on the `online` event
await sync.syncOnce();   // eager push, e.g. right after saving a draft
sync.stop();
```

Service-Worker periodic background sync (closed-app freshness) is not wired
yet — the loop above runs while the app is open. E2E coverage:
`web/bicdb-client/test/sync-e2e.mjs` (real server + two isolated browser
contexts: compose → bootstrap → two-device convergence).
