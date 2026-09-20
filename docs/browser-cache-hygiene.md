# Browser cache hygiene: quota, compaction, retention, attachments

Phase 3 of `docs/wasm-browser-cache-todo.md`. How the browser cache stays
bounded on a 4 GB Chromebook, and which side of the wire owns each lever.
Protocol background: `docs/browser-sync.md`.

## Who bounds what

The cache is an event-sourced sync replica, and that shapes the design:

- **The server bounds the working set.** Composition (what a user gets) and
  retention (how long they keep it) both act on the scope database, and the
  resulting deletes ride the normal sync stream to every device.
- **The client reclaims garbage.** Append-only churn (updated/deleted rows,
  superseded log entries) is reclaimed by compaction, scheduled under
  pressure during idle time.
- **Whole-cache reset is the local escape hatch.** `BicDbClient.destroy()`
  wipes the OPFS directory; the next open + sync re-bootstraps the working
  set from the server (Phase 2 made bootstrap-from-zero the normal path).

**Why no per-collection LRU eviction on the client:** locally dropping one
collection's records would leave its full history in the event log (little
space actually won) and the next import's audit reconciliation would
resurrect the records anyway. Partial local eviction fights the sync model;
bounding belongs where data is composed. This supersedes the original 3.2
sketch — the mechanism is server retention + client reset, not local LRU.

## Client: CacheManager

```js
import { CacheManager } from "@bicdb/client";
const manager = new CacheManager(db, {
  maxBytes: 200 * 1024 * 1024,  // working-set budget
  softRatio: 0.7,               // compact above 70%, report full above 100%
  checkIntervalMs: 60_000,
  telemetry,                    // optional TelemetryReporter
  onState: ({ state, usageBytes }) => { /* show "cache full" UI, or reset */ },
});
manager.start();  // checks run inside requestIdleCallback
```

Escalation per check: measure pool bytes → over the soft threshold, run
`compact()` (serialized against sync via `runExclusive`) → still over
`maxBytes`, emit `cache-full`. The app resolves cache-full by shrinking the
working set server-side or by reset: `close()` →
`BicDbClient.destroy(name)` → `open()` → `syncOnce()`.

## Compaction × sync (the correctness core of this phase)

`compact()` rewrites event-log offsets, which invalidates any externally
stored byte-offset checkpoint. Both directions are handled:

- **Client push** is driven by the *engine's* export watermark
  (`sync_status().last_export_checkpoint`, advanced via
  `bicdb_sync_mark_exported` after the server confirms a push). The engine
  resets that watermark during compaction, so the next push re-exports from
  zero and the server's event-id dedup absorbs the replay. Covered by a
  native unit test (`export_pending_survives_compaction`) and in-browser by
  the `compact-write-sync` e2e phase.
- **Server pull checkpoints** are server-owned files; when the retention
  sweep compacts a scope database it rewrites every stored client
  checkpoint's server watermark to zero. The client's next pull is a full
  re-send, deduplicated on import. Covered by the retention e2e phase.
- The `SyncManager` tail-advance (skipping just-imported events) only marks
  the watermark when the pending export contains *nothing but* the imported
  events — a local write racing in during the pull keeps the watermark put
  (worst case a harmless echo, never a lost write).

## Server: retention

```sh
bicdb sync-serve <root> ... --retention retention.json --retention-interval-seconds 3600
```

```json
{
  "defaults": [
    { "table": "messages", "column": "created_at", "max_age_seconds": 7776000 }
  ],
  "scopes": { "clinician-42": [
    { "table": "appointments", "column": "starts_at", "max_age_seconds": 172800 }
  ] }
}
```

`column` is a numeric timestamp (`column_unit`: `epoch_seconds` default, or
`epoch_millis`). Each sweep deletes aged rows per scope, and — if anything
was deleted — compacts the scope database and resets its clients' pull
checkpoints. Scopes without the table skip the rule silently.

## Attachments (media outside the database)

`AttachmentStore`: AES-GCM-encrypted, 1 MiB-chunked OPFS files
(`bicdb-<db>-attachments/`), with a manifest row in the synced
`_attachments` table. Blobs are local-only cache: a manifest row without a
local blob (new device, wiped cache) is the signal to re-fetch from the URL
carried in the manifest metadata and `put()` again. `usageBytes()` feeds
the app's cache-pressure picture (attachment eviction is app policy — plain
`delete()` per id, coarsest-first).

## Telemetry

`TelemetryReporter` buffers events (bounded, offline-safe) and posts them
to `POST /v1/<scope>/telemetry`; the server appends JSONL to
`<root>/<scope>/telemetry.jsonl` with a `received_at` stamp. Feed it
`SyncManager` reports, `CacheManager` snapshots (built in — pass
`telemetry:` to the constructor), and cold-open timings.

## Event-horizon trimming (bounding history itself)

Record-audit history is the sync substrate, but state convergence only
consumes each record's *winning* event. `BicDb::trim_event_horizon`
exploits that: superseded events drop unconditionally (any peer still
holding one loses last-writer-wins to the retained winner), winning
upserts are always kept (they are the bootstrap set), and winning deletes
are kept until the caller's peer horizon passes them (a lagging device
still needs the delete or its next push would resurrect the record). User
event-sourcing streams are never touched. Like compaction, trimming
rewrites event offsets — the export watermark resets, re-exports dedup.

- **Server**: `bicdb sync-serve --event-horizon` trims each scope in the
  sweep; the horizon is the minimum pull offset across live client
  checkpoints (`--horizon-max-checkpoint-age-seconds`, default 30 days —
  staler devices re-bootstrap, and rows they deleted-but-never-synced can
  resurrect: the documented trade-off). Client checkpoints are reset after
  a trim, exactly as after compaction.
- **Client**: `client.trimEvents()` uses the engine's export watermark as
  its horizon (everything the server confirmed); `CacheManager` runs it
  before compaction under pressure.
- Measured effect in the e2e: a row churned 30× collapses to **one**
  bootstrap event; a fresh device pulls the live working set, not history.

## Known limits (deliberate, documented)

- Devices offline past the horizon age re-bootstrap via event-id dedup;
  deletes they never pulled can resurrect rows they still hold. Shorten
  the age only as far as your offline story tolerates.
- Retention needs a numeric timestamp column; ISO-8601 string columns are
  not yet supported.
- The telemetry endpoint appends unbounded JSONL; rotate/ship it with
  standard log tooling.
