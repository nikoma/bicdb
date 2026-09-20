# BicDB WASM browser cache — working TODO

Goal: bicdb-core + bicdb-sql compiled to WebAssembly, running in a dedicated
Web Worker inside the Hub PWA, persisting to an encrypted OPFS cache, syncing
per-user working sets over HTTP via `bicdb-sync`. Target hardware: low-end
Chromebooks (4–8 GB RAM, eMMC storage).

Grounding audit (2026-07-10, on main): every native dep that blocks wasm is
confined to a leaf module — `ort`/`tokenizers` only in
`crates/bicdb-core/src/memory_index.rs`, `rustls` only in
`replication_transport.rs`, `libc` one line (`O_DSYNC`, `storage.rs:285`),
`memmap2` behind the non-default `SegmentReadMode::Mmap`, `osmpbfreader` only
in the db.rs OSM import, `zstd` only in storage.rs compression, `wasmtime`
confined to bicdb-sql's `adaptive/` (7 refs). Real `thread::spawn` only at
`event.rs:298` and `replication_transport.rs:93`; db.rs uses scoped threads
sized by `available_parallelism`. bicdb-core has NO cargo features today.
Strategy: target `wasm32-wasip1` (std::fs/time/random come free via WASI)
with a JS shim backed by OPFS `createSyncAccessHandle` — avoids a VFS-trait
refactor. `bicdb-sync::SyncCoordinator` is sans-io behind the `SyncEndpoint`
trait; the browser needs an HTTP endpoint impl + server bundle API.

Hard constraint to design around: BicDB is RAM-resident (full dataset in
memory at open) and wasm32 is a 32-bit address space — per-user working sets
must stay bounded (target ≤ ~200 MB).

Work items in order. Each should be completable and verifiable on its own;
check it off with the verification listed.

## Phase 0 — Feature-gate native deps + first wasm compile (spike)

**Status: DONE 2026-07-10, merged to main (c4d3095).** Verified results:
`cargo check --target wasm32-wasip1 --no-default-features` green for
core+sql+sync; `wasi_smoke` passes under wasmtime 37 (100 rows written,
aggregate-queried, recovered after reopen through real WASI file I/O);
native workspace check green; core lib 73/73; sql suite at its clean-tree
baseline (the one failure, `secure_sql_rejects_plaintext_protected_data_...`, fails
identically on the clean tree). Binary: 18.8 MB stripped / **4.9 MB
gzipped** — above the <3 MB target; wasm-opt + panic=abort still untried
(see 1.10). One runtime fix beyond the plan: WASI cannot fsync a directory
fd, so `sync_parent`/`sync_parent_dir` are no-ops on wasm32 (OPFS has no
directory-durability concept either; item 1.2 owns rename durability).
Guard: `scripts/wasm-check.sh` (no GitHub Actions in this repo yet, so 0.8
is a script, not a workflow).

- [x] **0.1 Add cargo features to bicdb-core.** `embeddings` (ort +
  tokenizers), `tls-replication` (rustls + rustls-pemfile), `mmap` (memmap2),
  `osm-import` (osmpbfreader), `compression` (zstd). `default` enables all —
  zero behavior change natively. Make the deps `optional = true`.
  *Verify: `cargo check -p bicdb-core` (default) and
  `cargo check -p bicdb-core --no-default-features` both green.*
- [x] **0.2 Gate the code behind those features.** `#[cfg(feature)]` on
  `memory_index.rs` embedding paths (runtime error "embeddings unavailable"
  when off — same contract as the existing ort load-dynamic fallback),
  `replication_transport.rs`, the mmap arm of `SegmentReadMode`, the OSM
  import entry points, the zstd arms of `CompressionConfig` (reject
  compressed segments with a clear error when off).
  *Verify: `--no-default-features` builds and core tests pass with defaults.*
- [x] **0.3 Put the `libc::O_DSYNC` line (storage.rs:285) behind
  `#[cfg(unix)]`** with a no-op fallback (wasm/OPFS gets durability from
  explicit flush).
- [x] **0.4 Feature-gate `wasmtime` in bicdb-sql** (`adaptive-procs` feature,
  default on). Stub the adaptive/WASM stored-proc backend to "unavailable"
  when off. *Verify: `cargo check -p bicdb-sql --no-default-features` green;
  sql tests at baseline (234 pass / 8 known-fail) with defaults.*
- [x] **0.5 Make thread use wasm-safe.** Sequential in-line fallback when
  `available_parallelism() == 1` or `target_arch = "wasm32"` for the
  `thread::scope` recovery/decode paths in db.rs; gate or inline the
  `event.rs:298` background delivery thread. Audit remaining
  `thread::sleep` call sites reachable on wasm.
  *Verify: grep shows no unconditional spawn on the wasm path; native tests
  green.*
- [x] **0.6 First wasm compile.**
  `cargo check -p bicdb-core -p bicdb-sql -p bicdb-sync
  --target wasm32-wasip1 --no-default-features` green. Fix stragglers the
  compiler surfaces (this is the item where unknown unknowns appear —
  getrandom/uuid should be free on wasip1, confirm).
- [x] **0.7 WASI smoke test off-browser.** Tiny bin/example: open db, create
  collection, insert, SQL SELECT, close, reopen, verify recovery — run under
  `wasmtime` CLI (or Node's WASI) against a preopened temp dir.
  *Verify: script exits 0; this is the Phase 0 go/no-go.*
- [x] **0.8 CI job for the wasm target** (check + the smoke test) so the port
  doesn't rot. *Verify: CI green on a PR that touches core.*
- [x] **0.9 Merge gate:** full native suites at their known baselines
  (core lib 58 + backup known-fails per `docs`/memory; sql 234/8) with
  default features. Land Phase 0 on main.

## Phase 1 — Browser runtime (worker + OPFS + TS client)

**Status: CORE DONE 2026-07-10** — BicDB runs SQL in headless Chromium over
real OPFS. Deliverables: `crates/bicdb-wasm` (C-ABI reactor, native unit
test included) and `web/bicdb-client` (`OpfsPool` storage layer, worker,
`BicDbClient`, Playwright smoke). The end-to-end test
(`web/bicdb-client/test/browser-smoke.mjs`) proves: 50-row write → full
page reload → OPFS recovery → compaction → encrypted-db wrong-key
rejection → two-client Web-Lock contention. Verify wall time for worker
boot + wasm instantiate + OPFS recovery + 4 queries + compact: **189 ms**
(server-class x86; Chromebook numbers pending 3.7). Remaining in-phase:
1.6 leader-proxying, 1.9 crash-mid-write + quota-exhaustion tests, and
productionizing the size-optimized build.

- [x] **1.1 Decide the JS↔wasm interop route** — DECIDED, see
  `docs/decisions/ADR-002-browser-wasi-opfs-pool.md`: vendored
  `@bjorn3/browser_wasi_shim` 0.4.2 for syscall decoding + a custom
  sqlite-wasm-style sync-access-handle pool for storage (no Asyncify, no
  JSPI, no COOP/COEP requirement).
- [x] **1.2 OPFS backing for the WASI fs imports.**
  `web/bicdb-client/src/opfs-pool.js`: pre-acquired handle pool (default
  capacity 128, ENOSPC when exhausted), path→slot index persisted through
  its own sync handle, limbo-set + sweep for rename-safe recycling.
  *Verified: browser smoke passes in Chrome headless against real OPFS.*
- [x] **1.3 Wasm export surface.** `crates/bicdb-wasm`: `bicdb_open/exec/
  stats/compact/close` + `bicdb_alloc/free`; length-prefixed JSON
  envelopes; panics caught at the boundary. Native round-trip unit test.
- [x] **1.4 Worker harness + RPC.** `src/worker.js`: owns pool + instance,
  `{id, op, args}` protocol, `pool.sweep()` after every wasm call.
  (Cancellation not implemented — SQL calls are synchronous inside the
  worker; revisit with streaming results.)
- [x] **1.5 Client package.** `@bicdb/client` (`src/client.js`) —
  promise-based `open/query/stats/compact/close`; `stats()` folds in
  `navigator.storage.estimate()`. Written as plain ESM JavaScript, not TS —
  hand-written `.d.ts` still to do when Hub consumes it.
- [ ] **1.6 Multi-tab.** DONE: single-owner Web Lock (second `open()` queues
  until first `close()`; verified in the smoke test). TODO: leader election
  with request proxying so follower tabs get answers instead of waiting.
- [x] **1.7 Encryption at rest.** `config.raw_key_hex` (WebCrypto-unwrapped
  key pass-through) or `config.passphrase` (argon2) → core's ChaCha20-
  Poly1305. *Verified: wrong key fails open with "wrong key or tampered
  ciphertext"; right key recovers after reload.* Key lifecycle doc still
  owed to the Hub app.
- [x] **1.8 Persistence posture.** `open()` requests
  `navigator.storage.persist()` best-effort; cache-is-disposable semantics
  documented in the README.
- [ ] **1.9 Browser test rig.** DONE: reload persistence, compaction,
  encryption, lock contention (all in browser-smoke.mjs). TODO:
  crash-mid-write (terminate worker between inserts), quota-exhaustion
  (clean error, no corruption).
- [x] **1.10 Size/perf pass (numbers).** Default release profile: 18.3 MB
  raw / 4.86 MB gz; + `wasm-opt -Oz`: 12.9 MB / 4.18 MB gz; +
  `panic="abort"` + `opt-level="z"` + `wasm-opt -Oz`: **7.1 MB / 2.55 MB
  gz — under the 3 MB target.** TODO before shipping that build: benchmark
  opt-level=z query latency vs 3, then encode the chosen flags in a
  `[profile.wasm-release]` or build script.

## Phase 2 — Sync (client ↔ server working sets)

**Status: CORE DONE 2026-07-10.** Design + protocol in
`docs/browser-sync.md`. The server is itself a sync node: `bicdb
sync-serve` (`crates/bicdb-cli/src/sync_server.rs`) keeps one BicDB per
scope; push imports into it, pull exports from it (own-events filtered),
so new devices bootstrap the whole working set from offset 0 — schema
included. E2E (`test/sync-e2e.mjs`): admin-SQL composition → device A
bootstrap+write → device B bootstrap sees server+A rows → A converges on
all three; wrong bearer token rejected. Discovered en route: record-audit
events are the sync substrate and were OFF by default — wasm `bicdb_open`
now defaults `audit_events: true`. Remaining: per-session auth (static
bearer today), TLS in front, Service-Worker background sync, retention of
server event logs.

- [x] **2.1 Server HTTP bundle API.** `bicdb sync-serve <root> --token
  --admin-token [--fsync] [--cors-origin]`; hand-rolled HTTP/1.1 (repo
  style, zero new deps); routes for checkpoint load/save, push, pull,
  healthz. Static bearer auth — swapping in Hub session tokens is one
  function.
- [x] **2.2 Working-set composition (mechanism).** One scope database per
  user; the Hub backend composes via `POST /v1/<scope>/sql` (separate
  admin token; disabled unless set). The *policy* — which rows each user
  class gets — remains product work in the Hub backend, as designed.
- [x] **2.3 `HttpSyncEndpoint` + JS coordinator.** JS-side fetch (as
  recommended); `SyncManager` in `src/sync.js` mirrors
  `SyncCoordinator::sync_once` step-for-step against new wasm exports
  `bicdb_node_id/sync_export/sync_import` (native convergence unit test in
  bicdb-wasm). *Verified: browser ↔ dev server round trip in e2e.*
- [ ] **2.4 Background sync loop.** DONE: `SyncManager.start()` — interval
  loop + exponential backoff (8x cap) + immediate run on the `online`
  event; `syncOnce()` for eager draft pushes. TODO: Service Worker
  periodic-background-sync for closed-app freshness.
- [x] **2.5 Conflict policy, written down.** `docs/browser-sync.md`:
  event dedup by id; per-record LWW on `(timestamp, sequence, node_id)`;
  server-authoritative for server-written rows by construction; guidance
  to split shared-mutable rows per writer.
- [x] **2.6 End-to-end convergence test.** `npm run test:sync`: real
  `bicdb sync-serve` process + two isolated browser contexts (separate
  OPFS); compose → bootstrap → offline-style writes → convergence on both
  devices and on the server.

## Phase 3 — Cache hygiene + production readiness

**Status: CORE DONE 2026-07-10** (3.7 needs physical hardware; 3.8 drafted,
sign-off external). Design + limits in `docs/browser-cache-hygiene.md`.
The correctness centerpiece this phase surfaced: **compaction rewrites
event offsets**, which silently invalidated both sync directions' stored
checkpoints. Fixed structurally — client pushes are driven by the engine's
own compact-aware export watermark (new core API `mark_sync_exported` +
wasm `bicdb_sync_status/sync_export_pending/sync_mark_exported`), and the
server's retention sweep resets its clients' stored pull checkpoints after
compacting (full re-pull, absorbed by event-id dedup). Proven by a native
unit test and the `compact-write-sync` / retention e2e phases.

- [x] **3.1 Quota monitoring.** `CacheManager`: pool bytes +
  `navigator.storage.estimate()` per check; escalation compact →
  `cache-full` state via `onState`; snapshots feed telemetry.
- [x] **3.2 Bounded datasets (redesigned — no local LRU).** Local
  per-collection eviction fights the event-sourced sync model (history
  stays in the log; audit reconciliation resurrects dropped records), so
  bounding is server-side (composition + retention) with whole-cache reset
  as the local escape hatch (`BicDbClient.destroy()` → reopen → re-
  bootstrap; verified in browser-smoke). Rationale recorded in the hygiene
  doc; supersedes the original LRU sketch.
- [x] **3.3 Compaction scheduling.** `CacheManager.start()` runs checks in
  `requestIdleCallback`; compaction serialized against sync via
  `runExclusive` (compacting mid-sync would corrupt watermark handling).
  Verified: churn garbage reclaimed under pressure in browser-smoke.
- [x] **3.4 Server-controlled retention.** `bicdb sync-serve --retention
  <json> --retention-interval-seconds N`: per-scope age rules on numeric
  timestamp columns; sweep deletes → compacts → resets pull checkpoints;
  deletions ride sync to every device (verified in e2e). Client-side
  enforcement unnecessary by construction (server composes).
- [x] **3.5 Encrypted chunked attachments.** `AttachmentStore`: AES-GCM,
  1 MiB chunks with per-chunk IVs, blobs in OPFS outside the db, synced
  `_attachments` manifest, re-fetch-on-miss signal via `has()`. Verified:
  3-chunk roundtrip, ciphertext-only on disk, wrong-key rejection.
  (Range reads for streaming playback: format supports chunk seeking, API
  not yet exposed.)
- [x] **3.6 Telemetry.** `TelemetryReporter` (bounded offline-safe buffer)
  → `POST /v1/<scope>/telemetry` → per-scope JSONL with `received_at`.
  CacheManager auto-reports; sync reports/cold-open by the app. Verified
  end-to-end in sync-e2e.
- [ ] **3.7 Real-hardware validation.** On an actual low-end Chromebook
  (4 GB): cold open, first-query latency, memory ceiling at target
  working-set size, battery/thermal sanity during sync. Record numbers.
  (Blocked on hardware; all software prerequisites shipped.)
- [ ] **3.8 Security/compliance review.** Threat model DRAFTED —
  `docs/browser-cache-threat-model.md` (assets, boundaries, the 5 open
  gaps incl. static tokens + event-history-outlives-retention). FERPA/
  regulatory sign-off remains external and blocks regulated-data rollout only.

## Phase 6 — BicUI substrate enablers (generic; see ADR-003)

**Status: DONE 2026-07-10.** Each shipped as an ordinary, UI-agnostic
BicDB feature (ADR-003 records the boundary). Verified in the substrate
scenario of browser-smoke plus the direction/dual scenarios of sync-e2e;
native coverage in bicdb-wasm tests (sessions, generations).

- [x] **6.1 Multi-database worker/client.** The wasm registry already holds
  many databases; extend worker RPC + BicDbClient so one worker owns
  several OPFS databases (e.g. control + data + local-state), each with its
  own pool and encryption config.
- [x] **6.2 Session-persistent typed exec.** A worker-held SqlSession per
  database so multi-statement transactions and session GUCs survive across
  calls (today every bicdb_exec builds a fresh session); explicit
  begin/exec/commit worker ops.
- [x] **6.3 Change-generation reporting.** Expose collection generations
  through wasm/client (HotView precedent) so callers can invalidate bounded
  queries after commands/sync imports without polling.
- [x] **6.4 Sync direction policy.** Server-side per-collection
  write-direction enforcement in sync-serve: pushes carrying events for
  protected collections are rejected (prerequisite for any
  server-authoritative synced content, BicUI or otherwise).
- [x] **6.5 Additional sync cursors.** Support N independent
  scope-database cursors per client cleanly in the JS SyncManager (the
  "separate control database, separate cursor" pattern).


## Phase 4 — Later / optional

- [ ] **4.1 On-device embeddings.** onnxruntime-web / transformers.js on the
  JS side generating vectors, fed into core's HNSW (`hnsw.rs` is pure Rust
  and already ports) for fully-local semantic search.
- [ ] **4.2 wasm threads** (SharedArrayBuffer + COOP/COEP headers +
  wasm32-wasip1-threads) only if single-threaded query/recovery perf proves
  insufficient on real hardware. Measure first (3.7).
- [ ] **4.3 Binary startup snapshot for fast cold opens** — the
  `perf/startup-checkpoint` branch (opt-in derived cache, −16% open time)
  may matter more on eMMC than it did on the server; re-evaluate once 3.7
  numbers exist.
