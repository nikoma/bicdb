# ADR-002: Browser runtime = browser_wasi_shim + OPFS sync-access-handle pool

Date: 2026-07-10 · Status: accepted · Scope: web/bicdb-client, crates/bicdb-wasm

## Context

BicDB compiles to `wasm32-wasip1` (Phase 0, `c4d3095`): all storage I/O
arrives in the browser as synchronous WASI `fd_*`/`path_*` imports. The
browser filesystem that can satisfy synchronous reads/writes is OPFS via
`FileSystemSyncAccessHandle` — but *acquiring* a handle (`getFileHandle`,
`createSyncAccessHandle`) is async, and a synchronous WASI syscall cannot
await. BicDB creates and renames files continuously (segments, tmp files
for atomic rewrites), so "preopen everything up front" is not enough by
itself.

Options considered for the syscall layer:

1. **Hand-rolled WASI import object.** Full control, no dependency; but
   reimplements iovec/dirent/filestat encoding for ~23 syscalls — the
   highest-defect-rate part, already solved elsewhere.
2. **`@bjorn3/browser_wasi_shim`** (MIT/Apache-2.0, ~30 KB). Implements the
   full preview1 surface with an overridable `Fd`/`Inode` object model, and
   already ships `SyncOPFSFile` wrapping a sync access handle. Its in-memory
   `Directory` would, however, silently put *newly created* files in RAM.

Options considered for async handle acquisition:

- **Asyncify / JSPI**: rewrite or suspend the wasm to allow async imports.
  Size/complexity cost (Asyncify) or Chrome-version dependency (JSPI).
- **SharedArrayBuffer sync-over-async bridge**: needs COOP/COEP headers on
  the embedding app — an infrastructure demand on every Hub deployment.
- **Handle pool (sqlite-wasm "SAHPool" design)**: pre-acquire N handles to
  opaque pool files at boot (the only async moment), map logical paths to
  pool files in memory, persist the name map in its own pool slot. File
  create = take a handle from the free list; delete = recycle. No wasm
  transforms, no special headers, works in every browser with OPFS.

## Decision

Use **browser_wasi_shim for the syscall layer** and a **handle pool for
storage** (`web/bicdb-client/src/opfs-pool.js`):

- `OpfsPoolDirectory extends Directory` overrides `create_entry_for_path`
  so new regular files come from the pool (`ERRNO_NOSPC` when exhausted).
- A `Map` subclass on `contents` observes the shim's direct link/unlink
  mutations. Because the shim's `path_rename` is unlink-then-relink,
  deletions go to a **limbo set**; after every wasm entry point the worker
  runs `sweep()`, which recycles limbo inodes no longer reachable from the
  root and persists the path→slot index (one index write per call, max).
- The shim is **vendored** (`vendor/browser_wasi_shim/`, 0.4.2) so the
  worker runs without a bundler; apps that bundle can substitute the npm
  package.

## Consequences

- Directory capacity is fixed at boot (default 128 files); exceeding it is
  a clean error telling the operator to raise `capacity`. Handles cannot be
  added mid-syscall by construction.
- A crash between a rename and its sweep loses the rename (never the file
  contents) — the same window POSIX has before the parent-directory fsync,
  which BicDB recovery already tolerates. Data writes themselves are
  durable at `fd_sync` (`flush()` on the sync access handle).
- One OPFS directory per logical database (`bicdb-<name>/pool/fNNNN`);
  sync access handles' exclusive locking plus a Web Lock in the client
  enforce single ownership.
- Verified end-to-end in headless Chromium (`web/bicdb-client/test/`):
  write → full page reload → OPFS recovery → compaction → encrypted-db
  wrong-key rejection → two-client lock contention.
