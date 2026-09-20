# @bicdb/client

BicDB compiled to `wasm32-wasip1`, running inside a dedicated Web Worker,
persisting to an OPFS-backed cache. This is the browser runtime for the plan
in `docs/wasm-browser-cache-todo.md` (Phase 1).

## Layout

- `src/opfs-pool.js` — the storage layer: a pool of pre-acquired OPFS
  `FileSystemSyncAccessHandle`s mapped to logical paths (the sqlite-wasm
  "SAHPool" design), exposed to the WASI shim as a `Directory` tree.
- `src/worker.js` — the worker: boots the pool, instantiates
  `bicdb_wasm.wasm` behind `@bjorn3/browser_wasi_shim`, and serves a small
  RPC protocol.
- `src/client.js` — `BicDbClient`, the main-thread API: `open / query /
  stats / compact / close`, plus `navigator.storage.persist()` and a Web
  Lock so only one context owns a database at a time.
- `vendor/browser_wasi_shim/` — vendored `@bjorn3/browser_wasi_shim` 0.4.2
  (MIT OR Apache-2.0; see LICENSE-MIT there). Vendored so the worker runs
  without a bundler; swap for the npm package if you already bundle.

## Build the wasm module

```sh
cargo build --release -p bicdb-wasm --target wasm32-wasip1 \
  --config 'profile.release.strip="debuginfo"'
# serve target/wasm32-wasip1/release/bicdb_wasm.wasm next to your app
```

## Use

```js
import { BicDbClient } from "@bicdb/client";

const db = await BicDbClient.open({
  database: "hub",                      // one OPFS dir per database
  wasmUrl: "/bicdb_wasm.wasm",
  config: { raw_key_hex: keyHex },      // optional encryption at rest
});
await db.query("CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)");
await db.query("INSERT INTO notes VALUES (1, 'offline first')");
const { rows } = await db.query("SELECT body FROM notes WHERE id = 1");
const { db: stats, pool, quota } = await db.stats();
await db.close();
```

## Provision sync authority

Mesh synchronization is fail-closed. Creating a table does not make it
syncable, and an incoming peer cannot authenticate itself by presenting an
untrusted key in-band. Provision matching schemas at both endpoints, obtain
each endpoint's public mesh identity through an authenticated control plane,
pin the remote identity, and explicitly authorize each unprotected collection:

```js
const localIdentity = await db.meshIdentity();

// Send localIdentity through the trusted control plane. Receive the peer's
// authenticated identity through that same channel.
await db.syncPinPeer(peer.nodeId, peer.verifyingKey);
await db.syncAuthorizeCollection("notes", true);
```

`syncPinPeer` refuses a conflicting re-pin. Collection authorization refuses
policy-protected collections and can be revoked with
`syncAuthorizeCollection("notes", false)`.

Requirements: a secure context (https or localhost) and OPFS sync access
handles (Chrome/Edge 102+, Safari 16.4+, Firefox 111+ — every ChromeOS
device in scope). The `capacity` option (default 128) fixes the maximum
number of files in the database directory; raise it before opening if a
working set legitimately needs more.

## Test

```sh
npm install && npx playwright install chromium
npm test   # headless-Chromium smoke: write, reload, recover via real OPFS
```
