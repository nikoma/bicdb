// Main-thread client for BicDB-in-a-worker.
//
//   const db = await BicDbClient.open({
//     database: "hub",                       // or databases: [{name, config}, ...]
//     wasmUrl: new URL("/bicdb_wasm.wasm", location.href),
//   });
//   await db.query("CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)");
//   const { rows } = await db.query("SELECT body FROM notes");
//   await db.close();
//
// Multi-database (A6.1): one worker + one wasm instance own several OPFS
// databases (e.g. a BicUI control DB + data DB + local-state DB). Per-db
// operations route through handles:
//
//   const client = await BicDbClient.open({
//     databases: [{ name: "control" }, { name: "data", config: { raw_key_hex } }],
//     wasmUrl,
//   });
//   const control = client.db("control");
//   await control.query("SELECT ...");
//   const session = await control.session();   // persistent: BEGIN/COMMIT + GUCs
//   await session.exec("BEGIN"); ... await session.exec("COMMIT");
//
// Top-level query/stats/etc. operate on the PRIMARY (first) database, so
// single-database callers are unchanged.
//
// Single-owner guarantee: open() acquires a Web Lock per database name and
// holds them until close(). A second context opening any of the same
// databases waits rather than corrupting the OPFS pools.

// Per-database operation surface; also what SyncManager and CacheManager
// accept (anything with these methods + runExclusive).
export class DbHandle {
  #client;
  #name;

  constructor(client, name) {
    this.#client = client;
    this.#name = name;
  }

  get name() {
    return this.#name;
  }

  #rpc(op, args = {}) {
    return this.#client._rpc(op, { ...args, db: this.#name });
  }

  runExclusive(fn) {
    return this.#client.runExclusive(fn);
  }

  // Execute one SQL statement; resolves to {columns, rows, commandTag}.
  async query(sql) {
    const { result } = await this.#rpc("exec", { sql });
    return {
      columns: result.columns ?? [],
      rows: result.rows ?? [],
      commandTag: result.command_tag ?? null,
    };
  }

  // Persistent session: transactions and session GUCs survive across
  // exec() calls until close() (A6.2).
  async session() {
    const { session } = await this.#rpc("session-open");
    const rpc = (op, args) => this.#rpc(op, { ...args, session });
    return {
      exec: async (sql) => {
        const { result } = await rpc("session-exec", { sql });
        return {
          columns: result.columns ?? [],
          rows: result.rows ?? [],
          commandTag: result.command_tag ?? null,
        };
      },
      close: () => rpc("session-close"),
    };
  }

  // Per-collection change generations (A6.3) — rerun a bounded query only
  // when a collection it depends on changed.
  async generations() {
    const { generations } = await this.#rpc("generations");
    return generations;
  }

  async stats() {
    const { stats, pool } = await this.#rpc("stats");
    let quota = null;
    if (navigator.storage?.estimate) {
      try {
        const { usage, quota: total } = await navigator.storage.estimate();
        quota = { usageBytes: usage, quotaBytes: total };
      } catch {
        /* unsupported */
      }
    }
    return { db: stats, pool, quota };
  }

  async compact() {
    return this.runExclusive(async () => {
      const { report } = await this.#rpc("compact");
      return report;
    });
  }

  async trimEvents() {
    return this.runExclusive(async () => {
      const { report } = await this.#rpc("trim-events");
      return report;
    });
  }

  // --- sync plumbing (used by SyncManager; see src/sync.js) ---

  async nodeId() {
    const { nodeId } = await this.#rpc("node-id");
    return nodeId;
  }

  async meshIdentity() {
    return this.#rpc("mesh-identity");
  }

  async syncStatus() {
    const { status } = await this.#rpc("sync-status");
    return status;
  }

  // Mesh sync is fail-closed. The application must provision the same schema
  // at both endpoints and explicitly authorize each unprotected collection.
  async syncAuthorizeCollection(collection, enabled = true) {
    await this.#rpc("sync-authorize-collection", { collection, enabled });
  }

  // nodeId + verifyingKey must come from an authenticated control plane. This
  // method persists an immutable pin; it intentionally does no network TOFU.
  async syncPinPeer(nodeId, verifyingKey) {
    await this.#rpc("sync-pin-peer", { nodeId, verifyingKey });
  }

  async syncExport(checkpoint) {
    const { bundleJson, eventCount, nextCheckpoint, eventIds } = await this.#rpc(
      "sync-export",
      { checkpoint },
    );
    return { bundleJson, eventCount, nextCheckpoint, eventIds };
  }

  async syncExportPending() {
    const { bundleJson, eventCount, nextCheckpoint, eventIds } = await this.#rpc(
      "sync-export-pending",
    );
    return { bundleJson, eventCount, nextCheckpoint, eventIds };
  }

  async syncMarkExported(nextCheckpoint, eventCount) {
    await this.#rpc("sync-mark-exported", { nextCheckpoint, eventCount });
  }

  async syncImport(bundleJson) {
    const { report } = await this.#rpc("sync-import", { bundleJson });
    return report;
  }
}

export class BicDbClient {
  #worker;
  #pending = new Map();
  #nextId = 1;
  #releaseLocks = [];
  #exclusive = Promise.resolve();
  #handles = new Map();
  #primary = null;

  static async open({
    database,
    databases,
    wasmUrl,
    workerUrl = new URL("./worker.js", import.meta.url),
    capacity = 128,
    config = {},
    requestPersist = true,
    exclusive = true,
    // Forcibly break other holders of the per-database Web Locks. Only use
    // when the user has confirmed no other tab is actively writing — two
    // live owners of one OPFS pool can corrupt it. The escape hatch for a
    // stale/backgrounded tab that never released.
    stealLocks = false,
    onProgress = null,
  } = {}) {
    const specs =
      databases ?? (database ? [{ name: database, capacity, config }] : null);
    if (!specs?.length) throw new Error("database name(s) are required");
    if (!wasmUrl) throw new Error("wasmUrl is required");
    for (const spec of specs) {
      spec.capacity ??= capacity;
      spec.config ??= {};
    }

    const notify = (p) => {
      try {
        onProgress?.(p);
      } catch {
        /* progress observers must not break boot */
      }
    };

    if (requestPersist && navigator.storage?.persist) {
      try {
        await navigator.storage.persist();
      } catch {
        /* unsupported */
      }
    }

    const client = new BicDbClient();
    if (exclusive && navigator.locks?.request) {
      for (const spec of specs) {
        const lockName = `bicdb:${spec.name}`;
        // Probe whether it's free so the UI can distinguish "acquiring" from
        // "blocked by another tab" instead of silently hanging.
        let free = true;
        try {
          free = await navigator.locks.request(lockName, { ifAvailable: true }, (lock) => lock !== null);
        } catch {
          /* query unsupported → assume free and fall through */
        }
        notify({ stage: "waiting for lock", database: spec.name, contended: !free && !stealLocks });
        try {
          await new Promise((lockAcquired, lockFailed) => {
            navigator.locks
              .request(lockName, { steal: !!stealLocks }, (lock) => {
                if (lock === null) {
                  lockFailed(new Error(`another tab holds ${spec.name}`));
                  return;
                }
                lockAcquired();
                return new Promise((release) => client.#releaseLocks.push(release));
              })
              .catch(lockFailed);
          });
        } catch (error) {
          client.#teardown();
          throw error;
        }
      }
      notify({ stage: "lock acquired" });
    }

    // With a progress observer, download the module here (streaming, with
    // byte counts) and hand the bytes to the worker.
    let wasmBytes;
    if (onProgress) {
      notify({ stage: "downloading engine", loadedBytes: 0 });
      const response = await fetch(String(wasmUrl));
      if (!response.ok) {
        client.#teardown();
        throw new Error(`failed to fetch wasm: ${response.status}`);
      }
      const totalBytes =
        Number(response.headers.get("x-decompressed-length")) ||
        Number(response.headers.get("content-length")) ||
        null;
      const reader = response.body.getReader();
      const parts = [];
      let loadedBytes = 0;
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        parts.push(value);
        loadedBytes += value.length;
        notify({ stage: "downloading engine", loadedBytes, totalBytes });
      }
      const joined = new Uint8Array(loadedBytes);
      let at = 0;
      for (const part of parts) {
        joined.set(part, at);
        at += part.length;
      }
      wasmBytes = joined.buffer;
    }

    client.#worker = new Worker(workerUrl, { type: "module" });
    client.#worker.onmessage = (event) => {
      if (event.data?.progress !== undefined) {
        notify({ stage: event.data.progress });
        return;
      }
      client.#dispatch(event.data);
    };
    client.#worker.onerror = (event) => client.#failAll(event.message);

    try {
      const args = { wasmUrl: String(wasmUrl), databases: specs };
      if (wasmBytes !== undefined) {
        args.wasmBytes = wasmBytes;
        await client._rpcTransfer("open", args, [wasmBytes]);
      } else {
        await client._rpc("open", args);
      }
    } catch (error) {
      client.#teardown();
      throw error;
    }
    client.#primary = specs[0].name;
    for (const spec of specs) {
      client.#handles.set(spec.name, new DbHandle(client, spec.name));
    }
    return client;
  }

  // Per-database operation handle.
  db(name = this.#primary) {
    const handle = this.#handles.get(name);
    if (handle === undefined) throw new Error(`no open database named ${name}`);
    return handle;
  }

  get databaseNames() {
    return [...this.#handles.keys()];
  }

  // Attach another database to the running worker (own OPFS pool + mount).
  async openDatabase({ name, capacity = 128, config = {} }) {
    await this._rpc("open-database", { name, capacity, config });
    this.#handles.set(name, new DbHandle(this, name));
    return this.db(name);
  }

  async closeDatabase(name) {
    await this._rpc("close-database", { database: name });
    this.#handles.delete(name);
  }

  // Run `fn` with exclusive access relative to other runExclusive callers
  // (sync rounds, compaction, and horizon trims serialize through this).
  runExclusive(fn) {
    const run = this.#exclusive.then(fn);
    this.#exclusive = run.then(
      () => {},
      () => {},
    );
    return run;
  }

  // --- primary-database conveniences (single-db API compatibility) ---

  query(sql) {
    return this.db().query(sql);
  }
  stats() {
    return this.db().stats();
  }
  compact() {
    return this.db().compact();
  }
  trimEvents() {
    return this.db().trimEvents();
  }
  generations() {
    return this.db().generations();
  }
  session() {
    return this.db().session();
  }
  nodeId() {
    return this.db().nodeId();
  }
  meshIdentity() {
    return this.db().meshIdentity();
  }
  syncStatus() {
    return this.db().syncStatus();
  }
  syncAuthorizeCollection(collection, enabled = true) {
    return this.db().syncAuthorizeCollection(collection, enabled);
  }
  syncPinPeer(nodeId, verifyingKey) {
    return this.db().syncPinPeer(nodeId, verifyingKey);
  }
  syncExport(checkpoint) {
    return this.db().syncExport(checkpoint);
  }
  syncExportPending() {
    return this.db().syncExportPending();
  }
  syncMarkExported(nextCheckpoint, eventCount) {
    return this.db().syncMarkExported(nextCheckpoint, eventCount);
  }
  syncImport(bundleJson) {
    return this.db().syncImport(bundleJson);
  }

  async close() {
    try {
      await this._rpc("close");
    } finally {
      this.#teardown();
    }
  }

  // Remove a closed database's OPFS directory entirely (per database name).
  static async destroy(database) {
    const opfsRoot = await navigator.storage.getDirectory();
    await new Promise((resolve, reject) => {
      navigator.locks.request(`bicdb:${database}`, async () => {
        try {
          await opfsRoot.removeEntry(`bicdb-${database}`, { recursive: true });
          resolve();
        } catch (error) {
          if (error?.name === "NotFoundError") resolve();
          else reject(error);
        }
      });
    });
  }

  #teardown() {
    this.#worker?.terminate();
    this.#worker = null;
    this.#failAll("client closed");
    for (const release of this.#releaseLocks.splice(0)) release();
    this.#handles.clear();
  }

  _rpc(op, args) {
    return this._rpcTransfer(op, args, []);
  }

  _rpcTransfer(op, args, transfer) {
    if (this.#worker === null) {
      return Promise.reject(new Error("client is closed"));
    }
    const id = this.#nextId++;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      this.#worker.postMessage({ id, op, args }, transfer);
    });
  }

  #dispatch(message) {
    const pending = this.#pending.get(message.id);
    if (pending === undefined) return;
    this.#pending.delete(message.id);
    if (message.ok) {
      pending.resolve(message);
    } else {
      pending.reject(new Error(message.error));
    }
  }

  #failAll(reason) {
    for (const { reject } of this.#pending.values()) {
      reject(new Error(reason));
    }
    this.#pending.clear();
  }
}
