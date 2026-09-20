// Dedicated worker that owns one BicDB wasm instance and any number of
// databases (A6.1): each database gets its own OPFS sync-access-handle
// pool, mounted under the single WASI preopen as /data/<name>. Persistent
// SQL sessions (A6.2) and change-generation reporting (A6.3) are exposed
// per database.
//
// The main thread talks to us with {id, op, args} messages and gets
// {id, ok, ...} | {id, ok:false, error} back — see client.js. Ops taking
// an optional `db` argument (a database name) default to the primary
// (first-opened) database, which keeps the original single-db protocol
// working unchanged.

import {
  WASI,
  PreopenDirectory,
  ConsoleStdout,
  Fd,
} from "../vendor/browser_wasi_shim/index.js";
import { OpfsPool, MountRoot } from "./opfs-pool.js";

let instance = null;
let root = null; // MountRoot under /data
const databases = new Map(); // name -> {pool, dbId}
let primary = null;

const DATA_MOUNT = "/data";
const DB_DIR = "db";

function exports() {
  return instance.exports;
}

// Copy a JS string into wasm linear memory; caller frees.
function passString(text) {
  const bytes = new TextEncoder().encode(text);
  const ptr = exports().bicdb_alloc(bytes.length);
  new Uint8Array(exports().memory.buffer, ptr, bytes.length).set(bytes);
  return { ptr, len: bytes.length };
}

// Read + free a length-prefixed JSON envelope returned by any bicdb_* call.
function takeEnvelope(ptr) {
  const memory = exports().memory.buffer;
  const len = new DataView(memory).getUint32(ptr, true);
  const bytes = new Uint8Array(memory, ptr + 4, len).slice();
  exports().bicdb_free(ptr, 4 + len);
  const envelope = JSON.parse(new TextDecoder().decode(bytes));
  if (envelope.ok !== true) {
    throw new Error(envelope.error ?? "bicdb call failed");
  }
  return envelope;
}

// Boot-stage notes for the client's onProgress (no id → not an RPC reply).
const progress = (stage) => self.postMessage({ progress: stage });

function entryFor(args) {
  const name = args?.db ?? primary;
  const entry = databases.get(name);
  if (entry === undefined) throw new Error(`no open database named ${name}`);
  return entry;
}

async function attachDatabase({ name, capacity, config }) {
  if (databases.has(name)) throw new Error(`database ${name} is already open`);
  progress(`opening OPFS pool (${name})`);
  const opfsRoot = await navigator.storage.getDirectory();
  const baseDir = await opfsRoot.getDirectoryHandle(`bicdb-${name}`, {
    create: true,
  });
  const pool = await OpfsPool.boot(baseDir, { capacity });
  root.attach(name, pool.root);

  progress(`opening database (${name})`);
  const path = passString(`${DATA_MOUNT}/${name}/${DB_DIR}`);
  const cfg = passString(JSON.stringify(config ?? {}));
  try {
    const envelope = takeEnvelope(
      exports().bicdb_open(path.ptr, path.len, cfg.ptr, cfg.len),
    );
    databases.set(name, { pool, dbId: envelope.db });
  } catch (error) {
    root.detach(name);
    pool.close();
    throw error;
  } finally {
    exports().bicdb_free(path.ptr, path.len);
    exports().bicdb_free(cfg.ptr, cfg.len);
  }
  databases.get(name).pool.sweep();
  return databases.get(name).dbId;
}

function detachDatabase(name) {
  const entry = databases.get(name);
  if (entry === undefined) throw new Error(`no open database named ${name}`);
  try {
    takeEnvelope(exports().bicdb_close(entry.dbId));
  } finally {
    entry.pool.sweep();
    entry.pool.close();
    root.detach(name);
    databases.delete(name);
    if (primary === name) primary = databases.keys().next().value ?? null;
  }
}

async function opOpen({ wasmUrl, wasmBytes, database, databases: list, capacity, config }) {
  if (instance !== null) throw new Error("worker already has an open instance");
  const specs =
    list ?? [{ name: database, capacity, config }]; // single-db back-compat
  if (!specs.length || specs.some((s) => !s.name)) {
    throw new Error("open requires database name(s)");
  }

  root = new MountRoot();
  const preopen = new PreopenDirectory(DATA_MOUNT, new Map());
  preopen.dir = root;
  const wasiShim = new WASI(
    [],
    [],
    [
      new Fd(), // stdin: every op errors, nothing reads it
      ConsoleStdout.lineBuffered((line) => console.log(`[bicdb] ${line}`)),
      ConsoleStdout.lineBuffered((line) => console.warn(`[bicdb] ${line}`)),
      preopen,
    ],
  );

  progress("compiling engine");
  const imports = { wasi_snapshot_preview1: wasiShim.wasiImport };
  let wasmInstance;
  if (wasmBytes !== undefined) {
    ({ instance: wasmInstance } = await WebAssembly.instantiate(wasmBytes, imports));
  } else {
    const response = await fetch(wasmUrl);
    if (!response.ok) throw new Error(`failed to fetch wasm: ${response.status}`);
    ({ instance: wasmInstance } = await WebAssembly.instantiateStreaming(
      response,
      imports,
    ));
  }
  wasiShim.initialize(wasmInstance);
  instance = wasmInstance;

  for (const spec of specs) {
    await attachDatabase(spec);
  }
  primary = specs[0].name;
  return {
    db: databases.get(primary).dbId,
    databases: [...databases.keys()],
  };
}

// --- per-database ops ---------------------------------------------------

function withEntry(args, body) {
  const entry = entryFor(args);
  try {
    return body(entry);
  } finally {
    entry.pool.sweep();
  }
}

function opExec(args) {
  return withEntry(args, (entry) => {
    const arg = passString(args.sql);
    try {
      const envelope = takeEnvelope(
        exports().bicdb_exec(entry.dbId, arg.ptr, arg.len),
      );
      return { result: envelope.result };
    } finally {
      exports().bicdb_free(arg.ptr, arg.len);
    }
  });
}

function opStats(args) {
  return withEntry(args, (entry) => {
    const envelope = takeEnvelope(exports().bicdb_stats(entry.dbId));
    return { stats: envelope.stats, pool: entry.pool.stats() };
  });
}

function opCompact(args) {
  return withEntry(args, (entry) => {
    const envelope = takeEnvelope(exports().bicdb_compact(entry.dbId));
    return { report: envelope.report };
  });
}

function opTrimEvents(args) {
  return withEntry(args, (entry) => {
    const envelope = takeEnvelope(exports().bicdb_trim_events(entry.dbId));
    return { report: envelope.report };
  });
}

function opGenerations(args) {
  return withEntry(args, (entry) => {
    const envelope = takeEnvelope(exports().bicdb_generations(entry.dbId));
    return { generations: envelope.generations };
  });
}

// --- persistent sessions (A6.2) ------------------------------------------

function opSessionOpen(args) {
  return withEntry(args, (entry) => {
    const envelope = takeEnvelope(exports().bicdb_session_open(entry.dbId));
    return { session: envelope.session };
  });
}

function opSessionExec(args) {
  return withEntry(args, (entry) => {
    const arg = passString(args.sql);
    try {
      const envelope = takeEnvelope(
        exports().bicdb_session_exec(entry.dbId, args.session, arg.ptr, arg.len),
      );
      return { result: envelope.result };
    } finally {
      exports().bicdb_free(arg.ptr, arg.len);
    }
  });
}

function opSessionClose(args) {
  return withEntry(args, (entry) => {
    takeEnvelope(exports().bicdb_session_close(entry.dbId, args.session));
    return {};
  });
}

// --- sync plumbing ---------------------------------------------------------

function opNodeId(args) {
  const entry = entryFor(args);
  const envelope = takeEnvelope(exports().bicdb_node_id(entry.dbId));
  return { nodeId: envelope.node_id };
}

function opMeshIdentity(args) {
  const entry = entryFor(args);
  const envelope = takeEnvelope(exports().bicdb_mesh_identity(entry.dbId));
  return { nodeId: envelope.node_id, verifyingKey: envelope.verifying_key };
}

function opSyncStatus(args) {
  const entry = entryFor(args);
  const envelope = takeEnvelope(exports().bicdb_sync_status(entry.dbId));
  return { status: envelope.status };
}

function opSyncAuthorizeCollection(args) {
  return withEntry(args, (entry) => {
    if (typeof args.collection !== "string" || args.collection.length === 0) {
      throw new Error("collection is required");
    }
    if (typeof args.enabled !== "boolean") {
      throw new Error("enabled must be a boolean");
    }
    const collection = passString(args.collection);
    try {
      takeEnvelope(
        exports().bicdb_sync_authorize_collection(
          entry.dbId,
          collection.ptr,
          collection.len,
          args.enabled ? 1 : 0,
        ),
      );
      return {};
    } finally {
      exports().bicdb_free(collection.ptr, collection.len);
    }
  });
}

function opSyncPinPeer(args) {
  return withEntry(args, (entry) => {
    if (typeof args.nodeId !== "string" || args.nodeId.length === 0) {
      throw new Error("nodeId is required");
    }
    if (typeof args.verifyingKey !== "string" || args.verifyingKey.length === 0) {
      throw new Error("verifyingKey is required");
    }
    const nodeId = passString(args.nodeId);
    const verifyingKey = passString(args.verifyingKey);
    try {
      takeEnvelope(
        exports().bicdb_sync_pin_peer(
          entry.dbId,
          nodeId.ptr,
          nodeId.len,
          verifyingKey.ptr,
          verifyingKey.len,
        ),
      );
      return {};
    } finally {
      exports().bicdb_free(nodeId.ptr, nodeId.len);
      exports().bicdb_free(verifyingKey.ptr, verifyingKey.len);
    }
  });
}

function opSyncExportPending(args) {
  return withEntry(args, (entry) => {
    const envelope = takeEnvelope(exports().bicdb_sync_export_pending(entry.dbId));
    // bundleJson is opaque hash-verified bytes — deliver verbatim, never
    // JSON.parse + re-stringify it.
    return {
      bundleJson: envelope.bundle_json,
      eventCount: envelope.event_count,
      nextCheckpoint: envelope.next_checkpoint,
      eventIds: envelope.event_ids,
    };
  });
}

function opSyncExport(args) {
  return withEntry(args, (entry) => {
    const arg = passString(JSON.stringify(args.checkpoint ?? { event_offset: 0 }));
    try {
      const envelope = takeEnvelope(
        exports().bicdb_sync_export(entry.dbId, arg.ptr, arg.len),
      );
      return {
        bundleJson: envelope.bundle_json,
        eventCount: envelope.event_count,
        nextCheckpoint: envelope.next_checkpoint,
        eventIds: envelope.event_ids,
      };
    } finally {
      exports().bicdb_free(arg.ptr, arg.len);
    }
  });
}

function opSyncMarkExported(args) {
  return withEntry(args, (entry) => {
    const arg = passString(
      JSON.stringify({
        next_checkpoint: args.nextCheckpoint,
        event_count: args.eventCount ?? 0,
      }),
    );
    try {
      takeEnvelope(exports().bicdb_sync_mark_exported(entry.dbId, arg.ptr, arg.len));
      return {};
    } finally {
      exports().bicdb_free(arg.ptr, arg.len);
    }
  });
}

function opSyncImport(args) {
  return withEntry(args, (entry) => {
    const arg = passString(args.bundleJson); // exact bytes from the server
    try {
      const envelope = takeEnvelope(
        exports().bicdb_sync_import(entry.dbId, arg.ptr, arg.len),
      );
      return { report: envelope.report };
    } finally {
      exports().bicdb_free(arg.ptr, arg.len);
    }
  });
}

// --- lifecycle ---------------------------------------------------------------

async function opOpenDatabase(args) {
  if (instance === null) throw new Error("open the worker first");
  const dbId = await attachDatabase(args);
  return { db: dbId, databases: [...databases.keys()] };
}

function opCloseDatabase(args) {
  detachDatabase(args.database);
  return { databases: [...databases.keys()] };
}

function opClose() {
  for (const name of [...databases.keys()]) {
    try {
      detachDatabase(name);
    } catch {
      /* keep closing the rest */
    }
  }
  instance = null;
  root = null;
  return {};
}

const OPS = {
  open: opOpen,
  "open-database": opOpenDatabase,
  "close-database": opCloseDatabase,
  exec: opExec,
  stats: opStats,
  compact: opCompact,
  "trim-events": opTrimEvents,
  generations: opGenerations,
  "session-open": opSessionOpen,
  "session-exec": opSessionExec,
  "session-close": opSessionClose,
  close: opClose,
  "node-id": opNodeId,
  "mesh-identity": opMeshIdentity,
  "sync-status": opSyncStatus,
  "sync-authorize-collection": opSyncAuthorizeCollection,
  "sync-pin-peer": opSyncPinPeer,
  "sync-export": opSyncExport,
  "sync-export-pending": opSyncExportPending,
  "sync-mark-exported": opSyncMarkExported,
  "sync-import": opSyncImport,
};

self.onmessage = async (event) => {
  const { id, op, args } = event.data;
  try {
    const handler = OPS[op];
    if (handler === undefined) throw new Error(`unknown op: ${op}`);
    const payload = await handler(args ?? {});
    self.postMessage({ id, ok: true, ...payload });
  } catch (error) {
    self.postMessage({ id, ok: false, error: String(error?.message ?? error) });
  }
};
