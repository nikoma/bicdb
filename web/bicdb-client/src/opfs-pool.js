// OPFS sync-access-handle pool backing for the WASI filesystem.
//
// The problem this solves: FileSystemSyncAccessHandle gives synchronous
// read/write/flush (which BicDB's blocking WASI I/O needs), but *acquiring*
// a handle is async — impossible in the middle of a synchronous WASI
// syscall. So, following the sqlite-wasm "SAHPool" design, we pre-acquire a
// fixed pool of handles to opaque files (`pool/f0000`...) at boot, and map
// logical paths to pool files entirely in memory. Creating a file takes a
// handle from the free list; deleting returns it. The logical-name mapping
// persists in `pool/index` (written through its own sync handle), so the
// tree survives reloads.
//
// Deletion is deferred: the shim's path_rename is unlink-then-relink, so an
// inode leaving one directory may reappear in another within the same
// syscall. Deleted inodes go to a limbo set; `sweep()` (called by the worker
// after every wasm entry point) recycles the ones no longer reachable from
// the root and persists the index if the namespace changed. A crash between
// a rename and its sweep behaves like a lost rename — the same window a
// POSIX process has before its parent-directory fsync, which BicDB's
// recovery already tolerates.
//
// Capacity is fixed at boot (handles cannot be acquired mid-syscall). When
// the pool is exhausted, file creation fails with ENOSPC and the client
// surfaces "raise capacity". The default (128) is several times a typical
// working-set directory.

import {
  Directory,
  SyncOPFSFile,
  wasi,
} from "../vendor/browser_wasi_shim/index.js";

const POOL_DIR = "pool";
const INDEX_FILE = "index";
const POOL_PREFIX = "f";

class PoolFile extends SyncOPFSFile {
  constructor(handle, poolName) {
    super(handle);
    this.poolName = poolName;
  }
}

// contents Map that reports namespace mutations to the pool. The shim's
// OpenDirectory mutates `dir.contents` directly for link/unlink/rename, so
// this is the one reliable interception point.
class PoolContents extends Map {
  constructor(pool) {
    super();
    this.pool = pool;
  }

  set(name, inode) {
    const displaced = super.get(name);
    if (displaced !== undefined && displaced !== inode) {
      this.pool.limbo.add(displaced);
    }
    this.pool.limbo.delete(inode); // re-link half of a rename
    this.pool.dirty = true;
    return super.set(name, inode);
  }

  delete(name) {
    const inode = super.get(name);
    const had = super.delete(name);
    if (had) {
      this.pool.limbo.add(inode);
      this.pool.dirty = true;
    }
    return had;
  }
}

// Minimal stand-in for the shim's internal (unexported) Path: BicDB emits
// clean relative paths, so we only normalize and reject escapes.
function makePath(pathStr) {
  const is_dir = pathStr.endsWith("/");
  const parts = [];
  for (const part of pathStr.split("/")) {
    if (part === "" || part === ".") continue;
    if (part === "..") return null;
    parts.push(part);
  }
  return { parts, is_dir };
}

export class OpfsPoolDirectory extends Directory {
  constructor(pool) {
    super(new PoolContents(pool));
    this.pool = pool;
  }

  // Same contract as the shim's Directory.create_entry_for_path, but new
  // regular files come from the handle pool instead of RAM.
  create_entry_for_path(path_str, is_dir) {
    const path = makePath(path_str);
    if (path === null || path.parts.length === 0) {
      return { ret: wasi.ERRNO_INVAL, entry: null };
    }
    const { ret, parent_entry, filename, entry } =
      this.get_parent_dir_and_entry_for_path(path, true);
    if (parent_entry == null || filename == null) {
      return { ret, entry: null };
    }
    if (entry != null) {
      return { ret: wasi.ERRNO_EXIST, entry: null };
    }
    let child;
    if (is_dir) {
      child = new OpfsPoolDirectory(this.pool);
    } else {
      child = this.pool.allocateFile();
      if (child === null) {
        // Pool exhausted; the client maps this to a "raise capacity" error.
        return { ret: wasi.ERRNO_NOSPC, entry: null };
      }
    }
    parent_entry.contents.set(filename, child);
    return { ret: wasi.ERRNO_SUCCESS, entry: child };
  }
}

// Worker mount root: one WASI preopen (/data) whose children are the
// attached databases' pool roots (/data/<name> -> that database's own
// OpfsPool over its own OPFS directory). Databases attach and detach at
// runtime; the root itself refuses direct file creation so nothing can
// silently land in RAM outside a pool.
export class MountRoot extends Directory {
  constructor() {
    super(new Map());
  }

  // Creation routes into the owning mount's pool logic; creating new
  // top-level entries (i.e. outside any attached database) is refused, and
  // mkdir of an existing mount reports EEXIST so create_dir_all proceeds.
  create_entry_for_path(path_str, is_dir) {
    const parts = path_str.split("/").filter((p) => p !== "" && p !== ".");
    if (parts.length === 0 || parts.some((p) => p === "..")) {
      return { ret: wasi.ERRNO_ACCES, entry: null };
    }
    const mount = this.contents.get(parts[0]);
    if (mount === undefined) {
      return { ret: wasi.ERRNO_ACCES, entry: null };
    }
    if (parts.length === 1) {
      return { ret: wasi.ERRNO_EXIST, entry: null };
    }
    return mount.create_entry_for_path(parts.slice(1).join("/"), is_dir);
  }

  attach(name, poolRootDirectory) {
    this.contents.set(name, poolRootDirectory);
  }

  detach(name) {
    this.contents.delete(name);
  }
}

export class OpfsPool {
  constructor() {
    this.handles = new Map(); // poolName -> FileSystemSyncAccessHandle
    this.free = [];
    this.limbo = new Set();
    this.dirty = false;
    this.indexHandle = null;
    this.capacity = 0;
    this.root = new OpfsPoolDirectory(this);
  }

  // Async boot: the only place handles are acquired. `baseDir` is a
  // FileSystemDirectoryHandle (one per logical database).
  static async boot(baseDir, { capacity = 128 } = {}) {
    const pool = new OpfsPool();
    pool.capacity = capacity;
    const poolDir = await baseDir.getDirectoryHandle(POOL_DIR, { create: true });
    pool.indexHandle = await (
      await poolDir.getFileHandle(INDEX_FILE, { create: true })
    ).createSyncAccessHandle();

    let index = { files: {}, dirs: [] };
    const indexSize = pool.indexHandle.getSize();
    if (indexSize > 0) {
      const buf = new Uint8Array(indexSize);
      pool.indexHandle.read(buf, { at: 0 });
      try {
        index = JSON.parse(new TextDecoder().decode(buf));
      } catch {
        // Corrupt index = empty namespace; pool files get recycled below.
        // BicDB treats it as a fresh directory.
        index = { files: {}, dirs: [] };
      }
    }

    for (let i = 0; i < capacity; i++) {
      const name = POOL_PREFIX + String(i).padStart(4, "0");
      const handle = await (
        await poolDir.getFileHandle(name, { create: true })
      ).createSyncAccessHandle();
      pool.handles.set(name, handle);
    }

    for (const dirPath of index.dirs ?? []) {
      pool.ensureDir(dirPath);
    }
    const used = new Set();
    for (const [poolName, fullPath] of Object.entries(index.files ?? {})) {
      const handle = pool.handles.get(poolName);
      if (handle === undefined) continue; // capacity shrank; treat as lost
      const location = pool.ensureParent(fullPath);
      if (location === null) continue;
      used.add(poolName);
      location.dir.contents.set(location.name, new PoolFile(handle, poolName));
    }
    for (const [name, handle] of pool.handles) {
      if (!used.has(name)) {
        handle.truncate(0);
        pool.free.push(name);
      }
    }
    pool.free.sort().reverse(); // pop() hands out f0000 first — deterministic
    pool.limbo.clear();
    pool.dirty = false;
    return pool;
  }

  allocateFile() {
    const name = this.free.pop();
    if (name === undefined) return null;
    return new PoolFile(this.handles.get(name), name);
  }

  ensureDir(path) {
    let dir = this.root;
    const parsed = makePath(path);
    if (parsed === null) return null;
    for (const part of parsed.parts) {
      let next = dir.contents.get(part);
      if (next === undefined) {
        next = new OpfsPoolDirectory(this);
        dir.contents.set(part, next);
      }
      if (!(next instanceof Directory)) return null;
      dir = next;
    }
    return dir;
  }

  ensureParent(fullPath) {
    const parsed = makePath(fullPath);
    if (parsed === null || parsed.parts.length === 0) return null;
    const name = parsed.parts.pop();
    const dir = this.ensureDir(parsed.parts.join("/"));
    if (dir === null) return null;
    return { dir, name };
  }

  // Recycle unreachable limbo inodes and persist the namespace if it
  // changed. The worker calls this after every wasm entry point, so at most
  // one index write happens per call.
  sweep() {
    if (this.limbo.size > 0) {
      const reachable = new Set();
      const walk = (dir) => {
        for (const inode of dir.contents.values()) {
          reachable.add(inode);
          if (inode instanceof Directory) walk(inode);
        }
      };
      walk(this.root);
      for (const inode of this.limbo) {
        if (reachable.has(inode)) continue;
        if (inode instanceof PoolFile) {
          inode.handle.truncate(0);
          inode.handle.flush();
          this.free.push(inode.poolName);
        }
      }
      this.limbo.clear();
      this.dirty = true;
    }
    if (this.dirty) this.persistIndex();
  }

  persistIndex() {
    const index = { files: {}, dirs: [] };
    const walk = (dir, prefix) => {
      for (const [name, inode] of dir.contents) {
        const full = prefix === "" ? name : `${prefix}/${name}`;
        if (inode instanceof PoolFile) {
          index.files[inode.poolName] = full;
        } else if (inode instanceof Directory) {
          index.dirs.push(full);
          walk(inode, full);
        }
      }
    };
    walk(this.root, "");
    const bytes = new TextEncoder().encode(JSON.stringify(index));
    this.indexHandle.truncate(0);
    this.indexHandle.write(bytes, { at: 0 });
    this.indexHandle.flush();
    this.dirty = false;
  }

  stats() {
    let usedBytes = 0;
    for (const handle of this.handles.values()) usedBytes += handle.getSize();
    return {
      capacity: this.capacity,
      freeSlots: this.free.length,
      usedSlots: this.capacity - this.free.length,
      poolBytes: usedBytes,
    };
  }

  close() {
    this.sweep();
    for (const handle of this.handles.values()) {
      try {
        handle.flush();
        handle.close();
      } catch {
        // already closed
      }
    }
    try {
      this.indexHandle.close();
    } catch {
      // already closed
    }
  }
}
