//! C-ABI surface for running BicDB inside a browser Web Worker.
//!
//! The module is a wasip1 *reactor*: the host (web/bicdb-client's worker)
//! instantiates it, calls `_initialize`, then drives the exports below. All
//! file I/O goes through WASI imports, which the worker backs with OPFS
//! sync access handles.
//!
//! ## Calling convention
//!
//! Strings/buffers cross the boundary as (ptr, len) pairs in linear memory,
//! allocated with [`bicdb_alloc`] and released with [`bicdb_free`]. Every
//! entry point returns a pointer to a length-prefixed buffer — 4 bytes of
//! little-endian payload length, then that many bytes of UTF-8 JSON — which
//! the caller must read and then release with `bicdb_free(ptr, 4 + len)`:
//!
//! ```json
//! {"ok": true,  ...call-specific payload...}
//! {"ok": false, "error": "message"}
//! ```
//!
//! Panics are caught at the boundary and returned as error envelopes so a
//! bug in one call cannot trap the whole instance (worker restart = full
//! recovery pass, which is exactly what this layer is trying to avoid).

use std::cell::RefCell;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;

use bicdb_core::{
    BicDb, DbConfig, EncryptionConfig, EncryptionMode, KeySource, NodeId, SyncBundle,
    SyncCheckpoint,
};
use bicdb_sql::{SqlResult, SqlSession, SqlValue};
use serde::Deserialize;
use serde_json::json;

/// One open database plus its persistent sessions.
///
/// Sessions are `SqlSession::new_shared` handles — the same shared-session
/// model the pgwire server uses for its long-lived connections — created
/// over the boxed database and stored with an erased lifetime. Safety rests
/// on three invariants this module maintains:
/// 1. the `BicDb` lives in a `Box`, so its address is stable even when the
///    registry map rehashes;
/// 2. `sessions` is declared before `db`, so sessions drop first;
/// 3. wasm is single-threaded and every entry point runs to completion, so
///    a stored session is never *used* concurrently with anything else.
struct DbEntry {
    sessions: HashMap<u32, SqlSession<'static>>,
    next_session: u32,
    db: Box<BicDb>,
}

thread_local! {
    static DBS: RefCell<HashMap<u32, DbEntry>> = RefCell::new(HashMap::new());
    static NEXT_ID: RefCell<u32> = const { RefCell::new(1) };
}

/// Open options, passed as JSON. `fsync` defaults to true: an OPFS flush is
/// cheap and the browser can kill a worker at any time.
#[derive(Deserialize, Default)]
struct OpenConfig {
    fsync: Option<bool>,
    /// Record-audit events are the substrate sync bundles are built from;
    /// without them nothing syncs. Defaults ON for the browser cache (its
    /// whole point is a synced working set) — set false for a purely local
    /// scratch database to skip the event-log write amplification.
    audit_events: Option<bool>,
    /// Hex-encoded 32-byte raw encryption key (preferred in the browser:
    /// the app unwraps a server-delivered key via WebCrypto and passes it
    /// through; no KDF cost on a Chromebook).
    raw_key_hex: Option<String>,
    /// Argon2 passphrase alternative for raw_key_hex.
    passphrase: Option<String>,
}

// --- boundary plumbing ------------------------------------------------------

/// Allocate `len` bytes in linear memory for the host to write into.
#[no_mangle]
pub extern "C" fn bicdb_alloc(len: usize) -> *mut u8 {
    let mut buf = Vec::<u8>::with_capacity(len.max(1));
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

/// Release a buffer previously returned by [`bicdb_alloc`] or by an entry
/// point's envelope.
///
/// # Safety
/// `ptr`/`len` must come from this module's allocator and not be reused.
#[no_mangle]
pub unsafe extern "C" fn bicdb_free(ptr: *mut u8, len: usize) {
    if !ptr.is_null() {
        drop(Vec::from_raw_parts(ptr, len, len.max(1)));
    }
}

fn pack(payload: String) -> *mut u8 {
    let payload = payload.into_bytes();
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&payload);
    debug_assert_eq!(buf.len(), buf.capacity());
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

fn ok(payload: serde_json::Value) -> *mut u8 {
    let mut envelope = json!({ "ok": true });
    if let (Some(env_map), Some(extra)) = (envelope.as_object_mut(), payload.as_object()) {
        for (key, value) in extra {
            env_map.insert(key.clone(), value.clone());
        }
    }
    pack(envelope.to_string())
}

fn err(message: impl std::fmt::Display) -> *mut u8 {
    pack(json!({ "ok": false, "error": message.to_string() }).to_string())
}

/// Run `body`, converting both `Err` and panics into error envelopes.
fn guarded(body: impl FnOnce() -> Result<*mut u8, String>) -> *mut u8 {
    match std::panic::catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(envelope)) => envelope,
        Ok(Err(message)) => err(message),
        Err(panic) => {
            let message = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panic in bicdb-wasm".to_string());
            err(format!("panic: {message}"))
        }
    }
}

fn sql_result_json(result: SqlResult) -> Result<serde_json::Value, String> {
    let rows = result
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(sql_value_json)
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut encoded = json!({
        "columns": result.columns,
        "rows": rows,
    });
    let object = encoded.as_object_mut().expect("SQL result is an object");
    if let Some(command_tag) = result.command_tag {
        object.insert("command_tag".to_string(), json!(command_tag));
    }
    if !result.column_types.is_empty() {
        object.insert("column_types".to_string(), json!(result.column_types));
    }
    Ok(encoded)
}

fn sql_value_json(value: SqlValue) -> Result<serde_json::Value, String> {
    match value {
        SqlValue::Float(value) if value.is_nan() => Ok(json!("NaN")),
        SqlValue::Float(value) if value == f64::INFINITY => Ok(json!("Infinity")),
        SqlValue::Float(value) if value == f64::NEG_INFINITY => Ok(json!("-Infinity")),
        value => serde_json::to_value(value).map_err(|error| error.to_string()),
    }
}

/// # Safety
/// `ptr` must point at `len` valid bytes in linear memory.
unsafe fn slice_arg<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if ptr.is_null() || len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(ptr, len)
    }
}

fn utf8_arg(bytes: &[u8], what: &str) -> Result<String, String> {
    String::from_utf8(bytes.to_vec()).map_err(|_| format!("{what} is not valid UTF-8"))
}

// --- entry points ------------------------------------------------------------

/// Open (or recover) a database at `path`. Returns `{"ok":true,"db":<id>}`.
///
/// # Safety
/// Pointer arguments follow the module calling convention (see module docs).
#[no_mangle]
pub unsafe extern "C" fn bicdb_open(
    path_ptr: *const u8,
    path_len: usize,
    config_ptr: *const u8,
    config_len: usize,
) -> *mut u8 {
    let path_bytes = slice_arg(path_ptr, path_len);
    let config_bytes = slice_arg(config_ptr, config_len);
    guarded(move || {
        let path = utf8_arg(path_bytes, "path")?;
        let config: OpenConfig = if config_bytes.is_empty() {
            OpenConfig::default()
        } else {
            serde_json::from_slice(config_bytes)
                .map_err(|error| format!("invalid open config: {error}"))?
        };
        let db_config = DbConfig::default()
            .with_fsync(config.fsync.unwrap_or(true))
            .with_audit_events(config.audit_events.unwrap_or(true));

        let encryption = match (&config.raw_key_hex, &config.passphrase) {
            (Some(_), Some(_)) => {
                return Err("pass raw_key_hex or passphrase, not both".to_string())
            }
            (Some(key_hex), None) => {
                let key = hex::decode(key_hex)
                    .map_err(|error| format!("raw_key_hex is not valid hex: {error}"))?;
                Some(EncryptionConfig {
                    mode: EncryptionMode::Enabled,
                    key_source: KeySource::RawKey(key),
                    binding: None,
                })
            }
            (None, Some(passphrase)) => Some(EncryptionConfig {
                mode: EncryptionMode::Enabled,
                key_source: KeySource::Passphrase(passphrase.clone()),
                binding: None,
            }),
            (None, None) => None,
        };

        let db = match encryption {
            Some(encryption) => BicDb::open_with_encryption(&path, db_config, encryption),
            None => BicDb::open_with_config(&path, db_config),
        }
        .map_err(|error| error.to_string())?;

        let id = NEXT_ID.with(|next| {
            let mut next = next.borrow_mut();
            let id = *next;
            *next += 1;
            id
        });
        DBS.with(|dbs| {
            dbs.borrow_mut().insert(
                id,
                DbEntry {
                    sessions: HashMap::new(),
                    next_session: 1,
                    db: Box::new(db),
                },
            )
        });
        Ok(ok(json!({ "db": id })))
    })
}

fn with_db(
    id: u32,
    body: impl FnOnce(&mut BicDb) -> Result<*mut u8, String>,
) -> Result<*mut u8, String> {
    DBS.with(|dbs| {
        let mut dbs = dbs.borrow_mut();
        let entry = dbs
            .get_mut(&id)
            .ok_or_else(|| format!("no open database with id {id}"))?;
        body(&mut entry.db)
    })
}

fn with_entry(
    id: u32,
    body: impl FnOnce(&mut DbEntry) -> Result<*mut u8, String>,
) -> Result<*mut u8, String> {
    DBS.with(|dbs| {
        let mut dbs = dbs.borrow_mut();
        let entry = dbs
            .get_mut(&id)
            .ok_or_else(|| format!("no open database with id {id}"))?;
        body(entry)
    })
}

/// Execute one SQL statement. Returns
/// `{"ok":true,"result":{"columns":[...],"rows":[...],"command_tag":...}}`.
///
/// # Safety
/// Pointer arguments follow the module calling convention (see module docs).
#[no_mangle]
pub unsafe extern "C" fn bicdb_exec(db: u32, sql_ptr: *const u8, sql_len: usize) -> *mut u8 {
    let sql_bytes = slice_arg(sql_ptr, sql_len);
    guarded(move || {
        let sql = utf8_arg(sql_bytes, "sql")?;
        with_db(db, |db| {
            let mut session = SqlSession::new(db);
            let result = session.execute(&sql).map_err(|error| error.to_string())?;
            let result = sql_result_json(result)?;
            Ok(ok(json!({ "result": result })))
        })
    })
}

/// Event-horizon trim: drop record-audit history that no longer matters —
/// superseded events unconditionally, delete-events once the server has
/// acknowledged them (the engine's export watermark is the client's peer
/// horizon). Bounds the event log's growth; run it before compaction under
/// cache pressure. Returns `{"ok":true,"report":{...}}`.
#[no_mangle]
pub extern "C" fn bicdb_trim_events(db: u32) -> *mut u8 {
    guarded(move || {
        with_db(db, |db| {
            let acknowledged = db.sync_status().last_export_checkpoint;
            let report = db
                .trim_event_horizon(acknowledged)
                .map_err(|error| error.to_string())?;
            let report = serde_json::to_value(&report).map_err(|error| error.to_string())?;
            Ok(ok(json!({ "report": report })))
        })
    })
}

/// Compact the append-only log into fresh segments (quota reclamation).
#[no_mangle]
pub extern "C" fn bicdb_compact(db: u32) -> *mut u8 {
    guarded(move || {
        with_db(db, |db| {
            let report = db.compact().map_err(|error| error.to_string())?;
            let report = serde_json::to_value(&report).map_err(|error| error.to_string())?;
            Ok(ok(json!({ "report": report })))
        })
    })
}

/// Size/collection statistics, the input for quota monitoring.
#[no_mangle]
pub extern "C" fn bicdb_stats(db: u32) -> *mut u8 {
    guarded(move || {
        with_db(db, |db| {
            let stats = db.stats().map_err(|error| error.to_string())?;
            let stats = serde_json::to_value(&stats).map_err(|error| error.to_string())?;
            Ok(ok(json!({ "stats": stats })))
        })
    })
}

/// Open a persistent SQL session on a database. Unlike [`bicdb_exec`]
/// (which builds a throwaway session per statement), session state —
/// open transactions (`BEGIN`/`COMMIT`/`ROLLBACK`), savepoints, and
/// ordinary application GUCs (`SET app.locale = ...`) — survives across
/// calls until [`bicdb_session_close`]. Returns `{"ok":true,"session":id}`.
#[no_mangle]
pub extern "C" fn bicdb_session_open(db: u32) -> *mut u8 {
    guarded(move || {
        with_entry(db, |entry| {
            // Shared session over the boxed (address-stable) database; the
            // erased lifetime is governed by DbEntry's invariants.
            let db_ref: &BicDb = &entry.db;
            let session = SqlSession::new_shared(db_ref);
            let session: SqlSession<'static> = unsafe { std::mem::transmute(session) };
            let id = entry.next_session;
            entry.next_session += 1;
            entry.sessions.insert(id, session);
            Ok(ok(json!({ "session": id })))
        })
    })
}

/// Execute one statement on a persistent session.
///
/// # Safety
/// Pointer arguments follow the module calling convention (see module docs).
#[no_mangle]
pub unsafe extern "C" fn bicdb_session_exec(
    db: u32,
    session: u32,
    sql_ptr: *const u8,
    sql_len: usize,
) -> *mut u8 {
    let sql_bytes = slice_arg(sql_ptr, sql_len);
    guarded(move || {
        let sql = utf8_arg(sql_bytes, "sql")?;
        with_entry(db, |entry| {
            let session = entry
                .sessions
                .get_mut(&session)
                .ok_or_else(|| format!("no open session {session}"))?;
            let result = session.execute(&sql).map_err(|error| error.to_string())?;
            let result = sql_result_json(result)?;
            Ok(ok(json!({ "result": result })))
        })
    })
}

/// Close a persistent session (an open transaction is rolled back by the
/// session's drop path).
#[no_mangle]
pub extern "C" fn bicdb_session_close(db: u32, session: u32) -> *mut u8 {
    guarded(move || {
        with_entry(db, |entry| match entry.sessions.remove(&session) {
            Some(_) => Ok(ok(json!({}))),
            None => Err(format!("no open session {session}")),
        })
    })
}

/// Per-collection change generations — the invalidation signal for
/// reactive bounded queries (rerun a query only when a collection it
/// depends on changed). Returns `{"ok":true,"generations":{name:gen}}`.
#[no_mangle]
pub extern "C" fn bicdb_generations(db: u32) -> *mut u8 {
    guarded(move || {
        with_db(db, |db| {
            let mut generations = serde_json::Map::new();
            for collection in db.collections() {
                generations.insert(
                    collection.name.clone(),
                    json!(db.collection_generation(&collection.name)),
                );
            }
            Ok(ok(json!({ "generations": generations })))
        })
    })
}

/// The database's stable node identity (persisted in sync_state.json),
/// used to key sync checkpoints on the server.
#[no_mangle]
pub extern "C" fn bicdb_node_id(db: u32) -> *mut u8 {
    guarded(move || {
        with_db(db, |db| {
            Ok(ok(json!({ "node_id": db.node_id().to_string() })))
        })
    })
}

/// Return the local mesh identity that an authenticated control plane may
/// provision into a peer. The verifying key is public; possession of it grants
/// no signing or data access authority.
#[no_mangle]
pub extern "C" fn bicdb_mesh_identity(db: u32) -> *mut u8 {
    guarded(move || {
        with_db(db, |db| {
            let verifying_key = db.mesh_verifying_key().ok_or_else(|| {
                "mesh identity is unavailable because mesh signing is disabled".to_string()
            })?;
            Ok(ok(json!({
                "node_id": db.node_id().to_string(),
                "verifying_key": verifying_key,
            })))
        })
    })
}

/// Persist an out-of-band authenticated peer identity. This is deliberately a
/// pin, not an unauthenticated handshake: a conflicting key is refused, and
/// callers must obtain both values through their trusted control plane.
///
/// # Safety
/// Pointer arguments follow the module calling convention (see module docs).
#[no_mangle]
pub unsafe extern "C" fn bicdb_sync_pin_peer(
    db: u32,
    node_ptr: *const u8,
    node_len: usize,
    verifying_key_ptr: *const u8,
    verifying_key_len: usize,
) -> *mut u8 {
    let node_bytes = slice_arg(node_ptr, node_len);
    let verifying_key_bytes = slice_arg(verifying_key_ptr, verifying_key_len);
    guarded(move || {
        let node = utf8_arg(node_bytes, "peer node id")?
            .parse::<NodeId>()
            .map_err(|error| format!("peer node id is invalid: {error}"))?;
        let verifying_key = utf8_arg(verifying_key_bytes, "peer verifying key")?;
        with_db(db, |db| {
            db.pin_node_key(&node, &verifying_key)
                .map_err(|error| error.to_string())?;
            Ok(ok(json!({ "node_id": node.to_string() })))
        })
    })
}

/// Export a sync bundle of local events after `checkpoint` (a
/// `SyncCheckpoint` as JSON). Returns `{"ok":true,"bundle":<SyncBundle>}`;
/// the bundle's `event_count` may be 0. Checkpoint bookkeeping is the
/// caller's job (the JS sync loop mirrors bicdb-sync's SyncCoordinator).
///
/// # Safety
/// Pointer arguments follow the module calling convention (see module docs).
#[no_mangle]
pub unsafe extern "C" fn bicdb_sync_export(
    db: u32,
    checkpoint_ptr: *const u8,
    checkpoint_len: usize,
) -> *mut u8 {
    let checkpoint_bytes = slice_arg(checkpoint_ptr, checkpoint_len);
    guarded(move || {
        let checkpoint: SyncCheckpoint = if checkpoint_bytes.is_empty() {
            SyncCheckpoint::default()
        } else {
            serde_json::from_slice(checkpoint_bytes)
                .map_err(|error| format!("invalid sync checkpoint: {error}"))?
        };
        with_db(db, |db| {
            let bundle = db
                .export_sync_bundle_since(checkpoint)
                .map_err(|error| error.to_string())?;
            bundle_envelope(&bundle)
        })
    })
}

/// Sync status: node id, pending event count, and the engine-owned export
/// watermark (`last_export_checkpoint`). The watermark is persisted with
/// the database and reset by compaction alongside the event-offset rewrite,
/// so driving exports from it survives `bicdb_compact` — unlike an
/// externally stored offset.
#[no_mangle]
pub extern "C" fn bicdb_sync_status(db: u32) -> *mut u8 {
    guarded(move || {
        with_db(db, |db| {
            let status = db.sync_status();
            let status = serde_json::to_value(&status).map_err(|error| error.to_string())?;
            Ok(ok(json!({ "status": status })))
        })
    })
}

/// Explicitly opt an existing, unprotected collection into or out of mesh
/// synchronization. Collections fail closed by default: merely creating a
/// table never gives browser or peer sync authority over it, and protected
/// collections cannot be opted in through this API.
///
/// # Safety
/// Pointer arguments follow the module calling convention (see module docs).
#[no_mangle]
pub unsafe extern "C" fn bicdb_sync_authorize_collection(
    db: u32,
    collection_ptr: *const u8,
    collection_len: usize,
    enabled: u32,
) -> *mut u8 {
    let collection_bytes = slice_arg(collection_ptr, collection_len);
    guarded(move || {
        let collection = utf8_arg(collection_bytes, "collection")?;
        let enabled = match enabled {
            0 => false,
            1 => true,
            other => return Err(format!("enabled must be 0 or 1, got {other}")),
        };
        with_db(db, |db| {
            db.set_collection_mesh_sync_enabled(&collection, enabled)
                .map_err(|error| error.to_string())?;
            Ok(ok(json!({ "collection": collection, "enabled": enabled })))
        })
    })
}

// Bundles carry per-event payload hashes, so their JSON must cross the JS
// boundary byte-for-byte: JavaScript's JSON round-trip re-canonicalizes
// numbers (84.0 -> 84) and breaks verification. Exports therefore return the
// bundle as an opaque pre-serialized STRING (`bundle_json`) plus the
// bookkeeping fields the sync loop needs, and imports accept those exact
// bytes back.
fn bundle_envelope(bundle: &SyncBundle) -> Result<*mut u8, String> {
    let event_ids: Vec<String> = bundle
        .events
        .iter()
        .map(|entry| entry.envelope.event_id.to_string())
        .collect();
    let bundle_json = serde_json::to_string(bundle).map_err(|error| error.to_string())?;
    Ok(ok(json!({
        "bundle_json": bundle_json,
        "event_count": bundle.event_count,
        "next_checkpoint": bundle.next_checkpoint,
        "event_ids": event_ids,
    })))
}

/// Export every local event the engine has not yet marked exported (the
/// compact-safe push path). Follow a confirmed delivery with
/// [`bicdb_sync_mark_exported`]. Returns `bundle_json` (opaque — deliver
/// verbatim), `event_count`, `next_checkpoint`, and `event_ids`.
#[no_mangle]
pub extern "C" fn bicdb_sync_export_pending(db: u32) -> *mut u8 {
    guarded(move || {
        with_db(db, |db| {
            let since = db.sync_status().last_export_checkpoint;
            let bundle = db
                .export_sync_bundle_since(since)
                .map_err(|error| error.to_string())?;
            bundle_envelope(&bundle)
        })
    })
}

/// Advance the engine's export watermark after the server confirmed a push
/// (or to skip past just-imported events). Body: `{"next_checkpoint":
/// {"event_offset":N}, "event_count":N}`.
///
/// # Safety
/// Pointer arguments follow the module calling convention (see module docs).
#[no_mangle]
pub unsafe extern "C" fn bicdb_sync_mark_exported(
    db: u32,
    args_ptr: *const u8,
    args_len: usize,
) -> *mut u8 {
    #[derive(Deserialize)]
    struct MarkArgs {
        next_checkpoint: SyncCheckpoint,
        #[serde(default)]
        event_count: usize,
    }
    let args_bytes = slice_arg(args_ptr, args_len);
    guarded(move || {
        let args: MarkArgs = serde_json::from_slice(args_bytes)
            .map_err(|error| format!("invalid mark-exported args: {error}"))?;
        with_db(db, |db| {
            db.mark_sync_exported(args.next_checkpoint, args.event_count)
                .map_err(|error| error.to_string())?;
            Ok(ok(json!({})))
        })
    })
}

/// Import a sync bundle (a `SyncBundle` as JSON) pulled from the server.
/// Duplicate events are deduplicated by event id; record conflicts resolve
/// last-writer-wins. Returns `{"ok":true,"report":<SyncImportReport>}`.
///
/// # Safety
/// Pointer arguments follow the module calling convention (see module docs).
#[no_mangle]
pub unsafe extern "C" fn bicdb_sync_import(
    db: u32,
    bundle_ptr: *const u8,
    bundle_len: usize,
) -> *mut u8 {
    let bundle_bytes = slice_arg(bundle_ptr, bundle_len);
    guarded(move || {
        let bundle: SyncBundle = serde_json::from_slice(bundle_bytes)
            .map_err(|error| format!("invalid sync bundle: {error}"))?;
        with_db(db, |db| {
            let report = db
                .import_sync_bundle(bundle)
                .map_err(|error| error.to_string())?;
            let report = serde_json::to_value(&report).map_err(|error| error.to_string())?;
            Ok(ok(json!({ "report": report })))
        })
    })
}

/// Close a database, dropping it from the registry (flushes on drop).
#[no_mangle]
pub extern "C" fn bicdb_close(db: u32) -> *mut u8 {
    guarded(move || {
        let removed = DBS.with(|dbs| dbs.borrow_mut().remove(&db));
        match removed {
            Some(_) => Ok(ok(json!({}))),
            None => Err(format!("no open database with id {db}")),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call_json(ptr: *mut u8) -> serde_json::Value {
        let header = unsafe { std::slice::from_raw_parts(ptr, 4) };
        let len = u32::from_le_bytes(header.try_into().unwrap()) as usize;
        let bytes = unsafe { std::slice::from_raw_parts(ptr.add(4), len) }.to_vec();
        unsafe { bicdb_free(ptr, 4 + len) };
        serde_json::from_slice(&bytes).expect("envelope is JSON")
    }

    #[test]
    fn open_exec_stats_close_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db").to_string_lossy().into_owned();
        let opened =
            call_json(unsafe { bicdb_open(path.as_ptr(), path.len(), std::ptr::null(), 0) });
        assert_eq!(opened["ok"], true, "{opened}");
        let db = opened["db"].as_u64().unwrap() as u32;

        let create = "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)";
        let created = call_json(unsafe { bicdb_exec(db, create.as_ptr(), create.len()) });
        assert_eq!(created["ok"], true, "{created}");

        let insert = "INSERT INTO t (id, name) VALUES (1, 'chromebook')";
        let inserted = call_json(unsafe { bicdb_exec(db, insert.as_ptr(), insert.len()) });
        assert_eq!(inserted["ok"], true, "{inserted}");

        let select = "SELECT name FROM t WHERE id = 1";
        let selected = call_json(unsafe { bicdb_exec(db, select.as_ptr(), select.len()) });
        assert_eq!(selected["result"]["rows"][0][0], "chromebook", "{selected}");

        let stats = call_json(bicdb_stats(db));
        assert_eq!(stats["ok"], true, "{stats}");
        assert!(stats["stats"]["record_count"].as_u64().unwrap() >= 1);

        let bad = "SELECT FROM nowhere WHERE";
        let failed = call_json(unsafe { bicdb_exec(db, bad.as_ptr(), bad.len()) });
        assert_eq!(failed["ok"], false);

        assert_eq!(call_json(bicdb_close(db))["ok"], true);
        assert_eq!(
            call_json(bicdb_close(db))["ok"],
            false,
            "double close errors"
        );
    }

    fn open(dir: &std::path::Path, name: &str) -> u32 {
        let path = dir.join(name).to_string_lossy().into_owned();
        let opened =
            call_json(unsafe { bicdb_open(path.as_ptr(), path.len(), std::ptr::null(), 0) });
        assert_eq!(opened["ok"], true, "{opened}");
        opened["db"].as_u64().unwrap() as u32
    }

    fn exec(db: u32, sql: &str) -> serde_json::Value {
        let result = call_json(unsafe { bicdb_exec(db, sql.as_ptr(), sql.len()) });
        assert_eq!(result["ok"], true, "{sql}: {result}");
        result
    }

    fn authorize_sync(db: u32, collection: &str, enabled: bool) {
        let result = call_json(unsafe {
            bicdb_sync_authorize_collection(
                db,
                collection.as_ptr(),
                collection.len(),
                u32::from(enabled),
            )
        });
        assert_eq!(result["ok"], true, "{result}");
        assert_eq!(result["collection"], collection);
        assert_eq!(result["enabled"], enabled);
    }

    fn mesh_identity(db: u32) -> (String, String) {
        let result = call_json(bicdb_mesh_identity(db));
        assert_eq!(result["ok"], true, "{result}");
        (
            result["node_id"].as_str().unwrap().to_string(),
            result["verifying_key"].as_str().unwrap().to_string(),
        )
    }

    fn pin_peer(db: u32, peer_node_id: &str, peer_verifying_key: &str) {
        let result = call_json(unsafe {
            bicdb_sync_pin_peer(
                db,
                peer_node_id.as_ptr(),
                peer_node_id.len(),
                peer_verifying_key.as_ptr(),
                peer_verifying_key.len(),
            )
        });
        assert_eq!(result["ok"], true, "{result}");
        assert_eq!(result["node_id"], peer_node_id);
    }

    fn pin_each_other(left: u32, right: u32) {
        let (left_id, left_key) = mesh_identity(left);
        let (right_id, right_key) = mesh_identity(right);
        pin_peer(left, &right_id, &right_key);
        pin_peer(right, &left_id, &left_key);
    }

    #[test]
    fn mesh_authorization_and_identity_pins_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let source = open(dir.path(), "source");
        let target = open(dir.path(), "target");
        exec(
            source,
            "CREATE TABLE items (id INT PRIMARY KEY, value TEXT)",
        );
        exec(
            target,
            "CREATE TABLE items (id INT PRIMARY KEY, value TEXT)",
        );
        authorize_sync(source, "items", true);
        authorize_sync(target, "items", true);
        exec(source, "INSERT INTO items VALUES (1, 'signed')");

        let checkpoint = json!({ "event_offset": 0 }).to_string();
        let exported =
            call_json(unsafe { bicdb_sync_export(source, checkpoint.as_ptr(), checkpoint.len()) });
        assert_eq!(exported["ok"], true, "{exported}");
        assert!(exported["event_count"].as_u64().unwrap() > 0);
        let bundle = exported["bundle_json"].as_str().unwrap();

        // Collection authorization is not peer authentication: signed data
        // from an unpinned origin remains unacceptable.
        let refused =
            call_json(unsafe { bicdb_sync_import(target, bundle.as_ptr(), bundle.len()) });
        assert_eq!(refused["ok"], false, "{refused}");
        assert!(refused["error"]
            .as_str()
            .unwrap()
            .contains("unpinned origin"));

        let (source_id, source_key) = mesh_identity(source);
        let (_, target_key) = mesh_identity(target);
        pin_peer(target, &source_id, &source_key);
        let imported =
            call_json(unsafe { bicdb_sync_import(target, bundle.as_ptr(), bundle.len()) });
        assert_eq!(imported["ok"], true, "{imported}");

        // A pin is immutable until a future authenticated rotation protocol;
        // presenting another valid key for the same node is an alarm.
        let conflicting = call_json(unsafe {
            bicdb_sync_pin_peer(
                target,
                source_id.as_ptr(),
                source_id.len(),
                target_key.as_ptr(),
                target_key.len(),
            )
        });
        assert_eq!(conflicting["ok"], false, "{conflicting}");
        assert!(conflicting["error"]
            .as_str()
            .unwrap()
            .contains("conflicting with its pin"));

        authorize_sync(source, "items", false);
        let revoked =
            call_json(unsafe { bicdb_sync_export(source, checkpoint.as_ptr(), checkpoint.len()) });
        assert_eq!(revoked["event_count"], 0, "{revoked}");

        call_json(bicdb_close(source));
        call_json(bicdb_close(target));
    }

    // Move every event after `since` from one db to the other through the
    // same C-ABI surface the browser uses; returns the export's next
    // checkpoint so callers can advance their bookkeeping.
    fn relay(from: u32, to: u32, since: &serde_json::Value) -> serde_json::Value {
        let checkpoint = since.to_string();
        let exported =
            call_json(unsafe { bicdb_sync_export(from, checkpoint.as_ptr(), checkpoint.len()) });
        assert_eq!(exported["ok"], true, "{exported}");
        let bundle = exported["bundle_json"].as_str().unwrap();
        if exported["event_count"].as_u64().unwrap() > 0 {
            let imported =
                call_json(unsafe { bicdb_sync_import(to, bundle.as_ptr(), bundle.len()) });
            assert_eq!(imported["ok"], true, "{imported}");
        }
        exported["next_checkpoint"].clone()
    }

    #[test]
    fn sync_bundles_converge_two_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let a = open(dir.path(), "node-a");
        let b = open(dir.path(), "node-b");
        pin_each_other(a, b);
        assert_ne!(
            call_json(bicdb_node_id(a))["node_id"],
            call_json(bicdb_node_id(b))["node_id"],
            "distinct node identities"
        );

        exec(a, "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)");
        exec(b, "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)");
        authorize_sync(a, "notes", true);
        authorize_sync(b, "notes", true);
        exec(a, "INSERT INTO notes VALUES (1, 'from a')");
        let zero = serde_json::json!({ "event_offset": 0 });
        relay(a, b, &zero);

        let read_b = exec(b, "SELECT body FROM notes WHERE id = 1");
        assert_eq!(read_b["result"]["rows"][0][0], "from a");

        exec(b, "INSERT INTO notes VALUES (2, 'from b')");
        relay(b, a, &zero); // dedup makes replay-from-zero safe
        let read_a = exec(a, "SELECT COUNT(*) AS c FROM notes");
        assert_eq!(read_a["result"]["rows"][0][0], 2, "{read_a}");

        call_json(bicdb_close(a));
        call_json(bicdb_close(b));
    }

    #[test]
    fn typed_values_survive_browser_results_and_sync_bundles() {
        let dir = tempfile::tempdir().unwrap();
        let a = open(dir.path(), "typed-node-a");
        let b = open(dir.path(), "typed-node-b");
        pin_each_other(a, b);
        let schema = "CREATE TABLE typed_values (
                id TEXT PRIMARY KEY,
                amount NUMERIC,
                negative_zero FLOAT8,
                special FLOAT8,
                happened_at TIMESTAMPTZ,
                payload BYTEA,
                amounts NUMERIC[]
            )";
        exec(a, schema);
        exec(b, schema);
        authorize_sync(a, "typed_values", true);
        authorize_sync(b, "typed_values", true);
        exec(
            a,
            "INSERT INTO typed_values VALUES (
                'typed-1',
                '9007199254740993.0100'::numeric,
                '-0'::float8,
                'NaN'::float8,
                '2026-07-17 12:34:56.123456+00'::timestamptz,
                '\\x00ff10'::bytea,
                ARRAY['0.10'::numeric, '9007199254740993.01'::numeric]
            )",
        );

        let selected = exec(
            a,
            "SELECT amount, negative_zero, special, happened_at, payload, amounts FROM typed_values",
        );
        assert_eq!(selected["result"]["column_types"][0], "numeric");
        assert_eq!(selected["result"]["rows"][0][0], "9007199254740993.0100");
        assert_eq!(
            selected["result"]["rows"][0][1].as_f64().unwrap().to_bits(),
            (-0.0_f64).to_bits()
        );
        assert_eq!(selected["result"]["rows"][0][2], "NaN");
        assert_eq!(selected["result"]["rows"][0][4], "\\x00ff10");
        assert_eq!(
            selected["result"]["rows"][0][5],
            json!(["0.10", "9007199254740993.01"])
        );

        let zero = json!({ "event_offset": 0 });
        relay(a, b, &zero);
        let synced = exec(
            b,
            "SELECT amount, negative_zero, special, happened_at, payload, amounts FROM typed_values",
        );
        assert_eq!(synced["result"], selected["result"]);

        call_json(bicdb_close(a));
        call_json(bicdb_close(b));
    }

    fn session_exec(db: u32, session: u32, sql: &str) -> serde_json::Value {
        call_json(unsafe { bicdb_session_exec(db, session, sql.as_ptr(), sql.len()) })
    }

    // Persistent sessions: transactions and GUCs survive across calls —
    // the substrate BicUI's worker view sessions and any multi-statement
    // publisher need (A6.2).
    #[test]
    fn sessions_persist_transactions_and_gucs() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path(), "sessions");
        exec(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");

        let opened = call_json(bicdb_session_open(db));
        assert_eq!(opened["ok"], true, "{opened}");
        let s1 = opened["session"].as_u64().unwrap() as u32;

        // Ordinary application GUCs persist across separate exec calls.
        assert_eq!(
            session_exec(db, s1, "SET app.current_user = 'alice'")["ok"],
            true
        );
        let who = session_exec(
            db,
            s1,
            "SELECT current_setting('app.current_user', true) AS u",
        );
        assert_eq!(who["result"]["rows"][0][0], "alice", "{who}");
        let forged = session_exec(db, s1, "SET carrier.current_user = 'mallory'");
        assert_eq!(forged["ok"], false, "{forged}");

        // A transaction spans calls and rolls back atomically.
        assert_eq!(session_exec(db, s1, "BEGIN")["ok"], true);
        assert_eq!(
            session_exec(db, s1, "INSERT INTO t VALUES (1, 'doomed')")["ok"],
            true
        );
        assert_eq!(session_exec(db, s1, "ROLLBACK")["ok"], true);
        let after_rollback = exec(db, "SELECT COUNT(*) AS c FROM t");
        assert_eq!(
            after_rollback["result"]["rows"][0][0], 0,
            "{after_rollback}"
        );

        // ...and commits durably.
        assert_eq!(session_exec(db, s1, "BEGIN")["ok"], true);
        assert_eq!(
            session_exec(db, s1, "INSERT INTO t VALUES (2, 'kept')")["ok"],
            true
        );
        assert_eq!(session_exec(db, s1, "COMMIT")["ok"], true);
        let after_commit = exec(db, "SELECT v FROM t WHERE id = 2");
        assert_eq!(after_commit["result"]["rows"][0][0], "kept");

        // Sessions are isolated: a second session has its own GUCs.
        let s2 = call_json(bicdb_session_open(db))["session"]
            .as_u64()
            .unwrap() as u32;
        let who2 = session_exec(
            db,
            s2,
            "SELECT current_setting('app.current_user', true) AS u",
        );
        assert_ne!(who2["result"]["rows"][0][0], "alice", "{who2}");

        assert_eq!(call_json(bicdb_session_close(db, s1))["ok"], true);
        assert_eq!(
            call_json(bicdb_session_close(db, s1))["ok"],
            false,
            "double close errors"
        );
        assert_eq!(call_json(bicdb_session_close(db, s2))["ok"], true);
        call_json(bicdb_close(db));
    }

    // Change generations bump when (and only when) a collection changes —
    // the reactive-query invalidation signal (A6.3).
    #[test]
    fn generations_report_collection_changes() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path(), "generations");
        exec(db, "CREATE TABLE a (id INT PRIMARY KEY)");
        exec(db, "CREATE TABLE b (id INT PRIMARY KEY)");

        let before = call_json(bicdb_generations(db));
        assert_eq!(before["ok"], true, "{before}");
        exec(db, "INSERT INTO a VALUES (1)");
        let after = call_json(bicdb_generations(db));

        let gen = |v: &serde_json::Value, name: &str| v["generations"][name].as_u64().unwrap();
        assert!(
            gen(&after, "a") > gen(&before, "a"),
            "a changed: {before} -> {after}"
        );
        assert_eq!(gen(&after, "b"), gen(&before, "b"), "b untouched");
        call_json(bicdb_close(db));
    }

    // The push path must survive compaction: compact() rewrites event
    // offsets and resets the engine's export watermark, so export-pending
    // re-sends from zero (dedup absorbs it) and never strands new writes.
    #[test]
    fn export_pending_survives_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let a = open(dir.path(), "node-a");
        let b = open(dir.path(), "node-b");
        pin_each_other(a, b);

        exec(a, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        exec(b, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)");
        authorize_sync(a, "t", true);
        authorize_sync(b, "t", true);
        exec(a, "INSERT INTO t VALUES (1, 'before compact')");

        let pending = call_json(bicdb_sync_export_pending(a));
        let count = pending["event_count"].as_u64().unwrap();
        assert!(count > 0, "{pending}");
        let mark = serde_json::json!({
            "next_checkpoint": pending["next_checkpoint"],
            "event_count": count,
        })
        .to_string();
        let marked = call_json(unsafe { bicdb_sync_mark_exported(a, mark.as_ptr(), mark.len()) });
        assert_eq!(marked["ok"], true, "{marked}");
        let drained = call_json(bicdb_sync_export_pending(a));
        assert_eq!(
            drained["event_count"], 0,
            "nothing pending after mark: {drained}"
        );

        assert_eq!(call_json(bicdb_compact(a))["ok"], true);
        exec(a, "INSERT INTO t VALUES (2, 'after compact')");

        // Post-compact pending export must include the new write (it will
        // also re-include pre-compact events; the importer dedups those).
        let pending = call_json(bicdb_sync_export_pending(a));
        let bundle = pending["bundle_json"].as_str().unwrap();
        let imported = call_json(unsafe { bicdb_sync_import(b, bundle.as_ptr(), bundle.len()) });
        assert_eq!(imported["ok"], true, "{imported}");
        let rows = exec(b, "SELECT v FROM t ORDER BY id");
        assert_eq!(rows["result"]["rows"][0][0], "before compact");
        assert_eq!(rows["result"]["rows"][1][0], "after compact", "{rows}");

        call_json(bicdb_close(a));
        call_json(bicdb_close(b));
    }
}
