//! HTTP sync server: the server side of the browser working-set cache
//! (docs/wasm-browser-cache-todo.md Phase 2, docs/browser-sync.md).
//!
//! Each *scope* is one working set — typically one per user — stored as a
//! normal BicDB directory under `<root>/<scope>/db`. The server is itself a
//! sync node: a client push is imported into the scope database immediately,
//! and a pull exports the scope database's events since the client's last
//! recorded server checkpoint. That means bundles never accumulate
//! server-side, a brand-new device bootstraps by pulling the working set
//! from offset 0, and changes fan out between clients through the server's
//! own event log (dedup by event id, per-record last-writer-wins).
//!
//! Composition — which rows belong in a scope — happens through the
//! `POST /v1/<scope>/sql` admin endpoint (enabled only when `--admin-token`
//! is set): the Hub backend writes course/chat/appointment rows into scope
//! databases exactly like any other SQL client, and connected browsers see
//! them on their next pull. Do NOT open scope directories with a second
//! process while the server runs.
//!
//! The HTTP layer is deliberately minimal (HTTP/1.1, `Connection: close`,
//! thread per connection, JSON bodies) in the spirit of the pgwire/RESP
//! servers: no framework dependency. Auth is a static bearer token
//! (`--token`); wire it to real Hub sessions before exposing beyond
//! localhost — and use TLS termination in front (this listener is
//! plaintext).
//!
//! ## Routes (all JSON)
//! - `GET  /healthz` — liveness, no auth
//! - `GET  /v1/<scope>/checkpoint/<node-uuid>` — client's sync checkpoint
//! - `PUT  /v1/<scope>/checkpoint/<node-uuid>` — save it
//! - `POST /v1/<scope>/push` — body `SyncBundle`; imports into the scope db
//! - `POST /v1/<scope>/pull` — body `{node_id, checkpoint}`; returns
//!   `{server_node_id, bundles}` (the pulling client's own events are
//!   filtered out server-side)
//! - `POST /v1/<scope>/sql` — body `{sql}`; admin token required

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bicdb_core::{BicDb, DbConfig, NodeId, SecurityContext, SyncBundle, SyncCheckpoint};
use bicdb_sql::SqlSession;
use bicdb_sync::{ClientSyncCheckpoint, PushBundleReport};
use serde_json::{json, Value};

const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

pub struct SyncServeConfig {
    pub root: PathBuf,
    pub host: String,
    pub port: u16,
    pub token: Option<String>,
    pub admin_token: Option<String>,
    pub fsync: bool,
    pub cors_origin: String,
    pub retention: Option<RetentionConfig>,
    pub retention_interval_seconds: u64,
    pub rls_compose: Option<RlsComposeConfig>,
    /// Enable event-horizon trimming in the sweep: per scope, drop
    /// superseded record-audit events unconditionally and delete-events once
    /// every tracked client checkpoint has pulled past them.
    pub event_horizon: bool,
    /// Client checkpoints untouched for longer than this are ignored when
    /// computing the horizon (and reset afterwards): a device that stale
    /// re-bootstraps on its next sync. Rows it deleted-but-never-synced can
    /// resurrect — the documented horizon trade-off.
    pub horizon_max_checkpoint_age_seconds: u64,
    /// Direction policy: collections clients may NOT write. A push whose
    /// bundle carries any event for one of these collections is rejected
    /// whole (fail closed) — the substrate rule for server-authoritative
    /// synced content (BicUI control planes, catalogs, announcements).
    pub server_write_only: Vec<String>,
}

/// Multi-tenant mode: ONE master database carries every user's rows plus the
/// PostgreSQL-style RLS policies that say who sees what; per-user scopes
/// become materialized RLS views the composer maintains automatically.
///
/// Each tick, per user scope (`<user_scope_prefix><userid>`):
///
/// 1. **Reverse pass** (device → master): client-originated events in the
///    scope's log are replayed onto the master *as that user* using a
///    host-created immutable security context, so `WITH CHECK` / `USING`
///    policies authorize every write. A rejected write is simply skipped —
///    the forward pass then reverts the scope row, and the device converges
///    back to the authorized state.
/// 2. **Forward pass** (master → scope): `SELECT *` per table as the user
///    (RLS filters), diffed against the scope, applied as inserts/updates/
///    deletes. Grants appear, revocations become deletes — visibility changes
///    need no checkpoint surgery because the scope is state, not a stream.
///
/// The reverse pass rides the scope database's engine export watermark
/// (compact-safe, replays are idempotent upserts/deletes).
#[derive(Clone, Debug, serde::Deserialize)]
pub struct RlsComposeConfig {
    /// Scope name of the master multi-tenant database (seed it and manage
    /// policies through the admin SQL endpoint).
    #[serde(default = "default_master_scope")]
    pub master_scope: String,
    /// User scopes are `<prefix><userid>`; the composer derives the GUC
    /// user id from the scope name.
    #[serde(default = "default_user_prefix")]
    pub user_scope_prefix: String,
    /// Value for `bicdb.current_tenant` (defaults to the user id).
    #[serde(default)]
    pub tenant: Option<String>,
    /// Tables to compose. Every listed table syncs; visibility is whatever
    /// the master's RLS policies say (a table without policies is visible
    /// to everyone — announcements, course catalogs).
    pub tables: Vec<ComposeTable>,
    #[serde(default = "default_compose_interval")]
    pub interval_seconds: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct ComposeTable {
    pub table: String,
    /// Primary-key column used for diffing and row addressing.
    pub pk: String,
}

fn default_master_scope() -> String {
    "master".to_string()
}
fn default_user_prefix() -> String {
    "user-".to_string()
}
fn default_compose_interval() -> u64 {
    5
}

/// Server-controlled retention: age out rows so working sets stay bounded
/// everywhere. Rules delete on the server; the deletes ride the normal sync
/// stream to every device, and the sweep compacts the scope database
/// afterwards (resetting stored client pull-checkpoints, since compaction
/// rewrites event offsets — clients then re-bootstrap incrementally via
/// event-id dedup).
#[derive(Clone, Debug, Default, serde::Deserialize)]
pub struct RetentionConfig {
    /// Rules applied to every scope.
    #[serde(default)]
    pub defaults: Vec<RetentionRule>,
    /// Extra rules per scope name.
    #[serde(default)]
    pub scopes: HashMap<String, Vec<RetentionRule>>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct RetentionRule {
    pub table: String,
    /// Numeric timestamp column the age check runs against.
    pub column: String,
    pub max_age_seconds: u64,
    /// "epoch_seconds" (default) or "epoch_millis".
    #[serde(default)]
    pub column_unit: Option<String>,
}

struct ServerState {
    config: SyncServeConfig,
    scopes: Mutex<HashMap<String, Arc<Mutex<BicDb>>>>,
}

pub fn serve_blocking(config: SyncServeConfig) -> anyhow::Result<()> {
    let listener = TcpListener::bind((config.config_host(), config.port))?;
    let local = listener.local_addr()?;
    let auth = if config.token.is_some() { "on" } else { "off" };
    let admin = if config.admin_token.is_some() {
        "on"
    } else {
        "off"
    };
    // The e2e harness parses this line to find the bound port; keep the
    // "listening on" phrasing stable.
    println!(
        "bicdb sync server (HTTP) listening on {local} (auth {auth}, admin-sql {admin}, root {})",
        config.root.display()
    );
    let state = Arc::new(ServerState {
        config,
        scopes: Mutex::new(HashMap::new()),
    });
    if state.config.retention.is_some() || state.config.event_horizon {
        let sweeper = Arc::clone(&state);
        let interval = Duration::from_secs(state.config.retention_interval_seconds.max(1));
        std::thread::spawn(move || loop {
            if let Err(error) = retention_sweep(&sweeper) {
                eprintln!("retention sweep failed: {error}");
            }
            if sweeper.config.event_horizon {
                if let Err(error) = horizon_sweep(&sweeper) {
                    eprintln!("event-horizon sweep failed: {error}");
                }
            }
            std::thread::sleep(interval);
        });
    }
    if let Some(compose) = state.config.rls_compose.clone() {
        let composer = Arc::clone(&state);
        let interval = Duration::from_secs(compose.interval_seconds.max(1));
        std::thread::spawn(move || loop {
            compose_tick(&composer, &compose);
            std::thread::sleep(interval);
        });
    }
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let state = Arc::clone(&state);
        std::thread::spawn(move || {
            let _ = handle_connection(stream, &state);
        });
    }
    Ok(())
}

impl SyncServeConfig {
    fn config_host(&self) -> &str {
        self.host.as_str()
    }
}

struct Request {
    method: String,
    path: String,
    bearer: Option<String>,
    body: Vec<u8>,
}

fn handle_connection(stream: TcpStream, state: &ServerState) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let request = match read_request(&mut reader) {
        Ok(request) => request,
        Err(message) => return respond(stream, 400, &json!({ "error": message }), state),
    };
    if request.method == "OPTIONS" {
        return respond_raw(stream, 204, "", state);
    }
    let (status, body) = route(&request, state);
    respond(stream, status, &body, state)
}

fn read_request(reader: &mut BufReader<TcpStream>) -> Result<Request, String> {
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|error| error.to_string())?;
    let mut parts = line.split_whitespace();
    let method = parts.next().ok_or("missing method")?.to_uppercase();
    let path = parts.next().ok_or("missing path")?.to_string();

    let mut content_length = 0_usize;
    let mut bearer = None;
    let mut header_bytes = line.len();
    loop {
        let mut header = String::new();
        reader
            .read_line(&mut header)
            .map_err(|error| error.to_string())?;
        header_bytes += header.len();
        if header_bytes > MAX_HEADER_BYTES {
            return Err("headers too large".to_string());
        }
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        let Some((name, value)) = header.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.to_ascii_lowercase().as_str() {
            "content-length" => {
                content_length = value.parse().map_err(|_| "bad content-length")?;
            }
            "authorization" => {
                bearer = value
                    .strip_prefix("Bearer ")
                    .or_else(|| value.strip_prefix("bearer "))
                    .map(str::to_string);
            }
            _ => {}
        }
    }
    if content_length > MAX_BODY_BYTES {
        return Err("body too large".to_string());
    }
    let mut body = vec![0_u8; content_length];
    reader
        .read_exact(&mut body)
        .map_err(|error| error.to_string())?;
    Ok(Request {
        method,
        path,
        bearer,
        body,
    })
}

fn route(request: &Request, state: &ServerState) -> (u16, Value) {
    if request.method == "GET" && request.path == "/healthz" {
        return (200, json!({ "ok": true }));
    }

    let segments: Vec<&str> = request.path.trim_start_matches('/').split('/').collect();
    if segments.first() != Some(&"v1") || segments.len() < 3 {
        return (404, json!({ "error": "not found" }));
    }
    let scope = segments[1];
    if !valid_scope(scope) {
        return (400, json!({ "error": "invalid scope name" }));
    }
    let action = segments[2];

    // /sql is gated by the admin token alone; everything else by --token.
    if action == "sql" {
        match &state.config.admin_token {
            Some(admin) if request.bearer.as_deref() == Some(admin.as_str()) => {}
            Some(_) => return (401, json!({ "error": "bad admin token" })),
            None => return (404, json!({ "error": "admin sql disabled" })),
        }
    } else if let Some(token) = &state.config.token {
        if request.bearer.as_deref() != Some(token.as_str()) {
            return (401, json!({ "error": "bad token" }));
        }
    }

    match (request.method.as_str(), action, segments.len()) {
        ("GET", "checkpoint", 4) => load_checkpoint(state, scope, segments[3]),
        ("PUT", "checkpoint", 4) => save_checkpoint(state, scope, segments[3], &request.body),
        ("POST", "push", 3) => push_bundle(state, scope, &request.body),
        ("POST", "pull", 3) => pull_bundles(state, scope, &request.body),
        ("POST", "telemetry", 3) => record_telemetry(state, scope, &request.body),
        ("POST", "sql", 3) => admin_sql(state, scope, &request.body),
        _ => (404, json!({ "error": "not found" })),
    }
}

// --- retention ---------------------------------------------------------------

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn retention_sweep(state: &ServerState) -> Result<(), String> {
    let Some(retention) = &state.config.retention else {
        return Ok(());
    };
    let entries = match std::fs::read_dir(&state.config.root) {
        Ok(entries) => entries,
        Err(_) => return Ok(()), // root not created yet: nothing to sweep
    };
    for entry in entries.flatten() {
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let scope = entry.file_name().to_string_lossy().into_owned();
        if !valid_scope(&scope) || !entry.path().join("db").exists() {
            continue;
        }
        let mut rules: Vec<&RetentionRule> = retention.defaults.iter().collect();
        if let Some(extra) = retention.scopes.get(&scope) {
            rules.extend(extra.iter());
        }
        if rules.is_empty() {
            continue;
        }
        sweep_scope(state, &scope, &rules)?;
    }
    Ok(())
}

fn sweep_scope(state: &ServerState, scope: &str, rules: &[&RetentionRule]) -> Result<(), String> {
    let db = scope_db(state, scope)?;
    let mut db = db.lock().expect("scope db lock");
    let now = unix_now();
    let mut deleted_any = false;
    for rule in rules {
        let cutoff = match rule.column_unit.as_deref() {
            None | Some("epoch_seconds") => now.saturating_sub(rule.max_age_seconds) as i64,
            Some("epoch_millis") => {
                (now.saturating_sub(rule.max_age_seconds) as i64).saturating_mul(1000)
            }
            Some(other) => {
                eprintln!("retention: unknown column_unit `{other}`, skipping rule");
                continue;
            }
        };
        let sql = format!(
            "DELETE FROM {} WHERE {} < {}",
            rule.table, rule.column, cutoff
        );
        let mut session = SqlSession::new(&mut db);
        match session.execute(&sql) {
            Ok(result) => {
                let deleted = result
                    .command_tag
                    .as_deref()
                    .and_then(|tag| tag.rsplit(' ').next())
                    .and_then(|count| count.parse::<u64>().ok())
                    .unwrap_or(0);
                if deleted > 0 {
                    deleted_any = true;
                    println!(
                        "retention: {scope}: deleted {deleted} rows from {}",
                        rule.table
                    );
                }
            }
            // A scope legitimately may not have this table (yet).
            Err(_) => continue,
        }
    }
    if deleted_any {
        let server_node = db.node_id();
        db.compact().map_err(|error| error.to_string())?;
        drop(db);
        reset_pull_checkpoints(state, scope, &server_node)?;
    }
    Ok(())
}

// Compaction rewrites event offsets, so every stored client checkpoint's
// watermark for THIS server becomes meaningless. Reset them to zero: the
// next pull re-sends the (now compacted) working set and the client's
// event-id dedup absorbs everything it already has.
fn reset_pull_checkpoints(
    state: &ServerState,
    scope: &str,
    server_node: &NodeId,
) -> Result<(), String> {
    let dir = state.config.root.join(scope).join("checkpoints");
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(_) => return Ok(()),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let Ok(mut checkpoint) = serde_json::from_slice::<ClientSyncCheckpoint>(&bytes) else {
            continue;
        };
        checkpoint
            .remote_imports
            .insert(server_node.to_string(), SyncCheckpoint::default());
        if let Ok(bytes) = serde_json::to_vec_pretty(&checkpoint) {
            let tmp = path.with_extension("json.tmp");
            if std::fs::write(&tmp, bytes).is_ok() {
                let _ = std::fs::rename(&tmp, &path);
            }
        }
    }
    Ok(())
}

// --- RLS composition -----------------------------------------------------------

fn sql_literal(value: &bicdb_sql::SqlValue) -> String {
    use bicdb_sql::SqlValue;
    match value {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        SqlValue::Int(n) => n.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => {
            // Json/Geometry round-trip as their text form.
            let text = serde_json::to_string(other).unwrap_or_default();
            format!("'{}'", text.trim_matches('"').replace('\'', "''"))
        }
    }
}

fn json_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}

// Open a host-authorized session for the composer identity. The composer, not
// client SQL, derives this context from the configured scope-to-user mapping.
fn user_session<'db>(
    db: &'db mut BicDb,
    userid: &str,
    tenant: &Option<String>,
) -> Result<SqlSession<'db>, String> {
    let tenant = tenant.clone().unwrap_or_else(|| userid.to_string());
    // A REPLAYED end-user principal, not a system one. This used to inherit
    // `AuthenticationStrength::Internal` from `SecurityContext::new`, so every
    // replayed user session presented itself to RLS as the strongest principal
    // in the system — because an internal worker happened to be what executed
    // it. Running inside the server is not the same as being authenticated as
    // the server.
    //
    // The sync bundle does not currently carry the strength of the session
    // that authorized the write, so this stays at the weakest value. When the
    // replay format can propagate it, switch to
    // `SecurityContext::authenticated(userid, tenant, originating_strength)` —
    // do NOT infer a strength from the fact that execution is in-process.
    Ok(SqlSession::new_secure(
        db,
        SecurityContext::new(userid, tenant),
    ))
}

fn compose_tick(state: &ServerState, compose: &RlsComposeConfig) {
    let entries = match std::fs::read_dir(&state.config.root) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let scope = entry.file_name().to_string_lossy().into_owned();
        let Some(userid) = scope.strip_prefix(&compose.user_scope_prefix) else {
            continue;
        };
        if userid.is_empty() || !valid_scope(&scope) || !entry.path().join("db").exists() {
            continue;
        }
        if let Err(error) = compose_user(state, compose, &scope, userid) {
            eprintln!("compose {scope}: {error}");
        }
    }
}

fn compose_user(
    state: &ServerState,
    compose: &RlsComposeConfig,
    scope: &str,
    userid: &str,
) -> Result<(), String> {
    let master = scope_db(state, &compose.master_scope)?;
    let scope_handle = scope_db(state, scope)?;

    // ---- reverse pass: client-originated events -> master, as the user ----
    // Locks are taken one database at a time, never nested.
    let (bundle, scope_node) = {
        let mut db = scope_handle.lock().expect("scope db lock");
        let since = db.sync_status().last_export_checkpoint;
        let bundle = db
            .export_sync_bundle_since(since)
            .map_err(|error| error.to_string())?;
        (bundle, db.node_id())
    };
    if bundle.event_count > 0 {
        let mut db = master.lock().expect("master db lock");
        let mut session = user_session(&mut db, userid, &compose.tenant)?;
        for entry in &bundle.events {
            if entry.envelope.node_id == scope_node {
                continue; // the composer's own forward-pass writes
            }
            if let Err(error) = reverse_apply(&mut session, compose, &entry.event) {
                // Typically an RLS WITH CHECK rejection: skip; the forward
                // pass reverts the scope row and the device converges back.
                eprintln!("compose {scope}: rejected client write: {error}");
            }
        }
        drop(session);
        drop(db);
        let mut db = scope_handle.lock().expect("scope db lock");
        db.mark_sync_exported(bundle.next_checkpoint, bundle.event_count)
            .map_err(|error| error.to_string())?;
    }

    // ---- forward pass: master's RLS view of each table -> scope ------------
    for table in &compose.tables {
        let visible = {
            let mut db = master.lock().expect("master db lock");
            let mut session = user_session(&mut db, userid, &compose.tenant)?;
            session
                .execute(&format!("SELECT * FROM {}", table.table))
                .map_err(|error| format!("SELECT {} as {userid}: {error}", table.table))?
        };
        let mut db = scope_handle.lock().expect("scope db lock");
        forward_sync(&mut db, table, &visible)?;
    }
    Ok(())
}

/// Column names in a replayed sync event come from the client's record
/// metadata, so they are fully attacker-controlled by a compromised device.
/// They were interpolated into the composed UPDATE/INSERT verbatim while only
/// the VALUES were escaped, so a metadata key like `v = 'x' WHERE 1=1 --`
/// rewrote the statement — widening the WHERE to every row the session's RLS
/// admits, or injecting into the INSERT column list.
///
/// Validated rather than quoted: a strict identifier cannot contain a quote,
/// space, comma, semicolon or comment marker, so there is nothing left to
/// escape, and unlike double-quoting it does not silently change these
/// identifiers from case-folded to case-sensitive.
fn validate_sync_identifier(name: &str) -> Result<&str, String> {
    const MAX_IDENTIFIER_LEN: usize = 63;
    let valid = !name.is_empty()
        && name.len() <= MAX_IDENTIFIER_LEN
        && name
            .chars()
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if valid {
        Ok(name)
    } else {
        Err(format!(
            "sync event column name is not a plain identifier: {name:?}"
        ))
    }
}

// Replay one client audit event onto the master under the user's session.
// Upserts try UPDATE first and fall back to INSERT; deletes are direct. RLS
// USING/WITH CHECK policies authorize (or reject) each statement.
fn reverse_apply(
    session: &mut SqlSession<'_>,
    compose: &RlsComposeConfig,
    event: &bicdb_core::Event,
) -> Result<(), String> {
    if !matches!(
        event.event_type.as_str(),
        "RecordCreated" | "RecordUpdated" | "RecordDeleted"
    ) {
        return Ok(());
    }
    let collection = event
        .payload
        .get("collection")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let Some(table) = compose.tables.iter().find(|t| t.table == collection) else {
        return Ok(()); // not a composed table (e.g. client-local bookkeeping)
    };
    let record = event.payload.get("record").ok_or("event has no record")?;
    let columns = record
        .get("metadata")
        .and_then(Value::as_object)
        .ok_or("record has no column map")?;
    // SQL rows use the pk as the record identity (stringified); non-pk
    // columns live in metadata. Numeric-looking ids are emitted untyped so
    // INT/BIGINT keys compare correctly; anything else is a text literal.
    let record_id = event
        .payload
        .get("record_id")
        .and_then(Value::as_str)
        .ok_or("event has no record_id")?;
    let pk_literal = if record_id.parse::<i64>().is_ok() || record_id.parse::<f64>().is_ok() {
        record_id.to_string()
    } else {
        format!("'{}'", record_id.replace('\'', "''"))
    };

    if event.event_type == "RecordDeleted" {
        session
            .execute(&format!(
                "DELETE FROM {} WHERE {} = {}",
                table.table, table.pk, pk_literal
            ))
            .map_err(|error| error.to_string())?;
        return Ok(());
    }

    let assignments: Vec<String> = columns
        .iter()
        .filter(|(name, _)| !name.starts_with('_') && *name != &table.pk)
        .map(|(name, value)| {
            validate_sync_identifier(name).map(|name| format!("{name} = {}", json_literal(value)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let updated = if assignments.is_empty() {
        0
    } else {
        let result = session
            .execute(&format!(
                "UPDATE {} SET {} WHERE {} = {}",
                table.table,
                assignments.join(", "),
                table.pk,
                pk_literal
            ))
            .map_err(|error| error.to_string())?;
        command_tag_count(&result)
    };
    if updated == 0 {
        let (mut names, mut values): (Vec<&str>, Vec<String>) = columns
            .iter()
            .filter(|(name, _)| !name.starts_with('_') && *name != &table.pk)
            .map(|(name, value)| {
                validate_sync_identifier(name).map(|name| (name, json_literal(value)))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .unzip();
        names.insert(0, table.pk.as_str());
        values.insert(0, pk_literal);
        session
            .execute(&format!(
                "INSERT INTO {} ({}) VALUES ({})",
                table.table,
                names.join(", "),
                values.join(", ")
            ))
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn command_tag_count(result: &bicdb_sql::SqlResult) -> u64 {
    result
        .command_tag
        .as_deref()
        .and_then(|tag| tag.rsplit(' ').next())
        .and_then(|count| count.parse().ok())
        .unwrap_or(0)
}

// Diff the user's RLS-visible rows against the scope table and apply the
// difference. The scope carries no policies — it *is* the policy's output.
fn forward_sync(
    db: &mut BicDb,
    table: &ComposeTable,
    visible: &bicdb_sql::SqlResult,
) -> Result<(), String> {
    let Some(pk_index) = visible.columns.iter().position(|c| c == &table.pk) else {
        return Err(format!("{} has no pk column {}", table.table, table.pk));
    };
    let mut session = SqlSession::new(db);

    // Ensure the scope table exists, typed from the master's plan metadata.
    let create_columns: Vec<String> = visible
        .columns
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let sql_type = visible
                .column_types
                .get(i)
                .and_then(|t| t.clone())
                .unwrap_or_else(|| "text".to_string());
            let pk = if i == pk_index { " PRIMARY KEY" } else { "" };
            format!("{name} {sql_type}{pk}")
        })
        .collect();
    session
        .execute(&format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            table.table,
            create_columns.join(", ")
        ))
        .map_err(|error| error.to_string())?;

    let mut current = session
        .execute(&format!("SELECT * FROM {}", table.table))
        .map_err(|error| error.to_string())?;

    // Schema drift: a master migration may have added columns since this
    // scope's table was created. Additive migrations propagate as
    // ALTER TABLE ADD COLUMN (nullable, no default — the online-safe form);
    // columns the master dropped are left in place and simply go stale,
    // matching the expand/contract discipline in docs/schema-migrations.md.
    let mut altered = false;
    for (index, name) in visible.columns.iter().enumerate() {
        if current.columns.contains(name) {
            continue;
        }
        let sql_type = visible
            .column_types
            .get(index)
            .and_then(|t| t.clone())
            .unwrap_or_else(|| "text".to_string());
        session
            .execute(&format!(
                "ALTER TABLE {} ADD COLUMN {name} {sql_type}",
                table.table
            ))
            .map_err(|error| format!("propagating column {name}: {error}"))?;
        altered = true;
    }
    if altered {
        current = session
            .execute(&format!("SELECT * FROM {}", table.table))
            .map_err(|error| error.to_string())?;
    }
    let current_pk_index = current
        .columns
        .iter()
        .position(|c| c == &table.pk)
        .ok_or_else(|| format!("scope {} lost pk column", table.table))?;

    let visible_rows: HashMap<String, &Vec<bicdb_sql::SqlValue>> = visible
        .rows
        .iter()
        .map(|row| (sql_literal(&row[pk_index]), row))
        .collect();
    let current_rows: HashMap<String, &Vec<bicdb_sql::SqlValue>> = current
        .rows
        .iter()
        .map(|row| (sql_literal(&row[current_pk_index]), row))
        .collect();

    // Deletes: in scope, no longer visible (revocation or master delete).
    for pk_literal in current_rows.keys() {
        if !visible_rows.contains_key(pk_literal) {
            session
                .execute(&format!(
                    "DELETE FROM {} WHERE {} = {}",
                    table.table, table.pk, pk_literal
                ))
                .map_err(|error| error.to_string())?;
        }
    }
    // Inserts + updates.
    for (pk_literal, row) in &visible_rows {
        let same = current_rows.get(pk_literal).is_some_and(|existing| {
            existing.len() == row.len()
                && current.columns == visible.columns
                && existing.iter().zip(row.iter()).all(|(a, b)| a == b)
        });
        if same {
            continue;
        }
        if current_rows.contains_key(pk_literal) {
            let assignments: Vec<String> = visible
                .columns
                .iter()
                .zip(row.iter())
                .filter(|(name, _)| *name != &table.pk)
                .map(|(name, value)| format!("{name} = {}", sql_literal(value)))
                .collect();
            if assignments.is_empty() {
                continue;
            }
            session
                .execute(&format!(
                    "UPDATE {} SET {} WHERE {} = {}",
                    table.table,
                    assignments.join(", "),
                    table.pk,
                    pk_literal
                ))
                .map_err(|error| error.to_string())?;
        } else {
            let values: Vec<String> = row.iter().map(sql_literal).collect();
            session
                .execute(&format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    table.table,
                    visible.columns.join(", "),
                    values.join(", ")
                ))
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

// --- event-horizon trimming ----------------------------------------------------

// The horizon for a scope is the minimum event offset every *live* client
// checkpoint has pulled past; deletes at or below it can leave the log.
// Checkpoints older than the configured age don't hold the horizon back —
// their devices re-bootstrap (checkpoints get reset after a trim anyway,
// since the rewrite changes offsets).
fn horizon_sweep(state: &ServerState) -> Result<(), String> {
    let entries = match std::fs::read_dir(&state.config.root) {
        Ok(entries) => entries,
        Err(_) => return Ok(()),
    };
    for entry in entries.flatten() {
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let scope = entry.file_name().to_string_lossy().into_owned();
        if !valid_scope(&scope) || !entry.path().join("db").exists() {
            continue;
        }
        if let Err(error) = trim_scope_horizon(state, &scope) {
            eprintln!("horizon {scope}: {error}");
        }
    }
    Ok(())
}

fn trim_scope_horizon(state: &ServerState, scope: &str) -> Result<(), String> {
    let db = scope_db(state, scope)?;
    let mut db = db.lock().expect("scope db lock");
    let server_node = db.node_id().to_string();

    let checkpoints_dir = state.config.root.join(scope).join("checkpoints");
    let max_age = Duration::from_secs(state.config.horizon_max_checkpoint_age_seconds.max(1));
    let mut acknowledged = u64::MAX; // no live clients -> nothing can lag
    if let Ok(entries) = std::fs::read_dir(&checkpoints_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let stale = entry
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age > max_age);
            if stale {
                continue;
            }
            let Ok(bytes) = std::fs::read(&path) else {
                continue;
            };
            let Ok(checkpoint) = serde_json::from_slice::<ClientSyncCheckpoint>(&bytes) else {
                continue;
            };
            let pulled = checkpoint
                .remote_imports
                .get(&server_node)
                .map(|c| c.event_offset)
                .unwrap_or(0);
            acknowledged = acknowledged.min(pulled);
        }
    }

    let report = db
        .trim_event_horizon(bicdb_core::SyncCheckpoint::new(acknowledged))
        .map_err(|error| error.to_string())?;
    if report.superseded_dropped + report.deletes_dropped > 0 {
        println!(
            "horizon {scope}: {} -> {} events (-{} superseded, -{} deletes), {} -> {} bytes",
            report.events_before,
            report.events_after,
            report.superseded_dropped,
            report.deletes_dropped,
            report.bytes_before,
            report.bytes_after,
        );
        let server_node = db.node_id();
        drop(db);
        // Offsets changed: clients re-pull from zero (event-id dedup absorbs).
        reset_pull_checkpoints(state, scope, &server_node)?;
    }
    Ok(())
}

// --- telemetry ----------------------------------------------------------------

fn record_telemetry(state: &ServerState, scope: &str, body: &[u8]) -> (u16, Value) {
    #[derive(serde::Deserialize)]
    struct TelemetryBatch {
        events: Vec<Value>,
    }
    let batch: TelemetryBatch = match serde_json::from_slice(body) {
        Ok(batch) => batch,
        Err(error) => return (400, json!({ "error": format!("bad telemetry: {error}") })),
    };
    let path = state.config.root.join(scope).join("telemetry.jsonl");
    let result = (|| -> std::io::Result<()> {
        std::fs::create_dir_all(path.parent().expect("has parent"))?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        for event in &batch.events {
            let line = json!({ "received_at": unix_now(), "event": event });
            writeln!(file, "{line}")?;
        }
        Ok(())
    })();
    match result {
        Ok(()) => (200, json!({ "ok": true, "recorded": batch.events.len() })),
        Err(error) => (500, json!({ "error": error.to_string() })),
    }
}

fn valid_scope(scope: &str) -> bool {
    !scope.is_empty()
        && scope.len() <= 64
        && scope
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn checkpoint_path(state: &ServerState, scope: &str, node: &NodeId) -> PathBuf {
    state
        .config
        .root
        .join(scope)
        .join("checkpoints")
        .join(format!("{node}.json"))
}

fn parse_node(raw: &str) -> Result<NodeId, (u16, Value)> {
    raw.parse()
        .map_err(|_| (400, json!({ "error": "node id must be a uuid" })))
}

fn load_checkpoint(state: &ServerState, scope: &str, node: &str) -> (u16, Value) {
    let node = match parse_node(node) {
        Ok(node) => node,
        Err(response) => return response,
    };
    let path = checkpoint_path(state, scope, &node);
    if !path.exists() {
        return match serde_json::to_value(ClientSyncCheckpoint::default()) {
            Ok(value) => (200, value),
            Err(error) => (500, json!({ "error": error.to_string() })),
        };
    }
    match std::fs::read(&path)
        .map_err(|error| error.to_string())
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).map_err(|e| e.to_string()))
    {
        Ok(value) => (200, value),
        Err(error) => (500, json!({ "error": error })),
    }
}

fn save_checkpoint(state: &ServerState, scope: &str, node: &str, body: &[u8]) -> (u16, Value) {
    let node = match parse_node(node) {
        Ok(node) => node,
        Err(response) => return response,
    };
    let checkpoint: ClientSyncCheckpoint = match serde_json::from_slice(body) {
        Ok(checkpoint) => checkpoint,
        Err(error) => return (400, json!({ "error": format!("bad checkpoint: {error}") })),
    };
    if let Err(error) = checkpoint.validate() {
        return (400, json!({ "error": error.to_string() }));
    }
    let path = checkpoint_path(state, scope, &node);
    let result = (|| -> std::io::Result<()> {
        std::fs::create_dir_all(path.parent().expect("checkpoint has parent"))?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&checkpoint)?)?;
        std::fs::rename(&tmp, &path)
    })();
    match result {
        Ok(()) => (200, json!({ "ok": true })),
        Err(error) => (500, json!({ "error": error.to_string() })),
    }
}

fn scope_db(state: &ServerState, scope: &str) -> Result<Arc<Mutex<BicDb>>, String> {
    let mut scopes = state.scopes.lock().expect("scopes lock");
    if let Some(db) = scopes.get(scope) {
        return Ok(Arc::clone(db));
    }
    let path = state.config.root.join(scope).join("db");
    let db = BicDb::open_with_config(
        &path,
        DbConfig::default()
            .with_fsync(state.config.fsync)
            .with_audit_events(true),
    )
    .map_err(|error| error.to_string())?;
    let db = Arc::new(Mutex::new(db));
    scopes.insert(scope.to_string(), Arc::clone(&db));
    Ok(db)
}

fn push_bundle(state: &ServerState, scope: &str, body: &[u8]) -> (u16, Value) {
    let bundle: SyncBundle = match serde_json::from_slice(body) {
        Ok(bundle) => bundle,
        Err(error) => return (400, json!({ "error": format!("bad bundle: {error}") })),
    };
    let db = match scope_db(state, scope) {
        Ok(db) => db,
        Err(error) => return (500, json!({ "error": error })),
    };
    let mut db = db.lock().expect("scope db lock");
    // Direction policy: reject the whole push if it carries a NEW event for
    // a server-write-only collection. Events the server already has are
    // echoes (a client legitimately re-exports imported server content
    // after compaction resets its watermark) and import dedup drops them;
    // an attacker cannot forge "already known" — unseen ids are exactly
    // what gets rejected. Rejecting whole (not filtering) keeps the
    // client's watermark from advancing past a write it believes it made.
    if !state.config.server_write_only.is_empty() {
        for entry in &bundle.events {
            let Some(collection) = entry
                .event
                .payload
                .get("collection")
                .and_then(Value::as_str)
            else {
                continue;
            };
            if state
                .config
                .server_write_only
                .iter()
                .any(|protected| protected == collection)
                && !db.events().contains_event(&entry.envelope.event_id)
            {
                return (
                    403,
                    json!({ "error": format!(
                        "collection {collection} is server-write-only; client pushes to it are rejected"
                    ) }),
                );
            }
        }
    }
    let report = PushBundleReport::from(&bundle);
    match db.import_sync_bundle(bundle) {
        Ok(_) => match serde_json::to_value(&report) {
            Ok(value) => (200, value),
            Err(error) => (500, json!({ "error": error.to_string() })),
        },
        Err(error) => (400, json!({ "error": error.to_string() })),
    }
}

fn pull_bundles(state: &ServerState, scope: &str, body: &[u8]) -> (u16, Value) {
    #[derive(serde::Deserialize)]
    struct PullRequest {
        node_id: NodeId,
        checkpoint: ClientSyncCheckpoint,
    }
    let request: PullRequest = match serde_json::from_slice(body) {
        Ok(request) => request,
        Err(error) => {
            return (
                400,
                json!({ "error": format!("bad pull request: {error}") }),
            );
        }
    };
    let db = match scope_db(state, scope) {
        Ok(db) => db,
        Err(error) => return (500, json!({ "error": error })),
    };
    let mut db = db.lock().expect("scope db lock");
    let server_node = db.node_id();
    let since = request.checkpoint.remote_checkpoint(&server_node);
    let exported = match db.export_sync_bundle_since(since) {
        Ok(bundle) => bundle,
        Err(error) => return (500, json!({ "error": error.to_string() })),
    };
    // Don't echo the puller's own events back at it; the next_checkpoint
    // still covers them, so the client's server-watermark advances past
    // its own writes.
    let from = exported.from_checkpoint;
    let next = exported.next_checkpoint;
    let events: Vec<_> = exported
        .events
        .into_iter()
        .filter(|entry| entry.envelope.node_id != request.node_id)
        .collect();
    let bundle = match SyncBundle::new(server_node.clone(), from, next, events) {
        Ok(bundle) => bundle,
        Err(error) => return (500, json!({ "error": error.to_string() })),
    };
    let bundles = if bundle.event_count > 0 || next.event_offset > since.event_offset {
        vec![bundle]
    } else {
        Vec::new()
    };
    // Bundles are hash-verified byte content: serialize each ONCE here and
    // ship it as an opaque string, so the browser can hand the exact bytes
    // to its engine without a JS JSON round-trip re-canonicalizing numbers
    // (84.0 -> 84) and breaking payload-hash verification.
    let bundles: Vec<String> = match bundles
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<_, _>>()
    {
        Ok(bundles) => bundles,
        Err(error) => return (500, json!({ "error": error.to_string() })),
    };
    (
        200,
        json!({ "server_node_id": server_node.to_string(), "bundles": bundles }),
    )
}

fn admin_sql(state: &ServerState, scope: &str, body: &[u8]) -> (u16, Value) {
    // `sql` is one statement or an array executed in ONE session — the array
    // form supports transactional administration and ordinary app settings:
    //   ["SET app.request_label = 'sync-delete'", "DELETE FROM docs WHERE id=1"]
    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum Statements {
        One(String),
        Many(Vec<String>),
    }
    #[derive(serde::Deserialize)]
    struct SqlRequest {
        sql: Statements,
    }
    let request: SqlRequest = match serde_json::from_slice(body) {
        Ok(request) => request,
        Err(error) => return (400, json!({ "error": format!("bad sql request: {error}") })),
    };
    let statements = match request.sql {
        Statements::One(sql) => vec![sql],
        Statements::Many(list) => list,
    };
    let db = match scope_db(state, scope) {
        Ok(db) => db,
        Err(error) => return (500, json!({ "error": error })),
    };
    let mut db = db.lock().expect("scope db lock");
    let mut session = SqlSession::new(&mut db);
    let mut results = Vec::with_capacity(statements.len());
    for sql in &statements {
        match session.execute(sql) {
            Ok(result) => match serde_json::to_value(&result) {
                Ok(value) => results.push(value),
                Err(error) => return (500, json!({ "error": error.to_string() })),
            },
            Err(error) => {
                return (400, json!({ "error": format!("{sql}: {error}") }));
            }
        }
    }
    let last = results.last().cloned().unwrap_or(Value::Null);
    (200, json!({ "result": last, "results": results }))
}

fn respond(
    stream: TcpStream,
    status: u16,
    body: &Value,
    state: &ServerState,
) -> std::io::Result<()> {
    respond_raw(stream, status, &body.to_string(), state)
}

fn respond_raw(
    mut stream: TcpStream,
    status: u16,
    body: &str,
    state: &ServerState,
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        _ => "Internal Server Error",
    };
    let origin = &state.config.cors_origin;
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Access-Control-Allow-Origin: {origin}\r\n\
         Access-Control-Allow-Methods: GET, PUT, POST, OPTIONS\r\n\
         Access-Control-Allow-Headers: authorization, content-type\r\n\
         Connection: close\r\n\r\n{body}",
        body.len(),
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()
}

#[cfg(test)]
mod reverse_apply_tests {
    use super::*;

    /// D-5 (CWE-89): a compromised client device controls the keys of the
    /// record metadata it syncs, and those keys were interpolated into the
    /// composed UPDATE/INSERT unescaped. Only the VALUES were quoted.
    #[test]
    fn a_crafted_column_name_cannot_rewrite_the_composed_statement() {
        let directory = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(directory.path(), DbConfig::default().with_fsync(false))
                .unwrap();
        {
            let mut admin = SqlSession::new(&mut db);
            admin
                .execute("CREATE TABLE notes (id INT PRIMARY KEY, v TEXT)")
                .unwrap();
            admin
                .execute("INSERT INTO notes VALUES (1, 'mine'), (2, 'someone-elses')")
                .unwrap();
        }
        let compose = RlsComposeConfig {
            master_scope: "master".to_string(),
            user_scope_prefix: "user-".to_string(),
            tenant: None,
            tables: vec![ComposeTable {
                table: "notes".to_string(),
                pk: "id".to_string(),
            }],
            interval_seconds: 1,
        };
        // The payload a compromised device can craft: the metadata KEY closes
        // the assignment and widens the WHERE to every row.
        let event = bicdb_core::Event::new(
            "sync",
            "RecordUpdated",
            json!({
                "collection": "notes",
                "record_id": "1",
                "record": { "metadata": { "v = 'pwned' WHERE 1=1 --": "x" } },
            }),
        );
        let mut session = SqlSession::new(&mut db);
        let outcome = reverse_apply(&mut session, &compose, &event);
        assert!(
            outcome.is_err(),
            "a non-identifier column name must be refused, got {outcome:?}"
        );
        let rows = session.execute("SELECT v FROM notes WHERE id = 2").unwrap();
        assert!(
            format!("{rows:?}").contains("someone-elses"),
            "the untargeted row must be untouched: {rows:?}"
        );
    }

    /// Ordinary column names must still replay.
    #[test]
    fn an_ordinary_column_name_still_replays() {
        let directory = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(directory.path(), DbConfig::default().with_fsync(false))
                .unwrap();
        {
            let mut admin = SqlSession::new(&mut db);
            admin
                .execute("CREATE TABLE notes (id INT PRIMARY KEY, v TEXT)")
                .unwrap();
            admin
                .execute("INSERT INTO notes VALUES (1, 'before')")
                .unwrap();
        }
        let compose = RlsComposeConfig {
            master_scope: "master".to_string(),
            user_scope_prefix: "user-".to_string(),
            tenant: None,
            tables: vec![ComposeTable {
                table: "notes".to_string(),
                pk: "id".to_string(),
            }],
            interval_seconds: 1,
        };
        let event = bicdb_core::Event::new(
            "sync",
            "RecordUpdated",
            json!({
                "collection": "notes",
                "record_id": "1",
                "record": { "metadata": { "v": "after" } },
            }),
        );
        let mut session = SqlSession::new(&mut db);
        reverse_apply(&mut session, &compose, &event).expect("a plain column must replay");
        let rows = session.execute("SELECT v FROM notes WHERE id = 1").unwrap();
        assert!(
            format!("{rows:?}").contains("after"),
            "the legitimate update must have applied: {rows:?}"
        );
    }

    #[test]
    fn identifier_validation_accepts_plain_names_and_rejects_the_rest() {
        for good in ["v", "_v", "col_1", "A1"] {
            assert!(validate_sync_identifier(good).is_ok(), "{good} must pass");
        }
        for bad in [
            "",
            "1col",
            "v v",
            "v;DROP",
            "v'",
            "v\"",
            "v = 'x' WHERE 1=1 --",
            "col,other",
        ] {
            assert!(validate_sync_identifier(bad).is_err(), "{bad:?} must fail");
        }
    }
}
