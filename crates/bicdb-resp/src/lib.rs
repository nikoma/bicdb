//! bicdb-resp: a Redis-compatible (RESP2) cache server backed by the bicdb
//! engine.
//!
//! Scope: the string/TTL command family internal apps use for caching
//! (Rails.cache, Django cache, session stores, memoization) — GET/SET and
//! variants, counters, TTL management, key iteration, multiple logical
//! databases, optional AUTH, and optional bounded-size eviction. Values are
//! ordinary bicdb records, so the cache is durable across restarts (WAL),
//! TTLs included. Lists, hashes, sets, and pub/sub are not implemented; those
//! commands return an error.

mod hotview;
mod resp;
mod store;

use std::io::{BufRead, BufReader, Write as IoWrite};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use bicdb_core::BicDbError;

use hotview::HotViewRegistry;
pub use hotview::{HotViewMeta, RefreshMode};
use store::{now_ms, CacheStore, SetCondition, KEEP_TTL_SENTINEL};
pub use store::{CacheStoreConfig, Entry, EvictionPolicy, TtlState, NUM_DATABASES};

#[derive(Debug, thiserror::Error)]
pub enum RespServerError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("engine error: {0}")]
    Db(#[from] BicDbError),
    #[error("{0}")]
    Command(String),
    /// Internal control flow for NX/XX writes; never surfaces to the client.
    #[error("condition failed")]
    ConditionFailed(Option<Entry>),
    #[error("configuration error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, RespServerError>;

#[derive(Clone, Debug)]
pub struct RespConfig {
    pub host: String,
    pub port: u16,
    /// When set, clients must AUTH before issuing commands.
    pub password: Option<String>,
    /// fsync every write commit (durable to disk, slower). Off by default.
    pub fsync: bool,
    /// Total key budget across all databases; writes beyond it evict or fail
    /// per `eviction`.
    pub max_keys: Option<usize>,
    pub eviction: EvictionPolicy,
    pub max_connections: usize,
    /// Enable the HotView surface (`SQL` + `HOTVIEW.*` commands). Off by
    /// default: it exposes arbitrary SQL execution on the cache port, so it
    /// must be a deliberate choice. When off, those commands return an error
    /// and no view machinery runs at all.
    pub hotview: bool,
    /// Keep cache entries in memory only: no engine commit per write, so SET
    /// runs at memory speed, but keys do not survive a restart. SQL tables
    /// and HotView definitions remain durable — materialized views recompute
    /// at startup, so derived entries come back hot even in this mode.
    pub ephemeral: bool,
}

impl Default for RespConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            password: None,
            fsync: false,
            max_keys: None,
            eviction: EvictionPolicy::NoEviction,
            max_connections: 1000,
            hotview: false,
            ephemeral: false,
        }
    }
}

const SWEEP_INTERVAL: Duration = Duration::from_millis(100);
const IDLE_POLL_TIMEOUT: Duration = Duration::from_millis(250);
const COMMAND_READ_TIMEOUT: Duration = Duration::from_secs(30);
const SERVER_VERSION: &str = "7.4.0";

pub struct RespServerHandle {
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
}

impl RespServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stop accepting, wake the accept loop, and wait for it (and all
    /// connection threads it joined) to finish, releasing the database.
    pub fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
        if let Some(thread) = self.accept_thread.take() {
            let _ = thread.join();
        }
    }
}

/// Open the cache database at `path` and serve RESP on the configured address,
/// blocking forever (until process exit).
pub fn serve(path: impl AsRef<Path>, config: RespConfig) -> Result<()> {
    let handle = start(path, config)?;
    if let Some(thread) = {
        let mut handle = handle;
        handle.accept_thread.take()
    } {
        let _ = thread.join();
    }
    Ok(())
}

/// Open the cache database and serve in background threads; returns a handle
/// with the bound address (use port 0 for an ephemeral port).
pub fn start(path: impl AsRef<Path>, config: RespConfig) -> Result<RespServerHandle> {
    let store = Arc::new(CacheStore::open(
        path.as_ref(),
        &CacheStoreConfig {
            fsync: config.fsync,
            max_keys: config.max_keys,
            eviction: config.eviction,
            ephemeral: config.ephemeral,
        },
    )?);
    // Recompute all persisted hotviews before accepting connections: the
    // cache comes up hot, never serving pre-restart staleness. Disabled =>
    // no registry at all; persisted definitions stay dormant on disk.
    let hotviews = if config.hotview {
        Some(Arc::new(HotViewRegistry::load(&store)?))
    } else {
        None
    };
    let listener = TcpListener::bind((config.host.as_str(), config.port))?;
    let addr = listener.local_addr()?;
    let shutdown = Arc::new(AtomicBool::new(false));

    let accept_thread = {
        let store = Arc::clone(&store);
        let hotviews = hotviews.clone();
        let shutdown = Arc::clone(&shutdown);
        let config = config.clone();
        std::thread::Builder::new()
            .name("bicdb-resp-accept".to_string())
            .spawn(move || accept_loop(listener, store, hotviews, config, shutdown))?
    };

    Ok(RespServerHandle {
        addr,
        shutdown,
        accept_thread: Some(accept_thread),
    })
}

fn accept_loop(
    listener: TcpListener,
    store: Arc<CacheStore>,
    hotviews: Option<Arc<HotViewRegistry>>,
    config: RespConfig,
    shutdown: Arc<AtomicBool>,
) {
    let active = Arc::new(AtomicUsize::new(0));
    let mut connection_threads: Vec<JoinHandle<()>> = Vec::new();
    let sweeper = {
        let store = Arc::clone(&store);
        let shutdown = Arc::clone(&shutdown);
        std::thread::Builder::new()
            .name("bicdb-resp-sweeper".to_string())
            .spawn(move || {
                while !shutdown.load(Ordering::SeqCst) {
                    let _ = store.sweep_expired();
                    std::thread::sleep(SWEEP_INTERVAL);
                }
            })
            .ok()
    };

    for stream in listener.incoming() {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        let Ok(stream) = stream else { continue };
        // Request/reply over small frames: Nagle + delayed ACK otherwise adds
        // hundreds of ms of stall per command (same lesson as pgwire).
        let _ = stream.set_nodelay(true);
        connection_threads.retain(|thread| !thread.is_finished());
        if active.load(Ordering::SeqCst) >= config.max_connections {
            let mut out = Vec::new();
            resp::write_error(&mut out, "ERR max number of clients reached");
            let mut stream = stream;
            let _ = stream.write_all(&out);
            continue;
        }
        active.fetch_add(1, Ordering::SeqCst);
        let store = Arc::clone(&store);
        let hotviews = hotviews.clone();
        let shutdown = Arc::clone(&shutdown);
        let conn_active = Arc::clone(&active);
        let password = config.password.clone();
        let spawned = std::thread::Builder::new()
            .name("bicdb-resp-conn".to_string())
            .spawn(move || {
                let _ = handle_connection(stream, store, hotviews, password, shutdown);
                conn_active.fetch_sub(1, Ordering::SeqCst);
            });
        match spawned {
            Ok(thread) => connection_threads.push(thread),
            Err(_) => {
                active.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }
    for thread in connection_threads {
        let _ = thread.join();
    }
    if let Some(sweeper) = sweeper {
        let _ = sweeper.join();
    }
}

struct ConnState {
    db_index: u8,
    authed: bool,
    password: Option<String>,
    client_name: String,
}

enum Action {
    Continue,
    Close,
}

fn handle_connection(
    stream: TcpStream,
    store: Arc<CacheStore>,
    hotviews: Option<Arc<HotViewRegistry>>,
    password: Option<String>,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<()> {
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);
    let mut state = ConnState {
        db_index: 0,
        authed: password.is_none(),
        password,
        client_name: String::new(),
    };
    let mut out = Vec::with_capacity(4096);
    loop {
        // Idle-poll so the thread notices shutdown; once bytes arrive, allow a
        // longer window for the rest of the command.
        reader.get_ref().set_read_timeout(Some(IDLE_POLL_TIMEOUT))?;
        match reader.fill_buf() {
            Ok(buf) if buf.is_empty() => return Ok(()),
            Ok(_) => {}
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if shutdown.load(Ordering::SeqCst) {
                    return Ok(());
                }
                continue;
            }
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
        reader
            .get_ref()
            .set_read_timeout(Some(COMMAND_READ_TIMEOUT))?;
        let args = match resp::read_command(&mut reader) {
            Ok(Some(args)) => args,
            Ok(None) => return Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::InvalidData => {
                let mut error_out = Vec::new();
                resp::write_error(&mut error_out, &format!("ERR {err}"));
                let _ = writer.write_all(&error_out);
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        if args.is_empty() {
            continue;
        }
        out.clear();
        let action = dispatch(&store, hotviews.as_deref(), &mut state, &args, &mut out);
        writer.write_all(&out)?;
        writer.flush()?;
        if matches!(action, Action::Close) {
            return Ok(());
        }
        if shutdown.load(Ordering::SeqCst) {
            return Ok(());
        }
    }
}

fn dispatch(
    store: &CacheStore,
    hotviews: Option<&HotViewRegistry>,
    state: &mut ConnState,
    args: &[Vec<u8>],
    out: &mut Vec<u8>,
) -> Action {
    match dispatch_inner(store, hotviews, state, args, out) {
        Ok(action) => action,
        Err(err) => {
            write_command_error(out, &err);
            Action::Continue
        }
    }
}

fn dispatch_inner(
    store: &CacheStore,
    hotviews: Option<&HotViewRegistry>,
    state: &mut ConnState,
    args: &[Vec<u8>],
    out: &mut Vec<u8>,
) -> Result<Action> {
    let command = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    let args = &args[1..];

    if !state.authed && !matches!(command.as_str(), "AUTH" | "HELLO" | "QUIT" | "RESET") {
        resp::write_error(out, "NOAUTH Authentication required.");
        return Ok(Action::Continue);
    }

    let result = match command.as_str() {
        "PING" => {
            match args.first() {
                Some(message) => resp::write_bulk(out, message),
                None => resp::write_simple(out, "PONG"),
            }
            Ok(())
        }
        "ECHO" => with_arity(args, 1, out, &command, |args, out| {
            resp::write_bulk(out, &args[0]);
            Ok(())
        }),
        "QUIT" => {
            resp::write_simple(out, "OK");
            return Ok(Action::Close);
        }
        "RESET" => {
            state.db_index = 0;
            state.authed = state.password.is_none();
            resp::write_simple(out, "RESET");
            Ok(())
        }
        "AUTH" => cmd_auth(state, args, out),
        "HELLO" => cmd_hello(state, args, out),
        "SELECT" => with_arity(args, 1, out, &command, |args, out| {
            let index = parse_int_arg(&args[0])?;
            if !(0..NUM_DATABASES as i64).contains(&index) {
                return Err(RespServerError::Command("DB index is out of range".into()));
            }
            state.db_index = index as u8;
            resp::write_simple(out, "OK");
            Ok(())
        }),
        "CLIENT" => cmd_client(state, args, out),
        "COMMAND" => {
            // Enough for client handshakes: an empty command table.
            match args.first().map(|sub| sub.to_ascii_uppercase()) {
                Some(sub) if sub == b"COUNT" => resp::write_int(out, 0),
                _ => resp::write_array_header(out, 0),
            }
            Ok(())
        }
        "INFO" => cmd_info(store, out),
        "DBSIZE" => {
            let count = store.keys(state.db_index).map(|keys| keys.len())?;
            resp::write_int(out, count as i64);
            Ok(())
        }
        "FLUSHDB" => {
            store.flush_db(state.db_index)?;
            resp::write_simple(out, "OK");
            Ok(())
        }
        "FLUSHALL" => {
            store.flush_all()?;
            resp::write_simple(out, "OK");
            Ok(())
        }
        "GET" => with_arity(args, 1, out, &command, |args, out| {
            write_entry_value(out, store.get(state.db_index, &args[0])?);
            Ok(())
        }),
        "SET" => cmd_set(store, state.db_index, args, out),
        "SETNX" => with_arity(args, 2, out, &command, |args, out| {
            let (written, _) = store.set_conditional(
                state.db_index,
                &args[0],
                args[1].clone(),
                None,
                SetCondition::IfAbsent,
            )?;
            resp::write_int(out, written as i64);
            Ok(())
        }),
        "SETEX" | "PSETEX" => with_arity(args, 3, out, &command, |args, out| {
            let amount = parse_int_arg(&args[1])?;
            if amount <= 0 {
                return Err(RespServerError::Command(format!(
                    "invalid expire time in '{}' command",
                    command.to_ascii_lowercase()
                )));
            }
            let millis = if command == "SETEX" {
                amount.saturating_mul(1000)
            } else {
                amount
            };
            let deadline = now_ms().saturating_add(millis);
            store.set(state.db_index, &args[0], args[2].clone(), Some(deadline))?;
            resp::write_simple(out, "OK");
            Ok(())
        }),
        "GETSET" => with_arity(args, 2, out, &command, |args, out| {
            let previous = store.set(state.db_index, &args[0], args[1].clone(), None)?;
            write_entry_value(out, previous);
            Ok(())
        }),
        "GETDEL" => with_arity(args, 1, out, &command, |args, out| {
            write_entry_value(out, store.get_del(state.db_index, &args[0])?);
            Ok(())
        }),
        "GETEX" => cmd_getex(store, state.db_index, args, out),
        "MGET" => {
            if args.is_empty() {
                Err(arity_error(&command))
            } else {
                resp::write_array_header(out, args.len());
                for key in args {
                    write_entry_value(out, store.get(state.db_index, key)?);
                }
                Ok(())
            }
        }
        "MSET" => {
            if args.is_empty() || args.len() % 2 != 0 {
                Err(arity_error(&command))
            } else {
                for pair in args.chunks(2) {
                    store.set(state.db_index, &pair[0], pair[1].clone(), None)?;
                }
                resp::write_simple(out, "OK");
                Ok(())
            }
        }
        "MSETNX" => {
            if args.is_empty() || args.len() % 2 != 0 {
                Err(arity_error(&command))
            } else {
                let pairs: Vec<(&[u8], Vec<u8>)> = args
                    .chunks(2)
                    .map(|pair| (pair[0].as_slice(), pair[1].clone()))
                    .collect();
                let written = store.mset_nx(state.db_index, &pairs)?;
                resp::write_int(out, written as i64);
                Ok(())
            }
        }
        "APPEND" => with_arity(args, 2, out, &command, |args, out| {
            let new_len = store.append(state.db_index, &args[0], &args[1])?;
            resp::write_int(out, new_len as i64);
            Ok(())
        }),
        "STRLEN" => with_arity(args, 1, out, &command, |args, out| {
            let len = store
                .get(state.db_index, &args[0])?
                .map(|entry| entry.value.len())
                .unwrap_or(0);
            resp::write_int(out, len as i64);
            Ok(())
        }),
        "INCR" | "DECR" => with_arity(args, 1, out, &command, |args, out| {
            let delta = if command == "INCR" { 1 } else { -1 };
            let value = store.incr_by(state.db_index, &args[0], delta)?;
            resp::write_int(out, value);
            Ok(())
        }),
        "INCRBY" | "DECRBY" => with_arity(args, 2, out, &command, |args, out| {
            let mut delta = parse_int_arg(&args[1])?;
            if command == "DECRBY" {
                delta = delta
                    .checked_neg()
                    .ok_or_else(|| RespServerError::Command("decrement would overflow".into()))?;
            }
            let value = store.incr_by(state.db_index, &args[0], delta)?;
            resp::write_int(out, value);
            Ok(())
        }),
        "INCRBYFLOAT" => with_arity(args, 2, out, &command, |args, out| {
            let delta = parse_float_arg(&args[1])?;
            let value = store.incr_by_float(state.db_index, &args[0], delta)?;
            resp::write_bulk(out, value.as_bytes());
            Ok(())
        }),
        "DEL" | "UNLINK" => {
            if args.is_empty() {
                Err(arity_error(&command))
            } else {
                let keys: Vec<&[u8]> = args.iter().map(|key| key.as_slice()).collect();
                let removed = store.delete(state.db_index, &keys)?;
                resp::write_int(out, removed as i64);
                Ok(())
            }
        }
        "EXISTS" => {
            if args.is_empty() {
                Err(arity_error(&command))
            } else {
                let mut found = 0i64;
                for key in args {
                    if store.get(state.db_index, key)?.is_some() {
                        found += 1;
                    }
                }
                resp::write_int(out, found);
                Ok(())
            }
        }
        "TYPE" => with_arity(args, 1, out, &command, |args, out| {
            let kind = if store.get(state.db_index, &args[0])?.is_some() {
                "string"
            } else {
                "none"
            };
            resp::write_simple(out, kind);
            Ok(())
        }),
        "RENAME" => with_arity(args, 2, out, &command, |args, out| {
            store.rename(state.db_index, &args[0], &args[1])?;
            resp::write_simple(out, "OK");
            Ok(())
        }),
        "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => {
            with_arity(args, 2, out, &command, |args, out| {
                let amount = parse_int_arg(&args[1])?;
                let deadline = match command.as_str() {
                    "EXPIRE" => now_ms().saturating_add(amount.saturating_mul(1000)),
                    "PEXPIRE" => now_ms().saturating_add(amount),
                    "EXPIREAT" => amount.saturating_mul(1000),
                    _ => amount,
                };
                let applied = store.set_expiry(state.db_index, &args[0], Some(deadline))?;
                resp::write_int(out, applied as i64);
                Ok(())
            })
        }
        "PERSIST" => with_arity(args, 1, out, &command, |args, out| {
            let had_ttl = matches!(
                store.ttl(state.db_index, &args[0])?,
                TtlState::ExpiresInMs(_)
            );
            let applied = had_ttl && store.set_expiry(state.db_index, &args[0], None)?;
            resp::write_int(out, applied as i64);
            Ok(())
        }),
        "TTL" | "PTTL" => with_arity(args, 1, out, &command, |args, out| {
            let value = match store.ttl(state.db_index, &args[0])? {
                TtlState::Missing => -2,
                TtlState::NoExpiry => -1,
                TtlState::ExpiresInMs(millis) => {
                    if command == "TTL" {
                        (millis + 999) / 1000
                    } else {
                        millis
                    }
                }
            };
            resp::write_int(out, value);
            Ok(())
        }),
        "KEYS" => with_arity(args, 1, out, &command, |args, out| {
            let matched: Vec<Vec<u8>> = store
                .keys(state.db_index)?
                .into_iter()
                .filter(|key| glob_match(&args[0], key))
                .collect();
            resp::write_array_header(out, matched.len());
            for key in matched {
                resp::write_bulk(out, &key);
            }
            Ok(())
        }),
        "SCAN" => cmd_scan(store, state.db_index, args, out),
        "SQL" => cmd_sql(store, require_hotviews(hotviews)?, args, out),
        "HOTVIEW.CREATE" => cmd_hotview_create(
            store,
            require_hotviews(hotviews)?,
            state.db_index,
            args,
            out,
        ),
        "HOTVIEW.DROP" => with_arity(args, 1, out, &command, |args, out| {
            let key = utf8_arg(&args[0], "hotview key")?;
            let existed = require_hotviews(hotviews)?.drop_view(store, state.db_index, key)?;
            resp::write_int(out, existed as i64);
            Ok(())
        }),
        "HOTVIEW.LIST" => {
            let keys = require_hotviews(hotviews)?.list(state.db_index);
            resp::write_array_header(out, keys.len());
            for key in keys {
                resp::write_bulk(out, key.as_bytes());
            }
            Ok(())
        }
        "HOTVIEW.STATUS" => with_arity(args, 1, out, &command, |args, out| {
            let key = utf8_arg(&args[0], "hotview key")?;
            let Some(view) = require_hotviews(hotviews)?.status(state.db_index, key) else {
                return Err(RespServerError::Command(format!("no such hotview '{key}'")));
            };
            let deps = view.deps.iter().cloned().collect::<Vec<_>>().join(" ");
            let pairs: [(&str, String); 7] = [
                ("sql", view.sql),
                ("mode", format!("{:?}", view.mode).to_ascii_lowercase()),
                ("deps", deps),
                ("generation", view.generation.to_string()),
                ("stale", view.stale.to_string()),
                (
                    "last_refresh_unix_ms",
                    view.last_refresh_unix_ms.to_string(),
                ),
                ("last_error", view.last_error.unwrap_or_default()),
            ];
            resp::write_array_header(out, pairs.len() * 2);
            for (field, value) in pairs {
                resp::write_bulk(out, field.as_bytes());
                resp::write_bulk(out, value.as_bytes());
            }
            Ok(())
        }),
        "HOTVIEW.REFRESH" => with_arity(args, 1, out, &command, |args, out| {
            let key = utf8_arg(&args[0], "hotview key")?;
            let generation = require_hotviews(hotviews)?.refresh(store, state.db_index, key)?;
            resp::write_int(out, generation as i64);
            Ok(())
        }),
        "HGET" | "HSET" | "HDEL" | "HGETALL" | "HMGET" | "HMSET" | "LPUSH" | "RPUSH" | "LPOP"
        | "RPOP" | "LRANGE" | "SADD" | "SREM" | "SMEMBERS" | "ZADD" | "ZRANGE" | "SUBSCRIBE"
        | "PSUBSCRIBE" | "PUBLISH" | "EVAL" | "EVALSHA" | "MULTI" | "EXEC" | "WATCH" => {
            Err(RespServerError::Command(format!(
                "unsupported command '{}': bicdb-resp implements the string/TTL cache family; \
                 lists, hashes, sets, pub/sub, scripting, and transactions are not available",
                command.to_ascii_lowercase()
            )))
        }
        _ => Err(RespServerError::Command(format!(
            "unknown command '{command}'"
        ))),
    };

    result.map(|()| Action::Continue)
}

fn write_command_error(out: &mut Vec<u8>, err: &RespServerError) {
    let message = match err {
        RespServerError::Command(message) => {
            if message.starts_with("OOM ")
                || message.starts_with("NOAUTH")
                || message.starts_with("NOPROTO")
                || message.starts_with("WRONGPASS")
            {
                message.clone()
            } else {
                format!("ERR {message}")
            }
        }
        RespServerError::ConditionFailed(_) => "ERR condition failed".to_string(),
        other => format!("ERR internal error: {other}"),
    };
    resp::write_error(out, &message);
}

fn with_arity(
    args: &[Vec<u8>],
    expected: usize,
    out: &mut Vec<u8>,
    command: &str,
    handler: impl FnOnce(&[Vec<u8>], &mut Vec<u8>) -> Result<()>,
) -> Result<()> {
    if args.len() != expected {
        return Err(arity_error(command));
    }
    handler(args, out)
}

fn arity_error(command: &str) -> RespServerError {
    RespServerError::Command(format!(
        "wrong number of arguments for '{}' command",
        command.to_ascii_lowercase()
    ))
}

fn parse_int_arg(bytes: &[u8]) -> Result<i64> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or_else(|| RespServerError::Command("value is not an integer or out of range".into()))
}

fn parse_float_arg(bytes: &[u8]) -> Result<f64> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .ok_or_else(|| RespServerError::Command("value is not a valid float".into()))
}

fn write_entry_value(out: &mut Vec<u8>, entry: Option<Entry>) {
    match entry {
        Some(entry) => resp::write_bulk(out, &entry.value),
        None => resp::write_nil(out),
    }
}

fn cmd_auth(state: &mut ConnState, args: &[Vec<u8>], out: &mut Vec<u8>) -> Result<()> {
    let offered = match args {
        [password] => password,
        // AUTH <username> <password>: only the "default" user exists.
        [username, password] => {
            if username.as_slice() != b"default" {
                return Err(RespServerError::Command(
                    "WRONGPASS invalid username-password pair or user is disabled.".into(),
                ));
            }
            password
        }
        _ => return Err(arity_error("AUTH")),
    };
    let Some(expected) = &state.password else {
        return Err(RespServerError::Command(
            "Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?"
                .into(),
        ));
    };
    // Constant-time comparison: a plain `==` short-circuits on the first
    // mismatched byte (and on a length difference), leaking the password
    // byte-by-byte through response timing. Compare fixed-size SHA-256 digests
    // so neither the password length nor its contents affect the timing —
    // matching the constant-time discipline pgwire's auth already uses.
    use sha2::{Digest, Sha256};
    use subtle::ConstantTimeEq;
    let offered_digest = Sha256::digest(offered.as_slice());
    let expected_digest = Sha256::digest(expected.as_bytes());
    if bool::from(offered_digest.ct_eq(&expected_digest)) {
        state.authed = true;
        resp::write_simple(out, "OK");
        Ok(())
    } else {
        Err(RespServerError::Command(
            "WRONGPASS invalid username-password pair or user is disabled.".into(),
        ))
    }
}

fn cmd_hello(state: &mut ConnState, args: &[Vec<u8>], out: &mut Vec<u8>) -> Result<()> {
    let mut rest = args;
    if let Some(proto) = rest.first() {
        let version = parse_int_arg(proto)?;
        if version != 2 {
            return Err(RespServerError::Command(
                "NOPROTO unsupported protocol version".into(),
            ));
        }
        rest = &rest[1..];
    }
    while !rest.is_empty() {
        let option = rest[0].to_ascii_uppercase();
        match option.as_slice() {
            b"AUTH" if rest.len() >= 3 => {
                let mut discard = Vec::new();
                cmd_auth(state, &rest[1..3], &mut discard)?;
                rest = &rest[3..];
            }
            b"SETNAME" if rest.len() >= 2 => {
                state.client_name = String::from_utf8_lossy(&rest[1]).into_owned();
                rest = &rest[2..];
            }
            _ => return Err(RespServerError::Command("syntax error in HELLO".into())),
        }
    }
    if !state.authed {
        return Err(RespServerError::Command("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time".into()));
    }
    // RESP2 map-as-flat-array handshake payload.
    resp::write_array_header(out, 14);
    resp::write_bulk(out, b"server");
    resp::write_bulk(out, b"redis");
    resp::write_bulk(out, b"version");
    resp::write_bulk(out, SERVER_VERSION.as_bytes());
    resp::write_bulk(out, b"proto");
    resp::write_int(out, 2);
    resp::write_bulk(out, b"id");
    resp::write_int(out, 1);
    resp::write_bulk(out, b"mode");
    resp::write_bulk(out, b"standalone");
    resp::write_bulk(out, b"role");
    resp::write_bulk(out, b"master");
    resp::write_bulk(out, b"modules");
    resp::write_array_header(out, 0);
    Ok(())
}

fn cmd_client(state: &mut ConnState, args: &[Vec<u8>], out: &mut Vec<u8>) -> Result<()> {
    let Some(subcommand) = args.first() else {
        return Err(arity_error("CLIENT"));
    };
    match subcommand.to_ascii_uppercase().as_slice() {
        b"SETNAME" if args.len() == 2 => {
            state.client_name = String::from_utf8_lossy(&args[1]).into_owned();
            resp::write_simple(out, "OK");
        }
        b"GETNAME" => resp::write_bulk(out, state.client_name.as_bytes()),
        b"ID" => resp::write_int(out, 1),
        b"SETINFO" | b"NO-EVICT" | b"NO-TOUCH" => resp::write_simple(out, "OK"),
        other => {
            return Err(RespServerError::Command(format!(
                "unknown CLIENT subcommand '{}'",
                String::from_utf8_lossy(other).to_ascii_lowercase()
            )))
        }
    }
    Ok(())
}

fn cmd_info(store: &CacheStore, out: &mut Vec<u8>) -> Result<()> {
    let mut info = String::new();
    info.push_str("# Server\r\n");
    info.push_str(&format!("redis_version:{SERVER_VERSION}\r\n"));
    info.push_str(&format!(
        "bicdb_resp_version:{}\r\n",
        env!("CARGO_PKG_VERSION")
    ));
    info.push_str("redis_mode:standalone\r\n");
    info.push_str("role:master\r\n");
    info.push_str("# Keyspace\r\n");
    for db_index in 0..NUM_DATABASES as u8 {
        let keys = store.keys(db_index)?.len();
        if keys > 0 {
            info.push_str(&format!("db{db_index}:keys={keys},expires=0,avg_ttl=0\r\n"));
        }
    }
    resp::write_bulk(out, info.as_bytes());
    Ok(())
}

fn cmd_set(store: &CacheStore, db_index: u8, args: &[Vec<u8>], out: &mut Vec<u8>) -> Result<()> {
    if args.len() < 2 {
        return Err(arity_error("SET"));
    }
    let key = &args[0];
    let value = args[1].clone();
    let mut deadline: Option<i64> = None;
    let mut condition = SetCondition::Always;
    let mut return_old = false;
    let mut index = 2;
    while index < args.len() {
        let option = args[index].to_ascii_uppercase();
        match option.as_slice() {
            b"NX" => condition = SetCondition::IfAbsent,
            b"XX" => condition = SetCondition::IfPresent,
            b"GET" => return_old = true,
            b"KEEPTTL" => deadline = Some(KEEP_TTL_SENTINEL),
            b"EX" | b"PX" | b"EXAT" | b"PXAT" => {
                index += 1;
                let amount = parse_int_arg(args.get(index).ok_or_else(|| syntax_error())?)?;
                if matches!(option.as_slice(), b"EX" | b"PX") && amount <= 0 {
                    return Err(RespServerError::Command(
                        "invalid expire time in 'set' command".into(),
                    ));
                }
                deadline = Some(match option.as_slice() {
                    b"EX" => now_ms().saturating_add(amount.saturating_mul(1000)),
                    b"PX" => now_ms().saturating_add(amount),
                    b"EXAT" => amount.saturating_mul(1000),
                    _ => amount,
                });
            }
            _ => return Err(syntax_error()),
        }
        index += 1;
    }
    let (written, previous) = store.set_conditional(db_index, key, value, deadline, condition)?;
    if return_old {
        write_entry_value(out, previous);
    } else if written {
        resp::write_simple(out, "OK");
    } else {
        resp::write_nil(out);
    }
    Ok(())
}

fn cmd_getex(store: &CacheStore, db_index: u8, args: &[Vec<u8>], out: &mut Vec<u8>) -> Result<()> {
    if args.is_empty() {
        return Err(arity_error("GETEX"));
    }
    let key = &args[0];
    // None = leave TTL untouched; Some(None) = PERSIST; Some(Some(ms)) = new deadline.
    let mut ttl_change: Option<Option<i64>> = None;
    let mut index = 1;
    while index < args.len() {
        let option = args[index].to_ascii_uppercase();
        match option.as_slice() {
            b"PERSIST" => ttl_change = Some(None),
            b"EX" | b"PX" | b"EXAT" | b"PXAT" => {
                index += 1;
                let amount = parse_int_arg(args.get(index).ok_or_else(|| syntax_error())?)?;
                let deadline = match option.as_slice() {
                    b"EX" => now_ms().saturating_add(amount.saturating_mul(1000)),
                    b"PX" => now_ms().saturating_add(amount),
                    b"EXAT" => amount.saturating_mul(1000),
                    _ => amount,
                };
                ttl_change = Some(Some(deadline));
            }
            _ => return Err(syntax_error()),
        }
        index += 1;
    }
    let entry = store.get(db_index, key)?;
    if entry.is_some() {
        if let Some(deadline) = ttl_change {
            store.set_expiry(db_index, key, deadline)?;
        }
    }
    write_entry_value(out, entry);
    Ok(())
}

fn cmd_scan(store: &CacheStore, db_index: u8, args: &[Vec<u8>], out: &mut Vec<u8>) -> Result<()> {
    if args.is_empty() {
        return Err(arity_error("SCAN"));
    }
    let cursor = parse_int_arg(&args[0])? as usize;
    let mut pattern: Option<Vec<u8>> = None;
    let mut count = 10usize;
    let mut index = 1;
    while index < args.len() {
        let option = args[index].to_ascii_uppercase();
        match option.as_slice() {
            b"MATCH" => {
                index += 1;
                pattern = Some(args.get(index).ok_or_else(|| syntax_error())?.clone());
            }
            b"COUNT" => {
                index += 1;
                let value = parse_int_arg(args.get(index).ok_or_else(|| syntax_error())?)?;
                if value <= 0 {
                    return Err(syntax_error());
                }
                count = value as usize;
            }
            b"TYPE" => {
                index += 1;
                let kind = args.get(index).ok_or_else(|| syntax_error())?;
                if !kind.eq_ignore_ascii_case(b"string") {
                    // Only strings exist; scanning another type yields nothing.
                    resp::write_array_header(out, 2);
                    resp::write_bulk(out, b"0");
                    resp::write_array_header(out, 0);
                    return Ok(());
                }
            }
            _ => return Err(syntax_error()),
        }
        index += 1;
    }
    // Cursor = offset into the stable (id-sorted) key listing. Weaker than
    // Redis's reverse-binary cursor but honors the same client contract:
    // iterate until the returned cursor is 0.
    let all_keys = store.keys(db_index)?;
    let window: Vec<&Vec<u8>> = all_keys.iter().skip(cursor).take(count).collect();
    let next_cursor = if cursor + window.len() >= all_keys.len() {
        0
    } else {
        cursor + window.len()
    };
    let matched: Vec<&&Vec<u8>> = window
        .iter()
        .filter(|key| pattern.as_deref().is_none_or(|pat| glob_match(pat, key)))
        .collect();
    resp::write_array_header(out, 2);
    resp::write_bulk(out, next_cursor.to_string().as_bytes());
    resp::write_array_header(out, matched.len());
    for key in matched {
        resp::write_bulk(out, key);
    }
    Ok(())
}

fn syntax_error() -> RespServerError {
    RespServerError::Command("syntax error".into())
}

fn require_hotviews(hotviews: Option<&HotViewRegistry>) -> Result<&HotViewRegistry> {
    hotviews.ok_or_else(|| {
        RespServerError::Command(
            "HotView support is disabled; start the server with --hotview".into(),
        )
    })
}

fn utf8_arg<'a>(bytes: &'a [u8], what: &str) -> Result<&'a str> {
    std::str::from_utf8(bytes)
        .map_err(|_| RespServerError::Command(format!("{what} must be valid UTF-8")))
}

/// SQL over RESP: execute a statement (or `;`-separated script) and reply
/// with a JSON object. Writes refresh/invalidate dependent hotviews *before*
/// this reply is sent, so once the caller sees the result, every dependent
/// cache entry is already consistent with the commit.
fn cmd_sql(
    store: &CacheStore,
    hotviews: &HotViewRegistry,
    args: &[Vec<u8>],
    out: &mut Vec<u8>,
) -> Result<()> {
    if args.is_empty() {
        return Err(arity_error("SQL"));
    }
    let parts: Vec<&str> = args
        .iter()
        .map(|arg| utf8_arg(arg, "SQL statement"))
        .collect::<Result<_>>()?;
    let sql = parts.join(" ");
    let (result, changed) = store.execute_sql(&sql)?;
    let (refreshed, invalidated) = hotviews.on_collections_changed(store, &changed);
    let rows: Vec<serde_json::Value> = result
        .rows
        .iter()
        .map(|row| serde_json::Value::Array(row.iter().map(hotview::sql_value_to_json).collect()))
        .collect();
    let payload = serde_json::json!({
        "columns": result.columns,
        "rows": rows,
        "command_tag": result.command_tag,
        "hotviews_refreshed": refreshed,
        "hotviews_invalidated": invalidated,
    });
    resp::write_bulk(
        out,
        &serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec()),
    );
    Ok(())
}

/// HOTVIEW.CREATE <key> <select-sql> [MODE refresh|invalidate]
fn cmd_hotview_create(
    store: &CacheStore,
    hotviews: &HotViewRegistry,
    db_index: u8,
    args: &[Vec<u8>],
    out: &mut Vec<u8>,
) -> Result<()> {
    if args.len() != 2 && args.len() != 4 {
        return Err(arity_error("HOTVIEW.CREATE"));
    }
    let key = utf8_arg(&args[0], "hotview key")?;
    let sql = utf8_arg(&args[1], "hotview query")?;
    let mode = if args.len() == 4 {
        if !args[2].eq_ignore_ascii_case(b"MODE") {
            return Err(syntax_error());
        }
        let mode_text = utf8_arg(&args[3], "hotview mode")?;
        match mode_text.to_ascii_lowercase().as_str() {
            "refresh" => RefreshMode::Refresh,
            "invalidate" => RefreshMode::Invalidate,
            _ => {
                return Err(RespServerError::Command(
                    "MODE must be refresh or invalidate".into(),
                ))
            }
        }
    } else {
        RefreshMode::Refresh
    };
    let generation = hotviews.create(store, db_index, key, sql, mode)?;
    resp::write_int(out, generation as i64);
    Ok(())
}

/// Redis-style glob: `*`, `?`, `[abc]` / `[^abc]` / `[a-z]`, and `\` escapes.
/// Iterative with single-star backtracking (linear for cache-sized keys).
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0usize;
    let mut t = 0usize;
    let mut star: Option<(usize, usize)> = None;
    while t < text.len() {
        if p < pattern.len() {
            match pattern[p] {
                b'*' => {
                    star = Some((p, t));
                    p += 1;
                    continue;
                }
                b'?' => {
                    p += 1;
                    t += 1;
                    continue;
                }
                b'[' => {
                    if let Some((matched, next_p)) = match_bracket(pattern, p, text[t]) {
                        if matched {
                            p = next_p;
                            t += 1;
                            continue;
                        }
                    }
                }
                b'\\' if p + 1 < pattern.len() => {
                    if pattern[p + 1] == text[t] {
                        p += 2;
                        t += 1;
                        continue;
                    }
                }
                literal => {
                    if literal == text[t] {
                        p += 1;
                        t += 1;
                        continue;
                    }
                }
            }
        }
        // Mismatch: backtrack to the last star, consuming one more text byte.
        if let Some((star_p, star_t)) = star {
            p = star_p + 1;
            t = star_t + 1;
            star = Some((star_p, star_t + 1));
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

/// Match one byte against a `[...]` class starting at `pattern[start] == b'['`.
/// Returns (matched, index just past `]`), or None for an unterminated class.
fn match_bracket(pattern: &[u8], start: usize, byte: u8) -> Option<(bool, usize)> {
    let mut index = start + 1;
    let negated = pattern.get(index) == Some(&b'^');
    if negated {
        index += 1;
    }
    let mut matched = false;
    let mut first = true;
    while index < pattern.len() {
        match pattern[index] {
            b']' if !first => {
                return Some((matched != negated, index + 1));
            }
            b'\\' if index + 1 < pattern.len() => {
                if pattern[index + 1] == byte {
                    matched = true;
                }
                index += 2;
            }
            low if index + 2 < pattern.len()
                && pattern[index + 1] == b'-'
                && pattern[index + 2] != b']' =>
            {
                if (low..=pattern[index + 2]).contains(&byte) {
                    matched = true;
                }
                index += 3;
            }
            literal => {
                if literal == byte {
                    matched = true;
                }
                index += 1;
            }
        }
        first = false;
    }
    None
}

#[cfg(test)]
mod glob_tests {
    use super::glob_match;

    #[test]
    fn glob_basics() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"user:*", b"user:42"));
        assert!(!glob_match(b"user:*", b"session:42"));
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[^ae]llo", b"hallo"));
        assert!(glob_match(b"h[a-c]llo", b"hbllo"));
        assert!(glob_match(b"exact", b"exact"));
        assert!(!glob_match(b"exact", b"exactly"));
        assert!(glob_match(b"a*c*e", b"abcde"));
        assert!(glob_match(b"\\*", b"*"));
        assert!(!glob_match(b"\\*", b"x"));
    }
}

#[cfg(test)]
mod auth_tests {
    use super::{cmd_auth, ConnState};

    fn state(pw: &str) -> ConnState {
        ConnState {
            db_index: 0,
            authed: false,
            password: Some(pw.to_string()),
            client_name: String::new(),
        }
    }

    #[test]
    fn constant_time_auth_still_accepts_and_rejects_correctly() {
        let mut out = Vec::new();
        let mut s = state("s3cr3t");
        cmd_auth(&mut s, &[b"s3cr3t".to_vec()], &mut out).unwrap();
        assert!(s.authed);

        let mut s = state("s3cr3t");
        assert!(cmd_auth(&mut s, &[b"XXXXXX".to_vec()], &mut out).is_err());
        assert!(!s.authed);

        let mut s = state("s3cr3t");
        assert!(cmd_auth(&mut s, &[b"s3".to_vec()], &mut out).is_err());
        assert!(!s.authed);

        let mut s = state("s3cr3t");
        assert!(cmd_auth(&mut s, &[b"s3cr3t-and-more".to_vec()], &mut out).is_err());
        assert!(!s.authed);

        let mut s = state("s3cr3t");
        cmd_auth(&mut s, &[b"default".to_vec(), b"s3cr3t".to_vec()], &mut out).unwrap();
        assert!(s.authed);
    }
}
