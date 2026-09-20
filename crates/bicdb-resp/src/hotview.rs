//! HotView: commit-aware materialized cache entries ("causal cache").
//!
//! A hotview binds a cache key to a SQL query. The materialized result (a
//! JSON array of row objects) is stored as an ordinary cache entry, so any
//! Redis client reads it with plain GET. When a write executed through the
//! server's SQL command changes a collection the query depends on, the entry
//! is recomputed (or invalidated) *before the write's reply is sent* — so the
//! writer, and anything it triggers afterwards, never observes a stale cache.
//!
//! Dependencies are extracted from the query AST at CREATE time (every
//! relation the SELECT references). Change detection does not trust parsing:
//! the SQL executor diffs per-collection generation counters around each
//! write, so triggers/cascades/multi-statement scripts invalidate correctly.
//!
//! Definitions persist in the `cache_hotviews` collection; on startup every
//! view is recomputed, so the cache comes up hot.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::ControlFlow;

use bicdb_sql::{SqlResult, SqlValue};
use parking_lot::Mutex;
use serde_json::{json, Value};
use sqlparser::ast::{visit_relations, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::store::{now_ms, CacheStore};
use crate::{RespServerError, Result};

pub const HOTVIEW_META_COLLECTION: &str = "cache_hotviews";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshMode {
    /// Recompute the materialized value on every dependent change.
    Refresh,
    /// Delete the cached value on change; the app refreshes on its schedule.
    Invalidate,
}

impl RefreshMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Refresh => "refresh",
            Self::Invalidate => "invalidate",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text.to_ascii_lowercase().as_str() {
            "refresh" => Some(Self::Refresh),
            "invalidate" => Some(Self::Invalidate),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HotViewMeta {
    pub key: String,
    pub db_index: u8,
    pub sql: String,
    pub mode: RefreshMode,
    pub deps: BTreeSet<String>,
    pub generation: u64,
    pub stale: bool,
    pub last_refresh_unix_ms: i64,
    pub last_duration_us: u64,
    pub last_error: Option<String>,
}

pub struct HotViewRegistry {
    views: Mutex<HashMap<(u8, String), HotViewMeta>>,
}

fn meta_record_id(db_index: u8, key: &str) -> String {
    format!("{db_index}:{key}")
}

/// Parse the view query, require a single SELECT, and collect every relation
/// it references (FROM, JOINs, subqueries, CTE bodies) as lowercase names.
pub fn extract_dependencies(sql: &str) -> Result<BTreeSet<String>> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|err| RespServerError::Command(format!("cannot parse view query: {err}")))?;
    let [statement] = statements.as_slice() else {
        return Err(RespServerError::Command(
            "a hotview must be exactly one statement".into(),
        ));
    };
    if !matches!(statement, Statement::Query(_)) {
        return Err(RespServerError::Command(
            "a hotview must be a SELECT query".into(),
        ));
    }
    let mut deps = BTreeSet::new();
    let _: ControlFlow<()> = visit_relations(statement, |relation| {
        if let Some(last) = relation.0.last() {
            if let Some(ident) = last.as_ident() {
                deps.insert(ident.value.to_ascii_lowercase());
            }
        }
        ControlFlow::Continue(())
    });
    if deps.is_empty() {
        return Err(RespServerError::Command(
            "view query references no tables; nothing to track".into(),
        ));
    }
    Ok(deps)
}

/// Materialize a query result as a JSON array of `{column: value}` objects.
fn result_to_json_bytes(result: &SqlResult) -> Vec<u8> {
    let rows: Vec<Value> = result
        .rows
        .iter()
        .map(|row| {
            let object: serde_json::Map<String, Value> = result
                .columns
                .iter()
                .zip(row)
                .map(|(column, value)| (column.clone(), sql_value_to_json(value)))
                .collect();
            Value::Object(object)
        })
        .collect();
    serde_json::to_vec(&Value::Array(rows)).unwrap_or_else(|_| b"[]".to_vec())
}

/// Mirror of the SQL layer's own JSON mapping (that fn is private there).
pub fn sql_value_to_json(value: &SqlValue) -> Value {
    match value {
        SqlValue::Json(inner) => inner.clone(),
        other => serde_json::to_value(other).unwrap_or(Value::Null),
    }
}

impl HotViewRegistry {
    /// Load persisted definitions and recompute every view so the cache is
    /// hot before the server accepts connections. Views whose query now fails
    /// (e.g. table dropped while down) are kept but marked stale.
    pub fn load(store: &CacheStore) -> Result<Self> {
        let registry = Self {
            views: Mutex::new(HashMap::new()),
        };
        for meta in store.load_meta(HOTVIEW_META_COLLECTION)? {
            let (Some(key), Some(db_index), Some(sql)) = (
                meta.get("key").and_then(Value::as_str),
                meta.get("db_index").and_then(Value::as_u64),
                meta.get("sql").and_then(Value::as_str),
            ) else {
                continue;
            };
            let mode = meta
                .get("mode")
                .and_then(Value::as_str)
                .and_then(RefreshMode::parse)
                .unwrap_or(RefreshMode::Refresh);
            let Ok(deps) = extract_dependencies(sql) else {
                continue;
            };
            registry.views.lock().insert(
                (db_index as u8, key.to_string()),
                HotViewMeta {
                    key: key.to_string(),
                    db_index: db_index as u8,
                    sql: sql.to_string(),
                    mode,
                    deps,
                    generation: 0,
                    stale: true,
                    last_refresh_unix_ms: 0,
                    last_duration_us: 0,
                    last_error: None,
                },
            );
        }
        let all: Vec<(u8, String)> = registry.views.lock().keys().cloned().collect();
        for (db_index, key) in all {
            let _ = registry.refresh(store, db_index, &key);
        }
        Ok(registry)
    }

    /// Define a view: validate + extract deps, persist the definition, and
    /// materialize immediately. Returns the first generation number.
    pub fn create(
        &self,
        store: &CacheStore,
        db_index: u8,
        key: &str,
        sql: &str,
        mode: RefreshMode,
    ) -> Result<u64> {
        let deps = extract_dependencies(sql)?;
        store.save_meta(
            HOTVIEW_META_COLLECTION,
            &meta_record_id(db_index, key),
            json!({
                "key": key,
                "db_index": db_index,
                "sql": sql,
                "mode": mode.as_str(),
            }),
        )?;
        self.views.lock().insert(
            (db_index, key.to_string()),
            HotViewMeta {
                key: key.to_string(),
                db_index,
                sql: sql.to_string(),
                mode,
                deps,
                generation: 0,
                stale: true,
                last_refresh_unix_ms: 0,
                last_duration_us: 0,
                last_error: None,
            },
        );
        self.refresh(store, db_index, key)
    }

    /// Remove the definition and the cached value. Returns whether it existed.
    pub fn drop_view(&self, store: &CacheStore, db_index: u8, key: &str) -> Result<bool> {
        let existed = self
            .views
            .lock()
            .remove(&(db_index, key.to_string()))
            .is_some();
        if existed {
            store.delete_meta(HOTVIEW_META_COLLECTION, &meta_record_id(db_index, key))?;
            store.delete(db_index, &[key.as_bytes()])?;
        }
        Ok(existed)
    }

    pub fn list(&self, db_index: u8) -> Vec<String> {
        let mut keys: Vec<String> = self
            .views
            .lock()
            .keys()
            .filter(|(index, _)| *index == db_index)
            .map(|(_, key)| key.clone())
            .collect();
        keys.sort();
        keys
    }

    pub fn status(&self, db_index: u8, key: &str) -> Option<HotViewMeta> {
        self.views.lock().get(&(db_index, key.to_string())).cloned()
    }

    /// Recompute one view and store the fresh value under its cache key.
    /// On query failure the view is marked stale and the error recorded.
    pub fn refresh(&self, store: &CacheStore, db_index: u8, key: &str) -> Result<u64> {
        let sql = {
            let views = self.views.lock();
            let Some(view) = views.get(&(db_index, key.to_string())) else {
                return Err(RespServerError::Command(format!("no such hotview '{key}'")));
            };
            view.sql.clone()
        };
        let started = std::time::Instant::now();
        let computed = store
            .query_sql(&sql)
            .map(|result| result_to_json_bytes(&result));
        let duration_us = started.elapsed().as_micros() as u64;
        match computed {
            Ok(bytes) => {
                store.set(db_index, key.as_bytes(), bytes, None)?;
                let mut views = self.views.lock();
                let Some(view) = views.get_mut(&(db_index, key.to_string())) else {
                    return Err(RespServerError::Command(format!("no such hotview '{key}'")));
                };
                view.generation += 1;
                view.stale = false;
                view.last_refresh_unix_ms = now_ms();
                view.last_duration_us = duration_us;
                view.last_error = None;
                Ok(view.generation)
            }
            Err(err) => {
                let message = err.to_string();
                if let Some(view) = self.views.lock().get_mut(&(db_index, key.to_string())) {
                    view.stale = true;
                    view.last_error = Some(message.clone());
                }
                Err(RespServerError::Command(message))
            }
        }
    }

    /// React to a set of changed collections: refresh or invalidate every
    /// dependent view. Returns (refreshed, invalidated) counts. A failing
    /// view is marked stale but never blocks the write that triggered it.
    pub fn on_collections_changed(
        &self,
        store: &CacheStore,
        changed: &HashSet<String>,
    ) -> (usize, usize) {
        if changed.is_empty() {
            return (0, 0);
        }
        let affected: Vec<(u8, String, RefreshMode)> = self
            .views
            .lock()
            .values()
            .filter(|view| view.deps.iter().any(|dep| changed.contains(dep)))
            .map(|view| (view.db_index, view.key.clone(), view.mode))
            .collect();
        let mut refreshed = 0usize;
        let mut invalidated = 0usize;
        for (db_index, key, mode) in affected {
            match mode {
                RefreshMode::Refresh => {
                    if self.refresh(store, db_index, &key).is_ok() {
                        refreshed += 1;
                    }
                }
                RefreshMode::Invalidate => {
                    if store.delete(db_index, &[key.as_bytes()]).is_ok() {
                        if let Some(view) = self.views.lock().get_mut(&(db_index, key.clone())) {
                            view.stale = true;
                        }
                        invalidated += 1;
                    }
                }
            }
        }
        (refreshed, invalidated)
    }
}
