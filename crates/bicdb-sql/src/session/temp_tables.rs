//! Lifetime of session temporary schemas (`pg_temp_<backend>`): a
//! connection's temporary tables are dropped when it ends, and none survives
//! a restart of the server.
use super::*;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;

/// Backend numbers for sessions without a connection: far above connection
/// ids, unique in the process.
static NEXT_EMBEDDED_BACKEND: AtomicI32 = AtomicI32::new(1_000_000_000);

pub(crate) fn next_embedded_temp_backend() -> i32 {
    NEXT_EMBEDDED_BACKEND.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OnCommitAction {
    DeleteRows,
    Drop,
}

/// `ON COMMIT DELETE ROWS` / `ON COMMIT DROP` of temporary tables, by
/// temporary schema then physical table. Temporary tables do not outlive the
/// process, so neither does this registry.
static ON_COMMIT: Mutex<BTreeMap<String, BTreeMap<String, OnCommitAction>>> =
    Mutex::new(BTreeMap::new());

pub(crate) fn register_on_commit(temp_schema: &str, table: &str, action: OnCommitAction) {
    ON_COMMIT
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry(temp_schema.to_string())
        .or_default()
        .insert(table.to_string(), action);
}

fn on_commit_actions(temp_schema: &str) -> Vec<(String, OnCommitAction)> {
    ON_COMMIT
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(temp_schema)
        .map(|tables| {
            tables
                .iter()
                .map(|(table, action)| (table.clone(), *action))
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn forget_on_commit(temp_schema: &str, table: Option<&str>) {
    let mut registry = ON_COMMIT
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match table {
        Some(table) => {
            if let Some(tables) = registry.get_mut(temp_schema) {
                tables.remove(table);
            }
        }
        None => {
            registry.remove(temp_schema);
        }
    }
}

/// Drops the ON COMMIT DROP tables of backend `backend_pid` once its
/// transaction has committed.
pub fn drop_temp_on_commit_tables(db: &mut BicDb, backend_pid: i32) -> Result<()> {
    let temp = temp_schema_for_backend(backend_pid);
    let drops = on_commit_actions(&temp)
        .into_iter()
        .filter(|(_, action)| *action == OnCommitAction::Drop)
        .map(|(table, _)| table)
        .collect::<Vec<_>>();
    if drops.is_empty() {
        return Ok(());
    }
    let _effects = crate::statement_context::StatementScope::enter();
    let mut session = SqlSession::new(db);
    for table in drops {
        forget_on_commit(&temp, Some(&table));
        if load_schema(session.db_ref(), &table)?.is_none() {
            continue;
        }
        let name = format!(
            "\"{}\".\"{}\"",
            temp.replace('"', "\"\""),
            catalog_display_name(&table).replace('"', "\"\"")
        );
        session.execute(&format!("DROP TABLE {name}"))?;
    }
    Ok(())
}

/// Whether temporary schema `temp_schema` has ON COMMIT actions to run.
pub fn temp_on_commit_pending(temp_schema: &str) -> bool {
    !on_commit_actions(temp_schema).is_empty()
}

/// Whether temporary schema `temp_schema` has ON COMMIT DELETE ROWS tables.
pub fn temp_on_commit_delete_pending(temp_schema: &str) -> bool {
    on_commit_actions(temp_schema)
        .iter()
        .any(|(_, action)| *action == OnCommitAction::DeleteRows)
}

/// Whether temporary schema `temp_schema` has ON COMMIT DROP tables.
pub fn temp_on_commit_drop_pending(temp_schema: &str) -> bool {
    on_commit_actions(temp_schema)
        .iter()
        .any(|(_, action)| *action == OnCommitAction::Drop)
}

impl<'db> SqlSession<'db> {
    /// Runs this session's ON COMMIT actions in its pending transaction, for
    /// a server that commits the transaction itself.
    /// ON COMMIT DROP tables are left to [`drop_temp_on_commit_tables`]
    /// after the commit.
    pub fn run_temp_on_commit_actions(&mut self) -> Result<()> {
        self.apply_temp_on_commit_actions_with(false)
    }

    /// Runs the ON COMMIT actions of this session's temporary tables inside
    /// the committing transaction: DELETE ROWS empties the table, DROP drops
    /// it. A table that no longer exists is forgotten.
    pub(crate) fn apply_temp_on_commit_actions(&mut self) -> Result<()> {
        self.apply_temp_on_commit_actions_with(true)
    }

    fn apply_temp_on_commit_actions_with(&mut self, drop_tables: bool) -> Result<()> {
        let Some(temp) = self.temp_schema_name() else {
            return Ok(());
        };
        let actions = on_commit_actions(&temp);
        if actions.is_empty() {
            return Ok(());
        }
        for (table, action) in actions {
            if load_schema(self.db_ref(), &table)?.is_none() {
                forget_on_commit(&temp, Some(&table));
                continue;
            }
            let name = format!(
                "\"{}\".\"{}\"",
                temp.replace('"', "\"\""),
                catalog_display_name(&table).replace('"', "\"\"")
            );
            match action {
                OnCommitAction::DeleteRows => {
                    // A read-only commit of an empty table writes nothing.
                    let probe =
                        self.execute_text_statements(&format!("SELECT 1 FROM {name} LIMIT 1"))?;
                    if !probe.rows.is_empty() {
                        self.execute_text_statements(&format!("TRUNCATE {name}"))?;
                    }
                }
                OnCommitAction::Drop if drop_tables => {
                    self.execute_text_statements(&format!("DROP TABLE {name}"))?;
                    forget_on_commit(&temp, Some(&table));
                }
                OnCommitAction::Drop => {}
            }
        }
        Ok(())
    }
}

/// Whether backend `backend_pid` has a temporary schema.
pub fn temp_schema_exists(db: &BicDb, backend_pid: i32) -> bool {
    load_namespace(db, &temp_schema_for_backend(backend_pid))
        .ok()
        .flatten()
        .is_some()
}

/// Drops the temporary schema of backend `backend_pid` (a connection that
/// ended) with everything in it. Returns whether there was one.
pub fn drop_connection_temp_schema(db: &mut BicDb, backend_pid: i32) -> Result<bool> {
    let schema = temp_schema_for_backend(backend_pid);
    if load_namespace(db, &schema)?.is_none() {
        return Ok(false);
    }
    let _effects = crate::statement_context::StatementScope::enter();
    let mut session = SqlSession::new(db);
    session.drop_temp_schema_named(&schema)?;
    forget_on_commit(&schema, None);
    Ok(true)
}

/// Drops every temporary schema left behind by connections of an earlier
/// server process. Returns how many there were.
pub fn drop_orphan_temp_schemas(db: &mut BicDb) -> Result<usize> {
    repair_orphan_views(db)?;
    let schemas = list_namespaces(db)?
        .into_iter()
        .map(|namespace| namespace.name)
        .filter(|name| crate::search_path::is_temp_schema(name))
        .collect::<Vec<_>>();
    if schemas.is_empty() {
        return Ok(0);
    }
    let _effects = crate::statement_context::StatementScope::enter();
    let mut session = SqlSession::new(db);
    for schema in &schemas {
        session.drop_temp_schema_named(schema)?;
        forget_on_commit(schema, None);
    }
    Ok(schemas.len())
}

/// Drops the temporary schema of a session without a connection when the
/// session ends. Held as the session's last field, so it runs after the
/// session's transaction state is gone.
#[derive(Debug)]
pub(crate) struct EmbeddedTempGuard {
    db: std::ptr::NonNull<BicDb>,
    backend: i32,
}

impl EmbeddedTempGuard {
    pub(crate) fn new(db: std::ptr::NonNull<BicDb>, backend: i32) -> Self {
        Self { db, backend }
    }
}

impl Drop for EmbeddedTempGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        // Safety: the guard is created only for an exclusive session, whose
        // `&'db mut BicDb` outlives the session and so this guard.
        let db = unsafe { self.db.as_mut() };
        let _ = drop_connection_temp_schema(db, self.backend);
    }
}

/// Removes views (and rules) left without a schema by earlier builds: a
/// view created as `pg_temp.v` was stored under a schema that never
/// existed, and a permanent view over a temporary table outlived it. Either
/// made every catalog read fail with "relation ... has no matching
/// namespace". A view holds no rows, so dropping one loses no data; tables
/// and sequences are left alone. Returns how many views were removed.
pub fn repair_orphan_views(db: &mut BicDb) -> Result<usize> {
    let orphans = list_views(db)?
        .into_iter()
        .map(|view| view.name)
        .filter(|name| {
            name.starts_with("__bicdb_s_") && relation_catalog_namespace(db, name).is_err()
        })
        .collect::<Vec<_>>();
    for view in &orphans {
        delete_view(db, view)?;
    }
    let orphan_rules = crate::query_rules::list_rules(db)?
        .iter()
        .filter(|rule| !relation_exists(db, &rule.table))
        .map(|rule| rule.table.clone())
        .collect::<Vec<_>>();
    crate::query_rules::drop_rules_of_relations(db, &orphan_rules)?;
    Ok(orphans.len())
}
