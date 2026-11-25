//! Lifetime of session temporary schemas (`pg_temp_<backend>`): a
//! connection's temporary tables are dropped when it ends, and none survives
//! a restart of the server.
use super::*;

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
    Ok(true)
}

/// Drops every temporary schema left behind by connections of an earlier
/// server process. Returns how many there were.
pub fn drop_orphan_temp_schemas(db: &mut BicDb) -> Result<usize> {
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
    }
    Ok(schemas.len())
}
