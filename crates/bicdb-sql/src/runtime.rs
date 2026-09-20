use std::collections::BTreeMap;
use std::fmt::Debug;

use bicdb_core::CancellationToken;

use crate::{Result, SqlValue};

/// Connection-local PostgreSQL behavior supplied by a wire-protocol runtime.
///
/// The standalone SQL engine intentionally has no process/session registry.
/// Servers that do have one provide it through this interface so built-ins
/// such as `pg_backend_pid()` and advisory locks retain PostgreSQL semantics
/// even when invoked from inside stored routines.
pub trait SqlSessionRuntime: Debug + Send + Sync {
    fn backend_pid(&self) -> i32;

    fn execute_advisory_lock(
        &self,
        name: &str,
        args: &[SqlValue],
        cancellation: &CancellationToken,
    ) -> Result<Option<SqlValue>>;

    fn advisory_lock_rows(&self, database_oid: i64) -> Vec<BTreeMap<String, SqlValue>>;
}
