//! `pg_export_snapshot()` and `SET TRANSACTION SNAPSHOT 'id'`: a
//! repeatable-read transaction exports its fixed MVCC snapshot, and another
//! session's repeatable-read transaction adopts exactly that snapshot before
//! its first query, as PostgreSQL's exported snapshots work.
//!
//! Exported snapshots are registered per database and are valid while the
//! exporting transaction is pending; the identifier only names a registered
//! entry, so a forged identifier cannot select an arbitrary snapshot.

use crate::*;
use bicdb_core::{TransactionId, TransactionIsolation};
use std::sync::{LazyLock, Mutex};

#[derive(Clone, Copy)]
struct Exported {
    watermark: u64,
    exporter: TransactionId,
}

static EXPORTED: LazyLock<Mutex<HashMap<(u64, String), Exported>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn invalid_identifier(id: &str) -> SqlError {
    SqlError::invalid_parameter_value(format!("invalid snapshot identifier: \"{id}\""))
}

fn well_formed(id: &str) -> bool {
    let parts = id.split('-').collect::<Vec<_>>();
    matches!(parts.as_slice(), [a, b, c]
        if !a.is_empty() && !b.is_empty() && !c.is_empty()
            && a.chars().all(|ch| ch.is_ascii_hexdigit())
            && b.chars().all(|ch| ch.is_ascii_hexdigit())
            && c.chars().all(|ch| ch.is_ascii_digit()))
}

impl SqlSession<'_> {
    /// `pg_export_snapshot()`.
    pub(crate) fn export_transaction_snapshot(&mut self) -> Result<SqlValue> {
        let instance = self.db_ref().instance_id();
        let Some(tx) = self.tx.as_ref() else {
            return Err(SqlError::Unsupported(
                "pg_export_snapshot outside a transaction block is not supported".into(),
            ));
        };
        if tx.isolation() != TransactionIsolation::RepeatableRead {
            // A read-committed snapshot moves with every statement; keeping an
            // exported one alive would need a separate registration.
            return Err(SqlError::Unsupported(
                "pg_export_snapshot requires a REPEATABLE READ transaction".into(),
            ));
        }
        let exported = Exported {
            watermark: tx.visibility_watermark(),
            exporter: tx.id(),
        };
        let mut registry = EXPORTED.lock().unwrap_or_else(|poison| poison.into_inner());
        let db = self.db_ref();
        registry.retain(|(database, _), entry| {
            *database != instance || db.transaction_is_pending(entry.exporter)
        });
        let count = registry
            .keys()
            .filter(|(database, _)| *database == instance)
            .count();
        let id = format!(
            "{:08X}-{:08X}-{}",
            exported.exporter.0,
            exported.watermark,
            count + 1
        );
        registry.insert((instance, id.clone()), exported);
        Ok(SqlValue::String(id))
    }

    /// `SET TRANSACTION SNAPSHOT 'id'`.
    pub(crate) fn set_transaction_snapshot(&mut self, id: &str) -> Result<()> {
        let instance = self.db_ref().instance_id();
        let Some(tx) = self.tx.as_ref() else {
            return Err(SqlError::InvalidTransactionState {
                message: "SET TRANSACTION SNAPSHOT can only be used in transaction blocks".into(),
            });
        };
        if tx.isolation() != TransactionIsolation::RepeatableRead {
            return Err(SqlError::Unsupported(
                "a snapshot-importing transaction must have isolation level REPEATABLE READ; SERIALIZABLE import is not supported"
                    .into(),
            ));
        }
        if !well_formed(id) {
            return Err(invalid_identifier(id));
        }
        let entry = EXPORTED
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(&(instance, id.to_string()))
            .copied()
            .filter(|entry| self.db_ref().transaction_is_pending(entry.exporter))
            .ok_or_else(|| invalid_identifier(id))?;
        let tx = self.tx.as_mut().expect("checked above");
        tx.adopt_exported_snapshot(entry.watermark, entry.exporter)
            .map_err(|error| match error {
                BicDbError::TransactionConflict(message)
                    if message.contains("before transaction access") =>
                {
                    SqlError::data_exception_public(
                        "0A000",
                        "SET TRANSACTION SNAPSHOT must be called before any query",
                        None,
                    )
                }
                BicDbError::TransactionConflict(_) => invalid_identifier(id),
                other => other.into(),
            })
    }
}
