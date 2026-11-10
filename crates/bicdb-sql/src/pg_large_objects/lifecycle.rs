//! Host transaction boundaries; these operations never perform database I/O.
use super::*;

/// Opaque connection-descriptor savepoint. Positions of surviving descriptors
/// are deliberately outside transactional rollback, matching PostgreSQL.
#[derive(Clone, Copy, Debug)]
pub struct SqlLargeObjectSavepoint(Option<descriptors::DescriptorSavepoint>);

/// Close implicit-transaction descriptors on every exit, including cancellation
/// and unwinding, while the host retains its connection catalog cache.
pub struct SqlLargeObjectTransactionCleanup(DescriptorState);

impl Drop for SqlLargeObjectTransactionCleanup {
    fn drop(&mut self) {
        self.0.clear();
    }
}

impl SqlSessionCatalogCache {
    pub fn large_object_savepoint(&self) -> Result<SqlLargeObjectSavepoint> {
        self.pg_large_objects.savepoint()
    }

    pub fn rollback_large_objects_to(&self, mark: SqlLargeObjectSavepoint) -> Result<()> {
        self.pg_large_objects.restore(mark)
    }

    pub fn close_transaction_large_objects(&self) {
        self.pg_large_objects.clear();
    }

    pub fn large_object_transaction_cleanup(&self) -> SqlLargeObjectTransactionCleanup {
        SqlLargeObjectTransactionCleanup(self.pg_large_objects.clone())
    }

    pub fn large_object_retained_bytes(&self) -> usize {
        // A poisoned registry cannot silently disappear from resource accounting.
        self.pg_large_objects.retained_bytes().unwrap_or(usize::MAX)
    }
}

impl DescriptorState {
    pub(super) fn savepoint(&self) -> Result<SqlLargeObjectSavepoint> {
        self.current_mark().map(SqlLargeObjectSavepoint)
    }

    pub(super) fn restore(&self, mark: SqlLargeObjectSavepoint) -> Result<()> {
        if let Some(mark) = mark.0 {
            self.rollback_to(mark)
        } else {
            // No context/handle existed at SAVEPOINT. Every current handle was
            // opened in that subtransaction, including after repeated rollback.
            self.clear();
            Ok(())
        }
    }
}
