//! EXPERIMENTAL, disposable test stores only: create a database whose page
//! store may carry page-delta WAL records. See
//! `docs/page-delta-redo-design.md`.
//!
//! Deltas need two locks: this creation-time opt-in, which writes the sticky
//! `BICDBPG5` superblock marker on a brand-new page store, and the process
//! gate `BICDB_PAGE_WAL_DELTAS=1`. A database created here cannot be opened by
//! engines that predate the marker and is neither upgradeable nor
//! downgradeable. Never use it for data you want to keep.

use std::path::Path;

use crate::error::{BicDbError, Result};
use crate::{DbConfig, DEFAULT_PAGED_DIR};

/// Create the page store of a NEW `server_paged` database at `db_path` as a
/// disposable experimental page-delta store. Refuses when `db_path` already
/// holds a page store. Open the database normally afterwards; deltas are
/// written only while `BICDB_PAGE_WAL_DELTAS=1`.
pub fn create_disposable_page_delta_store(
    db_path: impl AsRef<Path>,
    config: &DbConfig,
) -> Result<()> {
    let paged_dir = db_path.as_ref().join(DEFAULT_PAGED_DIR);
    if paged_dir.join("store.pages").try_exists()? {
        return Err(BicDbError::PagedStorage(format!(
            "{} already holds a page store; experimental page deltas can only be enabled \
             when a disposable test store is created",
            paged_dir.display()
        )));
    }
    let options = bicdb_page::PagedStoreOptions::default()
        .with_page_size(config.paged_page_size)
        .with_fsync(config.fsync)
        .with_wal_max_bytes(config.paged_wal_max_bytes)
        .with_wal_segment_bytes(config.paged_wal_segment_bytes)
        .with_extent_bytes(config.paged_extent_bytes)
        .with_experimental_page_deltas_disposable(true);
    let (store, _) = bicdb_page::PagedStore::open(&paged_dir, options)
        .map_err(|error| BicDbError::PagedStorage(error.to_string()))?;
    drop(store);
    Ok(())
}
