//! PostgreSQL large-object storage and descriptor support.
//! SQL/fastpath dispatch is integrated only with its privilege/lifetime gates.
use crate::*;

mod storage;

pub(crate) const METADATA: &str = "__bicdb_pg_large_objects";
pub(crate) const PAGES: &str = "__bicdb_pg_large_object_pages";
