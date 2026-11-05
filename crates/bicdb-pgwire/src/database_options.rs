//! Stable, process-local engine tuning for server configuration files.
use super::*;

macro_rules! options {
    ($($name:ident: $kind:ty),* $(,)?) => {
        #[derive(Clone, Debug, Default, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct ServerDatabaseOptions {
            $(#[serde(default, skip_serializing_if = "Option::is_none")]
            pub $name: Option<$kind>,)*
        }
        impl ServerDatabaseOptions {
            pub fn apply(&self, config: &mut DbConfig) {
                $(if let Some(value) = self.$name { config.$name = value; })*
            }
            pub fn effective_values(&self) -> serde_json::Value {
                let mut config = DbConfig::default();
                self.apply(&mut config);
                serde_json::json!({$(stringify!($name): config.$name),*})
            }
        }
    }
}

options! {
    paged_buffer_pool_bytes: u64,
    paged_read_ahead_queue_pages: usize,
    paged_extent_bytes: u64,
    paged_value_compression: bool,
    snapshot: bool,
    sync_outbox: bool,
    audit_events: bool,
    fts_query_partitions: usize,
    fts_packed_segments: bool,
    fts_build_memory_bytes: usize,
    fts_build_workers: usize,
    fts_block_cache_bytes: usize,
    fts_prefetch_blocks: usize,
    query_work_memory_bytes: usize,
    query_temp_space_bytes: u64,
    query_server_temp_space_bytes: u64,
    query_merge_fan_in: usize,
}

impl ServerDatabaseOptions {
    pub fn validate(&self) -> Result<()> {
        let provided = serde_json::to_value(self).expect("serializable options");
        for (name, value) in provided.as_object().expect("option map") {
            if matches!(
                name.as_str(),
                "paged_buffer_pool_bytes" | "fts_build_memory_bytes" | "fts_build_workers"
            ) && value.as_u64() == Some(0)
            {
                return Err(PgWireError::Server(format!(
                    "engine.{name} must be positive"
                )));
            }
        }
        if self.fts_build_workers.is_some_and(|workers| workers > 64) {
            return Err(PgWireError::Server(
                "engine.fts_build_workers must be at most 64".into(),
            ));
        }
        if self
            .fts_build_memory_bytes
            .is_some_and(|bytes| bytes < 64 * 1024)
        {
            return Err(PgWireError::Server(
                "engine.fts_build_memory_bytes must be at least 65536".into(),
            ));
        }
        if self
            .paged_buffer_pool_bytes
            .is_some_and(|bytes| bytes < bicdb_core::MIN_PAGE_SIZE as u64)
        {
            return Err(PgWireError::Server(
                "engine.paged_buffer_pool_bytes must hold at least one minimum-size page".into(),
            ));
        }
        for (name, bytes) in [
            (
                "query_work_memory_bytes",
                self.query_work_memory_bytes.map(|v| v as u64),
            ),
            ("query_temp_space_bytes", self.query_temp_space_bytes),
            (
                "query_server_temp_space_bytes",
                self.query_server_temp_space_bytes,
            ),
        ] {
            if bytes.is_some_and(|v| v < 64 * 1024) {
                return Err(PgWireError::Server(format!(
                    "engine.{name} must be at least 65536"
                )));
            }
        }
        if self
            .query_merge_fan_in
            .is_some_and(|value| !(2..=128).contains(&value))
        {
            return Err(PgWireError::Server(
                "engine.query_merge_fan_in must be between 2 and 128".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn meaningful_zero_values_are_preserved_and_budgets_are_checked() {
        ServerDatabaseOptions {
            paged_extent_bytes: Some(0),
            fts_query_partitions: Some(0),
            paged_read_ahead_queue_pages: Some(0),
            fts_block_cache_bytes: Some(0),
            fts_prefetch_blocks: Some(0),
            ..Default::default()
        }
        .validate()
        .unwrap();
        assert!(ServerDatabaseOptions {
            fts_build_workers: Some(65),
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(ServerDatabaseOptions {
            query_work_memory_bytes: Some(1),
            ..Default::default()
        }
        .validate()
        .is_err());
    }
}
