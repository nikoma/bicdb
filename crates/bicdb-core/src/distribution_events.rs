//! Range-local, idempotent application events over the member-authenticated
//! data plane. Events are immutable; reads are bounded and eventually consistent.
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{distribution_key_token, BicDb, BicDbError, RangeDescriptor, RangeId, Record, Result};

pub const CLUSTER_EVENTS_COLLECTION: &str = "__bic_cluster_events";
pub const MAX_CLUSTER_EVENT_BYTES: usize = 64 * 1024;
pub const MAX_CLUSTER_EVENT_PAGE_ROWS: usize = 128;

fn invalid(message: &str) -> BicDbError {
    BicDbError::Cluster(message.to_owned())
}

fn component(value: &str, max: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err(invalid(
            "event names must use bounded ASCII letters, digits, '-', '_' or '.'",
        ));
    }
    Ok(())
}

pub fn cluster_event_record_id(namespace: &str, key: &str) -> Result<String> {
    component(namespace, 128)?;
    component(key, 256)?;
    Ok(format!("{namespace}/{key}"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClusterEventOperation {
    Append {
        namespace: String,
        key: String,
        value: Value,
    },
    Get {
        namespace: String,
        key: String,
    },
    /// The cursor is the last examined key, including records filtered out by
    /// range. Restart from None to see insertions before an earlier cursor.
    Scan {
        namespace: String,
        after: Option<String>,
        limit: usize,
    },
}

impl ClusterEventOperation {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Append {
                namespace,
                key,
                value,
            } => {
                cluster_event_record_id(namespace, key)?;
                if serde_json::to_vec(value)?.len() > MAX_CLUSTER_EVENT_BYTES {
                    return Err(invalid("event value exceeds 64 KiB"));
                }
            }
            Self::Get { namespace, key } => {
                cluster_event_record_id(namespace, key)?;
            }
            Self::Scan {
                namespace,
                after,
                limit,
            } => {
                component(namespace, 128)?;
                if let Some(after) = after {
                    component(after, 256)?;
                }
                if *limit == 0 || *limit > MAX_CLUSTER_EVENT_PAGE_ROWS {
                    return Err(invalid("event page limit must be between 1 and 128"));
                }
            }
        }
        Ok(())
    }

    pub fn point_record_id(&self) -> Result<Option<String>> {
        self.validate()?;
        match self {
            Self::Append { namespace, key, .. } | Self::Get { namespace, key } => {
                Ok(Some(cluster_event_record_id(namespace, key)?))
            }
            Self::Scan { .. } => Ok(None),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterEventRequest {
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub operation: ClusterEventOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClusterEvent {
    pub key: String,
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum ClusterEventResponse {
    Appended {
        inserted: bool,
    },
    Value {
        value: Option<Value>,
    },
    Page {
        events: Vec<ClusterEvent>,
        next: Option<String>,
    },
}

/// Cursor for an eventually consistent walk over the current range set.
/// Membership changes preserve range IDs. A fresh walk discovers events added
/// before an earlier cursor; consumers deduplicate by their stable event keys.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterEventCursor {
    pub range_id: RangeId,
    pub after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClusterEventPage {
    pub events: Vec<ClusterEvent>,
    pub next: Option<ClusterEventCursor>,
}

#[cfg(feature = "tls-replication")]
mod client {
    use super::*;
    use crate::{
        ClusterNodeId, ClusterTopology, DistributionConfig, TcpClusterRelocationTransport,
    };
    use parking_lot::RwLock;
    use std::sync::Arc;

    /// Trusted host-side client. Membership certificates stay with this host;
    /// never provide them, this client, or arbitrary namespaces to guest code.
    pub struct ClusterEventClient {
        topology: Arc<RwLock<ClusterTopology>>,
        transport: TcpClusterRelocationTransport,
        request_timeout: std::time::Duration,
    }

    impl ClusterEventClient {
        pub fn new(config: &DistributionConfig, topology: ClusterTopology) -> Result<Self> {
            let topology = Arc::new(RwLock::new(topology));
            let transport = TcpClusterRelocationTransport::new(
                config.cluster_id.clone(),
                config.node_id.clone(),
                Arc::clone(&topology),
                config.transport.clone(),
            )?;
            Ok(Self {
                topology,
                transport,
                request_timeout: std::time::Duration::from_secs(30),
            })
        }

        /// Bound route discovery and retry work. One in-flight transport call
        /// may outlive this budget by its configured connect/I/O timeout.
        pub fn with_request_timeout(mut self, timeout: std::time::Duration) -> Result<Self> {
            if timeout.is_zero() || timeout > std::time::Duration::from_secs(300) {
                return Err(invalid(
                    "event request timeout must be positive and at most 300 seconds",
                ));
            }
            self.request_timeout = timeout;
            Ok(self)
        }

        /// Discover a newer committed topology from known members. Equal
        /// generations with conflicting contents are rejected by the transport.
        pub fn refresh(&self) -> Result<()> {
            self.refresh_until(std::time::Instant::now() + self.request_timeout)
        }

        fn refresh_until(&self, deadline: std::time::Instant) -> Result<()> {
            let members: Vec<ClusterNodeId> = self.topology.read().nodes.keys().cloned().collect();
            let mut observed = false;
            for member in members {
                if std::time::Instant::now() >= deadline {
                    break;
                }
                if let Ok(topology) = self.transport.fetch_topology(&member) {
                    self.transport.install_topology(topology)?;
                    observed = true;
                }
            }
            if !observed {
                return Err(invalid("no event cluster member is reachable"));
            }
            Ok(())
        }

        fn request(
            &self,
            operation: ClusterEventOperation,
            scan_range: Option<RangeId>,
        ) -> Result<ClusterEventResponse> {
            operation.validate()?;
            let deadline = std::time::Instant::now() + self.request_timeout;
            let mut last_error = invalid("event cluster request failed");
            for attempt in 0..5 {
                if std::time::Instant::now() >= deadline {
                    break;
                }
                if attempt > 0 {
                    std::thread::sleep(
                        std::time::Duration::from_millis(100 << (attempt - 1))
                            .min(deadline.saturating_duration_since(std::time::Instant::now())),
                    );
                    self.refresh_until(deadline)?;
                }
                if std::time::Instant::now() >= deadline {
                    break;
                }
                let (destination, request) = {
                    let topology = self.topology.read();
                    let range = if let Some(id) = scan_range {
                        topology
                            .range_by_id(id)
                            .ok_or_else(|| invalid("unknown event cursor range"))?
                    } else {
                        let id = operation
                            .point_record_id()?
                            .ok_or_else(|| invalid("missing event point key"))?;
                        topology.range_for_key(CLUSTER_EVENTS_COLLECTION, &id)?
                    };
                    (
                        range.leader.clone(),
                        ClusterEventRequest {
                            range_id: range.id,
                            range_epoch: range.epoch,
                            operation: operation.clone(),
                        },
                    )
                };
                match self.transport.application_event(&destination, request) {
                    Ok(response) => return Ok(response),
                    Err(error) => last_error = error,
                }
            }
            Err(last_error)
        }

        /// Return true for a newly committed event, false for an identical
        /// retry. Reusing a key with different content is an error. A transport
        /// failure may have an unknown outcome; retry with the exact same key.
        pub fn append(&self, namespace: &str, key: &str, value: Value) -> Result<bool> {
            match self.request(
                ClusterEventOperation::Append {
                    namespace: namespace.into(),
                    key: key.into(),
                    value,
                },
                None,
            )? {
                ClusterEventResponse::Appended { inserted } => Ok(inserted),
                _ => Err(invalid("unexpected event append response")),
            }
        }

        pub fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>> {
            match self.request(
                ClusterEventOperation::Get {
                    namespace: namespace.into(),
                    key: key.into(),
                },
                None,
            )? {
                ClusterEventResponse::Value { value } => Ok(value),
                _ => Err(invalid("unexpected event get response")),
            }
        }

        /// Read at most one bounded range page. An empty page may still have
        /// a continuation. There is no global ordering or cross-range snapshot.
        pub fn scan(
            &self,
            namespace: &str,
            cursor: Option<ClusterEventCursor>,
            limit: usize,
        ) -> Result<ClusterEventPage> {
            let range_ids: Vec<RangeId> = self
                .topology
                .read()
                .ranges
                .values()
                .map(|range| range.id)
                .collect();
            let cursor = cursor.unwrap_or(ClusterEventCursor {
                range_id: *range_ids
                    .first()
                    .ok_or_else(|| invalid("event cluster has no ranges"))?,
                after: None,
            });
            let index = range_ids
                .iter()
                .position(|id| *id == cursor.range_id)
                .ok_or_else(|| invalid("unknown event cursor range"))?;
            match self.request(
                ClusterEventOperation::Scan {
                    namespace: namespace.into(),
                    after: cursor.after,
                    limit,
                },
                Some(cursor.range_id),
            )? {
                ClusterEventResponse::Page { events, next } => {
                    let next = next
                        .map(|key| ClusterEventCursor {
                            range_id: cursor.range_id,
                            after: Some(key),
                        })
                        .or_else(|| {
                            range_ids.get(index + 1).map(|id| ClusterEventCursor {
                                range_id: *id,
                                after: None,
                            })
                        });
                    Ok(ClusterEventPage { events, next })
                }
                _ => Err(invalid("unexpected event scan response")),
            }
        }
    }
}

#[cfg(feature = "tls-replication")]
pub use client::ClusterEventClient;

pub(crate) fn execute_cluster_event(
    db: &BicDb,
    range: &RangeDescriptor,
    operation: ClusterEventOperation,
) -> Result<ClusterEventResponse> {
    operation.validate()?;
    if !db.is_server_paged() {
        return Err(invalid("cluster events require bounded paged storage"));
    }
    if let Some(id) = operation.point_record_id()? {
        if !range.contains_token(distribution_key_token(CLUSTER_EVENTS_COLLECTION, &id)) {
            return Err(invalid("event key is outside the requested range"));
        }
    }
    match operation {
        ClusterEventOperation::Append {
            namespace,
            key,
            value,
        } => {
            // Commit admission is mandatory: the engine verifies the current
            // leader, recovered epoch and schema and replicates to a quorum.
            // Do not turn this RPC into a bypass for standalone mutations.
            if !db.commit_admission_ready() {
                return Err(invalid("event write admission is not ready"));
            }
            let id = cluster_event_record_id(&namespace, &key)?;
            // Immutable event retries read the latest locally applied record.
            // A transaction's contiguous snapshot can lag a quorum-certified
            // apply while another range is committing or being recovered.
            // Retrying that already-applied event must not become a conflicting
            // blind upsert merely because its earlier RPC lost the reply.
            if let Some(previous) = db.get(CLUSTER_EVENTS_COLLECTION, &id)? {
                if previous.metadata != value {
                    return Err(invalid("event key already exists with a different value"));
                }
                return Ok(ClusterEventResponse::Appended { inserted: false });
            }
            let mut tx = db.begin_transaction()?;
            tx.insert(
                CLUSTER_EVENTS_COLLECTION,
                Record::new(id).with_metadata(value),
            )?;
            tx.commit()?;
            Ok(ClusterEventResponse::Appended { inserted: true })
        }
        ClusterEventOperation::Get { namespace, key } => {
            let id = cluster_event_record_id(&namespace, &key)?;
            let value = db
                .get(CLUSTER_EVENTS_COLLECTION, &id)?
                .map(|r| r.metadata.clone());
            Ok(ClusterEventResponse::Value { value })
        }
        ClusterEventOperation::Scan {
            namespace,
            after,
            limit,
        } => {
            let prefix = format!("{namespace}/");
            let start = after.as_ref().map(|key| format!("{prefix}{key}"));
            // Account for framing/record overhead above the 64 KiB value cap.
            let batch = db.scan_collection_batch_after(
                CLUSTER_EVENTS_COLLECTION,
                Some(start.as_deref().unwrap_or(&prefix)),
                limit,
                1024 * 1024,
            )?;
            let mut events = Vec::new();
            let mut next = None;
            for record in batch {
                let Some(key) = record.id.strip_prefix(&prefix) else {
                    next = None;
                    break;
                };
                next = Some(key.to_owned());
                if range.contains_token(distribution_key_token(
                    CLUSTER_EVENTS_COLLECTION,
                    &record.id,
                )) {
                    events.push(ClusterEvent {
                        key: key.to_owned(),
                        value: record.metadata,
                    });
                }
            }
            Ok(ClusterEventResponse::Page { events, next })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DbConfig, DistributionConfig, DistributionStore, StorageMode};
    use serde_json::json;

    #[test]
    fn an_empty_schema_recipient_can_export_the_next_range_snapshot() {
        let source_root = tempfile::tempdir().unwrap();
        let target_root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged);
        let mut source = BicDb::open_with_config(source_root.path(), config.clone()).unwrap();
        source.create_collection(CLUSTER_EVENTS_COLLECTION).unwrap();
        let bundle = source.cluster_schema_bundle().unwrap();
        let mut target = BicDb::open_with_config(target_root.path(), config).unwrap();
        target.install_cluster_schema_bundle(&bundle).unwrap();
        let store = DistributionStore::initialize_at(
            source_root.path(),
            DistributionConfig {
                enabled: true,
                initial_ranges: 1,
                replication_factor: 1,
                ..Default::default()
            },
            false,
            1000,
        )
        .unwrap();
        let range = store.topology().ranges.values().next().unwrap();
        let mut rows = 0;
        target
            .for_each_range_snapshot_batch(
                CLUSTER_EVENTS_COLLECTION,
                range,
                None,
                &crate::RangeSnapshotOptions::default(),
                |batch| {
                    rows += batch.records.len();
                    Ok(true)
                },
            )
            .unwrap();
        assert_eq!(rows, 0);
        target
            .insert(
                CLUSTER_EVENTS_COLLECTION,
                Record::new("a/one").with_metadata(json!({"saved":true})),
            )
            .unwrap();
        target
            .for_each_range_snapshot_batch(
                CLUSTER_EVENTS_COLLECTION,
                range,
                None,
                &crate::RangeSnapshotOptions::default(),
                |batch| {
                    rows += batch.records.len();
                    Ok(true)
                },
            )
            .unwrap();
        assert_eq!(rows, 1);
    }

    #[test]
    fn bounded_pages_do_not_cross_namespace_and_writes_require_admission() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::ServerPaged),
        )
        .unwrap();
        db.create_collection(CLUSTER_EVENTS_COLLECTION).unwrap();
        for id in ["a/one", "a/three", "a/two", "b/private"] {
            db.insert(
                CLUSTER_EVENTS_COLLECTION,
                Record::new(id).with_metadata(json!({"id":id})),
            )
            .unwrap();
        }
        let store = DistributionStore::initialize_at(
            root.path(),
            DistributionConfig {
                enabled: true,
                initial_ranges: 1,
                replication_factor: 1,
                ..Default::default()
            },
            false,
            1000,
        )
        .unwrap();
        let range = store.topology().ranges.values().next().unwrap();
        let first = execute_cluster_event(
            &db,
            range,
            ClusterEventOperation::Scan {
                namespace: "a".into(),
                after: None,
                limit: 2,
            },
        )
        .unwrap();
        let ClusterEventResponse::Page { events, next } = first else {
            panic!()
        };
        assert_eq!(events.len(), 2);
        let second = execute_cluster_event(
            &db,
            range,
            ClusterEventOperation::Scan {
                namespace: "a".into(),
                after: next,
                limit: 2,
            },
        )
        .unwrap();
        let ClusterEventResponse::Page { events, next } = second else {
            panic!()
        };
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].key, "two");
        assert!(next.is_none());
        assert!(execute_cluster_event(
            &db,
            range,
            ClusterEventOperation::Append {
                namespace: "a".into(),
                key: "new".into(),
                value: json!({"saved":true}),
            }
        )
        .unwrap_err()
        .to_string()
        .contains("admission is not ready"));
        assert!(db
            .get(CLUSTER_EVENTS_COLLECTION, "a/new")
            .unwrap()
            .is_none());
        assert!(ClusterEventOperation::Scan {
            namespace: "a".into(),
            after: None,
            limit: 129
        }
        .validate()
        .is_err());
        assert!(cluster_event_record_id("a", "../b/private").is_err());
        assert!(ClusterEventOperation::Append {
            namespace: "a".into(),
            key: "key".into(),
            value: json!("x".repeat(MAX_CLUSTER_EVENT_BYTES))
        }
        .validate()
        .is_err());
    }
}
