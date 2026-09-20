use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::consensus::{AppendEntries, AppendResponse, RequestVote, VoteResponse};
use crate::error::{BicDbError, Result};
use crate::record::CollectionMeta;

pub const REPLICATION_PROTOCOL_VERSION: u32 = 1;
pub const DEFAULT_REPLICATION_STREAM_ID: &str = "default";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationApplyState {
    pub cluster_id: String,
    pub source_node_id: Option<String>,
    pub stream_id: Option<String>,
    pub last_applied_commit_seq: u64,
    pub last_applied_at: Option<i64>,
    pub last_error: Option<String>,
}

impl ReplicationApplyState {
    pub fn new(cluster_id: impl Into<String>, last_applied_commit_seq: u64) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            source_node_id: None,
            stream_id: None,
            last_applied_commit_seq,
            last_applied_at: None,
            last_error: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationMode {
    Primary,
    Standby,
    Disabled,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationTlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: PathBuf,
    pub require_client_cert: bool,
    #[serde(default)]
    pub dev_localhost_plaintext: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationConfig {
    pub enabled: bool,
    pub mode: ReplicationMode,
    pub listen_addr: Option<String>,
    pub advertise_addr: Option<String>,
    pub tls: Option<ReplicationTlsConfig>,
    pub allowed_node_ids: Vec<String>,
    pub cluster_id: String,
    pub node_id: String,
    pub compression: bool,
    pub max_frame_bytes: usize,
    pub heartbeat_interval_ms: u64,
    pub connect_timeout_ms: u64,
    pub reconnect_backoff_ms: u64,
    pub retention_bytes: u64,
    pub retention_commits: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: ReplicationMode::Disabled,
            listen_addr: None,
            advertise_addr: None,
            tls: None,
            allowed_node_ids: Vec::new(),
            cluster_id: "default".to_string(),
            node_id: Uuid::new_v4().to_string(),
            compression: false,
            max_frame_bytes: 128 * 1024 * 1024,
            heartbeat_interval_ms: 5_000,
            connect_timeout_ms: 10_000,
            reconnect_backoff_ms: 1_000,
            retention_bytes: 1024 * 1024 * 1024,
            retention_commits: 1_000_000,
        }
    }
}

impl ReplicationConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if self.cluster_id.trim().is_empty() {
            return Err(replication_error(
                "replication.cluster_id must not be empty",
            ));
        }
        if self.node_id.trim().is_empty() {
            return Err(replication_error("replication.node_id must not be empty"));
        }
        if self.max_frame_bytes == 0 {
            return Err(replication_error(
                "replication.max_frame_bytes must be greater than zero",
            ));
        }
        let listen_addr = self.listen_addr.as_deref().unwrap_or("");
        let localhost = listen_addr.starts_with("127.0.0.1:")
            || listen_addr.starts_with("[::1]:")
            || listen_addr == "localhost"
            || listen_addr.starts_with("localhost:");
        let dev_plaintext = self
            .tls
            .as_ref()
            .is_some_and(|tls| tls.dev_localhost_plaintext);
        if dev_plaintext {
            if !localhost {
                return Err(replication_error(
                    "plaintext replication is only allowed in explicit localhost dev mode",
                ));
            }
            return Ok(());
        }
        let tls = self
            .tls
            .as_ref()
            .ok_or_else(|| replication_error("TLS is mandatory for non-localhost replication"))?;
        if !tls.require_client_cert {
            return Err(replication_error(
                "mTLS client certificates are required for node replication",
            ));
        }
        if tls.cert_path.as_os_str().is_empty()
            || tls.key_path.as_os_str().is_empty()
            || tls.ca_path.as_os_str().is_empty()
        {
            return Err(replication_error(
                "replication TLS cert_path, key_path, and ca_path are required",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationOperationType {
    Insert,
    Upsert,
    Delete,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationWrite {
    pub collection: String,
    pub record_id: String,
    pub operation: ReplicationOperationType,
    pub payload: Vec<u8>,
    pub schema_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection_meta: Option<CollectionMeta>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitFrame {
    pub protocol_version: u32,
    pub cluster_id: String,
    pub source_node_id: String,
    pub stream_id: String,
    pub commit_seq: u64,
    pub previous_commit_seq: u64,
    pub tx_id: u64,
    pub timestamp: i64,
    pub writes: Vec<ReplicationWrite>,
    pub compressed: bool,
    pub encryption_metadata: Option<Vec<u8>>,
    pub checksum: [u8; 32],
}

impl CommitFrame {
    pub fn new(
        cluster_id: impl Into<String>,
        source_node_id: impl Into<String>,
        stream_id: impl Into<String>,
        commit_seq: u64,
        tx_id: u64,
        timestamp: i64,
        writes: Vec<ReplicationWrite>,
    ) -> Self {
        let mut frame = Self {
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            cluster_id: cluster_id.into(),
            source_node_id: source_node_id.into(),
            stream_id: stream_id.into(),
            commit_seq,
            previous_commit_seq: commit_seq.saturating_sub(1),
            tx_id,
            timestamp,
            writes,
            compressed: false,
            encryption_metadata: None,
            checksum: [0; 32],
        };
        frame.checksum = frame.calculate_checksum();
        frame
    }

    pub fn calculate_checksum(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(self.checksum_payload());
        hasher.finalize().into()
    }

    pub fn verify_checksum(&self) -> Result<()> {
        let actual = self.calculate_checksum();
        if actual != self.checksum {
            return Err(replication_error("replication frame checksum mismatch"));
        }
        Ok(())
    }

    pub fn encode_deterministic(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_payload(&mut out, true);
        out
    }

    fn checksum_payload(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        self.encode_payload(&mut bytes, false);
        bytes
    }

    fn encode_payload(&self, out: &mut Vec<u8>, include_checksum: bool) {
        put_u32(out, self.protocol_version);
        put_string(out, &self.cluster_id);
        put_string(out, &self.source_node_id);
        put_string(out, &self.stream_id);
        put_u64(out, self.commit_seq);
        put_u64(out, self.previous_commit_seq);
        put_u64(out, self.tx_id);
        put_i64(out, self.timestamp);
        put_u32(out, self.writes.len() as u32);
        for write in &self.writes {
            put_string(out, &write.collection);
            put_string(out, &write.record_id);
            out.push(match write.operation {
                ReplicationOperationType::Insert => 1,
                ReplicationOperationType::Upsert => 2,
                ReplicationOperationType::Delete => 3,
            });
            put_bytes(out, &write.payload);
            put_u64(out, write.schema_version);
            match &write.collection_meta {
                Some(meta) => {
                    out.push(1);
                    let encoded = serde_json::to_vec(meta).unwrap_or_default();
                    put_bytes(out, &encoded);
                }
                None => out.push(0),
            }
        }
        out.push(u8::from(self.compressed));
        match &self.encryption_metadata {
            Some(bytes) => {
                out.push(1);
                put_bytes(out, bytes);
            }
            None => out.push(0),
        }
        if include_checksum {
            out.extend_from_slice(&self.checksum);
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "frame", rename_all = "snake_case")]
pub enum ReplicationFrame {
    Hello {
        protocol_version: u32,
        cluster_id: String,
        node_id: String,
    },
    Authenticate {
        node_id: String,
        nonce: Vec<u8>,
        signature: Vec<u8>,
    },
    Heartbeat {
        cluster_id: String,
        node_id: String,
        last_commit_seq: u64,
    },
    SnapshotStart {
        cluster_id: String,
        source_node_id: String,
        snapshot_id: String,
        snapshot_commit_seq: u64,
    },
    SnapshotChunk {
        snapshot_id: String,
        offset: u64,
        bytes: Vec<u8>,
        checksum: [u8; 32],
    },
    SnapshotEnd {
        snapshot_id: String,
        snapshot_commit_seq: u64,
        snapshot_hash: [u8; 32],
    },
    Commit(CommitFrame),
    Ack {
        cluster_id: String,
        node_id: String,
        commit_seq: u64,
    },
    Nack {
        cluster_id: String,
        node_id: String,
        expected_commit_seq: u64,
        message: String,
    },
    Error {
        code: String,
        message: String,
    },
    ConsensusRequestVote(RequestVote),
    ConsensusVoteResponse(VoteResponse),
    ConsensusAppendEntries(AppendEntries),
    ConsensusAppendResponse(AppendResponse),
    TopologyInfo {
        cluster_id: String,
        nodes: Vec<String>,
        primary_node_id: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationWatermark {
    pub current_commit_seq: u64,
    pub last_applied_commit_seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationRetentionStatus {
    pub oldest_available_commit_seq: u64,
    pub newest_available_commit_seq: u64,
    pub retained_commits: u64,
    pub retained_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationApplyReport {
    pub applied: usize,
    pub duplicates: usize,
    pub last_applied_commit_seq: u64,
}

impl fmt::Debug for ReplicationTlsConfigRedacted<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplicationTlsConfig")
            .field("cert_path", &self.0.cert_path)
            .field("key_path", &"<redacted>")
            .field("ca_path", &self.0.ca_path)
            .field("require_client_cert", &self.0.require_client_cert)
            .field("dev_localhost_plaintext", &self.0.dev_localhost_plaintext)
            .finish()
    }
}

pub struct ReplicationTlsConfigRedacted<'a>(pub &'a ReplicationTlsConfig);

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_string(out: &mut Vec<u8>, value: &str) {
    put_bytes(out, value.as_bytes());
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) {
    put_u32(out, value.len() as u32);
    out.extend_from_slice(value);
}

pub(crate) fn replication_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Replication(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_frame_checksum_detects_payload_changes() {
        let frame = CommitFrame::new(
            "cluster",
            "node-a",
            DEFAULT_REPLICATION_STREAM_ID,
            1,
            7,
            123,
            vec![ReplicationWrite {
                collection: "accounts".to_string(),
                record_id: "a1".to_string(),
                operation: ReplicationOperationType::Upsert,
                payload: b"payload".to_vec(),
                schema_version: 0,
                collection_meta: None,
            }],
        );
        frame.verify_checksum().unwrap();

        let mut corrupted = frame.clone();
        corrupted.writes[0].payload[0] ^= 0x01;
        assert!(corrupted.verify_checksum().is_err());
    }

    #[test]
    fn deterministic_encoding_is_stable_for_equal_frames() {
        let left = CommitFrame::new("cluster", "node-a", "s", 2, 9, 456, Vec::new());
        let right = CommitFrame::new("cluster", "node-a", "s", 2, 9, 456, Vec::new());
        assert_eq!(left.encode_deterministic(), right.encode_deterministic());
        assert_eq!(left.checksum, right.checksum);
    }

    #[test]
    fn tls_config_rejects_remote_plaintext() {
        let config = ReplicationConfig {
            enabled: true,
            mode: ReplicationMode::Standby,
            listen_addr: Some("0.0.0.0:9443".to_string()),
            tls: Some(ReplicationTlsConfig {
                cert_path: PathBuf::new(),
                key_path: PathBuf::new(),
                ca_path: PathBuf::new(),
                require_client_cert: true,
                dev_localhost_plaintext: true,
            }),
            ..ReplicationConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn tls_config_requires_mtls_for_remote_replication() {
        let config = ReplicationConfig {
            enabled: true,
            mode: ReplicationMode::Standby,
            listen_addr: Some("10.0.0.1:9443".to_string()),
            tls: Some(ReplicationTlsConfig {
                cert_path: PathBuf::from("node.crt"),
                key_path: PathBuf::from("node.key"),
                ca_path: PathBuf::from("ca.crt"),
                require_client_cert: false,
                dev_localhost_plaintext: false,
            }),
            ..ReplicationConfig::default()
        };
        assert!(config.validate().is_err());
    }
}
