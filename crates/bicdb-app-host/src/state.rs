//! Operator-pinned native state groups and host database lifecycle.
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{ensure, Context, Result};
use bicdb_core::{ReplicationConfig, ReplicationTlsConfig, SyncTimeoutPolicy};
use bicdb_pgwire::{
    AllowedStandby, FailoverConfig, HaPeer, HaServerConfig, HaServerRole, HaWriterLease,
};
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
use serde::Deserialize;

use crate::{database::Database, store::read_bounded};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub format: u32,
    pub applications: BTreeMap<String, Group>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Group {
    pub cluster_id: String,
    pub node_id: String,
    pub initial_primary: String,
    pub nodes: BTreeMap<String, Node>,
    /// Stateless local HTTP processes fenced and routed with this state owner.
    #[serde(default)]
    pub companions: Vec<String>,
    pub tls: Option<Tls>,
    /// Same operator-held 32-byte hex key on each physical state host.
    pub edge_sync_key_file: Option<PathBuf>,
    #[serde(default)]
    pub dev_localhost_plaintext: bool,
    #[serde(default = "timeout")]
    pub failover_timeout_ms: u64,
    #[serde(default = "timeout")]
    pub synchronous_timeout_ms: u64,
}
fn timeout() -> u64 {
    5000
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    pub certificate: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
    pub server_name: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    pub replication: SocketAddr,
    pub endpoint: String,
    pub certificate_sha256: Option<String>,
}

impl Config {
    pub fn read(path: &Path) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                std::fs::metadata(path)?.permissions().mode() & 0o077 == 0,
                "state configuration must have mode 0600"
            );
        }
        let mut config: Self = serde_json::from_slice(&read_bounded(path, 1024 * 1024)?)?;
        ensure!(
            config.format == 1
                && !config.applications.is_empty()
                && config.applications.len() <= 128,
            "invalid state configuration"
        );
        let mut allocated = BTreeSet::new();
        for (application, group) in &config.applications {
            ensure!(
                allocated.insert(application.clone()),
                "duplicate state application allocation"
            );
            ensure!(group.companions.len() <= 64, "too many state companions");
            for companion in &group.companions {
                ensure!(
                    crate::manifest::valid_name(companion) && allocated.insert(companion.clone()),
                    "invalid or duplicate state companion allocation"
                );
            }
        }
        let mut listeners = BTreeSet::new();
        let mut clusters = BTreeSet::new();
        for (application, group) in &mut config.applications {
            ensure!(
                crate::manifest::valid_name(application),
                "invalid state application name"
            );
            group.validate()?;
            ensure!(
                clusters.insert(group.cluster_id.clone()),
                "applications must use distinct state cluster identities"
            );
            ensure!(
                listeners.insert(group.nodes[&group.node_id].replication),
                "state groups must use distinct replication listeners"
            );
            if let Some(file) = &mut group.edge_sync_key_file {
                if file.is_relative() {
                    *file = path.parent().unwrap_or(Path::new(".")).join(&file);
                }
                crate::sync::PeerCipher::read(file, application, &group.cluster_id)?;
            }
            if let Some(tls) = &mut group.tls {
                let base = path.parent().unwrap_or(Path::new("."));
                for file in [&mut tls.certificate, &mut tls.key, &mut tls.ca] {
                    if file.is_relative() {
                        *file = base.join(&file);
                    }
                }
            }
        }
        Ok(config)
    }
}

impl Group {
    fn forwarding_client(&self) -> Result<reqwest::Client> {
        let mut client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(2));
        if let Some(tls) = &self.tls {
            client = client.add_root_certificate(reqwest::Certificate::from_pem(&read_bounded(
                &tls.ca,
                1024 * 1024,
            )?)?);
            let mut pem = read_bounded(&tls.certificate, 1024 * 1024)?;
            pem.extend_from_slice(&read_bounded(&tls.key, 1024 * 1024)?);
            client = client.identity(reqwest::Identity::from_pem(&pem)?);
        }
        Ok(client.build()?)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            crate::manifest::valid_name(&self.cluster_id)
                && crate::manifest::valid_name(&self.node_id),
            "invalid state identity"
        );
        ensure!(
            (3..=9).contains(&self.nodes.len()) && self.nodes.len() % 2 == 1,
            "state groups require 3, 5, 7 or 9 nodes"
        );
        ensure!(
            self.nodes.contains_key(&self.node_id)
                && self.nodes.contains_key(&self.initial_primary),
            "state group lacks a local or initial primary identity"
        );
        ensure!(
            (3000..=60000).contains(&self.failover_timeout_ms)
                && (1..=60000).contains(&self.synchronous_timeout_ms),
            "invalid state deadlines"
        );
        ensure!(
            self.dev_localhost_plaintext != self.tls.is_some(),
            "configure mutual TLS or explicit loopback development transport"
        );
        let mut addresses = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        for (id, node) in &self.nodes {
            ensure!(
                crate::manifest::valid_name(id)
                    && node.replication.port() != 0
                    && !node.replication.ip().is_unspecified()
                    && !node.replication.ip().is_multicast(),
                "invalid state peer address"
            );
            ensure!(
                addresses.insert(node.replication)
                    && endpoints.insert(node.endpoint.trim_end_matches('/')),
                "duplicate state peer address"
            );
            crate::fleet::validate_origin(&node.endpoint)?;
            if self.dev_localhost_plaintext {
                ensure!(
                    node.replication.ip().is_loopback(),
                    "development state transport must stay on loopback"
                );
                let url = reqwest::Url::parse(&node.endpoint)?;
                ensure!(
                    matches!(
                        url.host_str(),
                        Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
                    ),
                    "development state endpoints must stay on loopback"
                );
            } else {
                ensure!(
                    node.certificate_sha256
                        .as_ref()
                        .is_some_and(|hash| hash.len() == 64
                            && hash.bytes().all(|byte| byte.is_ascii_hexdigit())),
                    "state peer certificates must be pinned"
                );
            }
        }
        if let Some(tls) = &self.tls {
            ensure!(
                !tls.server_name.is_empty() && tls.server_name.len() <= 253,
                "invalid state TLS server name"
            );
        }
        Ok(())
    }

    pub fn replication(&self) -> Result<HaServerConfig> {
        self.validate()?;
        let local = &self.nodes[&self.node_id];
        let primary = HaServerRole::Primary {
            listen: local.replication.to_string(),
            allowed_standbys: self
                .nodes
                .iter()
                .filter(|(id, _)| *id != &self.node_id)
                .map(|(id, node)| AllowedStandby {
                    node_id: id.clone(),
                    certificate_sha256: node
                        .certificate_sha256
                        .as_ref()
                        .map(|hash| hash.to_ascii_lowercase()),
                })
                .collect(),
            synchronous_standbys: self.nodes.len() / 2,
            synchronous_timeout: Duration::from_millis(self.synchronous_timeout_ms),
            on_timeout: SyncTimeoutPolicy::Fail,
        };
        let server_name = self
            .tls
            .as_ref()
            .map(|tls| tls.server_name.as_str())
            .unwrap_or("localhost");
        Ok(HaServerConfig {
            replication: ReplicationConfig {
                enabled: true,
                cluster_id: self.cluster_id.clone(),
                node_id: self.node_id.clone(),
                reconnect_backoff_ms: 100,
                record_journal: self.edge_sync_key_file.is_some(),
                tls: Some(match &self.tls {
                    Some(tls) => ReplicationTlsConfig {
                        cert_path: tls.certificate.clone(),
                        key_path: tls.key.clone(),
                        ca_path: tls.ca.clone(),
                        require_client_cert: true,
                        dev_localhost_plaintext: false,
                    },
                    None => ReplicationTlsConfig {
                        cert_path: Default::default(),
                        key_path: Default::default(),
                        ca_path: Default::default(),
                        require_client_cert: false,
                        dev_localhost_plaintext: true,
                    },
                }),
                ..Default::default()
            },
            role: if self.node_id == self.initial_primary {
                primary.clone()
            } else {
                HaServerRole::Standby {
                    primary: self.nodes[&self.initial_primary].replication.to_string(),
                    primary_server_name: server_name.into(),
                }
            },
            // Client routing uses the pinned HTTP origin map, not this SQL hint.
            advertise_sql: None,
            failover: Some(FailoverConfig {
                peers: self
                    .nodes
                    .iter()
                    .filter(|(id, _)| *id != &self.node_id)
                    .map(|(id, node)| HaPeer {
                        node_id: id.clone(),
                        address: node.replication.to_string(),
                    })
                    .collect(),
                timeout: Duration::from_millis(self.failover_timeout_ms),
                as_primary: primary,
                peer_server_name: server_name.into(),
            }),
        })
    }
}

#[derive(Clone)]
pub(crate) struct DatabaseHandle(pub Arc<Mutex<Option<Database>>>);
impl DatabaseHandle {
    pub fn new(database: Database) -> Self {
        Self(Arc::new(Mutex::new(Some(database))))
    }
    pub fn lock(&self) -> Result<MappedMutexGuard<'_, Database>> {
        MutexGuard::try_map(self.0.lock(), |database| database.as_mut())
            .map_err(|_| anyhow::anyhow!("application state is reopening"))
    }
}

pub(crate) struct Replica {
    pub group: Group,
    pub client: reqwest::Client,
    pub database: DatabaseHandle,
    pub path: PathBuf,
    pub lease: Mutex<Option<HaWriterLease>>,
    pub primary: Mutex<Option<String>>,
    pub error: Mutex<Option<String>>,
}
impl Replica {
    pub fn open(group: Group, path: PathBuf) -> Result<Arc<Self>> {
        let client = group.forwarding_client()?;
        let database = Database::open_replicated(&path, group.replication()?)?;
        let lease = database.writer_lease();
        Ok(Arc::new(Self {
            group,
            client,
            database: DatabaseHandle::new(database),
            path,
            lease: Mutex::new(lease),
            primary: Mutex::new(None),
            error: Mutex::new(None),
        }))
    }
    pub fn authoritative(&self) -> bool {
        self.lease
            .lock()
            .as_ref()
            .is_some_and(HaWriterLease::active)
    }
    pub fn refresh(&self) -> Result<()> {
        let mut slot = self.database.0.lock();
        if slot.as_ref().is_none_or(Database::restart_required) {
            *self.lease.lock() = None;
            slot.take();
            let database = Database::open_replicated(&self.path, self.group.replication()?)?;
            *self.lease.lock() = database.writer_lease();
            *slot = Some(database);
        }
        let database = slot.as_ref().context("application state is reopening")?;
        *self.primary.lock() = database.db.read().ha_node_status().primary_id;
        Ok(())
    }
    pub fn primary_endpoint(&self) -> Result<String> {
        let primary = self
            .primary
            .lock()
            .clone()
            .unwrap_or_else(|| self.group.initial_primary.clone());
        ensure!(
            primary != self.group.node_id,
            "state writer authority is unavailable"
        );
        Ok(self
            .group
            .nodes
            .get(&primary)
            .context("unknown state primary")?
            .endpoint
            .trim_end_matches('/')
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        net::TcpListener,
        process::{Command, Stdio},
        time::Instant,
    };

    struct Peer(std::process::Child);
    impl Drop for Peer {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    fn openssl(directory: &Path, args: &[&str]) {
        let result = Command::new("openssl")
            .current_dir(directory)
            .args(args)
            .output()
            .expect("OpenSSL test prerequisite");
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn private_state_forwarding_trusts_the_group_ca_and_authenticates_the_member() {
        let root = tempfile::tempdir().unwrap();
        openssl(
            root.path(),
            &[
                "req",
                "-x509",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:P-256",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=state-test-ca",
                "-keyout",
                "ca.key",
                "-out",
                "ca.crt",
            ],
        );
        openssl(
            root.path(),
            &[
                "req",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:P-256",
                "-nodes",
                "-subj",
                "/CN=state-member",
                "-keyout",
                "node.key",
                "-out",
                "node.csr",
            ],
        );
        std::fs::write(root.path().join("extensions"),"basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=IP:127.0.0.1,DNS:state.internal\n").unwrap();
        openssl(
            root.path(),
            &[
                "x509",
                "-req",
                "-in",
                "node.csr",
                "-CA",
                "ca.crt",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-days",
                "1",
                "-extfile",
                "extensions",
                "-out",
                "node.crt",
            ],
        );
        let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reservation.local_addr().unwrap();
        drop(reservation);
        let _peer = Peer(
            Command::new("openssl")
                .current_dir(root.path())
                .args([
                    "s_server",
                    "-accept",
                    &address.to_string(),
                    "-cert",
                    "node.crt",
                    "-key",
                    "node.key",
                    "-CAfile",
                    "ca.crt",
                    "-Verify",
                    "1",
                    "-verify_return_error",
                    "-www",
                    "-quiet",
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let mut group:Group=serde_json::from_value(serde_json::json!({"cluster_id":"test","node_id":"n2","initial_primary":"n1","nodes":{},"tls":{"certificate":root.path().join("node.crt"),"key":root.path().join("node.key"),"ca":root.path().join("ca.crt"),"server_name":"state.internal"}})).unwrap();
        let url = format!("https://{address}/");
        let client = group.forwarding_client().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let response = loop {
            match client
                .get(&url)
                .timeout(Duration::from_secs(2))
                .send()
                .await
            {
                Ok(response) => break response,
                Err(error) => {
                    assert!(
                        Instant::now() < deadline,
                        "private peer TLS failed: {error:#}"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };
        assert!(response.status().is_success());
        let body = response.text().await.unwrap();
        assert!(
            body.contains("Client certificate") && body.contains("state-member"),
            "peer must verify and receive the member certificate"
        );
        let ca =
            reqwest::Certificate::from_pem(&std::fs::read(root.path().join("ca.crt")).unwrap())
                .unwrap();
        let anonymous = reqwest::Client::builder()
            .no_proxy()
            .add_root_certificate(ca)
            .build()
            .unwrap();
        assert!(
            anonymous
                .get(&url)
                .timeout(Duration::from_secs(2))
                .send()
                .await
                .is_err(),
            "peer requires client authentication"
        );
        group.tls = None;
        assert!(
            group
                .forwarding_client()
                .unwrap()
                .get(&url)
                .timeout(Duration::from_secs(2))
                .send()
                .await
                .is_err(),
            "public trust alone cannot authenticate a private peer"
        );
    }
}
