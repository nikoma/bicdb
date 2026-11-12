//! Explicit metadata-only host. Never calls generic pgwire/MStore startup.
use anyhow::{ensure, Context, Result};
use bicdb_core::NativeMetadataLearnerHost;
use bicdb_core::{
    CancellationToken, ClusterNetworkTransportConfig, ClusterNode, ClusterNodeId,
    NativeMetadataBootstrapPlan, NativeMetadataHost, NativeMetadataHostAction,
    NativeMetadataHostPhase, ReplicationTlsConfig, TcpClusterBootstrapClient,
};
use clap::{Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

#[derive(Debug, Subcommand)]
pub enum Command {
    /// FRESH roots only. Starts metadata listeners; never SQL/data Serving.
    ServeFresh {
        root: PathBuf,
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        node_id: String,
        #[arg(long)]
        address: String,
        #[arg(long)]
        cluster_tls_cert: PathBuf,
        #[arg(long)]
        cluster_tls_key: PathBuf,
        #[arg(long)]
        cluster_tls_ca: PathBuf,
        /// Private operator command/status files must be outside the fresh DB root.
        #[arg(long)]
        command_file: PathBuf,
        #[arg(long)]
        status_file: PathBuf,
        #[arg(long, default_value_t = 8_388_608)]
        buffer_pool_bytes: u64,
        #[arg(long, default_value_t = 3600)]
        run_seconds: u64,
    },
    /// Real fresh fifth factory/birth listener; metadata backend is a later action.
    ServeLearnerFresh {
        root: PathBuf,
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        seed_node_id: String,
        #[arg(long)]
        seed_address: String,
        #[arg(long)]
        node_id: String,
        #[arg(long)]
        address: String,
        #[arg(long, default_value_t = 1)]
        incarnation: u64,
        #[arg(long)]
        cluster_tls_cert: PathBuf,
        #[arg(long)]
        cluster_tls_key: PathBuf,
        #[arg(long)]
        cluster_tls_ca: PathBuf,
        #[arg(long)]
        command_file: PathBuf,
        #[arg(long)]
        status_file: PathBuf,
        #[arg(long, default_value_t = 8_388_608)]
        buffer_pool_bytes: u64,
        #[arg(long, default_value_t = 3600)]
        run_seconds: u64,
    },
    /// Send one local operator action; JSON commands are DATA, not capabilities.
    Step {
        #[arg(long)]
        command_file: PathBuf,
        #[arg(long)]
        status_file: PathBuf,
        #[arg(long)]
        sequence: u64,
        #[arg(long, value_enum)]
        action: Action,
        #[arg(long)]
        target_node_id: Option<String>,
        #[arg(long, default_value_t = 5000)]
        timeout_ms: u64,
    },
    /// Register ONLY this TLS node's metadata learner CONFIG identity.
    Register {
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        seed_node_id: String,
        #[arg(long)]
        seed_address: String,
        #[arg(long)]
        node_id: String,
        #[arg(long)]
        address: String,
        #[arg(long, default_value_t = 1)]
        incarnation: u64,
        #[arg(long, default_value_t = 1_099_511_627_776)]
        capacity_bytes: u64,
        #[arg(long)]
        cluster_tls_cert: PathBuf,
        #[arg(long)]
        cluster_tls_key: PathBuf,
        #[arg(long)]
        cluster_tls_ca: PathBuf,
        #[arg(long, default_value_t = 5000)]
        timeout_ms: u64,
    },
}
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Action {
    Prepare,
    Launch,
    Elect,
    EnableControl,
    Heartbeat,
    EnrollLearner,
    CatchUpLearner,
    StartLearnerMetadata,
    Status,
    Stop,
}
impl Action {
    fn into_core(self, target: Option<String>) -> Result<NativeMetadataHostAction> {
        use NativeMetadataHostAction as C;
        let needs_target = matches!(self, Self::EnrollLearner | Self::CatchUpLearner);
        ensure!(
            needs_target == target.is_some(),
            "target-node-id is required ONLY for enroll-learner/catch-up-learner"
        );
        Ok(match self {
            Self::Prepare => C::Prepare,
            Self::Launch => C::Launch,
            Self::Elect => C::Elect,
            Self::EnableControl => C::EnableControl,
            Self::Heartbeat => C::Heartbeat,
            Self::EnrollLearner => C::EnrollLearner {
                node: ClusterNodeId::new(target.unwrap())?,
            },
            Self::CatchUpLearner => C::CatchUpLearner {
                node: ClusterNodeId::new(target.unwrap())?,
            },
            Self::StartLearnerMetadata => C::StartLearnerMetadata,
            Self::Status => C::Status,
            Self::Stop => C::Stop,
        })
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    version: u32,
    sequence: u64,
    action: NativeMetadataHostAction,
    budget_ms: u64,
}
#[derive(Serialize)]
struct Reply<'a, T: Serialize> {
    version: u32,
    sequence: u64,
    process_id: u32,
    host: Option<&'a T>,
    error: Option<String>,
    data_serving: bool,
}
fn read_bounded(path: &Path, max: u64) -> Result<Vec<u8>> {
    use std::io::Read;
    let file = std::fs::File::open(path)?;
    ensure!(
        file.metadata()?.len() <= max,
        "operator input exceeds bound"
    );
    let mut bytes = Vec::new();
    file.take(max + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= max, "operator input exceeds bound");
    Ok(bytes)
}
fn private_parent(path: &Path) -> Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let actual = parent.canonicalize()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = actual.metadata()?;
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() } && metadata.mode() & 0o022 == 0,
            "native operator files need a caller-owned parent without group/other write access"
        );
    }
    Ok(actual)
}
fn write(path: &Path, value: &impl Serialize) -> Result<()> {
    use std::io::Write;
    let parent = private_parent(path)?;
    let target = parent.join(path.file_name().context("operator filename absent")?);
    let temporary = parent.join(format!(".native-metadata-{}.tmp", uuid::Uuid::now_v7()));
    let result = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(&serde_json::to_vec_pretty(value)?)?;
        file.sync_all()?;
        std::fs::rename(&temporary, &target)?;
        std::fs::File::open(&parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}
fn own_network(cert: PathBuf, key: PathBuf, ca: PathBuf) -> ClusterNetworkTransportConfig {
    ClusterNetworkTransportConfig {
        connect_timeout_ms: 200,
        io_timeout_ms: 2000,
        max_frame_bytes: 4 * 1024 * 1024,
        max_inbound_connections: 16,
        dev_localhost_plaintext: false,
        tls: Some(ReplicationTlsConfig {
            cert_path: cert,
            key_path: key,
            ca_path: ca,
            require_client_cert: true,
            dev_localhost_plaintext: false,
        }),
    }
}
pub fn run(command: Command) -> Result<()> {
    match command {
        Command::ServeFresh {
            root,
            plan,
            node_id,
            address,
            cluster_tls_cert,
            cluster_tls_key,
            cluster_tls_ca,
            command_file,
            status_file,
            buffer_pool_bytes,
            run_seconds,
        } => {
            ensure!(
                (1..=86400).contains(&run_seconds),
                "metadata host run duration outside1..86400 seconds"
            );
            ensure!(
                !root.exists(),
                "native fresh metadata host refuses any existing root"
            );
            ensure!(
                command_file != status_file
                    && !command_file.starts_with(&root)
                    && !status_file.starts_with(&root),
                "operator files must differ and stay outside fresh root"
            );
            ensure!(
                !command_file.exists() && !status_file.exists(),
                "native operator control/status files already exist; refuse stale commands"
            );
            let root_parent = root
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."))
                .canonicalize()?;
            let actual_root =
                root_parent.join(root.file_name().context("native root basename absent")?);
            for path in [&command_file, &status_file] {
                let actual = private_parent(path)?
                    .join(path.file_name().context("operator filename absent")?);
                ensure!(
                    !actual.starts_with(&actual_root),
                    "operator files may not occupy the native fresh root"
                );
            }
            let plan: NativeMetadataBootstrapPlan =
                serde_json::from_slice(&read_bounded(&plan, 2 * 1024 * 1024)?)?;
            let network = own_network(cluster_tls_cert, cluster_tls_key, cluster_tls_ca);
            let canceled = Arc::new(AtomicBool::new(false));
            let signal = Arc::clone(&canceled);
            ctrlc::set_handler(move || {
                signal.store(true, Ordering::SeqCst);
            })?;
            let lifetime = CancellationToken::new(
                canceled,
                Some(Instant::now() + Duration::from_secs(run_seconds)),
            );
            let mut host = NativeMetadataHost::start_fresh_metadata_only(
                &root,
                plan,
                ClusterNodeId::new(node_id)?,
                &address,
                network,
                buffer_pool_bytes,
                &lifetime.child_with_deadline(Instant::now() + Duration::from_secs(30)),
            )?;
            let mut status = host.status(&lifetime)?;
            write(
                &status_file,
                &Reply {
                    version: 1,
                    sequence: 0,
                    process_id: std::process::id(),
                    host: Some(&status),
                    error: None,
                    data_serving: false,
                },
            )?;
            let mut sequence = 0;
            let mut last_tick = Instant::now();
            while lifetime.check().is_ok() && status.phase != NativeMetadataHostPhase::Stopped {
                if command_file.exists() {
                    let request: Request =
                        serde_json::from_slice(&read_bounded(&command_file, 16 * 1024)?)?;
                    ensure!(
                        request.version == 1 && request.budget_ms > 0 && request.budget_ms <= 5000,
                        "unsupported metadata operator request"
                    );
                    if request.sequence > sequence {
                        ensure!(
                            request.sequence == sequence + 1,
                            "metadata operator sequence must advance exactly one"
                        );
                        let token = lifetime.child_with_deadline(
                            Instant::now() + Duration::from_millis(request.budget_ms),
                        );
                        let error = match host.apply(request.action, &token) {
                            Ok(s) => {
                                status = s;
                                None
                            }
                            Err(error) => {
                                // This is a NEW diagnostic read, not an action
                                // budget extension or successful phase receipt.
                                if let Ok(actual) = host.status(&lifetime.child_with_deadline(
                                    Instant::now() + Duration::from_millis(250),
                                )) {
                                    status = actual;
                                }
                                Some(error.to_string())
                            }
                        };
                        sequence = request.sequence;
                        // No successful phase/readiness claim is inferred on error.
                        write(
                            &status_file,
                            &Reply {
                                version: 1,
                                sequence,
                                process_id: std::process::id(),
                                host: Some(&status),
                                error,
                                data_serving: false,
                            },
                        )?;
                    }
                }
                // Retained finite metadata maintenance, not election/data recovery.
                if last_tick.elapsed() >= Duration::from_millis(500)
                    && status
                        .metadata
                        .as_ref()
                        .is_some_and(|m| m.role == bicdb_core::MetadataConsensusRole::Leader)
                    && matches!(
                        status.phase,
                        NativeMetadataHostPhase::OwnedMetadata
                            | NativeMetadataHostPhase::MetadataControl
                    )
                {
                    let token =
                        lifetime.child_with_deadline(Instant::now() + Duration::from_secs(1));
                    if let Ok(next) = host.apply(NativeMetadataHostAction::Heartbeat, &token) {
                        status = next;
                    }
                    last_tick = Instant::now();
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            // Cancellation ends verdicts, but cleanup gets a separate explicit
            // drain budget and retains the actual owner until this call exits.
            host.apply(
                NativeMetadataHostAction::Stop,
                &CancellationToken::uncancelable()
                    .child_with_deadline(Instant::now() + Duration::from_secs(5)),
            )?;
        }
        Command::ServeLearnerFresh {
            root,
            cluster_id,
            seed_node_id,
            seed_address,
            node_id,
            address,
            incarnation,
            cluster_tls_cert,
            cluster_tls_key,
            cluster_tls_ca,
            command_file,
            status_file,
            buffer_pool_bytes,
            run_seconds,
        } => {
            ensure!(
                (1..=86400).contains(&run_seconds) && !root.exists(),
                "fresh learner requires a new root and bounded run duration"
            );
            ensure!(
                command_file != status_file && !command_file.exists() && !status_file.exists(),
                "learner operator files must differ and be absent"
            );
            let root_parent = root
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."))
                .canonicalize()?;
            let actual_root =
                root_parent.join(root.file_name().context("learner root basename absent")?);
            for path in [&command_file, &status_file] {
                ensure!(
                    !private_parent(path)?
                        .join(path.file_name().context("operator filename absent")?)
                        .starts_with(&actual_root),
                    "learner operator files must stay outside native root"
                );
            }
            let canceled = Arc::new(AtomicBool::new(false));
            let signal = Arc::clone(&canceled);
            ctrlc::set_handler(move || {
                signal.store(true, Ordering::SeqCst);
            })?;
            let lifetime = CancellationToken::new(
                canceled,
                Some(Instant::now() + Duration::from_secs(run_seconds)),
            );
            let mut host = NativeMetadataLearnerHost::start_fresh_metadata_only(
                &root,
                bicdb_core::ClusterId::new(cluster_id)?,
                ClusterNodeId::new(node_id)?,
                &address,
                incarnation,
                ClusterNodeId::new(seed_node_id)?,
                &seed_address,
                own_network(cluster_tls_cert, cluster_tls_key, cluster_tls_ca),
                buffer_pool_bytes,
                &lifetime,
            )?;
            let mut status = host.refresh(&lifetime)?;
            write(
                &status_file,
                &Reply {
                    version: 1,
                    sequence: 0,
                    process_id: std::process::id(),
                    host: Some(&status),
                    error: None,
                    data_serving: false,
                },
            )?;
            let mut sequence = 0;
            let mut last_refresh = Instant::now();
            let mut stopping = false;
            while lifetime.check().is_ok() && !stopping {
                if command_file.exists() {
                    let request: Request =
                        serde_json::from_slice(&read_bounded(&command_file, 16 * 1024)?)?;
                    ensure!(
                        request.version == 1 && (1..=5000).contains(&request.budget_ms),
                        "unsupported learner command version/budget"
                    );
                    if request.sequence > sequence {
                        ensure!(
                            request.sequence == sequence + 1,
                            "learner sequence must advance exactly one"
                        );
                        let token = lifetime.child_with_deadline(
                            Instant::now() + Duration::from_millis(request.budget_ms),
                        );
                        let result = match request.action {
                            NativeMetadataHostAction::StartLearnerMetadata => {
                                host.start_metadata(&token)
                            }
                            NativeMetadataHostAction::Status => host.refresh(&token),
                            NativeMetadataHostAction::Stop => host.stop(&token).map(|_| {
                                stopping = true;
                                status.phase = "stopped";
                                status.metadata_listener_owned = false;
                                status.clone()
                            }),
                            _ => Err(bicdb_core::BicDbError::Unsupported(
                                "learner owner permits only start-learner-metadata/status/stop"
                                    .into(),
                            )),
                        };
                        let error = match result {
                            Ok(next) => {
                                status = next;
                                None
                            }
                            Err(error) => Some(error.to_string()),
                        };
                        sequence = request.sequence;
                        write(
                            &status_file,
                            &Reply {
                                version: 1,
                                sequence,
                                process_id: std::process::id(),
                                host: Some(&status),
                                error,
                                data_serving: false,
                            },
                        )?;
                    }
                }
                if last_refresh.elapsed() >= Duration::from_millis(500) && !stopping {
                    if let Ok(next) = host.refresh(
                        &lifetime.child_with_deadline(Instant::now() + Duration::from_secs(2)),
                    ) {
                        status = next;
                    }
                    last_refresh = Instant::now();
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            host.stop(
                &CancellationToken::uncancelable()
                    .child_with_deadline(Instant::now() + Duration::from_secs(5)),
            )?;
        }
        Command::Step {
            command_file,
            status_file,
            sequence,
            action,
            target_node_id,
            timeout_ms,
        } => {
            ensure!(
                sequence > 0 && (1..=5000).contains(&timeout_ms),
                "unsupported operator sequence/budget"
            );
            ensure!(
                command_file != status_file,
                "operator command/status paths must differ"
            );
            let deadline = Instant::now() + Duration::from_millis(timeout_ms);
            write(
                &command_file,
                &Request {
                    version: 1,
                    sequence,
                    action: action.into_core(target_node_id)?,
                    budget_ms: timeout_ms,
                },
            )?;
            loop {
                let reply: serde_json::Value =
                    serde_json::from_slice(&read_bounded(&status_file, 1024 * 1024)?)?;
                if reply["sequence"].as_u64() == Some(sequence) {
                    ensure!(
                        reply["error"].is_null(),
                        "native metadata action refused: {}",
                        reply["error"]
                    );
                    println!("{}", serde_json::to_string(&reply)?);
                    break;
                }
                ensure!(
                    Instant::now() < deadline,
                    "native metadata action timed out; actual pending state is retained"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        Command::Register {
            cluster_id,
            seed_node_id,
            seed_address,
            node_id,
            address,
            incarnation,
            capacity_bytes,
            cluster_tls_cert,
            cluster_tls_key,
            cluster_tls_ca,
            timeout_ms,
        } => {
            ensure!(
                (1..=30000).contains(&timeout_ms),
                "native registration budget outside1..30000ms"
            );
            let pin = bicdb_core::replication_transport::replication_certificate_sha256(
                &cluster_tls_cert,
            )?;
            let network = own_network(cluster_tls_cert, cluster_tls_key, cluster_tls_ca);
            let id = ClusterNodeId::new(node_id)?;
            let registration_time_ms = u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis(),
            )?;
            let desired = ClusterNode::new(
                id.clone(),
                address,
                incarnation,
                capacity_bytes,
                registration_time_ms,
            )?
            .as_metadata_learner()
            .with_tls_certificate_sha256(pin)?;
            let client = TcpClusterBootstrapClient::new(
                bicdb_core::ClusterId::new(cluster_id)?,
                id.clone(),
                network,
            )?;
            let seed = ClusterNodeId::new(seed_node_id)?;
            let deadline = Instant::now() + Duration::from_millis(timeout_ms);
            let mut last = String::from("no committed native registration");
            loop {
                ensure!(
                    Instant::now() < deadline,
                    "registration unavailable (no joined/data claim): {last}"
                );
                let snapshot = client
                    .fetch_snapshot(&seed, &seed_address)
                    .context("native CONFIG bootstrap unavailable")?;
                if let Some(actual) = snapshot.topology.nodes.get(&id) {
                    ensure!(
                        actual.address == desired.address
                            && actual.incarnation == desired.incarnation
                            && actual.tls_certificate_sha256 == desired.tls_certificate_sha256
                            && actual.pending_tls_certificate_sha256.is_none()
                            && actual.capacity_bytes == desired.capacity_bytes
                            && actual.lifecycle == bicdb_core::ClusterNodeLifecycle::Active,
                        "native CONFIG contains another identity; never reset/demote it"
                    );
                    ensure!(
                        Instant::now() < deadline,
                        "registration verdict budget expired"
                    );
                    println!(
                        "{}",
                        serde_json::json!({"metadata_identity_committed": true, "node_id": id,
                        "metadata_role": actual.metadata_role, "native_birth_bound": actual.native_store_birth_v1.is_some(),
                        "topology_generation": snapshot.topology.generation, "data_serving": false})
                    );
                    break;
                }
                if let Some(leader) = snapshot
                    .metadata_leader_id
                    .as_ref()
                    .and_then(|l| snapshot.topology.nodes.get(l))
                {
                    if let Err(error) = client.register_metadata_learner(
                        &leader.id,
                        &leader.address,
                        desired.clone(),
                        registration_time_ms,
                    ) {
                        last = error.to_string();
                    }
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
    Ok(())
}
