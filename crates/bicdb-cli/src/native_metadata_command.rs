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
        /// Manual election/heartbeat/enrollment scheduling; proof/admission guards stay mandatory.
        #[arg(long)]
        manual_enrollment: bool,
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
    /// Register and start ONE fresh metadata learner; automatic leader maintenance promotes it.
    JoinFresh {
        root: PathBuf,
        /// After real metadata enrollment/control, expose a NEW replica-only
        /// receiver. This never activates DATA, genesis, APP or Strong reads.
        #[arg(long)]
        replica_receiver: bool,
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
        #[arg(long, default_value_t = 1_099_511_627_776)]
        capacity_bytes: u64,
        #[arg(long, default_value_t = 8_388_608)]
        buffer_pool_bytes: u64,
        #[arg(long, default_value_t = 3600)]
        run_seconds: u64,
    },
    /// Strict existing enrolled learner root; never recreates schema/genesis/metadata identity.
    ServeLearnerReopen {
        root: PathBuf,
        /// Run typed metadata continuation/control/maintenance on this persisted owner.
        /// Default reopen remains manual; this never registers or imports schema.
        #[arg(long)]
        automatic_metadata: bool,
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
    /// Move ONE range onto a registered member through the joint handoff, or
    /// abort that move before its source cut (--abort). Send it to the host
    /// that is BOTH the metadata leader and the leader of the range; any other
    /// host refuses. Prerequisite: the target is already registered and has
    /// opened its replica receiver (open-replica-receiver). The request only
    /// creates the controller; the serving host advances it one stage per
    /// step and reports progress in the `range_move` status section.
    MoveRange {
        #[arg(long)]
        command_file: PathBuf,
        #[arg(long)]
        status_file: PathBuf,
        #[arg(long)]
        sequence: u64,
        #[arg(long)]
        range_id: u64,
        /// Required to start a move. With --abort it is not needed: a host
        /// has at most one move, identified by its range.
        #[arg(long)]
        target_node_id: Option<String>,
        #[arg(long)]
        abort: bool,
        #[arg(long, default_value_t = 5000)]
        timeout_ms: u64,
    },
    /// Hand metadata leadership from the host that leads metadata now to
    /// another metadata voter (--to), so that the metadata leader and the
    /// leader of a range coincide before move-range. Send it to the current
    /// metadata leader (`metadata_leader` in every host's status); any other
    /// host refuses. Bounded: on failure the error says who leads.
    TransferMetadataLeadership {
        #[arg(long)]
        command_file: PathBuf,
        #[arg(long)]
        status_file: PathBuf,
        #[arg(long)]
        sequence: u64,
        #[arg(long)]
        to: String,
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
    /// INITIAL owners only: retained native policy/runtime, new DATA listener, real initial readiness.
    OpenInitialData,
    OpenReplicaReceiver,
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
            Self::OpenInitialData => C::OpenInitialData,
            Self::OpenReplicaReceiver => C::OpenReplicaReceiver,
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
/// What the operator's command file holds for a host that last applied the
/// sequence `applied`.
enum Pending {
    /// No file, or a command at or below `applied`: the one already
    /// answered. A reused sequence is ignored here; the sending command
    /// refuses it before it writes (see `require_next_sequence`).
    Nothing,
    /// The well-formed command with exactly the next sequence.
    Next(Request),
    /// A command the host refuses and ignores: unreadable, of an unsupported
    /// version or budget, or with a skipped sequence. The text is recorded
    /// in the status file. An operator's typo never ends the host.
    Refused(String),
}
fn pending_request(command_file: &Path, applied: u64) -> Pending {
    let bytes = match read_bounded(command_file, 16 * 1024) {
        Ok(bytes) => bytes,
        Err(_) if !command_file.exists() => return Pending::Nothing,
        Err(error) => {
            return Pending::Refused(format!(
                "operator command refused and ignored: unreadable command file: {error}"
            ))
        }
    };
    let request: Request = match serde_json::from_slice(&bytes) {
        Ok(request) => request,
        Err(error) => {
            return Pending::Refused(format!(
                "operator command refused and ignored: malformed command file: {error}"
            ))
        }
    };
    if request.sequence <= applied {
        return Pending::Nothing;
    }
    if request.version != 1 || !(1..=5000).contains(&request.budget_ms) {
        return Pending::Refused(format!(
            "operator command with sequence {} refused and ignored: unsupported version or budget",
            request.sequence
        ));
    }
    if request.sequence != applied + 1 {
        return Pending::Refused(format!(
            "operator command with sequence {} refused and ignored: the sequence must advance exactly one, to {}; the host keeps running and its last applied sequence is still {applied}",
            request.sequence,
            applied + 1
        ));
    }
    Pending::Next(request)
}
/// Sender side, before the command file is written: `sequence` must be
/// exactly one above the sequence the host last applied. A reused sequence
/// would be ignored by the host while its old reply, which carries that
/// sequence, would look like the answer to this command; a skipped one is
/// refused by the host. An earlier command that is still being applied is
/// waited for inside this command's own budget.
fn require_next_sequence(status_file: &Path, sequence: u64, deadline: Instant) -> Result<()> {
    loop {
        let reply: serde_json::Value = serde_json::from_slice(
            &read_bounded(status_file, 1024 * 1024)
                .context("the host's status file is unreadable; is the host running?")?,
        )?;
        let applied = reply["sequence"]
            .as_u64()
            .context("the host's status file carries no applied sequence")?;
        ensure!(
            applied < sequence,
            "sequence {sequence} was already used: the host last applied sequence {applied} and ignores a reused one, so nothing was sent; send the command with --sequence {}",
            applied + 1
        );
        if applied + 1 == sequence {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "sequence {sequence} skips ahead: the host last applied sequence {applied} and refuses a sequence that does not advance exactly one, so nothing was sent; send the command with --sequence {}",
            applied + 1
        );
        std::thread::sleep(Duration::from_millis(5));
    }
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
    let command_started = Instant::now();
    let automatic_join = matches!(&command, Command::JoinFresh { .. });
    let automatic_replica_receiver = matches!(
        &command,
        Command::JoinFresh {
            replica_receiver: true,
            ..
        }
    );
    let automatic_metadata = automatic_join
        || matches!(
            &command,
            Command::ServeLearnerReopen {
                automatic_metadata: true,
                ..
            }
        );
    // Registration returns committed CONFIG DATA only. The subsequent fresh
    // factory independently checks actual own MQ, TLS, schema and native birth.
    let command = match command {
        Command::JoinFresh {
            root,
            replica_receiver: _,
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
            capacity_bytes,
            buffer_pool_bytes,
            run_seconds,
        } => {
            ensure!(!root.exists(), "join-fresh refuses an existing native root");
            ensure!(
                (1..=86400).contains(&run_seconds),
                "join duration outside1..86400 seconds"
            );
            let endpoint: std::net::SocketAddr =
                address.parse().context("join needs own numeric endpoint")?;
            ensure!(endpoint.port() != 0, "join needs own fixed endpoint");
            ensure!(
                (8 * 1024 * 1024..=1024 * 1024 * 1024).contains(&buffer_pool_bytes),
                "join buffer pool outside supported bound"
            );
            ensure!(
                command_file != status_file && !command_file.exists() && !status_file.exists(),
                "join control/status paths must differ and be absent"
            );
            let actual_root = root
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."))
                .canonicalize()?
                .join(root.file_name().context("join root basename absent")?);
            for path in [&command_file, &status_file] {
                ensure!(
                    !private_parent(path)?
                        .join(path.file_name().context("join operator filename absent")?)
                        .starts_with(&actual_root),
                    "join operator files must stay outside native root"
                );
            }
            run(Command::Register {
                cluster_id: cluster_id.clone(),
                seed_node_id: seed_node_id.clone(),
                seed_address: seed_address.clone(),
                node_id: node_id.clone(),
                address: address.clone(),
                incarnation,
                capacity_bytes,
                cluster_tls_cert: cluster_tls_cert.clone(),
                cluster_tls_key: cluster_tls_key.clone(),
                cluster_tls_ca: cluster_tls_ca.clone(),
                timeout_ms: (run_seconds * 1000).min(5000),
            })?;
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
            }
        }
        other => other,
    };
    let reopening_learner = matches!(&command, Command::ServeLearnerReopen { .. });
    match command {
        Command::ServeFresh {
            root,
            manual_enrollment,
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
                Some(command_started + Duration::from_secs(run_seconds)),
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
                    data_serving: status.data_serving,
                },
            )?;
            let mut sequence = 0;
            let mut command_error: Option<String> = None;
            let mut refusal: Option<String> = None;
            let mut move_error: Option<String> = None;
            let mut last_tick = Instant::now();
            while lifetime.check().is_ok() && status.phase != NativeMetadataHostPhase::Stopped {
                match pending_request(&command_file, sequence) {
                    Pending::Nothing => {}
                    Pending::Refused(text) => {
                        // Recorded once per distinct refusal; the applied
                        // sequence does not move and the host keeps running.
                        if refusal.as_ref() != Some(&text) {
                            eprintln!("native metadata host: {text}");
                            command_error = Some(text.clone());
                            write(
                                &status_file,
                                &Reply {
                                    version: 1,
                                    sequence,
                                    process_id: std::process::id(),
                                    host: Some(&status),
                                    error: Some(text.clone()),
                                    data_serving: status.data_serving,
                                },
                            )?;
                            refusal = Some(text);
                        }
                    }
                    Pending::Next(request) => {
                        refusal = None;
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
                        command_error = error.clone();
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
                                data_serving: status.data_serving,
                            },
                        )?;
                    }
                }
                if last_tick.elapsed() >= Duration::from_millis(500) {
                    // The one general supervisor owns election/heartbeats. This
                    // separately bounded controller owns CONFIG enrollment only.
                    if matches!(
                        status.phase,
                        NativeMetadataHostPhase::MetadataControl
                            | NativeMetadataHostPhase::InitialDataOpening
                            | NativeMetadataHostPhase::InitialData
                    ) {
                        if !manual_enrollment {
                            let _ = host.start_automatic_metadata(&lifetime);
                        }
                        if !manual_enrollment
                            && status.metadata.as_ref().is_some_and(|m| {
                                m.role == bicdb_core::MetadataConsensusRole::Leader
                            })
                        {
                            let token = lifetime
                                .child_with_deadline(Instant::now() + Duration::from_millis(1250));
                            if let Ok(next) = host.maintain_metadata(&token) {
                                status = next;
                            }
                        }
                    }
                    // Process-owned range-move scheduling: starts at most one
                    // step on its own worker and never waits for it. Its
                    // outcome is the `range_move` status section only.
                    // A step that could not be started is reported on stderr,
                    // once per distinct error.
                    match host.advance_range_move(&lifetime) {
                        Ok(()) => move_error = None,
                        Err(error) => {
                            let text = error.to_string();
                            if move_error.as_ref() != Some(&text) {
                                eprintln!(
                                    "native metadata host: range move step not started: {text}"
                                );
                                move_error = Some(text);
                            }
                        }
                    }
                    if !manual_enrollment {
                        // A distinct finite process-maintenance attempt. Public
                        // EventClient request budgets remain unchanged. No root
                        // reopen, genesis retry, or caller shard action.
                        let token =
                            lifetime.child_with_deadline(Instant::now() + Duration::from_secs(5));
                        let _ = host.refresh_initial_data_registry(&token);
                    }
                    if let Ok(next) = host.status(
                        &lifetime.child_with_deadline(Instant::now() + Duration::from_millis(250)),
                    ) {
                        status = next;
                    }
                    write(
                        &status_file,
                        &Reply {
                            version: 1,
                            sequence,
                            process_id: std::process::id(),
                            host: Some(&status),
                            error: command_error.clone(),
                            data_serving: status.data_serving,
                        },
                    )?;
                    last_tick = Instant::now();
                } else if host.range_move_step_due() {
                    // The last step completed its stage: the next stage is
                    // started now, not on the next tick. A blocked, waiting
                    // or paused move keeps the tick cadence above. The stage
                    // is published at once for whoever reads the status file.
                    if host.advance_range_move(&lifetime).is_ok() {
                        if let Ok(next) = host.status(
                            &lifetime
                                .child_with_deadline(Instant::now() + Duration::from_millis(250)),
                        ) {
                            status = next;
                            write(
                                &status_file,
                                &Reply {
                                    version: 1,
                                    sequence,
                                    process_id: std::process::id(),
                                    host: Some(&status),
                                    error: command_error.clone(),
                                    data_serving: status.data_serving,
                                },
                            )?;
                        }
                    }
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
        }
        | Command::ServeLearnerReopen {
            root,
            automatic_metadata: _,
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
                (1..=86400).contains(&run_seconds)
                    && if reopening_learner {
                        root.is_dir()
                    } else {
                        !root.exists()
                    },
                "learner requires bounded duration and the explicit fresh/reopen root mode"
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
            let actual_root = if reopening_learner {
                root.canonicalize()?
            } else {
                root_parent.join(root.file_name().context("learner root basename absent")?)
            };
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
                Some(command_started + Duration::from_secs(run_seconds)),
            );
            let constructor = if reopening_learner {
                NativeMetadataLearnerHost::reopen_metadata_only
            } else {
                NativeMetadataLearnerHost::start_fresh_metadata_only
            };
            let mut host = constructor(
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
            let mut command_error: Option<String> = None;
            let mut refusal: Option<String> = None;
            let mut last_refresh = Instant::now();
            let mut stopping = false;
            while lifetime.check().is_ok() && !stopping {
                match pending_request(&command_file, sequence) {
                    Pending::Nothing => {}
                    Pending::Refused(text) => {
                        if refusal.as_ref() != Some(&text) {
                            eprintln!("native metadata learner host: {text}");
                            command_error = Some(text.clone());
                            write(
                                &status_file,
                                &Reply {
                                    version: 1,
                                    sequence,
                                    process_id: std::process::id(),
                                    host: Some(&status),
                                    error: Some(text.clone()),
                                    data_serving: false,
                                },
                            )?;
                            refusal = Some(text);
                        }
                    }
                    Pending::Next(request) => {
                        refusal = None;
                        let token = lifetime.child_with_deadline(
                            Instant::now() + Duration::from_millis(request.budget_ms),
                        );
                        let result = match request.action {
                            NativeMetadataHostAction::StartLearnerMetadata => {
                                host.start_metadata(&token)
                            }
                            NativeMetadataHostAction::OpenReplicaReceiver => host.open_replica_receiver(&token),
                            NativeMetadataHostAction::Elect => host.elect_metadata(&token),
                            NativeMetadataHostAction::Status => host.refresh(&token),
                            NativeMetadataHostAction::Stop => host.stop(&token).map(|_| {
                                stopping = true;
                                status.phase = "stopped";
                                status.metadata_listener_owned = false;
                                status.clone()
                            }),
                            _ => Err(bicdb_core::BicDbError::Unsupported(
                                "learner owner permits only start-learner-metadata/elect/status/stop"
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
                        command_error = error.clone();
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
                    let token =
                        lifetime.child_with_deadline(Instant::now() + Duration::from_secs(2));
                    let refreshed = host.refresh(&token);
                    let mut error = None;
                    match refreshed {
                        Ok(next) => {
                            status = next;
                            if automatic_metadata
                                && status.birth_bound
                                && !status.metadata_listener_owned
                            {
                                match host.start_metadata(&token) {
                                    Ok(next) => status = next,
                                    Err(e) => error = Some(e.to_string()),
                                }
                            }
                        }
                        Err(e) => error = Some(e.to_string()),
                    }
                    if automatic_metadata
                        && status.metadata_role == bicdb_core::MetadataMemberRole::Voter
                        && status.replica_receiver.is_none()
                    {
                        match host.enable_metadata_control(&token) {
                            Ok(next) => status = next,
                            Err(e) => error = Some(e.to_string()),
                        }
                    }
                    if automatic_replica_receiver
                        && status.metadata_role == bicdb_core::MetadataMemberRole::Voter
                        && (status.metadata_control_owned || status.replica_receiver.is_some())
                    {
                        match host.open_replica_receiver(&token) {
                            Ok(next) => status = next,
                            Err(failure) => error = Some(failure.to_string()),
                        }
                    }
                    if automatic_metadata
                        && status.metadata_control_owned
                        && status
                            .metadata
                            .as_ref()
                            .is_some_and(|m| m.role == bicdb_core::MetadataConsensusRole::Leader)
                    {
                        match host.maintain_metadata(&token) {
                            Ok(next) => status = next,
                            Err(e) => error = Some(e.to_string()),
                        }
                    }
                    if automatic_metadata && status.metadata_listener_owned {
                        if let Err(e) = host.start_automatic_metadata(&lifetime) {
                            error = Some(e.to_string());
                        }
                    }
                    write(
                        &status_file,
                        &Reply {
                            version: 1,
                            sequence,
                            process_id: std::process::id(),
                            host: Some(&status),
                            error: if sequence > 0 || refusal.is_some() {
                                command_error.clone()
                            } else {
                                error
                            },
                            data_serving: false,
                        },
                    )?;
                    last_refresh = Instant::now();
                }
                // Process-owned joint handoff host commands of this member:
                // at most one per pass, outcome retained for the
                // coordinator's repeated step. Without a recorded command
                // this reads one memory slot, so it is asked every pass and a
                // recorded command does not wait for the next 500 ms tick.
                if !stopping {
                    let _ = host.advance_joint_bridge_host_commands(&lifetime);
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            host.stop(
                &CancellationToken::uncancelable()
                    .child_with_deadline(Instant::now() + Duration::from_secs(5)),
            )?;
        }
        Command::JoinFresh { .. } => unreachable!("join-fresh is normalized before dispatch"),
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
            require_next_sequence(&status_file, sequence, deadline)?;
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
        Command::MoveRange {
            command_file,
            status_file,
            sequence,
            range_id,
            target_node_id,
            abort,
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
            ensure!(
                abort || target_node_id.is_some(),
                "target-node-id is required to start a range move"
            );
            let range = bicdb_core::RangeId::new(range_id)?;
            let action = match target_node_id {
                Some(target) if !abort => NativeMetadataHostAction::MoveRange {
                    range,
                    target: ClusterNodeId::new(target)?,
                },
                _ => NativeMetadataHostAction::AbortMoveRange { range },
            };
            let deadline = Instant::now() + Duration::from_millis(timeout_ms);
            require_next_sequence(&status_file, sequence, deadline)?;
            write(
                &command_file,
                &Request {
                    version: 1,
                    sequence,
                    action,
                    budget_ms: timeout_ms,
                },
            )?;
            loop {
                let reply: serde_json::Value =
                    serde_json::from_slice(&read_bounded(&status_file, 1024 * 1024)?)?;
                if reply["sequence"].as_u64() == Some(sequence) {
                    ensure!(
                        reply["error"].is_null(),
                        "native range move refused: {}",
                        reply["error"]
                    );
                    println!("{}", serde_json::to_string(&reply["host"]["range_move"])?);
                    break;
                }
                ensure!(
                    Instant::now() < deadline,
                    "native range move request timed out; actual pending state is retained"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        Command::TransferMetadataLeadership {
            command_file,
            status_file,
            sequence,
            to,
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
            require_next_sequence(&status_file, sequence, deadline)?;
            write(
                &command_file,
                &Request {
                    version: 1,
                    sequence,
                    action: NativeMetadataHostAction::TransferMetadataLeadership {
                        to: ClusterNodeId::new(to)?,
                    },
                    budget_ms: timeout_ms,
                },
            )?;
            loop {
                let reply: serde_json::Value =
                    serde_json::from_slice(&read_bounded(&status_file, 1024 * 1024)?)?;
                if reply["sequence"].as_u64() == Some(sequence) {
                    ensure!(
                        reply["error"].is_null(),
                        "native metadata leadership transfer refused: {}",
                        reply["error"]
                    );
                    println!(
                        "{}",
                        serde_json::to_string(&reply["host"]["last_metadata_leadership_transfer"])?
                    );
                    break;
                }
                ensure!(
                    Instant::now() < deadline,
                    "native metadata leadership transfer request timed out; read `metadata_leader` in the host status"
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
