use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use bicdb_app_runtime::HttpHostPolicy;
use bicdb_cell::{
    bind_volume, load_manifest_json, load_signing_key, load_verified_manifest,
    open_inherited_key_lease_reader, parse_trusted_key_specs, rotate_cell_key, sha256_file,
    sign_manifest, AttestedKeyLeaseCellKeyProvider, CellAdmissionRuntimeConfig,
    CellApplicationHostConfig, CellDeviceRuntimeConfig, CellGrantRuntimeConfig,
    CellHaRuntimeConfig, CellId, CellKeyProvider, CellKeyRotationConfig, CellReplicaRole,
    CellRuntime, CellRuntimeConfig, DevelopmentFileCellKeyProvider, Sha256Digest,
};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "bicdb-cell",
    version,
    about = "Capability-reduced BicDB Cell runtime"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
enum Command {
    /// Verify and open exactly one signed, volume-bound cell.
    Serve(ServeArgs),
    /// Verify a signed cell manifest without opening storage.
    Verify {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long = "trusted-key")]
        trusted_keys: Vec<String>,
    },
    /// Offline, crash-safe rotation through an exact signed Phase-2 manifest transition.
    RotateKey(RotateKeyArgs),
    /// Sign a strict JSON CellManifest as deterministic CBOR.
    ManifestSign {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        signer_key_id: String,
        #[arg(long)]
        signing_key: PathBuf,
    },
    /// Immutably bind an empty volume to a verified manifest.
    VolumeBind {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        volume: PathBuf,
        #[arg(long = "trusted-key")]
        trusted_keys: Vec<String>,
        #[arg(long)]
        signer_key_id: String,
        #[arg(long)]
        signing_key: PathBuf,
    },
    /// Derive an Ed25519 verifying key from a restricted signing-key seed.
    KeyPublic {
        #[arg(long)]
        signing_key: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Print a manifest-compatible sha256 digest for a file.
    Digest { path: PathBuf },
}

#[derive(Clone, Debug, clap::Args)]
struct RotateKeyArgs {
    #[arg(long)]
    current_manifest: PathBuf,
    #[arg(long)]
    next_manifest: PathBuf,
    #[arg(long)]
    volume: PathBuf,
    #[arg(long)]
    expected_cell_id: String,
    #[arg(long)]
    expected_volume_id: String,
    #[arg(long)]
    guest_image_digest: String,
    #[arg(long = "trusted-key")]
    trusted_keys: Vec<String>,
    #[arg(long)]
    current_key_lease_fd: i32,
    #[arg(long)]
    current_attestation_nonce: String,
    #[arg(long)]
    next_key_lease_fd: i32,
    #[arg(long)]
    next_attestation_nonce: String,
    #[arg(long = "kms-trusted-key")]
    kms_trusted_keys: Vec<String>,
    #[arg(long)]
    json: bool,
}

#[derive(Clone, Debug, clap::Args)]
struct ServeArgs {
    #[arg(long)]
    manifest: PathBuf,
    #[arg(long)]
    volume: PathBuf,
    #[arg(long)]
    expected_cell_id: String,
    #[arg(long)]
    expected_volume_id: String,
    /// Externally measured guest image digest bound to the signed manifest.
    #[arg(long)]
    guest_image_digest: String,
    #[arg(long = "trusted-key")]
    trusted_keys: Vec<String>,
    /// Development-only fixed key file for Phase-1 manifests.
    #[arg(long)]
    key_file: Option<PathBuf>,
    /// Inherited pipe containing one short-lived, signed KMS key lease.
    #[arg(long, conflicts_with = "key_file")]
    key_lease_fd: Option<i32>,
    /// Independent KMS lease-signing keys (`key-id=path`).
    #[arg(long = "kms-trusted-key")]
    kms_trusted_keys: Vec<String>,
    /// Fresh attestation challenge bound into the signed key lease.
    #[arg(long)]
    attestation_nonce: Option<String>,
    #[arg(long)]
    artifact_root: PathBuf,
    /// Manifest-pinned application release roots. Required when applications
    /// are pinned in the CellManifest.
    #[arg(long)]
    release_policy: Option<PathBuf>,
    /// Manifest-pinned OIDC identity verifier policy.
    #[arg(long)]
    identity_policy: Option<PathBuf>,
    /// Manifest-pinned application egress policy.
    #[arg(long)]
    egress_policy: Option<PathBuf>,
    /// Manifest-pinned cell-local members and device keys (Phase 3).
    #[arg(long)]
    authorization_policy: Option<PathBuf>,
    /// Manifest-pinned exact-binary database feature certification (Phase 3).
    #[arg(long)]
    feature_certification: Option<PathBuf>,
    /// Manifest-pinned threshold fleet trust policy (Phase 4).
    #[arg(long)]
    fleet_trust_policy: Option<PathBuf>,
    /// Signed release/transparency/cohort/Cell activation bundle (Phase 4).
    #[arg(long)]
    fleet_activation_bundle: Option<PathBuf>,
    /// Signed immediately preceding CellManifest for a Phase-4 transition.
    #[arg(long)]
    previous_manifest: Option<PathBuf>,
    /// Manifest-pinned Phase-5 HA authority policy.
    #[arg(long)]
    ha_trust_policy: Option<PathBuf>,
    /// Quorum-certified active writer epoch.
    #[arg(long)]
    ha_writer_epoch: Option<PathBuf>,
    /// Quorum-certified short-lived authority for this replica.
    #[arg(long)]
    ha_replica_lease: Option<PathBuf>,
    /// Certified immediately preceding writer epoch (promotions only).
    #[arg(long)]
    ha_previous_writer_epoch: Option<PathBuf>,
    /// Certified predecessor primary lease (promotions only).
    #[arg(long)]
    ha_previous_primary_lease: Option<PathBuf>,
    /// Workload-local signing seed matching the replica lease public key.
    #[arg(long)]
    ha_replica_signing_key: Option<PathBuf>,
    /// Manifest-pinned Phase-6 device authority and offline-bound policy.
    #[arg(long)]
    device_trust_policy: Option<PathBuf>,
    /// Cell-local export signing key id named in the device trust policy.
    #[arg(long)]
    device_exporter_key_id: Option<String>,
    /// Cell-local export signing seed. This key cannot unwrap a Cell key.
    #[arg(long)]
    device_exporter_signing_key: Option<PathBuf>,
    /// Manifest-pinned Phase-7 cross-Cell grant trust policy.
    #[arg(long)]
    grant_trust_policy: Option<PathBuf>,
    /// Cell-local grant exporter key id named in the grant trust policy.
    #[arg(long)]
    grant_exporter_key_id: Option<String>,
    /// Cell-local grant exporter signing seed. This key cannot unwrap a Cell key.
    #[arg(long)]
    grant_exporter_signing_key: Option<PathBuf>,
    /// Cell-local HPKE recipient key id named in the grant trust policy.
    #[arg(long)]
    grant_recipient_key_id: Option<String>,
    /// Cell-local HPKE recipient private key. It can decrypt only packages for
    /// the matching recipient key certificate.
    #[arg(long)]
    grant_recipient_private_key: Option<PathBuf>,
    /// Manifest-pinned Phase-8 admission authority policy.
    #[arg(long)]
    admission_trust_policy: Option<PathBuf>,
    /// Exact twelve-gate evidence, attestation, checkpoint, and authorization bundle.
    #[arg(long)]
    admission_evidence_bundle: Option<PathBuf>,
    /// Actual launcher-selected isolation tier bound by the attestation and policy.
    #[arg(long)]
    deployment_isolation_tier: Option<String>,
    /// Authenticated application HTTP listener. Cleartext is loopback-only;
    /// non-loopback addresses require the TLS pair below.
    #[arg(long)]
    http_listen: Option<SocketAddr>,
    #[arg(long)]
    http_tls_cert: Option<PathBuf>,
    #[arg(long)]
    http_tls_key: Option<PathBuf>,
    /// Complete the startup ceremony and exit instead of holding the cell.
    #[arg(long)]
    check: bool,
    #[arg(long)]
    json: bool,
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Serve(args) => serve(args),
        Command::Verify {
            manifest,
            trusted_keys,
        } => {
            let trusted = parse_trusted_key_specs(&trusted_keys)?;
            let verified = load_verified_manifest(&manifest, &trusted)?;
            println!(
                "verified {} generation {} {}",
                verified.manifest.cell_id, verified.manifest.manifest_generation, verified.digest
            );
            Ok(())
        }
        Command::RotateKey(args) => rotate_key(args),
        Command::ManifestSign {
            input,
            output,
            signer_key_id,
            signing_key,
        } => {
            if output.exists() {
                bail!(
                    "refusing to replace existing signed manifest {}",
                    output.display()
                );
            }
            let manifest = load_manifest_json(&input)?;
            let signing_key = load_signing_key(&signing_key)?;
            let signed = sign_manifest(&manifest, &signer_key_id, &signing_key)?;
            write_new_restricted(&output, &signed)?;
            println!("signed manifest {}", output.display());
            Ok(())
        }
        Command::VolumeBind {
            manifest,
            volume,
            trusted_keys,
            signer_key_id,
            signing_key,
        } => {
            let trusted = parse_trusted_key_specs(&trusted_keys)?;
            let verified = load_verified_manifest(&manifest, &trusted)?;
            let signing_key = load_signing_key(&signing_key)?;
            let trusted_signer = trusted.get(&signer_key_id).ok_or_else(|| {
                anyhow::anyhow!("volume signer {signer_key_id} is not in the trusted key set")
            })?;
            if trusted_signer != &signing_key.verifying_key() {
                bail!("volume signing key does not match trusted signer {signer_key_id}");
            }
            bind_volume(&volume, &verified, &signer_key_id, &signing_key)?;
            println!(
                "bound volume {} to cell {}",
                verified.manifest.storage.volume_id, verified.manifest.cell_id
            );
            Ok(())
        }
        Command::KeyPublic {
            signing_key,
            output,
        } => {
            if output.exists() {
                bail!("refusing to replace verifying key {}", output.display());
            }
            let signing_key = load_signing_key(&signing_key)?;
            write_new_restricted(&output, signing_key.verifying_key().as_bytes())?;
            println!("wrote verifying key {}", output.display());
            Ok(())
        }
        Command::Digest { path } => {
            println!("{}", sha256_file(&path)?);
            Ok(())
        }
    }
}

fn rotate_key(args: RotateKeyArgs) -> Result<()> {
    if args.current_key_lease_fd == args.next_key_lease_fd {
        bail!("current and next key leases require distinct inherited pipes");
    }
    let trusted_manifest_keys = parse_trusted_key_specs(&args.trusted_keys)?;
    let trusted_kms_keys = parse_trusted_key_specs(&args.kms_trusted_keys)?;
    let current_provider = AttestedKeyLeaseCellKeyProvider::new(
        open_inherited_key_lease_reader(args.current_key_lease_fd)?,
        trusted_kms_keys.clone(),
        args.current_attestation_nonce,
    )?;
    let next_provider = AttestedKeyLeaseCellKeyProvider::new(
        open_inherited_key_lease_reader(args.next_key_lease_fd)?,
        trusted_kms_keys,
        args.next_attestation_nonce,
    )?;
    let report = rotate_cell_key(CellKeyRotationConfig {
        current_manifest_path: args.current_manifest,
        next_manifest_path: args.next_manifest,
        volume_path: args.volume,
        expected_cell_id: CellId::parse(args.expected_cell_id)?,
        expected_volume_id: args.expected_volume_id,
        expected_guest_image_digest: Sha256Digest::parse(args.guest_image_digest)?,
        trusted_manifest_keys,
        current_key_provider: Box::new(current_provider),
        next_key_provider: Box::new(next_provider),
    })?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("BicDB Cell Key Rotation");
        println!("Cell ID             {}", report.cell_id);
        println!("Volume ID           {}", report.volume_id);
        println!(
            "Manifest generation {} -> {}",
            report.previous_manifest_generation, report.active_manifest_generation
        );
        println!(
            "Key epoch           {} -> {}",
            report.storage.old_key_epoch, report.storage.new_key_epoch
        );
        println!("Storage activated   {}", report.storage.activated);
        println!(
            "Retired ciphertext  {}",
            report.storage.retired_path.display()
        );
        println!("Regulated-data admission  DENIED");
    }
    Ok(())
}

fn serve(args: ServeArgs) -> Result<()> {
    if args.check && args.http_listen.is_some() {
        bail!("--check cannot be combined with --http-listen");
    }
    let tls = match (args.http_tls_cert, args.http_tls_key) {
        (Some(certificate), Some(private_key)) => Some((certificate, private_key)),
        (None, None) => None,
        _ => bail!("--http-tls-cert and --http-tls-key must be supplied together"),
    };
    if args.http_listen.is_none() && tls.is_some() {
        bail!("cell application TLS credentials require --http-listen");
    }
    if args
        .http_listen
        .is_some_and(|address| !address.ip().is_loopback() && tls.is_none())
    {
        bail!("a non-loopback cell application listener requires TLS");
    }
    let trusted = parse_trusted_key_specs(&args.trusted_keys)?;
    let expected_cell_id = CellId::parse(args.expected_cell_id)?;
    let guest_image_digest = Sha256Digest::parse(args.guest_image_digest)?;
    let verified = load_verified_manifest(&args.manifest, &trusted)?;
    if args.http_listen.is_some() && verified.manifest.applications.is_empty() {
        bail!("--http-listen requires applications pinned in the CellManifest");
    }
    if args.http_listen.is_some()
        && verified.manifest.uses_cell_ha()
        && verified.manifest.replication.role != CellReplicaRole::Primary
    {
        bail!("a standby/recovery Cell cannot construct an application listener");
    }
    let key_provider: Box<dyn CellKeyProvider> = if verified.manifest.uses_bound_cryptography() {
        let fd = args.key_lease_fd.ok_or_else(|| {
            anyhow::anyhow!("bound-cryptography manifests require --key-lease-fd")
        })?;
        if args.key_file.is_some() {
            bail!("bound-cryptography manifests refuse --key-file");
        }
        let nonce = args.attestation_nonce.ok_or_else(|| {
            anyhow::anyhow!("bound-cryptography manifests require --attestation-nonce")
        })?;
        let kms_keys = parse_trusted_key_specs(&args.kms_trusted_keys)?;
        Box::new(AttestedKeyLeaseCellKeyProvider::new(
            open_inherited_key_lease_reader(fd)?,
            kms_keys,
            nonce,
        )?)
    } else {
        if args.key_lease_fd.is_some()
            || !args.kms_trusted_keys.is_empty()
            || args.attestation_nonce.is_some()
        {
            bail!("Phase-1 manifests accept only --key-file, not attested lease options");
        }
        let key_file = args
            .key_file
            .ok_or_else(|| anyhow::anyhow!("Phase-1 manifests require --key-file"))?;
        Box::new(DevelopmentFileCellKeyProvider::new(
            key_file,
            expected_cell_id.clone(),
            verified.manifest.keys.cell_kek_id.clone(),
        ))
    };
    let application_host = match (
        args.release_policy,
        args.identity_policy,
        args.egress_policy,
        args.authorization_policy,
        args.feature_certification,
        args.fleet_trust_policy,
        args.fleet_activation_bundle,
        args.previous_manifest,
    ) {
        (
            Some(release_policy_path),
            Some(identity_policy_path),
            Some(egress_policy_path),
            authorization_policy_path,
            feature_certification_path,
            fleet_trust_policy_path,
            fleet_activation_bundle_path,
            previous_manifest_path,
        ) if authorization_policy_path.is_some() == feature_certification_path.is_some()
            && fleet_trust_policy_path.is_some() == fleet_activation_bundle_path.is_some()
            && (previous_manifest_path.is_none() || fleet_trust_policy_path.is_some()) =>
        {
            Some(CellApplicationHostConfig {
                release_policy_path,
                identity_policy_path,
                egress_policy_path,
                authorization_policy_path,
                feature_certification_path,
                fleet_trust_policy_path,
                fleet_activation_bundle_path,
                previous_manifest_path,
            })
        }
        (None, None, None, None, None, None, None, None) => None,
        _ => bail!(
            "release/identity/egress policies must be supplied together; authorization/feature-certification and fleet-policy/activation-bundle are required pairs"
        ),
    };
    let ha = match (
        args.ha_trust_policy,
        args.ha_writer_epoch,
        args.ha_replica_lease,
        args.ha_previous_writer_epoch,
        args.ha_previous_primary_lease,
        args.ha_replica_signing_key,
    ) {
        (
            Some(trust_policy_path),
            Some(writer_epoch_path),
            Some(replica_lease_path),
            previous_writer_epoch_path,
            previous_primary_lease_path,
            Some(replica_signing_key_path),
        ) if previous_writer_epoch_path.is_some() == previous_primary_lease_path.is_some() => {
            Some(CellHaRuntimeConfig {
                trust_policy_path,
                writer_epoch_path,
                replica_lease_path,
                previous_writer_epoch_path,
                previous_primary_lease_path,
                replica_signing_key_path,
            })
        }
        (None, None, None, None, None, None) => None,
        _ => bail!(
            "Phase-5 HA policy, epoch, replica lease, and replica signing key are required together; predecessor epoch/lease are an optional pair"
        ),
    };
    let device = match (
        args.device_trust_policy,
        args.device_exporter_key_id,
        args.device_exporter_signing_key,
    ) {
        (Some(trust_policy_path), Some(exporter_key_id), Some(exporter_signing_key_path)) => {
            Some(CellDeviceRuntimeConfig {
                trust_policy_path,
                exporter_key_id,
                exporter_signing_key_path,
            })
        }
        (None, None, None) => None,
        _ => bail!(
            "Phase-6 device trust policy, exporter key id, and exporter signing key are required together"
        ),
    };
    let grant = match (
        args.grant_trust_policy,
        args.grant_exporter_key_id,
        args.grant_exporter_signing_key,
        args.grant_recipient_key_id,
        args.grant_recipient_private_key,
    ) {
        (
            Some(trust_policy_path),
            Some(exporter_key_id),
            Some(exporter_signing_key_path),
            Some(recipient_key_id),
            Some(recipient_private_key_path),
        ) => Some(CellGrantRuntimeConfig {
            trust_policy_path,
            exporter_key_id,
            exporter_signing_key_path,
            recipient_key_id,
            recipient_private_key_path,
        }),
        (None, None, None, None, None) => None,
        _ => bail!(
            "Phase-7 grant trust policy, exporter key id/signing key, and recipient key id/private key are required together"
        ),
    };
    let admission = match (
        args.admission_trust_policy,
        args.admission_evidence_bundle,
        args.deployment_isolation_tier,
    ) {
        (
            Some(trust_policy_path),
            Some(evidence_bundle_path),
            Some(expected_deployment_isolation_tier),
        ) => Some(CellAdmissionRuntimeConfig {
            trust_policy_path,
            evidence_bundle_path,
            expected_deployment_isolation_tier,
        }),
        (None, None, None) => None,
        _ => bail!(
            "Phase-8 admission trust policy, evidence bundle, and deployment isolation tier are required together"
        ),
    };
    let runtime = CellRuntime::open(CellRuntimeConfig {
        manifest_path: args.manifest,
        volume_path: args.volume,
        expected_cell_id,
        expected_volume_id: args.expected_volume_id,
        expected_guest_image_digest: guest_image_digest,
        artifact_root: args.artifact_root,
        trusted_manifest_keys: trusted,
        key_provider,
        application_host,
        ha,
        device,
        grant,
        admission,
    })?;
    if args.check {
        print_runtime(&runtime, None, args.json)?;
        return runtime.close().map_err(Into::into);
    }
    let _admission_watchdog = runtime.start_admission_watchdog()?;
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_handler = Arc::clone(&stop);
    ctrlc::set_handler(move || stop_for_handler.store(true, Ordering::SeqCst))?;
    if let Some(address) = args.http_listen {
        let host = runtime
            .application_host()
            .expect("application pins were validated before cell construction");
        let async_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        async_runtime.block_on(async {
            let server = match tls {
                Some((certificate, private_key)) => {
                    host.serve_http_tls(
                        address,
                        certificate,
                        private_key,
                        HttpHostPolicy::default(),
                    )
                    .await?
                }
                None => {
                    let listener = tokio::net::TcpListener::bind(address).await?;
                    host.serve_http(listener, HttpHostPolicy::default()).await?
                }
            };
            print_runtime(&runtime, Some(server.address), args.json)?;
            while !stop.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            server.shutdown().await.map_err(anyhow::Error::from)
        })?;
    } else {
        print_runtime(&runtime, None, args.json)?;
        while !stop.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    runtime.close()?;
    Ok(())
}

fn print_runtime(runtime: &CellRuntime, listener: Option<SocketAddr>, json: bool) -> Result<()> {
    let report = runtime.admission_report();
    let readiness = runtime.application_host().map(|host| host.readiness());
    let ha_status = runtime.ha_status();
    let grant_policy = runtime.grant_trust_policy();
    let admission_evidence = runtime.verified_admission_evidence();
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "runtime": "BicDB Cell Runtime",
                "cell_id": runtime.manifest().cell_id.as_str(),
                "volume_id": runtime.manifest().storage.volume_id,
                "manifest_generation": runtime.manifest().manifest_generation,
                "manifest_digest": runtime.manifest_digest().as_str(),
                "application_count": runtime.manifest().applications.len(),
                "application_readiness": readiness,
                "key_provider": runtime.key_provider_kind(),
                "application_listener": listener.map(|address| address.to_string()),
                "pgwire_listener": "not_constructed",
                "remote_cell_handle": "not_constructed",
                "object_grant_policy": grant_policy.map(|policy| policy.policy_id.as_str()),
                "admission_evidence_complete": admission_evidence.map(|evidence| evidence.evidence_complete),
                "admission_checkpoint_sequence": admission_evidence.map(|evidence| evidence.checkpoint_sequence),
                "admission_authorization_expires_at": admission_evidence.map(|evidence| evidence.authorization_expires_at),
                "ha": ha_status,
                "regulated_data_admission": report,
            }))?
        );
    } else {
        println!("BicDB Cell Runtime");
        println!("Cell ID             {}", runtime.manifest().cell_id);
        println!(
            "Volume ID           {}",
            runtime.manifest().storage.volume_id
        );
        println!(
            "Manifest generation {}",
            runtime.manifest().manifest_generation
        );
        println!(
            "Application pins    {}",
            runtime.manifest().applications.len()
        );
        println!(
            "Application host    {}",
            readiness
                .map(|readiness| if readiness.ready {
                    "READY"
                } else {
                    "NOT READY"
                })
                .unwrap_or("NOT CONSTRUCTED")
        );
        println!(
            "Application listener {}",
            listener
                .map(|address| address.to_string())
                .unwrap_or_else(|| "DISABLED".to_string())
        );
        println!("Pgwire listener     NOT CONSTRUCTED");
        println!("Remote Cell handle  NOT CONSTRUCTED");
        println!(
            "Object grants       {}",
            grant_policy
                .map(|policy| format!("READY ({})", policy.policy_id))
                .unwrap_or_else(|| "NOT CONSTRUCTED".to_string())
        );
        println!(
            "Admission evidence  {}",
            admission_evidence
                .map(|evidence| format!(
                    "VERIFIED (12/12; checkpoint {}; authorization expires {})",
                    evidence.checkpoint_sequence, evidence.authorization_expires_at
                ))
                .unwrap_or_else(|| "NOT CONSTRUCTED".to_string())
        );
        println!(
            "Cell HA              {}",
            ha_status
                .map(|status| format!(
                    "{:?} epoch {} lease {} expires {}{}",
                    status.role,
                    status.writer_epoch,
                    status.lease_sequence,
                    status.expires_at,
                    if status.revoked { " FENCED" } else { "" }
                ))
                .unwrap_or_else(|| "NOT CONSTRUCTED".to_string())
        );
        println!(
            "Regulated-data admission  {} ({})",
            if report.regulated_data_admitted {
                "ADMITTED"
            } else {
                "DENIED"
            },
            report.phase
        );
    }
    Ok(())
}

fn write_new_restricted(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
