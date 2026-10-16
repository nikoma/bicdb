use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use clap::{Parser, Subcommand};
use uuid::Uuid;

use crate::{manifest::Release, store::write_synced};

#[derive(Debug, Parser)]
#[command(name = "bic", about = "Deploy and operate durable BicDB applications")]
pub struct Cli {
    /// Local platform state and default management-token location.
    #[arg(long, global = true, default_value = ".bicdb-platform")]
    pub data: PathBuf,
    /// Management service URL. Remote services require HTTPS.
    #[arg(long, global = true, default_value = "http://127.0.0.1:8788")]
    pub endpoint: String,
    /// File containing the management token; defaults to DATA/management.token.
    #[arg(long, global = true)]
    pub token_file: Option<PathBuf>,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Create a fetch application with a database migration and durable job handler.
    Init {
        #[arg(default_value = ".")]
        directory: PathBuf,
        #[arg(long, default_value = "my-app")]
        name: String,
    },
    /// Start the routing, database and application supervisor.
    Serve {
        #[arg(long, default_value = "127.0.0.1:8788")]
        listen: SocketAddr,
        #[arg(long)]
        node: Option<PathBuf>,
        /// Operator-owned Linux microVM images and private network allocations.
        #[arg(long)]
        microvm_config: Option<PathBuf>,
        /// Operator-owned worker fleet and placement allocations.
        #[arg(long)]
        fleet_config: Option<PathBuf>,
    },
    /// Run a leased compute worker attached to an authoritative state service.
    Worker {
        #[arg(long, default_value = "127.0.0.1:8789")]
        listen: SocketAddr,
        #[arg(long)]
        node: Option<PathBuf>,
        #[arg(long)]
        state_endpoint: String,
        #[arg(long, default_value_t = 16)]
        capacity: usize,
        #[arg(long)]
        microvm_config: Option<PathBuf>,
    },
    #[command(hide = true)]
    VmExecute { specification: PathBuf },
    /// Upload a release, apply pending migrations and activate its endpoints.
    Deploy {
        #[arg(default_value = "bic.json")]
        manifest: PathBuf,
    },
    /// Download an encrypted application recovery image, including local jobs.
    Backup {
        application: String,
        output: PathBuf,
        #[arg(long)]
        passphrase_file: PathBuf,
    },
    /// Restore an application into an unused local catalog, always inactive.
    Restore {
        archive: PathBuf,
        #[arg(long)]
        passphrase_file: PathBuf,
    },
    /// Inspect desired and running application state.
    List,
    /// Inspect the stable database node ID, signing key and active release.
    Identity { application: String },
    /// Configure authorized synchronization peers and inspect their status.
    Peer {
        #[command(subcommand)]
        command: PeerCommand,
    },
    /// Generate a separate application-peer transport token.
    PeerToken { output: PathBuf },
    /// Stop an application while retaining its durable state.
    Stop { application: String },
    /// Start a stopped application using its persisted release.
    Start { application: String },
    /// Activate the prior release if its migration history remains compatible.
    Rollback { application: String },
    /// Inspect durable job delivery and dead-letter counts.
    Jobs { application: String },
    /// Read the last 64 KiB of runtime output.
    Logs { application: String },
}

#[derive(Debug, Subcommand)]
pub enum PeerCommand {
    List {
        application: String,
    },
    Add {
        application: String,
        #[arg(long)]
        node_id: bicdb_core::NodeId,
        #[arg(long)]
        public_key: String,
        #[arg(long)]
        peer_endpoint: String,
        #[arg(long)]
        peer_token_file: PathBuf,
    },
    Remove {
        application: String,
        node_id: bicdb_core::NodeId,
    },
}

pub fn token(path: &Path) -> Result<String> {
    use std::io::Read;
    let file = std::fs::File::open(path)
        .context("read secret file; provide the owner-only token or key file")?;
    let metadata = file.metadata()?;
    ensure!(metadata.len() <= 4096, "invalid secret file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "secret file must have mode 0600"
        );
    }
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 4096, "secret file exceeds size limit");
    let value = String::from_utf8(bytes)?.trim().to_owned();
    ensure!(
        value.len() >= 32 && !value.chars().any(char::is_whitespace),
        "invalid management token"
    );
    Ok(value)
}

pub fn run(cli: Cli) -> Result<()> {
    let token_path = cli
        .token_file
        .unwrap_or_else(|| cli.data.join("management.token"));
    match cli.command {
        Command::VmExecute { specification } => crate::microvm::execute(&specification),
        Command::Init { directory, name } => init(&directory, &name),
        Command::PeerToken { output } => {
            write_synced(
                &output,
                format!("{}{}\n", Uuid::new_v4().simple(), Uuid::new_v4().simple()).as_bytes(),
            )?;
            println!("Created {}", output.display());
            Ok(())
        }
        Command::Restore {
            archive,
            passphrase_file,
        } => {
            let passphrase = token(&passphrase_file)?;
            let store = crate::store::Store::open(&cli.data)?;
            let deployment = crate::recovery::restore(&store, &archive, &passphrase)?;
            println!("{}", serde_json::to_string(&deployment)?);
            Ok(())
        }
        Command::Serve {
            listen,
            node,
            microvm_config,
            fleet_config,
        } => {
            std::fs::create_dir_all(&cli.data)?;
            if !token_path.exists() {
                if let Some(parent) = token_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                write_synced(
                    &token_path,
                    format!("{}{}\n", Uuid::new_v4().simple(), Uuid::new_v4().simple()).as_bytes(),
                )?;
            }
            let token = token(&token_path)?;
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(crate::server::serve(
                    cli.data,
                    listen,
                    token,
                    node,
                    microvm_config,
                    fleet_config,
                ))
        }
        Command::Worker {
            listen,
            node,
            state_endpoint,
            capacity,
            microvm_config,
        } => {
            std::fs::create_dir_all(&cli.data)?;
            if !token_path.exists() {
                write_synced(
                    &token_path,
                    format!("{}{}\n", Uuid::new_v4().simple(), Uuid::new_v4().simple()).as_bytes(),
                )?;
            }
            let token = token(&token_path)?;
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(crate::worker::serve(
                    cli.data,
                    listen,
                    token,
                    node,
                    state_endpoint,
                    capacity,
                    microvm_config,
                ))
        }
        command => {
            let url = reqwest::Url::parse(&cli.endpoint)?;
            ensure!(
                url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && url.path() == "/",
                "endpoint must be an origin URL without credentials"
            );
            let local = matches!(
                url.host_str(),
                Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
            );
            ensure!(
                url.scheme() == "https" || (url.scheme() == "http" && local),
                "remote management requires HTTPS"
            );
            let management_token = token(&token_path)?;
            let client = reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(120))
                .redirect(reqwest::redirect::Policy::none())
                .build()?;
            let base = cli.endpoint.trim_end_matches('/');
            let backup_output = if let Command::Backup { output, .. } = &command {
                ensure!(!output.exists(), "backup output already exists");
                Some(output.clone())
            } else {
                None
            };
            let request = match command {
                Command::Deploy { manifest } => client
                    .post(format!("{base}/v1/apps"))
                    .json(&Release::read(&manifest)?),
                Command::Backup {
                    application,
                    output: _,
                    passphrase_file,
                } => {
                    ensure!(
                        crate::manifest::valid_name(&application),
                        "invalid application name"
                    );
                    let passphrase = token(&passphrase_file)?;
                    client
                        .post(format!("{base}/v1/apps/{application}/backup"))
                        .timeout(std::time::Duration::from_secs(3600))
                        .json(&serde_json::json!({"passphrase":passphrase}))
                }
                Command::List => client.get(format!("{base}/v1/apps")),
                Command::Jobs { application } => {
                    ensure!(
                        crate::manifest::valid_name(&application),
                        "invalid application name"
                    );
                    client.get(format!("{base}/v1/apps/{application}/jobs"))
                }
                Command::Identity { application } => {
                    ensure!(
                        crate::manifest::valid_name(&application),
                        "invalid application name"
                    );
                    client.get(format!("{base}/v1/apps/{application}/identity"))
                }
                Command::Peer { command } => match command {
                    PeerCommand::List { application } => {
                        ensure!(
                            crate::manifest::valid_name(&application),
                            "invalid application name"
                        );
                        client.get(format!("{base}/v1/apps/{application}/peers"))
                    }
                    PeerCommand::Add {
                        application,
                        node_id,
                        public_key,
                        peer_endpoint,
                        peer_token_file,
                    } => {
                        ensure!(
                            crate::manifest::valid_name(&application),
                            "invalid application name"
                        );
                        let peer = crate::sync::Peer {
                            node_id,
                            public_key,
                            endpoint: peer_endpoint,
                            token: token(&peer_token_file)?,
                        };
                        peer.validate()?;
                        client
                            .post(format!("{base}/v1/apps/{application}/peers"))
                            .json(&peer)
                    }
                    PeerCommand::Remove {
                        application,
                        node_id,
                    } => {
                        ensure!(
                            crate::manifest::valid_name(&application),
                            "invalid application name"
                        );
                        client.delete(format!("{base}/v1/apps/{application}/peers/{node_id}"))
                    }
                },
                Command::Logs { application } => {
                    ensure!(
                        crate::manifest::valid_name(&application),
                        "invalid application name"
                    );
                    client.get(format!("{base}/v1/apps/{application}/logs"))
                }
                Command::Stop { application } => operation(&client, base, &application, "stop")?,
                Command::Start { application } => operation(&client, base, &application, "start")?,
                Command::Rollback { application } => {
                    operation(&client, base, &application, "rollback")?
                }
                _ => unreachable!(),
            };
            let response = request
                .bearer_auth(management_token)
                .send()
                .context("contact application platform")?;
            let status = response.status();
            if let Some(output) = backup_output {
                ensure!(status.is_success(), "backup request failed: {status}");
                let temporary = output.with_extension(format!("download-{}.tmp", Uuid::new_v4()));
                let result = (|| -> Result<()> {
                    let mut options = std::fs::OpenOptions::new();
                    options.write(true).create_new(true);
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::OpenOptionsExt;
                        options.mode(0o600);
                    }
                    let mut file = options.open(&temporary)?;
                    let mut response = response;
                    std::io::copy(&mut response, &mut file)?;
                    file.sync_all()?;
                    // Publish without replacing an existing file, even if it
                    // appeared while the network download was in progress.
                    std::fs::hard_link(&temporary, &output)?;
                    std::fs::remove_file(&temporary)?;
                    let parent = output
                        .parent()
                        .filter(|p| !p.as_os_str().is_empty())
                        .unwrap_or(Path::new("."));
                    std::fs::File::open(parent)?.sync_all()?;
                    Ok(())
                })();
                if temporary.exists() {
                    let _ = std::fs::remove_file(&temporary);
                }
                result?;
                println!("{}", serde_json::json!({"backup":output}));
                return Ok(());
            }
            let text = response.text()?;
            ensure!(status.is_success(), "platform returned {status}: {text}");
            println!("{text}");
            Ok(())
        }
    }
}

fn operation(
    client: &reqwest::blocking::Client,
    base: &str,
    name: &str,
    operation: &str,
) -> Result<reqwest::blocking::RequestBuilder> {
    ensure!(
        crate::manifest::valid_name(name),
        "invalid application name"
    );
    Ok(client.post(format!("{base}/v1/apps/{name}/{operation}")))
}

fn init(directory: &Path, name: &str) -> Result<()> {
    ensure!(
        crate::manifest::valid_name(name),
        "invalid application name"
    );
    std::fs::create_dir_all(directory)?;
    let files: [(&str, &[u8]); 3] = [
        (
            "bic.json",
            include_bytes!("../../../examples/platform/counter/bic.json"),
        ),
        (
            "app.mjs",
            include_bytes!("../../../examples/platform/counter/app.mjs"),
        ),
        (
            "001-counter.sql",
            include_bytes!("../../../examples/platform/counter/001-counter.sql"),
        ),
    ];
    for (path, _) in &files {
        if directory.join(path).exists() {
            bail!("refusing to overwrite existing {path}");
        }
    }
    for (path, bytes) in files {
        let bytes = if path == "bic.json" {
            let mut value: serde_json::Value = serde_json::from_slice(bytes)?;
            value["name"] = serde_json::json!(name);
            serde_json::to_vec_pretty(&value)?
        } else {
            bytes.to_vec()
        };
        write_synced(&directory.join(path), &bytes)?;
    }
    println!(
        "Created {name}. Start `bic serve`, then deploy with `bic deploy {}`.",
        directory.join("bic.json").display()
    );
    Ok(())
}
