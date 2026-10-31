use super::*;

#[derive(Debug, clap::Args)]
pub(crate) struct BackupConnectionArgs {
    /// Local server address or SSH-forwarded SQL port. For remote TLS, use the
    /// same bicdb_backup_* functions through a TLS-capable PostgreSQL client.
    #[arg(long, default_value = "127.0.0.1:5433")]
    server: SocketAddr,
    #[arg(long, default_value = "bicdb")]
    user: String,
    #[arg(long, default_value = "bicdb")]
    dbname: String,
    #[arg(long)]
    password_env: Option<String>,
}

fn statement(function: &str, args: &[String]) -> String {
    format!(
        "SELECT {function}({})",
        args.iter()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub(crate) fn remote(
    args: BackupConnectionArgs,
    function: &str,
    values: Vec<String>,
    wait: bool,
) -> Result<()> {
    if !args.server.ip().is_loopback() {
        bail!("the backup CLI SQL connection requires a loopback/SSH-forwarded address; use a TLS-capable PostgreSQL client for remote bicdb_backup_* calls");
    }
    let password = args
        .password_env
        .as_ref()
        .map(|name| std::env::var(name).with_context(|| format!("{name} is not set")))
        .transpose()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let mut config = tokio_postgres::Config::new();
        config
            .host(&args.server.ip().to_string())
            .port(args.server.port())
            .user(&args.user)
            .dbname(&args.dbname)
            .connect_timeout(Duration::from_secs(5));
        if let Some(password) = password {
            config.password(password);
        }
        let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
        tokio::spawn(connection);
        let result = query(&client, &statement(function, &values)).await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        if wait {
            let id = result["id"]
                .as_str()
                .context("server did not return a backup job id")?
                .to_string();
            loop {
                let value =
                    query(&client, &statement("bicdb_backup_status", &[id.clone()])).await?;
                let job = value
                    .as_array()
                    .and_then(|v| v.first())
                    .context("backup job is no longer in server history")?;
                match job["phase"].as_str() {
                    Some("complete") => {
                        println!("{}", serde_json::to_string_pretty(job)?);
                        break;
                    }
                    Some("failed" | "cancelled") => bail!(
                        "backup {id} {}: {}",
                        job["phase"],
                        job["error"].as_str().unwrap_or("no details")
                    ),
                    _ => tokio::time::sleep(Duration::from_millis(250)).await,
                }
            }
        }
        Ok(())
    })
}

async fn query(client: &tokio_postgres::Client, sql: &str) -> Result<serde_json::Value> {
    let rows = tokio::time::timeout(Duration::from_secs(30), client.simple_query(sql)).await??;
    let value = rows
        .iter()
        .find_map(|row| match row {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
            _ => None,
        })
        .context("server did not return backup status")?;
    Ok(serde_json::from_str(value)?)
}

pub(crate) fn configure(config: &mut PgWireConfig, args: &OperatorArgs) -> Result<()> {
    config.online_backup_limits = args
        .backup_limits
        .as_ref()
        .map(|path| -> Result<_> { Ok(serde_json::from_slice(&std::fs::read(path)?)?) })
        .transpose()?
        .unwrap_or_default();
    config.online_backup_limits.validate()?;
    if let Some(directory) = &args.backup_repository {
        let environment = args
            .backup_key_env
            .as_ref()
            .context("--backup-key-env is required")?;
        let key =
            std::env::var(environment).with_context(|| format!("{environment} is not set"))?;
        if key.is_empty() {
            bail!("backup key environment variable is empty");
        }
        config.online_backups = Some(bicdb_pgwire::OnlineBackupConfig {
            directory: directory.clone(),
            key,
        });
    }
    Ok(())
}
