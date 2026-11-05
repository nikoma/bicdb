//! One server option resolver shared by startup and operator inspection.
use std::any::TypeId;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use bicdb_pgwire::{PgWireConfig, ServerDatabaseOptions};
use clap::{parser::ValueSource, Arg, ArgAction, ArgMatches, Command};
use clap::{CommandFactory, FromArgMatches};
use serde_json::{json, Value};

const MAX_CONFIG_BYTES: u64 = 1024 * 1024;
const CONTROLS: &[&str] = &[
    "config",
    "check_config",
    "print_effective_config",
    "help",
    "version",
];

pub(super) struct Loaded {
    pub matches: ArgMatches,
    pub effective: Option<Value>,
    pub engine: toml::Table,
}

/// Optional boolean values preserve bare flags while permitting explicit
/// `--require-auth=false` overrides of a file setting. Equals is mandatory
/// for the value so the following positional database path is never consumed.
fn command_schema(command: Command) -> Command {
    command.mut_subcommands(|subcommand| {
        if !matches!(subcommand.get_name(), "serve" | "serve-pg") {
            return subcommand;
        }
        subcommand.mut_args(|arg| {
            if matches!(arg.get_action(), ArgAction::SetTrue)
                && !CONTROLS.contains(&arg.get_id().as_str())
            {
                arg.action(ArgAction::Set)
                    .num_args(0..=1)
                    .require_equals(true)
                    .default_missing_value("true")
                    .default_value("false")
                    .value_parser(clap::value_parser!(bool))
            } else {
                arg
            }
        })
    })
}

fn key(arg: &Arg) -> String {
    arg.get_long()
        .unwrap_or(arg.get_id().as_str())
        .replace('-', "_")
}

fn error(command: &Command, message: impl Into<String>) -> clap::Error {
    command
        .clone()
        .error(clap::error::ErrorKind::ValueValidation, message.into())
}

fn numeric(arg: &Arg) -> bool {
    let id = arg.get_value_parser().type_id();
    [
        TypeId::of::<u16>(),
        TypeId::of::<u32>(),
        TypeId::of::<u64>(),
        TypeId::of::<usize>(),
    ]
    .iter()
    .any(|candidate| id == *candidate)
}

fn path_argument(arg: &Arg) -> bool {
    arg.get_value_parser().type_id() == TypeId::of::<PathBuf>()
}

fn boolean(arg: &Arg) -> bool {
    arg.get_value_parser().type_id() == TypeId::of::<bool>()
}

fn repeated(arg: &Arg) -> bool {
    matches!(arg.get_action(), ArgAction::Append)
}

fn file_scalar(value: &toml::Value, arg: &Arg, base: &Path) -> Result<OsString, String> {
    if boolean(arg) {
        return value
            .as_bool()
            .map(|v| v.to_string().into())
            .ok_or_else(|| "must be a boolean".into());
    }
    if numeric(arg) {
        return value
            .as_integer()
            .filter(|v| *v >= 0)
            .map(|v| v.to_string().into())
            .ok_or_else(|| "must be a nonnegative integer".into());
    }
    let text = value
        .as_str()
        .ok_or_else(|| "must be a string".to_string())?;
    if path_argument(arg) {
        let path = Path::new(text);
        return Ok(if path.is_absolute() {
            path.to_path_buf()
        } else {
            base.join(path)
        }
        .into_os_string());
    }
    Ok(text.into())
}

fn file_tokens(value: &toml::Value, arg: &Arg, base: &Path) -> Result<Vec<OsString>, String> {
    if repeated(arg) {
        value
            .as_array()
            .ok_or_else(|| "must be an array".to_string())?
            .iter()
            .map(|v| file_scalar(v, arg, base))
            .collect()
    } else {
        Ok(vec![file_scalar(value, arg, base)?])
    }
}

fn environment_tokens(value: &OsStr, arg: &Arg, cwd: &Path) -> Result<Vec<OsString>, String> {
    if path_argument(arg) && !repeated(arg) {
        let path = Path::new(value);
        return Ok(vec![if path.is_absolute() {
            path.to_path_buf()
        } else {
            cwd.join(path)
        }
        .into_os_string()]);
    }
    let text = value.to_str().ok_or_else(|| "must be UTF-8".to_string())?;
    if repeated(arg) {
        let values: Vec<String> = serde_json::from_str(text)
            .map_err(|_| "must be a JSON array of strings".to_string())?;
        return Ok(values.into_iter().map(OsString::from).collect());
    }
    if boolean(arg) && !matches!(text, "true" | "false") {
        return Err("must be true or false".into());
    }
    if numeric(arg) && text.parse::<u64>().is_err() {
        return Err("must be a nonnegative integer".into());
    }
    Ok(vec![value.to_os_string()])
}

fn read_file(path: &Path) -> Result<(PathBuf, toml::Table), String> {
    let path = std::fs::canonicalize(path)
        .map_err(|e| format!("cannot locate configuration {}: {e}", path.display()))?;
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW);
    }
    let file = options
        .open(&path)
        .map_err(|e| format!("cannot read configuration {}: {e}", path.display()))?;
    let metadata = file.metadata().map_err(|e| e.to_string())?;
    if !metadata.is_file() || metadata.len() > MAX_CONFIG_BYTES {
        return Err("configuration must be a regular file of at most 1 MiB".into());
    }
    let mut text = String::new();
    file.take(MAX_CONFIG_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(|_| "configuration must contain UTF-8 text".to_string())?;
    if text.len() as u64 > MAX_CONFIG_BYTES {
        return Err("configuration exceeds 1 MiB".into());
    }
    let document: toml::Table = toml::from_str(&text).map_err(|e: toml::de::Error| {
        // TOML's normal Display includes source lines. Never echo potentially
        // sensitive file contents in a validation error.
        let line = e.span().map_or(1, |span| {
            text[..span.start].bytes().filter(|b| *b == b'\n').count() + 1
        });
        format!("invalid TOML in {} at line {line}", path.display())
    })?;
    for name in document.keys() {
        if !matches!(name.as_str(), "version" | "server" | "engine") {
            return Err(format!("unknown configuration section {name}"));
        }
    }
    if document.get("version").and_then(toml::Value::as_integer) != Some(1) {
        return Err("configuration requires version = 1".into());
    }
    Ok((path, document))
}

fn os_value(value: &OsStr, arg: &Arg, cwd: &Path) -> Value {
    let absolute;
    let value = if path_argument(arg) && !Path::new(value).is_absolute() {
        absolute = cwd.join(value);
        absolute.as_os_str()
    } else {
        value
    };
    let Some(text) = value.to_str() else {
        return json!({"encoding":"os_bytes_hex","value":hex::encode(value.as_encoded_bytes())});
    };
    if boolean(arg) {
        return Value::Bool(text == "true");
    }
    if numeric(arg) {
        if let Ok(value) = text.parse::<u64>() {
            return json!(value);
        }
    }
    Value::String(text.into())
}

fn secret(arg: &Arg) -> bool {
    let name = key(arg);
    !path_argument(arg)
        && !name.ends_with("_env")
        && ["password", "secret", "token", "key"]
            .iter()
            .any(|part| name.contains(part))
}

/// Resolve file/environment settings to real CLI arguments. The final parser
/// performs the same enum, range, dependency and conflict checks as startup.
pub(super) fn load(
    command: Command,
    mut args: Vec<OsString>,
    environment: &BTreeMap<OsString, OsString>,
    cwd: &Path,
) -> Result<Loaded, clap::Error> {
    let mut command = command_schema(command);
    command.build();
    let preliminary = command
        .clone()
        .ignore_errors(true)
        .try_get_matches_from(args.clone())?;
    let server_name = preliminary
        .subcommand_name()
        .filter(|name| matches!(*name, "serve" | "serve-pg"));
    let explicit_file = preliminary.get_one::<PathBuf>("config").cloned();
    let inspection =
        preliminary.get_flag("check_config") || preliminary.get_flag("print_effective_config");
    let Some(server_name) = server_name else {
        if (explicit_file.is_some() || inspection)
            && preliminary.subcommand_name() != Some("config")
        {
            return Err(error(
                &command,
                "server configuration options require serve, serve-pg, or config",
            ));
        }
        return Ok(Loaded {
            matches: command.try_get_matches_from(args)?,
            effective: None,
            engine: toml::Table::new(),
        });
    };
    let spec = command
        .find_subcommand(server_name)
        .expect("parsed server command")
        .clone();
    let before = preliminary
        .subcommand_matches(server_name)
        .expect("server matches");
    let arguments: BTreeMap<String, Arg> = spec
        .get_arguments()
        .filter(|arg| !CONTROLS.contains(&arg.get_id().as_str()))
        .map(|arg| (key(arg), arg.clone()))
        .collect();
    let explicit: BTreeSet<String> = arguments
        .values()
        .filter(|arg| before.value_source(arg.get_id().as_str()) == Some(ValueSource::CommandLine))
        .map(|arg| key(arg))
        .collect();
    let selected_file = explicit_file.or_else(|| {
        environment
            .get(OsStr::new("BICDB_CONFIG"))
            .map(PathBuf::from)
    });
    let mut settings: BTreeMap<String, (Vec<OsString>, Value)> = BTreeMap::new();
    let mut engine = toml::Table::new();
    let mut config_file = None;
    if let Some(path) = selected_file {
        let path = if path.is_absolute() {
            path
        } else {
            cwd.join(path)
        };
        let (path, document) = read_file(&path).map_err(|e| error(&command, e))?;
        let server = document
            .get("server")
            .and_then(toml::Value::as_table)
            .ok_or_else(|| error(&command, "configuration requires a [server] table"))?;
        for (name, value) in server {
            let arg = arguments
                .get(name)
                .ok_or_else(|| error(&command, format!("unknown server setting {name}")))?;
            let tokens = file_tokens(value, arg, path.parent().expect("absolute file parent"))
                .map_err(|e| error(&command, format!("server.{name} {e}")))?;
            settings.insert(name.clone(), (tokens, json!({"kind":"file","file":path})));
        }
        if let Some(value) = document.get("engine") {
            engine = value
                .as_table()
                .ok_or_else(|| error(&command, "engine must be a table"))?
                .clone();
        }
        config_file = Some(path);
    }
    for (name, value) in environment {
        let Some(name) = name.to_str().and_then(|s| s.strip_prefix("BICDB_SERVER_")) else {
            continue;
        };
        let key = name.to_ascii_lowercase();
        let arg = arguments.get(&key).ok_or_else(|| {
            error(
                &command,
                format!("unknown server environment setting BICDB_SERVER_{name}"),
            )
        })?;
        let tokens = environment_tokens(value, arg, cwd)
            .map_err(|e| error(&command, format!("BICDB_SERVER_{name} {e}")))?;
        settings.insert(
            key,
            (
                tokens,
                json!({"kind":"environment","name":format!("BICDB_SERVER_{name}")}),
            ),
        );
    }
    let mut extra = Vec::new();
    for (name, (values, _)) in &settings {
        if explicit.contains(name) {
            continue;
        }
        let arg = &arguments[name];
        for value in values {
            if let Some(long) = arg.get_long() {
                let mut token = OsString::from(format!("--{long}="));
                token.push(value);
                extra.push(token);
            } else {
                extra.push(value.clone());
            }
        }
    }
    let position = args
        .iter()
        .position(|arg| arg == "--")
        .unwrap_or(args.len());
    args.splice(position..position, extra);
    let matches = command.try_get_matches_from(args)?;
    let resolved = matches
        .subcommand_matches(server_name)
        .expect("final server matches");
    let mut server = serde_json::Map::new();
    for (name, arg) in arguments {
        let values = resolved
            .get_raw(arg.get_id().as_str())
            .map(|values| {
                values
                    .map(|value| os_value(value, &arg, cwd))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let source = if explicit.contains(&name) {
            json!({"kind":"command_line"})
        } else {
            settings
                .get(&name)
                .map(|(_, source)| source.clone())
                .unwrap_or_else(|| json!({"kind":"default"}))
        };
        let redacted = secret(&arg);
        let value = if redacted && !values.is_empty() {
            json!("[REDACTED]")
        } else if repeated(&arg) {
            json!(values)
        } else {
            values.into_iter().next().unwrap_or(Value::Null)
        };
        server.insert(
            name,
            json!({"value":value,"source":source,"redacted":redacted}),
        );
    }
    Ok(Loaded {
        matches,
        effective: Some(
            json!({"format":1,"server":server,"config_file":config_file,"secret_values_compared":false}),
        ),
        engine,
    })
}

pub(super) struct Invocation {
    pub cli: super::Cli,
    pub engine: ServerDatabaseOptions,
    pub effective: Option<Value>,
}

fn engine_settings(
    table: &toml::Table,
    environment: &BTreeMap<OsString, OsString>,
) -> anyhow::Result<(ServerDatabaseOptions, Value)> {
    let defaults = ServerDatabaseOptions::default().effective_values();
    let schema = defaults.as_object().expect("engine option schema");
    let mut values = serde_json::Map::new();
    let mut origins = serde_json::Map::new();
    for (name, value) in table {
        let expected = schema
            .get(name)
            .with_context(|| format!("unknown engine setting {name}"))?;
        let value = if expected.is_boolean() {
            json!(value
                .as_bool()
                .with_context(|| format!("engine.{name} must be a boolean"))?)
        } else {
            json!(value
                .as_integer()
                .filter(|v| *v >= 0)
                .with_context(|| format!("engine.{name} must be a nonnegative integer"))?)
        };
        values.insert(name.clone(), value);
        origins.insert(name.clone(), json!({"kind":"file"}));
    }
    for (name, value) in environment {
        let Some(suffix) = name.to_str().and_then(|s| s.strip_prefix("BICDB_ENGINE_")) else {
            continue;
        };
        let key = suffix.to_ascii_lowercase();
        let expected = schema
            .get(&key)
            .with_context(|| format!("unknown engine environment setting BICDB_ENGINE_{suffix}"))?;
        let text = value
            .to_str()
            .with_context(|| format!("BICDB_ENGINE_{suffix} must be UTF-8"))?;
        let parsed = if expected.is_boolean() {
            json!(text
                .parse::<bool>()
                .map_err(|_| anyhow::anyhow!("BICDB_ENGINE_{suffix} must be true or false"))?)
        } else {
            json!(text.parse::<u64>().map_err(|_| anyhow::anyhow!(
                "BICDB_ENGINE_{suffix} must be a nonnegative integer"
            ))?)
        };
        values.insert(key.clone(), parsed);
        origins.insert(
            key,
            json!({"kind":"environment","name":format!("BICDB_ENGINE_{suffix}")}),
        );
    }
    let options: ServerDatabaseOptions = serde_json::from_value(Value::Object(values))?;
    options.validate()?;
    let mut effective = serde_json::Map::new();
    for (name, value) in options.effective_values().as_object().expect("engine map") {
        effective.insert(name.clone(),json!({"value":value,"source":origins.get(name).cloned().unwrap_or_else(||json!({"kind":"engine_default_or_legacy_environment"})),"redacted":false}));
    }
    Ok((options, Value::Object(effective)))
}

fn resolve(
    args: Vec<OsString>,
    environment: &BTreeMap<OsString, OsString>,
    cwd: &Path,
) -> Result<Invocation, clap::Error> {
    // The complete derived CLI schema is large. Construct and parse it on a
    // bounded stack rather than depending on the caller's thread stack (Rust
    // test threads and embedded callers can have substantially smaller stacks
    // than a process main thread).
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("bicdb-config".into())
            .stack_size(8 * 1024 * 1024)
            .spawn_scoped(scope, || resolve_inner(args, environment, cwd))
            .map_err(|error| clap::Error::raw(clap::error::ErrorKind::Io, error))?
            .join()
            .map_err(|_| {
                clap::Error::raw(
                    clap::error::ErrorKind::Io,
                    "configuration parser thread failed",
                )
            })?
    })
}

fn resolve_inner(
    args: Vec<OsString>,
    environment: &BTreeMap<OsString, OsString>,
    cwd: &Path,
) -> Result<Invocation, clap::Error> {
    let loaded = load(super::Cli::command(), args, environment, cwd)?;
    let cli = super::Cli::from_arg_matches(&loaded.matches)?;
    let Some(mut effective) = loaded.effective else {
        return Ok(Invocation {
            cli,
            engine: ServerDatabaseOptions::default(),
            effective: None,
        });
    };
    let (engine, engine_report) = engine_settings(&loaded.engine, environment)
        .map_err(|e| error(&super::Cli::command(), e.to_string()))?;
    effective["engine"] = engine_report;
    effective["scope"]=json!("server startup options and documented engine tuning; excludes SQL session changes, referenced file contents, secret contents and database-owned topology");
    let version = effective["server"]["postgres_server_version"]["value"]
        .as_str()
        .expect("server version");
    let derived = bicdb_pgwire::derive_postgres_server_version_num(version)
        .map_err(|e| error(&super::Cli::command(), e))?;
    if effective["server"]["postgres_server_version_num"]["value"].is_null() {
        effective["server"]["postgres_server_version_num"] = json!({"value":derived,"source":{"kind":"derived_default","from":"postgres_server_version"},"redacted":false});
    }
    if effective["server"]["channel_binding"]["value"].is_null() {
        let mut config = PgWireConfig::default();
        if !effective["server"]["tls_cert"]["value"].is_null() {
            config.tls_cert = Some(PathBuf::new());
        }
        let policy = match config.effective_channel_binding_policy() {
            bicdb_pgwire::ChannelBindingPolicy::Require => "require",
            bicdb_pgwire::ChannelBindingPolicy::Prefer => "prefer",
            bicdb_pgwire::ChannelBindingPolicy::Disable => "disable",
        };
        effective["server"]["channel_binding"] = json!({"value":policy,"source":{"kind":"derived_default","from":"tls_cert"},"redacted":false});
    }
    effective["engine"]["fsync"] = json!({"value":effective["server"]["storage_sync"]["value"]=="durable","source":{"kind":"derived","from":"server.storage_sync"},"redacted":false});
    effective["binary_version"] = json!(env!("CARGO_PKG_VERSION"));
    effective["validation"] = json!("not_performed");
    let values = configuration_values(&effective);
    use sha2::Digest;
    effective["fingerprint"] = json!(hex::encode(sha2::Sha256::digest(
        serde_json::to_vec(&values).expect("configuration values")
    )));
    Ok(Invocation {
        cli,
        engine,
        effective: Some(effective),
    })
}

pub(super) fn parse() -> Result<Invocation, clap::Error> {
    let environment = std::env::vars_os().collect();
    let cwd = std::env::current_dir().map_err(|e| error(&super::Cli::command(), e.to_string()))?;
    resolve(std::env::args_os().collect(), &environment, &cwd)
}

#[derive(Debug, clap::Args)]
pub(super) struct FileArgs {
    /// Defaults to BICDB_CONFIG, then ./bicdb.toml.
    #[arg(long, value_name = "FILE")]
    file: Option<PathBuf>,
}

#[derive(Debug, clap::Subcommand)]
pub(super) enum ConfigCommand {
    /// Validate parameters and references without opening a database or listening.
    Validate {
        #[command(flatten)]
        file: FileArgs,
        #[arg(long)]
        json: bool,
    },
    /// Show redacted desired startup settings, defaults and their origins.
    Show {
        #[command(flatten)]
        file: FileArgs,
        #[arg(long)]
        effective: bool,
    },
    /// Compare desired settings with a running server; never applies changes.
    Diff {
        #[command(flatten)]
        file: FileArgs,
        #[arg(long)]
        running: bool,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
}

#[derive(Debug, clap::Args)]
pub(super) struct ConnectionArgs {
    /// SQL endpoint HOST:PORT; defaults to the file's listener (wildcards use loopback).
    #[arg(long)]
    server: Option<String>,
    #[arg(long, default_value = "bicdb")]
    user: String,
    #[arg(long)]
    dbname: Option<String>,
    /// Environment variable containing the SQL login password.
    #[arg(long)]
    password_env: Option<String>,
    /// PEM trust roots; otherwise use the operating system trust store.
    #[arg(long)]
    tls_ca: Option<PathBuf>,
    /// TLS hostname for an IP/SSH-forwarded endpoint.
    #[arg(long)]
    server_name: Option<String>,
}

fn configuration_values(document: &Value) -> Value {
    let mut result = serde_json::Map::new();
    for section in ["server", "engine"] {
        let mut values = serde_json::Map::new();
        if let Some(settings) = document[section].as_object() {
            for (name, setting) in settings {
                values.insert(name.clone(), setting["value"].clone());
            }
        }
        result.insert(section.into(), Value::Object(values));
    }
    Value::Object(result)
}

fn from_file(file: FileArgs, global: Option<PathBuf>) -> anyhow::Result<Invocation> {
    let path = file
        .file
        .or(global)
        .or_else(|| std::env::var_os("BICDB_CONFIG").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("bicdb.toml"));
    resolve(
        vec![
            "bicdb".into(),
            "serve".into(),
            "--config".into(),
            path.into_os_string(),
        ],
        &std::env::vars_os().collect(),
        &std::env::current_dir()?,
    )
    .map_err(|e| anyhow::anyhow!(e.to_string()))
}

pub(super) fn run(command: ConfigCommand, global: Option<PathBuf>) -> anyhow::Result<()> {
    let outcome = (|| -> anyhow::Result<bool> {
        match command {
            ConfigCommand::Validate {
                file,
                json: json_output,
            } => {
                let invocation = from_file(file, global)?;
                let mut prepared = super::prepare_server_command(
                    invocation.cli.command.context("missing server command")?,
                )?;
                prepared.config.database_options = invocation.engine;
                prepared.config.validate()?;
                if json_output {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(
                            &json!({"valid":true,"config_file":invocation.effective.as_ref().and_then(|d|d.get("config_file")),"database_opened":false,"listeners_bound":false})
                        )?
                    );
                } else {
                    println!("Configuration is valid. No database opened or listener bound.");
                }
                Ok(false)
            }
            ConfigCommand::Show { file, effective } => {
                if !effective {
                    bail!("config show requires --effective");
                }
                let invocation = from_file(file, global)?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &invocation
                            .effective
                            .context("missing effective configuration")?
                    )?
                );
                Ok(false)
            }
            ConfigCommand::Diff {
                file,
                running,
                connection,
            } => {
                if !running {
                    bail!("config diff requires --running");
                }
                let desired = from_file(file, global)?
                    .effective
                    .context("missing effective configuration")?;
                let actual = running_configuration(connection, &desired)?;
                let report = diff(&desired, &actual)?;
                let changed = report["different"].as_bool() == Some(true);
                println!("{}", serde_json::to_string_pretty(&report)?);
                Ok(changed)
            }
        }
    })();
    match outcome {
        Ok(false) => Ok(()),
        Ok(true) => std::process::exit(1),
        Err(error) => {
            eprintln!("configuration: {error:#}");
            std::process::exit(2)
        }
    }
}

fn diff(desired: &Value, running: &Value) -> anyhow::Result<Value> {
    if running["format"] != 1 || !running["server"].is_object() || !running["engine"].is_object() {
        bail!("server returned an unsupported configuration snapshot");
    }
    let mut changes = Vec::new();
    let mut provenance = Vec::new();
    for section in ["server", "engine"] {
        let wanted = desired[section]
            .as_object()
            .context("desired settings missing")?;
        let active = running[section]
            .as_object()
            .context("running settings missing")?;
        let names: BTreeSet<_> = wanted.keys().chain(active.keys()).collect();
        for name in names {
            let a = active.get(name).unwrap_or(&Value::Null);
            let b = wanted.get(name).unwrap_or(&Value::Null);
            if a["value"] != b["value"] || a.is_null() != b.is_null() {
                changes.push(json!({"setting":format!("{section}.{name}"),"running":a,"desired":b,"restart_required":true}));
            } else if a["source"] != b["source"] {
                provenance.push(json!({"setting":format!("{section}.{name}"),"running_source":a["source"],"desired_source":b["source"]}));
            }
        }
    }
    Ok(
        json!({"different":!changes.is_empty(),"restart_required":!changes.is_empty(),"changes":changes,"provenance_changes":provenance,"running_fingerprint":running["fingerprint"],"desired_fingerprint":desired["fingerprint"],"secret_values_compared":false,"scope":desired["scope"],"applied":false}),
    )
}

fn running_configuration(args: ConnectionArgs, desired: &Value) -> anyhow::Result<Value> {
    let default_host = desired["server"]["host"]["value"]
        .as_str()
        .context("missing host")?;
    let default_host = match default_host {
        "0.0.0.0" | "::" => "127.0.0.1",
        host => host,
    };
    let default_port = desired["server"]["port"]["value"]
        .as_u64()
        .context("missing port")? as u16;
    let (host, port) = match args.server.as_deref() {
        Some(address) => {
            let (host, port) = address
                .rsplit_once(':')
                .context("--server requires HOST:PORT")?;
            (
                host.trim_start_matches('[')
                    .trim_end_matches(']')
                    .to_string(),
                port.parse::<u16>()?,
            )
        }
        None => (default_host.to_string(), default_port),
    };
    let local = host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    let tls = args.tls_ca.is_some()
        || args.server_name.is_some()
        || !local
        || desired["server"]["require_tls"]["value"] == true
        || !desired["server"]["tls_cert"]["value"].is_null();
    let password = args
        .password_env
        .as_ref()
        .map(|name| {
            std::env::var(name)
                .with_context(|| format!("password environment variable {name} is not set"))
        })
        .transpose()?;
    let mut config = tokio_postgres::Config::new();
    let database = args
        .dbname
        .as_deref()
        .or_else(|| desired["server"]["default_database"]["value"].as_str())
        .unwrap_or("bicdb");
    config
        .host(&host)
        .port(port)
        .user(&args.user)
        .dbname(database)
        .connect_timeout(std::time::Duration::from_secs(5));
    if let Some(password) = password {
        config.password(password);
    }
    if let Some(name) = args.server_name {
        let address = host
            .parse::<std::net::IpAddr>()
            .context("--server-name requires an IP endpoint in --server")?;
        config = tokio_postgres::Config::new();
        config
            .host(&name)
            .hostaddr(address)
            .port(port)
            .user(&args.user)
            .dbname(database)
            .connect_timeout(std::time::Duration::from_secs(5));
        if let Some(name) = &args.password_env {
            config.password(std::env::var(name)?);
        }
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let client=if tls {
            let mut roots=rustls::RootCertStore::empty();
            if let Some(path)=args.tls_ca {
                let mut reader=std::io::BufReader::new(std::fs::File::open(path)?);
                for cert in rustls_pemfile::certs(&mut reader) { roots.add(cert?)?; }
            } else {
                let certificates=rustls_native_certs::load_native_certs();
                for cert in certificates.certs { roots.add(cert)?; }
            }
            if roots.is_empty() { bail!("no TLS trust roots available; provide --tls-ca"); }
            let tls=rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
                .with_safe_default_protocol_versions()?.with_root_certificates(roots).with_no_client_auth();
            config.ssl_mode(tokio_postgres::config::SslMode::Require);
            let (client,connection)=tokio::time::timeout(std::time::Duration::from_secs(15),config.connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls))).await.context("SQL TLS connection/startup timed out")??;
            tokio::spawn(connection);
            client
        } else {
            config.ssl_mode(tokio_postgres::config::SslMode::Disable);
            let (client,connection)=tokio::time::timeout(std::time::Duration::from_secs(15),config.connect(tokio_postgres::NoTls)).await.context("SQL connection/startup timed out")??;
            tokio::spawn(connection);
            client
        };
        let messages=tokio::time::timeout(std::time::Duration::from_secs(15),client.simple_query("SELECT * FROM bicdb_server_configuration")).await??;
        let text=messages.iter().find_map(|message|match message {tokio_postgres::SimpleQueryMessage::Row(row)=>row.get(0),_=>None}).context("server returned no startup configuration; use a server version supporting configuration snapshots")?;
        Ok(serde_json::from_str(text)?)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(text: &str) -> (tempfile::TempDir, PathBuf) {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("bicdb.toml");
        std::fs::write(&file, text).unwrap();
        (root, file)
    }
    fn invoke(file: &Path, tail: &[&str], env: &[(&str, &str)]) -> Result<Invocation, clap::Error> {
        let mut args: Vec<OsString> = vec![
            "bicdb".into(),
            "serve".into(),
            "--config".into(),
            file.into(),
        ];
        args.extend(tail.iter().map(OsString::from));
        resolve(
            args,
            &env.iter()
                .map(|(k, v)| (OsString::from(k), OsString::from(v)))
                .collect(),
            file.parent().unwrap(),
        )
    }
    #[test]
    fn precedence_paths_booleans_and_list_replacement() {
        let (root,file)=fixture("version=1\n[server]\npath='data'\nport=4400\nrequire_auth=true\nredact_field=['one','two']\n[engine]\nquery_work_memory_bytes=1048576\n");
        let invocation = invoke(
            &file,
            &[
                "--port=4500",
                "--require-auth=false",
                "--redact-field=three",
            ],
            &[
                ("BICDB_SERVER_PORT", "4450"),
                ("BICDB_ENGINE_QUERY_WORK_MEMORY_BYTES", "2097152"),
            ],
        )
        .unwrap();
        let report = invocation.effective.unwrap();
        assert_eq!(report["server"]["port"]["value"], 4500);
        assert_eq!(report["server"]["require_auth"]["value"], false);
        assert_eq!(report["server"]["redact_field"]["value"], json!(["three"]));
        assert_eq!(
            report["server"]["path"]["value"],
            root.path().join("data").to_str().unwrap()
        );
        assert_eq!(
            report["engine"]["query_work_memory_bytes"]["value"],
            2097152
        );
        assert_eq!(report["server"]["port"]["source"]["kind"], "command_line");
        assert!(!root.path().join("data").exists());
        let report = invoke(
            &file,
            &["--require-auth"],
            &[("BICDB_SERVER_REDACT_FIELD", "[]")],
        )
        .unwrap()
        .effective
        .unwrap();
        assert_eq!(report["server"]["require_auth"]["value"], true);
        assert_eq!(report["server"]["redact_field"]["value"], json!([]));
    }
    #[test]
    fn unknown_keys_types_and_versions_fail_closed() {
        for text in [
            "version=2\n[server]\npath='db'",
            "version=1\n[server]\npath='db'\nprrt=123",
            "version=1\n[server]\npath='db'\nport='123'",
            "version=1\n[server]\npath='db'\nrequire_auth='true'",
            "version=1\n[server]\npath='db'\n[engine]\nwrong=1",
            "version=1\n[server]\npath='db'\n[engine]\nquery_merge_fan_in=1",
        ] {
            let (_root, file) = fixture(text);
            assert!(invoke(&file, &[], &[]).is_err(), "accepted {text}");
        }
        let (_root, file) = fixture("version=1\n[server]\npath='db'");
        assert!(invoke(&file, &[], &[("BICDB_SERVER_PRRT", "1")]).is_err());
        assert!(invoke(&file, &[], &[("BICDB_ENGINE_WRONG", "1")]).is_err());
    }
    #[test]
    fn syntax_errors_do_not_print_source_secrets_and_files_are_bounded() {
        let (_root, file) = fixture("version=1\n[server]\npassword='PRIVATE_SECRET' invalid");
        let error = invoke(&file, &[], &[]).err().unwrap().to_string();
        assert!(!error.contains("PRIVATE_SECRET"));
        std::fs::write(&file, vec![b' '; MAX_CONFIG_BYTES as usize + 1]).unwrap();
        assert!(invoke(&file, &[], &[]).is_err());
    }
    #[test]
    fn plain_startup_and_server_alias_keep_boolean_flags() {
        for command in ["serve", "serve-pg"] {
            let invocation = resolve(
                vec![
                    "bicdb".into(),
                    command.into(),
                    "db".into(),
                    "--require-auth".into(),
                ],
                &BTreeMap::new(),
                Path::new("/tmp"),
            )
            .unwrap();
            let report = invocation.effective.unwrap();
            assert_eq!(report["server"]["require_auth"]["value"], true);
            assert_eq!(report["server"]["path"]["value"], "/tmp/db");
        }
    }
    #[cfg(unix)]
    #[test]
    fn fifo_is_rejected_without_waiting_for_a_writer() {
        use std::os::unix::ffi::OsStrExt;
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("config.pipe");
        let name = std::ffi::CString::new(file.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        assert!(invoke(&file, &[], &[]).is_err());
    }

    #[test]
    fn environment_file_selection_and_literal_paths_are_not_shell_evaluated() {
        let (root, file) = fixture("version=1\n[server]\npath='$(never-execute)'\nport=5433\n");
        let environment = [
            (OsString::from("BICDB_CONFIG"), file.into_os_string()),
            (OsString::from("BICDB_SERVER_PORT"), OsString::from("5444")),
        ]
        .into_iter()
        .collect();
        let invocation = resolve(
            vec!["bicdb".into(), "serve".into()],
            &environment,
            root.path(),
        )
        .unwrap();
        let report = invocation.effective.unwrap();
        assert_eq!(report["server"]["port"]["value"], 5444);
        assert_eq!(
            report["server"]["path"]["value"],
            root.path().join("$(never-execute)").to_str().unwrap()
        );
        assert!(!root.path().join("$(never-execute)").exists());
    }

    #[test]
    fn diff_distinguishes_values_from_provenance() {
        let (_root, file) = fixture("version=1\n[server]\npath='db'\nport=5433");
        let running = invoke(&file, &[], &[]).unwrap().effective.unwrap();
        let same = invoke(&file, &["--port=5433"], &[])
            .unwrap()
            .effective
            .unwrap();
        let report = diff(&same, &running).unwrap();
        assert_eq!(report["different"], false);
        assert!(!report["provenance_changes"].as_array().unwrap().is_empty());
        let changed = invoke(&file, &["--port=5434"], &[])
            .unwrap()
            .effective
            .unwrap();
        let report = diff(&changed, &running).unwrap();
        assert_eq!(report["different"], true);
        assert_eq!(report["restart_required"], true);
        assert_eq!(report["applied"], false);
    }
}
