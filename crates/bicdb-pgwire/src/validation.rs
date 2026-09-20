//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn validate_username(username: &str) -> Result<()> {
    if username.is_empty()
        || !username
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(PgWireError::Server(format!(
            "invalid username `{username}`: use ASCII letters, digits, '-' or '_'"
        )));
    }
    Ok(())
}

// Every read/modify/write operation, including local operator tools, uses this
// cross-process lock. Readers see a complete old or new snapshot via rename.
pub(crate) fn lock_user_catalog(path: &Path) -> Result<File> {
    std::fs::create_dir_all(path)?;
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let lock = options.open(path.join(".server_users.lock"))?;
    lock.lock()?;
    Ok(lock)
}

// Persist the authority boundary independently of optional listener flags. The
// marker is monotonic and shared by every server using this auth directory.
const OPERATOR_ONLY_FILE: &str = ".server_operator_only";

pub(crate) fn operator_only_credentials(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path.join(OPERATOR_ONLY_FILE)) {
        Ok(_) => Ok(true), // Fail closed even for an unexpected file type.
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn enable_operator_only_credentials(path: &Path) -> Result<()> {
    let _lock = lock_user_catalog(path)?;
    if !operator_only_credentials(path)? {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        options.open(path.join(OPERATOR_ONLY_FILE))?.sync_all()?;
    }
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    Ok(())
}

fn operator_credentials_denied() -> PgWireError {
    SqlError::RaisedException {
        sqlstate: "42501".into(),
        message:
            "login credentials are host-managed; use the operator API or local bicdb user commands"
                .into(),
        detail: None,
    }
    .into()
}

// Called under the user-catalog lock at the actual SQL-origin mutation, not
// just during preflight: enabling the API may race an already-running query.
pub(crate) fn check_sql_credential_mutation(path: &Path) -> Result<()> {
    if operator_only_credentials(path)? {
        return Err(operator_credentials_denied());
    }
    Ok(())
}

pub(crate) fn load_user_catalog(path: &Path) -> Result<UserCatalog> {
    let path = path.join(USER_CATALOG_FILE);
    if !path.exists() {
        return Ok(UserCatalog::default());
    }
    let bytes = std::fs::read(path)?;
    if bytes.is_empty() {
        return Ok(UserCatalog::default());
    }
    serde_json::from_slice(&bytes).map_err(|error| PgWireError::Server(error.to_string()))
}

/// Persist the user catalog with owner-only permissions, atomically.
///
/// The file holds password-EQUIVALENT material: the Argon2id hash and the
/// SCRAM StoredKey/ServerKey, either of which lets an attacker impersonate
/// a user or mount an offline attack. A bare `fs::write` created it with
/// the process umask — world-readable 0644 on a default host — while every
/// other secret in the tree is 0600. It also rewrote in place, so a crash
/// mid-write could leave a truncated catalog and lock everyone out.
///
/// Written to a private temporary file (0600, `O_NOFOLLOW`) and renamed
/// over the target, which is atomic within a directory. An existing file's
/// mode is tightened on the way past.
pub(crate) fn persist_user_catalog(path: &Path, catalog: &UserCatalog) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(catalog)
        .map_err(|error| PgWireError::Server(error.to_string()))?;
    let target = path.join(USER_CATALOG_FILE);
    let temporary = path.join(format!(
        ".{USER_CATALOG_FILE}.{}.tmp",
        Uuid::new_v4().simple()
    ));
    {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        use std::io::Write as _;
        if let Err(error) = file.write_all(&bytes).and_then(|()| file.sync_all()) {
            let _ = std::fs::remove_file(&temporary);
            return Err(error.into());
        }
    }
    if let Err(error) = std::fs::rename(&temporary, &target) {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o600));
    }
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    Ok(())
}

pub(crate) fn verify_user_password(path: &Path, username: &str, password: &str) -> Result<bool> {
    let catalog = load_user_catalog(path)?;
    let (salt, expected, user_exists) = if let Some(user) =
        catalog.users.get(username).filter(|user| !user.disabled)
    {
        if user.algorithm != "argon2id" {
            return Err(PgWireError::Server(format!(
                "unsupported password hash algorithm {}",
                user.algorithm
            )));
        }
        (
            hex::decode(&user.salt_hex).map_err(|error| PgWireError::Server(error.to_string()))?,
            hex::decode(&user.hash_hex).map_err(|error| PgWireError::Server(error.to_string()))?,
            true,
        )
    } else {
        // Unknown users still pay the same Argon2id work as known users, so
        // authentication time does not disclose catalog membership.
        (vec![0x42; 16], vec![0; 32], false)
    };
    let actual = hash_password(password, &salt)?;
    Ok(user_exists && bool::from(expected.as_slice().ct_eq(actual.as_slice())))
}

pub(crate) fn user_record_digest(user: &StoredUser) -> Result<[u8; 32]> {
    let bytes = serde_json::to_vec(user).map_err(|_| PgWireError::Authentication)?;
    Ok(Sha256::digest(&bytes).into())
}

pub(crate) fn connection_security_context(
    server: &PgWireServer,
    username: &str,
    connection_id: u64,
    expected_record: Option<&[u8; 32]>,
) -> Result<Option<SecurityContext>> {
    let session_id = format!("pgwire:{connection_id}:{}", Uuid::new_v4());
    if !server.config.require_auth {
        return Ok(server.config.security_context.clone().map(|context| {
            let strength = context.authentication_strength;
            context.with_authenticated_session(session_id, strength)
        }));
    }

    let catalog = load_user_catalog(&server.auth_path)?;
    let stored = catalog
        .users
        .get(username)
        .filter(|user| !user.disabled)
        .ok_or(PgWireError::Authentication)?;
    if let Some(expected) = expected_record {
        if !bool::from(user_record_digest(stored)?.ct_eq(expected)) {
            return Err(PgWireError::Authentication);
        }
    }
    let identity = stored
        .security_identity
        .clone()
        .unwrap_or_else(|| PgWireUserIdentity::new(username, ""));
    let strength = match server.config.auth_method {
        AuthMethod::Cleartext => AuthenticationStrength::Password,
        AuthMethod::ScramSha256 => AuthenticationStrength::ScramSha256,
    };
    let mut context = SecurityContext::new(identity.user_id, identity.tenant_id)
        .with_roles(identity.roles)
        .with_scopes(identity.scopes)
        .with_authenticated_session(session_id, strength);
    if let Some(client_id) = identity.client_id {
        context = context.with_client_id(client_id);
    }
    if let Some(workspace_id) = identity.workspace_id {
        context = context.with_workspace_id(workspace_id);
    }
    Ok(Some(context))
}

#[derive(Clone, Debug)]
pub(crate) struct ScramVerifier {
    pub(crate) salt: Vec<u8>,
    pub(crate) stored_key: Vec<u8>,
    pub(crate) server_key: Vec<u8>,
    pub(crate) iterations: u32,
}

impl ScramVerifier {
    pub(crate) fn from_password(password: &str, salt: &[u8], iterations: u32) -> Result<Self> {
        let mut salted_password = [0_u8; 32];
        pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted_password);
        let client_key = hmac_sha256(&salted_password, b"Client Key")?;
        let stored_key = Sha256::digest(&client_key).to_vec();
        let server_key = hmac_sha256(&salted_password, b"Server Key")?;
        Ok(Self {
            salt: salt.to_vec(),
            stored_key,
            server_key,
            iterations,
        })
    }
}

pub(crate) fn scram_verifier_for_user(
    path: &Path,
    username: &str,
) -> Result<(ScramVerifier, bool)> {
    let catalog = load_user_catalog(path)?;
    let Some(user) = catalog.users.get(username).filter(|user| !user.disabled) else {
        return Ok((
            ScramVerifier {
                salt: vec![0x42; 16],
                stored_key: vec![0; 32],
                server_key: vec![0; 32],
                iterations: SCRAM_ITERATIONS,
            },
            false,
        ));
    };
    let salt_hex = user.scram_salt_hex.as_ref().ok_or_else(|| {
        PgWireError::Server("SCRAM verifier is missing; recreate the user".to_string())
    })?;
    let stored_key_hex = user.scram_stored_key_hex.as_ref().ok_or_else(|| {
        PgWireError::Server("SCRAM stored key is missing; recreate the user".to_string())
    })?;
    let server_key_hex = user.scram_server_key_hex.as_ref().ok_or_else(|| {
        PgWireError::Server("SCRAM server key is missing; recreate the user".to_string())
    })?;
    Ok((
        ScramVerifier {
            salt: hex::decode(salt_hex).map_err(|error| PgWireError::Server(error.to_string()))?,
            stored_key: hex::decode(stored_key_hex)
                .map_err(|error| PgWireError::Server(error.to_string()))?,
            server_key: hex::decode(server_key_hex)
                .map_err(|error| PgWireError::Server(error.to_string()))?,
            iterations: user.scram_iterations.unwrap_or(SCRAM_ITERATIONS),
        },
        true,
    ))
}

pub(crate) fn hmac_sha256(key: &[u8], message: &[u8]) -> Result<Vec<u8>> {
    let mut mac =
        HmacSha256::new_from_slice(key).map_err(|error| PgWireError::Server(error.to_string()))?;
    mac.update(message);
    Ok(mac.finalize().into_bytes().to_vec())
}

pub(crate) fn hash_password(password: &str, salt: &[u8]) -> Result<[u8; 32]> {
    let params = Params::new(19 * 1024, 2, 1, Some(32))
        .map_err(|error| PgWireError::Server(error.to_string()))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut hash = [0_u8; 32];
    argon2
        .hash_password_into(password.as_bytes(), salt, &mut hash)
        .map_err(|error| PgWireError::Server(error.to_string()))?;
    Ok(hash)
}

pub(crate) fn load_tls_config(config: &PgWireConfig) -> Result<Option<Arc<PgWireTlsConfig>>> {
    let (Some(cert_path), Some(key_path)) = (&config.tls_cert, &config.tls_key) else {
        if config.tls_cert.is_some() || config.tls_key.is_some() {
            return Err(PgWireError::Server(
                "both tls_cert and tls_key must be provided".to_string(),
            ));
        }
        return Ok(None);
    };

    let cert_file = std::fs::File::open(cert_path)?;
    let mut cert_reader = io::BufReader::new(cert_file);
    let certs = rustls_pemfile::certs(&mut cert_reader)
        .collect::<std::result::Result<Vec<CertificateDer<'static>>, _>>()?;
    if certs.is_empty() {
        return Err(PgWireError::Server(format!(
            "TLS certificate file {} contained no certificates",
            cert_path.display()
        )));
    }

    let key_file = std::fs::File::open(key_path)?;
    let mut key_reader = io::BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| PgWireError::Server("TLS key file contained no private key".to_string()))?;

    let channel_binding =
        tls_server_endpoint_binding(certs[0].as_ref()).map_err(|error| error.to_string());
    let config = ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|error| PgWireError::Server(error.to_string()))?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(|error| PgWireError::Server(error.to_string()))?;
    Ok(Some(Arc::new(PgWireTlsConfig {
        server: Arc::new(config),
        channel_binding,
    })))
}

pub(crate) fn is_local_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

pub(crate) fn enforce_connection_memory(
    state: &ConnectionState,
    config: &PgWireConfig,
) -> Result<()> {
    let estimated = state.memory_estimate();
    enforce_connection_memory_estimate(estimated, config)
}

pub(crate) fn enforce_persistent_connection_memory(
    state: &ConnectionState,
    config: &PgWireConfig,
) -> Result<()> {
    // The current frontend payload was already checked before execution and
    // is released as soon as this handler returns. The post-execution gate is
    // specifically for state that would remain while the connection is idle.
    let estimated = state
        .memory_estimate()
        .saturating_sub(state.active_request_bytes);
    enforce_connection_memory_estimate(estimated, config)
}

pub(crate) fn classify_memory_error_after_transaction_cleanup(
    state: &ConnectionState,
    config: &PgWireConfig,
    recoverable_error: PgWireError,
) -> PgWireError {
    match enforce_persistent_connection_memory(state, config) {
        Err(PgWireError::Server(message)) => PgWireError::PersistentConnectionMemoryLimit(message),
        Err(error) => PgWireError::PersistentConnectionMemoryLimit(error.to_string()),
        Ok(()) => recoverable_error,
    }
}

pub(crate) fn enforce_connection_memory_estimate(
    estimated: usize,
    config: &PgWireConfig,
) -> Result<()> {
    if estimated > config.per_connection_memory_limit {
        return Err(PgWireError::Server(format!(
            "connection memory estimate {estimated} exceeds limit {}",
            config.per_connection_memory_limit
        )));
    }
    Ok(())
}

/// Trimmed, `;`-stripped, whitespace-collapsed, ASCII-lowercased text in one
/// pass and one allocation (this runs on every query the server receives).
pub(crate) fn normalize_sql(sql: &str) -> String {
    let sql = sql.trim().trim_end_matches(';').trim_end();
    let mut normalized = String::with_capacity(sql.len());
    let mut pending_space = false;
    for ch in sql.chars() {
        if ch.is_whitespace() {
            pending_space = true;
            continue;
        }
        if pending_space {
            normalized.push(' ');
            pending_space = false;
        }
        normalized.push(ch.to_ascii_lowercase());
    }
    normalized
}

pub(crate) fn normalize_executable_sql(sql: &str) -> String {
    normalize_sql(&strip_sql_comments(sql))
}

pub(crate) fn unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[test]
    fn sql_credential_writer_rechecks_policy_after_preflight() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        create_user(path, "reader", "original-password").unwrap();
        // An already-running SQL statement passed preflight before activation.
        check_sql_credential_changes(path, "ALTER ROLE reader PASSWORD 'changed'").unwrap();
        enable_operator_only_credentials(path).unwrap();
        assert!(create_user_record(path, "reader", "changed", None, true).is_err());
        assert!(create_user_record(path, "new_reader", "changed", None, true).is_err());
        assert!(remove_user_record(path, "reader", true).is_err());
        assert!(verify_user_password(path, "reader", "original-password").unwrap());
        assert!(!user_exists(path, "new_reader").unwrap());
        // It is still valid to drop a SQL-only role, and local host tools work.
        remove_user_record(path, "sql_only", true).unwrap();
        create_user(path, "reader", "host-password").unwrap();
        assert!(verify_user_password(path, "reader", "host-password").unwrap());
        remove_user(path, "reader").unwrap();
    }

    #[derive(Debug)]
    struct TestPlacementPlanner;

    impl bicdb_core::ClusterPlacementPlanner for TestPlacementPlanner {
        fn plan_failure_repair(
            &self,
            topology: &bicdb_core::ClusterTopology,
            config: &bicdb_core::DistributionConfig,
            options: &bicdb_core::RebalanceOptions,
            now_ms: u64,
        ) -> bicdb_core::Result<bicdb_core::FailureRepairPlan> {
            bicdb_core::build_failure_repair_plan(topology, config, options, now_ms)
        }
    }

    fn automatic_test_supervisor() -> ClusterSupervisor {
        ClusterSupervisor::with_planner(
            ClusterSupervisorConfig {
                refresh_topology_each_tick: false,
                automatically_rebalance: true,
                retry_failed_relocations: true,
                ..ClusterSupervisorConfig::default()
            },
            Box::new(TestPlacementPlanner),
        )
        .unwrap()
    }

    #[derive(Debug)]
    struct RecordingHostService {
        starts: Arc<AtomicUsize>,
    }

    impl PgWireHostService for RecordingHostService {
        fn name(&self) -> &'static str {
            "recording-service"
        }

        fn start(&self, _context: PgWireHostContext) -> Result<()> {
            self.starts.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn pgwire_host_services_are_explicit_and_start_once() {
        let root = tempfile::tempdir().unwrap();
        let server = PgWireServer::open(
            root.path(),
            PgWireConfig {
                fsync: false,
                ..PgWireConfig::default()
            },
        )
        .unwrap();
        let starts = Arc::new(AtomicUsize::new(0));
        server
            .install_host_service(Arc::new(RecordingHostService {
                starts: Arc::clone(&starts),
            }))
            .unwrap();

        start_host_services(server.clone()).unwrap();
        assert_eq!(starts.load(AtomicOrdering::SeqCst), 1);
        assert!(server
            .install_host_service(Arc::new(RecordingHostService { starts }))
            .is_err());
    }

    #[test]
    fn postgres_compatibility_versions_are_numeric_and_coherent() {
        assert_eq!(
            derive_postgres_server_version_num("18.4").unwrap(),
            "180004"
        );
        assert_eq!(
            derive_postgres_server_version_num("16.0").unwrap(),
            "160000"
        );
        assert_eq!(
            derive_postgres_server_version_num("9.6.24").unwrap(),
            "90624"
        );
        for invalid in ["", "18.x", "18.4.1", "0.1", "18.100", " 18.4"] {
            assert!(
                derive_postgres_server_version_num(invalid).is_err(),
                "accepted invalid PostgreSQL version {invalid:?}"
            );
        }

        let mismatch = PgWireConfig {
            postgres_server_version: "16.7".to_string(),
            postgres_server_version_num: "180004".to_string(),
            ..PgWireConfig::default()
        };
        assert!(validate_server_config(&mismatch).is_err());
    }

    /// A bound parameter is DATA, not SQL.
    ///
    /// The int/bool arm passed client text through verbatim, so a driver
    /// binding `$1::int4` could inject statements into the query an
    /// application believed was parameterized — the exact guarantee
    /// prepared statements exist to provide.
    #[test]
    fn bound_scalar_parameters_can_never_carry_sql() {
        // Historical exploit: int4 payload closing the expression and
        // appending a statement.
        for (oid, payload) in [
            (23, "1); DROP TABLE victim; --"),
            (20, "1); DROP TABLE victim; --"),
            (21, "1); DROP TABLE victim; --"),
            (16, "true); DROP TABLE victim; --"),
            (23, "1 OR 1=1"),
            (16, "'; SELECT 1; --"),
        ] {
            let error = sql_literal_for_text_parameter(payload, oid)
                .expect_err("a non-numeric integer/boolean parameter must be refused");
            let rendered = format!("{error}");
            assert!(
                rendered.contains("invalid input syntax"),
                "unexpected error for oid {oid}: {rendered}"
            );
        }

        // Legitimate values still render as bare literals so typing and
        // comparisons behave exactly as before.
        assert_eq!(sql_literal_for_text_parameter("42", 23).unwrap(), "42");
        assert_eq!(sql_literal_for_text_parameter("-7", 20).unwrap(), "-7");
        assert_eq!(sql_literal_for_text_parameter(" 5 ", 21).unwrap(), "5");
        assert_eq!(sql_literal_for_text_parameter("true", 16).unwrap(), "true");
        assert_eq!(sql_literal_for_text_parameter("f", 16).unwrap(), "false");
        assert_eq!(sql_literal_for_text_parameter("1", 16).unwrap(), "true");

        // Declared width is enforced, as PostgreSQL does.
        assert!(sql_literal_for_text_parameter("99999", 21).is_err());
        assert!(sql_literal_for_text_parameter("3000000000", 23).is_err());

        // Text-ish and unknown-OID parameters are quoted, so even a payload
        // full of quotes and semicolons stays a single inert literal.
        let quoted = sql_literal_for_text_parameter("'; DROP TABLE victim; --", 25).unwrap();
        assert!(
            quoted.starts_with('\'') && quoted.ends_with('\''),
            "text parameter must be a quoted literal: {quoted}"
        );
        // The payload's own quote must be doubled, so it closes nothing:
        // the whole payload stays one string literal.
        assert!(
            quoted.contains("''"),
            "embedded quote must be escaped: {quoted}"
        );
        assert_eq!(
            quoted, "'''; DROP TABLE victim; --'",
            "text parameters must render as one inert literal"
        );

        // Array elements go through the same gate, one element at a time.
        let error = sql_literal_for_text_parameter("{1,2); DROP TABLE victim; --}", 1007)
            .expect_err("array elements must be validated too");
        assert!(format!("{error}").contains("invalid input syntax"));
        assert_eq!(
            sql_literal_for_text_parameter("{1,2,3}", 1007).unwrap(),
            "ARRAY[1, 2, 3]"
        );
    }

    /// Render the visibility-filtered view the way the dispatcher does,
    /// so the test exercises the real rule rather than a copy of it.
    fn render_connection_rows(
        connections: &[ServerConnectionSnapshot],
        caller: Option<VirtualQueryCaller<'_>>,
        privileged: bool,
    ) -> String {
        connections
            .iter()
            .cloned()
            .map(|connection| {
                let connection = redact_connection_for_caller(connection, caller, privileged);
                format!("{connection:?}")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Other sessions' in-flight SQL and peer addresses are not public.
    /// Query text routinely embeds literals — patient identifiers, tokens,
    /// another tenant's keys — so an unprivileged session reading
    /// `bicdb_server_connections` was a cross-session disclosure channel.
    #[test]
    fn connection_view_hides_other_sessions_from_unprivileged_callers() {
        let snapshot = |connection_id: u64, user: &str| ServerConnectionSnapshot {
            connection_id,
            user: user.to_string(),
            peer_addr: Some(format!("10.0.0.{connection_id}:5432")),
            connected_at: 0,
            last_query_at: None,
            queries_executed: 0,
            failed_queries: 0,
            in_transaction: false,
            active_query: Some(format!(
                "SELECT * FROM patients WHERE ssn = 'secret{connection_id}'"
            )),
        };
        let connections = vec![snapshot(1, "tenant_a"), snapshot(2, "tenant_b")];

        // An unprivileged caller sees its own row intact and the other
        // session stripped of both sensitive columns.
        let caller = VirtualQueryCaller {
            connection_id: 1,
            user: "tenant_a",
        };
        let rendered = render_connection_rows(&connections, Some(caller), false);
        assert!(
            rendered.contains("secret1"),
            "a session must still see its own query: {rendered}"
        );
        assert!(
            !rendered.contains("secret2"),
            "another session's query text leaked: {rendered}"
        );
        assert!(
            !rendered.contains("10.0.0.2"),
            "another session's peer address leaked: {rendered}"
        );
        // The row itself remains visible: the view still reports who is
        // connected, just not what they are running.
        assert!(rendered.contains("tenant_b"), "row vanished: {rendered}");

        // An administrator sees everything, as PostgreSQL allows for
        // superusers and pg_read_all_stats members.
        let rendered = render_connection_rows(&connections, Some(caller), true);
        assert!(
            rendered.contains("secret2"),
            "admin must see all: {rendered}"
        );
    }

    /// `ALTER ROLE ... PASSWORD` reaches `create_user` for an existing login.
    /// Rewriting the verifiers must keep the trusted identity that
    /// `bind-identity` attached; it is not part of the password.
    #[test]
    fn password_rewrite_keeps_the_bound_security_identity() {
        let directory = tempfile::tempdir().unwrap();
        create_user(directory.path(), "carrier_app", "first").unwrap();
        set_user_security_identity(
            directory.path(),
            "carrier_app",
            PgWireUserIdentity::new("1", "walknorth").with_roles(["org_admin"]),
        )
        .unwrap();
        create_user(directory.path(), "carrier_app", "second").unwrap();

        let catalog = load_user_catalog(directory.path()).unwrap();
        let user = &catalog.users["carrier_app"];
        let identity = user.security_identity.as_ref().expect("identity survives");
        assert_eq!(identity.user_id, "1");
        assert_eq!(identity.tenant_id, "walknorth");
    }

    /// The user catalog holds password-equivalent material: the Argon2id
    /// hash and the SCRAM StoredKey/ServerKey. A bare `fs::write` created
    /// it 0644 under a default umask, so any local user could read the
    /// verifiers and attack them offline — while every other secret file in
    /// the tree is 0600.
    #[cfg(unix)]
    #[test]
    fn user_catalog_is_written_owner_only_and_atomically() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let mut catalog = UserCatalog::default();
        catalog.users.insert(
            "alice".to_string(),
            StoredUser {
                username: "alice".to_string(),
                salt_hex: "00".to_string(),
                hash_hex: "11".to_string(),
                disabled: false,
                algorithm: "argon2id".to_string(),
                scram_salt_hex: None,
                scram_stored_key_hex: None,
                scram_server_key_hex: None,
                scram_iterations: None,
                security_identity: None,
            },
        );
        persist_user_catalog(directory.path(), &catalog).unwrap();

        let target = directory.path().join(USER_CATALOG_FILE);
        let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "credential catalog is readable by others");

        // Rewriting keeps the mode and leaves no temporary behind.
        persist_user_catalog(directory.path(), &catalog).unwrap();
        let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let leftovers = std::fs::read_dir(directory.path())
            .unwrap()
            .flatten()
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
            .count();
        assert_eq!(leftovers, 0, "a temporary catalog file was left behind");

        // The catalog still round-trips.
        let loaded = load_user_catalog(directory.path()).unwrap();
        assert!(loaded.users.contains_key("alice"));
    }

    fn automatic_checkpoint_test_config() -> DbConfig {
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_page_size(512)
            .with_paged_buffer_pool_bytes(512 * 256)
            .with_paged_wal_max_bytes(512 * 1_024)
    }

    fn automatic_checkpoint_test_limits() -> PagedCheckpointScheduleLimits {
        PagedCheckpointScheduleLimits {
            step_interval_ms: 1,
            saturation_retry_ms: 1,
            failure_retry_base_ms: 1,
            failure_retry_max_ms: 10,
            ..PagedCheckpointScheduleLimits::default()
        }
    }

    fn write_checkpoint_test_rows(db: &mut BicDb, count: usize) {
        db.create_collection("automatic_checkpoint_rows").unwrap();
        db.batch_insert(
            "automatic_checkpoint_rows",
            (0..count).map(|index| {
                bicdb_core::Record::new(format!("row-{index:05}"))
                    .with_metadata(json!({ "payload": "x".repeat(400) }))
            }),
        )
        .unwrap();
    }

    fn checkpoint_handle(db: &BicDb) -> bicdb_core::PagedCheckpointMaintenanceHandle {
        db.paged_checkpoint_maintenance_handle()
            .expect("test database is server_paged")
    }

    #[test]
    fn automatic_checkpoint_driver_resumes_the_same_operation_after_reopen() {
        let root = tempfile::tempdir().unwrap();
        let config = automatic_checkpoint_test_config();
        let operation_id = {
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            write_checkpoint_test_rows(&mut db, 64);
            let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
            let mut driver = AutomaticPagedCheckpointDriver::new(
                100,
                Duration::from_millis(10),
                Duration::from_millis(1),
                automatic_checkpoint_test_limits(),
            )
            .unwrap();
            assert!(matches!(
                driver
                    .tick(&checkpoint_handle(&db), &governor, 100)
                    .unwrap(),
                AutomaticPagedCheckpointTick::Idle
            ));
            assert!(matches!(
                driver
                    .tick(&checkpoint_handle(&db), &governor, 110)
                    .unwrap(),
                AutomaticPagedCheckpointTick::Advanced(_)
            ));
            let schedule = db.paged_checkpoint_maintenance_status().unwrap().unwrap();
            assert!(!schedule.completed);
            schedule.operation_id
        };

        let db = BicDb::open_with_config(root.path(), config).unwrap();
        let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 200).unwrap();
        let mut driver = AutomaticPagedCheckpointDriver::new(
            200,
            Duration::from_millis(10),
            Duration::from_millis(1),
            automatic_checkpoint_test_limits(),
        )
        .unwrap();
        let mut now_ms = 200;
        loop {
            let _ = driver
                .tick(&checkpoint_handle(&db), &governor, now_ms)
                .unwrap();
            let schedule = db.paged_checkpoint_maintenance_status().unwrap().unwrap();
            assert_eq!(schedule.operation_id, operation_id);
            if schedule.completed {
                assert_eq!(schedule.totals.checkpoints_completed, 1);
                break;
            }
            now_ms = now_ms.saturating_add(1);
            assert!(now_ms < 1_000, "checkpoint did not finish after restart");
        }
        assert!(db
            .get("automatic_checkpoint_rows", "row-00063")
            .unwrap()
            .is_some());
        assert_eq!(db.paged_storage_snapshot().unwrap().unwrap().wal_bytes, 0);
    }

    #[test]
    fn automatic_checkpoint_driver_starts_immediately_under_wal_pressure() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            // Over the checkpoint threshold, but under the hard cap at 4x it
            // where commit() drains the WAL itself. At the old 512 the writes
            // below tripped that backpressure and left wal_bytes at 0, so the
            // pressure this test exists to create never survived to be seen.
            automatic_checkpoint_test_config().with_paged_wal_max_bytes(32_768),
        )
        .unwrap();
        write_checkpoint_test_rows(&mut db, 64);
        let snapshot = db.paged_storage_snapshot().unwrap().unwrap();
        assert!(
            snapshot.wal_bytes > snapshot.wal_max_bytes,
            "wal {} must exceed the threshold {}",
            snapshot.wal_bytes,
            snapshot.wal_max_bytes
        );

        let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
        let mut driver = AutomaticPagedCheckpointDriver::new(
            100,
            Duration::from_secs(60),
            Duration::from_millis(1),
            automatic_checkpoint_test_limits(),
        )
        .unwrap();
        assert!(matches!(
            driver
                .tick(&checkpoint_handle(&db), &governor, 100)
                .unwrap(),
            AutomaticPagedCheckpointTick::Advanced(_)
        ));
        assert!(db
            .paged_checkpoint_maintenance_status()
            .unwrap()
            .is_some_and(|schedule| !schedule.completed));
    }

    #[test]
    fn automatic_checkpoint_driver_never_overrides_an_operator_pause() {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), automatic_checkpoint_test_config()).unwrap();
        write_checkpoint_test_rows(&mut db, 4);
        let limits = automatic_checkpoint_test_limits();
        let schedule = db.start_paged_checkpoint_maintenance(100, limits).unwrap();
        let paused = db
            .pause_paged_checkpoint_maintenance(schedule.operation_id, "operator hold", 101)
            .unwrap();
        let sequence = paused.state_sequence;
        let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 101).unwrap();
        let mut driver = AutomaticPagedCheckpointDriver::new(
            101,
            Duration::from_millis(1),
            Duration::from_millis(1),
            limits,
        )
        .unwrap();
        assert!(matches!(
            driver.tick(&checkpoint_handle(&db), &governor, 102).unwrap(),
            AutomaticPagedCheckpointTick::Paused { operation_id }
                if operation_id == schedule.operation_id
        ));
        let after = db.paged_checkpoint_maintenance_status().unwrap().unwrap();
        assert_eq!(after.state_sequence, sequence);
        assert_eq!(after.paused_reason.as_deref(), Some("operator hold"));
    }

    #[test]
    fn automatic_checkpoint_driver_rejects_an_impossible_restored_demand() {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), automatic_checkpoint_test_config()).unwrap();
        write_checkpoint_test_rows(&mut db, 4);
        let limits = automatic_checkpoint_test_limits();
        let schedule = db.start_paged_checkpoint_maintenance(100, limits).unwrap();

        let mut governor_config = ResourceGovernorConfig::default();
        governor_config
            .lanes
            .get_mut(&ResourceLane::Compaction)
            .unwrap()
            .capacity
            .memory_bytes = 1;
        let governor = ResourceGovernor::new(governor_config, 100).unwrap();
        let mut driver = AutomaticPagedCheckpointDriver::new(
            100,
            Duration::from_millis(1),
            Duration::from_millis(1),
            limits,
        )
        .unwrap();
        assert!(driver
            .tick(&checkpoint_handle(&db), &governor, 100)
            .unwrap_err()
            .to_string()
            .contains("hard bound"));
        let after = db.paged_checkpoint_maintenance_status().unwrap().unwrap();
        assert_eq!(after.operation_id, schedule.operation_id);
        assert_eq!(after.state_sequence, schedule.state_sequence);
    }

    #[test]
    fn automatic_checkpoint_driver_is_a_noop_for_embedded_storage() {
        let root = tempfile::tempdir().unwrap();
        let db = BicDb::open(root.path()).unwrap();
        let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
        let mut driver = AutomaticPagedCheckpointDriver::new(
            100,
            Duration::from_millis(1),
            Duration::from_millis(1),
            automatic_checkpoint_test_limits(),
        )
        .unwrap();
        // Not server_paged: no handle exists, so the worker exits before its
        // first tick and never creates a schedule.
        let _ = driver;
        let _ = governor;
        assert!(db.paged_checkpoint_maintenance_handle().is_none());
        assert!(!root
            .path()
            .join(bicdb_core::DEFAULT_PAGED_CHECKPOINT_SCHEDULE)
            .exists());
    }

    #[test]
    fn automatic_checkpoint_configuration_fails_before_server_open() {
        let mut zero_poll = PgWireConfig::default();
        zero_poll.automatic_paged_checkpoint_poll_interval = Duration::ZERO;
        assert!(validate_server_config(&zero_poll).is_err());

        let mut underdeclared_lane = PgWireConfig::default();
        let compaction = underdeclared_lane
            .resource_governor
            .lanes
            .get_mut(&ResourceLane::Compaction)
            .unwrap();
        compaction.capacity.memory_bytes = 1;
        assert!(validate_server_config(&underdeclared_lane).is_err());

        let mut zero_read_ahead_poll = PgWireConfig::default();
        zero_read_ahead_poll.automatic_paged_read_ahead_poll_interval = Duration::ZERO;
        assert!(validate_server_config(&zero_read_ahead_poll).is_err());

        let mut underdeclared_read_ahead = PgWireConfig::default();
        underdeclared_read_ahead
            .automatic_paged_read_ahead_demand
            .io_charge_bytes = underdeclared_read_ahead
            .automatic_paged_read_ahead_limits
            .max_io_bytes
            - 1;
        assert!(validate_server_config(&underdeclared_read_ahead).is_err());
    }

    #[test]
    fn cluster_databases_share_one_node_resource_governor() {
        let root = tempfile::tempdir().unwrap();
        let cluster = PgWireCluster::open(
            root.path(),
            "primary",
            PgWireConfig {
                fsync: false,
                ..PgWireConfig::default()
            },
        )
        .unwrap();
        let primary = cluster.default_server().unwrap();

        let directory = Uuid::new_v4().to_string();
        fs::create_dir_all(root.path().join("databases").join(&directory)).unwrap();
        cluster.manifest.lock().unwrap().databases.insert(
            "secondary".to_string(),
            ClusterDatabaseEntry {
                directory,
                owner: "bicdb".to_string(),
            },
        );
        let secondary = cluster.server_for_database("secondary").unwrap();

        let demand = automatic_checkpoint_test_limits().demand;
        let permit = primary
            .resource_governor
            .try_admit(ResourceLane::Compaction, demand, cluster_now_ms())
            .unwrap();
        let primary_snapshot = primary.resource_governor.snapshot();
        assert_eq!(primary_snapshot.background.active, 1);
        assert_eq!(secondary.resource_governor.snapshot(), primary_snapshot);
        assert_eq!(cluster.resource_governor.snapshot(), primary_snapshot);
        drop(permit);
        assert_eq!(secondary.resource_governor.snapshot().background.active, 0);
    }

    #[test]
    fn serve_worker_drives_periodic_checkpoint_to_durable_completion() {
        let root = tempfile::tempdir().unwrap();
        let mut db =
            BicDb::open_with_config(root.path(), automatic_checkpoint_test_config()).unwrap();
        write_checkpoint_test_rows(&mut db, 32);
        let shared = Arc::new(RwLock::new(db));
        let server = PgWireServer::open_with_shared(
            root.path(),
            PgWireConfig {
                fsync: false,
                checkpoint_interval: Duration::from_millis(1),
                automatic_paged_checkpoint_poll_interval: Duration::from_millis(1),
                automatic_paged_checkpoint_limits: automatic_checkpoint_test_limits(),
                ..PgWireConfig::default()
            },
            shared,
        )
        .unwrap();
        let worker = start_automatic_paged_checkpoint_worker(server.clone());
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if server
                .db
                .read()
                .paged_checkpoint_maintenance_status()
                .unwrap()
                .is_some_and(|schedule| schedule.completed)
            {
                break;
            }
            assert!(Instant::now() < deadline, "serve worker did not complete");
            thread::sleep(Duration::from_millis(1));
        }
        server.request_shutdown();
        worker.join().unwrap();
        assert!(server.last_checkpoint.lock().unwrap().is_some());
        assert_eq!(
            server
                .db
                .read()
                .paged_storage_snapshot()
                .unwrap()
                .unwrap()
                .wal_bytes,
            0
        );
    }

    #[test]
    fn automatic_checkpoint_worker_interrupts_maximum_poll_sleep_on_shutdown() {
        let root = tempfile::tempdir().unwrap();
        let shared = Arc::new(RwLock::new(BicDb::open(root.path()).unwrap()));
        let server = PgWireServer::open_with_shared(
            root.path(),
            PgWireConfig {
                fsync: false,
                automatic_paged_checkpoint_poll_interval: MAX_BACKGROUND_TASK_POLL_INTERVAL,
                ..PgWireConfig::default()
            },
            shared,
        )
        .unwrap();
        let worker = start_automatic_paged_checkpoint_worker(server.clone());

        // The embedded database produces a no-op tick and then enters the
        // maximum permitted host poll wait.
        thread::sleep(Duration::from_millis(25));
        let shutdown_started = Instant::now();
        server.request_shutdown();
        let (joined_tx, joined_rx) = std::sync::mpsc::sync_channel(1);
        thread::spawn(move || {
            worker.join().unwrap();
            let _ = joined_tx.send(());
        });
        joined_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("checkpoint worker outlived bounded shutdown wait");
        assert!(shutdown_started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn automatic_read_ahead_worker_drains_exact_query_hints_under_governance() {
        let root = tempfile::tempdir().unwrap();
        let db_config = automatic_checkpoint_test_config()
            .with_paged_buffer_pool_bytes(64 * 512)
            .with_paged_read_ahead_queue_pages(16);
        {
            let mut db = BicDb::open_with_config(root.path(), db_config.clone()).unwrap();
            write_checkpoint_test_rows(&mut db, 400);
            db.close().unwrap();
        }
        let db = BicDb::open_with_config(root.path(), db_config).unwrap();
        // The pool only ACCEPTS speculative requests while a driver is
        // registered (a live PagedReadAheadHandle) — exactly the contract
        // the production worker follows by holding its handle for its whole
        // life. Hold one across the scan, as the worker would.
        let driver = db.paged_read_ahead_handle().unwrap();
        assert!(db
            .for_each_record_batch("automatic_checkpoint_rows", 1, |_batch| Ok(false))
            .unwrap());
        assert!(
            db.paged_read_ahead_queue_depth().unwrap() > 0,
            "an early-stopped range scan did not leave its exact next leaf queued"
        );
        drop(driver);

        let shared = Arc::new(RwLock::new(db));
        let server = PgWireServer::open_with_shared(
            root.path(),
            PgWireConfig {
                fsync: false,
                automatic_paged_checkpoint: false,
                automatic_paged_read_ahead_poll_interval: Duration::from_millis(1),
                automatic_paged_read_ahead_limits: ReadAheadLimits {
                    max_candidates: 1,
                    max_io_bytes: 512,
                    max_duration_millis: 1_000,
                },
                automatic_paged_read_ahead_demand: ResourceDemand {
                    memory_bytes: 64 * 1024,
                    io_bytes: 512,
                    cpu_slots: 1,
                    io_charge_bytes: 512,
                },
                ..PgWireConfig::default()
            },
            shared,
        )
        .unwrap();
        if server.db.read().paged_read_ahead_queue_depth().unwrap_or(0) == 0 {
            server
                .db
                .read()
                .for_each_record_batch("automatic_checkpoint_rows", 1, |_batch| Ok(false))
                .unwrap();
        }
        assert!(server.db.read().paged_read_ahead_queue_depth().unwrap() > 0);
        let worker = start_automatic_paged_read_ahead_worker(server.clone());
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let snapshot = server.db.read().paged_storage_snapshot().unwrap().unwrap();
            if snapshot.buffer_pool.read_ahead_queue_depth == 0
                && snapshot.buffer_pool.read_ahead_steps != 0
            {
                assert!(snapshot.buffer_pool.read_ahead_pages_loaded > 0);
                break;
            }
            assert!(Instant::now() < deadline, "read-ahead worker did not drain");
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(server.resource_governor.snapshot().background.active, 0);
        server.request_shutdown();
        worker.join().unwrap();
    }

    #[test]
    fn automatic_read_ahead_worker_interrupts_maximum_poll_sleep_on_shutdown() {
        let root = tempfile::tempdir().unwrap();
        let shared = Arc::new(RwLock::new(
            BicDb::open_with_config(root.path(), automatic_checkpoint_test_config()).unwrap(),
        ));
        let server = PgWireServer::open_with_shared(
            root.path(),
            PgWireConfig {
                fsync: false,
                automatic_paged_read_ahead_poll_interval: MAX_BACKGROUND_TASK_POLL_INTERVAL,
                ..PgWireConfig::default()
            },
            shared,
        )
        .unwrap();
        let worker = start_automatic_paged_read_ahead_worker(server.clone());
        thread::sleep(Duration::from_millis(25));
        let shutdown_started = Instant::now();
        server.request_shutdown();
        worker.join().unwrap();
        assert!(shutdown_started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn automatic_read_ahead_rejects_limits_below_the_durable_page_size() {
        let root = tempfile::tempdir().unwrap();
        let db = BicDb::open_with_config(
            root.path(),
            automatic_checkpoint_test_config().with_paged_page_size(4 * 1024),
        )
        .unwrap();
        let shared = Arc::new(RwLock::new(db));
        let error = PgWireServer::open_with_shared(
            root.path(),
            PgWireConfig {
                fsync: false,
                automatic_paged_checkpoint: false,
                automatic_paged_read_ahead_limits: ReadAheadLimits {
                    max_candidates: 1,
                    max_io_bytes: bicdb_core::MIN_PAGE_SIZE as u64,
                    max_duration_millis: 1_000,
                },
                automatic_paged_read_ahead_demand: ResourceDemand {
                    memory_bytes: 64 * 1024,
                    io_bytes: bicdb_core::MIN_PAGE_SIZE as u64,
                    cpu_slots: 1,
                    io_charge_bytes: bicdb_core::MIN_PAGE_SIZE as u64,
                },
                ..PgWireConfig::default()
            },
            shared,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("invalid for the active page size"));
    }

    #[test]
    fn server_auto_installs_a_checked_distributed_router() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open_with_config(
                dir.path(),
                DbConfig::default()
                    .with_fsync(false)
                    .with_storage_mode(StorageMode::ServerPaged),
            )
            .unwrap();
            db.create_collection("documents").unwrap();
            db.close().unwrap();
        }
        let distribution = bicdb_core::DistributionConfig {
            enabled: true,
            cluster_id: bicdb_core::ClusterId::new("router-cluster").unwrap(),
            node_id: bicdb_core::ClusterNodeId::new("server-1").unwrap(),
            node_address: "127.0.0.1:9444".to_string(),
            node_capacity_bytes: 1_000_000,
            replication_factor: 1,
            initial_ranges: 4,
            ..bicdb_core::DistributionConfig::default()
        };
        let store =
            DistributionStore::initialize_at(dir.path(), distribution, false, 1_000).unwrap();
        let generation = store.topology().generation;
        drop(store);

        let server = PgWireServer::open(
            dir.path(),
            PgWireConfig {
                fsync: false,
                ..PgWireConfig::default()
            },
        )
        .unwrap();
        let router = server
            .distribution_router()
            .expect("distribution config should install a router");
        assert_eq!(router.topology_generation().unwrap(), generation);
        assert_eq!(router.topology().unwrap().ranges.len(), 4);
    }

    fn unused_loopback_address() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().to_string()
    }

    fn open_distribution_store_for_test(root: &Path) -> DistributionStore {
        let config = load_distribution_config(root).unwrap();
        DistributionStore::open(root, config, false).unwrap()
    }

    #[test]
    fn serve_supervisors_automatically_move_data_between_two_members() {
        let source_root = tempfile::tempdir().unwrap();
        let target_root = tempfile::tempdir().unwrap();
        let source_address = unused_loopback_address();
        let target_address = unused_loopback_address();
        let cluster_id = bicdb_core::ClusterId::new("serve-moving-cluster").unwrap();
        // These ids give the target a deliberately shorter deterministic
        // election delay than the range source. The regression only matters
        // when metadata and range leadership live on different nodes; leaving
        // that to thread scheduling made this test pass or fail by runner.
        let source_id = bicdb_core::ClusterNodeId::new("data-source").unwrap();
        let target_id = bicdb_core::ClusterNodeId::new("control-leader").unwrap();
        let now_ms = cluster_now_ms();
        let distribution = bicdb_core::DistributionConfig {
            enabled: true,
            cluster_id: cluster_id.clone(),
            node_id: source_id.clone(),
            node_address: source_address,
            node_capacity_bytes: 10_000_000,
            replication_factor: 1,
            initial_ranges: 4,
            suspect_after_ms: 10_000,
            dead_after_ms: 20_000,
            ..bicdb_core::DistributionConfig::default()
        };
        let mut store =
            DistributionStore::initialize_at(source_root.path(), distribution, false, now_ms)
                .unwrap();
        store
            .join_node(
                bicdb_core::ClusterNode::new(
                    target_id.clone(),
                    target_address,
                    1,
                    10_000_000,
                    now_ms.saturating_add(1),
                )
                .unwrap(),
                &source_id,
                now_ms.saturating_add(1),
            )
            .unwrap();
        let (_, relocation_ids) = store
            .start_failure_repair_cycle(
                &bicdb_core::RebalanceOptions {
                    max_replica_moves: 1,
                    max_moves_per_node: 1,
                    unknown_range_bytes: 1,
                    ..bicdb_core::RebalanceOptions::default()
                },
                &source_id,
                now_ms.saturating_add(2),
            )
            .unwrap();
        let relocation_id = relocation_ids[0];
        let range = store
            .topology()
            .range_by_id(store.relocation(relocation_id).unwrap().range_id)
            .unwrap()
            .clone();
        store
            .provision_member_directory(target_root.path(), &target_id, false)
            .unwrap();
        drop(store);

        let pgwire = PgWireConfig {
            fsync: false,
            ..PgWireConfig::default()
        };
        {
            let mut source = BicDb::open_with_config(
                source_root.path(),
                database_config_for_server(source_root.path(), &pgwire)
                    .unwrap()
                    .with_required_commit_admission(false),
            )
            .unwrap();
            source.create_collection("documents").unwrap();
            source
                .batch_insert(
                    "documents",
                    (0..100).map(|number| {
                        bicdb_core::Record::new(format!("doc-{number:05}"))
                            .with_metadata(json!({"number": number}))
                    }),
                )
                .unwrap();
            source.close().unwrap();
        }
        let source_server = PgWireServer::open(source_root.path(), pgwire.clone()).unwrap();
        let target_server = PgWireServer::open(target_root.path(), pgwire).unwrap();
        let inside_ids = (0..100)
            .map(|number| format!("doc-{number:05}"))
            .filter(|id| range.contains_token(bicdb_core::distribution_key_token("documents", id)))
            .collect::<Vec<_>>();
        assert!(!inside_ids.is_empty());

        // Start the member first so its data listener is ready before the
        // controller begins bounded copy steps.
        start_distribution_supervisor(Arc::clone(&target_server), automatic_test_supervisor());
        start_distribution_supervisor(Arc::clone(&source_server), automatic_test_supervisor());

        let leadership_deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let target_is_leader = bicdb_core::MetadataConsensusStore::inspect(target_root.path())
                .ok()
                .is_some_and(|status| {
                    status.role == bicdb_core::MetadataConsensusRole::Leader
                        && status.leader_id.as_ref() == Some(&target_id)
                });
            if target_is_leader {
                break;
            }
            assert!(
                Instant::now() < leadership_deadline,
                "the non-source metadata controller was not elected"
            );
            thread::sleep(Duration::from_millis(25));
        }

        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let source_config = load_distribution_config(source_root.path()).unwrap();
            let observed =
                DistributionStore::open(source_root.path(), source_config, false).unwrap();
            if observed
                .relocation(relocation_id)
                .is_some_and(|relocation| {
                    relocation.phase == bicdb_core::RelocationPhase::Completed
                })
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "automatic cross-server relocation did not complete"
            );
            thread::sleep(Duration::from_millis(50));
        }
        for id in &inside_ids {
            assert!(
                source_server
                    .db
                    .read()
                    .get("documents", id)
                    .unwrap()
                    .is_none(),
                "source retained moved record {id}"
            );
            assert!(
                target_server
                    .db
                    .read()
                    .get("documents", id)
                    .unwrap()
                    .is_some(),
                "target missed moved record {id}"
            );
        }
        let source_fingerprint = source_server
            .db
            .read()
            .schema_compatibility_fingerprint()
            .unwrap()
            .sha256;
        let target_fingerprint = target_server
            .db
            .read()
            .schema_compatibility_fingerprint()
            .unwrap()
            .sha256;
        assert_eq!(target_fingerprint, source_fingerprint);
        let observed = open_distribution_store_for_test(target_root.path());
        let target = &observed.topology().nodes[&target_id];
        assert_eq!(
            target
                .labels
                .get(bicdb_core::SCHEMA_COMPATIBILITY_NODE_LABEL),
            Some(&source_fingerprint)
        );
        assert!(!target
            .labels
            .contains_key(bicdb_core::SCHEMA_BOOTSTRAP_NODE_LABEL));
        source_server.request_shutdown();
        target_server.request_shutdown();
        thread::sleep(Duration::from_millis(1_100));
    }

    #[test]
    fn metadata_quorum_elects_and_commits_after_leader_failure() {
        let roots = [
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        ];
        let addresses = [
            unused_loopback_address(),
            unused_loopback_address(),
            unused_loopback_address(),
        ];
        let node_ids = [
            bicdb_core::ClusterNodeId::new("n1").unwrap(),
            bicdb_core::ClusterNodeId::new("n2").unwrap(),
            bicdb_core::ClusterNodeId::new("n3").unwrap(),
        ];
        let cluster_id = bicdb_core::ClusterId::new("metadata-failover-cluster").unwrap();
        let now_ms = cluster_now_ms();
        let distribution = bicdb_core::DistributionConfig {
            enabled: true,
            cluster_id,
            node_id: node_ids[0].clone(),
            node_address: addresses[0].clone(),
            node_capacity_bytes: 10_000_000,
            replication_factor: 1,
            initial_ranges: 2,
            metadata_election_timeout_ms: 300,
            metadata_heartbeat_interval_ms: 50,
            transport: bicdb_core::ClusterNetworkTransportConfig {
                connect_timeout_ms: 100,
                io_timeout_ms: 500,
                ..bicdb_core::ClusterNetworkTransportConfig::default()
            },
            ..bicdb_core::DistributionConfig::default()
        };
        let mut store =
            DistributionStore::initialize_at(roots[0].path(), distribution, false, now_ms).unwrap();
        for index in 1..node_ids.len() {
            store
                .join_node(
                    bicdb_core::ClusterNode::new(
                        node_ids[index].clone(),
                        addresses[index].clone(),
                        1,
                        10_000_000,
                        now_ms.saturating_add(index as u64),
                    )
                    .unwrap(),
                    &node_ids[0],
                    now_ms.saturating_add(index as u64),
                )
                .unwrap();
        }
        for index in 1..node_ids.len() {
            store
                .provision_member_directory(roots[index].path(), &node_ids[index], false)
                .unwrap();
        }
        drop(store);

        let pgwire = PgWireConfig {
            fsync: false,
            ..PgWireConfig::default()
        };
        let mut servers = roots
            .iter()
            .map(|root| PgWireServer::open(root.path(), pgwire.clone()).unwrap())
            .collect::<Vec<_>>();
        for server in &servers {
            start_distribution_supervisor(Arc::clone(server), automatic_test_supervisor());
        }

        let first_deadline = Instant::now() + Duration::from_secs(10);
        let initial = loop {
            let leaders = roots
                .iter()
                .filter_map(|root| bicdb_core::MetadataConsensusStore::inspect(root.path()).ok())
                .filter(|status| {
                    status.role == bicdb_core::MetadataConsensusRole::Leader
                        && status.commit_index == status.last_log_index
                        && status.commit_index > 0
                })
                .collect::<Vec<_>>();
            if leaders.len() == 1 {
                break leaders[0].clone();
            }
            assert!(
                Instant::now() < first_deadline,
                "three-node metadata quorum did not elect a clean leader"
            );
            thread::sleep(Duration::from_millis(25));
        };
        let failed_index = node_ids
            .iter()
            .position(|node_id| node_id == &initial.node_id)
            .unwrap();
        let heartbeat_deadline = Instant::now() + Duration::from_secs(5);
        let heartbeat_before_failure = loop {
            let config = load_distribution_config(roots[failed_index].path()).unwrap();
            let observed =
                DistributionStore::open(roots[failed_index].path(), config, false).unwrap();
            let heartbeats = node_ids
                .iter()
                .map(|node_id| observed.topology().nodes[node_id].last_heartbeat_ms)
                .collect::<Vec<_>>();
            if heartbeats
                .iter()
                .all(|heartbeat| *heartbeat > now_ms.saturating_add(2))
            {
                break heartbeats;
            }
            assert!(
                Instant::now() < heartbeat_deadline,
                "metadata leader did not quorum-commit every member heartbeat"
            );
            thread::sleep(Duration::from_millis(25));
        };
        let failed_server = servers.remove(failed_index);
        failed_server.request_shutdown();
        drop(failed_server);

        let failover_deadline = Instant::now() + Duration::from_secs(10);
        let replacement = loop {
            let replacement = roots.iter().filter_map(|root| {
                bicdb_core::MetadataConsensusStore::inspect(root.path())
                    .ok()
                    .filter(|status| {
                        status.node_id != initial.node_id
                            && status.role == bicdb_core::MetadataConsensusRole::Leader
                            && status.current_term > initial.current_term
                            && status.commit_index > initial.commit_index
                            && status.commit_index == status.last_log_index
                    })
            });
            if let Some(replacement) = replacement.into_iter().next() {
                break replacement;
            }
            assert!(
                Instant::now() < failover_deadline,
                "surviving metadata quorum did not elect and commit through a replacement leader"
            );
            thread::sleep(Duration::from_millis(25));
        };

        let follower_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let converged = roots.iter().any(|root| {
                bicdb_core::MetadataConsensusStore::inspect(root.path())
                    .ok()
                    .is_some_and(|status| {
                        status.node_id != initial.node_id
                            && status.node_id != replacement.node_id
                            && status.current_term == replacement.current_term
                            && status.leader_id.as_ref() == Some(&replacement.node_id)
                            && status.commit_index >= replacement.commit_index
                    })
            });
            if converged {
                break;
            }
            assert!(
                Instant::now() < follower_deadline,
                "replacement metadata leader did not converge its surviving follower"
            );
            thread::sleep(Duration::from_millis(25));
        }

        let replacement_index = node_ids
            .iter()
            .position(|node_id| node_id == &replacement.node_id)
            .unwrap();
        let heartbeat_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let config = load_distribution_config(roots[replacement_index].path()).unwrap();
            let observed =
                DistributionStore::open(roots[replacement_index].path(), config, false).unwrap();
            let advanced = node_ids.iter().enumerate().all(|(index, node_id)| {
                node_id == &initial.node_id
                    || observed.topology().nodes[node_id].last_heartbeat_ms
                        > heartbeat_before_failure[index]
            });
            if advanced {
                break;
            }
            assert!(
                Instant::now() < heartbeat_deadline,
                "surviving members did not continue quorum-committed heartbeats after failover"
            );
            thread::sleep(Duration::from_millis(25));
        }

        // Reopen the failed process from its durable files and prove it
        // rejoins as a follower of the replacement rather than reviving its
        // former authority or stale topology.
        thread::sleep(Duration::from_millis(1_100));
        let restarted = PgWireServer::open(roots[failed_index].path(), pgwire.clone()).unwrap();
        start_distribution_supervisor(Arc::clone(&restarted), automatic_test_supervisor());
        let restart_deadline = Instant::now() + Duration::from_secs(10);
        let mut authority_checkpoint = None;
        loop {
            let status =
                bicdb_core::MetadataConsensusStore::inspect(roots[failed_index].path()).unwrap();
            let leaders = roots
                .iter()
                .filter_map(|root| bicdb_core::MetadataConsensusStore::inspect(root.path()).ok())
                .filter(|candidate| {
                    candidate.node_id != initial.node_id
                        && candidate.role == bicdb_core::MetadataConsensusRole::Leader
                })
                .collect::<Vec<_>>();
            if leaders.len() == 1 {
                let leader = &leaders[0];
                let authority_changed =
                    authority_checkpoint
                        .as_ref()
                        .is_none_or(|(node_id, term, _, _)| {
                            node_id != &leader.node_id || *term != leader.current_term
                        });
                if authority_changed {
                    // Freeze the first committed point observed for this
                    // authority. Comparing against the leader's continuously
                    // advancing live indexes makes follower convergence a
                    // moving target and can miss forever on a busy runner.
                    authority_checkpoint = Some((
                        leader.node_id.clone(),
                        leader.current_term,
                        leader.commit_index,
                        leader.topology_generation,
                    ));
                }
            } else {
                authority_checkpoint = None;
            }
            if let Some((leader_id, term, commit_index, topology_generation)) =
                authority_checkpoint.as_ref()
            {
                if status.role == bicdb_core::MetadataConsensusRole::Follower
                    && status.leader_id.as_ref() == Some(leader_id)
                    && status.current_term == *term
                    && status.commit_index >= *commit_index
                    && status.topology_generation >= *topology_generation
                {
                    break;
                }
            }
            assert!(
                Instant::now() < restart_deadline,
                "restarted former metadata leader did not rejoin the replacement authority"
            );
            thread::sleep(Duration::from_millis(25));
        }

        for server in &servers {
            server.request_shutdown();
        }
        restarted.request_shutdown();
        thread::sleep(Duration::from_millis(1_100));
    }

    #[test]
    fn live_metadata_leader_bootstraps_and_promotes_a_new_learner() {
        let leader_root = tempfile::tempdir().unwrap();
        let learner_root = tempfile::tempdir().unwrap();
        let leader_address = unused_loopback_address();
        let learner_address = unused_loopback_address();
        let leader_id = bicdb_core::ClusterNodeId::new("n1").unwrap();
        let learner_id = bicdb_core::ClusterNodeId::new("n2").unwrap();
        let cluster_id = bicdb_core::ClusterId::new("metadata-bootstrap-cluster").unwrap();
        let now_ms = cluster_now_ms();
        let distribution = bicdb_core::DistributionConfig {
            enabled: true,
            cluster_id: cluster_id.clone(),
            node_id: leader_id.clone(),
            node_address: leader_address,
            node_capacity_bytes: 10_000_000,
            replication_factor: 1,
            initial_ranges: 2,
            metadata_election_timeout_ms: 300,
            metadata_heartbeat_interval_ms: 50,
            transport: bicdb_core::ClusterNetworkTransportConfig {
                connect_timeout_ms: 100,
                io_timeout_ms: 500,
                ..bicdb_core::ClusterNetworkTransportConfig::default()
            },
            ..bicdb_core::DistributionConfig::default()
        };
        let initial =
            DistributionStore::initialize_at(leader_root.path(), distribution, false, now_ms)
                .unwrap();
        let rpc = TcpClusterRelocationTransport::from_topology(
            cluster_id,
            leader_id.clone(),
            initial.topology().clone(),
            initial.config().transport.clone(),
        )
        .unwrap();
        drop(initial);

        let pgwire = PgWireConfig {
            fsync: false,
            ..PgWireConfig::default()
        };
        let leader_server = PgWireServer::open(leader_root.path(), pgwire.clone()).unwrap();
        start_distribution_supervisor(Arc::clone(&leader_server), automatic_test_supervisor());
        let election_deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if bicdb_core::MetadataConsensusStore::inspect(leader_root.path())
                .ok()
                .is_some_and(|status| {
                    status.role == bicdb_core::MetadataConsensusRole::Leader
                        && status.commit_index == status.last_log_index
                        && status.commit_index > 0
                })
            {
                break;
            }
            assert!(
                Instant::now() < election_deadline,
                "single metadata voter did not elect itself"
            );
            thread::sleep(Duration::from_millis(25));
        }

        let learner = bicdb_core::ClusterNode::new(
            learner_id.clone(),
            learner_address,
            1,
            10_000_000,
            cluster_now_ms(),
        )
        .unwrap()
        .as_metadata_learner();
        rpc.register_metadata_learner(&leader_id, learner, cluster_now_ms())
            .unwrap();
        let registration_deadline = Instant::now() + Duration::from_secs(10);
        let registered = loop {
            match rpc.fetch_topology(&leader_id) {
                Ok(topology)
                    if topology.nodes.get(&learner_id).is_some_and(|node| {
                        node.metadata_role == bicdb_core::MetadataMemberRole::Learner
                    }) =>
                {
                    break topology;
                }
                Ok(topology) => {
                    rpc.install_topology(topology).unwrap();
                }
                Err(_) => {}
            }
            assert!(
                Instant::now() < registration_deadline,
                "learner registration was not quorum committed"
            );
            thread::sleep(Duration::from_millis(25));
        };
        assert!(registered.generation > 1);
        assert!(registered.ranges.values().all(|range| {
            range
                .replicas
                .iter()
                .all(|replica| replica.node_id != learner_id)
        }));
        rpc.install_topology(registered.clone()).unwrap();
        let leader_store = open_distribution_store_for_test(leader_root.path());
        leader_store
            .provision_member_directory_from_snapshot(
                learner_root.path(),
                registered,
                &learner_id,
                leader_store.config().transport.clone(),
                false,
            )
            .unwrap();

        let learner_server = PgWireServer::open(learner_root.path(), pgwire).unwrap();
        start_distribution_supervisor(Arc::clone(&learner_server), automatic_test_supervisor());
        let promotion_deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let leader_status =
                bicdb_core::MetadataConsensusStore::inspect(leader_root.path()).ok();
            let learner_status =
                bicdb_core::MetadataConsensusStore::inspect(learner_root.path()).ok();
            let leader_topology = open_distribution_store_for_test(leader_root.path())
                .topology()
                .clone();
            let learner_topology = open_distribution_store_for_test(learner_root.path())
                .topology()
                .clone();
            if leader_status.as_ref().is_some_and(|status| {
                status.voters.contains(&learner_id) && !status.learners.contains(&learner_id)
            }) && learner_status.as_ref().is_some_and(|status| {
                status.voters.contains(&learner_id)
                    && !status.learners.contains(&learner_id)
                    && status.commit_index == status.last_log_index
            }) && leader_topology.nodes[&learner_id].metadata_role
                == bicdb_core::MetadataMemberRole::Voter
                && learner_topology == leader_topology
            {
                break;
            }
            assert!(
                Instant::now() < promotion_deadline,
                "caught-up metadata learner was not jointly promoted"
            );
            thread::sleep(Duration::from_millis(25));
        }

        leader_server.request_shutdown();
        learner_server.request_shutdown();
        thread::sleep(Duration::from_millis(1_100));
    }

    fn assert_connection_identity_cleared(state: &ConnectionState, context: &str) {
        assert!(!state.in_transaction, "{context}: transaction still open");
        assert!(
            !state.session_state.transaction_active(),
            "{context}: session transaction still open"
        );
        assert!(
            !state
                .session_state
                .session_gucs()
                .contains_key("app.identity"),
            "{context}: identity still present"
        );
    }

    #[test]
    fn commit_durability_failure_publishes_cleaned_session_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let mut db = BicDb::open(dir.path()).expect("open setup database");
            let mut session = SqlSession::new(&mut db);
            session
                .execute("CREATE TABLE cleanup_rows (id INT PRIMARY KEY)")
                .expect("create table");
        }
        let server = PgWireServer::open(dir.path(), PgWireConfig::default()).expect("open server");
        let mut state = ConnectionState::new(1, "bicdb".to_string(), None);
        begin_buffered_transaction(&server, &mut state).expect("begin");
        execute_server_sql(
            &server,
            &mut state,
            "SELECT set_config('app.identity', 'transaction-user', true)",
        )
        .expect("set local identity");
        execute_server_sql(&server, &mut state, "INSERT INTO cleanup_rows VALUES (1)")
            .expect("insert row");

        let transaction_log = dir.path().join("transactions.log");
        std::fs::remove_file(&transaction_log).expect("remove transaction log");
        std::fs::create_dir(&transaction_log).expect("replace log with directory");
        let error =
            commit_buffered_transaction(&server, &mut state, &CancellationToken::uncancelable())
                .expect_err("durability must fail");

        assert!(error.to_string().contains("io error"));
        assert_connection_identity_cleared(&state, "durability failure");
    }

    #[test]
    fn rollback_ddl_cleanup_failure_publishes_cleaned_session_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let server = PgWireServer::open(dir.path(), PgWireConfig::default()).expect("open server");
        let mut state = ConnectionState::new(1, "bicdb".to_string(), None);
        execute_server_sql(
            &server,
            &mut state,
            "CREATE TABLE cleanup_target (id INT PRIMARY KEY, value TEXT)",
        )
        .expect("create table");
        execute_server_sql(
            &server,
            &mut state,
            "CREATE INDEX cleanup_target_value_idx ON cleanup_target (value)",
        )
        .expect("create index");
        begin_buffered_transaction(&server, &mut state).expect("begin");
        execute_server_sql(
            &server,
            &mut state,
            "SELECT set_config('app.identity', 'transaction-user', true)",
        )
        .expect("set local identity");
        execute_server_sql(&server, &mut state, "DROP INDEX cleanup_target_value_idx")
            .expect("drop index");

        let mut competing = ConnectionState::new(2, "bicdb".to_string(), None);
        execute_server_sql(
            &server,
            &mut competing,
            "CREATE INDEX cleanup_target_value_idx ON cleanup_target (value)",
        )
        .expect("recreate conflicting index");
        rollback_open_transaction(&server, &mut state).expect_err("DDL undo must fail");

        assert_connection_identity_cleared(&state, "DDL rollback failure");
    }

    #[test]
    fn replay_error_and_cancellation_publish_cleaned_session_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let server = PgWireServer::open(dir.path(), PgWireConfig::default()).expect("open server");

        let mut error_state = ConnectionState::new(1, "bicdb".to_string(), None);
        error_state.in_transaction = true;
        error_state.session_state.begin_transaction();
        error_state.tx_statements = vec![
            TxBufferedOperation::Sql(
                "SELECT set_config('app.identity', 'transaction-user', true)".to_string(),
            ),
            TxBufferedOperation::Sql("SELECT * FROM missing_replay_table".to_string()),
        ];
        commit_buffered_transaction(
            &server,
            &mut error_state,
            &CancellationToken::uncancelable(),
        )
        .expect_err("replay must fail");
        assert_connection_identity_cleared(&error_state, "replay error");

        let mut canceled_state = ConnectionState::new(2, "bicdb".to_string(), None);
        canceled_state.in_transaction = true;
        canceled_state.session_state.begin_transaction();
        {
            let db = server.read_db().expect("read database");
            let mut session = with_connection_guc_state(
                sql_session_shared_for_server(&server, &db),
                &server,
                &canceled_state,
            );
            session
                .execute("SELECT set_config('app.identity', 'transaction-user', true)")
                .expect("set local identity");
            capture_connection_guc_state(&mut canceled_state, &session);
        }
        canceled_state.tx_statements = vec![TxBufferedOperation::Sql("SELECT 1".to_string())];
        let expired = CancellationToken::new(
            Arc::new(AtomicBool::new(false)),
            Some(Instant::now() - Duration::from_millis(1)),
        );
        commit_buffered_transaction(&server, &mut canceled_state, &expired)
            .expect_err("replay cancellation must fail");
        assert_connection_identity_cleared(&canceled_state, "replay cancellation");
    }

    #[test]
    fn with_update_is_classified_as_write() {
        let sql = "WITH affected AS (SELECT id FROM jobs) UPDATE jobs SET status = 0 FROM affected WHERE jobs.id = affected.id";
        let normalized = normalize_executable_sql(sql);

        assert!(is_write_sql(&normalized));
        assert!(matches!(
            query_kind_for_normalized_sql(&normalized),
            QueryKind::Write
        ));
        assert!(!is_read_only_sql_for_shared_execution(sql));
    }

    #[test]
    fn shared_role_ddl_excludes_database_local_object_privileges() {
        assert_eq!(
            shared_role_ddl_statements(
                "CREATE ROLE reader; GRANT parent TO reader; REVOKE parent FROM reader"
            )
            .len(),
            3
        );
        assert!(shared_role_ddl_statements("GRANT SELECT ON TABLE docs TO reader").is_empty());
        assert!(shared_role_ddl_statements("REVOKE USAGE ON TYPE status FROM reader").is_empty());
    }

    #[test]
    fn configure_accepted_socket_disables_nagle() {
        use std::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind loopback listener");
        let addr = listener.local_addr().expect("listener addr");
        let _client = TcpStream::connect(addr).expect("connect loopback");
        let (accepted, _) = listener.accept().expect("accept loopback");

        // The helper must leave TCP_NODELAY enabled so the pgwire request/response
        // loop is not stalled by Nagle + delayed-ACK on the accepted socket.
        configure_accepted_socket(&accepted);
        assert!(accepted.nodelay().expect("read nodelay"));
    }

    #[test]
    fn with_select_remains_read_only() {
        let sql = "WITH affected AS (SELECT id FROM jobs) SELECT id FROM affected";
        let normalized = normalize_executable_sql(sql);

        assert!(!is_write_sql(&normalized));
        assert!(matches!(
            query_kind_for_normalized_sql(&normalized),
            QueryKind::Read
        ));
        assert!(is_read_only_sql_for_shared_execution(sql));
        assert!(is_describable_query(sql));
    }

    #[test]
    fn dml_target_relation_preserves_target_aliases() {
        assert_eq!(
            dml_target_relation(
                "UPDATE app_overlays AS overlay SET patch = $1 RETURNING overlay.id"
            ),
            Some("app_overlays AS overlay".to_string())
        );
        assert_eq!(
            dml_target_relation(
                "DELETE FROM app_overlays AS overlay WHERE id = $1 RETURNING overlay.id"
            ),
            Some("app_overlays AS overlay".to_string())
        );
    }

    #[test]
    fn current_schemas_infers_name_array_oid() {
        assert_eq!(
            infer_select_cast_oids("SELECT pg_catalog.current_schemas(false)", 1),
            Some(vec![NAME_ARRAY_OID])
        );
        assert_eq!(
            infer_select_cast_oids("SELECT current_schemas(true) AS schemas", 1),
            Some(vec![NAME_ARRAY_OID])
        );
    }

    #[test]
    fn untyped_select_expressions_fall_back_to_value_inference() {
        assert_eq!(infer_select_cast_oids("SELECT 1", 1), None);
        assert_eq!(infer_select_cast_oids("SELECT $1", 1), None);
    }

    #[test]
    fn jsonb_expression_oids_follow_the_outer_expression() {
        assert_eq!(
            infer_select_cast_oids("SELECT jsonb_typeof($1::jsonb) = 'object'", 1),
            Some(vec![16])
        );
        assert_eq!(
            infer_select_cast_oids("SELECT $1::jsonb->'items'", 1),
            Some(vec![3802])
        );
        assert_eq!(
            infer_select_cast_oids("SELECT $1::jsonb->>'name'", 1),
            Some(vec![25])
        );
        assert_eq!(
            infer_select_cast_oids(
                "SELECT jsonb_build_object('a', 1), jsonb_array_length($1::jsonb)",
                2,
            ),
            Some(vec![3802, 23])
        );
        assert_eq!(
            infer_select_cast_oids("SELECT $1::jsonb #>> '{a,b}', $1::jsonb ? 'a'", 2),
            Some(vec![25, 16])
        );
    }

    #[test]
    fn coalesce_scalar_subquery_fallback_infers_boolean_oid() {
        assert_eq!(
            infer_select_cast_oids(
                "SELECT COALESCE((SELECT is_enabled FROM hub_feature_flags WHERE org_id = $1 AND hub_id = $2 AND feature::text = $3 LIMIT 1), true)",
                1,
            ),
            Some(vec![16])
        );
    }

    #[test]
    fn json_arrays_render_as_postgres_array_text_for_array_results() {
        let value = SqlValue::Json(json!(["public", "schema with space", "NULL", "a,b"]));

        assert_eq!(
            String::from_utf8(encode_text_result_value(&value, NAME_ARRAY_OID).unwrap()).unwrap(),
            r#"{public,"schema with space","NULL","a,b"}"#
        );
        assert_eq!(
            String::from_utf8(
                encode_text_result_value(&SqlValue::Json(json!([[1, 2], [3, 4]])), 1007).unwrap()
            )
            .unwrap(),
            "{{1,2},{3,4}}"
        );
        let bounded = SqlValue::Json(json!({
            "$bicdb_array_input": {
                "lower_bounds": [0, 5],
                "value": [[1, 2], [3, 4]],
            }
        }));
        assert_eq!(
            String::from_utf8(encode_text_result_value(&bounded, 1007).unwrap()).unwrap(),
            "[0:1][5:6]={{1,2},{3,4}}"
        );
    }

    #[test]
    fn jsonb_text_and_binary_results_use_postgres_canonical_rendering() {
        let value = SqlValue::Json(
            serde_json::from_str(r#"{"aa":1,"b":["x",null,2.00],"A":1e2}"#).unwrap(),
        );
        let expected = br#"{"A": 100, "b": ["x", null, 2.00], "aa": 1}"#;

        assert_eq!(encode_text_result_value(&value, 3802).unwrap(), expected);
        let mut binary = vec![1];
        binary.extend_from_slice(expected);
        assert_eq!(encode_binary_result_value(&value, 3802).unwrap(), binary);
        assert_eq!(
            postgres_jsonb_text(&serde_json::from_str::<JsonValue>("1.20e-2").unwrap()),
            "0.0120"
        );
        assert_eq!(
            postgres_jsonb_text(&serde_json::from_str::<JsonValue>("0e2").unwrap()),
            "0"
        );
    }

    #[test]
    fn float_text_results_use_postgresql_precision_and_special_spellings() {
        for (oid, value, expected) in [
            (700, f64::from(0.1_f32), "0.1"),
            (700, 1e6, "1e+06"),
            (701, 1e-5, "1e-05"),
            (701, 1e-4, "0.0001"),
            (701, -0.0, "-0"),
            (701, f64::NAN, "NaN"),
            (701, f64::INFINITY, "Infinity"),
            (701, f64::NEG_INFINITY, "-Infinity"),
        ] {
            assert_eq!(
                encode_text_result_value(&SqlValue::Float(value), oid).unwrap(),
                expected.as_bytes(),
            );
        }
    }

    #[test]
    fn inet_text_results_omit_only_full_width_masks() {
        for (input, expected) in [
            ("192.0.2.1/32", "192.0.2.1"),
            ("192.0.2.1/24", "192.0.2.1/24"),
            ("::1/128", "::1"),
            ("2001:db8::1/64", "2001:db8::1/64"),
            ("::ffff:c000:201/128", "::ffff:192.0.2.1"),
        ] {
            assert_eq!(
                encode_text_result_value(&SqlValue::String(input.to_string()), 869).unwrap(),
                expected.as_bytes(),
                "input: {input}",
            );
        }
        assert_eq!(
            encode_text_result_value(&SqlValue::String("192.0.2.1/32".to_string()), 650).unwrap(),
            b"192.0.2.1/32",
        );
    }

    #[test]
    fn network_array_text_and_binary_codecs_match_postgresql_wire_layouts() {
        let cases = [
            (
                1041,
                json!(["192.0.2.1/32", "2001:db8::1/128", "192.0.2.1/24"]),
                "{192.0.2.1,2001:db8::1,192.0.2.1/24}",
                "00000001000000000000036500000003000000010000000802200004c0000201000000140380001020010db80000000000000000000000010000000802180004c0000201",
                r#"[1:3]={"192.0.2.1/32","2001:db8::1/128","192.0.2.1/24"}"#,
            ),
            (
                651,
                json!(["192.0.2.0/24", "2001:db8::/32"]),
                "{192.0.2.0/24,2001:db8::/32}",
                "00000001000000000000028a00000002000000010000000802180104c0000200000000140320011020010db8000000000000000000000000",
                r#"[1:2]={"192.0.2.0/24","2001:db8::/32"}"#,
            ),
            (
                1040,
                json!(["08:00:2b:01:02:03", "08:00:2b:01:02:04"]),
                "{08:00:2b:01:02:03,08:00:2b:01:02:04}",
                "00000001000000000000033d00000002000000010000000608002b0102030000000608002b010204",
                r#"[1:2]={"08:00:2b:01:02:03","08:00:2b:01:02:04"}"#,
            ),
            (
                775,
                json!(["08:00:2b:01:02:03:04:05", "08:00:2b:01:02:03:04:06"]),
                "{08:00:2b:01:02:03:04:05,08:00:2b:01:02:03:04:06}",
                "00000001000000000000030600000002000000010000000808002b01020304050000000808002b0102030406",
                r#"[1:2]={"08:00:2b:01:02:03:04:05","08:00:2b:01:02:03:04:06"}"#,
            ),
        ];

        for (array_oid, value, text, binary_hex, decoded) in cases {
            let value = SqlValue::Json(value);
            assert_eq!(
                encode_text_result_value(&value, array_oid).unwrap(),
                text.as_bytes(),
                "text array oid {array_oid}",
            );
            let binary = hex::decode(binary_hex).unwrap();
            assert_eq!(
                encode_binary_result_value(&value, array_oid).unwrap(),
                binary,
                "binary array oid {array_oid}",
            );
            assert_eq!(
                decode_binary_parameter(&binary, array_oid).unwrap(),
                quote_sql_string(decoded),
                "decode array oid {array_oid}",
            );
        }
    }

    #[test]
    fn geometric_array_text_and_binary_codecs_match_postgresql_wire_layouts() {
        let cases = [
            (
                1017,
                json!(["(1,2)", "(3,4)"]),
                r#"{"(1,2)","(3,4)"}"#,
                "0000000100000000000002580000000200000001000000103ff000000000000040000000000000000000001040080000000000004010000000000000",
                r#"[1:2]={"(1,2)","(3,4)"}"#,
            ),
            (
                1018,
                json!(["[(1,2),(3,4)]", "[(5,6),(7,8)]"]),
                r#"{"[(1,2),(3,4)]","[(5,6),(7,8)]"}"#,
                "0000000100000000000002590000000200000001000000203ff00000000000004000000000000000400800000000000040100000000000000000002040140000000000004018000000000000401c0000000000004020000000000000",
                r#"[1:2]={"[(1,2),(3,4)]","[(5,6),(7,8)]"}"#,
            ),
            (
                1019,
                json!(["[(1,2),(3,4)]", "((5,6),(7,8))"]),
                r#"{"[(1,2),(3,4)]","((5,6),(7,8))"}"#,
                "00000001000000000000025a00000002000000010000002500000000023ff000000000000040000000000000004008000000000000401000000000000000000025010000000240140000000000004018000000000000401c0000000000004020000000000000",
                r#"[1:2]={"[(1,2),(3,4)]","((5,6),(7,8))"}"#,
            ),
            (
                1020,
                json!(["(1,2),(3,4)", "(5,6),(7,8)"]),
                "{(3,4),(1,2);(7,8),(5,6)}",
                "00000001000000000000025b000000020000000100000020400800000000000040100000000000003ff0000000000000400000000000000000000020401c000000000000402000000000000040140000000000004018000000000000",
                r#"[1:2]={"(3,4),(1,2)";"(7,8),(5,6)"}"#,
            ),
            (
                1027,
                json!(["((1,2),(3,4),(5,6))", "((7,8),(9,10),(11,12))"]),
                r#"{"((1,2),(3,4),(5,6))","((7,8),(9,10),(11,12))"}"#,
                "00000001000000000000025c000000020000000100000034000000033ff0000000000000400000000000000040080000000000004010000000000000401400000000000040180000000000000000003400000003401c00000000000040200000000000004022000000000000402400000000000040260000000000004028000000000000",
                r#"[1:2]={"((1,2),(3,4),(5,6))","((7,8),(9,10),(11,12))"}"#,
            ),
            (
                629,
                json!(["{1,2,3}", "{4,5,6}"]),
                r#"{"{1,2,3}","{4,5,6}"}"#,
                "0000000100000000000002740000000200000001000000183ff00000000000004000000000000000400800000000000000000018401000000000000040140000000000004018000000000000",
                r#"[1:2]={"{1,2,3}","{4,5,6}"}"#,
            ),
            (
                719,
                json!(["<(1,2),3>", "<(4,5),6>"]),
                r#"{"<(1,2),3>","<(4,5),6>"}"#,
                "0000000100000000000002ce0000000200000001000000183ff00000000000004000000000000000400800000000000000000018401000000000000040140000000000004018000000000000",
                r#"[1:2]={"<(1,2),3>","<(4,5),6>"}"#,
            ),
        ];

        for (array_oid, value, text, binary_hex, decoded) in cases {
            let value = SqlValue::Json(value);
            assert_eq!(
                encode_text_result_value(&value, array_oid).unwrap(),
                text.as_bytes(),
                "text array oid {array_oid}",
            );
            let binary = hex::decode(binary_hex).unwrap();
            assert_eq!(
                encode_binary_result_value(&value, array_oid).unwrap(),
                binary,
                "binary array oid {array_oid}",
            );
            assert_eq!(
                decode_binary_parameter(&binary, array_oid).unwrap(),
                quote_sql_string(decoded),
                "decode array oid {array_oid}",
            );
        }

        assert_eq!(
            sql_literal_for_text_parameter("{(3,4),(1,2);(7,8),(5,6)}", 1020,).unwrap(),
            "ARRAY['(3,4),(1,2)', '(7,8),(5,6)']",
        );
    }

    #[test]
    fn text_binary_codecs_match_postgresql_wire_layouts() {
        for (oid, text, binary) in [
            (19, "identifier", b"identifier".as_slice()),
            (25, "hello", b"hello".as_slice()),
            (114, r#"{"a":1}"#, br#"{"a":1}"#.as_slice()),
            (142, "<value/>", b"<value/>".as_slice()),
            (1042, "p", b"p".as_slice()),
            (1043, "world", b"world".as_slice()),
            (1790, "portal name", b"portal name".as_slice()),
        ] {
            assert_eq!(
                decode_binary_parameter(binary, oid).unwrap(),
                quote_sql_string(text)
            );
            assert_eq!(
                encode_binary_result_value(&SqlValue::String(text.to_string()), oid).unwrap(),
                binary
            );
        }
        assert_eq!(decode_binary_parameter(b"Z", 18).unwrap(), "90");
        assert_eq!(decode_binary_parameter(&[0xc3], 18).unwrap(), "(-61)");
        assert_eq!(
            decode_binary_parameter(&vec![b'a'; 70], 19).unwrap(),
            quote_sql_string(&"a".repeat(70))
        );
        assert_eq!(
            encode_binary_result_value(&bicdb_sql::pg_internal_char_value(0xc3), 18).unwrap(),
            vec![0xc3]
        );

        let mut refcursor_array = Vec::new();
        refcursor_array.extend_from_slice(&1_i32.to_be_bytes());
        refcursor_array.extend_from_slice(&0_i32.to_be_bytes());
        refcursor_array.extend_from_slice(&1790_i32.to_be_bytes());
        refcursor_array.extend_from_slice(&2_i32.to_be_bytes());
        refcursor_array.extend_from_slice(&1_i32.to_be_bytes());
        refcursor_array.extend_from_slice(&11_i32.to_be_bytes());
        refcursor_array.extend_from_slice(b"portal name");
        refcursor_array.extend_from_slice(&0_i32.to_be_bytes());
        assert_eq!(
            decode_binary_parameter(&refcursor_array, 2201).unwrap(),
            quote_sql_string(r#"[1:2]={"portal name",""}"#)
        );
        assert_eq!(
            encode_binary_result_value(&SqlValue::Json(json!(["portal name", ""])), 2201).unwrap(),
            refcursor_array
        );

        let jsonpath = [vec![1], br#"$.value ? (@ > 2)"#.to_vec()].concat();
        assert_eq!(
            decode_binary_parameter(&jsonpath, 4072).unwrap(),
            quote_sql_string(r#"$.value ? (@ > 2)"#)
        );
        assert_eq!(
            encode_binary_result_value(
                &SqlValue::String(r#"$.value ? (@ > 2)"#.to_string()),
                4072,
            )
            .unwrap(),
            jsonpath
        );
    }

    #[test]
    fn bit_binary_codec_packs_most_significant_bit_first() {
        let binary = vec![0, 0, 0, 6, 0b1010_0100];
        assert_eq!(decode_binary_parameter(&binary, 1562).unwrap(), "'101001'");
        assert_eq!(
            encode_binary_result_value(&SqlValue::String("101001".to_string()), 1562).unwrap(),
            binary
        );

        let nonzero_padding = vec![0, 0, 0, 6, 0b1010_0111];
        assert_eq!(
            decode_binary_parameter(&nonzero_padding, 1562).unwrap(),
            "'101001'"
        );
        assert_eq!(decode_binary_parameter(&[0, 0, 0, 0], 1562).unwrap(), "''");
    }

    #[test]
    fn text_and_bit_binary_codecs_reject_malformed_payloads() {
        assert!(decode_binary_parameter(b"ZZ", 18).is_err());
        assert!(decode_binary_parameter(&[0xff], 25).is_err());
        assert!(decode_binary_parameter(&[], 4072).is_err());
        assert!(decode_binary_parameter(&[2, b'$'], 4072).is_err());
        assert!(decode_binary_parameter(&[0, 0, 0], 1560).is_err());
        assert!(decode_binary_parameter(&(-1_i32).to_be_bytes(), 1560).is_err());
        assert!(decode_binary_parameter(&[0, 0, 0, 9, 0xff], 1560).is_err());
        assert!(encode_binary_result_value(&SqlValue::String("AB".into()), 18).is_err());
        assert!(encode_binary_result_value(&SqlValue::String("102".into()), 1562).is_err());
    }

    #[test]
    fn network_mac_and_geometric_binary_codecs_match_postgresql_layouts() {
        let ipv4 = vec![2, 24, 0, 4, 192, 0, 2, 1];
        assert_eq!(
            encode_binary_result_value(&SqlValue::String("192.0.2.1/24".into()), 869).unwrap(),
            ipv4
        );
        assert_eq!(
            decode_binary_parameter(&ipv4, 869).unwrap(),
            "'192.0.2.1/24'"
        );

        let mut ipv6 = vec![3, 32, 1, 16];
        ipv6.extend([0x20, 0x01, 0x0d, 0xb8]);
        ipv6.extend([0; 12]);
        assert_eq!(
            encode_binary_result_value(&SqlValue::String("2001:db8::/32".into()), 650).unwrap(),
            ipv6
        );
        assert_eq!(
            decode_binary_parameter(&ipv6, 650).unwrap(),
            "'2001:db8::/32'"
        );

        for (oid, text, binary) in [
            (829, "08:00:2b:01:02:03", vec![8, 0, 43, 1, 2, 3]),
            (
                774,
                "08:00:2b:01:02:03:04:05",
                vec![8, 0, 43, 1, 2, 3, 4, 5],
            ),
        ] {
            assert_eq!(
                encode_binary_result_value(&SqlValue::String(text.into()), oid).unwrap(),
                binary
            );
            assert_eq!(
                decode_binary_parameter(&binary, oid).unwrap(),
                quote_sql_string(text)
            );
        }

        let binary_floats = |values: &[f64]| {
            values
                .iter()
                .flat_map(|value| value.to_be_bytes())
                .collect::<Vec<_>>()
        };
        let mut open_path = vec![0];
        open_path.extend_from_slice(&2_i32.to_be_bytes());
        open_path.extend(binary_floats(&[1.0, 2.0, 3.0, 4.0]));
        let mut polygon = 3_i32.to_be_bytes().to_vec();
        polygon.extend(binary_floats(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]));
        for (oid, text, binary) in [
            (600, "(1,2)", binary_floats(&[1.0, 2.0])),
            (601, "[(1,2),(3,4)]", binary_floats(&[1.0, 2.0, 3.0, 4.0])),
            (603, "(3,4),(1,2)", binary_floats(&[3.0, 4.0, 1.0, 2.0])),
            (628, "{1,2,3}", binary_floats(&[1.0, 2.0, 3.0])),
            (602, "[(1,2),(3,4)]", open_path),
            (604, "((1,2),(3,4),(5,6))", polygon),
            (718, "<(1,2),3>", binary_floats(&[1.0, 2.0, 3.0])),
        ] {
            assert_eq!(
                encode_binary_result_value(&SqlValue::String(text.into()), oid).unwrap(),
                binary,
                "oid {oid}"
            );
            assert_eq!(
                decode_binary_parameter(&binary, oid).unwrap(),
                quote_sql_string(text),
                "oid {oid}"
            );
        }
    }

    #[test]
    fn range_multirange_oid_lsn_and_snapshot_binary_codecs_match_postgresql_layouts() {
        let int4range = [
            vec![RANGE_LB_INC],
            4_i32.to_be_bytes().to_vec(),
            1_i32.to_be_bytes().to_vec(),
            4_i32.to_be_bytes().to_vec(),
            3_i32.to_be_bytes().to_vec(),
        ]
        .concat();
        assert_eq!(
            encode_binary_result_value(&SqlValue::String("[1,3)".into()), 3904).unwrap(),
            int4range
        );
        assert_eq!(
            encode_binary_result_value(&SqlValue::String("[1,2]".into()), 3904).unwrap(),
            int4range,
            "discrete range binary output must use canonical bounds"
        );
        assert_eq!(
            decode_binary_parameter(&int4range, 3904).unwrap(),
            "'[1,3)'"
        );

        let second_range = [
            vec![RANGE_LB_INC],
            4_i32.to_be_bytes().to_vec(),
            5_i32.to_be_bytes().to_vec(),
            4_i32.to_be_bytes().to_vec(),
            8_i32.to_be_bytes().to_vec(),
        ]
        .concat();
        let multirange = [
            2_i32.to_be_bytes().to_vec(),
            (int4range.len() as i32).to_be_bytes().to_vec(),
            int4range.clone(),
            (second_range.len() as i32).to_be_bytes().to_vec(),
            second_range,
        ]
        .concat();
        assert_eq!(
            encode_binary_result_value(&SqlValue::String("{[1,3),[5,8)}".into()), 4451,).unwrap(),
            multirange
        );
        assert_eq!(
            decode_binary_parameter(&multirange, 4451).unwrap(),
            "'{[1,3),[5,8)}'"
        );

        for oid in [
            24, 26, 2202, 2203, 2204, 2205, 2206, 3734, 3769, 4089, 4096, 4191,
        ] {
            let binary = 42_u32.to_be_bytes();
            assert_eq!(
                encode_binary_result_value(&SqlValue::Int(42), oid).unwrap(),
                binary
            );
            assert_eq!(decode_binary_parameter(&binary, oid).unwrap(), "42");
        }
        let max_oid = u32::MAX.to_be_bytes();
        assert_eq!(
            encode_binary_result_value(&SqlValue::Int(i64::from(u32::MAX)), 26).unwrap(),
            max_oid
        );
        assert_eq!(
            decode_binary_parameter(&max_oid, 26).unwrap(),
            u32::MAX.to_string()
        );

        let lsn = 0x0000_0016_B374_D848_u64.to_be_bytes();
        assert_eq!(
            encode_binary_result_value(&SqlValue::String("16/B374D848".into()), 3220).unwrap(),
            lsn
        );
        assert_eq!(
            decode_binary_parameter(&lsn, 3220).unwrap(),
            "'16/B374D848'"
        );

        let snapshot = [
            2_i32.to_be_bytes().as_slice(),
            10_u64.to_be_bytes().as_slice(),
            20_u64.to_be_bytes().as_slice(),
            12_u64.to_be_bytes().as_slice(),
            15_u64.to_be_bytes().as_slice(),
        ]
        .concat();
        for oid in [2970, 5038] {
            assert_eq!(
                encode_binary_result_value(&SqlValue::String("10:20:12,15".into()), oid).unwrap(),
                snapshot
            );
            assert_eq!(
                decode_binary_parameter(&snapshot, oid).unwrap(),
                "'10:20:12,15'"
            );
        }
    }

    #[test]
    fn transaction_system_type_binary_codecs_match_postgresql_layouts() {
        let max_xid = u32::MAX.to_be_bytes();
        for oid in [28, 29] {
            assert_eq!(
                encode_binary_result_value(&SqlValue::String(u32::MAX.to_string()), oid,).unwrap(),
                max_xid
            );
            assert_eq!(
                decode_binary_parameter(&max_xid, oid).unwrap(),
                quote_sql_string(&u32::MAX.to_string())
            );
        }

        let max_xid8 = u64::MAX.to_be_bytes();
        assert_eq!(
            encode_binary_result_value(&SqlValue::String(u64::MAX.to_string()), 5069,).unwrap(),
            max_xid8
        );
        assert_eq!(
            decode_binary_parameter(&max_xid8, 5069).unwrap(),
            quote_sql_string(&u64::MAX.to_string())
        );

        let max_tid = [
            u32::MAX.to_be_bytes().as_slice(),
            u16::MAX.to_be_bytes().as_slice(),
        ]
        .concat();
        assert_eq!(
            encode_binary_result_value(&SqlValue::String("(4294967295,65535)".into()), 27,)
                .unwrap(),
            max_tid
        );
        assert_eq!(
            decode_binary_parameter(&max_tid, 27).unwrap(),
            "'(4294967295,65535)'"
        );

        for (array_oid, element_oid, values, expected) in [
            (
                1010,
                27,
                json!(["(0,1)", "(4294967295,65535)"]),
                "'[1:2]={\"(0,1)\",\"(4294967295,65535)\"}'",
            ),
            (
                1011,
                28,
                json!(["0", "4294967295"]),
                "'[1:2]={\"0\",\"4294967295\"}'",
            ),
            (
                1012,
                29,
                json!(["0", "4294967295"]),
                "'[1:2]={\"0\",\"4294967295\"}'",
            ),
            (
                271,
                5069,
                json!(["0", "18446744073709551615"]),
                "'[1:2]={\"0\",\"18446744073709551615\"}'",
            ),
        ] {
            let binary = encode_binary_result_value(&SqlValue::Json(values), array_oid).unwrap();
            assert_eq!(i32::from_be_bytes(binary[0..4].try_into().unwrap()), 1);
            assert_eq!(
                i32::from_be_bytes(binary[8..12].try_into().unwrap()),
                element_oid
            );
            assert_eq!(i32::from_be_bytes(binary[12..16].try_into().unwrap()), 2);
            assert_eq!(i32::from_be_bytes(binary[16..20].try_into().unwrap()), 1);
            assert_eq!(
                decode_binary_parameter(&binary, array_oid).unwrap(),
                expected
            );
        }

        assert!(decode_binary_parameter(&[0; 5], 27).is_err());
        assert!(decode_binary_parameter(&[0; 3], 28).is_err());
        assert!(decode_binary_parameter(&[0; 3], 29).is_err());
        assert!(decode_binary_parameter(&[0; 7], 5069).is_err());
    }

    #[test]
    fn structured_binary_codecs_reject_malformed_payloads() {
        assert!(decode_binary_parameter(&[2, 24, 0], 869).is_err());
        assert!(decode_binary_parameter(&[4, 24, 0, 4, 192, 0, 2, 1], 869).is_err());
        assert!(decode_binary_parameter(&[2, 33, 0, 4, 192, 0, 2, 1], 869).is_err());
        assert!(decode_binary_parameter(&[2, 24, 1, 4, 192, 0, 2, 1], 650).is_err());
        assert!(decode_binary_parameter(&[0; 5], 829).is_err());
        assert!(decode_binary_parameter(&[0; 7], 774).is_err());
        assert!(decode_binary_parameter(&[0; 15], 600).is_err());
        assert!(decode_binary_parameter(&[0; 24], 628).is_err());
        let invalid_path = [vec![1], 0_i32.to_be_bytes().to_vec()].concat();
        assert!(decode_binary_parameter(&invalid_path, 602).is_err());
        let negative_circle = [0_f64, 0_f64, -1_f64]
            .iter()
            .flat_map(|value| value.to_be_bytes())
            .collect::<Vec<_>>();
        assert!(decode_binary_parameter(&negative_circle, 718).is_err());
        assert!(decode_binary_parameter(&[], 3904).is_err());
        assert!(decode_binary_parameter(&[0x80], 3904).is_err());
        assert!(decode_binary_parameter(&[RANGE_EMPTY | RANGE_LB_INC], 3904).is_err());
        assert!(decode_binary_parameter(&(-1_i32).to_be_bytes(), 4451).is_err());
        assert!(decode_binary_parameter(&[0, 0, 0, 1, 0, 0, 0, 5, RANGE_EMPTY], 4451).is_err());
        assert!(decode_binary_parameter(&[0; 7], 3220).is_err());
        assert!(decode_binary_parameter(&[0; 3], 2203).is_err());
        assert!(decode_binary_parameter(&[0; 19], 5038).is_err());
        let invalid_snapshot = [
            0_i32.to_be_bytes().as_slice(),
            20_u64.to_be_bytes().as_slice(),
            10_u64.to_be_bytes().as_slice(),
        ]
        .concat();
        assert!(decode_binary_parameter(&invalid_snapshot, 5038).is_err());
    }

    #[test]
    fn count_driven_binary_decoders_reject_impossible_counts_before_allocation() {
        let huge = i32::MAX.to_be_bytes();

        let path = [vec![1], huge.to_vec()].concat();
        let error = decode_binary_parameter(&path, 602).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("geometric point count 2147483647 exceeds"),
            "{error}"
        );

        let error = decode_binary_parameter(&huge, 4451).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("multirange range count 2147483647 exceeds"),
            "{error}"
        );

        let root = tempfile::tempdir().unwrap();
        let db = BicDb::open(root.path()).unwrap();
        for decode in [
            decode_binary_composite_parameter(&db, &huge, 2249),
            decode_binary_composite_array_element(&db, &huge, 2249),
        ] {
            let error = decode.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("binary composite field count 2147483647 exceeds"),
                "{error}"
            );
        }
    }

    #[test]
    fn multidimensional_binary_arrays_preserve_nulls_and_lower_bounds() {
        let payload = [
            2_i32.to_be_bytes().to_vec(),
            1_i32.to_be_bytes().to_vec(),
            23_i32.to_be_bytes().to_vec(),
            2_i32.to_be_bytes().to_vec(),
            0_i32.to_be_bytes().to_vec(),
            2_i32.to_be_bytes().to_vec(),
            5_i32.to_be_bytes().to_vec(),
            4_i32.to_be_bytes().to_vec(),
            1_i32.to_be_bytes().to_vec(),
            (-1_i32).to_be_bytes().to_vec(),
            4_i32.to_be_bytes().to_vec(),
            3_i32.to_be_bytes().to_vec(),
            4_i32.to_be_bytes().to_vec(),
            4_i32.to_be_bytes().to_vec(),
        ]
        .concat();
        assert_eq!(
            decode_binary_parameter(&payload, 1007).unwrap(),
            quote_sql_string(r#"[0:1][5:6]={{"1",NULL},{"3","4"}}"#)
        );
        let value = SqlValue::Json(json!({
            "$bicdb_array_input": {
                "lower_bounds": [0, 5],
                "value": [[1, null], [3, 4]],
            }
        }));
        assert_eq!(encode_binary_result_value(&value, 1007).unwrap(), payload);

        let empty = [
            0_i32.to_be_bytes().as_slice(),
            0_i32.to_be_bytes().as_slice(),
            23_i32.to_be_bytes().as_slice(),
        ]
        .concat();
        assert_eq!(decode_binary_parameter(&empty, 1007).unwrap(), "'{}'");
        assert_eq!(
            encode_binary_result_value(&SqlValue::Json(json!([])), 1007).unwrap(),
            empty
        );
    }

    #[test]
    fn binary_arrays_reject_invalid_headers_dimensions_and_shapes() {
        let header = |rank: i32, nulls: i32, element_oid: i32| {
            [
                rank.to_be_bytes().as_slice(),
                nulls.to_be_bytes().as_slice(),
                element_oid.to_be_bytes().as_slice(),
            ]
            .concat()
        };
        assert!(decode_binary_parameter(&header(-1, 0, 23), 1007).is_err());
        assert!(decode_binary_parameter(&header(7, 0, 23), 1007).is_err());
        assert!(decode_binary_parameter(&header(0, 2, 23), 1007).is_err());
        assert!(decode_binary_parameter(&header(0, 0, 25), 1007).is_err());
        let oversized = [
            header(1, 0, 23),
            i32::MAX.to_be_bytes().to_vec(),
            1_i32.to_be_bytes().to_vec(),
        ]
        .concat();
        assert!(decode_binary_parameter(&oversized, 1007).is_err());

        let null_without_flag = [
            header(1, 0, 23),
            1_i32.to_be_bytes().to_vec(),
            1_i32.to_be_bytes().to_vec(),
            (-1_i32).to_be_bytes().to_vec(),
        ]
        .concat();
        assert_eq!(
            decode_binary_parameter(&null_without_flag, 1007).unwrap(),
            quote_sql_string("[1:1]={NULL}"),
        );
        assert!(encode_binary_result_value(&SqlValue::Json(json!([[1, 2], [3]])), 1007).is_err());
        assert!(encode_binary_result_value(
            &SqlValue::Json(json!({
                "$bicdb_array_input": {
                    "lower_bounds": [0],
                    "value": [[1, 2], [3, 4]],
                }
            })),
            1007
        )
        .is_err());
    }

    #[test]
    fn numeric_binary_codec_matches_postgresql_base_10000_vectors() {
        let cases = [
            ("12.30", vec![0, 2, 0, 0, 0, 0, 0, 2, 0, 12, 11, 184]),
            (
                "9007199254740993.0100",
                vec![
                    0, 5, 0, 3, 0, 0, 0, 4, 35, 47, 7, 200, 21, 98, 3, 225, 0, 100,
                ],
            ),
            ("-0.000012", vec![0, 1, 255, 254, 64, 0, 0, 6, 4, 176]),
            ("0.00", vec![0, 0, 0, 0, 0, 0, 0, 2]),
            ("NaN", vec![0, 0, 0, 0, 192, 0, 0, 0]),
            ("Infinity", vec![0, 0, 0, 0, 208, 0, 0, 0]),
            ("-Infinity", vec![0, 0, 0, 0, 240, 0, 0, 0]),
        ];
        for (text, binary) in cases {
            let value = SqlValue::String(text.to_string());
            assert_eq!(
                encode_binary_numeric_result(&value).unwrap(),
                binary,
                "{text}"
            );
            assert_eq!(
                decode_binary_numeric_parameter(&binary).unwrap(),
                text,
                "{text}"
            );
        }
    }

    #[test]
    fn numeric_binary_codec_rejects_malformed_payloads() {
        for payload in [
            vec![],
            vec![0, 1, 0, 0, 0, 0, 0, 0],
            vec![0, 1, 0, 0, 0, 0, 0, 0, 39, 16],
            vec![0, 0, 0, 0, 128, 0, 0, 0],
            vec![0, 0, 0, 0, 0, 0, 64, 0],
        ] {
            assert!(
                decode_binary_numeric_parameter(&payload).is_err(),
                "{payload:?}"
            );
        }
    }

    #[test]
    fn temporal_binary_codecs_match_postgresql_epoch_and_field_order() {
        let cases = [
            (1082, 8_767_i32.to_be_bytes().to_vec(), "2024-01-02"),
            (
                1083,
                11_045_000_006_i64.to_be_bytes().to_vec(),
                "03:04:05.000006",
            ),
            (
                1114,
                42_i64.to_be_bytes().to_vec(),
                "2000-01-01 00:00:00.000042",
            ),
            (
                1184,
                43_i64.to_be_bytes().to_vec(),
                "2000-01-01 00:00:00.000043+00",
            ),
            (
                1186,
                [
                    3_i64.to_be_bytes().as_slice(),
                    2_i32.to_be_bytes().as_slice(),
                    1_i32.to_be_bytes().as_slice(),
                ]
                .concat(),
                "1 mon 2 days 00:00:00.000003",
            ),
            (
                1266,
                [
                    11_045_000_000_i64.to_be_bytes().as_slice(),
                    (-7_200_i32).to_be_bytes().as_slice(),
                ]
                .concat(),
                "03:04:05+02",
            ),
        ];
        for (oid, binary, text) in cases {
            assert_eq!(
                decode_binary_parameter(&binary, oid).unwrap(),
                quote_sql_string(text)
            );
            assert_eq!(
                encode_binary_result_value(&SqlValue::String(text.to_string()), oid).unwrap(),
                binary
            );
        }
        for (oid, positive, negative) in [
            (
                1082,
                i32::MAX.to_be_bytes().to_vec(),
                i32::MIN.to_be_bytes().to_vec(),
            ),
            (
                1114,
                i64::MAX.to_be_bytes().to_vec(),
                i64::MIN.to_be_bytes().to_vec(),
            ),
            (
                1184,
                i64::MAX.to_be_bytes().to_vec(),
                i64::MIN.to_be_bytes().to_vec(),
            ),
        ] {
            assert_eq!(
                decode_binary_parameter(&positive, oid).unwrap(),
                "'infinity'"
            );
            assert_eq!(
                decode_binary_parameter(&negative, oid).unwrap(),
                "'-infinity'"
            );
            assert_eq!(
                encode_binary_result_value(&SqlValue::String("infinity".into()), oid).unwrap(),
                positive
            );
            assert_eq!(
                encode_binary_result_value(&SqlValue::String("-infinity".into()), oid).unwrap(),
                negative
            );
        }
    }

    #[test]
    fn temporal_binary_codecs_reject_invalid_lengths_times_and_zones() {
        assert!(decode_binary_parameter(&[0; 3], 1082).is_err());
        assert!(decode_binary_parameter(&86_400_000_001_i64.to_be_bytes(), 1083).is_err());
        let invalid_zone = [
            0_i64.to_be_bytes().as_slice(),
            57_600_i32.to_be_bytes().as_slice(),
        ]
        .concat();
        assert!(decode_binary_parameter(&invalid_zone, 1266).is_err());
        assert!(decode_binary_parameter(&[0; 15], 1186).is_err());
    }

    #[test]
    fn substitute_parameters_ignores_sql_literals_and_comments() {
        let sql = "SELECT $1, ?, '$2 ?', \"$3 ?\", $$ $4 ? $$, $tag$ $5 ? $tag$, /* $6 ? */ -- $7 ?\n $8, ?";
        let params = ["10", "20", "30", "40", "50", "60", "70", "80"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();

        assert_eq!(
            substitute_parameters(sql, &params).unwrap(),
            "SELECT 10, 10, '$2 ?', \"$3 ?\", $$ $4 ? $$, $tag$ $5 ? $tag$, /* $6 ? */ -- $7 ?\n 80, 20"
        );
    }

    #[test]
    fn numbered_parameters_preserve_jsonb_exists_operators() {
        let params = [r#"'{"roles":["admin"]}'"#.to_string()];

        assert_eq!(
            substitute_parameters("SELECT $1::jsonb ? 'roles'", &params).unwrap(),
            r#"SELECT '{"roles":["admin"]}'::jsonb ? 'roles'"#
        );
        assert_eq!(
            substitute_parameters("SELECT '$1', ?", &["42".to_string()]).unwrap(),
            "SELECT '$1', 42"
        );
        assert_eq!(
            substitute_parameters("SELECT '{}'::jsonb ? 'roles'", &[]).unwrap(),
            "SELECT '{}'::jsonb ? 'roles'"
        );
        assert_eq!(
            substitute_parameters("SELECT $1::jsonb ?| ARRAY['roles', 'groups']", &params).unwrap(),
            r#"SELECT '{"roles":["admin"]}'::jsonb ?| ARRAY['roles', 'groups']"#
        );
    }

    #[test]
    fn positional_parameters_after_distinct_and_case_are_not_json_operators() {
        let params = ["10", "20", "30", "40", "true", "60"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let sql = "SELECT DISTINCT ?, \
                   CASE ? WHEN 1 THEN ? ELSE ? END, \
                   CASE WHEN ? THEN payload ? 'active' ELSE ? END \
                   FROM documents";

        assert_eq!(
            substitute_parameters(sql, &params).unwrap(),
            "SELECT DISTINCT 10, \
             CASE 20 WHEN 1 THEN 30 ELSE 40 END, \
             CASE WHEN true THEN payload ? 'active' ELSE 60 END \
             FROM documents"
        );
    }

    #[test]
    fn positional_parameters_mix_with_json_operators_by_expression_context() {
        let params = [r#"'{"active":true}'"#, "'active'", "42"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let sql = "SELECT ?::jsonb ? ?, \
                   DISTINCT_VALUE ? 'active', \
                   CASE WHEN payload ? 'active' THEN ? ELSE 0 END";

        assert_eq!(
            substitute_parameters(sql, &params).unwrap(),
            r#"SELECT '{"active":true}'::jsonb ? 'active', DISTINCT_VALUE ? 'active', CASE WHEN payload ? 'active' THEN 42 ELSE 0 END"#
        );
        assert_eq!(
            substitute_parameters("SELECT ? ? 'active'", &[r#"'{"active":true}'"#.to_string()])
                .unwrap(),
            r#"SELECT '{"active":true}' ? 'active'"#
        );
    }

    #[test]
    fn positional_projection_after_distinct_on_is_not_a_json_operator() {
        let sql = "SELECT DISTINCT ON (tenant_id) /* projection ? */ ?, \
                   payload ? 'active' FROM documents ORDER BY tenant_id";

        assert_eq!(
            substitute_parameters(sql, &["42".to_string()]).unwrap(),
            "SELECT DISTINCT ON (tenant_id) /* projection ? */ 42, \
             payload ? 'active' FROM documents ORDER BY tenant_id"
        );
    }

    #[test]
    fn positional_parameters_after_between_and_fetch_bounds_are_not_json_operators() {
        assert_eq!(
            substitute_parameters(
                "SELECT * FROM events WHERE sequence BETWEEN ? AND ? FETCH FIRST ? ROWS ONLY",
                &["10".to_string(), "20".to_string(), "5".to_string()],
            )
            .unwrap(),
            "SELECT * FROM events WHERE sequence BETWEEN 10 AND 20 FETCH FIRST 5 ROWS ONLY"
        );
        assert_eq!(
            substitute_parameters(
                "SELECT * FROM events FETCH NEXT ? ROWS ONLY",
                &["7".to_string()],
            )
            .unwrap(),
            "SELECT * FROM events FETCH NEXT 7 ROWS ONLY"
        );
    }

    #[test]
    fn prepare_literal_dollar_argument_is_not_substituted() {
        let sql = "SELECT arbitrary_routine($1, $2, '$3', '0', 0)";
        let params = ["1", "42", "'literal'"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();

        assert_eq!(
            substitute_parameters(sql, &params).unwrap(),
            "SELECT arbitrary_routine(1, 42, '$3', '0', 0)"
        );
    }

    #[test]
    fn stats_snapshot_reads_database_size_without_taking_db_lock() {
        let dir = tempfile::tempdir().unwrap();
        let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
        {
            let mut db = server.db.write();
            db.create_collection("arbitrary_metrics_rows").unwrap();
            db.insert(
                "arbitrary_metrics_rows",
                bicdb_core::Record::new("row-1").with_metadata(json!({"payload": "value"})),
            )
            .unwrap();
        }
        let expected_size = database_directory_size(dir.path()).unwrap();
        assert!(expected_size > 0);

        let _db_guard = server.db.write();
        let snapshot = server.stats_snapshot();

        assert_eq!(snapshot.db_size_bytes, expected_size);
    }

    #[test]
    fn connection_state_reuses_catalog_cache_across_short_lived_sql_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
        {
            let mut db = server.db.write();
            let mut setup = sql_session_for_server(&server, &mut db);
            setup.execute("CREATE ROLE arbitrary_user LOGIN").unwrap();
            setup
                .execute(
                    "CREATE TABLE arbitrary_pgwire_catalog_cache_rows (
                        row_key INT PRIMARY KEY,
                        payload_value INT
                    )",
                )
                .unwrap();
            setup
                .execute(
                    "INSERT INTO arbitrary_pgwire_catalog_cache_rows (row_key, payload_value)
                     VALUES (9, 31)",
                )
                .unwrap();
            setup
                .execute(
                    "CREATE PROCEDURE arbitrary_pgwire_catalog_cache_probe(
                        selected_key IN INTEGER,
                        observed_value INOUT INTEGER
                    )
                    AS $$
                    BEGIN
                        SELECT payload_value
                        INTO observed_value
                        FROM arbitrary_pgwire_catalog_cache_rows
                        WHERE row_key = selected_key;
                    END;
                    $$
                    LANGUAGE 'plpgsql'",
                )
                .unwrap();
            setup
                .execute(
                    "GRANT EXECUTE ON FUNCTION arbitrary_pgwire_catalog_cache_probe(INTEGER, INTEGER) TO arbitrary_user",
                )
                .unwrap();
            setup
                .execute(
                    "GRANT SELECT ON TABLE arbitrary_pgwire_catalog_cache_rows TO arbitrary_user",
                )
                .unwrap();
        }

        let mut state = ConnectionState::new(42, "arbitrary_user".to_string(), None);
        let cancellation = CancellationToken::uncancelable();
        let first = execute_server_db_sql_for_state(
            &server,
            &mut state,
            "CALL arbitrary_pgwire_catalog_cache_probe(9, 0)",
            &normalize_executable_sql("CALL arbitrary_pgwire_catalog_cache_probe(9, 0)"),
            &cancellation,
        )
        .unwrap();
        let first_cache_len = state.catalog_cache.schema_cache_len();

        let second = execute_server_db_sql_for_state(
            &server,
            &mut state,
            "CALL arbitrary_pgwire_catalog_cache_probe(9, 0)",
            &normalize_executable_sql("CALL arbitrary_pgwire_catalog_cache_probe(9, 0)"),
            &cancellation,
        )
        .unwrap();

        assert_eq!(first.rows, vec![vec![SqlValue::Int(31)]]);
        assert_eq!(second.rows, vec![vec![SqlValue::Int(31)]]);
        assert!(
            first_cache_len > 0,
            "first CALL should populate the connection catalog cache"
        );
        assert_eq!(
            state.catalog_cache.schema_cache_len(),
            first_cache_len,
            "short-lived pgwire SQL sessions should preserve connection catalog cache entries"
        );
    }

    #[test]
    fn select_calling_a_writing_function_retries_with_exclusive_access() {
        let dir = tempfile::tempdir().unwrap();
        let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
        {
            let mut db = server.db.write();
            let mut setup = sql_session_for_server(&server, &mut db);
            setup
                .execute(
                    "CREATE TABLE pgwire_function_writes (
                        row_key INT PRIMARY KEY,
                        payload_value TEXT NOT NULL
                    )",
                )
                .unwrap();
            setup
                .execute(
                    r#"
CREATE FUNCTION record_pgwire_function_write(selected_key INT, selected_value TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO pgwire_function_writes (row_key, payload_value)
  VALUES (selected_key, selected_value);
END
$$
"#,
                )
                .unwrap();
        }

        let mut state = ConnectionState::new(42, "bicdb".to_string(), None);
        execute_server_sql_for_describe_with_session(
            &server,
            "SELECT record_pgwire_function_write(7, 'written')",
            &state.session_state,
            state.security_context.as_ref(),
        )
        .unwrap();
        let before_execute = execute_server_db_sql_for_state(
            &server,
            &mut state,
            "SELECT COUNT(*) FROM pgwire_function_writes",
            &normalize_executable_sql("SELECT COUNT(*) FROM pgwire_function_writes"),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
        assert_eq!(before_execute.rows, vec![vec![SqlValue::Int(0)]]);

        execute_server_db_sql_for_state(
            &server,
            &mut state,
            "SELECT record_pgwire_function_write(7, 'written')",
            &normalize_executable_sql("SELECT record_pgwire_function_write(7, 'written')"),
            &CancellationToken::uncancelable(),
        )
        .unwrap();

        let result = execute_server_db_sql_for_state(
            &server,
            &mut state,
            "SELECT payload_value FROM pgwire_function_writes WHERE row_key = 7",
            &normalize_executable_sql(
                "SELECT payload_value FROM pgwire_function_writes WHERE row_key = 7",
            ),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![SqlValue::String("written".to_string())]]
        );
    }

    #[test]
    fn unregistered_logical_types_never_fall_back_to_text_oid() {
        assert_eq!(oid_for_type_name("text"), Some(25));
        assert_eq!(oid_for_type_name("definitely_not_a_pg_type"), None);
        assert!(require_type_oid("definitely_not_a_pg_type").is_err());

        let result = SqlResult::new(
            vec!["value".to_string()],
            vec![vec![SqlValue::String("payload".to_string())]],
        )
        .with_column_types(vec![Some("definitely_not_a_pg_type".to_string())]);
        assert!(result_column_types(&result, None).is_err());

        let root = tempfile::tempdir().unwrap();
        let db = BicDb::open(root.path()).unwrap();
        let composite = SqlValue::Composite(bicdb_sql::SqlComposite::anonymous(vec![(
            "definitely_not_a_pg_type".to_string(),
            SqlValue::String("payload".to_string()),
        )]));
        let error = encode_binary_composite_result(&db, &composite, 2249).unwrap_err();
        assert!(error
            .to_string()
            .contains("definitely_not_a_pg_type\" is not registered"));
    }

    #[test]
    fn planner_result_types_are_authoritative_over_sql_text_inference() {
        let result = SqlResult::new(
            vec!["value".to_string()],
            vec![vec![SqlValue::String("planner".to_string())]],
        )
        .with_column_types(vec![Some("text".to_string())]);
        assert_eq!(
            result_column_types(&result, Some("SELECT 1::int4")).unwrap(),
            vec![25]
        );
    }

    #[test]
    fn describe_uses_schema_types_when_parameterized_query_is_empty() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        SqlSession::new(&mut db)
            .execute(
                "CREATE TABLE describe_schema_probe (
                    id OID PRIMARY KEY,
                    enabled BOOL,
                    options INT2[],
                    labels TEXT[]
                )",
            )
            .unwrap();
        let result = SqlResult::empty(vec![
            "id".to_string(),
            "enabled".to_string(),
            "options".to_string(),
            "labels".to_string(),
        ]);
        assert_eq!(
            result_column_types_with_db(
                &db,
                &result,
                Some(
                    "SELECT id, enabled, options, labels
                     FROM describe_schema_probe WHERE id = $1",
                ),
            )
            .unwrap(),
            vec![26, 16, 1005, 1009]
        );
    }

    #[test]
    fn catalog_vectors_use_native_oids_and_array_binary_payloads() {
        assert_eq!(require_type_oid("int2vector").unwrap(), 22);
        assert_eq!(require_type_oid("oidvector").unwrap(), 30);

        let root = tempfile::tempdir().unwrap();
        let db = BicDb::open(root.path()).unwrap();
        let encoded =
            encode_binary_result_value_with_db(&db, &SqlValue::String("1 2".to_string()), 22)
                .unwrap();
        assert_eq!(i32::from_be_bytes(encoded[0..4].try_into().unwrap()), 1);
        assert_eq!(i32::from_be_bytes(encoded[8..12].try_into().unwrap()), 21);
        assert_eq!(i32::from_be_bytes(encoded[12..16].try_into().unwrap()), 2);
        assert_eq!(i32::from_be_bytes(encoded[16..20].try_into().unwrap()), 0);
    }

    #[test]
    fn catalog_reference_aliases_use_names_in_text_and_oids_in_binary() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let oid = SqlSession::new(&mut db)
            .execute(
                "CREATE TABLE wire_alias_target (id int4 PRIMARY KEY);
                 SELECT 'wire_alias_target'::regclass",
            )
            .unwrap()
            .rows[0][0]
            .clone();
        let SqlValue::Int(oid_number) = oid else {
            panic!("regclass should be represented as an OID");
        };

        assert_eq!(
            encode_result_value_with_db(&db, &SqlValue::Int(oid_number), 2205, 0).unwrap(),
            b"wire_alias_target"
        );
        assert_eq!(
            encode_result_value_with_db(&db, &SqlValue::Int(oid_number), 2205, 1).unwrap(),
            u32::try_from(oid_number).unwrap().to_be_bytes()
        );

        let array = SqlValue::Json(serde_json::json!([oid_number]));
        assert_eq!(
            encode_result_value_with_db(&db, &array, 2210, 0).unwrap(),
            b"{wire_alias_target}"
        );
        let binary = encode_result_value_with_db(&db, &array, 2210, 1).unwrap();
        assert_eq!(i32::from_be_bytes(binary[0..4].try_into().unwrap()), 1);
        assert_eq!(i32::from_be_bytes(binary[8..12].try_into().unwrap()), 2205);
        assert_eq!(
            u32::from_be_bytes(binary[24..28].try_into().unwrap()),
            u32::try_from(oid_number).unwrap()
        );
    }

    #[test]
    fn row_description_encodes_origin_type_size_and_typmod() {
        let result = SqlResult::new(
            vec!["amount".to_string()],
            vec![vec![SqlValue::String("12.34".to_string())]],
        )
        .with_column_types(vec![Some("numeric".to_string())])
        .with_column_metadata(vec![bicdb_sql::SqlColumnMetadata {
            table_oid: 42,
            attribute_number: 2,
            type_modifier: ((10_i32 << 16) | 2) + 4,
        }]);

        let payload = row_description_payload(&result, &[1700], &[0]).unwrap();
        let mut expected = Vec::new();
        put_i16(&mut expected, 1);
        cstr(&mut expected, "amount");
        put_i32(&mut expected, 42);
        put_i16(&mut expected, 2);
        put_i32(&mut expected, 1700);
        put_i16(&mut expected, -1);
        put_i32(&mut expected, ((10_i32 << 16) | 2) + 4);
        put_i16(&mut expected, 0);

        assert_eq!(payload, expected);
    }
}
