//! Operator-authorized, transaction-scoped identity delegation for application pools.
use super::*;

const POLICY_FILE: &str = "server_delegation.json";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationPolicy {
    pub signing_key: String,
    /// Explicit tenant allowlist. A single `*` authorizes all tenants.
    pub tenants: Vec<String>,
}

/// Operator API; never exposed through SQL or ordinary login credentials.
/// Concurrent policy updates are serialized with an operator lock file.
pub fn set_user_delegation_policy(
    path: impl AsRef<Path>,
    username: &str,
    policy: Option<DelegationPolicy>,
) -> Result<()> {
    let path = path.as_ref();
    validate_username(username)?;
    let _catalog_lock = lock_user_catalog(path)?;
    if !user_exists(path, username)? {
        return Err(denied("delegation login does not exist"));
    }
    if let Some(policy) = &policy {
        if policy.signing_key.len() < 32
            || policy.tenants.is_empty()
            || policy.tenants.iter().any(|tenant| {
                tenant.trim().is_empty() || tenant.len() > 1024 || tenant.contains('\0')
            })
        {
            return Err(denied(
                "delegation requires a 32-byte signing key and explicit tenant authority",
            ));
        }
    }
    write_policy_locked(path, username, policy)
}

// Caller holds the user catalog lock; lock ordering is always users then policies.
pub(crate) fn write_policy_locked(
    path: &Path,
    username: &str,
    policy: Option<DelegationPolicy>,
) -> Result<()> {
    if policy.is_none() && !path.join(POLICY_FILE).exists() {
        return Ok(());
    }
    let mut lock_options = std::fs::OpenOptions::new();
    lock_options
        .read(true)
        .write(true)
        .create(true)
        .truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        lock_options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let policy_lock = lock_options.open(path.join(".server_delegation.lock"))?;
    policy_lock.lock()?;
    let mut policies = load_policies(path)?;
    if let Some(policy) = policy {
        policies.insert(username.to_owned(), policy);
    } else {
        policies.remove(username);
    }
    let temporary = path.join(format!(".{POLICY_FILE}.{}.tmp", Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    let bytes = serde_json::to_vec(&policies).map_err(|_| denied("invalid delegation policy"))?;
    if let Err(error) = file
        .write_all(&bytes)
        .and_then(|_| file.sync_all())
        .and_then(|_| std::fs::rename(&temporary, path.join(POLICY_FILE)))
    {
        let _ = std::fs::remove_file(&temporary);
        return Err(error.into());
    }
    #[cfg(unix)]
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

fn load_policies(path: &Path) -> Result<HashMap<String, DelegationPolicy>> {
    match std::fs::read(path.join(POLICY_FILE)) {
        Ok(bytes) => {
            serde_json::from_slice(&bytes).map_err(|_| denied("invalid delegation policy file"))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(error) => Err(error.into()),
    }
}

fn denied(message: &str) -> PgWireError {
    SqlError::RaisedException {
        sqlstate: "42501".into(),
        message: message.into(),
        detail: None,
    }
    .into()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Claims {
    version: u32,
    audience: String,
    challenge: String,
    issued_at: i64,
    expires_at: i64,
    user_id: String,
    tenant_id: String,
    client_id: Option<String>,
    workspace_id: Option<String>,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(default)]
    scopes: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct DelegatedIdentity {
    original: Option<SecurityContext>,
    expires_at: i64,
    policy_fingerprint: Vec<u8>,
}

fn fingerprint(policy: &DelegationPolicy) -> Vec<u8> {
    use sha2::Digest;
    Sha256::digest(serde_json::to_vec(policy).expect("serializable policy")).to_vec()
}

fn policy_for(server: &PgWireServer, state: &ConnectionState) -> Result<DelegationPolicy> {
    if !server.config.require_auth {
        return Err(denied(
            "delegation requires an authenticated application login",
        ));
    }
    load_policies(&server.auth_path)?
        .remove(&state.user)
        .ok_or_else(|| denied("application login is not authorized to delegate"))
}

pub(crate) fn validate_delegation(server: &PgWireServer, state: &ConnectionState) -> Result<()> {
    if let Some(delegation) = &state.delegated_identity {
        if unix_timestamp() >= delegation.expires_at
            || fingerprint(&policy_for(server, state)?) != delegation.policy_fingerprint
        {
            return Err(denied("transaction delegation expired or was revoked"));
        }
    }
    Ok(())
}

fn invalidate_portals(server: &PgWireServer, state: &mut ConnectionState) {
    for (_, portal) in state.portals.drain() {
        if portal.stream.is_some() {
            server.unregister_cursor_memory(portal.registered_stream_memory);
        }
    }
}

pub(crate) fn clear_delegation(server: &PgWireServer, state: &mut ConnectionState) {
    if let Some(delegation) = state.delegated_identity.take() {
        state.security_context = delegation.original;
        invalidate_portals(server, state);
    }
    state.delegation_challenge = None;
    // Catalog caches can contain plans prepared under the previous identity.
    state.catalog_cache = SqlSessionCatalogCache::default();
}

// Only direct, standalone calls are host operations. Expressions, routines and
// user-defined functions cannot smuggle an identity change into another query.
fn call(sql: &str, describe: bool) -> Result<Option<(String, Vec<String>)>> {
    if !sql.to_ascii_lowercase().contains("bicdb_delegat") {
        return Ok(None);
    }
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|_| denied("invalid delegation call"))?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return Ok(None);
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if !select.from.is_empty() || select.projection.len() != 1 || select.selection.is_some() {
        return Ok(None);
    }
    let SelectItem::UnnamedExpr(Expr::Function(function)) = &select.projection[0] else {
        return Ok(None);
    };
    let name = function.name.to_string().to_ascii_lowercase();
    if !matches!(
        name.as_str(),
        "bicdb_delegation_challenge" | "bicdb_delegate"
    ) {
        return Ok(None);
    }
    let FunctionArguments::List(args) = &function.args else {
        return Err(denied("invalid delegation arguments"));
    };
    // Compare the normalized AST rendering to the complete allowed statement.
    // Modifiers such as LIMIT 0, FILTER, WITH or INTO must never be ignored.
    let plain = format!(
        "SELECT {}({})",
        function.name,
        args.args
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );
    if statements[0].to_string() != plain
        || args.args.len() != if name == "bicdb_delegate" { 2 } else { 0 }
    {
        return Err(denied("delegation requires a standalone unmodified SELECT"));
    }
    let mut values = Vec::new();
    for arg in &args.args {
        if describe {
            values.push(String::new());
            continue;
        }
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) = arg else {
            return Err(denied("delegation arguments must be text literals"));
        };
        let sqlparser::ast::Value::SingleQuotedString(value) = &value.value else {
            return Err(denied("delegation arguments must be text literals"));
        };
        values.push(value.clone());
    }
    Ok(Some((name, values)))
}

pub(crate) fn delegation_description(sql: &str) -> Result<Option<SqlResult>> {
    let Some((name, _)) = call(sql, true)? else {
        return Ok(None);
    };
    Ok(Some(
        SqlResult::empty(vec![name]).with_column_types(vec![Some("text".to_owned())]),
    ))
}

pub(crate) fn execute_delegation(
    server: &PgWireServer,
    state: &mut ConnectionState,
    sql: &str,
) -> Result<Option<SqlResult>> {
    let Some((name, args)) = call(sql, false)? else {
        return Ok(None);
    };
    if !state.in_transaction {
        return Err(denied("delegation requires BEGIN"));
    }
    let policy = policy_for(server, state)?;
    let value = if name == "bicdb_delegation_challenge" {
        if !args.is_empty() || state.delegated_identity.is_some() {
            return Err(denied(
                "delegation challenge is only available before delegation",
            ));
        }
        let challenge = state
            .delegation_challenge
            .get_or_insert_with(|| Uuid::new_v4().to_string());
        json!({"version": 1, "audience": state.user, "challenge": challenge}).to_string()
    } else {
        if args.len() != 2
            || args[0].len() > 16384
            || state.delegated_identity.is_some()
            || state.tx.as_ref().is_some_and(|tx| tx.write_len() > 0)
            || !state.tx_ddl_undo.is_empty()
            || !state.savepoints.is_empty()
        {
            return Err(denied("invalid or repeated transaction delegation"));
        }
        let signature =
            hex::decode(&args[1]).map_err(|_| denied("invalid delegation signature"))?;
        let mut mac = Hmac::<Sha256>::new_from_slice(policy.signing_key.as_bytes())
            .map_err(|_| denied("invalid delegation key"))?;
        mac.update(args[0].as_bytes());
        mac.verify_slice(&signature)
            .map_err(|_| denied("invalid delegation signature"))?;
        let claims: Claims =
            serde_json::from_str(&args[0]).map_err(|_| denied("invalid delegation claims"))?;
        let now = unix_timestamp();
        if claims.version != 1
            || claims.audience != state.user
            || state.delegation_challenge.as_deref() != Some(claims.challenge.as_str())
            || claims.issued_at > now.saturating_add(5)
            || claims.issued_at.saturating_add(30) < now
            || claims.expires_at <= now
            || claims.expires_at > now.saturating_add(60)
            || claims.expires_at <= claims.issued_at
            || claims.expires_at.saturating_sub(claims.issued_at) > 60
            || claims.roles.iter().chain(&claims.scopes).any(|value| {
                value.is_empty()
                    || value.trim() != value
                    || value.contains([',', '\0'])
                    || value.len() > 256
            })
            || [
                Some(claims.user_id.as_str()),
                Some(claims.tenant_id.as_str()),
                claims.client_id.as_deref(),
                claims.workspace_id.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|value| value.trim().is_empty() || value.contains('\0') || value.len() > 1024)
            || claims.user_id.trim().is_empty()
            || claims.tenant_id.trim().is_empty()
            || !policy
                .tenants
                .iter()
                .any(|tenant| tenant == "*" || tenant == &claims.tenant_id)
        {
            return Err(denied(
                "delegation claims are expired or outside authorized scope",
            ));
        }
        let mut identity = SecurityContext::authenticated(
            claims.user_id,
            claims.tenant_id,
            AuthenticationStrength::Jwt,
        )
        .with_roles(claims.roles)
        .with_scopes(claims.scopes)
        .with_authenticated_session(
            format!("delegated:{}", claims.challenge),
            AuthenticationStrength::Jwt,
        );
        identity.client_id = claims.client_id;
        identity.workspace_id = claims.workspace_id;
        // A portal may already contain rows materialized under the login's
        // original identity. It cannot survive the switch to the delegate.
        invalidate_portals(server, state);
        let original = state.security_context.replace(identity);
        state.delegated_identity = Some(DelegatedIdentity {
            original,
            expires_at: claims.expires_at,
            policy_fingerprint: fingerprint(&policy),
        });
        state.catalog_cache = SqlSessionCatalogCache::default();
        "ok".to_owned()
    };
    let mut result = SqlResult::empty(vec![name]).with_column_types(vec![Some("text".to_owned())]);
    result.rows.push(vec![SqlValue::String(value)]);
    result.command_tag = Some("SELECT 1".into());
    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    const KEY: &str = "delegation-test-key-with-at-least-32-bytes";

    fn fixture() -> (tempfile::TempDir, Arc<PgWireServer>, ConnectionState) {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open(dir.path()).unwrap();
            let mut setup = SqlSession::new(&mut db);
            for sql in [
                "CREATE TABLE delegated_rows (id TEXT PRIMARY KEY, org_id TEXT, body TEXT)",
                "INSERT INTO delegated_rows VALUES ('a', 'tenant-a', 'Alice'), ('b', 'tenant-b', 'Bob')",
                "CREATE INDEX delegated_body_search ON delegated_rows USING GIN (body gin_trgm_ops)",
                "ALTER TABLE delegated_rows ENABLE ROW LEVEL SECURITY",
                "ALTER TABLE delegated_rows FORCE ROW LEVEL SECURITY",
                "CREATE POLICY tenant_rows ON delegated_rows USING (org_id = current_setting('carrier.current_tenant', true)) WITH CHECK (org_id = current_setting('carrier.current_tenant', true))",
                "GRANT SELECT, INSERT, UPDATE, DELETE ON delegated_rows TO PUBLIC",
                "CREATE TABLE delegated_private_rows (id TEXT PRIMARY KEY, org_id TEXT, user_id TEXT)",
                "INSERT INTO delegated_private_rows VALUES ('alice-row', 'tenant-a', 'alice'), ('bob-row', 'tenant-a', 'bob')",
                "ALTER TABLE delegated_private_rows ENABLE ROW LEVEL SECURITY",
                "ALTER TABLE delegated_private_rows FORCE ROW LEVEL SECURITY",
                "CREATE POLICY user_rows ON delegated_private_rows USING (org_id = current_setting('carrier.current_tenant', true) AND user_id = current_setting('carrier.current_user', true)) WITH CHECK (org_id = current_setting('carrier.current_tenant', true) AND user_id = current_setting('carrier.current_user', true))",
                "GRANT SELECT, INSERT, UPDATE, DELETE ON delegated_private_rows TO PUBLIC",
            ] {
                setup.execute(sql).unwrap();
            }
        }
        create_user(dir.path(), "app", "test-password").unwrap();
        set_user_delegation_policy(
            dir.path(),
            "app",
            Some(DelegationPolicy {
                signing_key: KEY.into(),
                tenants: vec!["tenant-a".into(), "tenant-b".into()],
            }),
        )
        .unwrap();
        let server = PgWireServer::open(
            dir.path(),
            PgWireConfig {
                require_auth: true,
                ..Default::default()
            },
        )
        .unwrap();
        let context = connection_security_context(&server, "app", 1, None).unwrap();
        let state = ConnectionState::new_with_security_context(1, "app".into(), None, context);
        (dir, server, state)
    }

    fn envelope(
        server: &PgWireServer,
        state: &mut ConnectionState,
        user: &str,
        tenant: &str,
    ) -> JsonValue {
        execute_delegation(server, state, "SELECT bicdb_delegation_challenge()").unwrap();
        json!({"version":1,"audience":"app","challenge":state.delegation_challenge,
            "issued_at":unix_timestamp(),"expires_at":unix_timestamp()+30,
            "user_id":user,"tenant_id":tenant,"roles":["member"],"scopes":[]})
    }

    fn signed_call(claims: &JsonValue, key: &str) -> String {
        let payload = claims.to_string();
        let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).unwrap();
        mac.update(payload.as_bytes());
        format!(
            "SELECT bicdb_delegate('{}', '{}')",
            payload.replace('\'', "''"),
            hex::encode(mac.finalize().into_bytes())
        )
    }

    #[test]
    fn pooled_connection_changes_user_only_after_transaction_end() {
        let (_dir, server, mut state) = fixture();
        execute_server_sql(&server, &mut state, "BEGIN").unwrap();
        let alice = envelope(&server, &mut state, "alice", "tenant-a");
        let call = signed_call(&alice, KEY);
        execute_server_sql(&server, &mut state, &call).unwrap();
        let result =
            execute_server_sql(&server, &mut state, "SELECT current_trusted_tenant()").unwrap();
        assert_eq!(result.rows[0][0], SqlValue::String("tenant-a".into()));
        assert!(execute_delegation(&server, &mut state, &call).is_err());
        execute_server_sql(&server, &mut state, "COMMIT").unwrap();
        assert_eq!(state.security_context.as_ref().unwrap().user_id, "app");
        assert!(state.delegation_challenge.is_none());
        execute_server_sql(&server, &mut state, "BEGIN").unwrap();
        let bob = envelope(&server, &mut state, "bob", "tenant-b");
        assert!(
            execute_delegation(&server, &mut state, &call).is_err(),
            "old transaction signature must not replay"
        );
        execute_server_sql(&server, &mut state, &signed_call(&bob, KEY)).unwrap();
        assert_eq!(state.security_context.as_ref().unwrap().user_id, "bob");
        execute_server_sql(&server, &mut state, "ROLLBACK").unwrap();
        assert_eq!(state.security_context.as_ref().unwrap().user_id, "app");
    }

    #[test]
    fn delegation_rejects_wrong_signature_scope_expiry_and_connection() {
        let (_dir, server, mut state) = fixture();
        assert!(
            execute_delegation(&server, &mut state, "SELECT bicdb_delegation_challenge()").is_err()
        );
        execute_server_sql(&server, &mut state, "BEGIN").unwrap();
        let claims = envelope(&server, &mut state, "alice", "tenant-a");
        assert!(
            execute_delegation(&server, &mut state, &signed_call(&claims, "wrong key")).is_err()
        );
        for (field, value) in [
            ("tenant_id", json!("outside")),
            ("audience", json!("other-app")),
            ("expires_at", json!(unix_timestamp() - 1)),
            ("challenge", json!("another-connection")),
            ("version", json!(2)),
            ("user_id", json!("")),
            ("client_id", json!("bad\0client")),
            ("workspace_id", json!(" ")),
            ("roles", json!(["member,admin"])),
            ("scopes", json!([" admin "])),
            ("scopes", json!(["read\0write"])),
            ("bypass_rls", json!(true)),
        ] {
            let mut invalid = claims.clone();
            invalid[field] = value;
            assert!(
                execute_delegation(&server, &mut state, &signed_call(&invalid, KEY)).is_err(),
                "{field}"
            );
            assert_eq!(state.security_context.as_ref().unwrap().user_id, "app");
        }
    }

    #[test]
    fn delegation_rejects_modified_selects_without_changing_identity() {
        let (_dir, server, mut state) = fixture();
        execute_server_sql(&server, &mut state, "BEGIN").unwrap();
        let claims = envelope(&server, &mut state, "alice", "tenant-a");
        for suffix in [
            " LIMIT 0",
            " FILTER (WHERE false)",
            " INTO stolen_identity",
            " ORDER BY 1",
        ] {
            let sql = format!("{}{suffix}", signed_call(&claims, KEY));
            assert!(
                execute_delegation(&server, &mut state, &sql).is_err(),
                "{suffix}"
            );
            assert!(state.delegated_identity.is_none());
        }
    }

    #[test]
    fn identity_switch_and_transaction_end_discard_materialized_portals() {
        let (_dir, server, mut state) = fixture();
        execute_server_sql(&server, &mut state, "BEGIN").unwrap();
        let claims = envelope(&server, &mut state, "alice", "tenant-a");
        let portal = || Portal {
            sql: "SELECT body FROM delegated_rows".into(),
            source_sql: "SELECT body FROM delegated_rows".into(),
            result_formats: Vec::new(),
            stream: None,
            registered_stream_memory: 0,
            predescribed: Some(SqlResult::empty(vec!["body".into()])),
        };
        state.portals.insert("previous_identity".into(), portal());
        execute_server_sql(&server, &mut state, &signed_call(&claims, KEY)).unwrap();
        assert!(state.portals.is_empty());
        state.portals.insert("delegated_identity".into(), portal());
        execute_server_sql(&server, &mut state, "COMMIT").unwrap();
        assert!(state.portals.is_empty());
    }

    #[test]
    fn operator_policy_updates_are_private_and_do_not_lose_other_logins() {
        let (dir, _server, _state) = fixture();
        create_user(dir.path(), "second_app", "test-password").unwrap();
        std::thread::scope(|scope| {
            for login in ["app", "second_app"] {
                let path = dir.path();
                scope.spawn(move || {
                    for _ in 0..8 {
                        set_user_delegation_policy(
                            path,
                            login,
                            Some(DelegationPolicy {
                                signing_key: KEY.into(),
                                tenants: vec!["tenant-a".into()],
                            }),
                        )
                        .unwrap();
                    }
                });
            }
        });
        let policies = load_policies(dir.path()).unwrap();
        assert!(policies.contains_key("app") && policies.contains_key("second_app"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.path().join(POLICY_FILE))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        set_user_delegation_policy(dir.path(), "app", None).unwrap();
        let policies = load_policies(dir.path()).unwrap();
        assert!(!policies.contains_key("app") && policies.contains_key("second_app"));
    }

    #[test]
    fn delegated_rls_filters_reads_and_rejects_cross_tenant_writes() {
        let (_dir, server, mut state) = fixture();
        for (user, tenant, expected) in [("alice", "tenant-a", "Alice"), ("bob", "tenant-b", "Bob")]
        {
            execute_server_sql(&server, &mut state, "BEGIN").unwrap();
            let claims = envelope(&server, &mut state, user, tenant);
            execute_server_sql(&server, &mut state, &signed_call(&claims, KEY)).unwrap();
            let result =
                execute_server_sql(&server, &mut state, "SELECT body FROM delegated_rows").unwrap();
            assert_eq!(result.rows, vec![vec![SqlValue::String(expected.into())]]);
            let insert = format!("INSERT INTO delegated_rows VALUES ('{user}', '{tenant}', 'new')");
            execute_server_sql(&server, &mut state, &insert).unwrap();
            execute_server_sql(&server, &mut state, "COMMIT").unwrap();
        }
        execute_server_sql(&server, &mut state, "BEGIN").unwrap();
        let claims = envelope(&server, &mut state, "alice", "tenant-a");
        execute_server_sql(&server, &mut state, &signed_call(&claims, KEY)).unwrap();
        assert!(execute_server_sql(
            &server,
            &mut state,
            "INSERT INTO delegated_rows VALUES ('forbidden', 'tenant-b', 'wrong')"
        )
        .is_err());
        execute_server_sql(&server, &mut state, "ROLLBACK").unwrap();
        let result =
            execute_server_sql(&server, &mut state, "SELECT body FROM delegated_rows").unwrap();
        assert!(
            result.rows.is_empty(),
            "application login has no delegated tenant after rollback"
        );
    }

    #[test]
    fn trigram_candidates_keep_delegated_tenant_isolation() {
        let (_dir, server, mut state) = fixture();
        for (user, tenant, expected) in [("alice", "tenant-a", 0), ("bob", "tenant-b", 1)] {
            execute_server_sql(&server, &mut state, "BEGIN").unwrap();
            let claims = envelope(&server, &mut state, user, tenant);
            execute_server_sql(&server, &mut state, &signed_call(&claims, KEY)).unwrap();
            let result = execute_server_sql(
                &server,
                &mut state,
                "SELECT body FROM delegated_rows WHERE body ILIKE '%bob%'",
            )
            .unwrap();
            assert_eq!(result.rows.len(), expected);
            execute_server_sql(&server, &mut state, "COMMIT").unwrap();
        }
        assert!(execute_server_sql(
            &server,
            &mut state,
            "SELECT body FROM delegated_rows WHERE body ILIKE '%bob%'"
        )
        .unwrap()
        .rows
        .is_empty());
    }

    #[test]
    fn users_in_the_same_tenant_retain_distinct_row_identity() {
        let (_dir, server, mut state) = fixture();
        for (user, other) in [("alice", "bob"), ("bob", "alice")] {
            execute_server_sql(&server, &mut state, "BEGIN").unwrap();
            let claims = envelope(&server, &mut state, user, "tenant-a");
            execute_server_sql(&server, &mut state, &signed_call(&claims, KEY)).unwrap();
            let result = execute_server_sql(
                &server,
                &mut state,
                "SELECT user_id FROM delegated_private_rows",
            )
            .unwrap();
            assert_eq!(result.rows, vec![vec![SqlValue::String(user.into())]]);
            let forbidden = format!(
                "UPDATE delegated_private_rows SET user_id = '{other}' WHERE user_id = '{user}'"
            );
            assert!(execute_server_sql(&server, &mut state, &forbidden).is_err());
            execute_server_sql(&server, &mut state, "ROLLBACK").unwrap();
        }
    }

    #[test]
    fn transaction_end_variants_clear_identity_and_rotate_challenge() {
        let (_dir, server, mut state) = fixture();
        for ending in [
            "COMMIT WORK",
            "END",
            "END TRANSACTION",
            "ROLLBACK WORK",
            "ABORT",
            "COMMIT AND CHAIN",
            "ROLLBACK AND CHAIN",
        ] {
            if !state.in_transaction {
                execute_server_sql(&server, &mut state, "BEGIN").unwrap();
            }
            let claims = envelope(&server, &mut state, "alice", "tenant-a");
            execute_server_sql(&server, &mut state, &signed_call(&claims, KEY)).unwrap();
            execute_server_sql(&server, &mut state, ending)
                .unwrap_or_else(|error| panic!("{ending}: {error}"));
            assert_eq!(
                state.security_context.as_ref().unwrap().user_id,
                "app",
                "{ending}"
            );
            assert!(state.delegation_challenge.is_none(), "{ending}");
            assert_eq!(state.in_transaction, ending.ends_with("AND CHAIN"));
        }
        execute_server_sql(&server, &mut state, "ROLLBACK").unwrap();
    }

    #[test]
    fn savepoint_rollback_does_not_allow_identity_switching() {
        let (_dir, server, mut state) = fixture();
        execute_server_sql(&server, &mut state, "BEGIN").unwrap();
        let claims = envelope(&server, &mut state, "alice", "tenant-a");
        execute_server_sql(&server, &mut state, &signed_call(&claims, KEY)).unwrap();
        execute_server_sql(&server, &mut state, "SAVEPOINT before_query").unwrap();
        execute_server_sql(&server, &mut state, "SELECT body FROM delegated_rows").unwrap();
        execute_server_sql(&server, &mut state, "ROLLBACK TO SAVEPOINT before_query").unwrap();
        assert_eq!(state.security_context.as_ref().unwrap().user_id, "alice");
        assert!(execute_delegation(&server, &mut state, &signed_call(&claims, KEY)).is_err());
        execute_server_sql(&server, &mut state, "ROLLBACK").unwrap();
        assert_eq!(state.security_context.as_ref().unwrap().user_id, "app");
    }

    #[test]
    fn expiry_and_key_rotation_are_checked_before_commit() {
        let (dir, server, mut state) = fixture();
        for rotate in [false, true] {
            execute_server_sql(&server, &mut state, "BEGIN").unwrap();
            let claims = envelope(&server, &mut state, "alice", "tenant-a");
            execute_server_sql(&server, &mut state, &signed_call(&claims, KEY)).unwrap();
            execute_server_sql(
                &server,
                &mut state,
                "UPDATE delegated_rows SET body = 'must roll back' WHERE id = 'a'",
            )
            .unwrap();
            if rotate {
                set_user_delegation_policy(
                    dir.path(),
                    "app",
                    Some(DelegationPolicy {
                        signing_key: "rotated-delegation-key-with-at-least-32-bytes".into(),
                        tenants: vec!["tenant-a".into()],
                    }),
                )
                .unwrap();
            } else {
                state.delegated_identity.as_mut().unwrap().expires_at = unix_timestamp() - 1;
            }
            assert!(execute_server_sql(&server, &mut state, "COMMIT WORK").is_err());
            execute_server_sql(&server, &mut state, "ROLLBACK WORK").unwrap();
        }
        set_user_delegation_policy(
            dir.path(),
            "app",
            Some(DelegationPolicy {
                signing_key: KEY.into(),
                tenants: vec!["tenant-a".into()],
            }),
        )
        .unwrap();
        execute_server_sql(&server, &mut state, "BEGIN").unwrap();
        let claims = envelope(&server, &mut state, "alice", "tenant-a");
        execute_server_sql(&server, &mut state, &signed_call(&claims, KEY)).unwrap();
        let result = execute_server_sql(
            &server,
            &mut state,
            "SELECT body FROM delegated_rows WHERE id = 'a'",
        )
        .unwrap();
        assert_eq!(result.rows, vec![vec![SqlValue::String("Alice".into())]]);
    }

    #[test]
    fn describe_does_not_delegate_or_consume_challenge() {
        let (_dir, server, mut state) = fixture();
        execute_server_sql(&server, &mut state, "BEGIN").unwrap();
        let claims = envelope(&server, &mut state, "alice", "tenant-a");
        assert!(delegation_description("SELECT bicdb_delegate($1, $2)")
            .unwrap()
            .is_some());
        execute_server_sql_for_describe_with_session(
            &server,
            &signed_call(&claims, KEY),
            &state.session_state,
            state.security_context.as_ref(),
        )
        .unwrap();
        assert!(state.delegated_identity.is_none());
        execute_server_sql(&server, &mut state, &signed_call(&claims, KEY)).unwrap();
        assert_eq!(state.security_context.as_ref().unwrap().user_id, "alice");
    }

    #[tokio::test]
    async fn wire_prepared_delegation_and_pool_reuse_enforce_rls() {
        let (_dir, server, _state) = fixture();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let serving = server.clone();
        let thread = std::thread::spawn(move || serve_existing_listener(serving, listener));
        let (mut client, connection) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(address.port())
            .user("app")
            .password("test-password")
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap();
        let connection_task = tokio::spawn(async move {
            connection.await.unwrap();
        });
        let statement = client
            .prepare("SELECT bicdb_delegate($1, $2)")
            .await
            .unwrap();
        for (user, tenant, expected) in [("alice", "tenant-a", "Alice"), ("bob", "tenant-b", "Bob")]
        {
            let tx = client.transaction().await.unwrap();
            let row = tx
                .query_one("SELECT bicdb_delegation_challenge()", &[])
                .await
                .unwrap();
            let mut claims: JsonValue = serde_json::from_str(row.get::<_, &str>(0)).unwrap();
            claims["issued_at"] = json!(unix_timestamp());
            claims["expires_at"] = json!(unix_timestamp() + 30);
            claims["user_id"] = json!(user);
            claims["tenant_id"] = json!(tenant);
            let payload = claims.to_string();
            let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).unwrap();
            mac.update(payload.as_bytes());
            let signature = hex::encode(mac.finalize().into_bytes());
            tx.query_one(&statement, &[&payload, &signature])
                .await
                .unwrap();
            let rows = tx
                .query("SELECT body FROM delegated_rows", &[])
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get::<_, &str>(0), expected);
            tx.commit().await.unwrap();
            let rows = client
                .query("SELECT body FROM delegated_rows", &[])
                .await
                .unwrap();
            assert!(rows.is_empty(), "no identity may survive pool reuse");
        }
        drop(client);
        connection_task.await.unwrap();
        server.request_shutdown();
        thread.join().unwrap().unwrap();
    }

    #[test]
    fn revocation_rejects_next_query_but_allows_rollback() {
        let (dir, server, mut state) = fixture();
        execute_server_sql(&server, &mut state, "BEGIN").unwrap();
        let claims = envelope(&server, &mut state, "alice", "tenant-a");
        execute_server_sql(&server, &mut state, &signed_call(&claims, KEY)).unwrap();
        set_user_delegation_policy(dir.path(), "app", None).unwrap();
        assert!(execute_server_sql(&server, &mut state, "SELECT 1").is_err());
        assert!(execute_server_sql(&server, &mut state, "COMMIT").is_err());
        execute_server_sql(&server, &mut state, "ROLLBACK").unwrap();
        assert_eq!(state.security_context.as_ref().unwrap().user_id, "app");
    }
}

#[derive(Clone, Copy)]
pub(crate) enum TransactionEnd {
    Commit { chain: bool },
    Rollback { chain: bool },
}

pub(crate) fn transaction_end(sql: &str, normalized: &str) -> Result<Option<TransactionEnd>> {
    if !matches!(
        normalized.split_whitespace().next(),
        Some("commit" | "end" | "rollback" | "abort")
    ) {
        return Ok(None);
    }
    let rewritten;
    let parse_sql = if normalized.split_whitespace().next() == Some("abort") {
        rewritten = format!("rollback{}", &normalized[5..]);
        rewritten.as_str()
    } else {
        sql
    };
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, parse_sql)
        .map_err(|error| PgWireError::Sql(SqlError::InvalidSql(error.to_string())))?;
    Ok(match statements.as_slice() {
        [Statement::Commit { chain, .. }] => Some(TransactionEnd::Commit { chain: *chain }),
        [Statement::Rollback {
            chain,
            savepoint: None,
            ..
        }] => Some(TransactionEnd::Rollback { chain: *chain }),
        _ => None,
    })
}
