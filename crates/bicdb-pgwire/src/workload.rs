//! Admission and cooperative execution budgets keyed by authenticated identity.
use super::*;
use serde::{Deserialize, Serialize};
use sqlparser::{ast::visit_relations, dialect::PostgreSqlDialect, parser::Parser};
use std::ops::ControlFlow;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoleLimits {
    pub max_concurrent_queries: usize,
    /// Aggregate working-memory envelope, partitioned among concurrent queries.
    pub memory_bytes: usize,
    pub query_timeout_ms: u64,
    /// Conservative input-row cost, including multiplicative join fan-out.
    pub max_estimated_rows: u64,
    /// Apply the cost gate at this many active server queries (0: always).
    pub cost_gate_active_queries: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkloadConfig {
    pub roles: BTreeMap<String, RoleLimits>,
    pub tenants: BTreeMap<String, RoleLimits>,
    pub live_evaluation_timeout_ms: u64,
    pub live_max_changes_per_round: usize,
}
impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            roles: BTreeMap::new(),
            tenants: BTreeMap::new(),
            live_evaluation_timeout_ms: 1000,
            live_max_changes_per_round: 10_000,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Usage {
    pub active: usize,
    pub reserved_bytes: usize,
    pub cached_bytes: usize,
    pub queries: u64,
    pub rejected: u64,
    pub peak_query_bytes: usize,
}
#[derive(Debug, Default)]
pub(crate) struct WorkloadState {
    pub usage: Mutex<BTreeMap<String, Usage>>,
}

pub(crate) struct Permit {
    server: Arc<PgWireServer>,
    reservations: Vec<(String, usize)>,
    pub memory_bytes: usize,
    pub timeout: Duration,
}
impl Permit {
    pub fn observe(&self, peak: usize) {
        let mut usage = self.server.workload.usage.lock().unwrap();
        for (key, _) in &self.reservations {
            if let Some(usage) = usage.get_mut(key) {
                usage.peak_query_bytes = usage.peak_query_bytes.max(peak);
            }
        }
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut usage = self.server.workload.usage.lock().unwrap();
        for (key, bytes) in &self.reservations {
            if let Some(usage) = usage.get_mut(key) {
                usage.active -= 1;
                usage.reserved_bytes -= bytes;
            }
        }
    }
}

fn tighter(current: usize, proposed: usize) -> usize {
    if proposed == 0 {
        current
    } else if current == 0 {
        proposed
    } else {
        current.min(proposed)
    }
}

/// Cost estimation is deliberately conservative: it visits every referenced
/// relation and multiplies its cardinality. Unknown relations are left to SQL
/// name resolution; this gate is protection, not an authorization mechanism.
fn estimate(db: &BicDb, sql: &str) -> Result<u64> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|error| PgWireError::QueryRejected(format!("cannot estimate query: {error}")))?;
    let mut cost = 0u64;
    for statement in statements {
        let mut input = 1u64;
        let _: ControlFlow<()> = visit_relations(&statement, |relation| {
            let mut parts = relation
                .to_string()
                .split('.')
                .map(|p| p.trim_matches('"').to_string())
                .collect::<Vec<_>>();
            if parts.first().is_some_and(|part| part == "public") {
                parts.remove(0);
            }
            let name = parts.join(".");
            if let Ok(rows) = db.estimated_record_count(&name) {
                input = input.saturating_mul(rows.max(1) as u64);
            }
            ControlFlow::Continue(())
        });
        cost = cost.saturating_add(input);
    }
    Ok(cost)
}

pub(crate) fn admit(
    server: &Arc<PgWireServer>,
    user: &str,
    context: Option<&SecurityContext>,
    sql: &str,
) -> Result<Permit> {
    let normalized = normalize_executable_sql(sql);
    if matches!(
        delegation::transaction_end(sql, &normalized)?,
        Some(delegation::TransactionEnd::Rollback { .. })
    ) {
        return Ok(Permit {
            server: server.clone(),
            reservations: Vec::new(),
            memory_bytes: 0,
            timeout: Duration::ZERO,
        });
    }
    let mut limits = Vec::new();
    if let Some(limit) = server
        .config
        .workload
        .roles
        .get(user)
        .or_else(|| server.config.workload.roles.get("*"))
    {
        limits.push((format!("role:{user}"), limit));
    }
    if let Some(tenant) = context.map(|context| context.tenant_id.as_str()) {
        if let Some(limit) = server
            .config
            .workload
            .tenants
            .get(tenant)
            .or_else(|| server.config.workload.tenants.get("*"))
        {
            limits.push((format!("tenant:{tenant}"), limit));
        }
    }
    let mut permit = Permit {
        server: server.clone(),
        reservations: Vec::new(),
        memory_bytes: 0,
        timeout: server.config.query_timeout,
    };
    let mut estimated = None;
    for (key, limit) in &limits {
        if limit.max_estimated_rows > 0
            && server.active_queries.load(Ordering::Relaxed) >= limit.cost_gate_active_queries
        {
            let cost = match estimated {
                Some(cost) => cost,
                None => {
                    let db = server.read_db()?;
                    let cost = estimate(&db, sql)?;
                    estimated = Some(cost);
                    cost
                }
            };
            if cost > limit.max_estimated_rows {
                server
                    .workload
                    .usage
                    .lock()
                    .unwrap()
                    .entry(key.clone())
                    .or_default()
                    .rejected += 1;
                return Err(PgWireError::QueryRejected(format!(
                    "{key} estimated row cost {cost} exceeds {}",
                    limit.max_estimated_rows
                )));
            }
        }
    }
    {
        let mut usage = server.workload.usage.lock().unwrap();
        for (key, limit) in &limits {
            let current = usage.entry(key.clone()).or_default();
            if limit.memory_bytes > 0
                && current.reserved_bytes.saturating_add(current.cached_bytes) >= limit.memory_bytes
            {
                current.rejected += 1;
                return Err(PgWireError::QueryRejected(format!(
                    "{key} memory envelope is fully reserved"
                )));
            }
            if limit.max_concurrent_queries > 0 && current.active >= limit.max_concurrent_queries {
                current.rejected += 1;
                return Err(PgWireError::QueryRejected(format!(
                    "{key} concurrent query limit reached"
                )));
            }
        }
        for (key, limit) in limits {
            let slots = if limit.max_concurrent_queries == 0 {
                server.config.max_active_queries.max(1)
            } else {
                limit.max_concurrent_queries
            };
            let current = usage.entry(key.clone()).or_default();
            let bytes = if limit.memory_bytes == 0 {
                0
            } else {
                let available = limit
                    .memory_bytes
                    .saturating_sub(current.reserved_bytes)
                    .saturating_sub(current.cached_bytes);
                (limit.memory_bytes / slots).max(1).min(available)
            };
            // All policies were checked before mutating admission counters.
            // A zero remaining envelope was rejected in the first pass.

            permit.memory_bytes = tighter(permit.memory_bytes, bytes);
            let timeout = Duration::from_millis(limit.query_timeout_ms);
            if !timeout.is_zero() && (permit.timeout.is_zero() || timeout < permit.timeout) {
                permit.timeout = timeout;
            }
            let current = usage.entry(key.clone()).or_default();
            current.active += 1;
            current.reserved_bytes = current.reserved_bytes.saturating_add(bytes);
            current.queries += 1;
            permit.reservations.push((key, bytes));
        }
    }
    Ok(permit)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn authenticated_roles_and_tenants_have_independent_bounded_reservations() {
        let root = tempfile::tempdir().unwrap();
        let mut config = PgWireConfig::default();
        config.workload.roles.insert(
            "limited".into(),
            RoleLimits {
                max_concurrent_queries: 1,
                memory_bytes: 4096,
                query_timeout_ms: 7,
                ..Default::default()
            },
        );
        config.workload.tenants.insert(
            "tenant".into(),
            RoleLimits {
                max_concurrent_queries: 1,
                memory_bytes: 2048,
                ..Default::default()
            },
        );
        let server = PgWireServer::open(root.path(), config).unwrap();
        let ctx = SecurityContext::new("limited", "tenant");
        let permit = admit(&server, "limited", Some(&ctx), "SELECT 1").unwrap();
        assert_eq!(permit.memory_bytes, 2048);
        assert_eq!(permit.timeout, Duration::from_millis(7));
        assert!(admit(&server, "limited", None, "SELECT 1").is_err());
        assert!(admit(&server, "another", Some(&ctx), "SELECT 1").is_err());
        assert!(admit(&server, "another", None, "SELECT 1").is_ok());
        assert!(admit(&server, "limited", Some(&ctx), "ROLLBACK").is_ok());
        drop(permit);
        assert_eq!(
            server.workload.usage.lock().unwrap()["role:limited"].reserved_bytes,
            0
        );
        assert!(admit(&server, "limited", Some(&ctx), "SELECT 1").is_ok());
    }
    #[test]
    fn cost_gate_rejects_before_a_scan_or_mutation_runs() {
        let root = tempfile::tempdir().unwrap();
        let mut config = PgWireConfig::default();
        config.workload.roles.insert(
            "limited".into(),
            RoleLimits {
                max_estimated_rows: 2,
                ..Default::default()
            },
        );
        let server = PgWireServer::open(root.path(), config).unwrap();
        {
            let mut db = server.db.write();
            db.create_collection("items").unwrap();
            for id in 0..4 {
                db.insert("items", bicdb_core::Record::new(id.to_string()))
                    .unwrap();
            }
        }
        assert!(admit(&server, "limited", None, "DELETE FROM items").is_err());
        assert_eq!(server.db.read().estimated_record_count("items").unwrap(), 4);
        assert!(admit(&server, "limited", None, "SELECT 1").is_ok());
    }
}

#[cfg(test)]
mod execution_tests {
    use super::*;
    #[test]
    fn query_memory_budget_is_checked_and_released_after_rejection() {
        let root = tempfile::tempdir().unwrap();
        let mut config = PgWireConfig::default();
        config.workload.roles.insert(
            "bicdb".into(),
            RoleLimits {
                max_concurrent_queries: 1,
                memory_bytes: 1024,
                ..Default::default()
            },
        );
        let server = PgWireServer::open(root.path(), config).unwrap();
        let mut state = ConnectionState::new(1, "bicdb".into(), None);
        let error =
            execute_server_sql(&server, &mut state, "SELECT repeat('x', 4096)").unwrap_err();
        assert_eq!(pgwire_error_sqlstate(&error), "53200", "{error}");
        assert_eq!(
            server.workload.usage.lock().unwrap()["role:bicdb"].active,
            0
        );
        assert!(execute_server_sql(&server, &mut state, "SELECT 1").is_ok());
    }
}

/// A subscription's retained result is charged for its whole lifetime, not
/// just while the dispatcher evaluates it. Weak ownership avoids a server cycle.
pub(crate) struct CacheReservation {
    server: std::sync::Weak<PgWireServer>,
    identities: Vec<(String, usize)>,
    bytes: usize,
}
impl CacheReservation {
    pub fn new(
        server: &Arc<PgWireServer>,
        user: &str,
        context: Option<&SecurityContext>,
        bytes: usize,
    ) -> Result<Self> {
        let mut identities = Vec::new();
        if let Some(limit) = server
            .config
            .workload
            .roles
            .get(user)
            .or_else(|| server.config.workload.roles.get("*"))
        {
            identities.push((format!("role:{user}"), limit.memory_bytes));
        }
        if let Some(tenant) = context.map(|context| context.tenant_id.as_str()) {
            if let Some(limit) = server
                .config
                .workload
                .tenants
                .get(tenant)
                .or_else(|| server.config.workload.tenants.get("*"))
            {
                identities.push((format!("tenant:{tenant}"), limit.memory_bytes));
            }
        }
        let mut reservation = Self {
            server: Arc::downgrade(server),
            identities,
            bytes: 0,
        };
        reservation.resize(bytes)?;
        Ok(reservation)
    }
    pub fn resize(&mut self, bytes: usize) -> Result<()> {
        let Some(server) = self.server.upgrade() else {
            return Ok(());
        };
        let mut usage = server.workload.usage.lock().unwrap();
        for (identity, limit) in &self.identities {
            let current = usage.entry(identity.clone()).or_default();
            let projected = current
                .cached_bytes
                .saturating_sub(self.bytes)
                .saturating_add(bytes)
                .saturating_add(current.reserved_bytes);
            if *limit > 0 && projected > *limit {
                current.rejected += 1;
                return Err(PgWireError::QueryRejected(format!(
                    "{identity} retained subscription memory exceeds its envelope"
                )));
            }
        }
        for (identity, _) in &self.identities {
            let current = usage.entry(identity.clone()).or_default();
            current.cached_bytes = current.cached_bytes - self.bytes + bytes;
        }
        self.bytes = bytes;
        Ok(())
    }
}
impl Drop for CacheReservation {
    fn drop(&mut self) {
        let _ = self.resize(0);
    }
}

#[cfg(test)]
mod cache_tests {
    use super::*;
    #[test]
    fn retained_subscription_memory_shares_the_role_envelope_with_active_queries() {
        let root = tempfile::tempdir().unwrap();
        let mut config = PgWireConfig::default();
        config.workload.roles.insert(
            "app".into(),
            RoleLimits {
                max_concurrent_queries: 1,
                memory_bytes: 4096,
                ..Default::default()
            },
        );
        let server = PgWireServer::open(root.path(), config).unwrap();
        let mut cache = CacheReservation::new(&server, "app", None, 2000).unwrap();
        let query = admit(&server, "app", None, "SELECT 1").unwrap();
        assert_eq!(query.memory_bytes, 2096);
        assert!(cache.resize(2100).is_err());
        drop(query);
        cache.resize(2100).unwrap();
        assert!(CacheReservation::new(&server, "app", None, 2100).is_err());
        drop(cache);
        assert_eq!(
            server.workload.usage.lock().unwrap()["role:app"].cached_bytes,
            0
        );
        assert_eq!(
            admit(&server, "app", None, "SELECT 1")
                .unwrap()
                .memory_bytes,
            4096
        );
    }
}
