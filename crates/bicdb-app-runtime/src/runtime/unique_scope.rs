//! Explicit compiler/operator-signed CLOSED-STORE unique scope migration.
//! This owner is never exposed as an ApplicationRuntime/Arc. It has no invocation,
//! listener, service dispatcher or supervisor entry points. A newly acquired core
//! store lease excludes every live process and every captured old runtime snapshot.
use super::application_runtime::{
    compile_scripts, validate_application_batch_order, validate_application_module_catalog,
    validate_application_module_upgrade,
};
use super::*;
use crate::TrustedSigningKeys;
use bicdb_extension::abi_v2::ResourceUniqueTargetV1;
use serde::de::DeserializeOwned;

const RECEIPT: &str = ".application-unique-scope.json";
const DOMAIN: &str = "bicdb.offline-unique-scope-transition.v1\n";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UniqueScopeEnvelope {
    pub plan: UniqueScopePlan,
    pub key_id: String,
    pub signature: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UniqueScopePlan {
    pub version: u32,
    pub transition_id: String,
    pub mode: String,
    /// Explicit operator deployment attestation; missing HA sidecars alone do
    /// not establish that an externally configured server is standalone.
    pub topology: String,
    pub old_catalog_sha256: String,
    pub packages: Vec<UniqueScopePackage>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UniqueScopePackage {
    pub application: String,
    pub old_package_sha256: String,
    pub new_package_sha256: String,
    pub new_file_sha256: String,
    pub resources: Vec<UniqueScopeResource>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UniqueScopeResource {
    pub resource: String,
    pub old_contract_sha256: String,
    pub new_contract_sha256: String,
    pub old_target: String,
    pub new_target: String,
    pub scope_fields: Vec<String>,
    pub remove_ambiguous_upsert: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexChange {
    old: IndexDefinition,
    new: IndexDefinition,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeReceipt {
    version: u32,
    phase: String,
    envelope: UniqueScopeEnvelope,
    package_root: PathBuf,
    old_catalog: Vec<u8>,
    new_catalog: Vec<u8>,
    changes: Vec<IndexChange>,
    rows_sha256: String,
    policies_sha256: String,
    committed_watermark: u64,
    migration_ids: Vec<String>,
}
#[derive(Clone, Debug, Serialize)]
pub struct UniqueScopeOutcome {
    pub transition_id: String,
    pub phase: String,
    pub plan_sha256: String,
    pub rows_sha256: String,
}

/// Owns a fresh exclusive database handle; never supplies a shared runtime handle.
pub struct OfflineUniqueScopeTransition {
    runtime: ApplicationRuntime,
    operator_keys: TrustedSigningKeys,
    _lease: LifecycleFileLock,
}

fn invalid(message: impl Into<String>) -> AppRuntimeError {
    AppRuntimeError::InvalidPackage(message.into())
}
fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}
fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    fn sort(value: Value) -> Value {
        match value {
            Value::Object(map) => {
                let ordered = map.into_iter().collect::<BTreeMap<_, _>>();
                let mut sorted = serde_json::Map::new();
                for (key, value) in ordered {
                    sorted.insert(key, sort(value));
                }
                Value::Object(sorted)
            }
            Value::Array(items) => Value::Array(items.into_iter().map(sort).collect()),
            other => other,
        }
    }
    // SQL enables preserve_order transitively: sort recursively explicitly.
    Ok(serde_json::to_vec(&sort(serde_json::to_value(value)?))?)
}

fn fingerprint<T: Serialize>(value: &T) -> Result<String> {
    Ok(digest(&canonical(value)?))
}
fn decode<T: DeserializeOwned>(path: &Path) -> Result<T> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn active_path(root: &Path) -> PathBuf {
    root.join("snapshots").join("active.json")
}
fn unique_index(
    resource: &ResourceContractV1,
    target: &ResourceUniqueTargetV1,
) -> Result<IndexDefinition> {
    if target.predicate.is_some() {
        return Err(invalid(
            "scope transition v1 does not support partial unique predicates",
        ));
    }
    Ok(IndexDefinition {
        name: target
            .index_name
            .clone()
            .unwrap_or_else(|| format!("{}_unique_{}", resource.name, target.target)),
        collection: resource.relation.clone(),
        fields: target
            .fields
            .iter()
            .map(|f| resource_index_field(resource, f))
            .collect::<Result<_>>()?,
        unique: true,
        nulls_not_distinct: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    })
}

impl OfflineUniqueScopeTransition {
    pub fn open(
        database: impl AsRef<Path>,
        config: ApplicationHostConfig,
        verifier: PackageVerifier,
        services: InvocationServices,
        operator_keys: TrustedSigningKeys,
    ) -> Result<Self> {
        // Opening this owned handle takes the native inode/OS store lease. No live
        // host or caller can retain a snapshot of this newly constructed runtime.
        let db = BicDb::open_for_application_unique_scope(database)?;
        if db.replication_config().enabled {
            return Err(invalid("offline scope transition v1 excludes replicated stores; use a separately qualified cluster protocol"));
        }
        db.check_catalog_recovery_quiescent()?;
        let lease = LifecycleFileLock::acquire(&config.package_root)?;
        let runtime = ApplicationRuntime::initialize(
            Arc::new(RwLock::new(db)),
            config,
            verifier,
            services,
            false,
        )?;
        // A pending intent may contain a mixed catalog; recovery below examines
        // exact durable bytes BEFORE restoring/compiling that active snapshot.
        Ok(Self {
            runtime,
            operator_keys,
            _lease: lease,
        })
    }

    fn verify_plan(&self, envelope: &UniqueScopeEnvelope) -> Result<()> {
        let p = &envelope.plan;
        if p.version != 1
            || p.mode != "closed-store-before-traffic"
            || p.topology != "standalone"
            || p.packages.is_empty()
            || p.transition_id.len() > 128
            || p.transition_id.is_empty()
            || !p
                .transition_id
                .bytes()
                .all(|x| x.is_ascii_alphanumeric() || x == b'-' || x == b'_')
        {
            return Err(invalid(
                "invalid offline unique-scope plan identity/mode/version",
            ));
        }
        let mut message = DOMAIN.as_bytes().to_vec();
        message.extend(canonical(p)?);
        self.operator_keys
            .verify_detached(&envelope.key_id, &message, &envelope.signature)
    }
    fn receipt_path(&self) -> PathBuf {
        self.runtime.db.read().data_path().join(RECEIPT)
    }
    fn save_receipt(&self, r: &ScopeReceipt) -> Result<()> {
        atomic_write(&self.receipt_path(), &serde_json::to_vec(r)?)
    }
    fn outcome(&self, r: &ScopeReceipt) -> Result<UniqueScopeOutcome> {
        Ok(UniqueScopeOutcome {
            transition_id: r.envelope.plan.transition_id.clone(),
            phase: r.phase.clone(),
            plan_sha256: fingerprint(&r.envelope.plan)?,
            rows_sha256: r.rows_sha256.clone(),
        })
    }

    /// Verify both compiler package signatures and explicit operator scope authority.
    /// `audit` performs no index/catalog changes. `apply` leaves admission CLOSED.
    pub fn prepare(
        &self,
        envelope: UniqueScopeEnvelope,
        package_paths: &[PathBuf],
        apply: bool,
    ) -> Result<UniqueScopeOutcome> {
        self.verify_plan(&envelope)?;
        if self.receipt_path().exists() {
            let old: ScopeReceipt = decode(&self.receipt_path())?;
            let same = fingerprint(&old.envelope)? == fingerprint(&envelope)?;
            if old.envelope.plan.transition_id == envelope.plan.transition_id && !same {
                return Err(invalid(
                    "scope transition ID cannot be reused with different signed bytes",
                ));
            }
            if same && matches!(old.phase.as_str(), "applied" | "admitted") {
                self.verify_receipt_bindings(&old)?;
                if old.phase == "applied" {
                    self.check_receipt_state(&old, true)?;
                }
                return self.outcome(&old);
            }
            if old.phase != "admitted" && old.phase != "reverted" {
                return Err(invalid(
                    "pending scope transition requires explicit recovery",
                ));
            }
        }
        let historical = self
            .runtime
            .db
            .read()
            .data_path()
            .join("application-unique-scope-receipts")
            .join(format!("{}.json", envelope.plan.transition_id));
        if historical.exists() {
            return Err(invalid("scope transition ID already has immutable history; choose a new signed transition identity"));
        }
        self.runtime
            .restore_active_snapshot_with_supervisors(false)?;
        let old_catalog = fs::read(active_path(&self.runtime.config.package_root))?;
        if digest(&old_catalog) != envelope.plan.old_catalog_sha256 {
            return Err(invalid(
                "old catalog fingerprint differs from operator-signed scope plan",
            ));
        }
        let current = self.runtime.active.read().clone();
        let mut projected = RuntimeCatalog {
            generation: current
                .generation
                .checked_add(1)
                .ok_or_else(|| invalid("catalog generation overflow"))?,
            packages: current.packages.clone(),
            routes: BTreeMap::new(),
        };
        let mut provided = BTreeMap::new();
        for path in package_paths {
            let bytes = fs::read(path)?;
            let package = self.runtime.verifier.read_package_file(path)?;
            let name = normalized(&package.manifest.identity.name);
            if provided.insert(name, (bytes, package)).is_some() {
                return Err(invalid("repeated scope package"));
            }
        }
        if provided.len() != envelope.plan.packages.len() {
            return Err(invalid("scope plan/package set mismatch"));
        }
        let mut changes = Vec::new();
        let mut candidates = Vec::new();
        let mut expected_migrations = Vec::new();
        for p in &envelope.plan.packages {
            let name = normalized(&p.application);
            let (bytes, package) = provided
                .remove(&name)
                .ok_or_else(|| invalid("planned scope package missing"))?;
            let old = current.packages.get(&name).ok_or_else(|| {
                invalid("scope transition requires an installed previous package")
            })?;
            let verified = self.runtime.verifier.verify(&package)?;
            if old.package_hash != p.old_package_sha256
                || verified.package_sha256 != p.new_package_sha256
                || digest(&bytes) != p.new_file_sha256
            {
                return Err(invalid(
                    "signed old/new package or exact file fingerprint mismatch",
                ));
            }
            validate_runtime_profile(&package)?;
            let modules = self.runtime.compile_modules(&package)?;
            let scripts = compile_scripts(&package)?;
            let package = Arc::new(package);
            let manifest = Arc::new(package.manifest.clone());
            let snapshot = Arc::new(PackageSnapshot {
                generation: projected.generation,
                package_hash: verified.package_sha256.clone(),
                package: package.clone(),
                verification: verified,
                state: ApplicationPackageState::Active,
                readiness: ApplicationReadiness::default(),
                activated_at_ms: crate::host::now_ms(),
                modules,
                scripts,
                carrier_plan: ApplicationExecutionPlan::prepare(manifest.clone()),
                manifest,
            });
            validate_application_module_upgrade(old, &snapshot)?;
            if package_semver(&snapshot)? <= package_semver(old)? {
                return Err(invalid("scope package version must advance"));
            }
            validate_workflow_plan_evolution(old, &snapshot)?;
            let old_app = application_contract(old);
            let app = application_contract(&snapshot);
            validate_security_contract_evolution(old_app, app)?;
            validate_blob_contract_evolution(old_app, app)?;
            if fingerprint(&old_app.invariants)? != fingerprint(&app.invariants)?
                || fingerprint(&old_app.auth_schemes)? != fingerprint(&app.auth_schemes)?
                || fingerprint(&old_app.routes)? != fingerprint(&app.routes)?
                || fingerprint(&old_app.relation_permissions)?
                    != fingerprint(&app.relation_permissions)?
                || fingerprint(&old.package.manifest.permissions)?
                    != fingerprint(&snapshot.package.manifest.permissions)?
            {
                return Err(invalid("scope-only transition must preserve financial invariants, routes, roles and permissions"));
            }

            validate_migration_boundary(old_app, app)?;
            for migration in &app.migrations {
                for step in migration.forward.iter().chain(&migration.rollback) {
                    if !matches!(step, MigrationStep::ValidateContract { .. }) {
                        return Err(invalid(
                            "scope-only forward/rollback plans support validate_contract only",
                        ));
                    }
                }
                for step in &migration.transformations {
                    let MigrationStep::BackfillField {
                        resource, field, ..
                    } = step
                    else {
                        return Err(invalid(
                            "scope-only transition cannot execute transformations",
                        ));
                    };
                    if !old_app
                        .migrations
                        .iter()
                        .any(|m| m.transformations.contains(step))
                    {
                        return Err(invalid("scope-only transition adds/changes a backfill"));
                    }
                    let contract = app
                        .resources
                        .iter()
                        .find(|r| r.name == *resource)
                        .ok_or_else(|| invalid("backfill contract missing"))?;
                    // Existing compiler-default backfills are tolerated only when
                    // proved no-op for EVERY physical row, never executed here.
                    for row in self
                        .runtime
                        .db
                        .read()
                        .scan_collection_unchecked(&contract.relation)?
                    {
                        if row.metadata.get(field).is_none_or(Value::is_null) {
                            return Err(invalid("dirty legacy row requires separate backfill qualification before scope migration"));
                        }
                    }
                }
            }
            let mut schema_comparison = app.clone();
            let mut named = BTreeSet::new();
            for t in &p.resources {
                if !named.insert(t.resource.clone()) {
                    return Err(invalid("repeated scope resource"));
                }
                let a = old_app
                    .resources
                    .iter()
                    .find(|r| r.name == t.resource)
                    .ok_or_else(|| invalid("old scope resource missing"))?;
                let b = app
                    .resources
                    .iter()
                    .find(|r| r.name == t.resource)
                    .ok_or_else(|| invalid("new scope resource missing"))?;
                changes.push(validate_scope_contract(
                    &self.runtime.db.read(),
                    a,
                    b,
                    t,
                    true,
                )?);
                // This copy is ONLY a comparison lens, after exact signed contract
                // validation. Original package/modules/signature bytes stay untouched.
                let compared = schema_comparison
                    .resources
                    .iter_mut()
                    .find(|r| r.name == t.resource)
                    .unwrap();
                compared.unique_targets = a.unique_targets.clone();
            }
            // No unrelated schema changes are admitted by this narrow protocol.
            for a in &old_app.resources {
                let b = schema_comparison
                    .resources
                    .iter()
                    .find(|r| r.name == a.name)
                    .ok_or_else(|| invalid("scope upgrade removes resource"))?;
                if !named.contains(&a.name) && a != b {
                    return Err(invalid("scope upgrade changes an unplanned resource"));
                }
            }
            if old_app.resources.len() != schema_comparison.resources.len() {
                return Err(invalid("scope upgrade adds unrelated resources"));
            }
            validate_additive_contracts(old_app, &schema_comparison)?;
            for m in &app.migrations {
                let id = format!(
                    "{}:{}",
                    snapshot.package.manifest.identity.name, m.schema_version
                );
                if self
                    .runtime
                    .db
                    .read()
                    .get("__bicdb_app_migrations", &id)?
                    .is_none()
                {
                    if m.schema_version
                        <= latest_migration(old_app)
                            .map(|m| m.schema_version)
                            .unwrap_or(0)
                    {
                        return Err(invalid("missing historical migration ledger requires a separate clean upgrade audit"));
                    }
                    expected_migrations.push(id);
                }
            }
            projected.packages.insert(name.clone(), snapshot.clone());
            candidates.push((name, snapshot));
        }
        if changes.is_empty() {
            return Err(invalid(
                "scope transition must explicitly name a changed unique target",
            ));
        }
        validate_scope_execution_boundaries(&current, &projected)?;
        validate_application_batch_order(&candidates)?;
        validate_application_module_catalog(&projected)?;
        for snapshot in projected.packages.values() {
            self.runtime.validate_bindings(snapshot, &projected)?;
            let ready = self.runtime.calculate_readiness(snapshot, &projected);
            if !ready.ready {
                return Err(AppRuntimeError::NotReady(ready.issues.join("; ")));
            }
        }
        build_routes(&mut projected)?;
        let (rows_sha256, policies_sha256) =
            audit_catalog(&self.runtime.db.read(), &projected, &envelope.plan)?;
        let mut history = self
            .runtime
            .history
            .lock()
            .expect("package history poisoned")
            .clone();
        for (name, _) in &candidates {
            history
                .entry(name.clone())
                .or_default()
                .push_back(current.packages[name].clone());
        }
        let next = PersistedRuntimeState {
            generation: projected.generation,
            packages: projected
                .packages
                .iter()
                .map(|(n, s)| (n.clone(), persisted_reference(s)))
                .collect(),
            staged: BTreeMap::new(),
            history: history
                .iter()
                .map(|(n, ss)| (n.clone(), ss.iter().map(persisted_reference).collect()))
                .collect(),
        };
        let mut receipt = ScopeReceipt {
            version: 1,
            phase: "audited".into(),
            envelope,
            package_root: self.runtime.config.package_root.canonicalize()?,
            old_catalog,
            new_catalog: serde_json::to_vec(&next)?,
            changes,
            rows_sha256,
            policies_sha256,
            committed_watermark: self.runtime.db.read().last_commit_seq(),
            migration_ids: expected_migrations,
        };
        if !apply {
            return self.outcome(&receipt);
        }
        for (_, s) in &candidates {
            persist_package(
                &self.runtime.config.package_root,
                &s.package_hash,
                &s.package,
            )?;
        }
        // Native reader fence precedes the durable intent. A process killed here
        // has changed no index/catalog and is still safe to open on this reader.
        self.runtime.db.write().enable_application_unique_scope()?;
        if self.receipt_path().exists() {
            let old: ScopeReceipt = decode(&self.receipt_path())?;
            let archive = self
                .runtime
                .db
                .read()
                .data_path()
                .join("application-unique-scope-receipts")
                .join(format!("{}.json", old.envelope.plan.transition_id));
            atomic_write(&archive, &serde_json::to_vec(&old)?)?;
        }
        receipt.phase = "prepared".into();
        self.save_receipt(&receipt)?;
        let result = (|| -> Result<()> {
            let mut db = self.runtime.db.write();
            for c in &receipt.changes {
                db.create_index(c.new.clone())?;
            }
            db.checkpoint_for_resume()?;
            for c in &receipt.changes {
                db.drop_index(&c.old.name)?;
            }
            db.checkpoint_for_resume()?;
            for (_, s) in &candidates {
                record_applied_migrations(&mut db, s)?;
            }
            db.checkpoint_for_resume()?;
            drop(db);
            // Only this exact catalog byte string is a successful commit witness.
            atomic_write(
                &active_path(&self.runtime.config.package_root),
                &receipt.new_catalog,
            )?;
            receipt.committed_watermark = self.runtime.db.read().last_commit_seq();
            receipt.phase = "applied".into();
            self.save_receipt(&receipt)?;
            self.check_receipt_state(&receipt, true)?;
            Ok(())
        })();
        match result {
            Ok(()) => self.outcome(&receipt),
            Err(error) => {
                self.reconcile(&mut receipt, false).map_err(|recovery| {
                    invalid(format!(
                        "{error}; scope compensation quarantined: {recovery}"
                    ))
                })?;
                Err(error)
            }
        }
    }

    /// Recovery requires the same exact signed plan. No pending intent is
    /// automatically admitted, and ordinary raw SQL open stays quarantined.
    pub fn finish(
        &self,
        envelope: &UniqueScopeEnvelope,
        action: &str,
    ) -> Result<UniqueScopeOutcome> {
        self.verify_plan(envelope)?;
        let mut receipt: ScopeReceipt = decode(&self.receipt_path())?;
        if receipt.version != 1
            || fingerprint(&receipt.envelope)? != fingerprint(envelope)?
            || receipt.package_root != self.runtime.config.package_root.canonicalize()?
        {
            return Err(invalid("scope receipt/authority/package root mismatch"));
        }
        self.verify_receipt_bindings(&receipt)?;
        if receipt.phase == "admitted" && action == "admit" {
            return self.outcome(&receipt);
        }
        if receipt.phase == "reverted" && action == "rollback" {
            return self.outcome(&receipt);
        }
        if receipt.phase == "admitted" {
            return Err(invalid(
                "scope traffic admitted: pre-traffic rollback/recovery forbidden",
            ));
        }
        if digest(&receipt.old_catalog) != envelope.plan.old_catalog_sha256 {
            return Err(invalid(
                "persisted old catalog fingerprint differs from signed plan",
            ));
        }
        match action {
            "recover" => {
                let active = fs::read(active_path(&self.runtime.config.package_root))?;
                let finalize = active == receipt.new_catalog;
                if !finalize && active != receipt.old_catalog {
                    return Err(invalid("ambiguous scope catalog commit: quarantined"));
                }
                self.reconcile(&mut receipt, finalize)?;
            }
            "rollback" => {
                if receipt.phase != "applied" {
                    return Err(invalid("pre-traffic rollback requires an applied receipt"));
                }
                if self.runtime.db.read().last_commit_seq() != receipt.committed_watermark {
                    return Err(invalid(
                        "post-transition writes forbid pre-traffic rollback",
                    ));
                }
                self.reconcile(&mut receipt, false)?;
            }
            "admit" => {
                if receipt.phase != "applied" {
                    return Err(invalid("scope admission requires applied receipt"));
                }
                if self.runtime.db.read().last_commit_seq() != receipt.committed_watermark {
                    return Err(invalid(
                        "post-transition writes before admission: quarantined",
                    ));
                }
                self.check_receipt_state(&receipt, true)?;
                receipt.phase = "admitted".into();
                self.save_receipt(&receipt)?;
            }
            _ => return Err(invalid("unsupported scope recovery action")),
        }
        self.outcome(&receipt)
    }
    fn verify_receipt_bindings(&self, r: &ScopeReceipt) -> Result<()> {
        if r.version != 1 || digest(&r.old_catalog) != r.envelope.plan.old_catalog_sha256 {
            return Err(invalid("scope receipt old catalog is not operator-signed"));
        }
        let old: PersistedRuntimeState = serde_json::from_slice(&r.old_catalog)?;
        let new: PersistedRuntimeState = serde_json::from_slice(&r.new_catalog)?;
        if new.generation
            != old
                .generation
                .checked_add(1)
                .ok_or_else(|| invalid("generation overflow"))?
            || old.packages.keys().collect::<Vec<_>>() != new.packages.keys().collect::<Vec<_>>()
        {
            return Err(invalid(
                "scope receipt changes unrelated catalog ownership/generation",
            ));
        }
        let mut planned = BTreeSet::new();
        let mut expected_indexes = Vec::new();
        let mut allowed_migrations = BTreeSet::new();
        let db = self.runtime.db.read();
        for p in &r.envelope.plan.packages {
            let name = normalized(&p.application);
            if !planned.insert(name.clone()) {
                return Err(invalid("duplicate package in signed scope receipt"));
            }
            let a = old
                .packages
                .get(&name)
                .ok_or_else(|| invalid("receipt old package missing"))?;
            let b = new
                .packages
                .get(&name)
                .ok_or_else(|| invalid("receipt new package missing"))?;
            if a.hash != p.old_package_sha256
                || b.hash != p.new_package_sha256
                || b.generation != new.generation
            {
                return Err(invalid(
                    "receipt catalog package differs from exact signed transition pair",
                ));
            }
            let previous = self.runtime.load_persisted_snapshot(&name, a)?;
            let candidate = self.runtime.load_persisted_snapshot(&name, b)?;
            let old_app = application_contract(&previous);
            let app = application_contract(&candidate);
            for t in &p.resources {
                let a = old_app
                    .resources
                    .iter()
                    .find(|x| x.name == t.resource)
                    .ok_or_else(|| invalid("receipt old resource missing"))?;
                let b = app
                    .resources
                    .iter()
                    .find(|x| x.name == t.resource)
                    .ok_or_else(|| invalid("receipt new resource missing"))?;
                expected_indexes.push(validate_scope_contract(&db, a, b, t, false)?);
            }
            let boundary = latest_migration(old_app)
                .map(|m| m.schema_version)
                .unwrap_or(0);
            for m in &app.migrations {
                if m.schema_version > boundary {
                    allowed_migrations.insert(format!(
                        "{}:{}",
                        candidate.package.manifest.identity.name, m.schema_version
                    ));
                }
            }
        }
        for (name, a) in &old.packages {
            if !planned.contains(name) && fingerprint(a)? != fingerprint(&new.packages[name])? {
                return Err(invalid("scope receipt changes an unplanned package"));
            }
        }
        if fingerprint(&r.changes)? != fingerprint(&expected_indexes)?
            || r.migration_ids.iter().collect::<BTreeSet<_>>().len() != r.migration_ids.len()
            || r.migration_ids
                .iter()
                .any(|id| !allowed_migrations.contains(id))
        {
            return Err(invalid(
                "receipt index/migration compensation differs from compiler/operator-signed plan",
            ));
        }
        Ok(())
    }

    fn check_receipt_state(&self, r: &ScopeReceipt, new: bool) -> Result<()> {
        let expected = if new { &r.new_catalog } else { &r.old_catalog };
        if fs::read(active_path(&self.runtime.config.package_root))? != *expected {
            return Err(invalid("scope catalog witness mismatch"));
        }
        let db = self.runtime.db.read();
        let indexes = db.index_definitions();
        for c in &r.changes {
            let keep = if new { &c.new } else { &c.old };
            let gone = if new { &c.old } else { &c.new };
            if !indexes.iter().any(|x| x == keep) || indexes.iter().any(|x| x.name == gone.name) {
                return Err(invalid("scope physical index state differs from receipt"));
            }
        }
        self.audit_receipt_rows(&db, r)
    }
    fn audit_receipt_rows(&self, db: &BicDb, r: &ScopeReceipt) -> Result<()> {
        // Load original signed candidate snapshots without restoring supervisors.
        let state: PersistedRuntimeState = serde_json::from_slice(&r.new_catalog)?;
        let mut catalog = RuntimeCatalog {
            generation: state.generation,
            ..RuntimeCatalog::default()
        };
        for (n, p) in state.packages {
            catalog
                .packages
                .insert(n.clone(), self.runtime.load_persisted_snapshot(&n, &p)?);
        }
        let (rows, policies) = audit_catalog(db, &catalog, &r.envelope.plan)?;
        if rows != r.rows_sha256 || policies != r.policies_sha256 {
            return Err(invalid(
                "scope historical rows/references/policies changed: quarantined",
            ));
        }
        Ok(())
    }
    fn reconcile(&self, r: &mut ScopeReceipt, finalize: bool) -> Result<()> {
        if r.phase == "admitted" {
            return Err(invalid("cannot recover/rollback after admission"));
        }
        let mut db = self.runtime.db.write();
        db.check_catalog_recovery_quiescent()?;
        self.audit_receipt_rows(&db, r)?;
        // Restore/create the retaining index BEFORE removing the other one.
        for c in &r.changes {
            let keep = if finalize { &c.new } else { &c.old };
            let existing = db
                .index_definitions()
                .into_iter()
                .find(|x| x.name == keep.name);
            match existing {
                Some(ref x) if x == keep => {}
                Some(_) => return Err(invalid("scope compensation index identity mismatch")),
                None => db.create_index(keep.clone())?,
            }
        }
        db.checkpoint_for_resume()?;
        for c in &r.changes {
            let remove = if finalize { &c.old } else { &c.new };
            if let Some(x) = db
                .index_definitions()
                .into_iter()
                .find(|x| x.name == remove.name)
            {
                if x != *remove {
                    return Err(invalid(
                        "scope compensation refuses unrelated physical index",
                    ));
                }
                db.drop_index(&remove.name)?;
            }
        }
        if !finalize {
            remove_applied_migrations(&mut db, &r.migration_ids)?;
        } else {
            let state: PersistedRuntimeState = serde_json::from_slice(&r.new_catalog)?;
            for (n, p) in state.packages {
                let snapshot = self.runtime.load_persisted_snapshot(&n, &p)?;
                record_applied_migrations(&mut db, &snapshot)?;
            }
        }
        db.checkpoint_for_resume()?;
        let watermark = db.last_commit_seq();
        drop(db);
        atomic_write(
            &active_path(&self.runtime.config.package_root),
            if finalize {
                &r.new_catalog
            } else {
                &r.old_catalog
            },
        )?;
        r.committed_watermark = watermark;
        r.phase = if finalize { "applied" } else { "reverted" }.into();
        self.save_receipt(r)?;
        self.check_receipt_state(r, finalize)
    }
}

fn validate_scope_contract(
    db: &BicDb,
    a: &ResourceContractV1,
    b: &ResourceContractV1,
    t: &UniqueScopeResource,
    check_physical_indexes: bool,
) -> Result<IndexChange> {
    if fingerprint(a)? != t.old_contract_sha256 || fingerprint(b)? != t.new_contract_sha256 {
        return Err(invalid("full signed scope resource fingerprint mismatch"));
    }
    let tenant = a
        .tenant_field
        .as_ref()
        .ok_or_else(|| invalid("scope extension requires an existing native tenant partition"))?;
    if a.tenant_field != b.tenant_field
        || t.scope_fields.first() != Some(tenant)
        || t.scope_fields.len() > 2
        || t.scope_fields.is_empty()
        || t.scope_fields.iter().collect::<BTreeSet<_>>().len() != t.scope_fields.len()
    {
        return Err(invalid(
            "scope extension changes/omits existing tenant semantics",
        ));
    }
    if !a.immutable_fields.contains(tenant)
        || !a.server_managed_fields.contains(tenant)
        || a.create_fields.contains(tenant)
        || a.update_fields.contains(tenant)
    {
        return Err(invalid(
            "scope tenant must already be immutable server-owned authenticated identity",
        ));
    }
    for field in &t.scope_fields {
        let f = a
            .fields
            .iter()
            .find(|f| &f.name == field)
            .ok_or_else(|| invalid("scope field missing"))?;
        if f.field_type != FieldType::Uuid || f.nullable {
            return Err(invalid("scope field must already be nonnullable UUID"));
        }
    }
    if t.scope_fields.len() == 2 && t.scope_fields[1] != "legal_entity_id" {
        return Err(invalid(
            "only existing legal entity is an allowed additional scope",
        ));
    }
    if b.schema_version < a.schema_version {
        return Err(invalid(
            "scope transition resource schema_version cannot regress; signed app migration must advance",
        ));
    }
    let old = a
        .unique_targets
        .iter()
        .find(|u| u.target == t.old_target)
        .ok_or_else(|| invalid("old exact unique target missing"))?;
    let new = b
        .unique_targets
        .iter()
        .find(|u| u.target == t.new_target)
        .ok_or_else(|| invalid("new exact unique target missing"))?;
    let expected = t
        .scope_fields
        .iter()
        .chain(&old.fields)
        .cloned()
        .collect::<Vec<_>>();
    if new.fields != expected
        || old.fields.iter().any(|f| t.scope_fields.contains(f))
        || old.predicate != new.predicate
    {
        return Err(invalid(
            "scope extension must preserve every old identity field/order and predicate",
        ));
    }
    for field in &old.fields {
        let f = a
            .fields
            .iter()
            .find(|f| &f.name == field)
            .ok_or_else(|| invalid("business field missing"))?;
        if f.field_type != FieldType::String
            || f.storage_name.as_ref().is_some_and(|name| name != field)
        {
            return Err(invalid(
                "scope transition v1 supports untransformed string business keys only",
            ));
        }
    }
    let mut x = a.clone();
    let mut y = b.clone();
    x.unique_targets.retain(|u| u.target != t.old_target);
    y.unique_targets.retain(|u| u.target != t.new_target);
    x.contract_sha256.clear();
    y.contract_sha256.clear();
    x.schema_version = y.schema_version;
    if t.remove_ambiguous_upsert {
        if !x.operations.contains(&ResourceOperation::Upsert)
            || y.operations.contains(&ResourceOperation::Upsert)
        {
            return Err(invalid(
                "declared upsert removal does not match signed authority reduction",
            ));
        }
        x.operations.retain(|op| *op != ResourceOperation::Upsert);
    }
    if x != y {
        return Err(invalid("scope resource changes other signed security, financial, lifecycle or identity contracts"));
    }
    let physical = db
        .mutation_policy(&a.relation)?
        .ok_or_else(|| invalid("missing existing native mutation policy"))?;
    if !physical.grants_required
        || physical.tenant_field.as_ref() != Some(tenant)
        || !physical.immutable_fields.contains(tenant)
    {
        return Err(invalid(
            "existing physical tenant mutation boundary is not enforced",
        ));
    }
    let policy = db
        .collection_policy(&a.relation)?
        .ok_or_else(|| invalid("missing native collection tenant policy"))?;
    if policy.tenant_field != *tenant {
        return Err(invalid(
            "physical collection tenant differs from signed partition",
        ));
    }
    let sql = embedded_resource_row_policy(a)
        .ok_or_else(|| invalid("scope target lacks signed SQL tenant policy"))?;
    bicdb_sql::audit_application_unique_scope_row_policy(db, &a.relation, &sql)
        .map_err(|e| invalid(format!("existing SQL tenant policy: {e}")))?;
    let old = unique_index(a, old)?;
    let new = unique_index(b, new)?;
    if old.name == new.name {
        return Err(invalid(
            "scope transition needs distinct old/new index identities for overlap safety",
        ));
    }
    let indexes = db.index_definitions();
    if check_physical_indexes
        && (!indexes.iter().any(|i| i == &old) || indexes.iter().any(|i| i.name == new.name))
    {
        return Err(invalid(
            "old physical unique definition/new name differs from signed transition boundary",
        ));
    }
    Ok(IndexChange { old, new })
}

fn row_value<'a>(row: &'a Record, field: &str) -> Option<&'a Value> {
    row.metadata.get(field)
}
fn row_id(row: &Record) -> String {
    row.metadata
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or(&row.id)
        .to_string()
}
fn valid_uuid(value: Option<&Value>) -> Result<String> {
    let text = value
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("missing/non-string scope UUID"))?;
    let id = uuid::Uuid::parse_str(text).map_err(|_| invalid("malformed scope UUID"))?;
    if id.is_nil() {
        return Err(invalid("nil scope identity"));
    }
    Ok(text.to_string())
}
fn audit_catalog(
    db: &BicDb,
    catalog: &RuntimeCatalog,
    plan: &UniqueScopePlan,
) -> Result<(String, String)> {
    db.check_catalog_recovery_quiescent()?;
    let mut resources = BTreeMap::new();
    let mut by_relation = BTreeMap::new();
    for s in catalog.packages.values() {
        for r in &application_contract(s).resources {
            if resources
                .insert(
                    format!(
                        "{}:{}",
                        normalized(&s.package.manifest.identity.name),
                        r.name
                    ),
                    r,
                )
                .is_some()
                || by_relation.insert(r.relation.clone(), r).is_some()
            {
                return Err(invalid("ambiguous signed resource/relation ownership"));
            }
        }
    }
    let mut rows = BTreeMap::<String, Vec<Record>>::new();
    let mut policies = BTreeMap::new();
    for r in resources.values() {
        let mut records = db.scan_collection_unchecked(&r.relation)?;
        records.sort_by(|a, b| a.id.cmp(&b.id));
        rows.insert(r.relation.clone(), records);
        policies.insert(
            r.relation.clone(),
            (
                db.collection_policy(&r.relation)?,
                db.mutation_policy(&r.relation)?,
            ),
        );
    }
    // SQL catalog bytes, including forced RLS and every inbound FK, are immutable
    // evidence too. There is no new SQL schema/financial invariant installation.
    let sql_catalog = if db
        .collections()
        .iter()
        .any(|c| c.name == "__bicdb_pg_schema")
    {
        let mut r = db.scan_collection_unchecked("__bicdb_pg_schema")?;
        r.sort_by(|a, b| a.id.cmp(&b.id));
        r
    } else {
        Vec::new()
    };
    for package in &plan.packages {
        for t in &package.resources {
            let r = resources
                .get(&format!(
                    "{}:{}",
                    normalized(&package.application),
                    t.resource
                ))
                .ok_or_else(|| invalid("planned audit resource missing"))?;
            let target = r
                .unique_targets
                .iter()
                .find(|u| u.target == t.new_target)
                .ok_or_else(|| invalid("planned audit target missing"))?;
            let business = &target.fields[t.scope_fields.len()..];
            let tenant = r
                .tenant_field
                .as_ref()
                .ok_or_else(|| invalid("audit tenant missing"))?;
            let records = &rows[&r.relation];
            let mut keys = BTreeSet::new();
            let mut ids = BTreeMap::new();
            for row in records {
                let org = valid_uuid(row_value(row, tenant))?;
                if let Some(stored) = row.metadata.get("id").and_then(Value::as_str) {
                    if stored != row.id {
                        return Err(invalid("physical and signed primary IDs differ"));
                    }
                }
                if ids.insert(row_id(row), row).is_some() {
                    return Err(invalid("duplicate physical primary ID"));
                }
                if t.scope_fields.len() == 2 {
                    let company = valid_uuid(row_value(row, "legal_entity_id"))?;
                    let owners = resources
                        .values()
                        .filter(|r| r.name == "LegalEntity")
                        .collect::<Vec<_>>();
                    if owners.len() != 1 {
                        return Err(invalid(
                            "unambiguous signed LegalEntity ownership contract required",
                        ));
                    }
                    let owner = owners[0];
                    if owner.tenant_field.as_ref() != Some(tenant) {
                        return Err(invalid("company owner uses a different tenant contract"));
                    }
                    let fact = rows[&owner.relation]
                        .iter()
                        .find(|x| row_id(x) == company)
                        .ok_or_else(|| invalid("dangling legal entity scope"))?;
                    if row_value(fact, tenant).and_then(Value::as_str) != Some(org.as_str()) {
                        return Err(invalid("company belongs to another tenant"));
                    }
                }
                let values = target
                    .fields
                    .iter()
                    .map(|f| {
                        if f == &r.primary_key {
                            Value::String(row_id(row))
                        } else {
                            row_value(row, f).cloned().unwrap_or(Value::Null)
                        }
                    })
                    .collect::<Vec<_>>();
                if business
                    .iter()
                    .any(|f| row_value(row, f).is_none_or(Value::is_null))
                {
                    continue;
                }
                if !keys.insert(canonical(&values)?) {
                    return Err(invalid("duplicate business identity within proposed scope"));
                }
            }
            for source in resources.values() {
                for fk in &source.foreign_keys {
                    if fk.target_resource.rsplit('.').next() != Some(r.name.as_str()) {
                        continue;
                    }
                    if fk.target_fields != vec![r.primary_key.clone()] || fk.fields.len() != 1 {
                        return Err(invalid("inbound natural/composite key reference needs a separate coordinated migration"));
                    }
                    if source.tenant_field.as_ref() != Some(tenant) {
                        return Err(invalid(
                            "inbound ID reference has unverifiable tenant scope",
                        ));
                    }
                    for row in &rows[&source.relation] {
                        let value = row_value(row, &fk.fields[0]);
                        if value.is_none_or(Value::is_null) {
                            continue;
                        }
                        let id = value
                            .and_then(Value::as_str)
                            .ok_or_else(|| invalid("malformed foreign ID"))?;
                        let target = ids
                            .get(id)
                            .ok_or_else(|| invalid("dangling inbound ID reference"))?;
                        if row_value(row, tenant) != row_value(target, tenant) {
                            return Err(invalid("cross-tenant inbound ID reference"));
                        }
                    }
                }
            }
            bicdb_sql::audit_application_unique_scope_references(
                db,
                &r.relation,
                &r.primary_key,
                tenant,
                business,
            )
            .map_err(|e| invalid(format!("SQL unique-scope reference audit: {e}")))?;
        }
    }
    Ok((fingerprint(&(rows, sql_catalog))?, fingerprint(&policies)?))
}

/// Scope-only index changes may alter ordinary known-ID action logic, but may
/// never detach/modify posting freezes, settlement locks, durable worker actor
/// guards, schedules, outbox bindings or their transitive implementation.
fn validate_scope_execution_boundaries(old: &RuntimeCatalog, new: &RuntimeCatalog) -> Result<()> {
    let mut roots = Vec::<(String, String)>::new();
    for (name, before) in &old.packages {
        let after = new
            .packages
            .get(name)
            .ok_or_else(|| invalid("scope execution removes application"))?;
        let a = application_contract(before);
        let b = application_contract(after);
        if fingerprint(&a.workers)? != fingerprint(&b.workers)?
            || fingerprint(&a.schedules)? != fingerprint(&b.schedules)?
            || fingerprint(&before.package.scripts)? != fingerprint(&after.package.scripts)?
        {
            return Err(invalid(
                "scope-only transition changes worker/schedule/script authority",
            ));
        }
        match (&a.application_program, &b.application_program) {
            (None, None) => {
                if !a.workers.is_empty() || a.schedules.iter().any(|s| !s.export.is_empty()) {
                    return Err(invalid(
                        "scope-only transition cannot inspect native guard worker exports",
                    ));
                }
            }
            (Some(ap), Some(bp)) => {
                let mut x = serde_json::to_value(ap)?;
                let mut y = serde_json::to_value(bp)?;
                x.as_object_mut()
                    .ok_or_else(|| invalid("invalid program boundary"))?
                    .remove("callables");
                y.as_object_mut()
                    .ok_or_else(|| invalid("invalid program boundary"))?
                    .remove("callables");
                // Includes mutation_bindings, durable jobs/events/realtime queues,
                // workflow/outbox topology, service/secret/client/provider authority,
                // limits and security metadata. Only callable action bodies differ.
                if fingerprint(&x)? != fingerprint(&y)? {
                    return Err(invalid("scope-only transition changes native mutation/worker/outbox authority bindings"));
                }
                for binding in &ap.mutation_bindings {
                    roots.push((name.clone(), binding.callable.clone()));
                }
                for worker in &a.workers {
                    roots.push((name.clone(), worker.export.clone()));
                }
                for schedule in &a.schedules {
                    if !schedule.export.is_empty() {
                        roots.push((name.clone(), schedule.export.clone()));
                    }
                }
            }
            _ => {
                return Err(invalid(
                    "scope-only transition adds/removes native behavior program",
                ))
            }
        }
    }
    let mut visited = BTreeSet::new();
    while let Some((name, callable)) = roots.pop() {
        if !visited.insert((name.clone(), callable.clone())) {
            continue;
        }
        let before = old
            .packages
            .get(&name)
            .ok_or_else(|| invalid("native guard dependency provider missing"))?;
        let after = new
            .packages
            .get(&name)
            .ok_or_else(|| invalid("native guard dependency provider removed"))?;
        let ap = application_contract(before)
            .application_program
            .as_ref()
            .ok_or_else(|| invalid("uninspectable native guard provider"))?;
        let bp = application_contract(after)
            .application_program
            .as_ref()
            .ok_or_else(|| invalid("native guard provider program removed"))?;
        let body = ap
            .callables
            .get(&callable)
            .ok_or_else(|| invalid("native guard/worker callable missing"))?;
        if bp.callables.get(&callable) != Some(body) {
            return Err(invalid(format!(
                "scope-only transition changes native guard/worker callable {name}::{callable}"
            )));
        }
        let mut targets = BTreeSet::new();
        collect_guard_call_targets(&serde_json::to_value(body)?, &mut targets);
        for target in targets {
            if ap.callables.contains_key(&target) {
                roots.push((name.clone(), target));
            } else if let Some(binding) = ap.service_bindings.get(&target) {
                // Binding topology/parameters/propagation were compared above;
                // follow the exact typed provider method as well, including a
                // different package in this same coherent transition envelope.
                roots.push((normalized(&binding.dependency), binding.method.clone()));
            } else {
                return Err(invalid(format!(
                    "unresolved native guard dependency {name}::{target}"
                )));
            }
        }
    }
    Ok(())
}

fn collect_guard_call_targets(value: &Value, targets: &mut BTreeSet<String>) {
    match value {
        Value::Object(object) => {
            // JSON data inside Literal is not executable AST.
            if object.get("op").and_then(Value::as_str) == Some("literal") {
                return;
            }
            if object.get("op").and_then(Value::as_str) == Some("call")
                && matches!(
                    object.get("kind").and_then(Value::as_str),
                    Some("function" | "action")
                )
            {
                if let Some(target) = object.get("target").and_then(Value::as_str) {
                    targets.insert(target.to_string());
                }
            }
            for value in object.values() {
                collect_guard_call_targets(value, targets);
            }
        }
        Value::Array(items) => {
            for value in items {
                collect_guard_call_targets(value, targets);
            }
        }
        _ => {}
    }
}
