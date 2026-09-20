//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

fn validate_application_module_catalog(catalog: &RuntimeCatalog) -> Result<()> {
    let mut modules =
        BTreeMap::<String, (&str, semver::Version, ApplicationModuleContractV1)>::new();
    let mut stable_keys = BTreeMap::<String, String>::new();
    let mut qualified_names = BTreeMap::<String, String>::new();
    for (application, snapshot) in &catalog.packages {
        let Some(contract) = application_module_contract(&snapshot.package)? else {
            continue;
        };
        let version = semver::Version::parse(&contract.semantic_version).map_err(|error| {
            AppRuntimeError::InvalidPackage(format!(
                "application module `{}` has an invalid semantic version: {error}",
                contract.namespace
            ))
        })?;
        if modules
            .insert(
                contract.namespace.clone(),
                (application, version, contract.clone()),
            )
            .is_some()
        {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "application module namespace `{}` is already active in this Cell",
                contract.namespace
            )));
        }
        for record_type in &contract.record_types {
            for (label, key, owners) in [
                ("stable key", &record_type.stable_key, &mut stable_keys),
                (
                    "qualified name",
                    &record_type.qualified_name,
                    &mut qualified_names,
                ),
            ] {
                if let Some(owner) = owners.insert(key.clone(), contract.namespace.clone()) {
                    return Err(AppRuntimeError::InvalidPackage(format!(
                        "application record type {label} `{key}` is owned by both `{owner}` and `{}`",
                        contract.namespace
                    )));
                }
            }
        }
    }

    for (_, _, contract) in modules.values() {
        let self_position = contract
            .install_order
            .iter()
            .position(|namespace| namespace == &contract.namespace)
            .expect("signed module contract validated during staging");
        for dependency in &contract.install_order[..self_position] {
            if dependency.starts_with("hub.") {
                continue;
            }
            if !modules.contains_key(dependency) {
                return Err(AppRuntimeError::NotReady(format!(
                    "application module `{}` requires installation-order predecessor `{dependency}` in this Cell",
                    contract.namespace
                )));
            }
        }
        for requirement in &contract.requires {
            if requirement.namespace.starts_with("hub.") {
                continue;
            }
            let Some((_, active_version, _)) = modules.get(&requirement.namespace) else {
                return Err(AppRuntimeError::NotReady(format!(
                    "application module `{}` requires `{}` {} in this Cell",
                    contract.namespace, requirement.namespace, requirement.version
                )));
            };
            let version_requirement = semver::VersionReq::parse(&requirement.version)
                .expect("signed module requirement validated during staging");
            if !version_requirement.matches(active_version) {
                return Err(AppRuntimeError::NotReady(format!(
                    "application module `{}` requires `{}` {}, but active version is {}",
                    contract.namespace, requirement.namespace, requirement.version, active_version
                )));
            }
        }
    }
    Ok(())
}

fn validate_application_module_upgrade(
    previous: &PackageSnapshot,
    candidate: &PackageSnapshot,
) -> Result<()> {
    let (previous_contract, candidate_contract) = match (
        application_module_contract(&previous.package)?,
        application_module_contract(&candidate.package)?,
    ) {
        (Some(previous), Some(candidate)) => (previous, candidate),
        (None, None) => return Ok(()),
        _ => {
            return Err(AppRuntimeError::InvalidPackage(
                "an installed application cannot add or remove its application module identity during upgrade"
                    .to_string(),
            ));
        }
    };
    let previous_version = semver::Version::parse(&previous_contract.semantic_version)
        .expect("active application module was verified");
    let candidate_version = semver::Version::parse(&candidate_contract.semantic_version)
        .expect("staged application module was verified");
    if previous_contract.namespace != candidate_contract.namespace
        || candidate_version < previous_version
    {
        return Err(AppRuntimeError::InvalidPackage(
            "application module namespace is immutable and semantic versions are monotonic"
                .to_string(),
        ));
    }
    for previous_record_type in &previous_contract.record_types {
        let candidate_record_type = candidate_contract
            .record_types
            .iter()
            .find(|record_type| record_type.stable_key == previous_record_type.stable_key)
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "application module upgrade removes DocType `{}`",
                    previous_record_type.stable_key
                ))
            })?;
        if candidate_record_type.qualified_name != previous_record_type.qualified_name
            || candidate_record_type.owner != previous_record_type.owner
            || candidate_record_type.scope_level != previous_record_type.scope_level
            || candidate_record_type.schema_version < previous_record_type.schema_version
        {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "application record type `{}` changes stable ownership, scope, or regresses its schema",
                previous_record_type.stable_key
            )));
        }
    }
    Ok(())
}

fn ensure_application_module_can_disable(
    catalog: &RuntimeCatalog,
    application: &str,
) -> Result<()> {
    let Some(snapshot) = catalog.packages.get(application) else {
        return Ok(());
    };
    let Some(contract) = application_module_contract(&snapshot.package)? else {
        return Ok(());
    };
    if contract.system {
        return Err(AppRuntimeError::CapabilityDenied(format!(
            "system application module `{}` cannot be disabled",
            contract.namespace
        )));
    }
    for dependent in catalog.packages.values() {
        let Some(dependent_contract) = application_module_contract(&dependent.package)? else {
            continue;
        };
        if dependent_contract
            .requires
            .iter()
            .any(|requirement| requirement.namespace == contract.namespace)
        {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "application module `{}` is required by `{}`",
                contract.namespace, dependent_contract.namespace
            )));
        }
    }
    Ok(())
}

fn validate_application_batch_order(candidates: &[(String, Arc<PackageSnapshot>)]) -> Result<()> {
    let mut positions = BTreeMap::new();
    let mut contracts = Vec::new();
    for (position, (_, snapshot)) in candidates.iter().enumerate() {
        if let Some(contract) = application_module_contract(&snapshot.package)? {
            positions.insert(contract.namespace.clone(), position);
            contracts.push((position, contract));
        }
    }
    for (position, contract) in contracts {
        for requirement in contract.requires {
            if positions
                .get(&requirement.namespace)
                .is_some_and(|dependency_position| *dependency_position >= position)
            {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "batch activation must place application dependency `{}` before `{}`",
                    requirement.namespace, contract.namespace
                )));
            }
        }
    }
    Ok(())
}

impl ApplicationRuntime {
    pub(crate) fn execute_signed_blob_http(
        &self,
        method: &str,
        namespace: &str,
        key: &str,
        expires: i64,
        signed_method: &str,
        download_name: Option<&str>,
        signature: &str,
        content_type: Option<&str>,
        body: &[u8],
    ) -> Result<(u16, Vec<(String, String)>, Vec<u8>)> {
        if !method.eq_ignore_ascii_case(signed_method)
            || !matches!(method.to_ascii_uppercase().as_str(), "GET" | "PUT")
        {
            return Err(AppRuntimeError::Authentication(
                "signed blob URL method does not match the request".to_string(),
            ));
        }
        let (application_name, declaration, contract) = {
            let active = self.active.read();
            active
                .packages
                .values()
                .find_map(|snapshot| {
                    let manifest = &snapshot.package.manifest;
                    let application = manifest.application.as_deref()?;
                    let contract = application.application_program.as_ref()?.blob.as_ref()?;
                    application.blobs.iter().find_map(|declaration| {
                        (application_blob_provider_namespace(
                            &manifest.identity.name,
                            &declaration.namespace,
                        ) == namespace)
                            .then_some((
                                manifest.identity.name.clone(),
                                declaration.clone(),
                                contract.clone(),
                            ))
                    })
                })
                .ok_or_else(|| {
                    AppRuntimeError::NotFound(
                        "signed blob URL references no active namespace".to_string(),
                    )
                })?
        };
        if !declaration.allow_signed_urls {
            return Err(AppRuntimeError::CapabilityDenied(
                "signed URLs are disabled for this blob namespace".to_string(),
            ));
        }
        if !contract.signed_methods.contains(signed_method) {
            return Err(AppRuntimeError::CapabilityDenied(
                "signed blob method is outside the active BicDB application contract".to_string(),
            ));
        }
        self.services.blobs.authorize_named_url(
            namespace,
            key,
            expires,
            signed_method,
            download_name,
            signature,
        )?;
        match method.to_ascii_uppercase().as_str() {
            "GET" => {
                let record = self
                    .services
                    .blobs
                    .get_named(namespace, key)?
                    .ok_or_else(|| AppRuntimeError::NotFound("blob not found".to_string()))?;
                if record.bytes.len() as u64 > declaration.max_blob_bytes {
                    return Err(AppRuntimeError::ResourceExhausted(
                        "blob exceeds its signed namespace limit".to_string(),
                    ));
                }
                let mut headers = vec![
                    (
                        "content-type".to_string(),
                        record
                            .metadata
                            .content_type
                            .unwrap_or_else(|| "application/octet-stream".to_string()),
                    ),
                    ("etag".to_string(), record.metadata.sha256),
                    ("x-content-type-options".to_string(), "nosniff".to_string()),
                ];
                if let Some(download_name) = download_name.filter(|name| !name.is_empty()) {
                    validate_blob_download_name(download_name)?;
                    headers.push((
                        "content-disposition".to_string(),
                        format!("attachment; filename=\"{download_name}\""),
                    ));
                }
                self.record_signed_blob_evidence(
                    &application_name,
                    "carrier.blob.signed_get",
                    key,
                    record.bytes.len() as u64,
                );
                Ok((200, headers, record.bytes))
            }
            "PUT" => {
                if body.len() as u64 > declaration.max_blob_bytes {
                    return Err(AppRuntimeError::ResourceExhausted(
                        "blob upload exceeds its signed namespace limit".to_string(),
                    ));
                }
                if content_type.is_some_and(|content_type| {
                    !declaration.content_types.is_empty()
                        && !declaration.content_types.contains(content_type)
                }) {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "blob upload content type is outside the signed namespace policy"
                            .to_string(),
                    ));
                }
                let metadata = self.services.blobs.put_named(
                    namespace,
                    key,
                    body,
                    content_type,
                    &BTreeMap::new(),
                    declaration.require_scan,
                )?;
                self.record_signed_blob_evidence(
                    &application_name,
                    "carrier.blob.signed_put",
                    key,
                    metadata.size,
                );
                Ok((
                    201,
                    vec![
                        ("content-type".to_string(), "application/json".to_string()),
                        ("etag".to_string(), metadata.sha256.clone()),
                    ],
                    serde_json::to_vec(&json!({
                        "key": key,
                        "size_bytes": metadata.size,
                        "content_type": metadata.content_type,
                        "sha256": metadata.sha256.clone(),
                        "etag": metadata.sha256,
                        "last_modified": metadata.last_modified,
                    }))?,
                ))
            }
            _ => unreachable!("signed method validated above"),
        }
    }

    pub(crate) fn record_signed_blob_evidence(
        &self,
        application: &str,
        action: &str,
        key: &str,
        size: u64,
    ) {
        let trace_id = uuid::Uuid::new_v4().to_string();
        let actor = ActorContext {
            service_id: Some(format!("{application}:signed-blob")),
            trace_id: trace_id.clone(),
            deadline_unix_ms: crate::host::now_ms().saturating_add(60_000),
            ..ActorContext::default()
        };
        let subject = carrier_idempotency_sha256(key.as_bytes());
        let fields = BTreeMap::from([
            (
                "application".to_string(),
                Value::String(application.to_string()),
            ),
            ("trace_id".to_string(), Value::String(trace_id)),
            ("size_bytes".to_string(), Value::from(size)),
        ]);
        self.services
            .observability
            .record(crate::ObservabilityEvent::Evidence {
                actor: actor.clone(),
                control: action.to_string(),
                outcome: "success".to_string(),
                fields: fields.clone(),
            });
        self.services
            .observability
            .record(crate::ObservabilityEvent::Audit {
                actor,
                action: action.to_string(),
                subject,
                fields,
            });
    }

    pub fn new(
        db: Arc<RwLock<BicDb>>,
        config: ApplicationHostConfig,
        verifier: PackageVerifier,
        services: InvocationServices,
    ) -> Result<Self> {
        Self::initialize(db, config, verifier, services, true)
    }

    pub(crate) fn initialize(
        db: Arc<RwLock<BicDb>>,
        config: ApplicationHostConfig,
        verifier: PackageVerifier,
        mut services: InvocationServices,
        restore: bool,
    ) -> Result<Self> {
        config.validate()?;
        let verifier = verifier.restrict_package_bytes(config.max_package_bytes);
        let wasm_engine = WasmEngine::pooled(config.wasm.clone())?;
        services.max_idempotency_entries = config.idempotency_entries;
        services.max_route_cache_entries = config.route_cache_entries;
        services.max_runtime_cache_entries = config.runtime_cache_entries;
        fs::create_dir_all(config.package_root.join("packages"))?;
        fs::create_dir_all(config.package_root.join("snapshots"))?;
        {
            let mut database = db.write();
            database.create_collection("__bicdb_app_idempotency")?;
            database.set_mutation_policy(
                "__bicdb_app_idempotency",
                MutationPolicy::grants_required(),
            )?;
            database.create_collection("__bicdb_app_route_cache")?;
            database.set_mutation_policy(
                "__bicdb_app_route_cache",
                MutationPolicy::grants_required(),
            )?;
            database.create_collection("__bicdb_app_runtime_cache")?;
            database.set_mutation_policy(
                "__bicdb_app_runtime_cache",
                MutationPolicy::grants_required(),
            )?;
            database.create_collection("__bicdb_app_security")?;
            database.set_mutation_policy(
                "__bicdb_app_security",
                MutationPolicy::grants_required().with_immutable_fields(["application", "kind"]),
            )?;
            database.create_collection("__bicdb_app_workflows")?;
            database.set_mutation_policy(
                "__bicdb_app_workflows",
                MutationPolicy::grants_required()
                    .with_version_field("revision")
                    .with_immutable_fields([
                        "application",
                        "workflow",
                        "tenant_id",
                        "workspace_id",
                        "created_at_ms",
                    ]),
            )?;
            database.create_collection("__bicdb_app_schedules")?;
            database.set_mutation_policy(
                "__bicdb_app_schedules",
                MutationPolicy::grants_required()
                    .with_version_field("revision")
                    .with_immutable_fields(["application", "schedule", "created_at_ms"]),
            )?;
            database.create_collection("__bicdb_app_audit")?;
            database.set_mutation_policy(
                "__bicdb_app_audit",
                MutationPolicy::grants_required().append_only(),
            )?;
            database.create_collection("__bicdb_app_migrations")?;
            database.set_mutation_policy(
                "__bicdb_app_migrations",
                MutationPolicy::grants_required().append_only(),
            )?;
        }
        let runtime = Self {
            db,
            config,
            verifier,
            services,
            staged: RwLock::new(BTreeMap::new()),
            active: RwLock::new(Arc::new(RuntimeCatalog::default())),
            lifecycle: Mutex::new(()),
            history: Mutex::new(BTreeMap::new()),
            supervisors: Mutex::new(BTreeMap::new()),
            wasm_engine,
            module_cache: Mutex::new(CompiledModuleCache::default()),
        };
        if restore {
            runtime.restore_active_snapshot()?;
        }
        Ok(runtime)
    }

    /// Constructs a first-party runtime whose plugin-service broker is bound
    /// back to this exact active snapshot catalog.
    pub fn new_shared(
        db: Arc<RwLock<BicDb>>,
        config: ApplicationHostConfig,
        verifier: PackageVerifier,
        mut services: InvocationServices,
    ) -> Result<Arc<Self>> {
        let dispatcher = Arc::new(RuntimeServiceDispatcher::default());
        services.services = dispatcher.clone();
        // Bind the service broker before restored workers and schedules become
        // active. Otherwise a fast restored worker can observe an unbound
        // dispatcher during process startup.
        let runtime = Arc::new(Self::initialize(db, config, verifier, services, false)?);
        dispatcher.bind(&runtime);
        runtime.restore_active_snapshot()?;
        Ok(runtime)
    }

    /// Read an unverified package using the same limits as install and restore.
    pub fn read_package_file(&self, path: impl AsRef<Path>) -> Result<ApplicationPackage> {
        self.verifier.read_package_file(path)
    }

    pub fn validate(&self, package: &ApplicationPackage) -> Result<PackageVerification> {
        let verification = self.verifier.verify(package)?;
        validate_runtime_profile(package)?;
        self.compile_modules(package)?;
        Ok(verification)
    }

    pub fn install(&self, package: ApplicationPackage) -> Result<PackageVerification> {
        self.stage(package)
    }

    pub fn stage(&self, package: ApplicationPackage) -> Result<PackageVerification> {
        let _lifecycle = self
            .lifecycle
            .lock()
            .expect("application lifecycle poisoned");
        let _file_lock = LifecycleFileLock::acquire(&self.config.package_root)?;
        self.stage_inner(package)
    }

    pub(crate) fn stage_inner(&self, package: ApplicationPackage) -> Result<PackageVerification> {
        let verification = self.verifier.verify(&package)?;
        validate_runtime_profile(&package)?;
        let modules = self.compile_modules(&package)?;
        persist_package(
            &self.config.package_root,
            &verification.package_sha256,
            &package,
        )?;
        let name = normalized(&package.manifest.identity.name);
        let package = Arc::new(package);
        let manifest = Arc::new(package.manifest.clone());
        let carrier_plan = ApplicationExecutionPlan::prepare(Arc::clone(&manifest));
        let snapshot = Arc::new(PackageSnapshot {
            generation: 0,
            package_hash: verification.package_sha256.clone(),
            package,
            verification: verification.clone(),
            state: ApplicationPackageState::Staged,
            readiness: ApplicationReadiness::default(),
            activated_at_ms: 0,
            modules,
            manifest,
            carrier_plan,
        });
        let mut next_staged = self.staged.read().clone();
        next_staged.insert(name, snapshot);
        let active = self.active.read().clone();
        let history = self
            .history
            .lock()
            .expect("package history poisoned")
            .clone();
        persist_runtime_state(&self.config.package_root, &active, &next_staged, &history)?;
        *self.staged.write() = next_staged;
        Ok(verification)
    }

    pub fn activate(&self, application: &str) -> Result<ApplicationReadiness> {
        let _lifecycle = self
            .lifecycle
            .lock()
            .expect("application lifecycle poisoned");
        let _file_lock = LifecycleFileLock::acquire(&self.config.package_root)?;
        self.activate_inner(application)
    }

    /// Atomically activate a dependency-ordered set of staged packages.
    ///
    /// Exact dependency locks make many upgrades impossible one package at a
    /// time: activating the provider first breaks its old dependents, while
    /// activating a dependent first cannot bind the new provider. Batch
    /// activation validates every candidate against the prospective catalog,
    /// prepares all schema and supervisor state, and persists one generation.
    pub fn activate_batch(
        &self,
        applications: &[String],
    ) -> Result<BTreeMap<String, ApplicationReadiness>> {
        if applications.is_empty() {
            return Err(AppRuntimeError::InvalidPackage(
                "batch activation requires at least one application".to_string(),
            ));
        }
        let _lifecycle = self
            .lifecycle
            .lock()
            .expect("application lifecycle poisoned");
        let _file_lock = LifecycleFileLock::acquire(&self.config.package_root)?;
        let staged = self.staged.read().clone();
        let mut names = BTreeSet::new();
        let mut candidates = Vec::with_capacity(applications.len());
        for application in applications {
            let name = normalized(application);
            if !names.insert(name.clone()) {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "batch activation repeats application `{application}`"
                )));
            }
            let candidate = staged.get(&name).cloned().ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "application `{application}` has no staged package"
                ))
            })?;
            candidates.push((name, candidate));
        }

        let current = self.active.read().clone();
        let mut projected = RuntimeCatalog {
            generation: current.generation.saturating_add(1),
            packages: current.packages.clone(),
            routes: BTreeMap::new(),
        };
        for (name, candidate) in &candidates {
            projected.packages.insert(name.clone(), candidate.clone());
        }
        validate_application_batch_order(&candidates)?;
        for (name, candidate) in &candidates {
            if let Some(previous) = current.packages.get(name) {
                validate_application_module_upgrade(previous, candidate)?;
                validate_forward_schema_evolution(previous, candidate)?;
            }
        }
        validate_application_module_catalog(&projected)?;
        for snapshot in projected.packages.values() {
            self.validate_bindings(snapshot, &projected)?;
        }

        let mut schema_receipts = Vec::with_capacity(candidates.len());
        for (_, candidate) in &candidates {
            match self.apply_schema(candidate) {
                Ok(receipt) => schema_receipts.push(receipt),
                Err(error) => {
                    return Err(self.compensate_schema_batch(&schema_receipts, error));
                }
            }
        }

        let mut prepared = BTreeMap::new();
        let mut readiness_by_name = BTreeMap::new();
        for (name, candidate) in &candidates {
            let supervisors = match self.prepare_supervisors(candidate) {
                Ok(supervisors) => supervisors,
                Err(error) => {
                    stop_supervisor_sets(prepared);
                    return Err(self.compensate_schema_batch(&schema_receipts, error));
                }
            };
            let mut readiness = self.calculate_readiness(candidate, &projected);
            readiness.workers_healthy = supervisors.healthy();
            readiness.schedules_healthy = supervisors.healthy();
            readiness.ready &= readiness.workers_healthy && readiness.schedules_healthy;
            if !readiness.ready {
                supervisors.stop();
                stop_supervisor_sets(prepared);
                return Err(self.compensate_schema_batch(
                    &schema_receipts,
                    AppRuntimeError::NotReady(readiness.issues.join("; ")),
                ));
            }
            readiness_by_name.insert(name.clone(), readiness);
            prepared.insert(name.clone(), supervisors);
        }

        let activated_at_ms = crate::host::now_ms();
        let mut next = RuntimeCatalog {
            generation: projected.generation,
            packages: current.packages.clone(),
            routes: BTreeMap::new(),
        };
        let mut next_history = self
            .history
            .lock()
            .expect("package history poisoned")
            .clone();
        for (name, candidate) in &candidates {
            let activated = Arc::new(PackageSnapshot {
                generation: next.generation,
                state: ApplicationPackageState::Active,
                readiness: readiness_by_name[name].clone(),
                activated_at_ms,
                ..candidate.as_ref().clone()
            });
            if let Some(previous) = next.packages.insert(name.clone(), activated) {
                let versions = next_history.entry(name.clone()).or_default();
                versions.push_back(previous);
                while versions.len() > self.config.max_history {
                    versions.pop_front();
                }
            }
        }
        if let Err(error) = build_routes(&mut next) {
            stop_supervisor_sets(prepared);
            return Err(self.compensate_schema_batch(&schema_receipts, error));
        }
        let mut next_staged = staged;
        for (name, _) in &candidates {
            next_staged.remove(name);
        }
        if let Err(error) = persist_runtime_state(
            &self.config.package_root,
            &next,
            &next_staged,
            &next_history,
        ) {
            stop_supervisor_sets(prepared);
            return Err(self.compensate_schema_batch(&schema_receipts, error));
        }

        *self.active.write() = Arc::new(next);
        *self.history.lock().expect("package history poisoned") = next_history;
        *self.staged.write() = next_staged;
        let previous = {
            let mut running = self
                .supervisors
                .lock()
                .expect("application supervisors poisoned");
            candidates
                .iter()
                .filter_map(|(name, _)| running.remove(name))
                .collect::<Vec<_>>()
        };
        for supervisors in previous {
            supervisors.stop();
        }
        for supervisors in prepared.values() {
            supervisors.activate();
        }
        self.supervisors
            .lock()
            .expect("application supervisors poisoned")
            .extend(prepared);
        Ok(readiness_by_name)
    }

    pub(crate) fn activate_inner(&self, application: &str) -> Result<ApplicationReadiness> {
        let name = normalized(application);
        let candidate = self.staged.read().get(&name).cloned().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "application `{application}` has no staged package"
            ))
        })?;
        let current = self.active.read().clone();
        if let Some(previous) = current.packages.get(&name) {
            validate_application_module_upgrade(previous, &candidate)?;
            validate_forward_schema_evolution(previous, &candidate)?;
        }
        let mut projected = RuntimeCatalog {
            generation: current.generation.saturating_add(1),
            packages: current.packages.clone(),
            routes: BTreeMap::new(),
        };
        projected.packages.insert(name.clone(), candidate.clone());
        validate_application_module_catalog(&projected)?;
        for snapshot in projected.packages.values() {
            self.validate_bindings(snapshot, &projected)?;
        }
        let schema_receipt = self.apply_schema(&candidate)?;
        let prepared_supervisors = match self.prepare_supervisors(&candidate) {
            Ok(supervisors) => supervisors,
            Err(error) => return Err(self.compensate_schema(&schema_receipt, error)),
        };
        let mut readiness = self.calculate_readiness(&candidate, &projected);
        readiness.workers_healthy = prepared_supervisors.healthy();
        readiness.schedules_healthy = prepared_supervisors.healthy();
        readiness.ready &= readiness.workers_healthy && readiness.schedules_healthy;
        if !readiness.ready {
            prepared_supervisors.stop();
            return Err(self.compensate_schema(
                &schema_receipt,
                AppRuntimeError::NotReady(readiness.issues.join("; ")),
            ));
        }

        let mut next = RuntimeCatalog {
            generation: current.generation.saturating_add(1),
            packages: current.packages.clone(),
            routes: BTreeMap::new(),
        };
        let activated = Arc::new(PackageSnapshot {
            generation: next.generation,
            state: ApplicationPackageState::Active,
            readiness: readiness.clone(),
            activated_at_ms: crate::host::now_ms(),
            ..candidate.as_ref().clone()
        });
        let previous_snapshot = next.packages.insert(name.clone(), activated.clone());
        if let Err(error) = build_routes(&mut next) {
            prepared_supervisors.stop();
            return Err(self.compensate_schema(&schema_receipt, error));
        }
        let mut next_history = self
            .history
            .lock()
            .expect("package history poisoned")
            .clone();
        if let Some(previous) = previous_snapshot {
            let versions = next_history.entry(name.clone()).or_default();
            versions.push_back(previous);
            while versions.len() > self.config.max_history {
                versions.pop_front();
            }
        }
        let mut next_staged = self.staged.read().clone();
        next_staged.remove(&name);
        if let Err(error) = persist_runtime_state(
            &self.config.package_root,
            &next,
            &next_staged,
            &next_history,
        ) {
            prepared_supervisors.stop();
            return Err(self.compensate_schema(&schema_receipt, error));
        }
        *self.active.write() = Arc::new(next);
        *self.history.lock().expect("package history poisoned") = next_history;
        *self.staged.write() = next_staged;
        let previous = self
            .supervisors
            .lock()
            .expect("application supervisors poisoned")
            .remove(&name);
        if let Some(previous) = previous {
            previous.stop();
        }
        prepared_supervisors.activate();
        self.supervisors
            .lock()
            .expect("application supervisors poisoned")
            .insert(name, prepared_supervisors);
        Ok(readiness)
    }

    pub fn upgrade(&self, package: ApplicationPackage) -> Result<ApplicationReadiness> {
        let _lifecycle = self
            .lifecycle
            .lock()
            .expect("application lifecycle poisoned");
        let _file_lock = LifecycleFileLock::acquire(&self.config.package_root)?;
        let name = package.manifest.identity.name.clone();
        self.stage_inner(package)?;
        self.activate_inner(&name)
    }

    pub fn rollback(&self, application: &str) -> Result<ApplicationReadiness> {
        let _lifecycle = self
            .lifecycle
            .lock()
            .expect("application lifecycle poisoned");
        let _file_lock = LifecycleFileLock::acquire(&self.config.package_root)?;
        let name = normalized(application);
        let mut next_history = self
            .history
            .lock()
            .expect("package history poisoned")
            .clone();
        let previous = next_history
            .get_mut(&name)
            .and_then(VecDeque::pop_back)
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "application `{application}` has no rollback snapshot"
                ))
            })?;
        let current = self.active.read().clone();
        let active = current.packages.get(&name).ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!("application `{application}` is not active"))
        })?;
        validate_schema_rollback(active, &previous)?;
        let mut projected = RuntimeCatalog {
            generation: current.generation.saturating_add(1),
            packages: current.packages.clone(),
            routes: BTreeMap::new(),
        };
        projected.packages.insert(name.clone(), previous.clone());
        validate_application_module_catalog(&projected)?;
        for snapshot in projected.packages.values() {
            self.validate_bindings(snapshot, &projected)?;
        }
        // Safe additive schema remains installed. The signed rollback plan
        // proves that the previous code contract can run against that superset.
        let readiness = self.calculate_readiness(&previous, &projected);
        if !readiness.ready {
            return Err(AppRuntimeError::NotReady(readiness.issues.join("; ")));
        }
        let prepared_supervisors = self.prepare_supervisors(&previous)?;
        let mut next = RuntimeCatalog {
            generation: current.generation.saturating_add(1),
            packages: current.packages.clone(),
            routes: BTreeMap::new(),
        };
        let rollback = Arc::new(PackageSnapshot {
            generation: next.generation,
            state: ApplicationPackageState::Active,
            readiness: readiness.clone(),
            activated_at_ms: crate::host::now_ms(),
            ..previous.as_ref().clone()
        });
        if let Some(displaced) = next.packages.insert(name.clone(), rollback) {
            let versions = next_history.entry(name.clone()).or_default();
            versions.push_back(displaced);
            while versions.len() > self.config.max_history {
                versions.pop_front();
            }
        }
        if let Err(error) = build_routes(&mut next) {
            prepared_supervisors.stop();
            return Err(error);
        }
        let staged = self.staged.read().clone();
        if let Err(error) =
            persist_runtime_state(&self.config.package_root, &next, &staged, &next_history)
        {
            prepared_supervisors.stop();
            return Err(error);
        }
        *self.active.write() = Arc::new(next);
        *self.history.lock().expect("package history poisoned") = next_history;
        let old = self
            .supervisors
            .lock()
            .expect("application supervisors poisoned")
            .remove(&name);
        if let Some(old) = old {
            old.stop();
        }
        prepared_supervisors.activate();
        self.supervisors
            .lock()
            .expect("application supervisors poisoned")
            .insert(name, prepared_supervisors);
        Ok(readiness)
    }

    pub fn disable(&self, application: &str) -> Result<()> {
        let _lifecycle = self
            .lifecycle
            .lock()
            .expect("application lifecycle poisoned");
        let _file_lock = LifecycleFileLock::acquire(&self.config.package_root)?;
        let name = normalized(application);
        let current = self.active.read().clone();
        if !current.packages.contains_key(&name) {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "application `{application}` is not active"
            )));
        }
        if self.config.required_packages.contains(&name) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "required application `{application}` cannot be disabled"
            )));
        }
        ensure_application_module_can_disable(&current, &name)?;
        ensure_no_active_dependents(&current, &name)?;
        let mut next = RuntimeCatalog {
            generation: current.generation.saturating_add(1),
            packages: current.packages.clone(),
            routes: BTreeMap::new(),
        };
        let mut next_history = self
            .history
            .lock()
            .expect("package history poisoned")
            .clone();
        if let Some(previous) = next.packages.remove(&name) {
            let versions = next_history.entry(name.clone()).or_default();
            versions.push_back(previous);
            while versions.len() > self.config.max_history {
                versions.pop_front();
            }
        }
        build_routes(&mut next)?;
        let staged = self.staged.read().clone();
        persist_runtime_state(&self.config.package_root, &next, &staged, &next_history)?;
        *self.active.write() = Arc::new(next);
        *self.history.lock().expect("package history poisoned") = next_history;
        if let Some(supervisors) = self
            .supervisors
            .lock()
            .expect("application supervisors poisoned")
            .remove(&name)
        {
            supervisors.stop();
        }
        Ok(())
    }

    pub fn remove(&self, application: &str) -> Result<()> {
        let _lifecycle = self
            .lifecycle
            .lock()
            .expect("application lifecycle poisoned");
        let _file_lock = LifecycleFileLock::acquire(&self.config.package_root)?;
        let name = normalized(application);
        if self.active.read().packages.contains_key(&name) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "disable application `{application}` before removal"
            )));
        }
        let mut staged = self.staged.read().clone();
        staged.remove(&name);
        let mut history = self
            .history
            .lock()
            .expect("package history poisoned")
            .clone();
        history.remove(&name);
        let active = self.active.read().clone();
        persist_runtime_state(&self.config.package_root, &active, &staged, &history)?;
        *self.staged.write() = staged;
        *self.history.lock().expect("package history poisoned") = history;
        Ok(())
    }

    pub fn list(&self) -> Vec<RuntimeDiagnostic> {
        let active = self.active.read();
        let mut diagnostics = active
            .packages
            .values()
            .map(|snapshot| diagnostic(snapshot))
            .collect::<Vec<_>>();
        diagnostics.extend(
            self.staged
                .read()
                .values()
                .map(|snapshot| diagnostic(snapshot)),
        );
        diagnostics
    }

    pub fn inspect(&self, application: &str) -> Result<RuntimeDiagnostic> {
        let name = normalized(application);
        self.active
            .read()
            .packages
            .get(&name)
            .cloned()
            .or_else(|| self.staged.read().get(&name).cloned())
            .map(|snapshot| diagnostic(&snapshot))
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "application `{application}` is neither active nor staged"
                ))
            })
    }

    /// Bounded runtime/cache state used by operator diagnostics and regression
    /// gates. No package contents, identities, or provider credentials are
    /// exposed.
    pub fn performance_snapshot(&self) -> ApplicationPerformanceSnapshot {
        let active = self.active.read();
        let cache = self
            .module_cache
            .lock()
            .expect("compiled module cache poisoned");
        ApplicationPerformanceSnapshot {
            active_packages: active.packages.len(),
            prepared_application_programs: active
                .packages
                .values()
                .filter(|snapshot| snapshot.carrier_plan.is_some())
                .count(),
            compiled_module_cache_entries: cache.entries.len(),
            compiled_module_cache_capacity: self.config.module_cache_entries,
            compiled_module_cache_hits: cache.hits,
            compiled_module_cache_misses: cache.misses,
            compiled_module_cache_evictions: cache.evictions,
            wasm_pool_capacity: self.config.wasm.max_pooled_instances,
        }
    }

    pub fn readiness(&self) -> ApplicationReadiness {
        let active = self.active.read();
        let mut readiness = ApplicationReadiness {
            ready: true,
            signatures_valid: true,
            dependencies_bound: true,
            migrations_complete: true,
            contracts_loaded: true,
            routes_active: true,
            workers_healthy: true,
            schedules_healthy: true,
            providers_available: true,
            schema_compatible: true,
            issues: Vec::new(),
        };
        for required in &self.config.required_packages {
            if !active.packages.contains_key(required) {
                readiness.ready = false;
                readiness
                    .issues
                    .push(format!("required package `{required}` is inactive"));
            }
        }
        for snapshot in active.packages.values() {
            if !snapshot.readiness.ready {
                readiness.ready = false;
                readiness.issues.extend(snapshot.readiness.issues.clone());
            }
        }
        let supervisors = self
            .supervisors
            .lock()
            .expect("application supervisors poisoned");
        for (name, snapshot) in &active.packages {
            let application = snapshot.package.manifest.application.as_deref().unwrap();
            let set = supervisors.get(name);
            for worker in application.workers.iter().filter(|worker| worker.required) {
                if !set
                    .and_then(|set| set.workers.get(&worker.name))
                    .is_some_and(|control| control.healthy.load(Ordering::Acquire))
                {
                    readiness.ready = false;
                    readiness.workers_healthy = false;
                    readiness.issues.push(format!(
                        "required worker `{name}/{}` is unhealthy",
                        worker.name
                    ));
                }
            }
            for schedule in application
                .schedules
                .iter()
                .filter(|schedule| schedule.required)
            {
                if !set
                    .and_then(|set| set.schedules.get(&schedule.name))
                    .is_some_and(|control| control.healthy.load(Ordering::Acquire))
                {
                    readiness.ready = false;
                    readiness.schedules_healthy = false;
                    readiness.issues.push(format!(
                        "required schedule `{name}/{}` is unhealthy",
                        schedule.name
                    ));
                }
            }
        }
        readiness
    }

    pub fn dependency_graph(&self) -> BTreeMap<String, Vec<String>> {
        self.active
            .read()
            .packages
            .iter()
            .map(|(name, snapshot)| {
                (
                    name.clone(),
                    snapshot
                        .package
                        .manifest
                        .dependencies
                        .iter()
                        .map(|dependency| dependency.name.clone())
                        .collect(),
                )
            })
            .collect()
    }

    pub fn routes(&self) -> Vec<String> {
        self.active
            .read()
            .routes
            .keys()
            .map(|(method, path)| format!("{method} {path}"))
            .collect()
    }

    pub fn services(&self) -> Vec<String> {
        self.active
            .read()
            .packages
            .values()
            .flat_map(|snapshot| {
                snapshot
                    .package
                    .manifest
                    .application
                    .as_deref()
                    .unwrap()
                    .service_exports
                    .iter()
                    .map(|service| {
                        format!(
                            "{}:{}@{}",
                            snapshot.package.manifest.identity.name,
                            service.service,
                            service.version
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn workers(&self) -> Vec<String> {
        definitions(self, |application| {
            application
                .workers
                .iter()
                .map(|worker| worker.name.clone())
                .collect()
        })
    }

    pub fn schedules(&self) -> Vec<String> {
        definitions(self, |application| {
            application
                .schedules
                .iter()
                .map(|schedule| schedule.name.clone())
                .collect()
        })
    }

    pub fn doctor(&self) -> Vec<RuntimeDiagnostic> {
        self.list()
    }

    pub(crate) fn record_observability(&self, event: crate::ObservabilityEvent) {
        self.services.observability.record(event);
    }

    pub fn export_diagnostics(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(&json!({
            "node_id": self.config.node_id,
            "readiness": self.readiness(),
            "packages": self.list(),
            "dependencies": self.dependency_graph(),
            "routes": self.routes(),
            "services": self.services(),
            "workers": self.workers(),
            "schedules": self.schedules(),
            "performance": self.performance_snapshot(),
            "observability": {
                "events": self.services.observability.snapshot(),
                "dropped": self.services.observability.dropped(),
            },
        }))?)
    }

    pub fn durable_observations(&self, application: &str, limit: usize) -> Result<Vec<Value>> {
        if limit == 0 || limit > 10_000 {
            return Err(AppRuntimeError::InvalidRequest(
                "durable observation limit must be in 1..=10000".to_string(),
            ));
        }
        let mut records = self
            .db
            .read()
            .scan_collection("__bicdb_app_audit")?
            .into_iter()
            .filter(|record| {
                record
                    .metadata
                    .get("plugin")
                    .and_then(Value::as_str)
                    .is_some_and(|plugin| plugin.eq_ignore_ascii_case(application))
            })
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            left.metadata
                .get("created_at_ms")
                .and_then(Value::as_i64)
                .cmp(&right.metadata.get("created_at_ms").and_then(Value::as_i64))
                .then_with(|| left.id.cmp(&right.id))
        });
        let start = records.len().saturating_sub(limit);
        Ok(records
            .into_iter()
            .skip(start)
            .map(|record| {
                let mut value = record.metadata;
                if let Some(object) = value.as_object_mut() {
                    object.insert("id".to_string(), Value::String(record.id));
                }
                value
            })
            .collect())
    }

    pub fn invoke_resource(
        &self,
        application: &str,
        resource: &str,
        actor: ActorContext,
        request: ResourceRequest,
    ) -> Result<ResourceResponse> {
        let started = std::time::Instant::now();
        let snapshot = self.active_snapshot(application)?;
        let contract = snapshot
            .package
            .manifest
            .application
            .as_deref()
            .unwrap()
            .resources
            .iter()
            .find(|contract| contract.name.eq_ignore_ascii_case(resource))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "application `{application}` has no resource `{resource}`"
                ))
            })?;
        let db = self.db.read();
        let mut host = CapabilityHost::new_validated(
            &db,
            Arc::clone(&snapshot.manifest),
            actor.clone(),
            self.services.clone(),
        )?;
        let result = execute_resource_operation_with_mutation_hook(
            &mut host,
            &contract,
            request,
            |host, transaction, mutation| {
                execute_carrier_mutation_bindings(host, transaction, &contract, mutation)
            },
        );
        if actor_trace_sampled(&actor) {
            self.services
                .observability
                .record(crate::ObservabilityEvent::Trace {
                    actor,
                    name: "bicdb.resource_invocation".to_string(),
                    fields: BTreeMap::from([
                        (
                            "application".to_string(),
                            Value::String(application.to_string()),
                        ),
                        ("resource".to_string(), Value::String(resource.to_string())),
                        (
                            "elapsed_us".to_string(),
                            Value::from(started.elapsed().as_micros() as u64),
                        ),
                        ("success".to_string(), Value::Bool(result.is_ok())),
                    ]),
                });
        }
        result
    }

    /// Executes a cross-tenant support operation exclusively through the
    /// trusted in-process host. SQL and HTTP callers cannot construct the
    /// required bypass-bearing [`SecurityContext`].
    pub fn invoke_resource_as_internal_admin(
        &self,
        application: &str,
        resource: &str,
        authorization: &SecurityContext,
        target_tenant: &str,
        target_workspace: Option<&str>,
        request: ResourceRequest,
    ) -> Result<ResourceResponse> {
        let bypass = authorization.bypass_policy.as_ref().ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(
                "cross-tenant resource access requires an audited internal bypass reason"
                    .to_string(),
            )
        })?;
        if authorization.authentication_strength != AuthenticationStrength::Internal
            || !authorization.scopes.contains("carrier:internal_admin")
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "cross-tenant resource access requires an internal host principal with carrier:internal_admin scope"
                    .to_string(),
            ));
        }
        let target_tenant = target_tenant.trim();
        if target_tenant.is_empty() || bypass.reason.trim().is_empty() {
            return Err(AppRuntimeError::CapabilityDenied(
                "cross-tenant resource access requires a target tenant and non-empty bypass reason"
                    .to_string(),
            ));
        }
        let mut policy_attributes = BTreeMap::from([
            (
                "bicdb.internal_bypass_reason".to_string(),
                bypass.reason.clone(),
            ),
            (
                "bicdb.authorizing_tenant".to_string(),
                authorization.tenant_id.clone(),
            ),
        ]);
        if let Some(session_id) = &authorization.session_id {
            policy_attributes.insert("bicdb.authorizing_session".to_string(), session_id.clone());
        }
        let actor = ActorContext {
            user_id: Some(authorization.user_id.clone()),
            service_id: None,
            client_id: authorization.client_id.clone(),
            acting_client_id: authorization.client_id.clone(),
            authentication_method: Some("internal".to_string()),
            roles: authorization.roles.clone(),
            scopes: authorization.scopes.clone(),
            tenant_id: Some(target_tenant.to_string()),
            workspace_id: target_workspace.map(str::to_string),
            organization_id: Some(target_tenant.to_string()),
            session_id: authorization.session_id.clone(),
            delegation_chain: Vec::new(),
            assurance_level: Some("internal_bypass".to_string()),
            request_origin: Some("carrier_internal_host".to_string()),
            trace_id: uuid::Uuid::new_v4().to_string(),
            correlation_id: None,
            causation_id: None,
            deadline_unix_ms: crate::host::now_ms() + 30_000,
            policy_attributes,
        };
        self.invoke_resource(application, resource, actor, request)
    }

    pub(crate) fn execute_carrier_callable(
        &self,
        application: &str,
        callable: &str,
        actor: ActorContext,
        globals: BTreeMap<String, Value>,
        arguments: Vec<(Option<String>, Value)>,
    ) -> Result<Value> {
        let snapshot = self.active_snapshot(application)?;
        let plan = snapshot.carrier_plan.as_ref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "application `{application}` has no BicDB application behavior program"
            ))
        })?;
        let application_manifest = plan
            .manifest
            .application
            .as_deref()
            .expect("active application package");
        let program = application_manifest
            .application_program
            .as_ref()
            .expect("prepared BicDB application plan has a behavior program");
        if !program.callables.contains_key(callable) {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "application `{application}` has no BicDB application callable `{callable}`"
            )));
        }
        let database = self.db.read();
        let started = std::time::Instant::now();
        let trace_actor = actor.clone();
        let observability = self.services.observability.clone();
        let mut capability_host = CapabilityHost::new_validated(
            &database,
            Arc::clone(&plan.manifest),
            actor,
            self.services.clone(),
        )?;
        let mut host = CapabilityApplicationProgramHost::from_plan(&mut capability_host, plan);
        let result = execute_application_program(program, callable, globals, arguments, &mut host);
        if actor_trace_sampled(&trace_actor) {
            observability.record(crate::ObservabilityEvent::Trace {
                actor: trace_actor,
                name: "bicdb.carrier_callable".to_string(),
                fields: BTreeMap::from([
                    (
                        "application".to_string(),
                        Value::String(plan.manifest.identity.name.clone()),
                    ),
                    ("callable".to_string(), Value::String(callable.to_string())),
                    (
                        "elapsed_us".to_string(),
                        Value::from(started.elapsed().as_micros() as u64),
                    ),
                    ("success".to_string(), Value::Bool(result.is_ok())),
                ]),
            });
        }
        result
    }

    pub fn evaluate_application(
        &self,
        application: &str,
        evaluation: &str,
        actor: ActorContext,
    ) -> Result<ApplicationEvaluationResult> {
        let snapshot = self.active_snapshot(application)?;
        let contract = snapshot
            .package
            .manifest
            .application
            .as_deref()
            .and_then(|application| application.application_program.as_ref())
            .and_then(|program| program.evaluations.as_ref())
            .and_then(|evaluations| evaluations.evaluations.get(evaluation))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::NotFound(format!(
                    "BicDB application evaluation `{evaluation}` is not installed"
                ))
            })?;
        let cases =
            self.services
                .evaluations
                .cases(application, &contract.provider, contract.max_cases)?;
        let program = snapshot
            .package
            .manifest
            .application
            .as_deref()
            .and_then(|application| application.application_program.as_ref());
        let mut evaluation_actor =
            carrier_evaluation_actor(program, contract.auth.as_ref(), &actor)?;
        evaluation_actor.deadline_unix_ms = evaluation_actor.deadline_unix_ms.min(
            crate::host::now_ms()
                .saturating_add(i64::try_from(contract.timeout_ms).unwrap_or(i64::MAX)),
        );
        let mut passed_cases = 0u64;
        for case in &cases {
            if crate::host::now_ms() >= evaluation_actor.deadline_unix_ms {
                return Err(AppRuntimeError::Timeout(format!(
                    "BicDB application evaluation `{evaluation}` exceeded its deadline"
                )));
            }
            let case = crate::http::coerce_carrier_parameter_value(
                case,
                &contract.case_type,
                "evaluation",
                evaluation,
            )?;
            let passed = self.execute_carrier_callable(
                application,
                &contract.case_callable,
                evaluation_actor.clone(),
                BTreeMap::new(),
                vec![(None, case)],
            )?;
            match passed.as_bool() {
                Some(true) => passed_cases += 1,
                Some(false) => {}
                None => {
                    return Err(AppRuntimeError::InvalidPackage(format!(
                        "BicDB application evaluation `{evaluation}` grade did not return Bool"
                    )));
                }
            }
        }
        let total_cases = cases.len() as u64;
        let pass_rate = passed_cases as f64 / total_cases as f64;
        let requirement = self.execute_carrier_callable(
            application,
            &contract.require_callable,
            evaluation_actor.clone(),
            BTreeMap::new(),
            vec![
                (None, Value::from(pass_rate)),
                (None, Value::from(total_cases)),
                (None, Value::from(passed_cases)),
            ],
        )?;
        let requirement_passed = requirement.as_bool().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application evaluation `{evaluation}` requirement did not return Bool"
            ))
        })?;
        self.services
            .observability
            .record(crate::ObservabilityEvent::Evidence {
                actor: evaluation_actor,
                control: "carrier.evaluation".to_string(),
                outcome: if requirement_passed {
                    "passed".to_string()
                } else {
                    "failed".to_string()
                },
                fields: BTreeMap::from([
                    (
                        "evaluation".to_string(),
                        Value::String(evaluation.to_string()),
                    ),
                    ("passed_cases".to_string(), Value::from(passed_cases)),
                    ("total_cases".to_string(), Value::from(total_cases)),
                ]),
            });
        Ok(ApplicationEvaluationResult {
            evaluation: evaluation.to_string(),
            passed_cases,
            total_cases,
            pass_rate,
            requirement_passed,
        })
    }

    /// Execute one compiler-signed BicDB application scenario or property test.
    ///
    /// This is an operator-only entry point. Authored `http.request` calls are
    /// dispatched through the ordinary host router on a sibling thread so the
    /// active database read lease cannot become a recursive lock, and no test
    /// capability is exposed to application routes or guest WASM.
    pub fn test_application(
        &self,
        application: &str,
        test: &str,
        actor: ActorContext,
    ) -> Result<ApplicationTestResult> {
        self.test_carrier_cases(application, test, actor, None)
    }

    /// Execute exactly one signed property-test case.
    ///
    /// BicDB application uses this entry point with a fresh ephemeral database per case,
    /// matching the generated Rust test harness's state-isolation contract.
    pub fn test_application_case(
        &self,
        application: &str,
        test: &str,
        actor: ActorContext,
        case_index: u32,
    ) -> Result<ApplicationTestResult> {
        self.test_carrier_cases(application, test, actor, Some(case_index))
    }

    pub(crate) fn test_carrier_cases(
        &self,
        application: &str,
        test: &str,
        actor: ActorContext,
        case_index: Option<u32>,
    ) -> Result<ApplicationTestResult> {
        let snapshot = self.active_snapshot(application)?;
        let plan = snapshot.carrier_plan.as_ref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "application `{application}` has no BicDB application behavior program"
            ))
        })?;
        let application_manifest = plan
            .manifest
            .application
            .as_deref()
            .expect("active application package");
        let program = application_manifest
            .application_program
            .as_ref()
            .expect("prepared BicDB application plan has a behavior program");
        let contract = program
            .tests
            .as_ref()
            .and_then(|tests| tests.tests.get(test))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::NotFound(format!(
                    "BicDB application test `{test}` is not installed"
                ))
            })?;
        let mut execution_actor =
            carrier_evaluation_actor(Some(program), contract.auth.as_ref(), &actor)?;
        if contract.auth.is_some() {
            execution_actor.authentication_method = Some("carrier_test".to_string());
        }
        execution_actor.deadline_unix_ms = execution_actor.deadline_unix_ms.min(
            crate::host::now_ms()
                .saturating_add(i64::try_from(contract.timeout_ms).unwrap_or(i64::MAX)),
        );
        let request_actor = contract.auth.as_ref().map(|_| execution_actor.clone());
        let (first_case, end_case) = match case_index {
            Some(index) if index < contract.cases => (index, index.saturating_add(1)),
            Some(index) => {
                return Err(AppRuntimeError::InvalidRequest(format!(
                "BicDB application test `{test}` case index {index} is outside its signed {} cases",
                contract.cases
            )))
            }
            None => (0, contract.cases),
        };
        let total_cases = u64::from(end_case.saturating_sub(first_case));
        let mut passed_cases = 0_u64;
        let mut failed_case = None;
        for index in first_case..end_case {
            if crate::host::now_ms() >= execution_actor.deadline_unix_ms {
                return Err(AppRuntimeError::Timeout(format!(
                    "BicDB application test `{test}` exceeded its deadline"
                )));
            }
            let case = contract
                .case_type
                .as_ref()
                .map(|value_type| {
                    let generated = carrier_test_case(value_type, contract.seed, index);
                    crate::http::coerce_carrier_parameter_value(
                        &generated,
                        value_type,
                        "test case",
                        test,
                    )
                })
                .transpose()?;
            let arguments = case
                .clone()
                .map(|value| vec![(None, value)])
                .unwrap_or_default();
            let callback = |target: &str, arguments: Vec<(Option<String>, Value)>| {
                carrier_test_http_call(self, request_actor.clone(), target, arguments)
            };
            let database = self.db.read();
            let mut capability_host = CapabilityHost::new_validated(
                &database,
                Arc::clone(&plan.manifest),
                execution_actor.clone(),
                self.services.clone(),
            )?;
            let mut host = CapabilityApplicationProgramHost::from_plan(&mut capability_host, plan)
                .with_test_http(&callback);
            let passed = execute_application_program(
                program,
                &contract.callable,
                BTreeMap::new(),
                arguments,
                &mut host,
            )?
            .as_bool()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "BicDB application test `{test}` callable did not return Bool"
                ))
            })?;
            drop(host);
            drop(capability_host);
            drop(database);
            if passed {
                passed_cases = passed_cases.saturating_add(1);
            } else {
                failed_case = case;
                break;
            }
        }
        let passed = passed_cases == total_cases;
        if contract.emit_evidence {
            self.services
                .observability
                .record(crate::ObservabilityEvent::Evidence {
                    actor: execution_actor,
                    control: "carrier.test".to_string(),
                    outcome: if passed { "passed" } else { "failed" }.to_string(),
                    fields: BTreeMap::from([
                        ("test".to_string(), Value::String(test.to_string())),
                        ("passed_cases".to_string(), Value::from(passed_cases)),
                        ("total_cases".to_string(), Value::from(total_cases)),
                        ("signed_cases".to_string(), Value::from(contract.cases)),
                        (
                            "case_index".to_string(),
                            case_index.map(Value::from).unwrap_or(Value::Null),
                        ),
                        ("seed".to_string(), Value::from(contract.seed)),
                    ]),
                });
        }
        Ok(ApplicationTestResult {
            test: test.to_string(),
            passed_cases,
            total_cases,
            seed: contract.seed,
            passed,
            failed_case,
        })
    }

    pub(crate) fn execute_carrier_security_route(
        &self,
        application: &str,
        export: &str,
        actor: ActorContext,
        body: Value,
        session_id: Option<&str>,
    ) -> Result<(u16, Value)> {
        let snapshot = self.active_snapshot(application)?;
        let plan = snapshot.carrier_plan.as_ref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "application `{application}` has no BicDB application security program"
            ))
        })?;
        let application_manifest = plan
            .manifest
            .application
            .as_deref()
            .expect("active application package");
        let program = application_manifest
            .application_program
            .as_ref()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "application `{application}` has no BicDB application security program"
                ))
            })?;
        let database = self.db.read();
        let mut capability = CapabilityHost::new_validated(
            &database,
            Arc::clone(&plan.manifest),
            actor.clone(),
            self.services.clone(),
        )?;
        let mut host = CapabilityApplicationProgramHost::from_plan(&mut capability, plan);
        let body_field = |name: &str| {
            body.get(name).cloned().ok_or_else(|| {
                AppRuntimeError::InvalidRequest(format!(
                    "BicDB application security route requires JSON field `{name}`"
                ))
            })
        };
        match export {
            "__carrier_security_register" => {
                let user = host.auth_register(
                    "auth.register",
                    &[
                        (Some("email".to_string()), body_field("email")?),
                        (Some("name".to_string()), body_field("name")?),
                        (Some("password".to_string()), body_field("password")?),
                    ],
                )?;
                let tokens = host.auth_issue_tokens(
                    "auth.issue_tokens",
                    &[(
                        None,
                        user.get("id").cloned().ok_or_else(|| {
                            AppRuntimeError::InvalidPackage(
                                "BicDB application registration returned no user id".to_string(),
                            )
                        })?,
                    )],
                )?;
                Ok((201, tokens))
            }
            "__carrier_security_login" => {
                let user = host.auth_login(
                    "auth.login",
                    &[
                        (Some("email".to_string()), body_field("email")?),
                        (Some("password".to_string()), body_field("password")?),
                    ],
                )?;
                let tokens = host.auth_issue_tokens(
                    "auth.issue_tokens",
                    &[(
                        None,
                        user.get("id").cloned().ok_or_else(|| {
                            AppRuntimeError::InvalidPackage(
                                "BicDB application login returned no user id".to_string(),
                            )
                        })?,
                    )],
                )?;
                Ok((200, tokens))
            }
            "__carrier_security_refresh" => Ok((
                200,
                host.auth_refresh(&carrier_string(
                    body_field("refresh_token")?,
                    "auth.refresh",
                )?)?,
            )),
            "__carrier_security_logout" => {
                let session_id = actor.session_id.as_deref().ok_or_else(|| {
                    AppRuntimeError::Authentication(
                        "logout requires a BicDB application-issued access token session"
                            .to_string(),
                    )
                })?;
                Ok((200, host.revoke_auth_session(&actor, session_id)?))
            }
            "__carrier_security_sessions" => Ok((200, host.auth_sessions(&actor)?)),
            "__carrier_security_session_revoke" => Ok((
                200,
                host.revoke_auth_session(
                    &actor,
                    session_id.ok_or_else(|| {
                        AppRuntimeError::InvalidRequest(
                            "session revoke route has no session id".to_string(),
                        )
                    })?,
                )?,
            )),
            _ => Err(AppRuntimeError::InvalidPackage(format!(
                "unknown BicDB application security route export `{export}`"
            ))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn execute_carrier_callable_idempotent(
        &self,
        application: &str,
        callable: &str,
        actor: ActorContext,
        globals: BTreeMap<String, Value>,
        arguments: Vec<(Option<String>, Value)>,
        contract: &IdempotencyContract,
        method: &str,
        template: &str,
        raw_key: &str,
        normalized_request: &Value,
    ) -> Result<IdempotentApplicationExecution> {
        let user_id = actor.user_id.as_deref().ok_or_else(|| {
            AppRuntimeError::Authentication(
                "idempotent routes require an authenticated user".to_string(),
            )
        })?;
        let raw_key = raw_key.trim();
        if raw_key.is_empty() {
            return Err(AppRuntimeError::MissingIdempotencyKey(format!(
                "{} header is required for this route",
                contract.header
            )));
        }
        if raw_key.len() > contract.max_key_bytes as usize {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "{} exceeds {} bytes",
                contract.header, contract.max_key_bytes
            )));
        }

        let snapshot = self.active_snapshot(application)?;
        let plan = snapshot.carrier_plan.as_ref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "application `{application}` has no BicDB application behavior program"
            ))
        })?;
        let application_manifest = plan
            .manifest
            .application
            .as_deref()
            .expect("active application package");
        let program = application_manifest
            .application_program
            .as_ref()
            .expect("prepared BicDB application plan has a behavior program");
        if !program.callables.contains_key(callable) {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "application `{application}` has no BicDB application callable `{callable}`"
            )));
        }

        let scope = format!(
            "{method} {template}:user:{user_id}:tenant:{}:workspace:{}",
            actor.tenant_id.as_deref().unwrap_or_default(),
            actor.workspace_id.as_deref().unwrap_or_default()
        );
        let identity = format!("{application}\0{scope}\0{raw_key}");
        let storage_key = carrier_idempotency_sha256(identity.as_bytes());
        let canonical_request = carrier_canonical_json(normalized_request);
        let request_sha256 = carrier_idempotency_sha256(&serde_json::to_vec(&canonical_request)?);
        let expires_at_ms = crate::host::now_ms().saturating_add(
            i64::try_from(contract.ttl_seconds.saturating_mul(1000)).unwrap_or(i64::MAX),
        );

        let database = self.db.read();
        let mut capability_host = CapabilityHost::new_validated(
            &database,
            Arc::clone(&plan.manifest),
            actor,
            self.services.clone(),
        )?;
        let mut program_host =
            CapabilityApplicationProgramHost::from_plan(&mut capability_host, plan);
        program_host.begin_transaction("read_committed")?;
        let transaction = program_host
            .current_transaction()
            .expect("idempotency transaction was opened");
        match program_host.host.begin_idempotency(
            transaction,
            &storage_key,
            &request_sha256,
            expires_at_ms,
        ) {
            Ok(Some(value)) => {
                program_host.commit_transaction()?;
                return Ok(IdempotentApplicationExecution {
                    value,
                    replayed: true,
                    scope,
                });
            }
            Ok(None) => {}
            Err(error) => {
                let _ = program_host.rollback_transaction();
                return Err(error);
            }
        }

        let execution =
            execute_application_program(program, callable, globals, arguments, &mut program_host);
        let value = match execution {
            Ok(value) => value,
            Err(error) => {
                let _ = program_host.rollback_transaction();
                return Err(error);
            }
        };
        if let Err(error) = program_host.host.complete_idempotency(
            transaction,
            &storage_key,
            &request_sha256,
            expires_at_ms,
            value.clone(),
        ) {
            let _ = program_host.rollback_transaction();
            return Err(error);
        }
        program_host.commit_transaction()?;
        Ok(IdempotentApplicationExecution {
            value,
            replayed: false,
            scope,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn execute_carrier_callable_cached(
        &self,
        application: &str,
        callable: &str,
        actor: ActorContext,
        globals: BTreeMap<String, Value>,
        arguments: Vec<(Option<String>, Value)>,
        contract: &RouteCacheContract,
        method: &str,
        template: &str,
        normalized_request: &Value,
        vary_by_user: bool,
    ) -> Result<Value> {
        let snapshot = self.active_snapshot(application)?;
        let plan = snapshot.carrier_plan.as_ref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "application `{application}` has no BicDB application behavior program"
            ))
        })?;
        let application_manifest = plan
            .manifest
            .application
            .as_deref()
            .expect("active application package");
        let program = application_manifest
            .application_program
            .as_ref()
            .expect("prepared BicDB application plan has a behavior program");
        if !program.callables.contains_key(callable) {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "application `{application}` has no BicDB application callable `{callable}`"
            )));
        }

        let scoped_user_id = vary_by_user.then(|| actor.user_id.clone()).flatten();
        let scope = carrier_canonical_json(&json!({
            "application": application,
            "method": method,
            "path": template,
            "user_id": scoped_user_id.as_deref(),
            "tenant_id": actor.tenant_id.as_deref(),
            "workspace_id": actor.workspace_id.as_deref(),
            "request": normalized_request,
        }));
        let storage_key = carrier_idempotency_sha256(&serde_json::to_vec(&scope)?);
        let expires_at_ms = crate::host::now_ms().saturating_add(
            i64::try_from(contract.ttl_seconds.saturating_mul(1000)).unwrap_or(i64::MAX),
        );

        let database = self.db.read();
        let mut capability_host = CapabilityHost::new_validated(
            &database,
            Arc::clone(&plan.manifest),
            actor,
            self.services.clone(),
        )?;
        let mut program_host =
            CapabilityApplicationProgramHost::from_plan(&mut capability_host, plan);
        program_host.begin_transaction("read_committed")?;
        let transaction = program_host
            .current_transaction()
            .expect("route cache transaction was opened");
        match program_host.host.read_route_cache(
            transaction,
            &storage_key,
            scoped_user_id.as_deref(),
        ) {
            Ok(Some(value)) => {
                program_host.commit_transaction()?;
                return Ok(value);
            }
            Ok(None) => {}
            Err(error) => {
                let _ = program_host.rollback_transaction();
                return Err(error);
            }
        }

        let execution =
            execute_application_program(program, callable, globals, arguments, &mut program_host);
        let value = match execution {
            Ok(value) => value,
            Err(error) => {
                let _ = program_host.rollback_transaction();
                return Err(error);
            }
        };
        if let Err(error) = program_host.host.write_route_cache(
            transaction,
            &storage_key,
            expires_at_ms,
            value.clone(),
            scoped_user_id.as_deref(),
        ) {
            let _ = program_host.rollback_transaction();
            return Err(error);
        }
        program_host.commit_transaction()?;
        Ok(value)
    }

    pub(crate) fn invoke_route_service(
        &self,
        caller: &str,
        dependency: &str,
        service: &str,
        method: &str,
        actor: ActorContext,
        payload: Value,
    ) -> Result<Value> {
        let caller_snapshot = self.active_snapshot(caller)?;
        if !caller_snapshot
            .package
            .manifest
            .capabilities
            .contains(&bicdb_extension::ExtensionCapability::PluginServices)
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "HTTP route caller did not declare plugin-services capability".to_string(),
            ));
        }
        let caller_application = caller_snapshot
            .package
            .manifest
            .application
            .as_deref()
            .expect("active application package");
        let import = caller_application
            .service_imports
            .iter()
            .find(|import| import.name == dependency && import.service == service)
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(
                    "HTTP route references an undeclared plugin service".to_string(),
                )
            })?;
        if caller_application.max_call_depth < 2 {
            return Err(AppRuntimeError::CapabilityDenied(
                "plugin call-depth limit exceeded".to_string(),
            ));
        }
        if caller.eq_ignore_ascii_case(dependency) && !import.allow_reentrant {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "plugin dependency cycle or forbidden reentrancy through `{dependency}`"
            )));
        }
        let database = self.db.read();
        self.invoke_plugin_service(PluginServiceCall {
            caller: caller.to_string(),
            caller_manifest: Arc::new(caller_snapshot.package.manifest.clone()),
            dependency: dependency.to_string(),
            service: service.to_string(),
            method: method.to_string(),
            payload,
            trace: vec![caller.to_ascii_lowercase(), dependency.to_ascii_lowercase()],
            deadline_unix_ms: actor.deadline_unix_ms,
            actor,
            transaction_requested: false,
            database: crate::host::InvocationDatabase::new(&database),
            transaction: None,
        })
    }

    pub fn invoke_wasm(
        &self,
        application: &str,
        module: &str,
        actor: ActorContext,
        invocation: ExtensionInvocation,
    ) -> Result<ExtensionInvocationResult> {
        let started = std::time::Instant::now();
        let module_name = module.to_string();
        let snapshot = self.active_snapshot(application)?;
        let module = snapshot.modules.get(module).cloned().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!("module `{module}` is not installed"))
        })?;
        let db = self.db.read();
        let host = CapabilityHost::new_validated(
            &db,
            Arc::clone(&snapshot.manifest),
            actor.clone(),
            self.services.clone(),
        )?;
        let result = module
            .invoke_with_host(&invocation, Box::new(host))
            .map_err(Into::into);
        if actor_trace_sampled(&actor) {
            self.services
                .observability
                .record(crate::ObservabilityEvent::Trace {
                    actor,
                    name: "bicdb.application_invocation".to_string(),
                    fields: BTreeMap::from([
                        (
                            "application".to_string(),
                            Value::String(application.to_string()),
                        ),
                        ("module".to_string(), Value::String(module_name)),
                        (
                            "elapsed_us".to_string(),
                            Value::from(started.elapsed().as_micros() as u64),
                        ),
                        ("success".to_string(), Value::Bool(result.is_ok())),
                    ]),
                });
        }
        result
    }

    pub fn invoke_export(
        &self,
        application: &str,
        target: &str,
        kind: InvocationKind,
        actor: ActorContext,
        payload: Value,
    ) -> Result<ExtensionInvocationResult> {
        let context = InvocationContext {
            role: actor.roles.iter().next().cloned(),
            tenant: actor.tenant_id.clone(),
            trace_id: Some(actor.trace_id.clone()),
            deadline_unix_ms: Some(actor.deadline_unix_ms),
            metadata: BTreeMap::from([
                (
                    "workspace_id".to_string(),
                    actor.workspace_id.clone().unwrap_or_default(),
                ),
                (
                    "correlation_id".to_string(),
                    actor.correlation_id.clone().unwrap_or_default(),
                ),
            ]),
        };
        self.invoke_wasm(
            application,
            application,
            actor,
            ExtensionInvocation {
                id: format!("app-{}", crate::host::now_ms()),
                kind,
                target: target.to_string(),
                payload,
                context,
            },
        )
    }

    pub(crate) fn active_snapshot(&self, application: &str) -> Result<Arc<PackageSnapshot>> {
        self.active
            .read()
            .packages
            .get(&normalized(application))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::NotReady(format!("application `{application}` is not active"))
            })
    }

    pub(crate) fn active_snapshots(&self) -> Vec<Arc<PackageSnapshot>> {
        self.active.read().packages.values().cloned().collect()
    }

    pub(crate) fn peek_realtime_queue(
        &self,
        queue: &str,
        from_sequence: u64,
        max: usize,
    ) -> Vec<bicdb_core::PeekedMessage> {
        self.db
            .read()
            .with_broker(|broker| broker.peek(queue, from_sequence, max))
    }

    pub(crate) fn realtime_queue_last_sequence(&self, queue: &str) -> u64 {
        self.db.read().with_broker(|broker| {
            broker
                .stats()
                .queues
                .into_iter()
                .find(|candidate| candidate.queue == queue)
                .map(|candidate| candidate.next_sequence)
                .unwrap_or_default()
        })
    }

    /// Generates OpenAPI from the exact signed contracts in the active
    /// immutable package snapshot.
    pub fn openapi(&self, application: &str) -> Result<Value> {
        let snapshot = self.active_snapshot(application)?;
        crate::generate_openapi(&snapshot.package.manifest)
    }

    pub(crate) fn take_realtime_response(
        &self,
        stream: u64,
        max_bytes: usize,
    ) -> Result<crate::RealtimeResponse> {
        self.services
            .realtime
            .take_response(stream, max_bytes)?
            .ok_or_else(|| {
                AppRuntimeError::Provider(
                    "stream handle was not created by the configured HTTP realtime provider"
                        .to_string(),
                )
            })
    }

    pub(crate) fn register_live_realtime_response(
        &self,
        scope: &str,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<crate::LiveRealtimeResponse>>> {
        self.services.realtime.register_live_response(scope)
    }

    pub(crate) fn cancel_live_realtime_response(&self, scope: &str, error: AppRuntimeError) {
        self.services.realtime.cancel_live_response(scope, error);
    }

    pub(crate) fn compile_modules(
        &self,
        package: &ApplicationPackage,
    ) -> Result<BTreeMap<String, Arc<WasmExtension>>> {
        let mut compiled = BTreeMap::new();
        for (name, bytes) in &package.modules {
            let sha256 = format!("{:x}", Sha256::digest(bytes));
            let cached = self
                .module_cache
                .lock()
                .expect("compiled module cache poisoned")
                .get(&sha256);
            let module = match cached {
                Some(module) => module,
                None => {
                    let candidate =
                        Arc::new(WasmExtension::load_with_engine(bytes, &self.wasm_engine)?);
                    self.module_cache
                        .lock()
                        .expect("compiled module cache poisoned")
                        .insert(sha256, candidate, self.config.module_cache_entries)
                }
            };
            if name == &package.manifest.identity.name
                && !embedded_manifest_matches(&package.manifest, module.manifest())
            {
                return Err(AppRuntimeError::InvalidPackage(
                    "entry module manifest differs from signed package manifest outside the \
                     package hash/signature placeholders"
                        .to_string(),
                ));
            }
            compiled.insert(name.clone(), module);
        }
        Ok(compiled)
    }

    pub(crate) fn restore_active_snapshot(&self) -> Result<()> {
        let path = self
            .config
            .package_root
            .join("snapshots")
            .join("active.json");
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let persisted: PersistedRuntimeState = serde_json::from_slice(&bytes)?;
        let mut catalog = RuntimeCatalog {
            generation: persisted.generation,
            packages: BTreeMap::new(),
            routes: BTreeMap::new(),
        };
        for (name, package_ref) in persisted.packages {
            catalog.packages.insert(
                name.clone(),
                self.load_persisted_snapshot(&name, &package_ref)?,
            );
        }
        let mut staged = BTreeMap::new();
        for (name, package_ref) in persisted.staged {
            let snapshot = self.load_persisted_snapshot(&name, &package_ref)?;
            staged.insert(
                name,
                Arc::new(PackageSnapshot {
                    state: ApplicationPackageState::Staged,
                    readiness: ApplicationReadiness::default(),
                    ..snapshot.as_ref().clone()
                }),
            );
        }
        let mut history = BTreeMap::new();
        for (name, package_refs) in persisted.history {
            let mut snapshots = VecDeque::new();
            for package_ref in package_refs {
                snapshots.push_back(self.load_persisted_snapshot(&name, &package_ref)?);
            }
            while snapshots.len() > self.config.max_history {
                snapshots.pop_front();
            }
            if !snapshots.is_empty() {
                history.insert(name, snapshots);
            }
        }
        validate_application_module_catalog(&catalog)?;
        build_routes(&mut catalog)?;
        let restored = catalog.packages.clone();
        for (name, snapshot) in restored {
            self.validate_bindings(&snapshot, &catalog)?;
            let readiness = self.calculate_readiness(&snapshot, &catalog);
            if !readiness.ready {
                return Err(AppRuntimeError::NotReady(format!(
                    "restored application `{name}` is not ready: {}",
                    readiness.issues.join("; ")
                )));
            }
            catalog.packages.insert(
                name,
                Arc::new(PackageSnapshot {
                    readiness,
                    ..snapshot.as_ref().clone()
                }),
            );
        }
        *self.active.write() = Arc::new(catalog);
        *self.staged.write() = staged;
        *self.history.lock().expect("package history poisoned") = history;
        let snapshots = self.active_snapshots();
        let mut supervisors = self
            .supervisors
            .lock()
            .expect("application supervisors poisoned");
        for snapshot in snapshots {
            let set = self.prepare_supervisors(&snapshot)?;
            set.activate();
            supervisors.insert(normalized(&snapshot.package.manifest.identity.name), set);
        }
        Ok(())
    }

    pub(crate) fn load_persisted_snapshot(
        &self,
        name: &str,
        package_ref: &PersistedPackageReference,
    ) -> Result<Arc<PackageSnapshot>> {
        let package = self.read_package_file(
            self.config
                .package_root
                .join("packages")
                .join(format!("{}.json", package_ref.hash)),
        )?;
        let verification = self.verifier.verify(&package)?;
        if verification.package_sha256 != package_ref.hash
            || !package.manifest.identity.name.eq_ignore_ascii_case(name)
        {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "persisted package `{name}` does not match its signed snapshot reference"
            )));
        }
        validate_runtime_profile(&package)?;
        let modules = self.compile_modules(&package)?;
        let package = Arc::new(package);
        let manifest = Arc::new(package.manifest.clone());
        let carrier_plan = ApplicationExecutionPlan::prepare(Arc::clone(&manifest));
        Ok(Arc::new(PackageSnapshot {
            generation: package_ref.generation,
            package_hash: package_ref.hash.clone(),
            package,
            verification,
            state: ApplicationPackageState::Active,
            readiness: ApplicationReadiness {
                ready: true,
                signatures_valid: true,
                dependencies_bound: true,
                migrations_complete: true,
                contracts_loaded: true,
                routes_active: true,
                workers_healthy: true,
                schedules_healthy: true,
                providers_available: true,
                schema_compatible: true,
                issues: Vec::new(),
            },
            activated_at_ms: package_ref.activated_at_ms,
            modules,
            manifest,
            carrier_plan,
        }))
    }

    pub(crate) fn validate_bindings(
        &self,
        candidate: &PackageSnapshot,
        current: &RuntimeCatalog,
    ) -> Result<()> {
        let mut installations = current
            .packages
            .values()
            .map(|snapshot| as_installation(snapshot))
            .collect::<Vec<_>>();
        installations.retain(|installation| {
            !installation
                .manifest
                .identity
                .name
                .eq_ignore_ascii_case(&candidate.package.manifest.identity.name)
        });
        installations.push(as_installation(candidate));
        resolve_extension_order(
            &installations,
            &[candidate.package.manifest.identity.name.clone()],
            true,
        )?;
        let lock = dependency_lock(&candidate.package)?;
        for dependency in &candidate.package.manifest.dependencies {
            let locked = lock
                .packages
                .iter()
                .find(|locked| locked.name.eq_ignore_ascii_case(&dependency.name));
            if locked.is_none() && dependency.optional {
                continue;
            }
            let locked = locked.expect("required dependency lock validated at staging");
            let provider = current.packages.iter().find_map(|(name, snapshot)| {
                name.eq_ignore_ascii_case(&dependency.name)
                    .then_some(snapshot.as_ref())
            });
            if provider.is_none() && dependency.optional {
                continue;
            }
            let provider = provider.ok_or_else(|| {
                AppRuntimeError::NotReady(format!(
                    "locked dependency `{}` is not active",
                    dependency.name
                ))
            })?;
            if provider.package.manifest.identity.version != locked.version
                || provider.package_hash != locked.package_sha256
                || locked.module_sha256.as_deref().is_some_and(|expected| {
                    provider
                        .verification
                        .module_sha256
                        .get(&provider.package.manifest.identity.name)
                        .is_none_or(|actual| actual != expected)
                })
            {
                return Err(AppRuntimeError::NotReady(format!(
                    "active dependency `{}` differs from the exact signed lock",
                    dependency.name
                )));
            }
        }
        let application = candidate.package.manifest.application.as_deref().unwrap();
        for import in &application.service_imports {
            let provider = installations
                .iter()
                .find(|installation| {
                    installation
                        .manifest
                        .identity
                        .name
                        .eq_ignore_ascii_case(&import.name)
                })
                .ok_or_else(|| {
                    AppRuntimeError::NotReady(format!(
                        "service dependency `{}` is unavailable",
                        import.name
                    ))
                })?;
            let export = provider
                .manifest
                .application
                .as_deref()
                .and_then(|application| {
                    application.service_exports.iter().find(|export| {
                        export.service == import.service
                            && export.contract_sha256 == import.contract_sha256
                    })
                })
                .ok_or_else(|| {
                    AppRuntimeError::NotReady(format!(
                        "service `{}` has no compatible provider contract",
                        import.service
                    ))
                })?;
            let requirement = semver::VersionReq::parse(&import.version)
                .map_err(|error| AppRuntimeError::InvalidPackage(error.to_string()))?;
            let version = semver::Version::parse(&export.version)
                .map_err(|error| AppRuntimeError::InvalidPackage(error.to_string()))?;
            if !requirement.matches(&version) {
                return Err(AppRuntimeError::NotReady(format!(
                    "service `{}` version {} does not satisfy {}",
                    import.service, export.version, import.version
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn compensate_schema(
        &self,
        receipt: &SchemaApplicationReceipt,
        error: AppRuntimeError,
    ) -> AppRuntimeError {
        let mut db = self.db.write();
        match receipt.rollback(&mut db) {
            Ok(()) => error,
            Err(rollback) => AppRuntimeError::InvalidPackage(format!(
                "{error}; schema compensation also failed: {rollback}"
            )),
        }
    }

    pub(crate) fn compensate_schema_batch(
        &self,
        receipts: &[SchemaApplicationReceipt],
        error: AppRuntimeError,
    ) -> AppRuntimeError {
        let mut db = self.db.write();
        let mut rollback_errors = Vec::new();
        for receipt in receipts.iter().rev() {
            if let Err(rollback) = receipt.rollback(&mut db) {
                rollback_errors.push(rollback.to_string());
            }
        }
        if rollback_errors.is_empty() {
            error
        } else {
            AppRuntimeError::InvalidPackage(format!(
                "{error}; batch schema compensation also failed: {}",
                rollback_errors.join("; ")
            ))
        }
    }

    pub(crate) fn apply_schema(
        &self,
        snapshot: &PackageSnapshot,
    ) -> Result<SchemaApplicationReceipt> {
        let application = snapshot.package.manifest.application.as_deref().unwrap();
        let mut db = self.db.write();
        let mut receipt = SchemaApplicationReceipt::default();
        let result = (|| -> Result<()> {
            for contract in &application.resources {
                validate_timeseries_intervals(contract)?;
                let existed = db
                    .collections()
                    .iter()
                    .any(|collection| collection.name == contract.relation);
                if existed {
                    receipt.policy_snapshots.push(RelationPolicySnapshot {
                        relation: contract.relation.clone(),
                        collection: db.collection_policy(&contract.relation)?,
                        mutation: db.mutation_policy(&contract.relation)?,
                    });
                }
                db.create_collection(&contract.relation)?;
                if !existed {
                    receipt.created_collections.push(contract.relation.clone());
                }
                if let Some(schema) = ensure_embedded_table_schema(
                    &mut db,
                    &contract.relation,
                    &embedded_resource_columns(contract),
                )
                .map_err(|error| {
                    AppRuntimeError::InvalidPackage(format!(
                        "resource `{}` cannot install its embedded SQL schema: {error}",
                        contract.name
                    ))
                })? {
                    receipt.embedded_sql_schemas.push(schema);
                }
                let row_policy = embedded_resource_row_policy(contract);
                let policy_receipt = reconcile_embedded_table_row_policy(
                    &mut db,
                    &contract.relation,
                    row_policy.as_ref(),
                )
                .map_err(|error| {
                    AppRuntimeError::InvalidPackage(format!(
                        "resource `{}` cannot install its embedded SQL row policy: {error}",
                        contract.name
                    ))
                })?;
                receipt.embedded_sql_schemas.push(policy_receipt);
                let mut mutation = MutationPolicy::grants_required()
                    .with_immutable_fields(contract.immutable_fields.iter().cloned());
                if let Some(field) = &contract.version_field {
                    mutation = mutation.with_version_field(field.clone());
                }
                if let Some(field) = &contract.tenant_field {
                    mutation = mutation.with_tenant_field(field.clone());
                }
                if let Some(field) = &contract.workspace_field {
                    mutation = mutation.with_workspace_field(field.clone());
                }
                if contract.audit.required {
                    mutation = mutation.with_audit();
                }
                db.set_mutation_policy(&contract.relation, mutation)?;
                if !contract.search_fields.is_empty() {
                    create_schema_index(
                        &mut db,
                        IndexDefinition {
                            name: resource_search_index_name(contract),
                            collection: contract.relation.clone(),
                            fields: contract
                                .search_fields
                                .iter()
                                .map(|field| resource_index_field(contract, field))
                                .collect::<Result<Vec<_>>>()?,
                            unique: false,
                            kind: IndexKind::FullText,
                            predicate: None,
                            exclusion: None,
                        },
                        &mut receipt,
                    )?;
                }
                for target in &contract.unique_targets {
                    create_schema_index(
                        &mut db,
                        IndexDefinition {
                            name: target.index_name.clone().unwrap_or_else(|| {
                                format!("{}_unique_{}", contract.name, target.target)
                            }),
                            collection: contract.relation.clone(),
                            fields: target
                                .fields
                                .iter()
                                .map(|field| resource_index_field(contract, field))
                                .collect::<Result<Vec<_>>>()?,
                            unique: true,
                            kind: IndexKind::BTree,
                            predicate: target
                                .predicate
                                .as_ref()
                                .map(|predicate| lower_index_predicate(contract, predicate))
                                .transpose()?,
                            exclusion: None,
                        },
                        &mut receipt,
                    )?;
                }
                for index in &contract.indexes {
                    let fields = if index.paths.is_empty() {
                        index
                            .fields
                            .iter()
                            .map(|field| resource_index_field(contract, field))
                            .collect::<Result<Vec<_>>>()?
                    } else {
                        index
                            .paths
                            .iter()
                            .map(|path| resource_index_path(contract, path))
                            .collect::<Result<Vec<_>>>()?
                    };
                    create_schema_index(
                        &mut db,
                        IndexDefinition {
                            name: index.name.clone(),
                            collection: contract.relation.clone(),
                            fields,
                            unique: false,
                            kind: match index.kind {
                                ResourceIndexKindV1::Btree => IndexKind::BTree,
                                ResourceIndexKindV1::Jsonb => IndexKind::Jsonb,
                                ResourceIndexKindV1::Array => IndexKind::Array,
                                ResourceIndexKindV1::Spatial => IndexKind::Spatial,
                            },
                            predicate: index
                                .predicate
                                .as_ref()
                                .map(|predicate| lower_index_predicate(contract, predicate))
                                .transpose()?,
                            exclusion: None,
                        },
                        &mut receipt,
                    )?;
                }
                for exclusion in &contract.exclusions {
                    let mut fields = Vec::new();
                    let mut elements = Vec::new();
                    for element in &exclusion.elements {
                        match (element.function.as_deref(), element.operator.as_str()) {
                            (None, "=") => {
                                let field = resource_index_field(contract, &element.fields[0])?;
                                fields.push(field.clone());
                                elements.push(IndexExclusionElement::Equal { field });
                            }
                            (Some(function), "&&") => {
                                let start = resource_index_field(contract, &element.fields[0])?;
                                let end = resource_index_field(contract, &element.fields[1])?;
                                fields.push(start.clone());
                                fields.push(end.clone());
                                let value_type = match function {
                                    "daterange" => IndexPredicateValueType::Date,
                                    "tsrange" | "tstzrange" => IndexPredicateValueType::Timestamp,
                                    "int4range" | "int8range" => IndexPredicateValueType::Int64,
                                    "numrange" => IndexPredicateValueType::Decimal,
                                    _ => {
                                        return Err(AppRuntimeError::InvalidPackage(format!(
                                            "unsupported exclusion range constructor `{function}`"
                                        )));
                                    }
                                };
                                elements.push(IndexExclusionElement::Overlaps {
                                    start,
                                    end,
                                    value_type,
                                });
                            }
                            _ => {
                                return Err(AppRuntimeError::InvalidPackage(format!(
                                    "invalid exclusion element in `{}`",
                                    exclusion.name
                                )));
                            }
                        }
                    }
                    create_schema_index(
                        &mut db,
                        IndexDefinition {
                            name: exclusion.name.clone(),
                            collection: contract.relation.clone(),
                            fields,
                            unique: false,
                            kind: IndexKind::BTree,
                            predicate: None,
                            exclusion: Some(IndexExclusion { elements }),
                        },
                        &mut receipt,
                    )?;
                }
                if let Some(tenant_field) = &contract.tenant_field {
                    let policy = CollectionPolicy::tenant_field(tenant_field.clone())
                        .with_read_roles(contract.required_roles.iter().cloned())
                        .with_write_roles(contract.required_roles.iter().cloned())
                        .with_delete_roles(contract.required_roles.iter().cloned())
                        .with_columns(contract.fields.iter().map(|field| field.name.clone()));
                    db.set_collection_policy(&contract.relation, policy)?;
                }
            }
            apply_migration_transformations(
                &mut db,
                &snapshot.package.manifest,
                self.services.clone(),
                &mut receipt,
            )?;
            schema_failure_point(
                &snapshot.package.manifest.identity.name,
                &snapshot.package.manifest.identity.version,
                "before_migration_ledger",
            )?;
            receipt.migration_records = record_applied_migrations(&mut db, snapshot)?;
            schema_failure_point(
                &snapshot.package.manifest.identity.name,
                &snapshot.package.manifest.identity.version,
                "after_migration_ledger",
            )?;
            Ok(())
        })();
        match result {
            Ok(()) => Ok(receipt),
            Err(error) => match receipt.rollback(&mut db) {
                Ok(()) => Err(error),
                Err(rollback) => Err(AppRuntimeError::InvalidPackage(format!(
                    "{error}; schema compensation also failed: {rollback}"
                ))),
            },
        }
    }

    pub(crate) fn calculate_readiness(
        &self,
        candidate: &PackageSnapshot,
        current: &RuntimeCatalog,
    ) -> ApplicationReadiness {
        let application = candidate.package.manifest.application.as_deref().unwrap();
        let mut readiness = ApplicationReadiness {
            ready: true,
            signatures_valid: true,
            dependencies_bound: true,
            migrations_complete: true,
            contracts_loaded: !application.resources.is_empty()
                || application
                    .routes
                    .iter()
                    .all(|route| route.resource.is_none()),
            routes_active: true,
            workers_healthy: true,
            schedules_healthy: true,
            providers_available: true,
            schema_compatible: true,
            issues: Vec::new(),
        };
        for secret in &application.secrets {
            if self.services.secrets.open(&secret.name, None).is_err() {
                readiness.providers_available = false;
                readiness
                    .issues
                    .push(format!("required secret `{}` is unavailable", secret.name));
            }
        }
        for egress in &application.egress {
            if self
                .services
                .egress
                .available(&candidate.package.manifest.identity.name, egress)
                .is_err()
            {
                readiness.providers_available = false;
                readiness.issues.push(format!(
                    "egress policy `{}` requires an unavailable provider",
                    egress.name
                ));
            }
        }
        if let Some(observability) = application
            .application_program
            .as_ref()
            .and_then(|program| program.observability.as_ref())
        {
            if self
                .services
                .observability
                .available(&candidate.package.manifest.identity.name, observability)
                .is_err()
            {
                readiness.providers_available = false;
                readiness.issues.push(format!(
                    "observability provider `{}` protocol `{:?}` is unavailable",
                    observability.provider, observability.protocol
                ));
            }
        }
        if let Some(redis) = application
            .application_program
            .as_ref()
            .and_then(|program| program.redis.as_ref())
        {
            if self
                .services
                .redis
                .available(&candidate.package.manifest.identity.name, &redis.provider)
                .is_err()
            {
                readiness.providers_available = false;
                readiness.issues.push(format!(
                    "Redis-compatible provider `{}` is unavailable",
                    redis.provider
                ));
            }
        }
        if let Some(email) = application
            .application_program
            .as_ref()
            .and_then(|program| program.email.as_ref())
        {
            if self
                .services
                .email
                .available(&candidate.package.manifest.identity.name, &email.provider)
                .is_err()
            {
                readiness.providers_available = false;
                readiness.issues.push(format!(
                    "email provider `{}` is unavailable",
                    email.provider
                ));
            }
        }
        if let Some(grpc) = application
            .application_program
            .as_ref()
            .and_then(|program| program.grpc.as_ref())
        {
            for client in grpc.clients.values() {
                if self
                    .services
                    .grpc
                    .available(&candidate.package.manifest.identity.name, &client.provider)
                    .is_err()
                {
                    readiness.providers_available = false;
                    readiness.issues.push(format!(
                        "gRPC provider `{}` is unavailable",
                        client.provider
                    ));
                }
            }
        }
        if let Some(tokenizer) = application
            .application_program
            .as_ref()
            .and_then(|program| program.tokenizer.as_ref())
        {
            for provider in tokenizer.providers.keys() {
                if self
                    .services
                    .tokenizer
                    .available(&candidate.package.manifest.identity.name, provider)
                    .is_err()
                {
                    readiness.providers_available = false;
                    readiness
                        .issues
                        .push(format!("tokenizer provider `{provider}` is unavailable"));
                }
            }
        }
        if let Some(embeddings) = application
            .application_program
            .as_ref()
            .and_then(|program| program.embeddings.as_ref())
        {
            for provider in embeddings.providers.keys() {
                if self
                    .services
                    .embeddings
                    .available(&candidate.package.manifest.identity.name, provider)
                    .is_err()
                {
                    readiness.providers_available = false;
                    readiness
                        .issues
                        .push(format!("embedding provider `{provider}` is unavailable"));
                }
            }
        }
        if let Some(llm) = application
            .application_program
            .as_ref()
            .and_then(|program| program.llm.as_ref())
        {
            for client in llm.clients.values() {
                if client.routing.is_some() {
                    continue;
                }
                if self
                    .services
                    .llm
                    .available(&candidate.package.manifest.identity.name, &client.provider)
                    .is_err()
                {
                    readiness.providers_available = false;
                    readiness
                        .issues
                        .push(format!("LLM provider `{}` is unavailable", client.provider));
                }
            }
        }
        if let Some(evaluations) = application
            .application_program
            .as_ref()
            .and_then(|program| program.evaluations.as_ref())
        {
            for evaluation in evaluations.evaluations.values() {
                if self
                    .services
                    .evaluations
                    .available(
                        &candidate.package.manifest.identity.name,
                        &evaluation.provider,
                    )
                    .is_err()
                {
                    readiness.providers_available = false;
                    readiness.issues.push(format!(
                        "evaluation provider `{}` is unavailable",
                        evaluation.provider
                    ));
                }
            }
        }
        let candidate_name = normalized(&candidate.package.manifest.identity.name);
        let mut paths = current
            .routes
            .iter()
            .filter(|(_, owner)| *owner != &candidate_name)
            .map(|(route, _)| route.clone())
            .collect::<BTreeSet<_>>();
        for route in &application.routes {
            let key = (
                format!("{:?}", route.method).to_ascii_uppercase(),
                route.template.clone(),
            );
            if !paths.insert(key.clone()) {
                readiness.routes_active = false;
                readiness
                    .issues
                    .push(format!("route conflict {} {}", key.0, key.1));
            }
            let stream_kind = if route.websocket {
                Some(bicdb_extension::abi_v2::StreamKind::WebSocket)
            } else if route.sse {
                Some(bicdb_extension::abi_v2::StreamKind::ServerSentEvents)
            } else if route.streaming_response {
                Some(bicdb_extension::abi_v2::StreamKind::Bytes)
            } else {
                None
            };
            let carrier_realtime = application.realtime.iter().any(|contract| {
                ["negotiate", "poll", "sse", "ws"]
                    .iter()
                    .any(|suffix| route.template == format!("{}/{suffix}", contract.path))
            });
            if !carrier_realtime
                && stream_kind.is_some_and(|kind| !self.services.realtime.supports(kind))
            {
                readiness.providers_available = false;
                readiness.issues.push(format!(
                    "route `{}` requires an unavailable realtime provider",
                    route.name
                ));
            }
        }
        readiness.ready = readiness.signatures_valid
            && readiness.dependencies_bound
            && readiness.migrations_complete
            && readiness.contracts_loaded
            && readiness.routes_active
            && readiness.workers_healthy
            && readiness.schedules_healthy
            && readiness.providers_available
            && readiness.schema_compatible;
        readiness
    }

    pub(crate) fn prepare_supervisors(&self, snapshot: &PackageSnapshot) -> Result<SupervisorSet> {
        let application = snapshot.package.manifest.application.as_deref().unwrap();
        let module = snapshot
            .modules
            .get(&snapshot.package.manifest.identity.name)
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage("application entry module is absent".to_string())
            })?;
        let manifest = Arc::new(snapshot.package.manifest.clone());
        let mut supervisors = SupervisorSet::default();
        for worker in &application.workers {
            let control = start_worker(
                self.db.clone(),
                module.clone(),
                manifest.clone(),
                self.services.clone(),
                worker.clone(),
                self.config.node_id.clone(),
            )?;
            supervisors.workers.insert(worker.name.clone(), control);
        }
        for schedule in &application.schedules {
            let plan = parse_schedule_plan(&schedule.schedule, &schedule.timezone)?;
            let control = start_schedule(
                self.db.clone(),
                module.clone(),
                manifest.clone(),
                self.services.clone(),
                schedule.clone(),
                plan,
                self.config.node_id.clone(),
            )?;
            supervisors.schedules.insert(schedule.name.clone(), control);
        }
        Ok(supervisors)
    }

    pub(crate) fn invoke_plugin_service(&self, call: PluginServiceCall) -> Result<Value> {
        if crate::host::now_ms() >= call.deadline_unix_ms {
            return Err(AppRuntimeError::Timeout(
                "plugin service deadline exceeded".to_string(),
            ));
        }
        let active_caller = self.active_snapshot(&call.caller)?;
        let callee = self.active_snapshot(&call.dependency)?;
        if !call
            .caller_manifest
            .identity
            .name
            .eq_ignore_ascii_case(&call.caller)
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "plugin service caller authority does not match the active caller".to_string(),
            ));
        }
        let active_caller_application = active_caller
            .package
            .manifest
            .application
            .as_deref()
            .expect("active caller application");
        let effective_caller_package = call
            .caller_manifest
            .application
            .as_deref()
            .map(|application| application.package.package_sha256.as_str());
        if call.caller_manifest.identity.version != active_caller.package.manifest.identity.version
            || effective_caller_package
                != Some(active_caller_application.package.package_sha256.as_str())
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "plugin service caller authority is stale".to_string(),
            ));
        }
        let caller_application = call.caller_manifest.application.as_deref().ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(
                "plugin service caller has no effective application authority".to_string(),
            )
        })?;
        let import = caller_application
            .service_imports
            .iter()
            .find(|import| import.name == call.dependency && import.service == call.service)
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(
                    "caller did not declare this service dependency".to_string(),
                )
            })?;
        if call.trace.len() < 2
            || !call
                .trace
                .last()
                .is_some_and(|entry| entry.eq_ignore_ascii_case(&call.dependency))
            || !call.trace[call.trace.len() - 2].eq_ignore_ascii_case(&call.caller)
            || call.trace.len() > usize::from(caller_application.max_call_depth)
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "plugin service trace violates the signed call graph".to_string(),
            ));
        }
        if call.trace[..call.trace.len() - 1]
            .iter()
            .any(|entry| entry.eq_ignore_ascii_case(&call.dependency))
            && !import.allow_reentrant
        {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "plugin dependency cycle or forbidden reentrancy through `{}`",
                call.dependency
            )));
        }
        if call.transaction_requested != call.transaction.is_some()
            || (call.transaction_requested && !import.propagate_transaction)
        {
            return Err(AppRuntimeError::CapabilityDenied(
                "service transaction propagation differs from the signed import".to_string(),
            ));
        }
        let export = callee
            .package
            .manifest
            .application
            .as_deref()
            .unwrap()
            .service_exports
            .iter()
            .find(|export| {
                export.service == call.service
                    && semver::VersionReq::parse(&import.version)
                        .ok()
                        .zip(semver::Version::parse(&export.version).ok())
                        .is_some_and(|(requirement, version)| requirement.matches(&version))
                    && export.contract_sha256 == import.contract_sha256
                    && export
                        .methods
                        .iter()
                        .any(|method| method.name == call.method)
            })
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(
                    "callee does not export the declared service method".to_string(),
                )
            })?;
        let method_contract = export
            .methods
            .iter()
            .find(|method| method.name == call.method)
            .expect("export lookup verified the service method")
            .clone();
        if import.delegated_authority != export.delegated_authority {
            return Err(AppRuntimeError::CapabilityDenied(
                "service authority delegation differs between the signed import and export"
                    .to_string(),
            ));
        }
        validate_service_request(&call.payload, &method_contract)?;
        let application_program = callee
            .package
            .manifest
            .application
            .as_deref()
            .and_then(|application| application.application_program.as_ref())
            .cloned();
        let carrier_callable = application_program
            .as_ref()
            .is_some_and(|program| program.callables.contains_key(&call.method));
        let carrier_workflow_service = application_program
            .as_ref()
            .and_then(|program| carrier_workflow_service_operation(program, &call.method));
        let carrier_resources = callee
            .package
            .manifest
            .application
            .as_deref()
            .expect("callee application")
            .resources
            .clone();
        let carrier_arguments = carrier_callable
            .then(|| {
                let arguments = match &call.payload {
                    Value::Null => Vec::new(),
                    Value::Object(values) => values
                        .iter()
                        .map(|(name, value)| (Some(name.clone()), value.clone()))
                        .collect(),
                    Value::Array(values) => {
                        values.iter().cloned().map(|value| (None, value)).collect()
                    }
                    value => vec![(None, value.clone())],
                };
                let globals = BTreeMap::from([
                    ("input".to_string(), call.payload.clone()),
                    ("actor".to_string(), serde_json::to_value(&call.actor)?),
                ]);
                Ok::<_, AppRuntimeError>((arguments, globals))
            })
            .transpose()?;
        let restricted = service_execution_manifest(
            &call.caller_manifest,
            &callee.package.manifest,
            import.delegated_authority,
        )?;
        let module = callee
            .modules
            .get(&callee.package.manifest.identity.name)
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage("callee entry module is absent".to_string())
            })?;
        let mut actor = call.actor;
        actor.deadline_unix_ms = actor.deadline_unix_ms.min(call.deadline_unix_ms);
        let context = InvocationContext {
            role: actor.roles.iter().next().cloned(),
            tenant: actor.tenant_id.clone(),
            deadline_unix_ms: Some(actor.deadline_unix_ms),
            trace_id: Some(actor.trace_id.clone()),
            metadata: BTreeMap::from([
                (
                    "workspace_id".to_string(),
                    actor.workspace_id.clone().unwrap_or_default(),
                ),
                ("caller".to_string(), call.caller),
            ]),
        };
        // The outer invocation already owns BicDB's exclusive application
        // guard. Reacquiring it here would deadlock. These host-only leases are
        // synchronous and cannot be serialized into WASM.
        let db = unsafe { call.database.as_ref() };
        let (mut host, propagated_handle) = CapabilityHost::with_service_trace_and_transaction(
            db,
            Arc::new(restricted),
            actor,
            self.services.clone(),
            call.trace,
            call.transaction
                .map(|transaction| unsafe { transaction.pointer() }),
        )?;
        if let Some(program) = application_program
            .as_ref()
            .filter(|_| carrier_workflow_service.is_some() || carrier_arguments.is_some())
        {
            let mut adapter = CapabilityApplicationProgramHost::new(
                &mut host,
                carrier_resources,
                program.service_bindings.clone(),
                program.client_bindings.clone(),
                program.secret_bindings.clone(),
                program.event_bindings.clone(),
                program.realtime_bindings.clone(),
                program.workflow_bindings.clone(),
            );
            if let Some(transaction) = propagated_handle {
                adapter
                    .transactions
                    .push(ApplicationTransactionFrame::Transaction(transaction));
            }
            if let Some((workflow, operation)) = carrier_workflow_service {
                let payload = call.payload.as_object().ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(
                        "BicDB application workflow service payload must be an object".to_string(),
                    )
                })?;
                let result = match operation {
                    ApplicationWorkflowServiceOperation::Start => {
                        let input = payload.get("input").cloned().ok_or_else(|| {
                            AppRuntimeError::InvalidRequest(
                                "BicDB application workflow service start lacks `input`"
                                    .to_string(),
                            )
                        })?;
                        let baggage = payload.get("baggage").cloned().unwrap_or_else(|| json!({}));
                        adapter.start_workflow(&[
                            (None, Value::String(workflow)),
                            (None, input),
                            (Some("baggage".to_string()), baggage),
                        ])
                    }
                    ApplicationWorkflowServiceOperation::Status => adapter.workflow_status(
                        &carrier_workflow_service_run_arguments(&workflow, payload, "status")?,
                    ),
                    ApplicationWorkflowServiceOperation::Result => {
                        let mut arguments = vec![
                            (None, Value::String("Json".to_string())),
                            (None, Value::String(workflow)),
                        ];
                        arguments.extend(carrier_workflow_service_run_id(payload, "result")?);
                        adapter.workflow_result(&arguments)
                    }
                    ApplicationWorkflowServiceOperation::Cancel => adapter.cancel_workflow(
                        &carrier_workflow_service_run_arguments(&workflow, payload, "cancel")?,
                    ),
                    ApplicationWorkflowServiceOperation::Evidence => adapter.workflow_evidence(
                        &carrier_workflow_service_run_arguments(&workflow, payload, "evidence")?,
                    ),
                    ApplicationWorkflowServiceOperation::Signal => {
                        let run_id = payload.get("run_id").cloned().ok_or_else(|| {
                            AppRuntimeError::InvalidRequest(
                                "BicDB application workflow service signal lacks `run_id`"
                                    .to_string(),
                            )
                        })?;
                        let signal = payload.get("signal").cloned().ok_or_else(|| {
                            AppRuntimeError::InvalidRequest(
                                "BicDB application workflow service signal lacks `signal`"
                                    .to_string(),
                            )
                        })?;
                        let signal_payload = payload.get("payload").cloned().ok_or_else(|| {
                            AppRuntimeError::InvalidRequest(
                                "BicDB application workflow service signal lacks `payload`"
                                    .to_string(),
                            )
                        })?;
                        adapter.signal_workflow(&[
                            (None, Value::String(workflow)),
                            (None, run_id),
                            (None, signal),
                            (None, signal_payload),
                        ])
                    }
                    ApplicationWorkflowServiceOperation::RetryCompensation => adapter
                        .retry_workflow_compensation(&carrier_workflow_service_run_arguments(
                            &workflow,
                            payload,
                            "retry_compensation",
                        )?),
                };
                return validate_service_outcome(result, &method_contract);
            }
            let (arguments, globals) =
                carrier_arguments.expect("BicDB application callable arguments");
            let result = execute_application_program(
                program,
                &call.method,
                globals,
                arguments,
                &mut adapter,
            );
            return validate_service_outcome(result, &method_contract);
        }
        let result = module.invoke_with_host(
            &ExtensionInvocation {
                id: format!("service-{}", crate::host::now_ms()),
                kind: InvocationKind::Function,
                target: export.export.clone(),
                payload: json!({
                    "service": call.service,
                    "method": call.method,
                    "payload": call.payload,
                    "transaction": propagated_handle,
                }),
                context,
            },
            Box::new(host),
        )?;
        if let Some(error) = result.error {
            return Err(AppRuntimeError::Invocation(error));
        }
        validate_service_response(&result.body, &method_contract)?;
        Ok(result.body)
    }
}
