//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn validate_schema_foreign_keys(
    application: &bicdb_extension::abi_v2::ApplicationManifestV2,
    records: &BTreeMap<String, Vec<Value>>,
) -> Result<()> {
    for contract in &application.resources {
        let source_records = records
            .get(&contract.name)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        for foreign_key in &contract.foreign_keys {
            let targets = records
                .get(&foreign_key.target_resource)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            for source in source_records {
                let values = foreign_key
                    .fields
                    .iter()
                    .map(|field| source.get(field).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                if values.iter().any(Value::is_null) {
                    continue;
                }
                let present = targets.iter().any(|target| {
                    foreign_key
                        .target_fields
                        .iter()
                        .zip(&values)
                        .all(|(field, value)| target.get(field) == Some(value))
                });
                if !present {
                    return Err(AppRuntimeError::InvalidPackage(format!(
                        "existing resource `{}` violates foreign key `{}`",
                        contract.name, foreign_key.name
                    )));
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn apply_migration_transformations(
    db: &mut BicDb,
    manifest: &ExtensionManifest,
    services: InvocationServices,
    receipt: &mut SchemaApplicationReceipt,
) -> Result<()> {
    let application = manifest.application.as_deref().ok_or_else(|| {
        AppRuntimeError::InvalidPackage(
            "schema transformations require an application manifest".to_string(),
        )
    })?;
    let mut backfills = BTreeMap::<String, Vec<(String, ApplicationExpressionV1)>>::new();
    for step in application
        .migrations
        .iter()
        .flat_map(|migration| &migration.transformations)
    {
        match step {
            MigrationStep::BackfillField {
                resource,
                field,
                expression,
            } => backfills
                .entry(resource.clone())
                .or_default()
                .push((field.clone(), expression.clone())),
            unsupported => {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "unsupported signed schema transformation `{unsupported:?}`"
                )))
            }
        }
    }
    let policies = suspend_relation_policies(
        db,
        application
            .resources
            .iter()
            .map(|resource| resource.relation.clone()),
    )?;
    let mut committed_backfills = Vec::new();
    let result = (|| -> Result<()> {
        let mut migration_host = application
            .resources
            .iter()
            .any(|contract| !contract.encrypted_fields.is_empty())
            .then(|| {
                CapabilityHost::new_validated(
                    db,
                    Arc::new(manifest.clone()),
                    ActorContext {
                        service_id: Some("bicdb-application-host".to_string()),
                        authentication_method: Some("signed-schema-migration".to_string()),
                        roles: BTreeSet::from(["system:migration".to_string()]),
                        trace_id: uuid::Uuid::new_v4().to_string(),
                        deadline_unix_ms: crate::host::now_ms().saturating_add(300_000),
                        ..ActorContext::default()
                    },
                    services,
                )
            })
            .transpose()?;
        let mut transaction = db.begin_application_transaction(migration_actor(application))?;
        for contract in &application.resources {
            let vector_field = contract
                .vector_search
                .as_ref()
                .map(|vector| vector.field.as_str());
            let transformations = backfills
                .get(&contract.name)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            for record in transaction.scan_collection(&contract.relation)? {
                let before = record.clone();
                let original_object = resource_record_json(&record, vector_field)
                    .as_object()
                    .cloned()
                    .expect("database records serialize as objects");
                let mut object = match migration_host.as_mut() {
                    Some(host) => decrypt_resource_value(
                        host,
                        contract,
                        Value::Object(original_object.clone()),
                    )?,
                    None => Value::Object(original_object.clone()),
                }
                .as_object()
                .cloned()
                .expect("database records decrypt as objects");
                let mut changed_fields = BTreeSet::new();
                for (field, expression) in transformations {
                    if object.get(field).is_some_and(|value| !value.is_null()) {
                        continue;
                    }
                    let value = evaluate_carrier_expression(
                        application.application_program.as_ref(),
                        expression,
                        schema_expression_globals(&object),
                    )?;
                    object.insert(field.clone(), value);
                    changed_fields.insert(field.clone());
                }
                normalize_resource_object(contract, &mut object).map_err(|error| {
                    AppRuntimeError::InvalidPackage(format!(
                        "existing resource `{}` does not match its evolved contract: {error}",
                        contract.name
                    ))
                })?;
                validate_resource_object(contract, &object).map_err(|error| {
                    AppRuntimeError::InvalidPackage(format!(
                        "existing resource `{}` does not match its evolved contract: {error}",
                        contract.name
                    ))
                })?;
                validate_schema_checks(application, contract, &object)?;
                if !changed_fields.is_empty() {
                    let mut persisted = original_object;
                    for field in changed_fields {
                        persisted.insert(
                            field.clone(),
                            object.get(&field).cloned().unwrap_or(Value::Null),
                        );
                    }
                    committed_backfills.push((contract.relation.clone(), before));
                    transaction.update(
                        &contract.relation,
                        record_from_resource_json(Value::Object(persisted), vector_field)?,
                    )?;
                }
            }
        }
        let mut records = BTreeMap::new();
        for contract in &application.resources {
            records.insert(
                contract.name.clone(),
                transaction
                    .scan_collection(&contract.relation)?
                    .iter()
                    .map(|record| {
                        let vector_field = contract
                            .vector_search
                            .as_ref()
                            .map(|vector| vector.field.as_str());
                        let object = resource_record_json(record, vector_field);
                        let mut object = match migration_host.as_mut() {
                            Some(host) => decrypt_resource_value(host, contract, object)?,
                            None => object,
                        }
                        .as_object()
                        .cloned()
                        .expect("database records decrypt as objects");
                        normalize_resource_object(contract, &mut object)?;
                        Ok(Value::Object(object))
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        validate_schema_foreign_keys(application, &records)?;
        for validator in lower_carrier_invariants(application)? {
            transaction.register_commit_validator(validator)?;
        }
        transaction.commit()?;
        receipt.backfilled_records.append(&mut committed_backfills);
        Ok(())
    })();
    let restored = restore_relation_policies(db, &policies);
    match (result, restored) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(restore)) => Err(AppRuntimeError::InvalidPackage(format!(
            "{error}; schema policy restoration also failed: {restore}"
        ))),
    }
}

pub(crate) fn restore_backfilled_records(
    db: &mut BicDb,
    records: &[(String, Record)],
) -> Result<()> {
    let policies =
        suspend_relation_policies(db, records.iter().map(|(relation, _)| relation.clone()))?;
    let result = (|| -> Result<()> {
        let mut transaction = db.begin_transaction()?;
        for (relation, record) in records {
            transaction.update(relation, record.clone())?;
        }
        transaction.commit()?;
        Ok(())
    })();
    let restored = restore_relation_policies(db, &policies);
    match (result, restored) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(restore)) => Err(AppRuntimeError::InvalidPackage(format!(
            "{error}; backfill policy restoration also failed: {restore}"
        ))),
    }
}

pub(crate) fn record_applied_migrations(
    db: &mut BicDb,
    snapshot: &PackageSnapshot,
) -> Result<Vec<String>> {
    const RELATION: &str = "__bicdb_app_migrations";
    let application = snapshot.package.manifest.application.as_deref().unwrap();
    let pending = application
        .migrations
        .iter()
        .map(|migration| {
            let id = format!(
                "{}:{}",
                snapshot.package.manifest.identity.name, migration.schema_version
            );
            let plan_sha256 = Sha256::digest(serde_json::to_vec(migration)?)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            Ok((id, migration, plan_sha256))
        })
        .collect::<Result<Vec<_>>>()?;
    if pending.is_empty() {
        return Ok(Vec::new());
    }
    let mut unapplied = Vec::new();
    for (id, migration, plan_sha256) in pending {
        if let Some(existing) = db.get(RELATION, &id)? {
            if existing.metadata.get("plan_sha256").and_then(Value::as_str)
                != Some(plan_sha256.as_str())
            {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "migration ledger `{id}` has a different signed plan"
                )));
            }
        } else {
            unapplied.push((id, migration, plan_sha256));
        }
    }
    if unapplied.is_empty() {
        return Ok(Vec::new());
    }
    let actor = MutationActor {
        actor_id: "bicdb-application-host".to_string(),
        roles: BTreeSet::from(["system:migration".to_string()]),
        scopes: BTreeSet::new(),
        tenant_id: None,
        workspace_id: None,
        originating_plugin: snapshot.package.manifest.identity.name.clone(),
        originating_resource: None,
        originating_action: Some("migration".to_string()),
        trace_id: uuid::Uuid::new_v4().to_string(),
        deadline_unix_ms: crate::host::now_ms().saturating_add(300_000),
    };
    let mut transaction = db.begin_application_transaction(actor)?;
    let inserted = unapplied
        .iter()
        .map(|(id, _, _)| id.clone())
        .collect::<Vec<_>>();
    for (id, migration, plan_sha256) in unapplied {
        let record = Record::new(&id).with_metadata(json!({
            "application": snapshot.package.manifest.identity.name,
            "application_version": snapshot.package.manifest.identity.version,
            "package_sha256": snapshot.package_hash,
            "schema_version": migration.schema_version,
            "contract_versions": migration.contract_versions,
            "plan_sha256": plan_sha256,
            "activation_boundary": migration.activation_boundary,
            "irreversible": migration.irreversible,
            "applied_at_ms": crate::host::now_ms(),
        }));
        let columns = record
            .metadata
            .as_object()
            .into_iter()
            .flatten()
            .map(|(field, _)| field.clone())
            .chain(std::iter::once("id".to_string()))
            .collect();
        let grant = transaction.issue_mutation_grant(MutationGrantSpec {
            relation: RELATION.to_string(),
            operation: MutationOperation::Insert,
            record_id: Some(id),
            record_id_prefix: None,
            expected_version: None,
            version_field: None,
            allowed_columns: columns,
            bulk: false,
            maximum_affected_rows: 1,
            cascade_relations: BTreeSet::new(),
            statement_budget: 1,
            tenant_field: None,
            workspace_field: None,
            audit_metadata: BTreeMap::from([(
                "kind".to_string(),
                "application-migration".to_string(),
            )]),
        })?;
        transaction.insert_with_grant(grant, RELATION, record)?;
    }
    transaction.commit()?;
    Ok(inserted)
}

pub(crate) fn remove_applied_migrations(db: &mut BicDb, ids: &[String]) -> Result<()> {
    const RELATION: &str = "__bicdb_app_migrations";
    let policy = db.mutation_policy(RELATION)?;
    db.clear_mutation_policy(RELATION)?;
    let deletion = (|| -> Result<()> {
        for id in ids.iter().rev() {
            db.delete(RELATION, id)?;
        }
        Ok(())
    })();
    let restoration = match policy {
        Some(policy) => db.set_mutation_policy(RELATION, policy).map_err(Into::into),
        None => db.clear_mutation_policy(RELATION).map_err(Into::into),
    };
    deletion?;
    restoration
}

pub(crate) fn validate_runtime_profile(package: &ApplicationPackage) -> Result<()> {
    let application = package.manifest.application.as_deref().unwrap();
    for route in &application.routes {
        let carrier_realtime = application.realtime.iter().any(|contract| {
            ["negotiate", "poll", "sse", "ws"]
                .into_iter()
                .any(|suffix| route.template == format!("{}/{suffix}", contract.path))
        });
        if route.streaming_request && !carrier_realtime {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "route `{}` requests a streaming request body, but this host build has no \
                 bounded asynchronous request-stream adapter",
                route.name
            )));
        }
        if route.websocket && !carrier_realtime {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "route `{}` requests WebSockets, but this host adapter does not implement \
                 persistent bidirectional callbacks",
                route.name
            )));
        }
    }
    if application
        .required_features
        .contains(&bicdb_extension::abi_v2::ApplicationFeature::WebSocket)
        && application.realtime.is_empty()
    {
        return Err(AppRuntimeError::InvalidPackage(
            "the WebSocket BicDB application feature is unavailable in this host build".to_string(),
        ));
    }
    if application
        .required_features
        .contains(&bicdb_extension::abi_v2::ApplicationFeature::OnlineMigrations)
    {
        return Err(AppRuntimeError::InvalidPackage(
            "the online-migration BicDB application feature requires a trusted migration adapter"
                .to_string(),
        ));
    }
    for worker in &application.workers {
        if !package
            .manifest
            .capabilities
            .contains(&bicdb_extension::ExtensionCapability::QueueEvents)
            || !package
                .manifest
                .capabilities
                .contains(&bicdb_extension::ExtensionCapability::Jobs)
            || !package
                .manifest
                .permissions
                .consume_queues
                .contains(&worker.queue)
        {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "worker `{}` lacks its declared queue capability/permission",
                worker.name
            )));
        }
        if let Some(dead_letter_queue) = &worker.dead_letter_queue {
            if !package
                .manifest
                .permissions
                .publish_queues
                .contains(dead_letter_queue)
            {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "worker `{}` lacks publish permission for dead-letter queue `{dead_letter_queue}`",
                    worker.name
                )));
            }
        }
    }
    if let Some(program) = &application.application_program {
        for (job, queue) in &program.job_bindings {
            let worker = application
                .workers
                .iter()
                .find(|worker| worker.export == *job && worker.queue == *queue)
                .ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(format!(
                        "BicDB application job `{job}` lacks its exact signed worker"
                    ))
                })?;
            if !worker.payload_arguments.is_empty()
                || !package.manifest.permissions.publish_queues.contains(queue)
                || !package.manifest.permissions.consume_queues.contains(queue)
            {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "BicDB application job `{job}` lacks durable queue authority"
                )));
            }
        }
        for (name, workflow) in &program.workflow_bindings {
            if let Some(plan_sha256) = &workflow.plan_sha256 {
                if *plan_sha256 != carrier_workflow_plan_sha256(workflow)? {
                    return Err(AppRuntimeError::InvalidPackage(format!(
                        "BicDB application workflow `{name}` plan hash does not match its signed topology"
                    )));
                }
            }
            let workers = application
                .workers
                .iter()
                .filter(|worker| {
                    worker.export == workflow.worker_export && worker.queue == workflow.queue
                })
                .collect::<Vec<_>>();
            if workers.len() != usize::from(workflow.max_parallelism)
                || workers
                    .iter()
                    .any(|worker| !worker.payload_arguments.is_empty() || !worker.required)
                || workers
                    .iter()
                    .map(|worker| worker.group.as_str())
                    .collect::<BTreeSet<_>>()
                    .len()
                    != 1
                || !package
                    .manifest
                    .permissions
                    .publish_queues
                    .contains(&workflow.queue)
                || !package
                    .manifest
                    .permissions
                    .consume_queues
                    .contains(&workflow.queue)
                || !package
                    .manifest
                    .capabilities
                    .contains(&bicdb_extension::ExtensionCapability::Database)
                || !package
                    .manifest
                    .capabilities
                    .contains(&bicdb_extension::ExtensionCapability::Transactions)
            {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "BicDB application workflow `{name}` lacks its exact parallel durable-state/queue authority"
                )));
            }
        }
    }
    for schedule in &application.schedules {
        if !package
            .manifest
            .capabilities
            .contains(&bicdb_extension::ExtensionCapability::Schedules)
            || !package
                .manifest
                .capabilities
                .contains(&bicdb_extension::ExtensionCapability::Jobs)
        {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "schedule `{}` lacks its declared schedule/job capabilities",
                schedule.name
            )));
        }
        parse_schedule_plan(&schedule.schedule, &schedule.timezone)?;
    }
    for resource in &application.resources {
        for validation in &resource.validation {
            if let bicdb_extension::abi_v2::ValidationRuleKind::Pattern { expression } =
                &validation.rule
            {
                regex::Regex::new(expression).map_err(|error| {
                    AppRuntimeError::InvalidPackage(format!(
                        "resource `{}` validation pattern for `{}` is invalid: {error}",
                        resource.name, validation.field
                    ))
                })?;
            }
        }
    }
    for statement in &application.raw_sql {
        let analysis = analyze_bounded_sql(&statement.sql).map_err(|error| {
            AppRuntimeError::InvalidPackage(format!(
                "declared SQL `{}` is not bounded: {error}",
                statement.id
            ))
        })?;
        if analysis.relations != statement.relations
            || analysis.routines != statement.routines
            || analysis.actions != statement.actions
        {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "declared SQL `{}` authority does not match its parsed relations/actions/routines",
                statement.id
            )));
        }
        if analysis.parameter_count != statement.parameters.len() {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "declared SQL `{}` has {} placeholders but {} signed parameter types",
                statement.id,
                analysis.parameter_count,
                statement.parameters.len()
            )));
        }
        for relation in &statement.relations {
            let permission = application.permission_for(relation).ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "declared SQL `{}` relation `{relation}` has no signed permission",
                    statement.id
                ))
            })?;
            for action in statement.actions.iter().filter(|action| {
                !matches!(action, DatabaseAction::Execute | DatabaseAction::RawSql)
            }) {
                if !permission.actions.contains(action) {
                    return Err(AppRuntimeError::InvalidPackage(format!(
                        "declared SQL `{}` lacks {action:?} permission on `{relation}`",
                        statement.id
                    )));
                }
            }
        }
        let hash = Sha256::digest(statement.sql.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        if hash != statement.sha256 {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "declared SQL `{}` hash does not match",
                statement.id
            )));
        }
    }
    for migration in &application.migrations {
        for step in migration
            .forward
            .iter()
            .chain(&migration.transformations)
            .chain(&migration.rollback)
        {
            match step {
                MigrationStep::Sql { statement_id } => {
                    if !application
                        .raw_sql
                        .iter()
                        .any(|sql| &sql.id == statement_id)
                    {
                        return Err(AppRuntimeError::InvalidPackage(format!(
                            "migration references undeclared SQL `{statement_id}`"
                        )));
                    }
                }
                MigrationStep::ValidateContract { resource } => {
                    if !application
                        .resources
                        .iter()
                        .any(|item| &item.name == resource)
                    {
                        return Err(AppRuntimeError::InvalidPackage(format!(
                            "migration references undeclared resource `{resource}`"
                        )));
                    }
                }
                MigrationStep::BackfillField {
                    resource, field, ..
                } => {
                    let Some(contract) = application
                        .resources
                        .iter()
                        .find(|item| &item.name == resource)
                    else {
                        return Err(AppRuntimeError::InvalidPackage(format!(
                            "migration backfill references undeclared resource `{resource}`"
                        )));
                    };
                    if !contract.fields.iter().any(|item| &item.name == field) {
                        return Err(AppRuntimeError::InvalidPackage(format!(
                            "migration backfill references undeclared field `{resource}.{field}`"
                        )));
                    }
                }
                MigrationStep::StartWorker { worker } | MigrationStep::StopWorker { worker } => {
                    if !application.workers.iter().any(|item| &item.name == worker) {
                        return Err(AppRuntimeError::InvalidPackage(format!(
                            "migration references undeclared worker `{worker}`"
                        )));
                    }
                }
                MigrationStep::OnlineRewrite { .. } => {
                    return Err(AppRuntimeError::InvalidPackage(
                        "online rewrite requires a trusted migration adapter in this host build"
                            .to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn intersect_manifests(
    caller: &bicdb_extension::ExtensionManifest,
    callee: &bicdb_extension::ExtensionManifest,
) -> Result<bicdb_extension::ExtensionManifest> {
    let mut effective = callee.clone();
    effective.capabilities = caller
        .capabilities
        .intersection(&callee.capabilities)
        .copied()
        .collect();
    effective.permissions.read_relations = caller
        .permissions
        .read_relations
        .intersection(&callee.permissions.read_relations)
        .cloned()
        .collect();
    effective.permissions.write_relations = caller
        .permissions
        .write_relations
        .intersection(&callee.permissions.write_relations)
        .cloned()
        .collect();
    effective.permissions.publish_queues = caller
        .permissions
        .publish_queues
        .intersection(&callee.permissions.publish_queues)
        .cloned()
        .collect();
    effective.permissions.consume_queues = caller
        .permissions
        .consume_queues
        .intersection(&callee.permissions.consume_queues)
        .cloned()
        .collect();
    effective.permissions.network_hosts = caller
        .permissions
        .network_hosts
        .intersection(&callee.permissions.network_hosts)
        .cloned()
        .collect();

    let caller_application = caller.application.as_deref().unwrap();
    let application = effective.application.as_deref_mut().unwrap();
    let caller_exact_columns = caller_application
        .required_features
        .contains(&bicdb_extension::abi_v2::ApplicationFeature::ExactColumnAuthority);
    let callee_exact_columns = application
        .required_features
        .contains(&bicdb_extension::abi_v2::ApplicationFeature::ExactColumnAuthority);
    if caller_exact_columns || callee_exact_columns {
        application
            .required_features
            .insert(bicdb_extension::abi_v2::ApplicationFeature::ExactColumnAuthority);
    }
    application.max_call_depth = application
        .max_call_depth
        .min(caller_application.max_call_depth);
    application.relation_permissions = application
        .relation_permissions
        .iter()
        .filter_map(|callee_permission| {
            let caller_permission =
                caller_application.permission_for(&callee_permission.relation)?;
            let mut permission = callee_permission.clone();
            permission.actions = permission
                .actions
                .intersection(&caller_permission.actions)
                .copied()
                .collect();
            permission.readable_columns = intersect_authority_columns(
                &caller_permission.readable_columns,
                &callee_permission.readable_columns,
                caller_exact_columns,
                callee_exact_columns,
            );
            permission.writable_columns = intersect_authority_columns(
                &caller_permission.writable_columns,
                &callee_permission.writable_columns,
                caller_exact_columns,
                callee_exact_columns,
            );
            (!permission.actions.is_empty()).then_some(permission)
        })
        .collect();
    application.raw_sql.retain(|statement| {
        caller_application
            .raw_sql
            .iter()
            .any(|caller| caller.id == statement.id && caller.sha256 == statement.sha256)
    });
    application.secrets = application
        .secrets
        .iter()
        .filter_map(|callee_secret| {
            let caller_secret = caller_application
                .secrets
                .iter()
                .find(|secret| secret.name == callee_secret.name)?;
            let mut secret = callee_secret.clone();
            secret.operations = secret
                .operations
                .intersection(&caller_secret.operations)
                .copied()
                .collect();
            secret.allow_plaintext_read = secret
                .operations
                .contains(&bicdb_extension::abi_v2::CryptoOperation::PlaintextRead);
            (!secret.operations.is_empty()).then_some(secret)
        })
        .collect();
    application.egress = application
        .egress
        .iter()
        .filter_map(|callee_policy| {
            let caller_policy = caller_application
                .egress
                .iter()
                .find(|policy| policy.name == callee_policy.name)?;
            let mut policy = callee_policy.clone();
            policy.schemes = policy
                .schemes
                .intersection(&caller_policy.schemes)
                .cloned()
                .collect();
            policy.hosts = policy
                .hosts
                .intersection(&caller_policy.hosts)
                .cloned()
                .collect();
            policy.ports = policy
                .ports
                .intersection(&caller_policy.ports)
                .copied()
                .collect();
            policy.allow_redirects &= caller_policy.allow_redirects;
            policy.allow_private_networks &= caller_policy.allow_private_networks;
            policy.max_request_bytes = policy
                .max_request_bytes
                .min(caller_policy.max_request_bytes);
            policy.max_response_bytes = policy
                .max_response_bytes
                .min(caller_policy.max_response_bytes);
            policy.timeout_ms = policy.timeout_ms.min(caller_policy.timeout_ms);
            policy.max_concurrency = policy.max_concurrency.min(caller_policy.max_concurrency);
            policy.requests_per_minute = policy
                .requests_per_minute
                .min(caller_policy.requests_per_minute);
            (!policy.schemes.is_empty() && !policy.hosts.is_empty() && !policy.ports.is_empty())
                .then_some(policy)
        })
        .collect();
    application.blobs = application
        .blobs
        .iter()
        .filter_map(|callee_blob| {
            let caller_blob = caller_application
                .blobs
                .iter()
                .find(|blob| blob.namespace == callee_blob.namespace)?;
            let mut blob = callee_blob.clone();
            blob.max_blob_bytes = blob.max_blob_bytes.min(caller_blob.max_blob_bytes);
            blob.content_types =
                intersect_columns(&caller_blob.content_types, &callee_blob.content_types);
            blob.allow_signed_urls &= caller_blob.allow_signed_urls;
            blob.require_scan |= caller_blob.require_scan;
            Some(blob)
        })
        .collect();
    // The callee's code, not the upstream caller, chooses whether to invoke a
    // nested dependency. Preserve those signed imports while executing the
    // callee under the transitive caller∩callee authority capsule. Requiring
    // every upstream package to import every transitive dependency would both
    // expose implementation details and make legitimate A -> B -> C calls
    // impossible.
    application.routes.clear();
    application.workers.clear();
    application.schedules.clear();
    application.migrations.clear();
    let effective_relation_permissions = application.relation_permissions.clone();
    application.resources.retain(|resource| {
        effective_relation_permissions
            .iter()
            .find(|permission| permission.relation == resource.relation)
            .is_some_and(|permission| {
                resource.operations.iter().all(|operation| {
                    let action = match operation {
                        bicdb_extension::abi_v2::ResourceOperation::List
                        | bicdb_extension::abi_v2::ResourceOperation::Get => DatabaseAction::Select,
                        bicdb_extension::abi_v2::ResourceOperation::Create => {
                            DatabaseAction::Insert
                        }
                        bicdb_extension::abi_v2::ResourceOperation::Upsert => {
                            DatabaseAction::Upsert
                        }
                        bicdb_extension::abi_v2::ResourceOperation::Update
                        | bicdb_extension::abi_v2::ResourceOperation::Restore
                        | bicdb_extension::abi_v2::ResourceOperation::Action => {
                            DatabaseAction::Update
                        }
                        bicdb_extension::abi_v2::ResourceOperation::Delete
                            if resource.soft_delete_field.is_some() =>
                        {
                            DatabaseAction::Update
                        }
                        bicdb_extension::abi_v2::ResourceOperation::Delete => {
                            DatabaseAction::Delete
                        }
                    };
                    permission.actions.contains(&action)
                })
            })
    });
    effective.validate()?;
    Ok(effective)
}

pub(crate) fn service_execution_manifest(
    caller: &bicdb_extension::ExtensionManifest,
    callee: &bicdb_extension::ExtensionManifest,
    delegated_authority: bool,
) -> Result<bicdb_extension::ExtensionManifest> {
    if delegated_authority {
        // The signed service contract is the capability boundary. The caller
        // can enter only the pinned exported method; its request was validated
        // before this point, while the callee retains its exact provider and
        // resource authority for the implementation.
        Ok(callee.clone())
    } else {
        intersect_manifests(caller, callee)
    }
}

pub(crate) fn intersect_columns(
    caller: &BTreeSet<String>,
    callee: &BTreeSet<String>,
) -> BTreeSet<String> {
    match (caller.is_empty(), callee.is_empty()) {
        (true, true) => BTreeSet::new(),
        (true, false) => callee.clone(),
        (false, true) => caller.clone(),
        (false, false) => caller.intersection(callee).cloned().collect(),
    }
}

pub(crate) fn intersect_authority_columns(
    caller: &BTreeSet<String>,
    callee: &BTreeSet<String>,
    caller_exact: bool,
    callee_exact: bool,
) -> BTreeSet<String> {
    match (
        caller.is_empty() && !caller_exact,
        callee.is_empty() && !callee_exact,
    ) {
        (true, true) => BTreeSet::new(),
        (true, false) => callee.clone(),
        (false, true) => caller.clone(),
        (false, false) => caller.intersection(callee).cloned().collect(),
    }
}

pub(crate) fn build_routes(catalog: &mut RuntimeCatalog) -> Result<()> {
    catalog.routes.clear();
    for (application_name, snapshot) in &catalog.packages {
        for route in &snapshot
            .package
            .manifest
            .application
            .as_deref()
            .unwrap()
            .routes
        {
            let key = (
                format!("{:?}", route.method).to_ascii_uppercase(),
                route.template.clone(),
            );
            if catalog
                .routes
                .insert(key.clone(), application_name.clone())
                .is_some()
            {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "duplicate active route {} {}",
                    key.0, key.1
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn ensure_no_active_dependents(
    catalog: &RuntimeCatalog,
    dependency: &str,
) -> Result<()> {
    for (name, snapshot) in &catalog.packages {
        if snapshot
            .package
            .manifest
            .dependencies
            .iter()
            .any(|item| item.name.eq_ignore_ascii_case(dependency) && !item.optional)
        {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "application `{name}` depends on `{dependency}`"
            )));
        }
    }
    Ok(())
}

pub(crate) fn as_installation(snapshot: &PackageSnapshot) -> ExtensionInstallation {
    ExtensionInstallation {
        manifest: snapshot.package.manifest.clone(),
        module_sha256: snapshot
            .verification
            .module_sha256
            .get(&snapshot.package.manifest.identity.name)
            .cloned()
            .unwrap_or_else(|| "0".repeat(64)),
        state: ExtensionState::Active,
        installed_at_ms: snapshot.activated_at_ms.max(1),
        activation: Some(bicdb_extension::ExtensionActivation {
            catalog_generation: snapshot.generation.max(1),
            topology_generation: 0,
            ready_nodes: BTreeSet::from(["local".to_string()]),
            required_nodes: BTreeSet::from(["local".to_string()]),
            quorum_committed: true,
            activated_at_ms: snapshot.activated_at_ms.max(1),
        }),
        last_error: None,
    }
}

pub(crate) fn diagnostic(snapshot: &PackageSnapshot) -> RuntimeDiagnostic {
    let application = snapshot.package.manifest.application.as_deref().unwrap();
    RuntimeDiagnostic {
        application: snapshot.package.manifest.identity.name.clone(),
        version: snapshot.package.manifest.identity.version.clone(),
        generation: snapshot.generation,
        state: snapshot.state.clone(),
        package_hash: snapshot.package_hash.clone(),
        readiness: snapshot.readiness.clone(),
        routes: application
            .routes
            .iter()
            .map(|route| format!("{:?} {}", route.method, route.template))
            .collect(),
        services: application
            .service_exports
            .iter()
            .map(|service| format!("{}@{}", service.service, service.version))
            .collect(),
        workers: application
            .workers
            .iter()
            .map(|worker| worker.name.clone())
            .collect(),
        schedules: application
            .schedules
            .iter()
            .map(|schedule| schedule.name.clone())
            .collect(),
    }
}

pub(crate) fn definitions(
    runtime: &ApplicationRuntime,
    get: impl Fn(&bicdb_extension::abi_v2::ApplicationManifestV2) -> Vec<String>,
) -> Vec<String> {
    runtime
        .active
        .read()
        .packages
        .values()
        .flat_map(|snapshot| get(snapshot.package.manifest.application.as_deref().unwrap()))
        .collect()
}

pub(crate) fn normalized(name: &str) -> String {
    name.to_ascii_lowercase()
}

pub(crate) fn persist_package(root: &Path, hash: &str, package: &ApplicationPackage) -> Result<()> {
    let path = root.join("packages").join(format!("{hash}.json"));
    if path.exists() {
        return Ok(());
    }
    atomic_write(&path, &serde_json::to_vec(package)?)
}

pub(crate) fn persist_runtime_state(
    root: &Path,
    catalog: &RuntimeCatalog,
    staged: &BTreeMap<String, Arc<PackageSnapshot>>,
    history: &BTreeMap<String, VecDeque<Arc<PackageSnapshot>>>,
) -> Result<()> {
    let packages = catalog
        .packages
        .iter()
        .map(|(name, snapshot)| (name.clone(), persisted_reference(snapshot)))
        .collect();
    let staged = staged
        .iter()
        .map(|(name, snapshot)| (name.clone(), persisted_reference(snapshot)))
        .collect();
    let history = history
        .iter()
        .map(|(name, snapshots)| {
            (
                name.clone(),
                snapshots.iter().map(persisted_reference).collect(),
            )
        })
        .collect();
    atomic_write(
        &root.join("snapshots").join("active.json"),
        &serde_json::to_vec(&PersistedRuntimeState {
            generation: catalog.generation,
            packages,
            staged,
            history,
        })?,
    )
}

pub(crate) fn persisted_reference(snapshot: &Arc<PackageSnapshot>) -> PersistedPackageReference {
    PersistedPackageReference {
        hash: snapshot.package_hash.clone(),
        generation: snapshot.generation,
        activated_at_ms: snapshot.activated_at_ms,
    }
}

pub(crate) struct LifecycleFileLock {
    pub(crate) file: File,
}

impl LifecycleFileLock {
    pub(crate) fn acquire(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(root.join(".lifecycle.lock"))?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
        Ok(Self { file })
    }
}

impl Drop for LifecycleFileLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
        }
    }
}

pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        AppRuntimeError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "path has no parent",
        ))
    })?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".bicdb-app-{}-{}.tmp",
        std::process::id(),
        crate::host::now_ms()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use base64::Engine;
    use bicdb_extension::abi_v2::{
        ApplicationAuthKindV1, ApplicationAuthSchemeV1, ApplicationCallableV1,
        ApplicationEvaluationAuthV1, ApplicationExpressionV1, ApplicationFeature,
        ApplicationManifestV2, ApplicationProgramV1, ApplicationRouteParameterTypeV1,
        ApplicationSecurityContractV1, ApplicationStatementV1, ApplicationWorkflowDefinitionV1,
        ApplicationWorkflowStepV1, BlobDeclaration, CryptoOperation, PackageMetadata,
        RelationPermission, SecretDeclaration, ServiceImport, APPLICATION_COMPATIBILITY_PROFILE,
    };
    use bicdb_extension::{
        ExtensionDependency, ExtensionIdentity, ExtensionLimits, ExtensionManifest,
        ExtensionPermissions,
    };
    use ed25519_dalek::{Signer, SigningKey};

    use super::*;
    use crate::package::canonical_signing_payload;
    use crate::{
        BlobProvider, InMemorySecretProvider, LocalBlobProvider, ObservabilityEvent,
        ProductionEgressProvider, TrustedSigningKeys,
    };

    fn invoke_security_builtin(
        db: &BicDb,
        manifest: Arc<ExtensionManifest>,
        services: InvocationServices,
        actor: ActorContext,
        target: &str,
        arguments: Vec<(Option<String>, Value)>,
    ) -> Result<Value> {
        let program = manifest
            .application
            .as_deref()
            .and_then(|application| application.application_program.as_ref())
            .cloned()
            .expect("test BicDB application program");
        let mut capability = CapabilityHost::new(db, manifest, actor, services)?;
        let mut adapter = CapabilityApplicationProgramHost::new(
            &mut capability,
            Vec::new(),
            program.service_bindings,
            program.client_bindings,
            program.secret_bindings,
            program.event_bindings,
            program.realtime_bindings,
            program.workflow_bindings,
        );
        adapter.call_builtin(target, arguments)
    }

    fn invoke_security_session(
        db: &BicDb,
        manifest: Arc<ExtensionManifest>,
        services: InvocationServices,
        actor: ActorContext,
        action: &str,
        value: Option<&str>,
    ) -> Result<Value> {
        let program = manifest
            .application
            .as_deref()
            .and_then(|application| application.application_program.as_ref())
            .cloned()
            .expect("test BicDB application program");
        let mut capability = CapabilityHost::new(db, manifest, actor.clone(), services)?;
        let mut adapter = CapabilityApplicationProgramHost::new(
            &mut capability,
            Vec::new(),
            program.service_bindings,
            program.client_bindings,
            program.secret_bindings,
            program.event_bindings,
            program.realtime_bindings,
            program.workflow_bindings,
        );
        match action {
            "refresh" => adapter.auth_refresh(value.expect("refresh token")),
            "sessions" => adapter.auth_sessions(&actor),
            "revoke" => adapter.revoke_auth_session(&actor, value.expect("session id")),
            _ => panic!("unknown test security session action"),
        }
    }

    fn security_named(arguments: Vec<(&str, Value)>) -> Vec<(Option<String>, Value)> {
        arguments
            .into_iter()
            .map(|(name, value)| (Some(name.to_string()), value))
            .collect()
    }

    #[test]
    fn carrier_evaluation_auth_replaces_operator_authority_fail_closed() {
        let operator = ActorContext {
            service_id: Some("operator".to_string()),
            client_id: Some("admin-client".to_string()),
            roles: BTreeSet::from(["superuser".to_string()]),
            scopes: BTreeSet::from(["all".to_string()]),
            tenant_id: Some("operator-tenant".to_string()),
            workspace_id: Some("operator-workspace".to_string()),
            trace_id: "trace-eval".to_string(),
            deadline_unix_ms: crate::host::now_ms().saturating_add(60_000),
            ..ActorContext::default()
        };
        let literal = |value| ApplicationExpressionV1::Literal { value };
        let auth = ApplicationEvaluationAuthV1 {
            id: literal(json!(7)),
            email: literal(json!("evaluator@example.com")),
            name: literal(json!("Evaluator")),
            roles: literal(json!(["support"])),
            tenant_id: Some(literal(json!("tenant-eval"))),
        };
        let actor = carrier_evaluation_actor(None, Some(&auth), &operator).unwrap();
        assert_eq!(actor.user_id.as_deref(), Some("7"));
        assert_eq!(actor.service_id, None);
        assert_eq!(actor.client_id, None);
        assert_eq!(actor.roles, BTreeSet::from(["support".to_string()]));
        assert!(actor.scopes.is_empty());
        assert_eq!(actor.tenant_id.as_deref(), Some("tenant-eval"));
        assert_eq!(actor.workspace_id, None);
        assert_eq!(
            actor.policy_attributes.get("email").map(String::as_str),
            Some("evaluator@example.com")
        );
        assert_eq!(actor.trace_id, "trace-eval");
    }

    #[test]
    fn carrier_security_helpers_are_durable_versioned_and_evidenced() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("security-database")).unwrap();
        db.create_collection("__bicdb_app_security").unwrap();
        db.set_mutation_policy(
            "__bicdb_app_security",
            MutationPolicy::grants_required().with_immutable_fields(["application", "kind"]),
        )
        .unwrap();

        let signing_key = SigningKey::from_bytes(&[42; 32]);
        let mut manifest = signed_package("1.0.0", &signing_key).manifest;
        manifest.capabilities.extend([
            bicdb_extension::ExtensionCapability::Database,
            bicdb_extension::ExtensionCapability::Transactions,
            bicdb_extension::ExtensionCapability::Clock,
            bicdb_extension::ExtensionCapability::Random,
            bicdb_extension::ExtensionCapability::SecretsCrypto,
            bicdb_extension::ExtensionCapability::Observability,
            bicdb_extension::ExtensionCapability::HttpRoutes,
        ]);
        let helpers = BTreeSet::from([
            "auth.register".to_string(),
            "auth.login".to_string(),
            "auth.issue_tokens".to_string(),
            "auth.password_policy".to_string(),
            "auth.password_hash".to_string(),
            "auth.password_verify".to_string(),
            "auth.password_breach_digest".to_string(),
            "auth.totp_secret".to_string(),
            "auth.totp_code".to_string(),
            "auth.totp_verify".to_string(),
            "auth.totp_uri".to_string(),
            "auth.magic_link_issue".to_string(),
            "auth.magic_link_verify".to_string(),
            "auth.oauth_authorize".to_string(),
            "auth.oauth_callback".to_string(),
        ]);
        let application = manifest.application.as_deref_mut().unwrap();
        application.auth_schemes.insert(
            "Auth".to_string(),
            ApplicationAuthSchemeV1 {
                kind: ApplicationAuthKindV1::JwtHs256,
                issuer: "carrier-security-test".to_string(),
                audience: "carrier-security-users".to_string(),
            },
        );
        application.secrets = vec![
            SecretDeclaration {
                name: "AUTH_SIGNING_KEY".to_string(),
                operations: BTreeSet::from([
                    CryptoOperation::Metadata,
                    CryptoOperation::Sign,
                    CryptoOperation::Verify,
                ]),
                versions: BTreeSet::new(),
                allow_plaintext_read: false,
            },
            SecretDeclaration {
                name: "MAGIC_LINK_KEY".to_string(),
                operations: BTreeSet::from([
                    CryptoOperation::Metadata,
                    CryptoOperation::Verify,
                    CryptoOperation::Hmac,
                ]),
                versions: BTreeSet::new(),
                allow_plaintext_read: false,
            },
            SecretDeclaration {
                name: "FIELD_KEY".to_string(),
                operations: BTreeSet::from([
                    CryptoOperation::Metadata,
                    CryptoOperation::Encrypt,
                    CryptoOperation::Decrypt,
                ]),
                versions: BTreeSet::new(),
                allow_plaintext_read: false,
            },
        ];
        application.application_program = Some(ApplicationProgramV1 {
            version: 1,
            max_steps: 100,
            max_call_depth: 16,
            blob: None,
            redis: None,
            email: None,
            grpc: None,
            tokenizer: None,
            embeddings: None,
            llm: None,
            rag: None,
            agents: None,
            evaluations: None,
            tests: None,
            observability: None,
            callables: BTreeMap::new(),
            service_bindings: BTreeMap::new(),
            client_bindings: BTreeMap::new(),
            flags: BTreeMap::new(),
            job_bindings: BTreeMap::new(),
            secret_bindings: BTreeMap::from([("Vault.value".to_string(), "FIELD_KEY".to_string())]),
            security: Some(ApplicationSecurityContractV1 {
                version: 1,
                helpers,
                auth_scheme: Some("Auth".to_string()),
                signing_secret: Some("AUTH_SIGNING_KEY".to_string()),
                signing_algorithm: Some("hmac-sha256".to_string()),
                access_ttl_seconds: 900,
                refresh_ttl_seconds: 86_400,
                magic_link_secrets: BTreeSet::from(["MAGIC_LINK_KEY".to_string()]),
                durable_replay: true,
                versioned_keys: true,
                emit_evidence: true,
            }),
            event_bindings: BTreeMap::new(),
            realtime_bindings: BTreeMap::new(),
            mutation_bindings: Vec::new(),
            workflow_bindings: BTreeMap::new(),
        });
        application
            .required_features
            .insert(ApplicationFeature::Http);
        application.routes = [
            ("refresh", "POST", "/auth/refresh", true),
            ("logout", "POST", "/auth/logout", false),
            ("sessions", "GET", "/auth/sessions", false),
            (
                "session_revoke",
                "DELETE",
                "/auth/sessions/{session_id}",
                false,
            ),
            ("register", "POST", "/auth/register", true),
            ("login", "POST", "/auth/login", true),
        ]
        .into_iter()
        .map(|(name, method, template, public)| {
            serde_json::from_value(json!({
                "name": format!("__carrier_security_{name}"),
                "method": method,
                "template": template,
                "export": format!("__carrier_security_{name}"),
                "public": public,
                "auth_scheme": (!public).then_some("Auth"),
                "max_request_bytes": 65_536,
                "max_response_bytes": 1_048_576
            }))
            .unwrap()
        })
        .collect();
        let manifest = Arc::new(manifest);
        manifest.validate().unwrap();

        let secrets = InMemorySecretProvider::default();
        secrets
            .insert(
                "AUTH_SIGNING_KEY",
                "v1",
                "auth-v1",
                "hmac-sha256",
                vec![41; 32],
                true,
            )
            .unwrap();
        secrets
            .insert(
                "MAGIC_LINK_KEY",
                "v1",
                "magic-v1",
                "hmac-sha256",
                vec![42; 32],
                true,
            )
            .unwrap();
        secrets
            .insert(
                "FIELD_KEY",
                "v1",
                "field-v1",
                "aes-256-gcm",
                vec![43; 32],
                true,
            )
            .unwrap();
        let observations = Arc::new(Mutex::<Vec<ObservabilityEvent>>::default());
        let mut services = InvocationServices::new(
            Arc::new(secrets.clone()),
            Arc::new(ProductionEgressProvider::new().unwrap()),
            Arc::new(
                LocalBlobProvider::open(directory.path().join("security-blobs"), vec![8; 32])
                    .unwrap(),
            ),
            observations.clone(),
        );
        services.deterministic_clock_ms = Some(Arc::new(std::sync::atomic::AtomicI64::new(
            crate::host::now_ms(),
        )));
        services.deterministic_random_seed = Some(Arc::new(Mutex::new(0x5eed_u64)));
        let actor = ActorContext {
            service_id: Some("carrier-auth".to_string()),
            trace_id: uuid::Uuid::new_v4().to_string(),
            deadline_unix_ms: crate::host::now_ms() + 60_000,
            ..ActorContext::default()
        };

        let registered = invoke_security_builtin(
            &db,
            manifest.clone(),
            services.clone(),
            actor.clone(),
            "auth.register",
            security_named(vec![
                ("email", json!("Ada@Example.com")),
                ("name", json!("Ada")),
                ("password", json!("Password123!")),
            ]),
        )
        .unwrap();
        assert_eq!(registered["email"], "ada@example.com");
        let user_id = registered["id"].as_i64().unwrap();
        let logged_in = invoke_security_builtin(
            &db,
            manifest.clone(),
            services.clone(),
            actor.clone(),
            "auth.login",
            security_named(vec![
                ("email", json!("ada@example.com")),
                ("password", json!("Password123!")),
            ]),
        )
        .unwrap();
        assert_eq!(logged_in["id"], user_id);
        let tokens = invoke_security_builtin(
            &db,
            manifest.clone(),
            services.clone(),
            actor.clone(),
            "auth.issue_tokens",
            vec![(None, json!(user_id))],
        )
        .unwrap();
        let authenticator = crate::JwtAuthenticator::hs256(
            crate::JwtConfiguration {
                issuer: "carrier-security-test".to_string(),
                audience: "carrier-security-users".to_string(),
                authentication_method: "carrier".to_string(),
                maximum_lifetime_seconds: 900,
                clock_skew_seconds: 30,
            },
            vec![41; 32],
        )
        .unwrap();
        let access = tokens["access_token"].as_str().unwrap();
        let refresh = tokens["refresh_token"].as_str().unwrap();
        let authenticated = authenticator
            .authenticate(
                access,
                "token-trace".to_string(),
                None,
                None,
                crate::host::now_ms() + 60_000,
            )
            .unwrap();
        assert_eq!(
            authenticated.user_id.as_deref(),
            Some(user_id.to_string().as_str())
        );
        assert!(authenticator
            .authenticate(
                refresh,
                "refresh-trace".to_string(),
                None,
                None,
                crate::host::now_ms() + 60_000,
            )
            .is_err());

        secrets
            .insert(
                "AUTH_SIGNING_KEY",
                "v2",
                "auth-v2",
                "hmac-sha256",
                vec![46; 32],
                true,
            )
            .unwrap();
        let rotated = invoke_security_session(
            &db,
            manifest.clone(),
            services.clone(),
            actor.clone(),
            "refresh",
            Some(refresh),
        )
        .unwrap();
        assert_ne!(rotated["refresh_token"].as_str(), Some(refresh));
        assert!(invoke_security_session(
            &db,
            manifest.clone(),
            services.clone(),
            actor.clone(),
            "refresh",
            Some(refresh),
        )
        .is_err());
        let overlap = crate::JwtAuthenticator::hs256_rotating(
            crate::JwtConfiguration {
                issuer: "carrier-security-test".to_string(),
                audience: "carrier-security-users".to_string(),
                authentication_method: "carrier".to_string(),
                maximum_lifetime_seconds: 900,
                clock_skew_seconds: 30,
            },
            [
                ("auth-v1".to_string(), vec![41; 32]),
                ("auth-v2".to_string(), vec![46; 32]),
            ],
        )
        .unwrap();
        let rotated_actor = overlap
            .authenticate(
                rotated["access_token"].as_str().unwrap(),
                "rotated-trace".to_string(),
                None,
                None,
                crate::host::now_ms() + 60_000,
            )
            .unwrap();
        assert_eq!(rotated_actor.session_id, authenticated.session_id);
        let sessions = invoke_security_session(
            &db,
            manifest.clone(),
            services.clone(),
            rotated_actor.clone(),
            "sessions",
            None,
        )
        .unwrap();
        assert_eq!(sessions.as_array().unwrap().len(), 1);
        let session_id = rotated_actor.session_id.as_deref().unwrap();
        invoke_security_session(
            &db,
            manifest.clone(),
            services.clone(),
            rotated_actor.clone(),
            "revoke",
            Some(session_id),
        )
        .unwrap();
        assert!(invoke_security_session(
            &db,
            manifest.clone(),
            services.clone(),
            actor.clone(),
            "refresh",
            rotated["refresh_token"].as_str(),
        )
        .is_err());

        let encrypted = invoke_security_builtin(
            &db,
            manifest.clone(),
            services.clone(),
            actor.clone(),
            "encrypt",
            vec![(None, json!("Vault.value")), (None, json!("classified"))],
        )
        .unwrap();
        assert!(encrypted.as_str().unwrap().starts_with("enc:v2:"));
        let magic = invoke_security_builtin(
            &db,
            manifest.clone(),
            services.clone(),
            actor.clone(),
            "auth.magic_link_issue",
            security_named(vec![
                ("subject", json!(user_id.to_string())),
                ("email", json!("ada@example.com")),
                ("secret", json!("MAGIC_LINK_KEY")),
                ("ttl_seconds", json!(300)),
            ]),
        )
        .unwrap();
        let oauth = invoke_security_builtin(
            &db,
            manifest.clone(),
            services.clone(),
            actor.clone(),
            "auth.oauth_authorize",
            security_named(vec![
                (
                    "authorize_url",
                    json!("https://identity.example.test/authorize"),
                ),
                ("client_id", json!("carrier-client")),
                (
                    "redirect_uri",
                    json!("https://carrier.example.test/callback"),
                ),
                ("scopes", json!(["openid", "email"])),
            ]),
        )
        .unwrap();

        secrets
            .insert(
                "MAGIC_LINK_KEY",
                "v2",
                "magic-v2",
                "hmac-sha256",
                vec![44; 32],
                true,
            )
            .unwrap();
        secrets
            .insert(
                "FIELD_KEY",
                "v2",
                "field-v2",
                "aes-256-gcm",
                vec![45; 32],
                true,
            )
            .unwrap();

        let decrypted = invoke_security_builtin(
            &db,
            manifest.clone(),
            services.clone(),
            actor.clone(),
            "decrypt",
            vec![(None, json!("Vault.value")), (None, encrypted)],
        )
        .unwrap();
        assert_eq!(decrypted, "classified");
        let magic_arguments = security_named(vec![
            ("token", magic["token"].clone()),
            ("secret", json!("MAGIC_LINK_KEY")),
        ]);
        let principal = invoke_security_builtin(
            &db,
            manifest.clone(),
            services.clone(),
            actor.clone(),
            "auth.magic_link_verify",
            magic_arguments.clone(),
        )
        .unwrap();
        assert_eq!(principal["subject"], user_id.to_string());
        assert!(invoke_security_builtin(
            &db,
            manifest.clone(),
            services.clone(),
            actor.clone(),
            "auth.magic_link_verify",
            magic_arguments,
        )
        .is_err());

        let callback_arguments = security_named(vec![(
            "query",
            json!({"state": oauth["state"], "code": "provider-code"}),
        )]);
        let callback = invoke_security_builtin(
            &db,
            manifest.clone(),
            services.clone(),
            actor.clone(),
            "auth.oauth_callback",
            callback_arguments.clone(),
        )
        .unwrap();
        assert_eq!(callback["code"], "provider-code");
        assert!(invoke_security_builtin(
            &db,
            manifest,
            services,
            actor,
            "auth.oauth_callback",
            callback_arguments,
        )
        .is_err());
        let observations = observations.lock().unwrap();
        assert!(observations.iter().any(|event| matches!(
            event,
            ObservabilityEvent::Evidence { control, .. }
                if control == "carrier.auth.tokens.issue"
        )));
        assert!(observations.iter().any(|event| matches!(
            event,
            ObservabilityEvent::Evidence { fields, .. }
                if fields.get("key_version") == Some(&json!("v1"))
        )));
    }

    #[test]
    fn carrier_blob_helpers_use_virtual_files_durable_keys_and_isolated_namespaces() {
        let directory = tempfile::tempdir().unwrap();
        let db = BicDb::open(directory.path().join("blob-database")).unwrap();
        let signing_key = SigningKey::from_bytes(&[42; 32]);
        let mut manifest = signed_package("1.0.0", &signing_key).manifest;
        manifest.capabilities.extend([
            bicdb_extension::ExtensionCapability::Blobs,
            bicdb_extension::ExtensionCapability::Observability,
        ]);
        let application = manifest.application.as_deref_mut().unwrap();
        application
            .required_features
            .extend([ApplicationFeature::Blobs, ApplicationFeature::Observability]);
        application.blobs = vec![BlobDeclaration {
            namespace: "carrier".to_string(),
            max_blob_bytes: 1_048_576,
            content_types: BTreeSet::new(),
            allow_signed_urls: true,
            require_scan: false,
        }];
        let call = |target: &str, arguments: Value| {
            json!({
                "body": [{
                    "op": "return",
                    "value": {
                        "op": "call",
                        "kind": "builtin",
                        "target": target,
                        "arguments": arguments
                    }
                }]
            })
        };
        application.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "max_steps": 100,
                "callables": {
                    "put": call("blob.put", json!([
                        {"value": {"op": "literal", "value": "/virtual/report.txt"}},
                        {"value": {"op": "literal", "value": "copies/report.txt"}}
                    ])),
                    "get": call("blob.get", json!([
                        {"value": {"op": "literal", "value": "incoming/report.txt"}},
                        {"value": {"op": "literal", "value": "/virtual/report.txt"}}
                    ])),
                    "metadata": call("blob.metadata", json!([
                        {"value": {"op": "literal", "value": "copies/report.txt"}}
                    ])),
                    "hash": call("blob.hash", json!([
                        {"value": {"op": "literal", "value": "copies/report.txt"}}
                    ])),
                    "download": call("blob.signed_url", json!([
                        {"value": {"op": "literal", "value": "copies/report.txt"}}
                    ])),
                    "upload": call("blob.signed_url", json!([
                        {"value": {"op": "literal", "value": "incoming/report.txt"}},
                        {
                            "name": "method",
                            "value": {"op": "literal", "value": "PUT"}
                        }
                    ]))
                },
                "blob": {
                    "version": 1,
                    "helpers": [
                        "blob.put",
                        "blob.get",
                        "blob.signed_url",
                        "blob.metadata",
                        "blob.hash"
                    ],
                    "namespace": "carrier",
                    "signed_methods": ["GET", "PUT"],
                    "max_blob_bytes": 1_048_576,
                    "virtual_files": true,
                    "durable_keys": true,
                    "emit_evidence": true
                }
            }))
            .unwrap(),
        );
        let manifest = Arc::new(manifest);
        let provider =
            Arc::new(LocalBlobProvider::open(directory.path().join("blobs"), vec![8; 32]).unwrap());
        let provider_namespace =
            application_blob_provider_namespace(&manifest.identity.name, "carrier");
        provider
            .put_named(
                &provider_namespace,
                "incoming/report.txt",
                b"BicDB blob slice",
                Some("text/plain"),
                &BTreeMap::new(),
                false,
            )
            .unwrap();
        let observations = Arc::new(Mutex::<Vec<ObservabilityEvent>>::default());
        let services = InvocationServices::new(
            Arc::new(InMemorySecretProvider::default()),
            Arc::new(ProductionEgressProvider::new().unwrap()),
            provider.clone(),
            observations.clone(),
        );
        let actor = ActorContext {
            service_id: Some("carrier-blob".to_string()),
            trace_id: uuid::Uuid::new_v4().to_string(),
            deadline_unix_ms: crate::host::now_ms() + 60_000,
            ..ActorContext::default()
        };
        let program = manifest
            .application
            .as_deref()
            .unwrap()
            .application_program
            .as_ref()
            .unwrap()
            .clone();
        let mut capability =
            CapabilityHost::new(&db, manifest, actor, services).expect("blob capability host");
        let mut adapter = CapabilityApplicationProgramHost::new(
            &mut capability,
            Vec::new(),
            program.service_bindings,
            program.client_bindings,
            program.secret_bindings,
            program.event_bindings,
            program.realtime_bindings,
            program.workflow_bindings,
        );

        let denied = adapter
            .call_builtin(
                "blob.put",
                vec![
                    (None, json!("/ambient/report.txt")),
                    (None, json!("denied/report.txt")),
                ],
            )
            .unwrap_err();
        assert!(denied.to_string().contains("ambient filesystem paths"));

        let fetched = adapter
            .call_builtin(
                "blob.get",
                vec![
                    (None, json!("incoming/report.txt")),
                    (None, json!("/virtual/report.txt")),
                ],
            )
            .unwrap();
        assert_eq!(fetched["size_bytes"], 16);
        assert_eq!(fetched["content_type"], "text/plain");
        assert_eq!(
            adapter
                .call_builtin(
                    "blob.content_type",
                    vec![(None, json!("/virtual/report.txt"))],
                )
                .unwrap(),
            json!("text/plain")
        );
        let copied = adapter
            .call_builtin(
                "blob.put",
                vec![
                    (None, json!("/virtual/report.txt")),
                    (None, json!("copies/report.txt")),
                ],
            )
            .unwrap();
        assert_eq!(copied["sha256"], fetched["sha256"]);
        assert_eq!(
            adapter
                .call_builtin("blob.hash", vec![(None, json!("copies/report.txt"))])
                .unwrap(),
            copied["sha256"]
        );
        let download = adapter
            .call_builtin(
                "blob.signed_url",
                vec![
                    (None, json!("copies/report.txt")),
                    (Some("expires_seconds".to_string()), json!(300)),
                    (Some("method".to_string()), json!("GET")),
                    (Some("download_name".to_string()), json!("report.txt")),
                ],
            )
            .unwrap();
        assert!(download.as_str().unwrap().starts_with("/_bicdb/blob?"));
        drop(adapter);

        assert_eq!(
            provider
                .get_named(&provider_namespace, "copies/report.txt")
                .unwrap()
                .unwrap()
                .bytes,
            b"BicDB blob slice"
        );
        let other_namespace = application_blob_provider_namespace("other-app", "carrier");
        assert!(provider
            .get_named(&other_namespace, "copies/report.txt")
            .unwrap()
            .is_none());
        assert!(observations.lock().unwrap().iter().any(|event| matches!(
            event,
            ObservabilityEvent::Evidence { control, .. } if control == "carrier.blob.put"
        )));
    }

    #[test]
    fn nested_service_intersection_preserves_callee_imports_and_exact_denials() {
        let signing_key = SigningKey::from_bytes(&[42; 32]);
        let mut caller = signed_package("1.0.0", &signing_key).manifest;
        let mut callee = signed_package("1.0.0", &signing_key).manifest;
        let caller_application = caller.application.as_deref_mut().unwrap();
        caller_application.max_call_depth = 3;
        caller_application
            .required_features
            .insert(ApplicationFeature::ExactColumnAuthority);
        caller_application
            .relation_permissions
            .push(RelationPermission {
                relation: "documents".to_string(),
                actions: BTreeSet::from([DatabaseAction::Select]),
                readable_columns: BTreeSet::new(),
                writable_columns: BTreeSet::new(),
            });

        let callee_application = callee.application.as_deref_mut().unwrap();
        callee_application.max_call_depth = 12;
        callee_application
            .required_features
            .insert(ApplicationFeature::ExactColumnAuthority);
        callee_application
            .relation_permissions
            .push(RelationPermission {
                relation: "documents".to_string(),
                actions: BTreeSet::from([DatabaseAction::Select]),
                readable_columns: BTreeSet::from(["id".to_string(), "body".to_string()]),
                writable_columns: BTreeSet::new(),
            });
        callee_application.service_imports.push(ServiceImport {
            name: "leaf_plugin".to_string(),
            service: "leaf_api".to_string(),
            version: "=1.0.0".to_string(),
            contract_sha256: "a".repeat(64),
            optional: false,
            propagate_transaction: false,
            allow_reentrant: false,
            delegated_authority: false,
        });
        let expected_imports = callee_application.service_imports.clone();

        let effective = intersect_manifests(&caller, &callee).unwrap();
        let application = effective.application.as_deref().unwrap();
        assert_eq!(application.max_call_depth, 3);
        assert_eq!(application.service_imports, expected_imports);
        assert!(application
            .required_features
            .contains(&ApplicationFeature::ExactColumnAuthority));
        let permission = application.permission_for("documents").unwrap();
        assert!(permission.readable_columns.is_empty());
        assert!(permission.writable_columns.is_empty());
    }

    #[test]
    fn delegated_service_authority_preserves_callee_provider_capabilities() {
        let signing_key = SigningKey::from_bytes(&[43; 32]);
        let caller = signed_package("1.0.0", &signing_key).manifest;
        let mut callee = signed_package("1.0.0", &signing_key).manifest;
        callee.capabilities.extend([
            bicdb_extension::ExtensionCapability::NetworkEgress,
            bicdb_extension::ExtensionCapability::Observability,
        ]);

        assert!(!caller
            .capabilities
            .contains(&bicdb_extension::ExtensionCapability::NetworkEgress));
        let legacy = service_execution_manifest(&caller, &callee, false).unwrap();
        assert!(!legacy
            .capabilities
            .contains(&bicdb_extension::ExtensionCapability::NetworkEgress));

        let delegated = service_execution_manifest(&caller, &callee, true).unwrap();
        assert!(delegated
            .capabilities
            .contains(&bicdb_extension::ExtensionCapability::NetworkEgress));
        assert!(delegated
            .capabilities
            .contains(&bicdb_extension::ExtensionCapability::Observability));
    }

    #[test]
    fn service_contracts_preserve_declared_failures_and_reject_shape_drift() {
        let method: ServiceMethod = serde_json::from_value(json!({
            "name": "tokenize",
            "request": [{
                "name": "input",
                "field_type": "json",
                "value_type": {"kind": "object", "fields": [{
                    "name": "text",
                    "value_type": {"kind": "string"},
                    "optional": false,
                    "validations": []
                }]},
                "nullable": false,
                "generated": false
            }],
            "response": [{
                "name": "value",
                "field_type": "int64",
                "value_type": {"kind": "int"},
                "nullable": false,
                "generated": false
            }],
            "errors": ["tokenizer_busy"],
            "allows_undeclared_errors": false,
            "retryable_errors": ["tokenizer_busy"]
        }))
        .unwrap();

        validate_service_request(&json!({"input": {"text": "hello"}}), &method).unwrap();
        assert!(validate_service_request(&json!({"input": {"text": 7}}), &method).is_err());
        assert!(validate_service_request(
            &json!({"input": {"text": "hello"}, "forged": true}),
            &method
        )
        .is_err());
        validate_service_response(&json!(3), &method).unwrap();
        assert!(validate_service_response(&json!("3"), &method).is_err());

        let error = validate_service_outcome(
            Err(AppRuntimeError::ApplicationFailure {
                code: "tokenizer_busy".to_string(),
                message: "retry later".to_string(),
                retryable: false,
            }),
            &method,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            AppRuntimeError::ApplicationFailure {
                code,
                retryable: true,
                ..
            } if code == "tokenizer_busy"
        ));
        assert!(matches!(
            validate_service_outcome(
                Err(AppRuntimeError::ApplicationFailure {
                    code: "undeclared".to_string(),
                    message: "forged".to_string(),
                    retryable: false,
                }),
                &method,
            ),
            Err(AppRuntimeError::Provider(_))
        ));
    }

    #[test]
    fn compiler_signed_feature_flags_match_tenant_and_stable_percentage_rules() {
        let actor = ActorContext {
            user_id: Some("user-a".to_string()),
            tenant_id: Some("tenant-a".to_string()),
            workspace_id: Some("workspace-a".to_string()),
            ..ActorContext::default()
        };
        let tenant = ApplicationFlagDefinitionV1 {
            default: false,
            rules: vec![ApplicationFlagRuleV1::TenantIn {
                tenants: vec!["tenant-a".to_string()],
                value: true,
            }],
        };
        assert!(carrier_feature_flag_enabled(
            "tenant-rollout",
            &tenant,
            &actor
        ));

        let percentage = ApplicationFlagDefinitionV1 {
            default: false,
            rules: vec![ApplicationFlagRuleV1::Percentage {
                percent: 100,
                grouped_by: ApplicationFlagGroupV1::WorkspaceId,
                value: true,
            }],
        };
        assert!(carrier_feature_flag_enabled(
            "workspace-rollout",
            &percentage,
            &actor
        ));
        assert_eq!(carrier_flag_bucket("workspace-rollout", "workspace-a"), 50);
    }

    #[test]
    fn carrier_commit_validation_is_an_http_conflict() {
        let error = carrier_host_error(bicdb_extension::abi_v2::HostError {
            code: "commit_validation".to_string(),
            class: ErrorClass::CommitValidation,
            message: "duplicate unique value".to_string(),
            retryable: false,
            retry_after_ms: None,
            trace_id: "trace".to_string(),
        });
        assert!(matches!(error, AppRuntimeError::Conflict(_)));
    }

    #[test]
    fn carrier_cron_and_bounded_intervals_are_accepted() {
        let now_ms = Utc::now().timestamp_millis();
        let cron =
            parse_schedule_plan("*/5 * * * *", "UTC").expect("five-field BicDB application cron");
        let next = cron.next_after_ms(now_ms).expect("next cron run");
        assert!(next > now_ms);
        assert!(next - now_ms <= 5 * 60 * 1_000);
        let interval = parse_schedule_plan("@every 250ms", "UTC").expect("bounded interval");
        assert_eq!(interval.next_after_ms(1_000).unwrap(), 1_250);
        assert!(parse_schedule_plan("not a schedule", "UTC").is_err());
        assert!(parse_schedule_plan("0 9 * * *", "Mars/Olympus").is_err());
    }

    fn durable_schedule(
        misfire: ScheduleMisfirePolicy,
        overlap: ScheduleOverlapPolicy,
        max_concurrency: u16,
        catch_up_limit: u16,
        upgrade: ScheduleUpgradePolicy,
    ) -> ScheduleDefinition {
        ScheduleDefinition {
            name: "daily_reconcile".to_string(),
            export: "reconcile".to_string(),
            schedule: "@every 1s".to_string(),
            timezone: "UTC".to_string(),
            payload: json!({"source": "scheduler-test"}),
            required: true,
            misfire,
            overlap,
            max_concurrency,
            catch_up_limit,
            upgrade,
        }
    }

    fn durable_schedule_state(cursor_unix_ms: i64) -> DurableScheduleState {
        DurableScheduleState {
            application: "carrier-app".to_string(),
            schedule: "daily_reconcile".to_string(),
            contract_sha256: "contract-a".to_string(),
            revision: 1,
            cursor_unix_ms,
            pending: VecDeque::new(),
            running: Vec::new(),
            completed_runs: 0,
            failed_runs: 0,
            last_completed_at_ms: None,
            last_error: None,
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }

    #[test]
    fn durable_schedule_misfire_policies_advance_the_cursor_without_unbounded_replay() {
        let plan = parse_schedule_plan("@every 1s", "UTC").unwrap();

        let mut skipped = durable_schedule_state(0);
        let skip = durable_schedule(
            ScheduleMisfirePolicy::Skip,
            ScheduleOverlapPolicy::Skip,
            1,
            100,
            ScheduleUpgradePolicy::Preserve,
        );
        assert!(apply_schedule_tick(
            &mut skipped,
            &skip,
            &plan,
            "node-a",
            "instance-a",
            "contract-a",
            5_500,
            10_000,
        )
        .unwrap()
        .is_none());
        assert_eq!(skipped.cursor_unix_ms, 5_500);

        let mut fired_once = durable_schedule_state(0);
        let fire_once = durable_schedule(
            ScheduleMisfirePolicy::FireOnce,
            ScheduleOverlapPolicy::Skip,
            1,
            100,
            ScheduleUpgradePolicy::Preserve,
        );
        let claim = apply_schedule_tick(
            &mut fired_once,
            &fire_once,
            &plan,
            "node-a",
            "instance-a",
            "contract-a",
            5_500,
            10_000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(claim.scheduled_for_ms, 5_500);
        assert_eq!(fired_once.cursor_unix_ms, 5_500);

        let mut caught_up = durable_schedule_state(0);
        let catch_up = durable_schedule(
            ScheduleMisfirePolicy::CatchUp,
            ScheduleOverlapPolicy::Queue,
            1,
            3,
            ScheduleUpgradePolicy::Preserve,
        );
        let claim = apply_schedule_tick(
            &mut caught_up,
            &catch_up,
            &plan,
            "node-a",
            "instance-a",
            "contract-a",
            5_500,
            10_000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(claim.scheduled_for_ms, 1_000);
        assert_eq!(caught_up.cursor_unix_ms, 3_000);
        assert_eq!(
            caught_up
                .pending
                .iter()
                .map(|run| run.scheduled_for_ms)
                .collect::<Vec<_>>(),
            vec![2_000, 3_000]
        );
    }

    #[test]
    fn durable_schedule_queue_respects_global_concurrency() {
        let plan = parse_schedule_plan("@every 1s", "UTC").unwrap();
        let schedule = durable_schedule(
            ScheduleMisfirePolicy::CatchUp,
            ScheduleOverlapPolicy::Queue,
            2,
            10,
            ScheduleUpgradePolicy::Preserve,
        );
        let mut state = durable_schedule_state(0);
        let first = apply_schedule_tick(
            &mut state,
            &schedule,
            &plan,
            "node-a",
            "instance-a",
            "contract-a",
            4_000,
            10_000,
        )
        .unwrap()
        .unwrap();
        let second = apply_schedule_tick(
            &mut state,
            &schedule,
            &plan,
            "node-a",
            "instance-a",
            "contract-a",
            4_000,
            10_000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            (first.scheduled_for_ms, second.scheduled_for_ms),
            (1_000, 2_000)
        );
        assert_eq!(state.running.len(), 2);
        assert_eq!(state.pending.len(), 2);
        assert!(apply_schedule_tick(
            &mut state,
            &schedule,
            &plan,
            "node-b",
            "instance-b",
            "contract-a",
            4_000,
            10_000,
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn durable_schedule_restart_reclaims_the_local_lease_exactly_once() {
        let plan = parse_schedule_plan("@every 1s", "UTC").unwrap();
        let schedule = durable_schedule(
            ScheduleMisfirePolicy::Skip,
            ScheduleOverlapPolicy::Queue,
            1,
            100,
            ScheduleUpgradePolicy::Preserve,
        );
        let mut state = durable_schedule_state(10_000);
        state.running.push(DurableRunningScheduleOccurrence {
            scheduled_for_ms: 9_000,
            attempt: 1,
            node_id: "node-a".to_string(),
            instance_id: "stopped-instance".to_string(),
            lease_until_ms: 20_000,
            started_at_ms: 9_000,
        });
        let claim = apply_schedule_tick(
            &mut state,
            &schedule,
            &plan,
            "node-a",
            "restarted-instance",
            "contract-a",
            10_000,
            10_000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(claim.scheduled_for_ms, 9_000);
        assert_eq!(claim.attempt, 2);
        assert_eq!(state.running.len(), 1);
        assert_eq!(state.running[0].instance_id, "restarted-instance");
    }

    #[test]
    fn durable_schedule_upgrade_policy_preserves_or_resets_state() {
        let plan = parse_schedule_plan("@every 1s", "UTC").unwrap();
        let mut preserved = durable_schedule_state(10_000);
        preserved.pending.push_back(DurableScheduleOccurrence {
            scheduled_for_ms: 9_000,
            attempt: 1,
        });
        preserved.running.push(DurableRunningScheduleOccurrence {
            scheduled_for_ms: 8_000,
            attempt: 1,
            node_id: "node-b".to_string(),
            instance_id: "instance-b".to_string(),
            lease_until_ms: 20_000,
            started_at_ms: 8_000,
        });
        let preserve = durable_schedule(
            ScheduleMisfirePolicy::Skip,
            ScheduleOverlapPolicy::Queue,
            1,
            100,
            ScheduleUpgradePolicy::Preserve,
        );
        assert!(apply_schedule_tick(
            &mut preserved,
            &preserve,
            &plan,
            "node-a",
            "instance-a",
            "contract-b",
            10_000,
            10_000,
        )
        .unwrap()
        .is_none());
        assert_eq!(preserved.contract_sha256, "contract-b");
        assert_eq!(preserved.pending.len(), 1);
        assert_eq!(preserved.running.len(), 1);

        let mut reset = preserved;
        let reset_policy = durable_schedule(
            ScheduleMisfirePolicy::Skip,
            ScheduleOverlapPolicy::Queue,
            1,
            100,
            ScheduleUpgradePolicy::Reset,
        );
        assert!(apply_schedule_tick(
            &mut reset,
            &reset_policy,
            &plan,
            "node-a",
            "instance-a",
            "contract-c",
            11_000,
            10_000,
        )
        .unwrap()
        .is_none());
        assert_eq!(reset.contract_sha256, "contract-c");
        assert_eq!(reset.cursor_unix_ms, 11_000);
        assert!(reset.pending.is_empty());
        assert!(reset.running.is_empty());
    }

    #[test]
    fn durable_schedule_cron_uses_the_signed_iana_timezone_across_dst() {
        let plan = parse_schedule_plan("30 1 * * *", "America/Los_Angeles").unwrap();
        let cursor = Utc
            .with_ymd_and_hms(2026, 11, 1, 7, 0, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        let next = plan.next_after_ms(cursor).unwrap();
        assert_eq!(
            next,
            Utc.with_ymd_and_hms(2026, 11, 1, 8, 30, 0)
                .single()
                .unwrap()
                .timestamp_millis()
        );
    }

    #[test]
    fn durable_schedule_state_is_versioned_on_disk_and_package_scoped() {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("schedule-db");
        let mut database = BicDb::open(&database_path).unwrap();
        database.create_collection(SCHEDULE_STATE_RELATION).unwrap();
        database
            .set_mutation_policy(
                SCHEDULE_STATE_RELATION,
                MutationPolicy::grants_required()
                    .with_version_field("revision")
                    .with_immutable_fields(["application", "schedule", "created_at_ms"]),
            )
            .unwrap();
        let db = Arc::new(RwLock::new(database));
        let signing_key = SigningKey::from_bytes(&[42; 32]);
        let first = signed_package("1.0.0", &signing_key);
        let second = signed_package("1.1.0", &signing_key);
        let schedule = durable_schedule(
            ScheduleMisfirePolicy::CatchUp,
            ScheduleOverlapPolicy::Queue,
            2,
            10,
            ScheduleUpgradePolicy::Preserve,
        );
        let first_contract = schedule_contract_sha256(&first.manifest, &schedule).unwrap();
        let second_contract = schedule_contract_sha256(&second.manifest, &schedule).unwrap();
        assert_ne!(first_contract, second_contract);
        let now_ms = crate::host::now_ms();

        mutate_durable_schedule_state(
            &db,
            &first.manifest,
            &schedule,
            &first_contract,
            now_ms,
            |state| {
                state.completed_runs = 7;
                Ok(())
            },
        )
        .unwrap();
        let record = db
            .read()
            .get(SCHEDULE_STATE_RELATION, "carrier-app:daily_reconcile")
            .unwrap()
            .unwrap();
        let state: DurableScheduleState = serde_json::from_value(record.metadata.clone()).unwrap();
        assert_eq!(state.revision, 1);
        assert_eq!(state.completed_runs, 7);
        assert_eq!(state.contract_sha256, first_contract);

        drop(record);
        drop(db);
        let reopened = BicDb::open(database_path).unwrap();
        let record = reopened
            .get(SCHEDULE_STATE_RELATION, "carrier-app:daily_reconcile")
            .unwrap()
            .unwrap();
        let state: DurableScheduleState = serde_json::from_value(record.metadata.clone()).unwrap();
        assert_eq!(state.revision, 1);
        assert_eq!(state.completed_runs, 7);
    }

    #[test]
    fn carrier_client_urls_cannot_escape_signed_origin_or_path() {
        assert_eq!(
            carrier_client_url(
                "Api",
                "https://api.example.test/v1",
                "/status",
                Some(json!({"tag": ["a", "b"], "page": 2})),
            )
            .unwrap(),
            "https://api.example.test/v1/status?page=2&tag=a&tag=b"
        );
        assert!(
            carrier_client_url("Api", "https://api.example.test/v1", "../admin", None,).is_err()
        );
        assert!(carrier_client_url(
            "Api",
            "https://api.example.test/v1",
            "https://evil.example/admin",
            None,
        )
        .is_err());
        assert_eq!(
            carrier_client_relative_url(
                "Api",
                "/status",
                Some(json!({"tag": ["a", "b"], "page": 2})),
            )
            .unwrap(),
            "/status?page=2&tag=a&tag=b"
        );
        for escape in [
            "../admin",
            "//evil.example/admin",
            "https://evil.example/admin",
            "/status\\admin",
        ] {
            assert!(
                carrier_client_relative_url("Api", escape, None).is_err(),
                "{escape}"
            );
        }
    }

    #[test]
    fn carrier_workflow_steps_checkpoint_and_complete_durably() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("workflow-db")).unwrap();
        db.create_collection("__bicdb_app_workflows").unwrap();
        db.set_mutation_policy(
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
        )
        .unwrap();
        db.create_collection("__bicdb_app_audit").unwrap();
        db.set_mutation_policy(
            "__bicdb_app_audit",
            MutationPolicy::grants_required().append_only(),
        )
        .unwrap();
        let services = InvocationServices::new(
            Arc::new(InMemorySecretProvider::default()),
            Arc::new(ProductionEgressProvider::new().unwrap()),
            Arc::new(
                LocalBlobProvider::open(directory.path().join("workflow-blobs"), vec![9; 32])
                    .unwrap(),
            ),
            Arc::new(Mutex::<Vec<ObservabilityEvent>>::default()),
        );
        let program = ApplicationProgramV1 {
            version: 1,
            max_steps: 100,
            max_call_depth: 16,
            blob: None,
            redis: None,
            email: None,
            grpc: None,
            tokenizer: None,
            embeddings: None,
            llm: None,
            rag: None,
            agents: None,
            evaluations: None,
            tests: None,
            observability: None,
            callables: BTreeMap::from([
                (
                    "wf_prepare".to_string(),
                    ApplicationCallableV1 {
                        parameters: Vec::new(),
                        body: vec![ApplicationStatementV1::Return {
                            value: ApplicationExpressionV1::Field {
                                target: Box::new(ApplicationExpressionV1::Variable {
                                    name: "input".to_string(),
                                }),
                                field: "value".to_string(),
                            },
                        }],
                    },
                ),
                (
                    "wf_finish".to_string(),
                    ApplicationCallableV1 {
                        parameters: Vec::new(),
                        body: vec![ApplicationStatementV1::Return {
                            value: ApplicationExpressionV1::Variable {
                                name: "prepare".to_string(),
                            },
                        }],
                    },
                ),
            ]),
            service_bindings: BTreeMap::new(),
            client_bindings: BTreeMap::new(),
            flags: BTreeMap::new(),
            job_bindings: BTreeMap::new(),
            secret_bindings: BTreeMap::new(),
            security: None,
            event_bindings: BTreeMap::new(),
            realtime_bindings: BTreeMap::new(),
            mutation_bindings: Vec::new(),
            workflow_bindings: BTreeMap::from([
                (
                    "Durable".to_string(),
                    ApplicationWorkflowDefinitionV1 {
                        queue: "carrier_workflow_test".to_string(),
                        worker_export: "workflow_driver".to_string(),
                        timeout_ms: Some(60_000),
                        max_retries: 2,
                        graph_execution: false,
                        max_parallelism: 1,
                        plan_sha256: Some("a".repeat(64)),
                        return_step: "finish".to_string(),
                        slas: Vec::new(),
                        invariants: vec![bicdb_extension::abi_v2::ApplicationWorkflowInvariantV1 {
                            name: "InputAccepted".to_string(),
                            kind: bicdb_extension::abi_v2::ApplicationInvariantKindV1::MustAlways,
                            expression: ApplicationExpressionV1::Binary {
                                operator: "not_equal".to_string(),
                                left: Box::new(ApplicationExpressionV1::Field {
                                    target: Box::new(ApplicationExpressionV1::Variable {
                                        name: "input".to_string(),
                                    }),
                                    field: "value".to_string(),
                                }),
                                right: Box::new(ApplicationExpressionV1::Literal {
                                    value: json!("rejected"),
                                }),
                                value_type: None,
                                left_type: None,
                                right_type: None,
                            },
                            source: "input.value != rejected".to_string(),
                        }],
                        steps: vec![
                            ApplicationWorkflowStepV1 {
                                name: "prepare".to_string(),
                                callable: "wf_prepare".to_string(),
                                condition_callable: None,
                                dependencies: Vec::new(),
                                max_retries: 2,
                                retry_delay_ms: 1,
                                compensation_callable: None,
                                wait: None,
                            },
                            ApplicationWorkflowStepV1 {
                                name: "finish".to_string(),
                                callable: "wf_finish".to_string(),
                                condition_callable: None,
                                dependencies: Vec::new(),
                                max_retries: 2,
                                retry_delay_ms: 1,
                                compensation_callable: None,
                                wait: None,
                            },
                        ],
                    },
                ),
                (
                    "WaitFlow".to_string(),
                    ApplicationWorkflowDefinitionV1 {
                        queue: "carrier_workflow_test".to_string(),
                        worker_export: "workflow_wait_driver".to_string(),
                        timeout_ms: Some(60_000),
                        max_retries: 2,
                        graph_execution: false,
                        max_parallelism: 1,
                        plan_sha256: Some("b".repeat(64)),
                        return_step: "approval".to_string(),
                        steps: vec![ApplicationWorkflowStepV1 {
                            name: "approval".to_string(),
                            callable: String::new(),
                            condition_callable: None,
                            dependencies: Vec::new(),
                            max_retries: 2,
                            retry_delay_ms: 1,
                            compensation_callable: None,
                            wait: Some(bicdb_extension::abi_v2::ApplicationWorkflowWaitV1 {
                                kind: ApplicationWorkflowWaitKindV1::Signal,
                                signal: Some("approved".to_string()),
                                delay_ms: None,
                                timeout_ms: Some(30_000),
                            }),
                        }],
                        slas: vec![bicdb_extension::abi_v2::ApplicationWorkflowSlaV1 {
                            name: "ApprovalSla".to_string(),
                            attainment_basis_points: 9_500,
                            deadline_ms: 10_000,
                            warning_at_basis_points: 5_000,
                            breach_at_basis_points: 10_000,
                            starts_when: None,
                            ends_when: None,
                            attach_to_audit: true,
                            scope: "tenant".to_string(),
                            measure_by: "calendar_month".to_string(),
                            reports: vec!["WorkflowHealth".to_string()],
                            exclusions: Vec::new(),
                            escalations: Vec::new(),
                        }],
                        invariants: Vec::new(),
                    },
                ),
            ]),
        };
        let mut package = signed_package("1.0.0", &SigningKey::from_bytes(&[42; 32])).manifest;
        package.capabilities.extend([
            bicdb_extension::ExtensionCapability::Database,
            bicdb_extension::ExtensionCapability::Transactions,
            bicdb_extension::ExtensionCapability::QueueEvents,
            bicdb_extension::ExtensionCapability::Jobs,
        ]);
        package
            .permissions
            .publish_queues
            .insert("carrier_workflow_test".to_string());
        package
            .permissions
            .consume_queues
            .insert("carrier_workflow_test".to_string());
        package
            .application
            .as_deref_mut()
            .unwrap()
            .application_program = Some(program.clone());
        let manifest = Arc::new(package);
        let actor = ActorContext {
            user_id: Some("7".to_string()),
            service_id: None,
            client_id: None,
            acting_client_id: None,
            authentication_method: Some("test".to_string()),
            roles: BTreeSet::from(["operator".to_string()]),
            scopes: BTreeSet::new(),
            tenant_id: Some("tenant-a".to_string()),
            workspace_id: Some("workspace-a".to_string()),
            organization_id: None,
            session_id: None,
            delegation_chain: Vec::new(),
            assurance_level: Some("test".to_string()),
            request_origin: Some("test".to_string()),
            trace_id: uuid::Uuid::new_v4().to_string(),
            correlation_id: None,
            causation_id: None,
            deadline_unix_ms: crate::host::now_ms() + 60_000,
            policy_attributes: BTreeMap::new(),
        };
        let run_id = {
            let mut capability =
                CapabilityHost::new(&db, manifest.clone(), actor.clone(), services.clone())
                    .unwrap();
            let mut adapter = CapabilityApplicationProgramHost::new(
                &mut capability,
                Vec::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                program.workflow_bindings.clone(),
            );
            assert!(matches!(
                adapter.start_workflow(&[
                    (None, Value::String("Durable".to_string())),
                    (None, json!({"value": "rejected"})),
                ]),
                Err(AppRuntimeError::InvalidRequest(message))
                    if message.contains("InputAccepted")
            ));
            adapter
                .start_workflow(&[
                    (None, Value::String("Durable".to_string())),
                    (None, json!({"value": "checkpointed"})),
                ])
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        };
        let queued = db.with_broker(|broker| broker.peek("carrier_workflow_test", 0, 10));
        let timeout = queued
            .iter()
            .find(|message| message.payload["kind"] == "workflow_timeout")
            .expect("workflow timeout message is queued");
        assert_eq!(timeout.available_at, timeout.created_at);
        assert!(matches!(
            execute_carrier_workflow_delivery(
                &db,
                manifest.clone(),
                actor.clone(),
                services.clone(),
                "Durable",
                &run_id,
                "step",
                Some("prepare"),
                None,
            )
            .unwrap(),
            ApplicationWorkflowDelivery::Ack
        ));
        assert!(matches!(
            execute_carrier_workflow_delivery(
                &db,
                manifest.clone(),
                actor.clone(),
                services.clone(),
                "Durable",
                &run_id,
                "step",
                Some("finish"),
                None,
            )
            .unwrap(),
            ApplicationWorkflowDelivery::Ack
        ));
        let mut capability =
            CapabilityHost::new(&db, manifest.clone(), actor.clone(), services.clone()).unwrap();
        let mut adapter = CapabilityApplicationProgramHost::new(
            &mut capability,
            Vec::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            program.workflow_bindings.clone(),
        );
        let state = adapter.load_workflow_state("Durable", &run_id).unwrap();
        assert_eq!(state.status, "completed");
        assert_eq!(
            state.output,
            Some(Value::String("checkpointed".to_string()))
        );
        assert_eq!(state.revision, 3);

        let wait_run = {
            let mut capability =
                CapabilityHost::new(&db, manifest.clone(), actor.clone(), services.clone())
                    .unwrap();
            let mut adapter = CapabilityApplicationProgramHost::new(
                &mut capability,
                Vec::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                program.workflow_bindings.clone(),
            );
            adapter
                .start_workflow(&[
                    (None, Value::String("WaitFlow".to_string())),
                    (None, json!({"request": "one"})),
                ])
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        };
        execute_carrier_workflow_delivery(
            &db,
            manifest.clone(),
            actor.clone(),
            services.clone(),
            "WaitFlow",
            &wait_run,
            "step",
            Some("approval"),
            None,
        )
        .unwrap();
        let mut capability =
            CapabilityHost::new(&db, manifest.clone(), actor.clone(), services.clone()).unwrap();
        let mut adapter = CapabilityApplicationProgramHost::new(
            &mut capability,
            Vec::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            program.workflow_bindings.clone(),
        );
        assert_eq!(
            adapter
                .signal_workflow(&[
                    (None, Value::String("WaitFlow".to_string())),
                    (None, Value::String(wait_run.clone())),
                    (None, Value::String("approved".to_string())),
                    (None, json!({"approved_by": "operator"})),
                ])
                .unwrap(),
            Value::String("completed".to_string())
        );
        let state = adapter.load_workflow_state("WaitFlow", &wait_run).unwrap();
        assert_eq!(state.status, "completed");
        assert_eq!(state.output, Some(json!({"approved_by": "operator"})));
        assert!(state
            .evidence
            .iter()
            .any(|entry| entry.kind == "wait_started"));
        assert!(state
            .evidence
            .iter()
            .any(|entry| entry.kind == "signal_received"));
        assert!(state
            .evidence
            .iter()
            .any(|entry| entry.kind == "sla_attained"));
        let cancelled_run = adapter
            .start_workflow(&[
                (None, Value::String("WaitFlow".to_string())),
                (None, json!({"request": "two"})),
            ])
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        drop(adapter);
        drop(capability);
        let audit = serde_json::to_string(&db.scan_collection("__bicdb_app_audit").unwrap())
            .expect("serialize workflow SLA audit evidence");
        assert!(audit.contains("sla_started"));
        assert!(audit.contains("sla_attained"));
        assert!(audit.contains("WaitFlow"));
        execute_carrier_workflow_delivery(
            &db,
            manifest.clone(),
            actor.clone(),
            services.clone(),
            "WaitFlow",
            &cancelled_run,
            "step",
            Some("approval"),
            None,
        )
        .unwrap();
        let mut capability = CapabilityHost::new(&db, manifest, actor, services).unwrap();
        let mut adapter = CapabilityApplicationProgramHost::new(
            &mut capability,
            Vec::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            program.workflow_bindings,
        );
        assert_eq!(
            adapter
                .cancel_workflow(&[
                    (None, Value::String("WaitFlow".to_string())),
                    (None, Value::String(cancelled_run.clone())),
                ])
                .unwrap(),
            Value::String("cancelled".to_string())
        );
        let state = adapter
            .load_workflow_state("WaitFlow", &cancelled_run)
            .unwrap();
        assert!(state.terminal());
        assert!(state.waiting_steps.is_empty());
        assert!(state
            .evidence
            .iter()
            .any(|entry| entry.kind == "workflow_cancelled"));
    }

    #[test]
    fn carrier_workflow_parallel_scheduler_releases_wait_slots() {
        let step = |name: &str| ApplicationWorkflowStepV1 {
            name: name.to_string(),
            callable: name.to_string(),
            condition_callable: None,
            dependencies: Vec::new(),
            max_retries: 1,
            retry_delay_ms: 1,
            compensation_callable: None,
            wait: None,
        };
        let definition = ApplicationWorkflowDefinitionV1 {
            queue: "parallel".to_string(),
            worker_export: "parallel_worker".to_string(),
            timeout_ms: None,
            max_retries: 1,
            graph_execution: true,
            max_parallelism: 2,
            plan_sha256: Some("c".repeat(64)),
            return_step: "finish".to_string(),
            steps: vec![step("a"), step("b"), step("c"), step("finish")],
            slas: Vec::new(),
            invariants: Vec::new(),
        };
        let mut state = ApplicationWorkflowStateV1 {
            application: "test".to_string(),
            workflow: "Parallel".to_string(),
            status: "queued".to_string(),
            input: json!({}),
            baggage: json!({}),
            step_results: BTreeMap::new(),
            skipped_steps: BTreeSet::new(),
            completed_step_order: Vec::new(),
            attempts: BTreeMap::new(),
            compensation_attempts: BTreeMap::new(),
            compensated_steps: BTreeSet::new(),
            scheduled_steps: BTreeSet::new(),
            active_steps: BTreeSet::new(),
            waiting_steps: BTreeMap::new(),
            sla_states: BTreeMap::new(),
            evidence: Vec::new(),
            audited_evidence_sequence: 0,
            cancel_requested: false,
            plan_sha256: definition.plan_sha256.clone(),
            active_step: None,
            last_error: None,
            output: None,
            timeout_at_ms: None,
            created_at_ms: 1,
            updated_at_ms: 1,
            finished_at_ms: None,
            tenant_id: None,
            workspace_id: None,
            revision: 1,
        };
        assert_eq!(
            schedule_ready_workflow_steps(&definition, &mut state),
            vec!["a", "b"]
        );
        state.waiting_steps.insert(
            "a".to_string(),
            ApplicationWorkflowWaitStateV1 {
                kind: "signal".to_string(),
                signal: Some("a".to_string()),
                due_at_ms: None,
            },
        );
        assert_eq!(
            schedule_ready_workflow_steps(&definition, &mut state),
            vec!["c"]
        );

        let previous = BTreeMap::from([("Parallel".to_string(), definition.clone())]);
        assert!(validate_workflow_binding_evolution(&previous, &previous).is_ok());
        let mut legacy = definition.clone();
        legacy.plan_sha256 = None;
        let mut hashed_legacy = legacy.clone();
        hashed_legacy.plan_sha256 = Some(carrier_workflow_plan_sha256(&legacy).unwrap());
        assert!(validate_workflow_binding_evolution(
            &BTreeMap::from([("Parallel".to_string(), legacy.clone())]),
            &BTreeMap::from([("Parallel".to_string(), hashed_legacy.clone())]),
        )
        .is_ok());
        assert!(validate_workflow_binding_evolution(
            &BTreeMap::from([("Parallel".to_string(), hashed_legacy)]),
            &BTreeMap::from([("Parallel".to_string(), legacy)]),
        )
        .is_ok());
        let mut changed = definition.clone();
        changed.plan_sha256 = Some("d".repeat(64));
        let error = validate_workflow_binding_evolution(
            &previous,
            &BTreeMap::from([("Parallel".to_string(), changed)]),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("changes durable BicDB application workflow"));
        assert!(validate_workflow_binding_evolution(&previous, &BTreeMap::new()).is_err());
    }

    fn digest(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn wasm_module(manifest: &ExtensionManifest, version: &str) -> Vec<u8> {
        let manifest = serde_json::to_string(manifest).unwrap();
        let escaped = manifest
            .as_bytes()
            .iter()
            .map(|byte| format!("\\{byte:02x}"))
            .collect::<String>();
        let result = serde_json::to_vec(&json!({
            "status": 200,
            "body": {"ok": true, "version": version},
            "ack": true
        }))
        .unwrap();
        let result_escaped = result
            .iter()
            .map(|byte| format!("\\{byte:02x}"))
            .collect::<String>();
        let result_pointer = 16_384_u32;
        wat::parse_str(format!(
            r#"(module
                (import "bicdb:app/host" "call"
                    (func $host_call (param i32 i32 i32 i32) (result i64)))
                (memory (export "memory") 2 1024)
                (global $next (mut i32) (i32.const 32768))
                (data (i32.const 1024) "{escaped}")
                (data (i32.const {result_pointer}) "{result_escaped}")
                (func (export "bicdb_extension_abi_version") (result i32)
                    i32.const 2)
                (func (export "bicdb_extension_manifest_ptr") (result i32)
                    i32.const 1024)
                (func (export "bicdb_extension_manifest_len") (result i32)
                    i32.const {manifest_len})
                (func (export "bicdb_extension_alloc") (param $len i32) (result i32)
                    (local $ptr i32)
                    global.get $next
                    local.tee $ptr
                    local.get $len
                    i32.add
                    global.set $next
                    local.get $ptr)
                (func (export "bicdb_extension_dealloc") (param i32 i32))
                (func (export "bicdb_extension_invoke") (param i32 i32) (result i64)
                    i64.const {packed}))"#,
            manifest_len = manifest.len(),
            packed = ((result_pointer as u64) << 32) | result.len() as u64,
        ))
        .unwrap()
    }

    fn signed_package(version: &str, signing_key: &SigningKey) -> ApplicationPackage {
        let dependency_lock = br#"{"format":1,"packages":[]}"#.to_vec();
        let sbom = br#"{"bomFormat":"CycloneDX","specVersion":"1.5"}"#.to_vec();
        let provenance = br#"{"builder":"bicdb-runtime-test"}"#.to_vec();
        let application = ApplicationManifestV2 {
            abi_version: 2,
            application_profile: APPLICATION_COMPATIBILITY_PROFILE.to_string(),
            package: PackageMetadata {
                application: "carrier-app".to_string(),
                version: version.to_string(),
                package_sha256: "0".repeat(64),
                dependency_lock_sha256: digest(&dependency_lock),
                sbom_sha256: digest(&sbom),
                provenance_sha256: digest(&provenance),
                signature_key_id: "release".to_string(),
                signature_algorithm: "ed25519".to_string(),
                signature: "unsigned-placeholder".to_string(),
            },
            relation_permissions: vec![],
            raw_sql: vec![],
            service_imports: vec![],
            service_exports: vec![],
            secrets: vec![],
            egress: vec![],
            blobs: vec![],
            routes: vec![],
            response_headers: BTreeMap::new(),
            auth_schemes: BTreeMap::new(),
            realtime: vec![],
            resources: vec![],
            invariants: vec![],
            migrations: vec![],
            workers: vec![],
            schedules: vec![],
            application_program: None,
            required_features: BTreeSet::new(),
            max_call_depth: 16,
        };
        let embedded = ExtensionManifest {
            identity: ExtensionIdentity {
                name: "carrier-app".to_string(),
                version: version.to_string(),
                abi_version: 2,
                description: "application lifecycle test".to_string(),
            },
            dependencies: vec![],
            capabilities: BTreeSet::new(),
            permissions: ExtensionPermissions::default(),
            limits: ExtensionLimits::default(),
            functions: vec![],
            indexes: vec![],
            storage: vec![],
            routes: vec![],
            subscriptions: vec![],
            observability: vec![],
            application: Some(Box::new(application)),
        };
        let module = wasm_module(&embedded, version);
        let mut package = ApplicationPackage {
            manifest: embedded,
            modules: BTreeMap::from([("carrier-app".to_string(), module)]),
            frontend_assets: BTreeMap::new(),
            components: Vec::new(),
            dependency_lock,
            sbom,
            provenance,
        };
        let payload = canonical_signing_payload(&package).unwrap();
        let package_sha256 = digest(&payload);
        let signature = signing_key.sign(package_sha256.as_bytes());
        let metadata = &mut package.manifest.application.as_deref_mut().unwrap().package;
        metadata.package_sha256 = package_sha256;
        metadata.signature = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
        package
    }

    fn test_application_runtime(
        root: &Path,
        signing_key: &SigningKey,
        module_cache_entries: usize,
        wasm_pool_capacity: u32,
    ) -> Arc<ApplicationRuntime> {
        let db = Arc::new(RwLock::new(BicDb::open(root.join("database")).unwrap()));
        let mut trusted = TrustedSigningKeys::default();
        trusted
            .insert_ed25519("release", signing_key.verifying_key().as_bytes())
            .unwrap();
        let verifier = PackageVerifier::new(trusted, 8 * 1024 * 1024).unwrap();
        let services = InvocationServices::new(
            Arc::new(InMemorySecretProvider::default()),
            Arc::new(ProductionEgressProvider::new().unwrap()),
            Arc::new(LocalBlobProvider::open(root.join("blobs"), vec![8; 32]).unwrap()),
            Arc::new(Mutex::<Vec<ObservabilityEvent>>::default()),
        );
        let mut config = ApplicationHostConfig::new(root.join("applications"), "perf-test-node");
        config.max_package_bytes = 8 * 1024 * 1024;
        config.max_history = 2;
        config.idempotency_entries = 1_000;
        config.route_cache_entries = 1_000;
        config.runtime_cache_entries = 1_000;
        config.module_cache_entries = module_cache_entries;
        config.wasm.max_pooled_instances = wasm_pool_capacity;
        ApplicationRuntime::new_shared(db, config, verifier, services).unwrap()
    }

    fn carrier_zero_hop_package(signing_key: &SigningKey) -> ApplicationPackage {
        let mut package = signed_package("1.0.0", signing_key);
        package
            .manifest
            .capabilities
            .insert(bicdb_extension::ExtensionCapability::HttpRoutes);
        let application = package.manifest.application.as_deref_mut().unwrap();
        application
            .required_features
            .insert(ApplicationFeature::Http);
        application.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "max_steps": 64,
                "max_call_depth": 8,
                "callables": {
                    "health": {
                        "body": [{
                            "op": "return",
                            "value": {
                                "op": "literal",
                                "value": {"status": "ok", "target": "bicdb"}
                            }
                        }]
                    }
                }
            }))
            .unwrap(),
        );
        application.routes = vec![serde_json::from_value(json!({
            "name": "health",
            "method": "GET",
            "template": "/health",
            "export": "health",
            "public": true,
            "max_request_bytes": 1024,
            "max_response_bytes": 4096
        }))
        .unwrap()];
        resign_package(&mut package, signing_key);
        package
    }

    fn resign_package(package: &mut ApplicationPackage, signing_key: &SigningKey) {
        let version = package.manifest.identity.version.clone();
        let name = package.manifest.identity.name.clone();
        let metadata = &mut package.manifest.application.as_deref_mut().unwrap().package;
        metadata.package_sha256 = "0".repeat(64);
        metadata.signature = "unsigned-placeholder".to_string();
        package
            .modules
            .insert(name.clone(), wasm_module(&package.manifest, &version));
        let payload = canonical_signing_payload(package).unwrap();
        let package_sha256 = digest(&payload);
        let signature = signing_key.sign(package_sha256.as_bytes());
        let metadata = &mut package.manifest.application.as_deref_mut().unwrap().package;
        metadata.package_sha256 = package_sha256;
        metadata.signature = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
    }

    fn signed_named_package(
        name: &str,
        version: &str,
        signing_key: &SigningKey,
    ) -> ApplicationPackage {
        let mut package = signed_package(version, signing_key);
        package.manifest.identity.name = name.to_string();
        package
            .manifest
            .application
            .as_deref_mut()
            .unwrap()
            .package
            .application = name.to_string();
        package.modules.clear();
        resign_package(&mut package, signing_key);
        package
    }

    fn signed_package_with_exact_dependency(
        name: &str,
        version: &str,
        provider: &ApplicationPackage,
        signing_key: &SigningKey,
    ) -> ApplicationPackage {
        let provider_name = provider.manifest.identity.name.clone();
        let provider_version = provider.manifest.identity.version.clone();
        let provider_hash = provider
            .manifest
            .application
            .as_deref()
            .unwrap()
            .package
            .package_sha256
            .clone();
        let mut package = signed_named_package(name, version, signing_key);
        package.manifest.dependencies = vec![ExtensionDependency {
            name: provider_name.clone(),
            version: format!("={provider_version}"),
            abi_version: 2,
            optional: false,
            capabilities: BTreeSet::new(),
            module_sha256: None,
        }];
        package.dependency_lock = serde_json::to_vec(&json!({
            "format": 1,
            "packages": [{
                "name": provider_name,
                "version": provider_version,
                "package_sha256": provider_hash,
            }],
        }))
        .unwrap();
        package
            .manifest
            .application
            .as_deref_mut()
            .unwrap()
            .package
            .dependency_lock_sha256 = digest(&package.dependency_lock);
        resign_package(&mut package, signing_key);
        package
    }

    fn signed_application_package(
        application: &str,
        namespace: &str,
        version: &str,
        system: bool,
        requires: &[(&str, &str)],
        stable_key: &str,
        signing_key: &SigningKey,
    ) -> ApplicationPackage {
        let mut package = signed_named_package(application, version, signing_key);
        let requirements = requires
            .iter()
            .map(|(namespace, version)| json!({"namespace": namespace, "version": version}))
            .collect::<Vec<_>>();
        let mut install_order = requires
            .iter()
            .map(|(namespace, _)| namespace.to_string())
            .collect::<Vec<_>>();
        install_order.push(namespace.to_string());
        let record_type_name = application
            .split('-')
            .map(|segment| {
                let mut chars = segment.chars();
                chars
                    .next()
                    .map(|first| first.to_ascii_uppercase().to_string() + chars.as_str())
                    .unwrap_or_default()
            })
            .collect::<String>();
        let contract = json!({
            "contract_version": 1,
            "namespace": namespace,
            "semantic_version": version,
            "system": system,
            "requires": requirements,
            "install_order": install_order,
            "record_types": [{
                "name": record_type_name,
                "namespace": namespace,
                "qualified_name": format!("{namespace}.{record_type_name}"),
                "stable_key": stable_key,
                "owner": namespace,
                "schema_version": 1,
                "scope_level": "organization",
                "model": record_type_name,
                "traits": ["OrganizationScoped"],
                "links": [],
            }],
            "traits": [],
            "extensions": [],
        });
        package.dependency_lock = serde_json::to_vec(&json!({
            "format": 1,
            "packages": [],
            "application_modules": {
                "namespace": namespace,
                "version": version,
                "system": system,
                "requires": contract["requires"],
                "install_order": contract["install_order"],
            },
        }))
        .unwrap();
        package.provenance = serde_json::to_vec(&json!({
            "builder": "bicdb-runtime-test",
            "target": "bicdb-application-v2",
            "application": application,
            "version": version,
            "resource_contracts": [],
            "plugin_dependencies": [],
            "application_module_contract": contract,
        }))
        .unwrap();
        let metadata = &mut package.manifest.application.as_deref_mut().unwrap().package;
        metadata.dependency_lock_sha256 = digest(&package.dependency_lock);
        metadata.provenance_sha256 = digest(&package.provenance);
        resign_package(&mut package, signing_key);
        package
    }

    fn blob_package(version: &str, signing_key: &SigningKey) -> ApplicationPackage {
        let mut package = signed_package(version, signing_key);
        package.manifest.capabilities.extend([
            bicdb_extension::ExtensionCapability::Blobs,
            bicdb_extension::ExtensionCapability::Observability,
        ]);
        let application = package.manifest.application.as_deref_mut().unwrap();
        application
            .required_features
            .extend([ApplicationFeature::Blobs, ApplicationFeature::Observability]);
        application.blobs = vec![BlobDeclaration {
            namespace: "carrier".to_string(),
            max_blob_bytes: 1_048_576,
            content_types: BTreeSet::from(["text/plain".to_string()]),
            allow_signed_urls: true,
            require_scan: false,
        }];
        application.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "callables": {
                    "download_url": {
                        "body": [{
                            "op": "return",
                            "value": {
                                "op": "call",
                                "kind": "builtin",
                                "target": "blob.signed_url",
                                "arguments": [{
                                    "value": {"op": "literal", "value": "reports/live.txt"}
                                }]
                            }
                        }]
                    },
                    "upload_url": {
                        "body": [{
                            "op": "return",
                            "value": {
                                "op": "call",
                                "kind": "builtin",
                                "target": "blob.signed_url",
                                "arguments": [
                                    {"value": {"op": "literal", "value": "reports/live.txt"}},
                                    {
                                        "name": "method",
                                        "value": {"op": "literal", "value": "PUT"}
                                    }
                                ]
                            }
                        }]
                    }
                },
                "blob": {
                    "version": 1,
                    "helpers": ["blob.signed_url"],
                    "namespace": "carrier",
                    "signed_methods": ["GET", "PUT"],
                    "max_blob_bytes": 1_048_576,
                    "virtual_files": true,
                    "durable_keys": true,
                    "emit_evidence": true
                }
            }))
            .unwrap(),
        );
        resign_package(&mut package, signing_key);
        package
    }

    fn schema_resource(
        include_name: bool,
        include_nickname: bool,
        nickname_index: bool,
    ) -> ResourceContractV1 {
        let mut fields = vec![json!({
            "name": "id",
            "field_type": "uuid"
        })];
        if include_name {
            fields.push(json!({
                "name": "name",
                "field_type": "string"
            }));
        }
        if include_nickname {
            fields.push(json!({
                "name": "nickname",
                "field_type": "string",
                "nullable": true
            }));
        }
        let indexes = if nickname_index {
            json!([{
                "name": "widgets_nickname_idx",
                "fields": ["nickname"]
            }])
        } else {
            json!([])
        };
        serde_json::from_value(json!({
            "version": 1,
            "name": "Widget",
            "relation": "widgets",
            "schema_version": 1,
            "schema_only": true,
            "primary_key": "id",
            "fields": fields,
            "indexes": indexes,
            "list_route": "/widgets",
            "item_route": "/widgets/{id}",
            "contract_sha256": "b".repeat(64),
            "openapi": {}
        }))
        .unwrap()
    }

    fn schema_package(
        version: &str,
        migration_version: u64,
        include_name: bool,
        include_nickname: bool,
        nickname_index: bool,
        signing_key: &SigningKey,
    ) -> ApplicationPackage {
        let mut package = signed_package(version, signing_key);
        let application = package.manifest.application.as_deref_mut().unwrap();
        application.resources = vec![schema_resource(
            include_name,
            include_nickname,
            nickname_index,
        )];
        application.migrations = vec![bicdb_extension::abi_v2::MigrationPlanV1 {
            version: 1,
            schema_version: migration_version,
            contract_versions: BTreeMap::from([("Widget".to_string(), 1)]),
            forward: vec![MigrationStep::ValidateContract {
                resource: "Widget".to_string(),
            }],
            compatibility_checks: vec!["additive".to_string()],
            transformations: vec![],
            activation_boundary: "catalog-swap".to_string(),
            rollback: vec![MigrationStep::ValidateContract {
                resource: "Widget".to_string(),
            }],
            irreversible: false,
            dependency_requirements: BTreeMap::new(),
        }];
        resign_package(&mut package, signing_key);
        package
    }

    fn required_fields_schema_package(
        version: &str,
        migration_version: u64,
        field_names: &[&str],
        signing_key: &SigningKey,
    ) -> ApplicationPackage {
        let mut package = schema_package(version, migration_version, true, true, true, signing_key);
        let application = package.manifest.application.as_deref_mut().unwrap();
        for field_name in field_names {
            let mut field = application.resources[0]
                .fields
                .iter()
                .find(|field| field.name == "nickname")
                .unwrap()
                .clone();
            field.name = (*field_name).to_string();
            field.storage_name = Some((*field_name).to_string());
            field.nullable = false;
            field.default_json = Some("\"unknown\"".to_string());
            application.resources[0].fields.push(field);
            application.migrations[0]
                .transformations
                .push(MigrationStep::BackfillField {
                    resource: "Widget".to_string(),
                    field: (*field_name).to_string(),
                    expression: ApplicationExpressionV1::Literal {
                        value: Value::String("unknown".to_string()),
                    },
                });
        }
        resign_package(&mut package, signing_key);
        package
    }

    fn required_field_schema_package(
        version: &str,
        migration_version: u64,
        field_name: &str,
        signing_key: &SigningKey,
    ) -> ApplicationPackage {
        required_fields_schema_package(version, migration_version, &[field_name], signing_key)
    }

    #[test]
    fn schema_validation_decrypts_existing_encrypted_fields() {
        let directory = tempfile::tempdir().unwrap();
        let signing_key = SigningKey::from_bytes(&[53; 32]);
        let mut package = schema_package("19.7.4", 1, true, true, false, &signing_key);
        package
            .manifest
            .capabilities
            .insert(bicdb_extension::ExtensionCapability::SecretsCrypto);
        let application = package.manifest.application.as_deref_mut().unwrap();
        application
            .required_features
            .extend([ApplicationFeature::Secrets, ApplicationFeature::Crypto]);
        application.resources[0]
            .encrypted_fields
            .insert("nickname".to_string(), "WIDGET_SECRET_KEY".to_string());
        application.resources[0].validation = serde_json::from_value(json!([{
            "field": "nickname",
            "kind": "max_length",
            "value": 16
        }]))
        .unwrap();
        application.secrets.push(SecretDeclaration {
            name: "WIDGET_SECRET_KEY".to_string(),
            operations: BTreeSet::from([CryptoOperation::Encrypt, CryptoOperation::Decrypt]),
            versions: BTreeSet::new(),
            allow_plaintext_read: false,
        });
        application.application_program = Some(
            serde_json::from_value(json!({
                "version": 1,
                "secret_bindings": {"Widget.nickname": "WIDGET_SECRET_KEY"}
            }))
            .unwrap(),
        );
        let contract = application.resources[0].clone();
        resign_package(&mut package, &signing_key);

        let secrets = InMemorySecretProvider::default();
        secrets
            .insert(
                "WIDGET_SECRET_KEY",
                "1",
                "widget-secret-key",
                "aes-256-gcm",
                vec![7; 32],
                true,
            )
            .unwrap();
        let services = InvocationServices::new(
            Arc::new(secrets),
            Arc::new(ProductionEgressProvider::new().unwrap()),
            Arc::new(
                LocalBlobProvider::open(directory.path().join("schema-blobs"), vec![8; 32])
                    .unwrap(),
            ),
            Arc::new(Mutex::<Vec<ObservabilityEvent>>::default()),
        );
        let mut db = BicDb::open(directory.path().join("database")).unwrap();
        db.create_collection("widgets").unwrap();
        let actor = ActorContext {
            service_id: Some("schema-encryption-test".to_string()),
            trace_id: "schema-encryption-test".to_string(),
            deadline_unix_ms: crate::host::now_ms().saturating_add(60_000),
            ..ActorContext::default()
        };
        let mut host = CapabilityHost::new_validated(
            &db,
            Arc::new(package.manifest.clone()),
            actor,
            services.clone(),
        )
        .unwrap();
        let stored = crate::resource::encrypt_resource_value(
            &mut host,
            &contract,
            &json!({
                "id": "11111111-1111-4111-8111-111111111111",
                "name": "Existing",
                "nickname": "short"
            }),
        )
        .unwrap();
        drop(host);
        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .insert("widgets", record_from_resource_json(stored, None).unwrap())
            .unwrap();
        transaction.commit().unwrap();

        let mut receipt = SchemaApplicationReceipt::default();
        apply_migration_transformations(&mut db, &package.manifest, services, &mut receipt)
            .unwrap();
    }

    fn security_application() -> ApplicationManifestV2 {
        let signing_key = SigningKey::from_bytes(&[44; 32]);
        let mut package = signed_package("19.0.0", &signing_key);
        let application = package.manifest.application.as_deref_mut().unwrap();
        application.auth_schemes.insert(
            "Auth".to_string(),
            ApplicationAuthSchemeV1 {
                kind: ApplicationAuthKindV1::JwtHs256,
                issuer: "https://security.example".to_string(),
                audience: "carrier-users".to_string(),
            },
        );
        application.secrets.push(SecretDeclaration {
            name: "AUTH_KEY".to_string(),
            operations: BTreeSet::from([
                CryptoOperation::Metadata,
                CryptoOperation::Sign,
                CryptoOperation::Verify,
            ]),
            versions: BTreeSet::new(),
            allow_plaintext_read: false,
        });
        application.application_program = Some(ApplicationProgramV1 {
            version: 1,
            max_steps: 100,
            max_call_depth: 16,
            blob: None,
            redis: None,
            email: None,
            grpc: None,
            tokenizer: None,
            embeddings: None,
            llm: None,
            rag: None,
            agents: None,
            evaluations: None,
            tests: None,
            observability: None,
            callables: BTreeMap::new(),
            service_bindings: BTreeMap::new(),
            client_bindings: BTreeMap::new(),
            flags: BTreeMap::new(),
            job_bindings: BTreeMap::new(),
            secret_bindings: BTreeMap::new(),
            security: Some(ApplicationSecurityContractV1 {
                version: 1,
                helpers: BTreeSet::from([
                    "auth.login".to_string(),
                    "auth.issue_tokens".to_string(),
                ]),
                auth_scheme: Some("Auth".to_string()),
                signing_secret: Some("AUTH_KEY".to_string()),
                signing_algorithm: Some("hmac-sha256".to_string()),
                access_ttl_seconds: 900,
                refresh_ttl_seconds: 86_400,
                magic_link_secrets: BTreeSet::new(),
                durable_replay: true,
                versioned_keys: true,
                emit_evidence: true,
            }),
            event_bindings: BTreeMap::new(),
            realtime_bindings: BTreeMap::new(),
            mutation_bindings: Vec::new(),
            workflow_bindings: BTreeMap::new(),
        });
        application.clone()
    }

    #[test]
    fn security_upgrade_preserves_active_helpers_keys_and_token_policy() {
        let previous = security_application();
        let mut tightened = previous.clone();
        let security = tightened
            .application_program
            .as_mut()
            .unwrap()
            .security
            .as_mut()
            .unwrap();
        security.access_ttl_seconds = 600;
        security.helpers.insert("auth.password_policy".to_string());
        validate_security_contract_evolution(&previous, &tightened).unwrap();

        let mut removed = previous.clone();
        removed
            .application_program
            .as_mut()
            .unwrap()
            .security
            .as_mut()
            .unwrap()
            .helpers
            .remove("auth.login");
        assert!(validate_security_contract_evolution(&previous, &removed)
            .unwrap_err()
            .to_string()
            .contains("removes an active BicDB application security helper"));

        let mut changed_key = previous.clone();
        changed_key
            .application_program
            .as_mut()
            .unwrap()
            .security
            .as_mut()
            .unwrap()
            .signing_secret = Some("NEXT_KEY".to_string());
        assert!(
            validate_security_contract_evolution(&previous, &changed_key)
                .unwrap_err()
                .to_string()
                .contains("token signing contract")
        );

        let mut weakened = previous.clone();
        weakened
            .application_program
            .as_mut()
            .unwrap()
            .security
            .as_mut()
            .unwrap()
            .refresh_ttl_seconds = 172_800;
        assert!(validate_security_contract_evolution(&previous, &weakened)
            .unwrap_err()
            .to_string()
            .contains("token lifetime policy"));
    }

    #[test]
    fn additive_compatibility_accepts_exact_schema_refinement_and_signed_nullable_tightening() {
        let signing_key = SigningKey::from_bytes(&[45; 32]);
        let previous_package = schema_package("20.0.0", 1, true, true, true, &signing_key);
        let previous = previous_package.manifest.application.as_deref().unwrap();

        let mut refined = previous.clone();
        refined.resources[0]
            .fields
            .iter_mut()
            .find(|field| field.name == "id")
            .unwrap()
            .value_type = Some(ApplicationRouteParameterTypeV1::Uuid);
        assert!(validate_additive_contracts(previous, &refined).unwrap());

        let mut incompatible_refinement = refined.clone();
        incompatible_refinement.resources[0]
            .fields
            .iter_mut()
            .find(|field| field.name == "id")
            .unwrap()
            .value_type = Some(ApplicationRouteParameterTypeV1::String);
        assert!(validate_additive_contracts(&refined, &incompatible_refinement).is_err());

        let mut required = previous.clone();
        let required_id = required.resources[0]
            .fields
            .iter_mut()
            .find(|field| field.name == "id")
            .unwrap();
        required_id.value_type = Some(ApplicationRouteParameterTypeV1::Uuid);
        required_id.nullable = false;
        let mut relaxed = required.clone();
        let relaxed_id = relaxed.resources[0]
            .fields
            .iter_mut()
            .find(|field| field.name == "id")
            .unwrap();
        relaxed_id.value_type = Some(ApplicationRouteParameterTypeV1::Optional {
            value: Box::new(ApplicationRouteParameterTypeV1::Uuid),
        });
        relaxed_id.nullable = true;
        assert!(validate_additive_contracts(&required, &relaxed).unwrap());

        let mut incompatible_relaxation = relaxed.clone();
        incompatible_relaxation.resources[0]
            .fields
            .iter_mut()
            .find(|field| field.name == "id")
            .unwrap()
            .value_type = Some(ApplicationRouteParameterTypeV1::Optional {
            value: Box::new(ApplicationRouteParameterTypeV1::String),
        });
        assert!(validate_additive_contracts(&required, &incompatible_relaxation).is_err());

        let mut tightened = previous.clone();
        let nickname = tightened.resources[0]
            .fields
            .iter_mut()
            .find(|field| field.name == "nickname")
            .unwrap();
        nickname.nullable = false;
        nickname.default_json = Some("\"unknown\"".to_string());
        tightened.migrations[0]
            .transformations
            .push(MigrationStep::BackfillField {
                resource: "Widget".to_string(),
                field: "nickname".to_string(),
                expression: ApplicationExpressionV1::Literal {
                    value: Value::String("unknown".to_string()),
                },
            });
        assert!(validate_additive_contracts(previous, &tightened).unwrap());

        tightened.migrations[0].transformations.clear();
        assert!(validate_additive_contracts(previous, &tightened)
            .unwrap_err()
            .to_string()
            .contains("required without a signed backfill transformation"));
    }

    #[test]
    fn additive_compatibility_tightens_audit_but_rejects_implicit_encryption_changes() {
        let signing_key = SigningKey::from_bytes(&[46; 32]);
        let previous_package = schema_package("20.0.0", 1, true, true, true, &signing_key);
        let previous = previous_package.manifest.application.as_deref().unwrap();

        let mut audited = previous.clone();
        audited.resources[0].audit.required = true;
        audited.resources[0]
            .audit
            .redact_fields
            .insert("nickname".to_string());
        assert!(!validate_additive_contracts(previous, &audited).unwrap());

        let mut weakened = audited.clone();
        weakened.resources[0].audit.redact_fields.clear();
        assert!(validate_additive_contracts(&audited, &weakened)
            .unwrap_err()
            .to_string()
            .contains("weakens its durable audit policy"));

        let mut immutable_tightening = previous.clone();
        immutable_tightening.resources[0]
            .immutable_fields
            .insert("nickname".to_string());
        assert!(!validate_additive_contracts(previous, &immutable_tightening).unwrap());

        let mut immutable_weakening = immutable_tightening.clone();
        immutable_weakening.resources[0]
            .immutable_fields
            .remove("nickname");
        assert!(
            validate_additive_contracts(&immutable_tightening, &immutable_weakening)
                .unwrap_err()
                .to_string()
                .contains("persisted mutation or collection policy")
        );

        let mut encrypted_without_transform = previous.clone();
        encrypted_without_transform.resources[0]
            .encrypted_fields
            .insert("nickname".to_string(), "WIDGET_KEY".to_string());
        assert!(
            validate_additive_contracts(previous, &encrypted_without_transform)
                .unwrap_err()
                .to_string()
                .contains("persisted mutation or collection policy")
        );

        let mut encrypted_new_field = previous.clone();
        let mut encrypted_field = encrypted_new_field.resources[0]
            .fields
            .iter()
            .find(|field| field.name == "nickname")
            .unwrap()
            .clone();
        encrypted_field.name = "private_note".to_string();
        encrypted_field.storage_name = Some("private_note".to_string());
        encrypted_new_field.resources[0]
            .fields
            .push(encrypted_field);
        encrypted_new_field.resources[0]
            .encrypted_fields
            .insert("private_note".to_string(), "WIDGET_KEY".to_string());
        assert!(validate_additive_contracts(previous, &encrypted_new_field).unwrap());
    }

    #[test]
    fn additive_compatibility_allows_immutable_policy_tightening_only() {
        let signing_key = SigningKey::from_bytes(&[47; 32]);
        let previous_package = schema_package("20.0.0", 1, true, true, true, &signing_key);
        let previous = previous_package.manifest.application.as_deref().unwrap();

        let mut tightened = previous.clone();
        tightened.resources[0]
            .immutable_fields
            .insert("name".to_string());
        assert!(!validate_additive_contracts(previous, &tightened).unwrap());

        let mut weakened = tightened.clone();
        weakened.resources[0].immutable_fields.remove("name");
        assert!(validate_additive_contracts(&tightened, &weakened)
            .unwrap_err()
            .to_string()
            .contains("persisted mutation or collection policy"));
    }

    #[test]
    fn additive_compatibility_accepts_only_non_nullable_tenant_partition_tightening() {
        let signing_key = SigningKey::from_bytes(&[48; 32]);
        let previous_package = schema_package("20.0.0", 1, true, true, true, &signing_key);
        let previous = previous_package.manifest.application.as_deref().unwrap();

        let mut tenant_partitioned = previous.clone();
        tenant_partitioned.resources[0].tenant_field = Some("name".to_string());
        assert!(!validate_additive_contracts(previous, &tenant_partitioned).unwrap());

        let mut nullable_partition = previous.clone();
        nullable_partition.resources[0].tenant_field = Some("nickname".to_string());
        assert!(validate_additive_contracts(previous, &nullable_partition)
            .unwrap_err()
            .to_string()
            .contains("persisted mutation or collection policy"));

        let mut changed_partition = tenant_partitioned.clone();
        changed_partition.resources[0].tenant_field = Some("id".to_string());
        assert!(
            validate_additive_contracts(&tenant_partitioned, &changed_partition)
                .unwrap_err()
                .to_string()
                .contains("persisted mutation or collection policy")
        );
    }

    #[test]
    fn additive_compatibility_allows_a_signed_unique_predicate_correction() {
        let signing_key = SigningKey::from_bytes(&[49; 32]);
        let previous_package = schema_package("20.0.0", 1, true, true, true, &signing_key);
        let mut previous = previous_package
            .manifest
            .application
            .as_deref()
            .unwrap()
            .clone();
        previous.resources[0].unique_targets.push(
            serde_json::from_value(json!({
                "target": "identity",
                "fields": ["name"],
                "index_name": "widgets_identity_uq"
            }))
            .unwrap(),
        );

        let mut corrected = previous.clone();
        corrected.resources[0].unique_targets[0].predicate = Some(
            serde_json::from_value(json!({
                "op": "binary",
                "operator": "equal",
                "left": {"op": "variable", "name": "nickname"},
                "right": {"op": "literal", "value": null},
                "value_type": "bool",
                "left_type": "string",
                "right_type": "string"
            }))
            .unwrap(),
        );
        assert!(validate_additive_contracts(&previous, &corrected).unwrap());

        let mut changed_fields = corrected.clone();
        changed_fields.resources[0].unique_targets[0].fields = vec!["id".to_string()];
        assert!(validate_additive_contracts(&previous, &changed_fields)
            .unwrap_err()
            .to_string()
            .contains("changes the identity or fields"));
    }

    #[test]
    fn schema_index_predicate_replacement_is_compensated() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("database")).unwrap();
        db.create_collection("widgets").unwrap();
        let previous = IndexDefinition {
            name: "widgets_identity_uq".to_string(),
            collection: "widgets".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["name".to_string()])],
            unique: true,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        };
        db.create_index(previous.clone()).unwrap();
        let mut candidate = previous.clone();
        candidate.predicate = Some(IndexPredicate::Literal {
            value: Value::Bool(true),
        });
        let mut receipt = SchemaApplicationReceipt::default();

        create_schema_index(&mut db, candidate.clone(), &mut receipt).unwrap();
        assert_eq!(db.index_definitions(), vec![candidate]);
        assert_eq!(receipt.replaced_indexes, vec![previous.clone()]);

        receipt.rollback(&mut db).unwrap();
        assert_eq!(db.index_definitions(), vec![previous]);
    }

    #[test]
    fn schema_non_unique_index_field_replacement_is_compensated() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path().join("database")).unwrap();
        db.create_collection("quotation_lines").unwrap();
        let previous = IndexDefinition {
            name: "quotation_lines_search".to_string(),
            collection: "quotation_lines".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["description".to_string()])],
            unique: false,
            kind: IndexKind::FullText,
            predicate: None,
            exclusion: None,
        };
        db.create_index(previous.clone()).unwrap();
        let mut candidate = previous.clone();
        candidate.fields.push(IndexField::MetadataPath(vec![
            "tax_category_code".to_string()
        ]));
        let mut receipt = SchemaApplicationReceipt::default();

        create_schema_index(&mut db, candidate.clone(), &mut receipt).unwrap();
        assert_eq!(db.index_definitions(), vec![candidate]);
        assert_eq!(receipt.replaced_indexes, vec![previous.clone()]);

        receipt.rollback(&mut db).unwrap();
        assert_eq!(db.index_definitions(), vec![previous]);
    }

    #[test]
    fn schema_unique_index_field_change_remains_incompatible() {
        let previous = IndexDefinition {
            name: "accounts_code_uq".to_string(),
            collection: "accounts".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["code".to_string()])],
            unique: true,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        };
        let mut candidate = previous.clone();
        candidate.fields = vec![IndexField::MetadataPath(vec!["name".to_string()])];

        assert!(!index_predicate_replacement_is_compatible(
            &previous, &candidate
        ));
    }

    #[test]
    fn additive_compatibility_preserves_idempotency_and_only_tightens_cache() {
        let signing_key = SigningKey::from_bytes(&[47; 32]);
        let previous_package = schema_package("20.0.0", 1, true, true, true, &signing_key);
        let mut previous = previous_package
            .manifest
            .application
            .as_deref()
            .unwrap()
            .clone();
        previous.resources[0].idempotency = Some(IdempotencyContract {
            header: "idempotency-key".to_string(),
            ttl_seconds: 3_600,
            max_key_bytes: 128,
        });
        previous.resources[0].cache = Some(bicdb_extension::abi_v2::ResourceCacheContract {
            max_age_seconds: 60,
            private: false,
            vary: BTreeSet::from(["accept-language".to_string()]),
        });

        let mut removed_idempotency = previous.clone();
        removed_idempotency.resources[0].idempotency = None;
        assert!(validate_additive_contracts(&previous, &removed_idempotency)
            .unwrap_err()
            .to_string()
            .contains("removes retry-safety"));

        let mut changed_idempotency = previous.clone();
        changed_idempotency.resources[0]
            .idempotency
            .as_mut()
            .unwrap()
            .ttl_seconds = 1_800;
        assert!(validate_additive_contracts(&previous, &changed_idempotency)
            .unwrap_err()
            .to_string()
            .contains("active idempotency key contract"));

        let mut tightened_cache = previous.clone();
        let cache = tightened_cache.resources[0].cache.as_mut().unwrap();
        cache.max_age_seconds = 30;
        cache.private = true;
        cache.vary.insert("authorization".to_string());
        assert!(!validate_additive_contracts(&previous, &tightened_cache).unwrap());

        let mut weakened_cache = previous.clone();
        weakened_cache.resources[0]
            .cache
            .as_mut()
            .unwrap()
            .max_age_seconds = 120;
        assert!(validate_additive_contracts(&previous, &weakened_cache)
            .unwrap_err()
            .to_string()
            .contains("weakens its active HTTP cache policy"));

        let mut disabled_cache = previous.clone();
        disabled_cache.resources[0].cache = None;
        assert!(!validate_additive_contracts(&previous, &disabled_cache).unwrap());
    }

    #[test]
    fn additive_schema_upgrade_is_versioned_reversible_and_compensates_failure() {
        let directory = tempfile::tempdir().unwrap();
        let db = Arc::new(RwLock::new(
            BicDb::open(directory.path().join("schema-database")).unwrap(),
        ));
        let signing_key = SigningKey::from_bytes(&[43; 32]);
        let mut trusted = TrustedSigningKeys::default();
        trusted
            .insert_ed25519("release", signing_key.verifying_key().as_bytes())
            .unwrap();
        let verifier = PackageVerifier::new(trusted, 8 * 1024 * 1024).unwrap();
        let services = InvocationServices::new(
            Arc::new(InMemorySecretProvider::default()),
            Arc::new(ProductionEgressProvider::new().unwrap()),
            Arc::new(
                LocalBlobProvider::open(directory.path().join("schema-blobs"), vec![8; 32])
                    .unwrap(),
            ),
            Arc::new(Mutex::<Vec<ObservabilityEvent>>::default()),
        );
        let runtime = ApplicationRuntime::new_shared(
            db.clone(),
            ApplicationHostConfig {
                package_root: directory.path().join("schema-applications"),
                wasm: WasmHostConfig::default(),
                max_package_bytes: 8 * 1024 * 1024,
                node_id: "schema-test-node".to_string(),
                max_history: 4,
                required_packages: BTreeSet::new(),
                idempotency_entries: 10_000,
                route_cache_entries: 10_000,
                runtime_cache_entries: 10_000,
                module_cache_entries: 16,
            },
            verifier,
            services,
        )
        .unwrap();

        runtime
            .install(schema_package(
                "19.6.0",
                1,
                true,
                false,
                false,
                &signing_key,
            ))
            .unwrap();
        runtime.activate("carrier-app").unwrap();
        let original_policy = db.read().mutation_policy("widgets").unwrap();
        assert!(db
            .read()
            .get("__bicdb_app_migrations", "carrier-app:1")
            .unwrap()
            .is_some());

        std::env::set_var(
            "BICDB_TEST_SCHEMA_FAILURE_POINT",
            "carrier-app@19.7.0:after_migration_ledger",
        );
        let failed = runtime.upgrade(schema_package("19.7.0", 2, true, true, true, &signing_key));
        std::env::remove_var("BICDB_TEST_SCHEMA_FAILURE_POINT");
        assert!(failed.is_err());
        assert_eq!(runtime.inspect("carrier-app").unwrap().version, "19.6.0");
        assert_eq!(
            db.read().mutation_policy("widgets").unwrap(),
            original_policy
        );
        assert!(db
            .read()
            .index_definitions()
            .iter()
            .all(|index| index.name != "widgets_nickname_idx"));
        assert!(db
            .read()
            .get("__bicdb_app_migrations", "carrier-app:2")
            .unwrap()
            .is_none());

        runtime.activate("carrier-app").unwrap();
        assert_eq!(runtime.inspect("carrier-app").unwrap().version, "19.7.0");
        assert!(db
            .read()
            .index_definitions()
            .iter()
            .any(|index| index.name == "widgets_nickname_idx"));
        assert!(db
            .read()
            .get("__bicdb_app_migrations", "carrier-app:2")
            .unwrap()
            .is_some());

        let mut unsafe_default =
            required_field_schema_package("19.7.1", 3, "required_alias", &signing_key);
        unsafe_default
            .manifest
            .application
            .as_deref_mut()
            .unwrap()
            .migrations[0]
            .transformations
            .clear();
        resign_package(&mut unsafe_default, &signing_key);
        let error = runtime.upgrade(unsafe_default).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("without a signed backfill transformation"),
            "{error}"
        );
        assert_eq!(runtime.inspect("carrier-app").unwrap().version, "19.7.0");

        {
            let mut database = db.write();
            database.clear_mutation_policy("widgets").unwrap();
            let mut transaction = database.begin_transaction().unwrap();
            transaction
                .insert(
                    "widgets",
                    Record::new("11111111-1111-4111-8111-111111111111")
                        .with_metadata(json!({"name": "existing"})),
                )
                .unwrap();
            transaction.commit().unwrap();
            database
                .set_mutation_policy("widgets", original_policy.clone().unwrap())
                .unwrap();
        }
        runtime
            .upgrade(required_field_schema_package(
                "19.7.2",
                3,
                "required_alias",
                &signing_key,
            ))
            .unwrap();
        assert_eq!(
            db.read()
                .get("widgets", "11111111-1111-4111-8111-111111111111")
                .unwrap()
                .unwrap()
                .metadata["required_alias"],
            json!("unknown")
        );

        std::env::set_var(
            "BICDB_TEST_SCHEMA_FAILURE_POINT",
            "carrier-app@19.7.3:after_migration_ledger",
        );
        let failed = runtime.upgrade(required_fields_schema_package(
            "19.7.3",
            4,
            &["required_alias", "compensated_alias"],
            &signing_key,
        ));
        std::env::remove_var("BICDB_TEST_SCHEMA_FAILURE_POINT");
        assert!(failed.is_err());
        assert_eq!(runtime.inspect("carrier-app").unwrap().version, "19.7.2");
        assert!(db
            .read()
            .get("widgets", "11111111-1111-4111-8111-111111111111")
            .unwrap()
            .unwrap()
            .metadata
            .get("compensated_alias")
            .is_none());

        let incompatible = schema_package("19.8.0", 3, false, true, true, &signing_key);
        assert!(runtime.upgrade(incompatible).is_err());
        assert_eq!(runtime.inspect("carrier-app").unwrap().version, "19.7.2");

        runtime.rollback("carrier-app").unwrap();
        assert_eq!(runtime.inspect("carrier-app").unwrap().version, "19.7.0");
        assert!(db
            .read()
            .index_definitions()
            .iter()
            .any(|index| index.name == "widgets_nickname_idx"));
        runtime.rollback("carrier-app").unwrap();
        assert_eq!(runtime.inspect("carrier-app").unwrap().version, "19.7.2");
    }

    #[test]
    fn signed_blob_http_is_bounded_evidenced_and_survives_restart() {
        let directory = tempfile::tempdir().unwrap();
        let db = Arc::new(RwLock::new(
            BicDb::open(directory.path().join("blob-http-database")).unwrap(),
        ));
        let signing_key = SigningKey::from_bytes(&[52; 32]);
        let mut trusted = TrustedSigningKeys::default();
        trusted
            .insert_ed25519("release", signing_key.verifying_key().as_bytes())
            .unwrap();
        let verifier = PackageVerifier::new(trusted, 8 * 1024 * 1024).unwrap();
        let provider = Arc::new(
            LocalBlobProvider::open(directory.path().join("blob-http-data"), vec![12; 32]).unwrap(),
        );
        let observations = Arc::new(Mutex::<Vec<ObservabilityEvent>>::default());
        let services = InvocationServices::new(
            Arc::new(InMemorySecretProvider::default()),
            Arc::new(ProductionEgressProvider::new().unwrap()),
            provider.clone(),
            observations.clone(),
        );
        let config = ApplicationHostConfig {
            package_root: directory.path().join("blob-http-applications"),
            wasm: WasmHostConfig::default(),
            max_package_bytes: 8 * 1024 * 1024,
            node_id: "blob-http-node".to_string(),
            max_history: 4,
            required_packages: BTreeSet::new(),
            idempotency_entries: 10_000,
            route_cache_entries: 10_000,
            runtime_cache_entries: 10_000,
            module_cache_entries: 16,
        };
        let runtime = ApplicationRuntime::new_shared(
            db.clone(),
            config.clone(),
            verifier.clone(),
            services.clone(),
        )
        .unwrap();
        runtime
            .install(blob_package("1.0.0", &signing_key))
            .unwrap();
        runtime.activate("carrier-app").unwrap();

        let namespace = application_blob_provider_namespace("carrier-app", "carrier");
        let signed = |url: &str| {
            Url::parse(&format!("http://bicdb.invalid{url}"))
                .unwrap()
                .query_pairs()
                .into_owned()
                .collect::<BTreeMap<_, _>>()
        };
        let put = signed(
            &provider
                .signed_url_named(&namespace, "reports/live.txt", 300, "PUT", None)
                .unwrap(),
        );
        let (status, headers, response) = runtime
            .execute_signed_blob_http(
                "PUT",
                &put["namespace"],
                &put["key"],
                put["expires"].parse().unwrap(),
                &put["method"],
                None,
                &put["signature"],
                Some("text/plain"),
                b"zero-hop blob",
            )
            .unwrap();
        assert_eq!(status, 201);
        assert!(headers.iter().any(|(name, _)| name == "etag"));
        let metadata: Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(metadata["key"], "reports/live.txt");
        assert_eq!(metadata["size_bytes"], 13);
        assert!(metadata.get("namespace").is_none());
        assert!(metadata.get("blob_id").is_none());

        let get_url = provider
            .signed_url_named(&namespace, "reports/live.txt", 300, "GET", Some("live.txt"))
            .unwrap();
        let get = signed(&get_url);
        let (status, headers, bytes) = runtime
            .execute_signed_blob_http(
                "GET",
                &get["namespace"],
                &get["key"],
                get["expires"].parse().unwrap(),
                &get["method"],
                Some(&get["download_name"]),
                &get["signature"],
                None,
                &[],
            )
            .unwrap();
        assert_eq!(status, 200);
        assert_eq!(bytes, b"zero-hop blob");
        assert!(headers.iter().any(|(name, value)| {
            name == "content-disposition" && value == "attachment; filename=\"live.txt\""
        }));
        assert!(runtime
            .execute_signed_blob_http(
                "PUT",
                &get["namespace"],
                &get["key"],
                get["expires"].parse().unwrap(),
                &get["method"],
                Some(&get["download_name"]),
                &get["signature"],
                Some("text/plain"),
                b"tampered",
            )
            .is_err());
        assert!(runtime
            .execute_signed_blob_http(
                "GET",
                &get["namespace"],
                &get["key"],
                crate::host::now_ms().saturating_div(1000) - 1,
                &get["method"],
                Some(&get["download_name"]),
                &get["signature"],
                None,
                &[],
            )
            .is_err());
        let mut forged_signature = get["signature"].clone();
        forged_signature.push('x');
        assert!(runtime
            .execute_signed_blob_http(
                "GET",
                &get["namespace"],
                &get["key"],
                get["expires"].parse().unwrap(),
                &get["method"],
                Some(&get["download_name"]),
                &forged_signature,
                None,
                &[],
            )
            .is_err());

        let mut incompatible = blob_package("1.1.0", &signing_key);
        incompatible
            .manifest
            .application
            .as_deref_mut()
            .unwrap()
            .application_program
            .as_mut()
            .unwrap()
            .blob = None;
        resign_package(&mut incompatible, &signing_key);
        let incompatible_error = runtime.upgrade(incompatible).unwrap_err();
        assert!(
            incompatible_error
                .to_string()
                .contains("removes its active BicDB application blob contract"),
            "{incompatible_error}"
        );
        drop(runtime);

        let runtime = ApplicationRuntime::new_shared(db, config, verifier, services).unwrap();
        let (_, _, bytes) = runtime
            .execute_signed_blob_http(
                "GET",
                &get["namespace"],
                &get["key"],
                get["expires"].parse().unwrap(),
                &get["method"],
                Some(&get["download_name"]),
                &get["signature"],
                None,
                &[],
            )
            .unwrap();
        assert_eq!(bytes, b"zero-hop blob");
        let events = observations.lock().unwrap();
        assert!(events.iter().any(|event| matches!(
            event,
            ObservabilityEvent::Evidence { control, .. }
                if control == "carrier.blob.signed_put"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            ObservabilityEvent::Evidence { control, .. }
                if control == "carrier.blob.signed_get"
        )));
    }

    #[test]
    fn signed_packages_activate_upgrade_rollback_and_restore_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let db = Arc::new(RwLock::new(
            BicDb::open(directory.path().join("database")).unwrap(),
        ));
        let signing_key = SigningKey::from_bytes(&[42; 32]);
        let mut trusted = TrustedSigningKeys::default();
        trusted
            .insert_ed25519("release", signing_key.verifying_key().as_bytes())
            .unwrap();
        let verifier = PackageVerifier::new(trusted, 8 * 1024 * 1024).unwrap();
        let services = InvocationServices::new(
            Arc::new(InMemorySecretProvider::default()),
            Arc::new(ProductionEgressProvider::new().unwrap()),
            Arc::new(LocalBlobProvider::open(directory.path().join("blobs"), vec![8; 32]).unwrap()),
            Arc::new(Mutex::<Vec<ObservabilityEvent>>::default()),
        );
        let config = ApplicationHostConfig {
            package_root: directory.path().join("applications"),
            wasm: WasmHostConfig::default(),
            max_package_bytes: 8 * 1024 * 1024,
            node_id: "test-node".to_string(),
            max_history: 4,
            required_packages: BTreeSet::new(),
            idempotency_entries: 10_000,
            route_cache_entries: 10_000,
            runtime_cache_entries: 10_000,
            module_cache_entries: 16,
        };
        let runtime = ApplicationRuntime::new_shared(
            db.clone(),
            config.clone(),
            verifier.clone(),
            services.clone(),
        )
        .unwrap();

        let first = signed_package("1.0.0", &signing_key);
        runtime.install(first).unwrap();
        drop(runtime);
        let runtime = ApplicationRuntime::new_shared(
            db.clone(),
            config.clone(),
            verifier.clone(),
            services.clone(),
        )
        .unwrap();
        assert!(runtime.activate("carrier-app").unwrap().ready);
        assert_eq!(runtime.inspect("carrier-app").unwrap().version, "1.0.0");

        let missing_bypass = SecurityContext::new("support-agent", "control-plane")
            .with_scopes(["carrier:internal_admin"]);
        let denied = runtime
            .invoke_resource_as_internal_admin(
                "carrier-app",
                "Widget",
                &missing_bypass,
                "tenant-a",
                Some("clinic-1"),
                carrier_resource_request(ResourceOperation::List),
            )
            .unwrap_err();
        assert!(denied
            .to_string()
            .contains("audited internal bypass reason"));

        let external_bypass = SecurityContext::new("support-agent", "control-plane")
            .with_scopes(["carrier:internal_admin"])
            .with_authenticated_session("session-a", AuthenticationStrength::Jwt)
            .with_bypass_reason("support case CASE-42");
        let denied = runtime
            .invoke_resource_as_internal_admin(
                "carrier-app",
                "Widget",
                &external_bypass,
                "tenant-a",
                Some("clinic-1"),
                carrier_resource_request(ResourceOperation::List),
            )
            .unwrap_err();
        assert!(denied.to_string().contains("internal host principal"));

        let authorized = SecurityContext::new("support-agent", "control-plane")
            .with_client_id("carrier-support-host")
            .with_roles(["platform_support"])
            .with_scopes(["carrier:internal_admin"])
            .with_authenticated_session("internal-session-a", AuthenticationStrength::Internal)
            .with_bypass_reason("support case CASE-42");
        let routed = runtime
            .invoke_resource_as_internal_admin(
                "carrier-app",
                "Widget",
                &authorized,
                "tenant-a",
                Some("clinic-1"),
                carrier_resource_request(ResourceOperation::List),
            )
            .unwrap_err();
        assert!(routed.to_string().contains("has no resource `Widget`"));

        let second = signed_package("1.1.0", &signing_key);
        assert!(runtime.upgrade(second).unwrap().ready);
        assert_eq!(runtime.inspect("carrier-app").unwrap().version, "1.1.0");

        let mut tampered = signed_package("2.0.0", &signing_key);
        tampered.modules.get_mut("carrier-app").unwrap().push(0xff);
        assert!(runtime.upgrade(tampered).is_err());
        assert_eq!(runtime.inspect("carrier-app").unwrap().version, "1.1.0");

        assert!(runtime.rollback("carrier-app").unwrap().ready);
        assert_eq!(runtime.inspect("carrier-app").unwrap().version, "1.0.0");
        drop(runtime);

        let restored = ApplicationRuntime::new_shared(db, config, verifier, services).unwrap();
        assert!(restored.readiness().ready);
        assert_eq!(restored.inspect("carrier-app").unwrap().version, "1.0.0");
        assert!(restored.rollback("carrier-app").unwrap().ready);
        assert_eq!(restored.inspect("carrier-app").unwrap().version, "1.1.0");

        restored
            .stage(signed_package("1.2.0", &signing_key))
            .unwrap();
        restored
            .stage(signed_named_package("companion", "1.0.0", &signing_key))
            .unwrap();
        let batch = restored
            .activate_batch(&["carrier-app".to_string(), "companion".to_string()])
            .unwrap();
        assert!(batch.values().all(|readiness| readiness.ready));
        assert_eq!(restored.inspect("carrier-app").unwrap().version, "1.2.0");
        assert_eq!(restored.inspect("companion").unwrap().version, "1.0.0");
        assert_eq!(
            restored.inspect("carrier-app").unwrap().generation,
            restored.inspect("companion").unwrap().generation
        );

        restored
            .stage(signed_package("1.3.0", &signing_key))
            .unwrap();
        assert!(restored
            .activate_batch(&["carrier-app".to_string(), "missing".to_string()])
            .is_err());
        assert_eq!(restored.inspect("carrier-app").unwrap().version, "1.2.0");
    }

    #[test]
    fn single_package_upgrade_cannot_break_an_active_exact_dependency_lock() {
        let directory = tempfile::tempdir().unwrap();
        let signing_key = SigningKey::from_bytes(&[53; 32]);
        let runtime = test_application_runtime(directory.path(), &signing_key, 8, 4);
        let provider_v1 = signed_named_package("provider", "1.0.0", &signing_key);
        let dependent_v1 =
            signed_package_with_exact_dependency("dependent", "1.0.0", &provider_v1, &signing_key);
        runtime.stage(provider_v1).unwrap();
        runtime.stage(dependent_v1).unwrap();
        runtime
            .activate_batch(&["provider".to_string(), "dependent".to_string()])
            .unwrap();

        let provider_v2 = signed_named_package("provider", "2.0.0", &signing_key);
        let error = runtime.upgrade(provider_v2.clone()).unwrap_err();
        assert!(
            error.to_string().contains("requires version `=1.0.0`"),
            "{error}"
        );
        assert_eq!(runtime.inspect("provider").unwrap().version, "1.0.0");
        assert_eq!(runtime.inspect("dependent").unwrap().version, "1.0.0");

        let dependent_v2 =
            signed_package_with_exact_dependency("dependent", "2.0.0", &provider_v2, &signing_key);
        runtime.stage(provider_v2).unwrap();
        runtime.stage(dependent_v2).unwrap();
        let activated = runtime
            .activate_batch(&["provider".to_string(), "dependent".to_string()])
            .unwrap();
        assert!(activated.values().all(|readiness| readiness.ready));
        assert_eq!(runtime.inspect("provider").unwrap().version, "2.0.0");
        assert_eq!(runtime.inspect("dependent").unwrap().version, "2.0.0");
    }

    #[test]
    fn application_modules_activate_in_signed_order_and_remain_monotonic_in_one_cell() {
        let directory = tempfile::tempdir().unwrap();
        let signing_key = SigningKey::from_bytes(&[54; 32]);
        let runtime = test_application_runtime(directory.path(), &signing_key, 8, 4);
        let foundation = signed_application_package(
            "sample-foundation",
            "sample.foundation",
            "1.0.0",
            true,
            &[("hub.identity", "^1.0")],
            "sample.foundation.party-role",
            &signing_key,
        );
        let sales = signed_application_package(
            "sample-sales",
            "sample.sales",
            "1.0.0",
            false,
            &[("sample.foundation", "^1.0")],
            "sample.sales.sales-order",
            &signing_key,
        );
        runtime.stage(sales.clone()).unwrap();
        assert!(runtime
            .activate("sample-sales")
            .unwrap_err()
            .to_string()
            .contains("sample.foundation"));
        runtime.stage(foundation).unwrap();
        assert!(runtime
            .activate_batch(&["sample-sales".to_string(), "sample-foundation".to_string()])
            .unwrap_err()
            .to_string()
            .contains("must place application dependency"));
        let activated = runtime
            .activate_batch(&["sample-foundation".to_string(), "sample-sales".to_string()])
            .unwrap();
        assert!(activated.values().all(|readiness| readiness.ready));
        assert!(runtime
            .disable("sample-foundation")
            .unwrap_err()
            .to_string()
            .contains("system application module"));

        let incompatible = signed_application_package(
            "sample-revenue",
            "sample.revenue",
            "1.0.0",
            false,
            &[("sample.foundation", "^2.0")],
            "sample.revenue.invoice",
            &signing_key,
        );
        runtime.stage(incompatible).unwrap();
        assert!(runtime
            .activate("sample-revenue")
            .unwrap_err()
            .to_string()
            .contains("active version"));

        let duplicate = signed_application_package(
            "sample-rental",
            "sample.rental",
            "1.0.0",
            false,
            &[("sample.foundation", "^1.0")],
            "sample.sales.sales-order",
            &signing_key,
        );
        runtime.stage(duplicate).unwrap();
        assert!(runtime
            .activate("sample-rental")
            .unwrap_err()
            .to_string()
            .contains("stable key"));

        let foundation_upgrade = signed_application_package(
            "sample-foundation",
            "sample.foundation",
            "1.1.0",
            true,
            &[("hub.identity", "^1.0")],
            "sample.foundation.party-role",
            &signing_key,
        );
        assert!(runtime.upgrade(foundation_upgrade).unwrap().ready);
        assert_eq!(
            runtime.inspect("sample-foundation").unwrap().version,
            "1.1.0"
        );
        assert!(runtime.rollback("sample-foundation").unwrap().ready);
        assert_eq!(
            runtime.inspect("sample-foundation").unwrap().version,
            "1.0.0"
        );

        let downgrade = signed_application_package(
            "sample-foundation",
            "sample.foundation",
            "0.9.0",
            true,
            &[("hub.identity", "^1.0")],
            "sample.foundation.party-role",
            &signing_key,
        );
        let downgrade_error = runtime.upgrade(downgrade).unwrap_err();
        assert!(
            downgrade_error.to_string().contains("monotonic"),
            "{downgrade_error}"
        );
        assert_eq!(
            runtime.inspect("sample-foundation").unwrap().version,
            "1.0.0"
        );
    }

    #[test]
    fn compiled_module_cache_is_content_addressed_bounded_and_pooled() {
        let directory = tempfile::tempdir().unwrap();
        let signing_key = SigningKey::from_bytes(&[42; 32]);
        let runtime = test_application_runtime(directory.path(), &signing_key, 2, 7);

        let first = signed_package("1.0.0", &signing_key);
        runtime.validate(&first).unwrap();
        runtime.validate(&first).unwrap();
        runtime
            .validate(&signed_package("1.1.0", &signing_key))
            .unwrap();
        runtime
            .validate(&signed_package("1.2.0", &signing_key))
            .unwrap();

        let snapshot = runtime.performance_snapshot();
        assert_eq!(snapshot.compiled_module_cache_capacity, 2);
        assert_eq!(snapshot.compiled_module_cache_entries, 2);
        assert_eq!(snapshot.compiled_module_cache_hits, 1);
        assert_eq!(snapshot.compiled_module_cache_misses, 3);
        assert_eq!(snapshot.compiled_module_cache_evictions, 1);
        assert_eq!(snapshot.wasm_pool_capacity, 7);
        let modules = runtime.compile_modules(&first).unwrap();
        assert!(modules.values().all(|module| module.uses_pooled_engine()));
    }

    #[test]
    #[ignore = "release-mode zero-hop latency/resource regression gate"]
    fn application_zero_hop_latency_and_resource_regression_gate() {
        const WARMUP: usize = 250;
        const ITERATIONS: usize = 4_000;
        const MAX_P50_MICROS: u128 = 500;
        const MAX_P95_MICROS: u128 = 2_000;

        let directory = tempfile::tempdir().unwrap();
        let signing_key = SigningKey::from_bytes(&[42; 32]);
        let runtime = test_application_runtime(directory.path(), &signing_key, 4, 32);
        let package = carrier_zero_hop_package(&signing_key);
        runtime.validate(&package).unwrap();
        runtime.install(package).unwrap();
        runtime.activate("carrier-app").unwrap();

        let request = crate::ApplicationHttpRequest {
            method: HttpMethod::Get,
            path: "/health".to_string(),
            query: Vec::new(),
            headers: vec![
                ("x-request-id".to_string(), "perf-request".to_string()),
                (
                    "traceparent".to_string(),
                    "00-00000000000000000000000000000001-0000000000000001-00".to_string(),
                ),
            ],
            body: HttpRequestBodyV2::Empty,
            peer_address: None,
        };
        let policy = crate::HttpHostPolicy::default();
        for _ in 0..WARMUP {
            runtime
                .dispatch_carrier_test_http(&policy, request.clone(), None)
                .unwrap();
        }
        let mut elapsed = Vec::with_capacity(ITERATIONS);
        for _ in 0..ITERATIONS {
            let started = std::time::Instant::now();
            let response = runtime
                .dispatch_carrier_test_http(&policy, request.clone(), None)
                .unwrap();
            elapsed.push(started.elapsed().as_micros());
            assert_eq!(response.status, 200);
            assert_eq!(
                response.body,
                HttpResponseBodyV2::Json(json!({"status": "ok", "target": "bicdb"}))
            );
        }
        elapsed.sort_unstable();
        let p50 = elapsed[ITERATIONS / 2];
        let p95 = elapsed[(ITERATIONS * 95) / 100];
        let snapshot = runtime.performance_snapshot();
        #[cfg(target_os = "linux")]
        let (peak_rss_kib, open_fds, threads) = {
            let status = fs::read_to_string("/proc/self/status").unwrap();
            let peak_rss_kib = status
                .lines()
                .find_map(|line| line.strip_prefix("VmHWM:"))
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap();
            let open_fds = fs::read_dir("/proc/self/fd").unwrap().count();
            let threads = fs::read_dir("/proc/self/task").unwrap().count();
            (peak_rss_kib, open_fds, threads)
        };
        #[cfg(not(target_os = "linux"))]
        let (peak_rss_kib, open_fds, threads) = (0_u64, 0_usize, 0_usize);
        eprintln!(
            "carrier_zero_hop iterations={ITERATIONS} p50_us={p50} p95_us={p95} peak_rss_kib={peak_rss_kib} open_fds={open_fds} threads={threads} cache={snapshot:?}"
        );
        assert!(
            p50 <= MAX_P50_MICROS,
            "zero-hop p50 {p50}us exceeds {MAX_P50_MICROS}us"
        );
        assert!(
            p95 <= MAX_P95_MICROS,
            "zero-hop p95 {p95}us exceeds {MAX_P95_MICROS}us"
        );
        assert_eq!(snapshot.active_packages, 1);
        assert_eq!(snapshot.prepared_application_programs, 1);
        assert_eq!(snapshot.compiled_module_cache_entries, 1);
        assert_eq!(snapshot.compiled_module_cache_misses, 1);
        assert!(snapshot.compiled_module_cache_hits >= 1);
        assert_eq!(snapshot.wasm_pool_capacity, 32);
        #[cfg(target_os = "linux")]
        {
            assert!(
                peak_rss_kib <= 512 * 1024,
                "zero-hop process peak RSS {peak_rss_kib} KiB exceeds 512 MiB"
            );
            assert!(
                open_fds <= 64,
                "zero-hop process leaked {open_fds} file descriptors"
            );
            assert!(threads <= 8, "zero-hop process used {threads} threads");
        }
    }
    #[test]
    fn host_and_verifier_limits_bound_both_file_reads_and_direct_staging() {
        use std::io::Write;
        for (host_limit, verifier_limit) in [(1024, 8 * 1024 * 1024), (8 * 1024 * 1024, 1024)] {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path();
            let signing_key = SigningKey::from_bytes(&[31; 32]);
            let db = Arc::new(RwLock::new(BicDb::open(root.join("database")).unwrap()));
            let mut trusted = TrustedSigningKeys::default();
            trusted
                .insert_ed25519("release", signing_key.verifying_key().as_bytes())
                .unwrap();
            let verifier = PackageVerifier::new(trusted, verifier_limit).unwrap();
            let services = InvocationServices::new(
                Arc::new(InMemorySecretProvider::default()),
                Arc::new(ProductionEgressProvider::new().unwrap()),
                Arc::new(LocalBlobProvider::open(root.join("blobs"), vec![8; 32]).unwrap()),
                Arc::new(Mutex::<Vec<ObservabilityEvent>>::default()),
            );
            let mut config = ApplicationHostConfig::new(root.join("applications"), "test");
            config.max_package_bytes = host_limit;
            let runtime = ApplicationRuntime::new_shared(db, config, verifier, services).unwrap();
            let package = signed_package("1.0.0", &signing_key);
            let error = runtime.stage(package).unwrap_err();
            assert!(error.to_string().contains("byte limit"), "{error}");
            let path = root.join("oversized.json");
            let mut file = std::fs::File::create(&path).unwrap();
            file.write_all(b"invalid JSON").unwrap();
            file.set_len(crate::encoded_package_byte_limit(1024).unwrap() + 1)
                .unwrap();
            let error = runtime.read_package_file(path).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("encoded package exceeds byte limit"),
                "{error}"
            );
        }
    }
}
