//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn start_schedule(
    db: Arc<RwLock<BicDb>>,
    module: Arc<WasmExtension>,
    manifest: Arc<bicdb_extension::ExtensionManifest>,
    services: InvocationServices,
    schedule: ScheduleDefinition,
    plan: SchedulePlan,
    node_id: String,
) -> Result<SupervisorControl> {
    let stop = Arc::new(AtomicBool::new(false));
    let active = Arc::new(AtomicBool::new(false));
    let healthy = Arc::new(AtomicBool::new(false));
    let thread_stop = stop.clone();
    let thread_active = active.clone();
    let thread_healthy = healthy.clone();
    let instance_id = uuid::Uuid::new_v4().to_string();
    let contract_sha256 = schedule_contract_sha256(&manifest, &schedule)?;
    let task = std::thread::Builder::new()
        .name(format!("bicdb-schedule-{}", schedule.name))
        .spawn(move || {
            thread_healthy.store(true, Ordering::Release);
            let mut executions = Vec::<JoinHandle<()>>::new();
            while !thread_stop.load(Ordering::Acquire) {
                let mut retained = Vec::with_capacity(executions.len());
                for execution in executions.drain(..) {
                    if execution.is_finished() {
                        if execution.join().is_err() {
                            thread_healthy.store(false, Ordering::Release);
                            eprintln!("bicdb: schedule `{}` execution panicked", schedule.name);
                        }
                    } else {
                        retained.push(execution);
                    }
                }
                executions = retained;
                if !thread_active.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                let now_ms = crate::host::now_ms();
                let lease_ms = i64::try_from(manifest.limits.timeout_ms)
                    .unwrap_or(i64::MAX)
                    .saturating_add(5_000)
                    .max(1_000);
                let claim = claim_due_schedule(
                    &db,
                    &manifest,
                    &schedule,
                    &plan,
                    &node_id,
                    &instance_id,
                    &contract_sha256,
                    now_ms,
                    lease_ms,
                );
                let claim = match claim {
                    Ok(claim) => {
                        thread_healthy.store(true, Ordering::Release);
                        claim
                    }
                    Err(error) => {
                        thread_healthy.store(false, Ordering::Release);
                        eprintln!("bicdb: schedule `{}` state failed: {error}", schedule.name);
                        std::thread::sleep(Duration::from_millis(50));
                        thread_healthy.store(true, Ordering::Release);
                        continue;
                    }
                };
                let Some(claim) = claim else {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                };
                let execution_db = db.clone();
                let execution_module = module.clone();
                let execution_manifest = manifest.clone();
                let execution_services = services.clone();
                let execution_schedule = schedule.clone();
                let execution_health = thread_healthy.clone();
                let execution_claim = claim.clone();
                let spawn = std::thread::Builder::new()
                    .name(format!(
                        "bicdb-schedule-run-{}-{}",
                        schedule.name, claim.scheduled_for_ms
                    ))
                    .spawn(move || {
                        let result = execute_schedule_occurrence(
                            &execution_db,
                            &execution_module,
                            execution_manifest.clone(),
                            execution_services,
                            &execution_schedule,
                            &execution_claim,
                        );
                        let error = result.as_ref().err().map(ToString::to_string);
                        if let Err(finish_error) = finish_schedule_claim(
                            &execution_db,
                            &execution_manifest,
                            &execution_schedule,
                            &execution_claim,
                            error.as_deref(),
                        ) {
                            execution_health.store(false, Ordering::Release);
                            eprintln!(
                                "bicdb: schedule `{}` could not persist completion: {finish_error}",
                                execution_schedule.name
                            );
                        }
                        if let Some(error) = error {
                            eprintln!(
                                "bicdb: schedule `{}` failed: {error}",
                                execution_schedule.name
                            );
                        }
                    });
                match spawn {
                    Ok(execution) => executions.push(execution),
                    Err(error) => {
                        let message = format!("failed to spawn schedule execution: {error}");
                        let _ = finish_schedule_claim(
                            &db,
                            &manifest,
                            &schedule,
                            &claim,
                            Some(&message),
                        );
                        thread_healthy.store(false, Ordering::Release);
                        eprintln!("bicdb: schedule `{}` failed: {message}", schedule.name);
                    }
                }
            }
            for execution in executions {
                if execution.join().is_err() {
                    thread_healthy.store(false, Ordering::Release);
                    eprintln!("bicdb: schedule `{}` execution panicked", schedule.name);
                }
            }
            thread_healthy.store(false, Ordering::Release);
        })
        .map_err(|error| AppRuntimeError::Provider(error.to_string()))?;
    healthy.store(true, Ordering::Release);
    Ok(SupervisorControl {
        stop,
        active,
        healthy,
        task: Some(task),
    })
}

pub(crate) fn execute_schedule_occurrence(
    db: &Arc<RwLock<BicDb>>,
    module: &Arc<WasmExtension>,
    manifest: Arc<bicdb_extension::ExtensionManifest>,
    services: InvocationServices,
    schedule: &ScheduleDefinition,
    claim: &ScheduleClaim,
) -> Result<()> {
    let now_ms = crate::host::now_ms();
    let run_id = format!(
        "{}:{}:{}:{}",
        manifest.identity.name, schedule.name, claim.scheduled_for_ms, claim.attempt
    );
    let trace_id = uuid::Uuid::new_v4().to_string();
    let sampling = manifest
        .application
        .as_deref()
        .and_then(|application| application.application_program.as_ref())
        .and_then(|program| program.observability.as_ref())
        .map(|observability| observability.sampling)
        .unwrap_or(ApplicationSamplingV1::ParentBasedAlwaysOn);
    let sampled = carrier_sampling_decision(sampling, &trace_id, None);
    let actor = ActorContext {
        user_id: None,
        service_id: Some(format!(
            "{}:schedule:{}",
            manifest.identity.name, schedule.name
        )),
        client_id: None,
        acting_client_id: None,
        authentication_method: Some("bicdb-scheduler".to_string()),
        roles: BTreeSet::from(["system:scheduled-work".to_string()]),
        scopes: BTreeSet::new(),
        tenant_id: None,
        workspace_id: None,
        organization_id: None,
        session_id: None,
        delegation_chain: Vec::new(),
        assurance_level: Some("host".to_string()),
        request_origin: Some("bicdb-scheduler".to_string()),
        trace_id,
        correlation_id: Some(run_id.clone()),
        causation_id: Some(run_id.clone()),
        deadline_unix_ms: now_ms.saturating_add(manifest.limits.timeout_ms as i64),
        policy_attributes: BTreeMap::from([
            ("carrier.trace.sampled".to_string(), sampled.to_string()),
            (
                "w3c.trace_flags".to_string(),
                if sampled { "01" } else { "00" }.to_string(),
            ),
        ]),
    };
    let invocation = ExtensionInvocation {
        id: run_id.clone(),
        kind: InvocationKind::QueueEvent,
        target: schedule.export.clone(),
        payload: json!({
            "schedule": schedule.name,
            "run_id": run_id,
            "scheduled_at_ms": claim.scheduled_for_ms,
            "started_at_ms": now_ms,
            "attempt": claim.attempt,
            "misfired": claim.scheduled_for_ms < now_ms.saturating_sub(SCHEDULE_MISFIRE_GRACE_MS),
            "payload": schedule.payload,
        }),
        context: actor_invocation_context(&actor),
    };
    let carrier_callable = manifest
        .application
        .as_deref()
        .and_then(|application| application.application_program.as_ref())
        .is_some_and(|program| program.callables.contains_key(&schedule.export));
    let database = db.read();
    if carrier_callable {
        execute_carrier_callable_with_host(
            &database,
            manifest,
            actor,
            services,
            &schedule.export,
            BTreeMap::from([
                ("input".to_string(), schedule.payload.clone()),
                ("schedule".to_string(), invocation.payload.clone()),
            ]),
            vec![(None, schedule.payload.clone())],
        )
        .map(|_| ())
    } else {
        CapabilityHost::new(&database, manifest, actor, services).and_then(|host| {
            module
                .invoke_with_host(&invocation, Box::new(host))
                .map_err(Into::into)
                .map(|_| ())
        })
    }
}

pub(crate) fn schedule_contract_sha256(
    manifest: &bicdb_extension::ExtensionManifest,
    schedule: &ScheduleDefinition,
) -> Result<String> {
    let package_sha256 = manifest
        .application
        .as_deref()
        .map(|application| application.package.package_sha256.as_str())
        .unwrap_or_default();
    Ok(Sha256::digest(serde_json::to_vec(&(
        &manifest.identity.name,
        &manifest.identity.version,
        package_sha256,
        schedule,
    ))?)
    .iter()
    .map(|byte| format!("{byte:02x}"))
    .collect())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn claim_due_schedule(
    db: &Arc<RwLock<BicDb>>,
    manifest: &bicdb_extension::ExtensionManifest,
    schedule: &ScheduleDefinition,
    plan: &SchedulePlan,
    node_id: &str,
    instance_id: &str,
    contract_sha256: &str,
    now_ms: i64,
    lease_ms: i64,
) -> Result<Option<ScheduleClaim>> {
    mutate_durable_schedule_state(db, manifest, schedule, contract_sha256, now_ms, |state| {
        apply_schedule_tick(
            state,
            schedule,
            plan,
            node_id,
            instance_id,
            contract_sha256,
            now_ms,
            lease_ms,
        )
    })
}

pub(crate) fn apply_schedule_tick(
    state: &mut DurableScheduleState,
    schedule: &ScheduleDefinition,
    plan: &SchedulePlan,
    node_id: &str,
    instance_id: &str,
    contract_sha256: &str,
    now_ms: i64,
    lease_ms: i64,
) -> Result<Option<ScheduleClaim>> {
    if state.contract_sha256 != contract_sha256 {
        if schedule.upgrade == ScheduleUpgradePolicy::Reset {
            state.cursor_unix_ms = now_ms;
            state.pending.clear();
            state.running.clear();
        }
        state.contract_sha256 = contract_sha256.to_string();
    }

    let mut retained = Vec::with_capacity(state.running.len());
    let mut recovered = Vec::new();
    for run in state.running.drain(..) {
        let replaced_local_instance = run.node_id == node_id && run.instance_id != instance_id;
        if run.lease_until_ms <= now_ms || replaced_local_instance {
            recovered.push(DurableScheduleOccurrence {
                scheduled_for_ms: run.scheduled_for_ms,
                attempt: run.attempt.saturating_add(1),
            });
        } else {
            retained.push(run);
        }
    }
    state.running = retained;
    for occurrence in recovered.into_iter().rev() {
        if !schedule_occurrence_exists(state, occurrence.scheduled_for_ms) {
            state.pending.push_front(occurrence);
        }
    }

    let state_limit = usize::from(schedule.catch_up_limit);
    let queue_capacity = state_limit.saturating_sub(state.pending.len() + state.running.len());
    let due_limit = match schedule.overlap {
        ScheduleOverlapPolicy::Queue => queue_capacity,
        ScheduleOverlapPolicy::Skip => state_limit,
    };
    if due_limit > 0 {
        let (due, cursor) =
            plan.due_occurrences(state.cursor_unix_ms, now_ms, schedule.misfire, due_limit)?;
        if let Some(cursor) = cursor {
            state.cursor_unix_ms = cursor;
        }
        let open_slots = usize::from(schedule.max_concurrency)
            .saturating_sub(state.running.len() + state.pending.len());
        let accepted = match schedule.overlap {
            ScheduleOverlapPolicy::Queue => due.len(),
            ScheduleOverlapPolicy::Skip => due.len().min(open_slots),
        };
        for scheduled_for_ms in due.into_iter().take(accepted) {
            if !schedule_occurrence_exists(state, scheduled_for_ms) {
                state.pending.push_back(DurableScheduleOccurrence {
                    scheduled_for_ms,
                    attempt: 1,
                });
            }
        }
    }

    if state.running.len() >= usize::from(schedule.max_concurrency) {
        return Ok(None);
    }
    let Some(pending) = state.pending.pop_front() else {
        return Ok(None);
    };
    let running = DurableRunningScheduleOccurrence {
        scheduled_for_ms: pending.scheduled_for_ms,
        attempt: pending.attempt,
        node_id: node_id.to_string(),
        instance_id: instance_id.to_string(),
        lease_until_ms: now_ms.saturating_add(lease_ms),
        started_at_ms: now_ms,
    };
    let claim = ScheduleClaim {
        scheduled_for_ms: running.scheduled_for_ms,
        attempt: running.attempt,
        node_id: running.node_id.clone(),
        instance_id: running.instance_id.clone(),
    };
    state.running.push(running);
    Ok(Some(claim))
}

pub(crate) fn schedule_occurrence_exists(
    state: &DurableScheduleState,
    scheduled_for_ms: i64,
) -> bool {
    state
        .pending
        .iter()
        .any(|run| run.scheduled_for_ms == scheduled_for_ms)
        || state
            .running
            .iter()
            .any(|run| run.scheduled_for_ms == scheduled_for_ms)
}

pub(crate) fn finish_schedule_claim(
    db: &Arc<RwLock<BicDb>>,
    manifest: &bicdb_extension::ExtensionManifest,
    schedule: &ScheduleDefinition,
    claim: &ScheduleClaim,
    error: Option<&str>,
) -> Result<()> {
    let now_ms = crate::host::now_ms();
    let contract_sha256 = schedule_contract_sha256(manifest, schedule)?;
    mutate_durable_schedule_state(db, manifest, schedule, &contract_sha256, now_ms, |state| {
        let before = state.running.len();
        state.running.retain(|run| {
            run.scheduled_for_ms != claim.scheduled_for_ms
                || run.attempt != claim.attempt
                || run.node_id != claim.node_id
                || run.instance_id != claim.instance_id
        });
        if state.running.len() == before {
            return Ok(());
        }
        state.last_completed_at_ms = Some(now_ms);
        state.last_error = error.map(str::to_string);
        if error.is_some() {
            state.failed_runs = state.failed_runs.saturating_add(1);
        } else {
            state.completed_runs = state.completed_runs.saturating_add(1);
        }
        Ok(())
    })
}

pub(crate) fn mutate_durable_schedule_state<T>(
    db: &Arc<RwLock<BicDb>>,
    manifest: &bicdb_extension::ExtensionManifest,
    schedule: &ScheduleDefinition,
    contract_sha256: &str,
    now_ms: i64,
    mut mutate: impl FnMut(&mut DurableScheduleState) -> Result<T>,
) -> Result<T> {
    let record_id = format!("{}:{}", manifest.identity.name, schedule.name);
    for attempt in 0..MAX_SCHEDULE_STATE_RETRIES {
        let database = db.read();
        let mut transaction = database
            .begin_application_transaction(schedule_mutation_actor(manifest, schedule, now_ms))?;
        let existing = transaction.get_authorized(SCHEDULE_STATE_RELATION, &record_id)?;
        let created = existing.is_none();
        let mut state = match existing {
            Some(record) => serde_json::from_value(record.metadata.clone())?,
            None => DurableScheduleState {
                application: manifest.identity.name.clone(),
                schedule: schedule.name.clone(),
                contract_sha256: contract_sha256.to_string(),
                revision: 0,
                cursor_unix_ms: now_ms,
                pending: VecDeque::new(),
                running: Vec::new(),
                completed_runs: 0,
                failed_runs: 0,
                last_completed_at_ms: None,
                last_error: None,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
            },
        };
        if state.application != manifest.identity.name || state.schedule != schedule.name {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "durable schedule state `{record_id}` crossed its application boundary"
            )));
        }
        let before = state.clone();
        let result = mutate(&mut state)?;
        if !created && state == before {
            return Ok(result);
        }
        let expected_revision = (!created).then_some(before.revision);
        state.revision = before.revision.saturating_add(1);
        state.updated_at_ms = now_ms;
        let record = Record::new(&record_id).with_metadata(serde_json::to_value(&state)?);
        let operation = if created {
            MutationOperation::Insert
        } else {
            MutationOperation::Update
        };
        let grant = transaction.issue_mutation_grant(MutationGrantSpec {
            relation: SCHEDULE_STATE_RELATION.to_string(),
            operation,
            record_id: Some(record_id.clone()),
            record_id_prefix: None,
            expected_version: expected_revision,
            version_field: Some("revision".to_string()),
            allowed_columns: schedule_record_columns(&record),
            bulk: false,
            maximum_affected_rows: 1,
            cascade_relations: BTreeSet::new(),
            statement_budget: 1,
            tenant_field: None,
            workspace_field: None,
            audit_metadata: BTreeMap::from([(
                "kind".to_string(),
                "durable-schedule-state".to_string(),
            )]),
        })?;
        match operation {
            MutationOperation::Insert => {
                transaction.insert_with_grant(grant, SCHEDULE_STATE_RELATION, record)?
            }
            MutationOperation::Update => {
                transaction.update_with_grant(grant, SCHEDULE_STATE_RELATION, record)?
            }
            _ => unreachable!("durable schedules use insert/update"),
        }
        match transaction.commit() {
            Ok(()) => return Ok(result),
            Err(bicdb_core::BicDbError::TransactionConflict(_))
                if attempt + 1 < MAX_SCHEDULE_STATE_RETRIES => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(AppRuntimeError::Conflict(format!(
        "durable schedule `{}` state remained contended",
        schedule.name
    )))
}

pub(crate) fn schedule_mutation_actor(
    manifest: &bicdb_extension::ExtensionManifest,
    schedule: &ScheduleDefinition,
    now_ms: i64,
) -> MutationActor {
    MutationActor {
        actor_id: format!("{}:schedule:{}", manifest.identity.name, schedule.name),
        roles: BTreeSet::from(["system:scheduler".to_string()]),
        scopes: BTreeSet::new(),
        tenant_id: None,
        workspace_id: None,
        originating_plugin: manifest.identity.name.clone(),
        originating_resource: Some(schedule.name.clone()),
        originating_action: Some("schedule_state".to_string()),
        trace_id: uuid::Uuid::new_v4().to_string(),
        deadline_unix_ms: now_ms.saturating_add(30_000),
    }
}

pub(crate) fn schedule_record_columns(record: &Record) -> BTreeSet<String> {
    record
        .metadata
        .as_object()
        .into_iter()
        .flatten()
        .map(|(field, _)| field.clone())
        .chain(std::iter::once("id".to_string()))
        .collect()
}

pub(crate) fn worker_actor(application: &str, worker: &str, headers: &Value) -> ActorContext {
    let header = |name: &str| {
        headers
            .get("bicdb_application")
            .and_then(|headers| headers.get(name))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    let string_set = |name: &str| {
        header(name)
            .and_then(|value| serde_json::from_str::<BTreeSet<String>>(&value).ok())
            .unwrap_or_default()
    };
    let mut roles = string_set("actor_roles");
    roles.insert("system:queue-worker".to_string());
    let mut policy_attributes = BTreeMap::new();
    if let Some(value) = header("trace_sampled") {
        policy_attributes.insert("carrier.trace.sampled".to_string(), value);
    }
    if let Some(value) = header("trace_flags") {
        policy_attributes.insert("w3c.trace_flags".to_string(), value);
    }
    if let Some(value) = header("tracestate") {
        policy_attributes.insert("w3c.tracestate".to_string(), value);
    }
    if let Some(value) = header("parent_span_id") {
        policy_attributes.insert("w3c.parent_span_id".to_string(), value);
    }
    ActorContext {
        user_id: header("actor_id"),
        service_id: Some(format!("{application}:worker:{worker}")),
        client_id: None,
        acting_client_id: None,
        authentication_method: Some("bicdb-broker".to_string()),
        roles,
        scopes: string_set("actor_scopes"),
        tenant_id: header("tenant_id"),
        workspace_id: header("workspace_id"),
        organization_id: header("organization_id"),
        session_id: None,
        delegation_chain: Vec::new(),
        assurance_level: Some("host".to_string()),
        request_origin: Some("bicdb-broker".to_string()),
        trace_id: header("trace_id").unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        correlation_id: header("correlation_id"),
        causation_id: header("causation_id"),
        deadline_unix_ms: crate::host::now_ms().saturating_add(30_000),
        policy_attributes,
    }
}

pub(crate) fn actor_invocation_context(actor: &ActorContext) -> InvocationContext {
    InvocationContext {
        role: actor.roles.iter().next().cloned(),
        tenant: actor.tenant_id.clone(),
        deadline_unix_ms: Some(actor.deadline_unix_ms),
        trace_id: Some(actor.trace_id.clone()),
        metadata: BTreeMap::from([
            (
                "workspace_id".to_string(),
                actor.workspace_id.clone().unwrap_or_default(),
            ),
            (
                "correlation_id".to_string(),
                actor.correlation_id.clone().unwrap_or_default(),
            ),
            (
                "causation_id".to_string(),
                actor.causation_id.clone().unwrap_or_default(),
            ),
        ]),
    }
}

#[derive(Clone)]
pub(crate) enum SchedulePlan {
    Interval(Duration),
    Cron {
        schedule: cron::Schedule,
        timezone: Tz,
    },
}

impl SchedulePlan {
    pub(crate) fn next_after_ms(&self, cursor_unix_ms: i64) -> Result<i64> {
        match self {
            Self::Interval(interval) => cursor_unix_ms
                .checked_add(i64::try_from(interval.as_millis()).map_err(|_| {
                    AppRuntimeError::InvalidPackage("schedule interval is too large".to_string())
                })?)
                .ok_or_else(|| {
                    AppRuntimeError::InvalidPackage("schedule occurrence overflowed".to_string())
                }),
            Self::Cron { schedule, timezone } => {
                let cursor = Utc
                    .timestamp_millis_opt(cursor_unix_ms)
                    .single()
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidPackage(
                            "schedule cursor is outside the supported timestamp range".to_string(),
                        )
                    })?;
                Ok(schedule
                    .after(&cursor.with_timezone(timezone))
                    .next()
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidPackage(
                            "schedule has no future occurrence".to_string(),
                        )
                    })?
                    .with_timezone(&Utc)
                    .timestamp_millis())
            }
        }
    }

    pub(crate) fn due_occurrences(
        &self,
        cursor_unix_ms: i64,
        now_ms: i64,
        misfire: ScheduleMisfirePolicy,
        limit: usize,
    ) -> Result<(Vec<i64>, Option<i64>)> {
        if limit == 0 {
            return Ok((Vec::new(), None));
        }
        let first = self.next_after_ms(cursor_unix_ms)?;
        if first > now_ms {
            return Ok((Vec::new(), None));
        }
        let overdue = first < now_ms.saturating_sub(SCHEDULE_MISFIRE_GRACE_MS);
        if overdue && misfire != ScheduleMisfirePolicy::CatchUp {
            return Ok((
                if misfire == ScheduleMisfirePolicy::FireOnce {
                    vec![now_ms]
                } else {
                    Vec::new()
                },
                Some(now_ms),
            ));
        }
        let mut due = Vec::with_capacity(limit.min(128));
        let mut cursor = cursor_unix_ms;
        while due.len() < limit {
            let next = self.next_after_ms(cursor)?;
            if next > now_ms {
                break;
            }
            if next <= cursor {
                return Err(AppRuntimeError::InvalidPackage(
                    "schedule did not advance its durable cursor".to_string(),
                ));
            }
            due.push(next);
            cursor = next;
        }
        Ok((due, (cursor != cursor_unix_ms).then_some(cursor)))
    }
}

pub(crate) fn parse_schedule_plan(schedule: &str, timezone: &str) -> Result<SchedulePlan> {
    let timezone = timezone.parse::<Tz>().map_err(|_| {
        AppRuntimeError::InvalidPackage(format!(
            "schedule timezone `{timezone}` is not a valid IANA zone"
        ))
    })?;
    if let Some(value) = schedule.strip_prefix("@every ") {
        return parse_schedule_interval(value).map(SchedulePlan::Interval);
    }
    let fields = schedule.split_whitespace().count();
    let expression = match fields {
        5 => format!("0 {schedule}"),
        6 | 7 => schedule.to_string(),
        _ => {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "schedule `{schedule}` must be a five-field BicDB application cron expression or `@every <N>ms|s|m|h`"
            )))
        }
    };
    expression
        .parse::<cron::Schedule>()
        .map(|schedule| SchedulePlan::Cron { schedule, timezone })
        .map_err(|error| {
            AppRuntimeError::InvalidPackage(format!("schedule `{schedule}` is invalid: {error}"))
        })
}

pub(crate) fn parse_schedule_interval(value: &str) -> Result<Duration> {
    let (number, multiplier) = if let Some(value) = value.strip_suffix("ms") {
        (value, 1_u64)
    } else if let Some(value) = value.strip_suffix('s') {
        (value, 1_000)
    } else if let Some(value) = value.strip_suffix('m') {
        (value, 60_000)
    } else if let Some(value) = value.strip_suffix('h') {
        (value, 3_600_000)
    } else {
        return Err(AppRuntimeError::InvalidPackage(
            "schedule interval lacks a supported unit".to_string(),
        ));
    };
    let milliseconds = number
        .parse::<u64>()
        .ok()
        .and_then(|number| number.checked_mul(multiplier))
        .filter(|value| *value > 0)
        .ok_or_else(|| AppRuntimeError::InvalidPackage("invalid schedule interval".to_string()))?;
    Ok(Duration::from_millis(milliseconds))
}

pub(crate) fn application_contract(
    snapshot: &PackageSnapshot,
) -> &bicdb_extension::abi_v2::ApplicationManifestV2 {
    snapshot
        .package
        .manifest
        .application
        .as_deref()
        .expect("validated application package")
}

pub(crate) fn package_semver(snapshot: &PackageSnapshot) -> Result<semver::Version> {
    semver::Version::parse(&snapshot.package.manifest.identity.version).map_err(|error| {
        AppRuntimeError::InvalidPackage(format!(
            "application `{}` has invalid semantic version: {error}",
            snapshot.package.manifest.identity.name
        ))
    })
}

pub(crate) fn latest_migration(
    application: &bicdb_extension::abi_v2::ApplicationManifestV2,
) -> Option<&bicdb_extension::abi_v2::MigrationPlanV1> {
    application
        .migrations
        .iter()
        .max_by_key(|migration| migration.schema_version)
}

pub(crate) fn ensure_preserved<T: PartialEq>(
    resource: &str,
    label: &str,
    previous: &[T],
    candidate: &[T],
) -> Result<bool> {
    if previous.iter().any(|item| !candidate.contains(item)) {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "resource `{resource}` removes or changes an existing {label}"
        )));
    }
    Ok(previous.len() != candidate.len())
}

pub(crate) fn tenant_partition_is_tightening(
    previous: &bicdb_extension::abi_v2::ResourceContractV1,
    candidate: &bicdb_extension::abi_v2::ResourceContractV1,
) -> bool {
    match (&previous.tenant_field, &candidate.tenant_field) {
        (previous, candidate) if previous == candidate => true,
        (None, Some(candidate_field)) => previous
            .fields
            .iter()
            .any(|field| field.name == candidate_field.as_str() && !field.nullable),
        _ => false,
    }
}

pub(crate) fn validate_unique_target_evolution(
    resource: &str,
    previous: &[bicdb_extension::abi_v2::ResourceUniqueTargetV1],
    candidate: &[bicdb_extension::abi_v2::ResourceUniqueTargetV1],
) -> Result<bool> {
    let mut changed = previous.len() != candidate.len();
    for old in previous {
        let new = candidate
            .iter()
            .find(|target| target.target == old.target)
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "resource `{resource}` removes an existing unique target"
                ))
            })?;
        if new.fields != old.fields || new.index_name != old.index_name {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "resource `{resource}` changes the identity or fields of existing unique target `{}`",
                old.target
            )));
        }
        changed |= new.predicate != old.predicate;
    }
    Ok(changed)
}

pub(crate) fn validate_additive_contracts(
    previous: &bicdb_extension::abi_v2::ApplicationManifestV2,
    candidate: &bicdb_extension::abi_v2::ApplicationManifestV2,
) -> Result<bool> {
    let mut structural_change = previous.resources.len() != candidate.resources.len();
    for old in &previous.resources {
        let new = candidate
            .resources
            .iter()
            .find(|resource| resource.name == old.name)
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "schema evolution removes resource `{}`",
                    old.name
                ))
            })?;
        if new.relation != old.relation || new.primary_key != old.primary_key {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "resource `{}` changes its relation or primary key",
                old.name
            )));
        }
        if new.schema_version < old.schema_version {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "resource `{}` schema version regresses from {} to {}",
                old.name, old.schema_version, new.schema_version
            )));
        }
        structural_change |= new.schema_version != old.schema_version;
        let encrypted_fields_are_additive = old
            .encrypted_fields
            .iter()
            .all(|(field, key)| new.encrypted_fields.get(field) == Some(key))
            && new.encrypted_fields.keys().all(|field| {
                old.encrypted_fields.contains_key(field)
                    || old
                        .fields
                        .iter()
                        .all(|old_field| old_field.name != field.as_str())
            });
        if new.version_field != old.version_field
            || !tenant_partition_is_tightening(old, new)
            || new.workspace_field != old.workspace_field
            || !old.immutable_fields.is_subset(&new.immutable_fields)
            || !encrypted_fields_are_additive
            || new.required_roles != old.required_roles
        {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "resource `{}` changes a persisted mutation or collection policy",
                old.name
            )));
        }
        if (old.audit.required && !new.audit.required)
            || !old.audit.redact_fields.is_subset(&new.audit.redact_fields)
        {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "resource `{}` weakens its durable audit policy",
                old.name
            )));
        }
        match (&old.idempotency, &new.idempotency) {
            (Some(_), None) => {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "resource `{}` removes retry-safety idempotency authority",
                    old.name
                )));
            }
            (Some(previous), Some(candidate)) if previous != candidate => {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "resource `{}` changes its active idempotency key contract",
                    old.name
                )));
            }
            _ => {}
        }
        if let (Some(previous), Some(candidate)) = (&old.cache, &new.cache) {
            if (previous.private && !candidate.private)
                || candidate.max_age_seconds > previous.max_age_seconds
                || !previous.vary.is_subset(&candidate.vary)
            {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "resource `{}` weakens its active HTTP cache policy",
                    old.name
                )));
            }
        }
        // Privacy presentation and durable-audit tightening are signed runtime
        // policy changes, not physical schema evolution. They therefore do
        // not require a data migration boundary. Encryption bindings remain a
        // persisted policy above and cannot change without a future explicit
        // ciphertext transformation.
        structural_change |= new.fields.len() != old.fields.len();
        for old_field in &old.fields {
            let new_field = new
                .fields
                .iter()
                .find(|field| field.name == old_field.name)
                .ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(format!(
                        "resource `{}` removes field `{}`",
                        old.name, old_field.name
                    ))
                })?;
            let has_backfill = has_signed_field_backfill(candidate, &old.name, &old_field.name);
            if old_field.nullable && !new_field.nullable && !has_backfill {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "resource `{}` makes field `{}` required without a signed backfill transformation",
                    old.name, old_field.name
                )));
            }
            let adds_required_default = old_field.nullable
                && !new_field.nullable
                && old_field.default_json.is_none()
                && new_field.default_json.is_some()
                && has_backfill;
            let value_type_is_compatible =
                match (old_field.value_type.as_ref(), new_field.value_type.as_ref()) {
                    (None, _) => true,
                    (Some(previous), Some(candidate)) if previous == candidate => true,
                    (
                        Some(previous),
                        Some(bicdb_extension::abi_v2::ApplicationRouteParameterTypeV1::Optional {
                            value,
                        }),
                    ) if !old_field.nullable && new_field.nullable => previous == value.as_ref(),
                    (
                        Some(bicdb_extension::abi_v2::ApplicationRouteParameterTypeV1::Optional {
                            value,
                        }),
                        Some(candidate),
                    ) if old_field.nullable && !new_field.nullable && has_backfill => {
                        value.as_ref() == candidate
                    }
                    _ => false,
                };
            if new_field.storage_name != old_field.storage_name
                || new_field.field_type != old_field.field_type
                || !value_type_is_compatible
                || new_field.generated != old_field.generated
                || new_field.generated_expression != old_field.generated_expression
                || (new_field.default_json != old_field.default_json && !adds_required_default)
            {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "resource `{}` changes incompatible field `{}` semantics",
                    old.name, old_field.name
                )));
            }
            structural_change |= new_field.nullable != old_field.nullable;
            structural_change |= new_field.value_type != old_field.value_type;
        }
        for new_field in &new.fields {
            if old.fields.iter().any(|field| field.name == new_field.name) {
                continue;
            }
            let has_backfill = has_signed_field_backfill(candidate, &old.name, &new_field.name);
            if !new_field.nullable && !has_backfill {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "resource `{}` adds required field `{}` without a signed backfill transformation",
                    old.name, new_field.name
                )));
            }
        }
        structural_change |=
            validate_unique_target_evolution(&old.name, &old.unique_targets, &new.unique_targets)?;
        structural_change |= ensure_preserved(&old.name, "index", &old.indexes, &new.indexes)?;
        structural_change |= ensure_preserved(&old.name, "check", &old.checks, &new.checks)?;
        structural_change |= ensure_preserved(
            &old.name,
            "foreign key",
            &old.foreign_keys,
            &new.foreign_keys,
        )?;
        structural_change |= ensure_preserved(
            &old.name,
            "exclusion constraint",
            &old.exclusions,
            &new.exclusions,
        )?;
        if !old.search_fields.is_subset(&new.search_fields) {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "resource `{}` removes an existing search index field",
                old.name
            )));
        }
        structural_change |= old.search_fields != new.search_fields;
    }
    Ok(structural_change)
}

pub(crate) fn validate_security_contract_evolution(
    previous: &bicdb_extension::abi_v2::ApplicationManifestV2,
    candidate: &bicdb_extension::abi_v2::ApplicationManifestV2,
) -> Result<()> {
    let previous_program = previous.application_program.as_ref();
    let candidate_program = candidate.application_program.as_ref();
    let previous_security = previous_program.and_then(|program| program.security.as_ref());
    let candidate_security = candidate_program.and_then(|program| program.security.as_ref());
    let Some(old) = previous_security else {
        return Ok(());
    };
    let new = candidate_security.ok_or_else(|| {
        AppRuntimeError::InvalidPackage(
            "application upgrade removes its active BicDB application security contract"
                .to_string(),
        )
    })?;
    if !old.helpers.is_subset(&new.helpers) {
        return Err(AppRuntimeError::InvalidPackage(
            "application upgrade removes an active BicDB application security helper".to_string(),
        ));
    }
    if !old.magic_link_secrets.is_subset(&new.magic_link_secrets) {
        return Err(AppRuntimeError::InvalidPackage(
            "application upgrade removes an active BicDB application magic-link verification key"
                .to_string(),
        ));
    }
    if old.auth_scheme != new.auth_scheme
        || old.signing_secret != new.signing_secret
        || old.signing_algorithm != new.signing_algorithm
    {
        return Err(AppRuntimeError::InvalidPackage(
            "application upgrade changes its active BicDB application token signing contract"
                .to_string(),
        ));
    }
    if new.access_ttl_seconds > old.access_ttl_seconds
        || new.refresh_ttl_seconds > old.refresh_ttl_seconds
    {
        return Err(AppRuntimeError::InvalidPackage(
            "application upgrade weakens its active BicDB application token lifetime policy"
                .to_string(),
        ));
    }
    if let Some(scheme_name) = old.auth_scheme.as_ref() {
        if previous.auth_schemes.get(scheme_name) != candidate.auth_schemes.get(scheme_name) {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "application upgrade changes active authentication scheme `{scheme_name}`"
            )));
        }
    }
    let security_secrets = old
        .magic_link_secrets
        .iter()
        .chain(old.signing_secret.iter());
    for secret_name in security_secrets {
        let old_secret = previous
            .secrets
            .iter()
            .find(|secret| secret.name == *secret_name)
            .expect("validated security secret");
        let new_secret = candidate
            .secrets
            .iter()
            .find(|secret| secret.name == *secret_name)
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "application upgrade removes active security secret `{secret_name}`"
                ))
            })?;
        if !old_secret.operations.is_subset(&new_secret.operations)
            || (old_secret.allow_plaintext_read && !new_secret.allow_plaintext_read)
        {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "application upgrade removes authority from active security secret `{secret_name}`"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_blob_contract_evolution(
    previous: &bicdb_extension::abi_v2::ApplicationManifestV2,
    candidate: &bicdb_extension::abi_v2::ApplicationManifestV2,
) -> Result<()> {
    let old = previous
        .application_program
        .as_ref()
        .and_then(|program| program.blob.as_ref());
    let Some(old) = old else {
        return Ok(());
    };
    let new = candidate
        .application_program
        .as_ref()
        .and_then(|program| program.blob.as_ref())
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "application upgrade removes its active BicDB application blob contract"
                    .to_string(),
            )
        })?;
    if old.namespace != new.namespace
        || old.max_blob_bytes != new.max_blob_bytes
        || !old.helpers.is_subset(&new.helpers)
        || !old.signed_methods.is_subset(&new.signed_methods)
    {
        return Err(AppRuntimeError::InvalidPackage(
            "application upgrade changes active BicDB application blob identity, limits, helpers, or signed methods"
                .to_string(),
        ));
    }
    let old_declaration = previous
        .blobs
        .iter()
        .find(|blob| blob.namespace == old.namespace)
        .expect("validated BicDB application blob declaration");
    let new_declaration = candidate
        .blobs
        .iter()
        .find(|blob| blob.namespace == old.namespace)
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "application upgrade removes its active blob namespace".to_string(),
            )
        })?;
    if old_declaration.max_blob_bytes != new_declaration.max_blob_bytes
        || old_declaration.content_types != new_declaration.content_types
        || (old_declaration.allow_signed_urls && !new_declaration.allow_signed_urls)
        || (old_declaration.require_scan && !new_declaration.require_scan)
    {
        return Err(AppRuntimeError::InvalidPackage(
            "application upgrade weakens or changes its active blob namespace policy".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn has_signed_field_backfill(
    candidate: &bicdb_extension::abi_v2::ApplicationManifestV2,
    resource_name: &str,
    field_name: &str,
) -> bool {
    candidate.migrations.iter().any(|migration| {
        migration.transformations.iter().any(|step| {
            matches!(
                step,
                MigrationStep::BackfillField { resource, field, .. }
                    if resource == resource_name && field == field_name
            )
        })
    })
}

pub(crate) fn validate_migration_boundary(
    previous: &bicdb_extension::abi_v2::ApplicationManifestV2,
    candidate: &bicdb_extension::abi_v2::ApplicationManifestV2,
) -> Result<()> {
    let old_schema_version = latest_migration(previous)
        .map(|migration| migration.schema_version)
        .unwrap_or(0);
    let migration = latest_migration(candidate).ok_or_else(|| {
        AppRuntimeError::InvalidPackage(
            "structural schema evolution requires a signed migration plan".to_string(),
        )
    })?;
    if migration.schema_version <= old_schema_version {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "schema migration version {} does not advance beyond {}",
            migration.schema_version, old_schema_version
        )));
    }
    let contract_versions = candidate
        .resources
        .iter()
        .map(|resource| (resource.name.clone(), resource.schema_version))
        .collect::<BTreeMap<_, _>>();
    if migration.contract_versions != contract_versions {
        return Err(AppRuntimeError::InvalidPackage(
            "migration contract versions do not match the candidate resource contracts".to_string(),
        ));
    }
    if migration.irreversible || migration.rollback.is_empty() {
        return Err(AppRuntimeError::InvalidPackage(
            "additive schema evolution requires a signed rollback boundary".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_forward_schema_evolution(
    previous: &PackageSnapshot,
    candidate: &PackageSnapshot,
) -> Result<()> {
    // Re-activating the byte-identical package is not an evolution: a host
    // restart stages and activates the same build again, and that must be
    // idempotent.
    if candidate.package_hash == previous.package_hash {
        return Ok(());
    }
    let previous_version = package_semver(previous)?;
    let candidate_version = package_semver(candidate)?;
    if candidate_version <= previous_version {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "application version {candidate_version} must advance beyond active version {previous_version}"
        )));
    }
    validate_workflow_plan_evolution(previous, candidate)?;
    let previous = application_contract(previous);
    let candidate = application_contract(candidate);
    validate_security_contract_evolution(previous, candidate)?;
    validate_blob_contract_evolution(previous, candidate)?;
    if validate_additive_contracts(previous, candidate)? {
        validate_migration_boundary(previous, candidate)?;
    }
    Ok(())
}

pub(crate) fn validate_schema_rollback(
    active: &PackageSnapshot,
    target: &PackageSnapshot,
) -> Result<()> {
    let active_version = package_semver(active)?;
    let target_version = package_semver(target)?;
    if target_version > active_version {
        return validate_forward_schema_evolution(active, target);
    }
    validate_workflow_plan_evolution(active, target)?;
    let target_contract = application_contract(target);
    let active_contract = application_contract(active);
    validate_security_contract_evolution(active_contract, target_contract)?;
    validate_blob_contract_evolution(active_contract, target_contract)?;
    if validate_additive_contracts(target_contract, active_contract)? {
        let migration = latest_migration(active_contract).ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "active structural schema has no signed rollback plan".to_string(),
            )
        })?;
        if migration.irreversible || migration.rollback.is_empty() {
            return Err(AppRuntimeError::InvalidPackage(
                "active structural schema cannot be rolled back safely".to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_workflow_plan_evolution(
    previous: &PackageSnapshot,
    candidate: &PackageSnapshot,
) -> Result<()> {
    let previous = previous
        .package
        .manifest
        .application
        .as_deref()
        .and_then(|application| application.application_program.as_ref())
        .map(|program| &program.workflow_bindings);
    let candidate = candidate
        .package
        .manifest
        .application
        .as_deref()
        .and_then(|application| application.application_program.as_ref())
        .map(|program| &program.workflow_bindings);
    let Some(previous) = previous else {
        return Ok(());
    };
    let candidate = candidate.ok_or_else(|| {
        AppRuntimeError::InvalidPackage(
            "application upgrade removes all signed BicDB application workflow plans".to_string(),
        )
    })?;
    validate_workflow_binding_evolution(previous, candidate)
}

pub(crate) fn validate_workflow_binding_evolution(
    previous: &BTreeMap<String, ApplicationWorkflowDefinitionV1>,
    candidate: &BTreeMap<String, ApplicationWorkflowDefinitionV1>,
) -> Result<()> {
    for (name, old) in previous {
        let new = candidate.get(name).ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "application upgrade removes active BicDB application workflow plan `{name}`"
            ))
        })?;
        let old_hash = old
            .plan_sha256
            .clone()
            .map(Ok)
            .unwrap_or_else(|| carrier_workflow_plan_sha256(old))?;
        let new_hash = new
            .plan_sha256
            .clone()
            .map(Ok)
            .unwrap_or_else(|| carrier_workflow_plan_sha256(new))?;
        if new_hash != old_hash {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "application upgrade changes durable BicDB application workflow plan `{name}`; publish a new workflow name for incompatible topology"
            )));
        }
    }
    Ok(())
}

pub(crate) fn resource_index_field(
    contract: &ResourceContractV1,
    field: &str,
) -> Result<IndexField> {
    if !contract
        .fields
        .iter()
        .any(|candidate| candidate.name == field)
    {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "resource `{}` index references undeclared field `{field}`",
            contract.name
        )));
    }
    Ok(if field == contract.primary_key {
        IndexField::Id
    } else if field == "timestamp" {
        IndexField::Timestamp
    } else if field == "geometry" {
        IndexField::Geometry
    } else {
        IndexField::MetadataPath(vec![field.to_string()])
    })
}

pub(crate) fn validate_timeseries_intervals(contract: &ResourceContractV1) -> Result<()> {
    use bicdb_sql::typed_value::PgInterval;

    let Some(timeseries) = &contract.timeseries else {
        return Ok(());
    };
    for (label, value) in [
        ("chunk interval", timeseries.chunk_interval.as_deref()),
        ("retention", timeseries.retention.as_deref()),
    ] {
        let Some(value) = value else {
            continue;
        };
        let parsed = PgInterval::from_postgres_text(value).map_err(|_| {
            AppRuntimeError::InvalidPackage(format!(
                "resource `{}` has an invalid timeseries {label} `{value}`",
                contract.name
            ))
        })?;
        if parsed.months < 0
            || parsed.days < 0
            || parsed.micros < 0
            || (parsed.months == 0 && parsed.days == 0 && parsed.micros == 0)
        {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "resource `{}` timeseries {label} must be positive",
                contract.name
            )));
        }
    }
    Ok(())
}

pub(crate) fn resource_index_path(
    contract: &ResourceContractV1,
    path: &[String],
) -> Result<IndexField> {
    let Some(field) = path.first() else {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "resource `{}` has an empty index path",
            contract.name
        )));
    };
    if path.len() == 1 {
        return resource_index_field(contract, field);
    }
    if !contract
        .fields
        .iter()
        .any(|candidate| candidate.name == *field)
    {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "resource `{}` index path references undeclared field `{field}`",
            contract.name
        )));
    }
    Ok(IndexField::MetadataPath(path.to_vec()))
}

pub(crate) fn index_predicate_value_type(
    value_type: Option<ApplicationExpressionTypeV1>,
) -> IndexPredicateValueType {
    match value_type.unwrap_or(ApplicationExpressionTypeV1::Json) {
        ApplicationExpressionTypeV1::Bool => IndexPredicateValueType::Bool,
        ApplicationExpressionTypeV1::Int => IndexPredicateValueType::Int64,
        ApplicationExpressionTypeV1::Float => IndexPredicateValueType::Float64,
        ApplicationExpressionTypeV1::Decimal | ApplicationExpressionTypeV1::Money => {
            IndexPredicateValueType::Decimal
        }
        ApplicationExpressionTypeV1::String | ApplicationExpressionTypeV1::TimeZone => {
            IndexPredicateValueType::String
        }
        ApplicationExpressionTypeV1::Uuid => IndexPredicateValueType::Uuid,
        ApplicationExpressionTypeV1::Timestamp
        | ApplicationExpressionTypeV1::LocalDateTime
        | ApplicationExpressionTypeV1::ZonedDateTime => IndexPredicateValueType::Timestamp,
        ApplicationExpressionTypeV1::Date => IndexPredicateValueType::Date,
        ApplicationExpressionTypeV1::Json | ApplicationExpressionTypeV1::Other => {
            IndexPredicateValueType::Json
        }
    }
}

pub(crate) fn lower_index_predicate(
    contract: &ResourceContractV1,
    expression: &ApplicationExpressionV1,
) -> Result<IndexPredicate> {
    Ok(match expression {
        ApplicationExpressionV1::Variable { name } => IndexPredicate::Field {
            field: resource_index_field(contract, name)?,
        },
        ApplicationExpressionV1::Literal { value } => IndexPredicate::Literal {
            value: value.clone(),
        },
        ApplicationExpressionV1::Unary {
            operator,
            value,
            value_type,
            operand_type,
        } => IndexPredicate::Unary {
            operator: match operator.as_str() {
                "not" => IndexPredicateUnaryOperator::Not,
                "negate" => IndexPredicateUnaryOperator::Negate,
                _ => {
                    return Err(AppRuntimeError::InvalidPackage(format!(
                        "unsupported partial-index unary operator `{operator}`"
                    )));
                }
            },
            value: Box::new(lower_index_predicate(contract, value)?),
            value_type: index_predicate_value_type((*operand_type).or(*value_type)),
        },
        ApplicationExpressionV1::Binary {
            operator,
            left,
            right,
            value_type,
            left_type,
            right_type,
        } => IndexPredicate::Binary {
            operator: match operator.as_str() {
                "and" => IndexPredicateBinaryOperator::And,
                "or" => IndexPredicateBinaryOperator::Or,
                "implies" => IndexPredicateBinaryOperator::Implies,
                "contains" => IndexPredicateBinaryOperator::Contains,
                "equal" => IndexPredicateBinaryOperator::Equal,
                "not_equal" => IndexPredicateBinaryOperator::NotEqual,
                "greater" => IndexPredicateBinaryOperator::Greater,
                "greater_equal" => IndexPredicateBinaryOperator::GreaterEqual,
                "less" => IndexPredicateBinaryOperator::Less,
                "less_equal" => IndexPredicateBinaryOperator::LessEqual,
                _ => {
                    return Err(AppRuntimeError::InvalidPackage(format!(
                        "unsupported partial-index binary operator `{operator}`"
                    )));
                }
            },
            left: Box::new(lower_index_predicate(contract, left)?),
            right: Box::new(lower_index_predicate(contract, right)?),
            value_type: index_predicate_value_type((*left_type).or(*right_type).or(*value_type)),
        },
        _ => {
            return Err(AppRuntimeError::InvalidPackage(
                "partial index predicate contains a non-portable expression".to_string(),
            ));
        }
    })
}

pub(crate) fn index_predicate_replacement_is_compatible(
    previous: &IndexDefinition,
    candidate: &IndexDefinition,
) -> bool {
    let same_identity = previous.name.eq_ignore_ascii_case(&candidate.name)
        && previous.collection == candidate.collection;
    let predicate_only = previous.fields == candidate.fields
        && previous.unique == candidate.unique
        && previous.kind == candidate.kind
        && previous.exclusion == candidate.exclusion
        && previous.predicate != candidate.predicate;
    let derived_non_unique_index = !previous.unique
        && !candidate.unique
        && previous.kind == candidate.kind
        && previous.exclusion == candidate.exclusion;

    same_identity && (predicate_only || derived_non_unique_index)
}

pub(crate) fn create_schema_index(
    db: &mut BicDb,
    definition: IndexDefinition,
    receipt: &mut SchemaApplicationReceipt,
) -> Result<()> {
    let name = definition.name.clone();
    if let Some(existing) = db
        .index_definitions()
        .into_iter()
        .find(|existing| existing.name.eq_ignore_ascii_case(&name))
    {
        if existing == definition {
            return Ok(());
        }
        if !index_predicate_replacement_is_compatible(&existing, &definition) {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "schema index `{name}` conflicts with its existing definition"
            )));
        }
        db.drop_index(&existing.name)?;
        if let Err(error) = db.create_index(definition) {
            return match db.create_index(existing) {
                Ok(()) => Err(error.into()),
                Err(restore) => Err(AppRuntimeError::InvalidPackage(format!(
                    "schema index `{name}` replacement failed: {error}; restoring its previous definition also failed: {restore}"
                ))),
            };
        }
        receipt.replaced_indexes.push(existing);
    } else {
        db.create_index(definition)?;
        receipt.created_indexes.push(name);
    }
    Ok(())
}

pub(crate) fn schema_failure_point(application: &str, version: &str, point: &str) -> Result<()> {
    let expected = format!("{application}@{version}:{point}");
    if cfg!(debug_assertions)
        && std::env::var("BICDB_TEST_SCHEMA_FAILURE_POINT")
            .ok()
            .as_deref()
            == Some(expected.as_str())
    {
        return Err(AppRuntimeError::InvalidPackage(format!(
            "injected schema activation failure at `{point}`"
        )));
    }
    Ok(())
}

pub(crate) fn suspend_relation_policies<I>(
    db: &mut BicDb,
    relations: I,
) -> Result<Vec<RelationPolicySnapshot>>
where
    I: IntoIterator<Item = String>,
{
    let mut snapshots = Vec::new();
    for relation in relations.into_iter().collect::<BTreeSet<_>>() {
        snapshots.push(RelationPolicySnapshot {
            collection: db.collection_policy(&relation)?,
            mutation: db.mutation_policy(&relation)?,
            relation,
        });
    }
    for (index, snapshot) in snapshots.iter().enumerate() {
        if let Err(error) = db
            .clear_collection_policy(&snapshot.relation)
            .and_then(|_| db.clear_mutation_policy(&snapshot.relation))
        {
            return match restore_relation_policies(db, &snapshots[..=index]) {
                Ok(()) => Err(error.into()),
                Err(restore) => Err(AppRuntimeError::InvalidPackage(format!(
                    "{error}; partially suspended schema policies also failed to restore: {restore}"
                ))),
            };
        }
    }
    Ok(snapshots)
}

pub(crate) fn restore_relation_policies(
    db: &mut BicDb,
    snapshots: &[RelationPolicySnapshot],
) -> Result<()> {
    for snapshot in snapshots.iter().rev() {
        match &snapshot.collection {
            Some(policy) => db.set_collection_policy(&snapshot.relation, policy.clone())?,
            None => db.clear_collection_policy(&snapshot.relation)?,
        }
        match &snapshot.mutation {
            Some(policy) => db.set_mutation_policy(&snapshot.relation, policy.clone())?,
            None => db.clear_mutation_policy(&snapshot.relation)?,
        }
    }
    Ok(())
}

pub(crate) fn migration_actor(
    application: &bicdb_extension::abi_v2::ApplicationManifestV2,
) -> MutationActor {
    MutationActor {
        actor_id: "bicdb-application-host".to_string(),
        roles: BTreeSet::from(["system:migration".to_string()]),
        scopes: BTreeSet::new(),
        tenant_id: None,
        workspace_id: None,
        originating_plugin: application.package.application.clone(),
        originating_resource: None,
        originating_action: Some("schema-transformation".to_string()),
        trace_id: uuid::Uuid::new_v4().to_string(),
        deadline_unix_ms: crate::host::now_ms().saturating_add(300_000),
    }
}

pub(crate) fn schema_expression_globals(
    object: &serde_json::Map<String, Value>,
) -> BTreeMap<String, Value> {
    let mut globals = object
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    globals.insert("source".to_string(), Value::Object(object.clone()));
    globals.insert("subject".to_string(), Value::Object(object.clone()));
    globals
}

pub(crate) fn validate_schema_checks(
    application: &bicdb_extension::abi_v2::ApplicationManifestV2,
    contract: &ResourceContractV1,
    object: &serde_json::Map<String, Value>,
) -> Result<()> {
    for check in &contract.checks {
        let value = evaluate_carrier_expression(
            application.application_program.as_ref(),
            &check.expression,
            schema_expression_globals(object),
        )?;
        if value.as_bool() != Some(true) {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "existing resource `{}` violates check `{}`",
                contract.name, check.name
            )));
        }
    }
    Ok(())
}
