//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

impl<'a> CapabilityApplicationProgramHost<'a> {
    pub(crate) fn new(
        host: &'a mut CapabilityHost,
        resources: Vec<ResourceContractV1>,
        service_bindings: BTreeMap<String, ApplicationServiceBindingV1>,
        client_bindings: BTreeMap<String, ApplicationClientBindingV1>,
        secret_bindings: BTreeMap<String, String>,
        event_bindings: BTreeMap<String, String>,
        realtime_bindings: BTreeMap<String, BTreeSet<String>>,
        workflow_bindings: BTreeMap<String, ApplicationWorkflowDefinitionV1>,
    ) -> Self {
        Self {
            host,
            resources: Arc::new(resources),
            service_bindings: Arc::new(service_bindings),
            client_bindings: Arc::new(client_bindings),
            secret_bindings: Arc::new(secret_bindings),
            event_bindings: Arc::new(event_bindings),
            realtime_bindings: Arc::new(realtime_bindings),
            workflow_bindings: Arc::new(workflow_bindings),
            secret_handles: BTreeMap::new(),
            transactions: Vec::new(),
            next_savepoint: 0,
            timeout_deadlines: Vec::new(),
            virtual_files: BTreeMap::new(),
            test_http: None,
        }
    }

    pub(crate) fn from_plan(host: &'a mut CapabilityHost, plan: &ApplicationExecutionPlan) -> Self {
        Self {
            host,
            resources: Arc::clone(&plan.resources),
            service_bindings: Arc::clone(&plan.service_bindings),
            client_bindings: Arc::clone(&plan.client_bindings),
            secret_bindings: Arc::clone(&plan.secret_bindings),
            event_bindings: Arc::clone(&plan.event_bindings),
            realtime_bindings: Arc::clone(&plan.realtime_bindings),
            workflow_bindings: Arc::clone(&plan.workflow_bindings),
            secret_handles: BTreeMap::new(),
            transactions: Vec::new(),
            next_savepoint: 0,
            timeout_deadlines: Vec::new(),
            virtual_files: BTreeMap::new(),
            test_http: None,
        }
    }

    pub(crate) fn with_test_http(mut self, callback: &'a ApplicationTestHttpCallback<'a>) -> Self {
        self.test_http = Some(callback);
        self
    }

    pub(crate) fn current_transaction(&self) -> Option<HostHandle> {
        self.transactions.first().map(|frame| match frame {
            ApplicationTransactionFrame::Transaction(transaction)
            | ApplicationTransactionFrame::Savepoint { transaction, .. } => *transaction,
        })
    }

    pub(crate) fn observability_contract(&self) -> Result<&ApplicationObservabilityContractV1> {
        self.host
            .application_manifest()
            .application_program
            .as_ref()
            .and_then(|program| program.observability.as_ref())
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(
                    "BicDB application observability helper lacks a signed contract".to_string(),
                )
            })
    }

    pub(crate) fn require_observability_helper(&self, target: &str) -> Result<()> {
        if self.observability_contract()?.helpers.contains(target) {
            Ok(())
        } else {
            Err(AppRuntimeError::CapabilityDenied(format!(
                "BicDB application observability helper `{target}` is outside signed authority"
            )))
        }
    }

    pub(crate) fn host_call(&mut self, request: HostRequest) -> Result<HostValue> {
        let result = self.host.call(HostCall {
            request_id: carrier_program_request_id(),
            request,
        });
        if let Some(error) = result.error {
            return Err(carrier_host_error(error));
        }
        result.value.ok_or_else(|| {
            AppRuntimeError::Invocation(
                "BicDB application capability call returned neither a value nor an error"
                    .to_string(),
            )
        })
    }

    pub(crate) fn load_workflow_state(
        &mut self,
        workflow: &str,
        run_id: &str,
    ) -> Result<ApplicationWorkflowStateV1> {
        let owned = self.current_transaction().is_none();
        if owned {
            self.begin_transaction("read_committed")?;
        }
        let transaction = self
            .current_transaction()
            .expect("workflow read transaction");
        let result: Result<ApplicationWorkflowStateV1> = (|| {
            let value = self
                .host
                .read_durable_workflow(transaction, run_id)?
                .ok_or_else(|| {
                    AppRuntimeError::NotFound(format!(
                        "BicDB application workflow run `{run_id}` was not found"
                    ))
                })?;
            let state: ApplicationWorkflowStateV1 =
                serde_json::from_value(value).map_err(|error| {
                    AppRuntimeError::InvalidPackage(format!(
                        "durable workflow state `{run_id}` is invalid: {error}"
                    ))
                })?;
            if state.workflow != workflow {
                return Err(AppRuntimeError::NotFound(format!(
                    "BicDB application workflow run `{run_id}` does not belong to `{workflow}`"
                )));
            }
            Ok(state)
        })();
        if owned {
            if result.is_ok() {
                self.commit_transaction()?;
            } else {
                let _ = self.rollback_transaction();
            }
        }
        result
    }

    pub(crate) fn store_workflow_state(
        &mut self,
        run_id: &str,
        state: &mut ApplicationWorkflowStateV1,
        expected_revision: Option<u64>,
    ) -> Result<()> {
        let transaction = self.current_transaction().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "durable workflow write has no active transaction".to_string(),
            )
        })?;
        let audited_slas = self
            .workflow_bindings
            .get(&state.workflow)
            .map(|definition| {
                definition
                    .slas
                    .iter()
                    .filter(|sla| sla.attach_to_audit)
                    .map(|sla| sla.name.clone())
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        for evidence in state
            .evidence
            .iter()
            .filter(|entry| entry.sequence > state.audited_evidence_sequence)
            .filter(|entry| {
                entry
                    .sla
                    .as_deref()
                    .is_some_and(|sla| audited_slas.contains(sla))
            })
        {
            self.host.register_durable_audit(
                transaction,
                evidence.kind.clone(),
                format!("workflow:{}:{run_id}", state.workflow),
                BTreeMap::from([
                    (
                        "sla".to_string(),
                        Value::String(evidence.sla.clone().unwrap_or_default()),
                    ),
                    (
                        "workflow".to_string(),
                        Value::String(state.workflow.clone()),
                    ),
                    ("run_id".to_string(), Value::String(run_id.to_string())),
                    ("sequence".to_string(), json!(evidence.sequence)),
                ]),
            )?;
        }
        state.audited_evidence_sequence = state
            .evidence
            .last()
            .map(|entry| entry.sequence)
            .unwrap_or(state.audited_evidence_sequence);
        self.host.write_durable_workflow(
            transaction,
            run_id,
            serde_json::to_value(state)?,
            expected_revision,
        )
    }

    pub(crate) fn publish_workflow_continuation(
        &mut self,
        definition: &ApplicationWorkflowDefinitionV1,
        run_id: &str,
        revision: u64,
    ) -> Result<()> {
        self.publish_workflow_message(definition, run_id, "drive", None, None, revision, None)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_workflow_message(
        &mut self,
        definition: &ApplicationWorkflowDefinitionV1,
        run_id: &str,
        kind: &str,
        step: Option<&str>,
        sla: Option<&str>,
        revision: u64,
        _delay_ms: Option<u64>,
    ) -> Result<()> {
        let transaction = self.current_transaction().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "workflow continuation has no active transaction".to_string(),
            )
        })?;
        let value = self.host_call(HostRequest::Broker(BrokerRequest::PublishOnCommit {
            transaction,
            queue: definition.queue.clone(),
            payload: json!({
                "run_id": run_id,
                "kind": kind,
                "step": step,
                "sla": sla,
            }),
            headers: BTreeMap::from([(
                "carrier_workflow".to_string(),
                definition.worker_export.clone(),
            )]),
            idempotency_key: Some(format!(
                "carrier-workflow:{run_id}:{revision}:{kind}:{}:{}",
                step.unwrap_or("-"),
                sla.unwrap_or("-")
            )),
            // Workflow deadlines are stored in durable workflow state. Publish timer
            // messages immediately so the ordered broker group can claim and nack
            // early timers until their deadline; a future-available message would
            // otherwise head-of-line block every later workflow continuation.
            delay_ms: None,
        }))?;
        match value {
            HostValue::String(_) => Ok(()),
            other => carrier_host_type_error("workflow continuation", "receipt", other),
        }
    }

    pub(crate) fn start_workflow(
        &mut self,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let positional = arguments
            .iter()
            .filter(|(name, _)| name.is_none())
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        if positional.len() != 2 {
            return Err(AppRuntimeError::InvalidRequest(
                "workflows.start requires a workflow name and input".to_string(),
            ));
        }
        let workflow = carrier_string(positional[0].clone(), "workflows.start")?;
        let baggage = arguments
            .iter()
            .find(|(name, _)| name.as_deref() == Some("baggage"))
            .map(|(_, value)| value.clone())
            .unwrap_or_else(|| json!({}));
        if !baggage.is_object()
            || arguments
                .iter()
                .any(|(name, _)| name.as_deref().is_some_and(|name| name != "baggage"))
        {
            return Err(AppRuntimeError::InvalidRequest(
                "workflows.start baggage must be an object and is the only named argument"
                    .to_string(),
            ));
        }
        let definition = match self.workflow_bindings.get(&workflow).cloned() {
            Some(definition) => definition,
            None => {
                let alias = carrier_federated_workflow_binding_alias(&workflow, "start");
                if !self.service_bindings.contains_key(&alias) {
                    return Err(AppRuntimeError::CapabilityDenied(format!(
                        "BicDB application workflow `{workflow}` has no signed local or federated binding"
                    )));
                }
                return self.call_service(
                    &alias,
                    vec![
                        (Some("input".to_string()), positional[1].clone()),
                        (Some("baggage".to_string()), baggage),
                    ],
                );
            }
        };
        for invariant in &definition.invariants {
            let value = evaluate_carrier_expression(
                None,
                &invariant.expression,
                BTreeMap::from([("input".to_string(), positional[1].clone())]),
            )?;
            let holds = value.as_bool().ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "BicDB application workflow invariant `{}` did not return Bool",
                    invariant.name
                ))
            })?;
            let violated = match invariant.kind {
                bicdb_extension::abi_v2::ApplicationInvariantKindV1::MustAlways => !holds,
                bicdb_extension::abi_v2::ApplicationInvariantKindV1::MustNever => holds,
            };
            if violated {
                return Err(AppRuntimeError::InvalidRequest(format!(
                    "BicDB application workflow invariant `{}` rejected the run",
                    invariant.name
                )));
            }
        }
        let run_id = uuid::Uuid::new_v4().to_string();
        let now = crate::host::now_ms();
        let timeout_at_ms = definition
            .timeout_ms
            .map(|timeout| now.saturating_add(i64::try_from(timeout).unwrap_or(i64::MAX)));
        let actor = self.host.actor().clone();
        let mut state = ApplicationWorkflowStateV1 {
            application: self.host.application_name().to_string(),
            workflow,
            status: "queued".to_string(),
            input: positional[1].clone(),
            baggage,
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
            timeout_at_ms,
            created_at_ms: now,
            updated_at_ms: now,
            finished_at_ms: None,
            tenant_id: actor.tenant_id,
            workspace_id: actor.workspace_id,
            revision: 1,
        };
        append_workflow_evidence(&mut state, "workflow_started", None, None, None);
        let ready = schedule_ready_workflow_steps(&definition, &mut state);
        let mut sla_timers =
            update_workflow_slas(&definition, &mut state, "workflow_started", None);
        sla_timers.extend(update_workflow_slas(
            &definition,
            &mut state,
            "status",
            Some("queued"),
        ));
        let owned = self.current_transaction().is_none();
        if owned {
            self.begin_transaction("read_committed")?;
        }
        let result: Result<()> = (|| {
            self.store_workflow_state(&run_id, &mut state, None)?;
            for step in &ready {
                self.publish_workflow_message(
                    &definition,
                    &run_id,
                    "step",
                    Some(step),
                    None,
                    state.revision,
                    None,
                )?;
            }
            for timer in &sla_timers {
                self.publish_workflow_message(
                    &definition,
                    &run_id,
                    timer.kind,
                    None,
                    Some(&timer.sla),
                    state.revision,
                    Some(timer.delay_ms),
                )?;
            }
            if let Some(timeout_ms) = definition.timeout_ms {
                self.publish_workflow_message(
                    &definition,
                    &run_id,
                    "workflow_timeout",
                    None,
                    None,
                    state.revision,
                    Some(timeout_ms),
                )?;
            }
            Ok(())
        })();
        if owned {
            if result.is_ok() {
                self.commit_transaction()?;
            } else {
                let _ = self.rollback_transaction();
            }
        }
        result?;
        Ok(Value::String(run_id))
    }

    pub(crate) fn workflow_status(
        &mut self,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let (workflow, run_id) = workflow_name_and_run_id(arguments, "workflows.status", 0)?;
        if !self.workflow_bindings.contains_key(&workflow) {
            let alias = carrier_federated_workflow_binding_alias(&workflow, "status");
            if !self.service_bindings.contains_key(&alias) {
                return Err(AppRuntimeError::CapabilityDenied(format!(
                    "BicDB application workflow `{workflow}` has no signed local or federated binding"
                )));
            }
            return self.call_service(
                &alias,
                vec![(Some("run_id".to_string()), Value::String(run_id))],
            );
        }
        Ok(Value::String(
            self.load_workflow_state(&workflow, &run_id)?.status,
        ))
    }

    pub(crate) fn workflow_evidence(
        &mut self,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let (workflow, run_id) = workflow_name_and_run_id(arguments, "workflows.evidence", 0)?;
        if !self.workflow_bindings.contains_key(&workflow) {
            let alias = carrier_federated_workflow_binding_alias(&workflow, "evidence");
            if !self.service_bindings.contains_key(&alias) {
                return Err(AppRuntimeError::CapabilityDenied(format!(
                    "BicDB application workflow `{workflow}` has no signed evidence binding"
                )));
            }
            return self.call_service(
                &alias,
                vec![(Some("run_id".to_string()), Value::String(run_id))],
            );
        }
        serde_json::to_value(self.load_workflow_state(&workflow, &run_id)?.evidence)
            .map_err(Into::into)
    }

    pub(crate) fn cancel_workflow(
        &mut self,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let (workflow, run_id) = workflow_name_and_run_id(arguments, "workflows.cancel", 0)?;
        let definition = match self.workflow_bindings.get(&workflow).cloned() {
            Some(definition) => definition,
            None => {
                let alias = carrier_federated_workflow_binding_alias(&workflow, "cancel");
                if !self.service_bindings.contains_key(&alias) {
                    return Err(AppRuntimeError::CapabilityDenied(format!(
                        "BicDB application workflow `{workflow}` has no signed cancellation binding"
                    )));
                }
                return self.call_service(
                    &alias,
                    vec![(Some("run_id".to_string()), Value::String(run_id))],
                );
            }
        };
        let owned = self.current_transaction().is_none();
        const MAX_CANCELLATION_ATTEMPTS: u32 = 64;
        let attempts = if owned { MAX_CANCELLATION_ATTEMPTS } else { 1 };
        for attempt in 0..attempts {
            if owned {
                self.begin_transaction("read_committed")?;
            }
            let result = (|| {
                let mut state = self.load_workflow_state(&workflow, &run_id)?;
                if state.terminal() {
                    return Ok(Value::String(state.status));
                }
                let expected_revision = state.revision;
                state.cancel_requested = true;
                state.scheduled_steps.clear();
                state.waiting_steps.clear();
                state.active_steps.clear();
                state.updated_at_ms = crate::host::now_ms();
                append_workflow_evidence(&mut state, "cancellation_requested", None, None, None);
                if let Some(compensation) = next_compensation_step(&definition, &state) {
                    state.status = "compensating".to_string();
                    state.active_step = Some(compensation.name.clone());
                } else {
                    state.status = "cancelled".to_string();
                    state.active_step = None;
                    state.finished_at_ms = Some(state.updated_at_ms);
                    append_workflow_evidence(&mut state, "workflow_cancelled", None, None, None);
                }
                let status = state.status.clone();
                let timers = update_workflow_slas(&definition, &mut state, "status", Some(&status));
                state.revision = state.revision.saturating_add(1);
                self.store_workflow_state(&run_id, &mut state, Some(expected_revision))?;
                if state.status == "compensating" {
                    self.publish_workflow_continuation(&definition, &run_id, state.revision)?;
                }
                publish_scheduled_workflow_messages(
                    self,
                    &definition,
                    &run_id,
                    state.revision,
                    &[],
                    &timers,
                )?;
                Ok(Value::String(state.status))
            })();
            let result = if owned {
                match result {
                    Ok(value) => self.commit_transaction().map(|()| value),
                    Err(error) => {
                        let _ = self.rollback_transaction();
                        Err(error)
                    }
                }
            } else {
                result
            };
            match result {
                Err(AppRuntimeError::Conflict(_) | AppRuntimeError::OptimisticConflict(_))
                    if attempt + 1 < attempts =>
                {
                    let delay_ms = 1_u64 << attempt.min(5);
                    self.sleep(Duration::from_millis(delay_ms))?;
                }
                result => return result,
            }
        }
        unreachable!("bounded workflow cancellation retry loop always returns")
    }

    pub(crate) fn signal_workflow(
        &mut self,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let positional = arguments
            .iter()
            .filter(|(name, _)| name.is_none())
            .map(|(_, value)| value.clone())
            .collect::<Vec<_>>();
        if positional.len() != 4 || arguments.iter().any(|(name, _)| name.is_some()) {
            return Err(AppRuntimeError::InvalidRequest(
                "workflows.signal requires workflow, run id, signal, and payload".to_string(),
            ));
        }
        let workflow = carrier_string(positional[0].clone(), "workflows.signal")?;
        let run_id = carrier_string(positional[1].clone(), "workflows.signal")?;
        let signal = carrier_string(positional[2].clone(), "workflows.signal")?;
        let definition = match self.workflow_bindings.get(&workflow).cloned() {
            Some(definition) => definition,
            None => {
                let alias = carrier_federated_workflow_binding_alias(&workflow, "signal");
                if !self.service_bindings.contains_key(&alias) {
                    return Err(AppRuntimeError::CapabilityDenied(format!(
                        "BicDB application workflow `{workflow}` has no signed signal binding"
                    )));
                }
                return self.call_service(
                    &alias,
                    vec![
                        (Some("run_id".to_string()), Value::String(run_id)),
                        (Some("signal".to_string()), Value::String(signal)),
                        (Some("payload".to_string()), positional[3].clone()),
                    ],
                );
            }
        };
        let owned = self.current_transaction().is_none();
        const MAX_SIGNAL_ATTEMPTS: u32 = 64;
        let attempts = if owned { MAX_SIGNAL_ATTEMPTS } else { 1 };
        for attempt in 0..attempts {
            if owned {
                self.begin_transaction("read_committed")?;
            }
            let result = (|| {
                let mut state = self.load_workflow_state(&workflow, &run_id)?;
                if state.terminal() {
                    return Err(AppRuntimeError::Conflict(format!(
                        "BicDB application workflow run `{run_id}` is already `{}`",
                        state.status
                    )));
                }
                let expected_revision = state.revision;
                let evidence_before = state.evidence.len();
                let mut timers =
                    update_workflow_slas(&definition, &mut state, "event", Some(&signal));
                let step_name = state
                    .waiting_steps
                    .iter()
                    .find(|(_, wait)| wait.signal.as_deref() == Some(signal.as_str()))
                    .map(|(step, _)| step.clone());
                let Some(step_name) = step_name else {
                    if state.evidence.len() == evidence_before {
                        return Err(AppRuntimeError::Conflict(format!(
                        "BicDB application workflow run `{run_id}` is not waiting for signal or SLA event `{signal}`"
                    )));
                    }
                    append_workflow_evidence(
                        &mut state,
                        "workflow_event_received",
                        None,
                        None,
                        Some(json!({"event": signal, "payload": positional[3].clone()})),
                    );
                    state.updated_at_ms = crate::host::now_ms();
                    state.revision = state.revision.saturating_add(1);
                    self.store_workflow_state(&run_id, &mut state, Some(expected_revision))?;
                    publish_scheduled_workflow_messages(
                        self,
                        &definition,
                        &run_id,
                        state.revision,
                        &[],
                        &timers,
                    )?;
                    return Ok(Value::String(state.status));
                };
                state.waiting_steps.remove(&step_name);
                state.scheduled_steps.remove(&step_name);
                state.active_steps.remove(&step_name);
                state
                    .step_results
                    .insert(step_name.clone(), positional[3].clone());
                state.completed_step_order.push(step_name.clone());
                state.active_step = Some(step_name.clone());
                state.updated_at_ms = crate::host::now_ms();
                state.last_error = None;
                append_workflow_evidence(
                    &mut state,
                    "signal_received",
                    Some(&step_name),
                    None,
                    Some(json!({"signal": signal})),
                );
                append_workflow_evidence(
                    &mut state,
                    "step_completed",
                    Some(&step_name),
                    None,
                    None,
                );
                timers.extend(update_workflow_slas(
                    &definition,
                    &mut state,
                    "step_completed",
                    Some(&step_name),
                ));
                let ready = if step_name == definition.return_step {
                    state.status = "completed".to_string();
                    state.output = Some(positional[3].clone());
                    state.finished_at_ms = Some(state.updated_at_ms);
                    append_workflow_evidence(&mut state, "workflow_completed", None, None, None);
                    timers.extend(update_workflow_slas(
                        &definition,
                        &mut state,
                        "status",
                        Some("completed"),
                    ));
                    Vec::new()
                } else {
                    let ready = schedule_ready_workflow_steps(&definition, &mut state);
                    state.status = workflow_nonterminal_status(&state);
                    let status = state.status.clone();
                    timers.extend(update_workflow_slas(
                        &definition,
                        &mut state,
                        "status",
                        Some(&status),
                    ));
                    ready
                };
                state.revision = state.revision.saturating_add(1);
                self.store_workflow_state(&run_id, &mut state, Some(expected_revision))?;
                publish_scheduled_workflow_messages(
                    self,
                    &definition,
                    &run_id,
                    state.revision,
                    &ready,
                    &timers,
                )?;
                Ok(Value::String(state.status))
            })();
            let result = if owned {
                match result {
                    Ok(value) => self.commit_transaction().map(|()| value),
                    Err(error) => {
                        let _ = self.rollback_transaction();
                        Err(error)
                    }
                }
            } else {
                result
            };
            match result {
                Err(error)
                    if attempt + 1 < attempts && carrier_workflow_concurrency_conflict(&error) =>
                {
                    let delay_ms = 1_u64 << attempt.min(5);
                    self.sleep(Duration::from_millis(delay_ms))?;
                }
                result => return result,
            }
        }
        unreachable!("bounded workflow signal retry loop always returns")
    }

    pub(crate) fn workflow_result(
        &mut self,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let (workflow, run_id) = workflow_name_and_run_id(arguments, "workflows.result_as", 1)?;
        if !self.workflow_bindings.contains_key(&workflow) {
            let alias = carrier_federated_workflow_binding_alias(&workflow, "result");
            if !self.service_bindings.contains_key(&alias) {
                return Err(AppRuntimeError::CapabilityDenied(format!(
                    "BicDB application workflow `{workflow}` has no signed local or federated binding"
                )));
            }
            return self.call_service(
                &alias,
                vec![(Some("run_id".to_string()), Value::String(run_id))],
            );
        }
        let state = self.load_workflow_state(&workflow, &run_id)?;
        if state.status != "completed" {
            return Err(AppRuntimeError::NotReady(format!(
                "BicDB application workflow run `{run_id}` is `{}` rather than completed",
                state.status
            )));
        }
        state.output.ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "completed BicDB application workflow run `{run_id}` has no output"
            ))
        })
    }

    pub(crate) fn retry_workflow_compensation(
        &mut self,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let (workflow, run_id) =
            workflow_name_and_run_id(arguments, "workflows.retry_compensation", 0)?;
        let definition = match self.workflow_bindings.get(&workflow).cloned() {
            Some(definition) => definition,
            None => {
                let alias =
                    carrier_federated_workflow_binding_alias(&workflow, "retry_compensation");
                if !self.service_bindings.contains_key(&alias) {
                    return Err(AppRuntimeError::CapabilityDenied(format!(
                        "BicDB application workflow `{workflow}` has no signed compensation-retry binding"
                    )));
                }
                return self.call_service(
                    &alias,
                    vec![(Some("run_id".to_string()), Value::String(run_id))],
                );
            }
        };
        let owned = self.current_transaction().is_none();
        if owned {
            self.begin_transaction("read_committed")?;
        }
        let result = (|| {
            let mut state = self.load_workflow_state(&workflow, &run_id)?;
            if state.status != "compensation_failed" {
                return Err(AppRuntimeError::Conflict(format!(
                    "BicDB application workflow run `{run_id}` is not awaiting compensation retry"
                )));
            }
            let active_step = state.active_step.clone().ok_or_else(|| {
                AppRuntimeError::Conflict(
                    "BicDB application workflow has no failed compensation step".to_string(),
                )
            })?;
            let expected_revision = state.revision;
            state.status = "compensating".to_string();
            state.compensation_attempts.insert(active_step, 0);
            state.finished_at_ms = None;
            state.updated_at_ms = crate::host::now_ms();
            let status = state.status.clone();
            let timers = update_workflow_slas(&definition, &mut state, "status", Some(&status));
            state.revision = state.revision.saturating_add(1);
            self.store_workflow_state(&run_id, &mut state, Some(expected_revision))?;
            self.publish_workflow_continuation(&definition, &run_id, state.revision)?;
            publish_scheduled_workflow_messages(
                self,
                &definition,
                &run_id,
                state.revision,
                &[],
                &timers,
            )?;
            Ok(Value::String(state.status))
        })();
        if owned {
            if result.is_ok() {
                self.commit_transaction()?;
            } else {
                let _ = self.rollback_transaction();
            }
        }
        result
    }

    pub(crate) fn call_model(
        &mut self,
        target: &str,
        method: &str,
        arguments: Vec<(Option<String>, Value)>,
    ) -> Result<Value> {
        let contract = self
            .resources
            .iter()
            .find(|contract| contract.name.eq_ignore_ascii_case(target))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "BicDB application model `{target}` has no signed resource contract"
                ))
            })?;
        let mut named = BTreeMap::new();
        let mut positional = Vec::new();
        for (name, value) in arguments {
            if let Some(name) = name {
                if named.insert(name.clone(), value).is_some() {
                    return Err(AppRuntimeError::InvalidPackage(format!(
                        "BicDB application model call `{target}.{method}` repeats argument `{name}`"
                    )));
                }
            } else {
                positional.push(value);
            }
        }
        let scope = named
            .remove("scope")
            .and_then(|value| value.as_str().map(str::to_string))
            .unwrap_or_else(|| "active".to_string());
        let scope = match scope.as_str() {
            "active" => ResourceRecordScope::Active,
            "all" => ResourceRecordScope::All,
            "deleted" => ResourceRecordScope::Deleted,
            _ => {
                return Err(AppRuntimeError::InvalidRequest(format!(
                    "invalid BicDB application record scope `{scope}`"
                )));
            }
        };
        if !matches!(
            method,
            "bulk_insert"
                | "bulk_upsert"
                | "similar"
                | "similar_with_scores"
                | "hybrid_search"
                | "recent"
                | "within"
                | "nearest"
                | "contains"
        ) && !positional.is_empty()
        {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "BicDB application model call `{target}.{method}` contains a positional argument"
            )));
        }

        match method {
            "similar" | "similar_with_scores" | "hybrid_search" => {
                self.call_model_vector(&contract, method, positional, named, scope)
            }
            "within" | "nearest" | "contains" => {
                self.call_model_spatial(&contract, method, positional, named, scope)
            }
            "recent" => self.call_model_recent(&contract, positional, named, scope),
            method
                if matches!(method, "get" | "get_for_update") || method.starts_with("get_by_") =>
            {
                if named.len() != 1 {
                    return Err(AppRuntimeError::InvalidPackage(format!(
                        "BicDB application model call `{target}.{method}` requires one lookup argument"
                    )));
                }
                let (field, value) = named.into_iter().next().expect("one lookup");
                if let Some(expected) = method.strip_prefix("get_by_") {
                    if field != expected {
                        return Err(AppRuntimeError::InvalidPackage(format!(
                            "BicDB application model call `{target}.{method}` requires lookup field `{expected}`"
                        )));
                    }
                }
                let mut request = carrier_resource_request(ResourceOperation::Get);
                request.scope = scope;
                request.internal_model_call = true;
                if field == contract.primary_key {
                    request.id = Some(carrier_scalar_id(value, target, method)?);
                } else {
                    request.operation = ResourceOperation::List;
                    request.filters.push(FilterExpression::Eq { field, value });
                    request.limit = 1;
                }
                let reread_request = request.clone();
                let transaction = self.current_transaction();
                if method == "get_for_update" && transaction.is_none() {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "BicDB application get_for_update requires an enclosing transaction".to_string(),
                    ));
                }
                let response = match transaction {
                    Some(transaction) => execute_resource_operation_in_transaction(
                        self.host,
                        &contract,
                        request,
                        transaction,
                    )?,
                    None => execute_resource_operation(self.host, &contract, request)?,
                };
                let row = if response.body.is_array() {
                    response
                        .body
                        .as_array()
                        .and_then(|rows| rows.first())
                        .cloned()
                        .ok_or_else(|| {
                            AppRuntimeError::NotFound(format!(
                                "BicDB application model lookup `{target}.{method}` found no row"
                            ))
                        })?
                } else {
                    response.body
                };
                if method == "get_for_update" {
                    let transaction = transaction.expect("checked transaction");
                    let id = row.get(&contract.primary_key).cloned().ok_or_else(|| {
                        AppRuntimeError::InvalidPackage(format!(
                            "resource `{target}` result lacks primary key `{}`",
                            contract.primary_key
                        ))
                    })?;
                    self.host_call(HostRequest::Database(DatabaseRequest::LockRows {
                        transaction,
                        relation: contract.relation.clone(),
                        ids: vec![carrier_scalar_id(id, target, method)?],
                    }))?;
                    // SELECT ... FOR UPDATE semantics: the pre-lock read may
                    // be stale if another transaction committed while this
                    // one waited for the row lock. Re-read after acquiring
                    // the lock and return the current committed version.
                    let response = execute_resource_operation_in_transaction(
                        self.host,
                        &contract,
                        reread_request,
                        transaction,
                    )?;
                    let fresh = if response.body.is_array() {
                        response
                            .body
                            .as_array()
                            .and_then(|rows| rows.first())
                            .cloned()
                    } else if response.body.is_null() {
                        None
                    } else {
                        Some(response.body)
                    };
                    return fresh.ok_or_else(|| {
                        AppRuntimeError::NotFound(format!(
                            "BicDB application model lookup `{target}.{method}` found no row"
                        ))
                    });
                }
                Ok(row)
            }
            "list" | "search" => self.call_model_list(&contract, method, named, scope),
            method
                if method.strip_prefix("list_").is_some_and(|name| {
                    contract
                        .reverse_relations
                        .iter()
                        .any(|relation| relation.name == name)
                }) =>
            {
                self.call_model_reverse_relation(&contract, method, named, scope)
            }
            method if method.starts_with("list_by_") => {
                self.call_model_list(&contract, method, named, scope)
            }
            method
                if matches!(method, "count" | "exists")
                    || method.starts_with("count_by_")
                    || method.starts_with("exists_by_") =>
            {
                self.call_model_count(&contract, method, named, scope)
            }
            method if method.starts_with("upsert_by_") => {
                self.call_model_upsert(&contract, method, named)
            }
            "bulk_insert" | "bulk_upsert" => {
                self.call_model_bulk(&contract, method, positional, named)
            }
            _ => Err(AppRuntimeError::CapabilityDenied(format!(
                "BicDB application model method `{target}.{method}` is not bound to the resource host"
            ))),
        }
    }

    pub(crate) fn call_model_vector(
        &mut self,
        contract: &ResourceContractV1,
        method: &str,
        mut positional: Vec<Value>,
        mut named: BTreeMap<String, Value>,
        scope: ResourceRecordScope,
    ) -> Result<Value> {
        let vector_contract = contract.vector_search.as_ref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application model `{}` has no signed vector-search contract",
                contract.name
            ))
        })?;
        let query = if method == "hybrid_search" {
            let query = match positional.len() {
                0 => named.remove("query").ok_or_else(|| {
                    AppRuntimeError::InvalidPackage(
                        "BicDB application hybrid_search requires its query".to_string(),
                    )
                })?,
                1 => positional.remove(0),
                _ => {
                    return Err(AppRuntimeError::InvalidPackage(
                        "BicDB application hybrid_search accepts one query".to_string(),
                    ))
                }
            };
            Some(carrier_string(
                query,
                &format!("{}.hybrid_search", contract.name),
            )?)
        } else {
            None
        };
        let embedding = if method == "hybrid_search" {
            named.remove("embedding")
        } else if positional.len() == 1 {
            Some(positional.remove(0))
        } else {
            named.remove("embedding")
        }
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application model `{}.{method}` lacks its embedding",
                contract.name
            ))
        })?;
        if !positional.is_empty() {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "BicDB application model `{}.{method}` has unexpected positional arguments",
                contract.name
            )));
        }
        let vector = carrier_vector(embedding, &format!("{}.{method}", contract.name))?;
        let limit = named
            .remove("limit")
            .and_then(|value| value.as_i64())
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest(format!(
                    "BicDB application model `{}.{method}` requires an integer limit",
                    contract.name
                ))
            })?;
        if limit <= 0 || limit > 10_000 {
            return Err(AppRuntimeError::InvalidRequest(
                "similarity limit must be in 1..=10000".to_string(),
            ));
        }
        let filters = resource_scope_filters(contract, scope);
        let request = if method == "hybrid_search" {
            let vector_weight = carrier_optional_weight(
                named.remove("vector_weight"),
                0.7,
                &format!("{}.hybrid_search vector_weight", contract.name),
            )?;
            let text_weight = carrier_optional_weight(
                named.remove("text_weight"),
                0.3,
                &format!("{}.hybrid_search text_weight", contract.name),
            )?;
            DatabaseRequest::HybridSearch {
                transaction: HostHandle(0),
                relation: contract.relation.clone(),
                vector_index: vector_contract.index_name.clone(),
                text_index: resource_search_index_name(contract),
                query: query.expect("hybrid query exists"),
                vector,
                vector_weight,
                text_weight,
                limit: limit as u32,
                filters,
            }
        } else {
            DatabaseRequest::VectorSearch {
                transaction: HostHandle(0),
                relation: contract.relation.clone(),
                index: vector_contract.index_name.clone(),
                vector,
                limit: limit as u32,
                filters,
            }
        };
        if !named.is_empty() {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "BicDB application model `{}.{method}` has unexpected named arguments",
                contract.name
            )));
        }

        let owned_transaction = self.current_transaction().is_none();
        if owned_transaction {
            self.begin_transaction("read_committed")?;
        }
        let result = (|| {
            let transaction = self.current_transaction().ok_or_else(|| {
                AppRuntimeError::InvalidPackage("vector-search transaction is absent".to_string())
            })?;
            let request = match request {
                DatabaseRequest::VectorSearch {
                    relation,
                    index,
                    vector,
                    limit,
                    filters,
                    ..
                } => DatabaseRequest::VectorSearch {
                    transaction,
                    relation,
                    index,
                    vector,
                    limit,
                    filters,
                },
                DatabaseRequest::HybridSearch {
                    relation,
                    vector_index,
                    text_index,
                    query,
                    vector,
                    vector_weight,
                    text_weight,
                    limit,
                    filters,
                    ..
                } => DatabaseRequest::HybridSearch {
                    transaction,
                    relation,
                    vector_index,
                    text_index,
                    query,
                    vector,
                    vector_weight,
                    text_weight,
                    limit,
                    filters,
                },
                _ => unreachable!("vector lowering creates a vector request"),
            };
            let rows = expect_carrier_rows(self.host_call(HostRequest::Database(request))?)?;
            rows.into_iter()
                .map(|row| shape_vector_match(self.host, contract, method, row))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array)
        })();
        if owned_transaction {
            match result {
                Ok(value) => {
                    if let Err(error) = self.commit_transaction() {
                        let _ = self.rollback_transaction();
                        return Err(error);
                    }
                    Ok(value)
                }
                Err(error) => {
                    let _ = self.rollback_transaction();
                    Err(error)
                }
            }
        } else {
            result
        }
    }

    pub(crate) fn call_model_spatial(
        &mut self,
        contract: &ResourceContractV1,
        method: &str,
        mut positional: Vec<Value>,
        mut named: BTreeMap<String, Value>,
        scope: ResourceRecordScope,
    ) -> Result<Value> {
        let point = match positional.len() {
            0 => named.remove("point"),
            1 => Some(positional.remove(0)),
            _ => {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "BicDB application model `{}.{method}` accepts one point",
                    contract.name
                )))
            }
        }
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application model `{}.{method}` lacks its point",
                contract.name
            ))
        })?;
        let operation = match method {
            "within" => {
                let radius = named
                    .remove("radius")
                    .and_then(|value| value.as_f64())
                    .filter(|value| value.is_finite() && *value >= 0.0)
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidRequest(
                            "within radius must be a finite non-negative number".to_string(),
                        )
                    })?;
                let field = carrier_spatial_field(contract, ApplicationGeometryTypeV1::Point)?;
                SpatialOperation::WithinRadius {
                    field,
                    point,
                    radius,
                    limit: 10_000,
                }
            }
            "nearest" => {
                let limit = carrier_model_limit(&mut named, "nearest")?;
                let field = carrier_spatial_field(contract, ApplicationGeometryTypeV1::Point)?;
                SpatialOperation::Nearest {
                    field,
                    point,
                    limit,
                }
            }
            "contains" => {
                let field = carrier_spatial_field(contract, ApplicationGeometryTypeV1::Polygon)?;
                SpatialOperation::Contains {
                    field,
                    point,
                    limit: 10_000,
                }
            }
            _ => unreachable!("spatial dispatch checked method"),
        };
        if !named.is_empty() {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "BicDB application model `{}.{method}` has unexpected named arguments",
                contract.name
            )));
        }
        let filters = resource_scope_filters(contract, scope);
        let owned_transaction = self.current_transaction().is_none();
        if owned_transaction {
            self.begin_transaction("read_committed")?;
        }
        let result = (|| {
            let transaction = self.current_transaction().ok_or_else(|| {
                AppRuntimeError::InvalidPackage("spatial transaction is absent".to_string())
            })?;
            let rows = expect_carrier_rows(self.host_call(HostRequest::Database(
                DatabaseRequest::Spatial {
                    transaction,
                    relation: contract.relation.clone(),
                    operation,
                    filters,
                },
            ))?)?;
            rows.into_iter()
                .map(|row| redact_resource(self.host, contract, row))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array)
        })();
        self.finish_owned_read_transaction(owned_transaction, result)
    }

    pub(crate) fn call_model_recent(
        &mut self,
        contract: &ResourceContractV1,
        mut positional: Vec<Value>,
        mut named: BTreeMap<String, Value>,
        scope: ResourceRecordScope,
    ) -> Result<Value> {
        let timeseries = contract.timeseries.as_ref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application model `{}` has no signed timeseries contract",
                contract.name
            ))
        })?;
        let window = match positional.len() {
            0 => named.remove("window"),
            1 => Some(positional.remove(0)),
            _ => {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "BicDB application model `{}.recent` accepts one window",
                    contract.name
                )))
            }
        }
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application model `{}.recent` lacks its window",
                contract.name
            ))
        })?;
        let window = carrier_string(window, &format!("{}.recent", contract.name))?;
        let limit = carrier_model_limit(&mut named, "recent")?;
        if !named.is_empty() {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "BicDB application model `{}.recent` has unexpected named arguments",
                contract.name
            )));
        }
        let filters = resource_scope_filters(contract, scope);
        let owned_transaction = self.current_transaction().is_none();
        if owned_transaction {
            self.begin_transaction("read_committed")?;
        }
        let result = (|| {
            let transaction = self.current_transaction().ok_or_else(|| {
                AppRuntimeError::InvalidPackage("recent transaction is absent".to_string())
            })?;
            let rows = expect_carrier_rows(self.host_call(HostRequest::Database(
                DatabaseRequest::Recent {
                    transaction,
                    relation: contract.relation.clone(),
                    time_field: timeseries.time_field.clone(),
                    window,
                    limit,
                    filters,
                },
            ))?)?;
            rows.into_iter()
                .map(|row| redact_resource(self.host, contract, row))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array)
        })();
        self.finish_owned_read_transaction(owned_transaction, result)
    }

    pub(crate) fn finish_owned_read_transaction(
        &mut self,
        owned: bool,
        result: Result<Value>,
    ) -> Result<Value> {
        if !owned {
            return result;
        }
        match result {
            Ok(value) => {
                if let Err(error) = self.commit_transaction() {
                    let _ = self.rollback_transaction();
                    return Err(error);
                }
                Ok(value)
            }
            Err(error) => {
                let _ = self.rollback_transaction();
                Err(error)
            }
        }
    }

    pub(crate) fn call_model_reverse_relation(
        &mut self,
        owner: &ResourceContractV1,
        method: &str,
        mut named: BTreeMap<String, Value>,
        scope: ResourceRecordScope,
    ) -> Result<Value> {
        let relation_name = method.strip_prefix("list_").ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application model method `{method}` is not a reverse-relation helper"
            ))
        })?;
        let relation = owner
            .reverse_relations
            .iter()
            .find(|relation| relation.name == relation_name)
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "BicDB application resource `{}` has no signed reverse relation `{relation_name}`",
                    owner.name
                ))
            })?;
        if relation.via_resource.is_some() || relation.via_target_field.is_some() {
            return self.call_model_many_to_many_relation(&relation, method, named, scope);
        }
        let lookup = named.remove(&relation.target_field).ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application `{method}` requires relation field `{}`",
                relation.target_field
            ))
        })?;
        if named.contains_key(&relation.source_field) {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "BicDB application `{method}` cannot override signed relation field `{}`",
                relation.source_field
            )));
        }
        named.insert(relation.source_field.clone(), lookup);
        let target = self
            .resources
            .iter()
            .find(|resource| resource.name == relation.target_resource)
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "BicDB application reverse relation `{}` target `{}` is absent",
                    relation.name, relation.target_resource
                ))
            })?;
        self.call_model_list(
            &target,
            &format!("list_by_{}", relation.source_field),
            named,
            scope,
        )
    }

    pub(crate) fn call_model_many_to_many_relation(
        &mut self,
        relation: &ResourceReverseRelationV1,
        method: &str,
        mut named: BTreeMap<String, Value>,
        scope: ResourceRecordScope,
    ) -> Result<Value> {
        let via_name = relation.via_resource.as_deref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application `{method}` has incomplete signed join-resource metadata"
            ))
        })?;
        let via_target_field = relation.via_target_field.as_deref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application `{method}` has incomplete signed join-target metadata"
            ))
        })?;
        let lookup = named.remove(&relation.target_field).ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application `{method}` requires relation field `{}`",
                relation.target_field
            ))
        })?;
        let page = take_u64(&mut named, "page", 1)?;
        let target = self
            .resources
            .iter()
            .find(|resource| resource.name == relation.target_resource)
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "BicDB application reverse relation `{}` target `{}` is absent",
                    relation.name, relation.target_resource
                ))
            })?;
        let per_page = take_u64(
            &mut named,
            "per_page",
            target
                .list_defaults
                .as_ref()
                .map_or(50, |defaults| u64::from(defaults.limit)),
        )?;
        if page == 0 {
            return Err(AppRuntimeError::InvalidRequest(
                "BicDB application page must be at least 1".to_string(),
            ));
        }
        if per_page == 0 || per_page > 1_000 {
            return Err(AppRuntimeError::InvalidRequest(
                "BicDB application per_page must be 1..=1000".to_string(),
            ));
        }
        let offset = page
            .checked_sub(1)
            .and_then(|page| page.checked_mul(per_page))
            .filter(|offset| *offset <= 10_000_000)
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest(
                    "BicDB application page offset exceeds 10000000".to_string(),
                )
            })?;
        let search = named
            .remove("q")
            .filter(|value| !value.is_null())
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(
                        "BicDB application search q must be a string".to_string(),
                    )
                })
            })
            .transpose()?;
        let sort = named
            .remove("sort")
            .filter(|value| !value.is_null())
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(
                        "BicDB application sort must be a string".to_string(),
                    )
                })
            })
            .transpose()?;
        let descending = named
            .remove("direction")
            .filter(|value| !value.is_null())
            .map(|value| {
                value
                    .as_str()
                    .map(|value| value.eq_ignore_ascii_case("desc"))
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidRequest(
                            "BicDB application direction must be a string".to_string(),
                        )
                    })
            })
            .transpose()?
            .unwrap_or(false);
        if let Some((argument, _)) = named.into_iter().find(|(_, value)| !value.is_null()) {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "BicDB application `{method}` received unexpected argument `{argument}`"
            )));
        }
        let via = self
            .resources
            .iter()
            .find(|resource| resource.name == via_name)
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "BicDB application reverse relation `{}` join resource `{via_name}` is absent",
                    relation.name
                ))
            })?;
        let target_field = via
            .relations
            .iter()
            .find(|candidate| {
                candidate.source_field == via_target_field
                    && candidate.target_resource == target.name
            })
            .map(|candidate| candidate.target_field.clone())
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "BicDB application reverse relation `{}` join target is not signed",
                    relation.name
                ))
            })?;
        let mut request = carrier_resource_request(ResourceOperation::List);
        request.scope = scope;
        request.internal_model_call = true;
        request.limit = u32::try_from(per_page).map_err(|_| {
            AppRuntimeError::InvalidRequest("BicDB application per_page exceeds u32".to_string())
        })?;
        request.offset = offset;
        request.search = search;
        if let Some(field) = sort {
            request.sort.push(SortField { field, descending });
        } else if let Some(defaults) = &target.list_defaults {
            request.sort = defaults.sort.clone();
        }
        authorize_resource(self.host, &target, &request)?;
        validate_resource_request(&target, &request)?;
        let mut via_request = carrier_resource_request(ResourceOperation::List);
        via_request.internal_model_call = true;
        authorize_resource(self.host, &via, &via_request)?;
        validate_resource_request(&via, &via_request)?;
        let query = RelationQuerySpec {
            source_relation: via.relation.clone(),
            source_field: relation.source_field.clone(),
            source_value: lookup,
            join_field: via_target_field.to_string(),
            target_relation: target.relation.clone(),
            target_field,
            target_filters: resource_scope_filters(&target, scope),
            target_sort: request.sort,
            // Let the host resolve an omitted projection to the target
            // relation's signed readable set. Legacy packages without exact
            // column authority continue to resolve this to all fields.
            target_columns: Vec::new(),
            search: request.search.map(|query| RelationSearchSpec {
                index: resource_search_index_name(&target),
                query,
            }),
            limit: request.limit,
            offset,
        };
        let owned_transaction = self.current_transaction().is_none();
        if owned_transaction {
            self.begin_transaction("read_committed")?;
        }
        let result = (|| {
            let transaction = self.current_transaction().ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "BicDB application `{method}` relation transaction is absent"
                ))
            })?;
            let total = expect_carrier_u64(self.host_call(HostRequest::Database(
                DatabaseRequest::RelationAggregate {
                    transaction,
                    query: query.clone(),
                    aggregate: AggregateSpec::Count,
                },
            ))?)?;
            let rows = expect_carrier_rows(self.host_call(HostRequest::Database(
                DatabaseRequest::RelationQuery { transaction, query },
            ))?)?;
            let items = rows
                .into_iter()
                .map(|row| redact_resource(self.host, &target, row))
                .collect::<Result<Vec<_>>>()?;
            Ok(json!({
                "items": items,
                "page_info": {
                    "page": page,
                    "per_page": per_page,
                    "total": total,
                }
            }))
        })();
        if owned_transaction {
            match result {
                Ok(value) => {
                    if let Err(error) = self.commit_transaction() {
                        let _ = self.rollback_transaction();
                        return Err(error);
                    }
                    Ok(value)
                }
                Err(error) => {
                    let _ = self.rollback_transaction();
                    Err(error)
                }
            }
        } else {
            result
        }
    }

    pub(crate) fn call_model_list(
        &mut self,
        contract: &ResourceContractV1,
        method: &str,
        mut named: BTreeMap<String, Value>,
        scope: ResourceRecordScope,
    ) -> Result<Value> {
        let page = take_u64(&mut named, "page", 1)?;
        let per_page = take_u64(
            &mut named,
            "per_page",
            contract
                .list_defaults
                .as_ref()
                .map_or(50, |defaults| u64::from(defaults.limit)),
        )?;
        if page == 0 {
            return Err(AppRuntimeError::InvalidRequest(
                "BicDB application page must be at least 1".to_string(),
            ));
        }
        if per_page == 0 || per_page > 1_000 {
            return Err(AppRuntimeError::InvalidRequest(
                "BicDB application per_page must be 1..=1000".to_string(),
            ));
        }
        let offset = page
            .checked_sub(1)
            .and_then(|page| page.checked_mul(per_page))
            .filter(|offset| *offset <= 10_000_000)
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest(
                    "BicDB application page offset exceeds 10000000".to_string(),
                )
            })?;
        let mut request = carrier_resource_request(ResourceOperation::List);
        request.scope = scope;
        request.include_total = true;
        request.internal_model_call = true;
        request.limit = u32::try_from(per_page).map_err(|_| {
            AppRuntimeError::InvalidRequest("BicDB application per_page exceeds u32".to_string())
        })?;
        request.offset = offset;
        request.search = named
            .remove("q")
            .filter(|value| !value.is_null())
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(
                        "BicDB application search q must be a string".to_string(),
                    )
                })
            })
            .transpose()?;
        let sort = named
            .remove("sort")
            .filter(|value| !value.is_null())
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(
                        "BicDB application sort must be a string".to_string(),
                    )
                })
            })
            .transpose()?;
        let descending = named
            .remove("direction")
            .filter(|value| !value.is_null())
            .map(|value| {
                value
                    .as_str()
                    .map(|value| value.eq_ignore_ascii_case("desc"))
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidRequest(
                            "BicDB application direction must be a string".to_string(),
                        )
                    })
            })
            .transpose()?
            .unwrap_or(false);
        if let Some(field) = sort {
            request.sort.push(SortField { field, descending });
        } else if let Some(defaults) = &contract.list_defaults {
            request.sort = defaults.sort.clone();
        }
        let list_by = method.strip_prefix("list_by_");
        if list_by.is_none() && !contract.filter_contracts.is_empty() {
            for filter in &contract.filter_contracts {
                match &filter.filter {
                    ResourceFilterKind::Exact { field } => {
                        if let Some(value) = named.remove(&filter.query_name) {
                            if !value.is_null() {
                                request.filters.push(FilterExpression::Eq {
                                    field: field.clone(),
                                    value,
                                });
                            }
                        }
                    }
                    ResourceFilterKind::Contains { field } => {
                        if let Some(value) = named.remove(&filter.query_name) {
                            if !value.is_null() {
                                let value = value.as_str().ok_or_else(|| {
                                    AppRuntimeError::InvalidRequest(format!(
                                        "BicDB application filter `{}` requires a string",
                                        filter.query_name
                                    ))
                                })?;
                                request.filters.push(FilterExpression::Contains {
                                    field: field.clone(),
                                    value: value.to_string(),
                                });
                            }
                        }
                    }
                    ResourceFilterKind::Minimum { field } => {
                        if let Some(value) = named.remove(&filter.query_name) {
                            if !value.is_null() {
                                request.filters.push(FilterExpression::Ge {
                                    field: field.clone(),
                                    value,
                                });
                            }
                        }
                    }
                    ResourceFilterKind::Exists { field } => {
                        if let Some(value) = named.remove(&filter.query_name) {
                            if !value.is_null() {
                                let exists = value.as_bool().ok_or_else(|| {
                                    AppRuntimeError::InvalidRequest(format!(
                                        "BicDB application filter `{}` requires a Bool",
                                        filter.query_name
                                    ))
                                })?;
                                request.filters.push(if exists {
                                    FilterExpression::Ne {
                                        field: field.clone(),
                                        value: Value::Null,
                                    }
                                } else {
                                    FilterExpression::Eq {
                                        field: field.clone(),
                                        value: Value::Null,
                                    }
                                });
                            }
                        }
                    }
                    ResourceFilterKind::Overlaps {
                        start_field,
                        end_field,
                    } => {
                        let start_name = format!("{}_start", filter.query_name);
                        let end_name = format!("{}_end", filter.query_name);
                        match (named.remove(&start_name), named.remove(&end_name)) {
                            (Some(start), Some(end)) if !start.is_null() && !end.is_null() => {
                                request.filters.extend([
                                    FilterExpression::Lt {
                                        field: start_field.clone(),
                                        value: end,
                                    },
                                    FilterExpression::Gt {
                                        field: end_field.clone(),
                                        value: start,
                                    },
                                ]);
                            }
                            (None, None) | (Some(Value::Null), Some(Value::Null)) => {}
                            _ => {
                                return Err(AppRuntimeError::InvalidRequest(format!(
                                    "BicDB application overlap filter `{}` requires both bounds",
                                    filter.query_name
                                )));
                            }
                        }
                    }
                    ResourceFilterKind::JsonExact {
                        field,
                        path,
                        value_type,
                    } => {
                        if let Some(value) = named.remove(&filter.query_name) {
                            if !value.is_null() {
                                request.filters.push(FilterExpression::JsonPathEq {
                                    field: field.clone(),
                                    path: path.clone(),
                                    value,
                                    value_type: Some(value_type.clone()),
                                });
                            }
                        }
                    }
                    ResourceFilterKind::JsonContains {
                        field, path, array, ..
                    } => {
                        if let Some(value) = named.remove(&filter.query_name) {
                            if !value.is_null() {
                                let value = value.as_str().ok_or_else(|| {
                                    AppRuntimeError::InvalidRequest(format!(
                                        "BicDB application filter `{}` requires a string",
                                        filter.query_name
                                    ))
                                })?;
                                request.filters.push(FilterExpression::JsonPathContains {
                                    field: field.clone(),
                                    path: path.clone(),
                                    value: value.to_string(),
                                    array: *array,
                                });
                            }
                        }
                    }
                    ResourceFilterKind::JsonMinimum {
                        field,
                        path,
                        value_type,
                    } => {
                        if let Some(value) = named.remove(&filter.query_name) {
                            if !value.is_null() {
                                request.filters.push(FilterExpression::JsonPathGe {
                                    field: field.clone(),
                                    path: path.clone(),
                                    value,
                                    value_type: value_type.clone(),
                                });
                            }
                        }
                    }
                    ResourceFilterKind::JsonExists { field, path } => {
                        if let Some(value) = named.remove(&filter.query_name) {
                            if !value.is_null() {
                                let exists = value.as_bool().ok_or_else(|| {
                                    AppRuntimeError::InvalidRequest(format!(
                                        "BicDB application filter `{}` requires a Bool",
                                        filter.query_name
                                    ))
                                })?;
                                request.filters.push(FilterExpression::JsonPathExists {
                                    field: field.clone(),
                                    path: path.clone(),
                                    exists,
                                });
                            }
                        }
                    }
                    ResourceFilterKind::RelationExact { .. }
                    | ResourceFilterKind::RelationContains { .. }
                    | ResourceFilterKind::RelationMinimum { .. } => {
                        if let Some(value) = named.remove(&filter.query_name) {
                            if !value.is_null() {
                                request
                                    .relation_filters
                                    .insert(filter.query_name.clone(), value);
                            }
                        }
                    }
                }
            }
        }
        for (field, value) in named {
            if value.is_null() {
                continue;
            }
            if list_by.is_some_and(|expected| expected != field) {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "BicDB application `{method}` received unexpected filter `{field}`"
                )));
            }
            request.filters.push(FilterExpression::Eq { field, value });
        }
        let response = match self.current_transaction() {
            Some(transaction) => execute_resource_operation_in_transaction(
                self.host,
                contract,
                request,
                transaction,
            )?,
            None => execute_resource_operation(self.host, contract, request)?,
        };
        let total = carrier_resource_total(&response)?;
        carrier_model_list_value(response.body, page, per_page, total)
    }

    pub(crate) fn call_model_count(
        &mut self,
        contract: &ResourceContractV1,
        method: &str,
        mut named: BTreeMap<String, Value>,
        scope: ResourceRecordScope,
    ) -> Result<Value> {
        let returns_exists = method == "exists" || method.starts_with("exists_by_");
        let expected_field = if method == "count" {
            None
        } else if method == "exists" {
            Some("id")
        } else {
            method
                .strip_prefix("count_by_")
                .or_else(|| method.strip_prefix("exists_by_"))
        };
        let filter = match expected_field {
            Some(expected) => {
                if named.len() != 1 || !named.contains_key(expected) {
                    return Err(AppRuntimeError::InvalidPackage(format!(
                        "BicDB application model call `{method}` requires lookup field `{expected}`"
                    )));
                }
                let value = named.remove(expected).expect("checked lookup field");
                Some(FilterExpression::Eq {
                    field: expected.to_string(),
                    value,
                })
            }
            None if named.is_empty() => None,
            None => {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "BicDB application model call `{method}` does not accept lookup fields"
                )))
            }
        };
        let mut request = carrier_resource_request(ResourceOperation::List);
        request.scope = scope;
        request.limit = 1;
        request.include_total = true;
        request.internal_model_call = true;
        if let Some(filter) = filter {
            request.filters.push(filter);
        }
        let response = match self.current_transaction() {
            Some(transaction) => execute_resource_operation_in_transaction(
                self.host,
                contract,
                request,
                transaction,
            )?,
            None => execute_resource_operation(self.host, contract, request)?,
        };
        let total = carrier_resource_total(&response)?;
        if returns_exists {
            Ok(Value::Bool(total != 0))
        } else {
            Ok(Value::from(i64::try_from(total).map_err(|_| {
                AppRuntimeError::ResourceExhausted(
                    "BicDB application model count exceeds signed Int range".to_string(),
                )
            })?))
        }
    }

    pub(crate) fn call_model_upsert(
        &mut self,
        contract: &ResourceContractV1,
        method: &str,
        mut named: BTreeMap<String, Value>,
    ) -> Result<Value> {
        let target_name = method.strip_prefix("upsert_by_").ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application model method `{method}` is not an upsert helper"
            ))
        })?;
        let create = named.remove("create").ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application model call `{method}` requires `create`"
            ))
        })?;
        let update = named.remove("update").ok_or_else(|| {
            AppRuntimeError::InvalidPackage(format!(
                "BicDB application model call `{method}` requires `update`"
            ))
        })?;
        if !named.is_empty() {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "BicDB application model call `{method}` contains unexpected arguments"
            )));
        }
        if self.current_transaction().is_some() {
            return self.call_model_upsert_values(contract, target_name, create, update);
        }

        // PostgreSQL ON CONFLICT resolves two concurrent inserts of the same
        // unique key by making the loser re-evaluate the update branch. BicDB's
        // native equivalent is an owning-transaction retry: the failed attempt
        // is already fully rolled back, and a fresh snapshot observes the winner.
        const MAX_UPSERT_ATTEMPTS: u32 = 64;
        for attempt in 0..MAX_UPSERT_ATTEMPTS {
            self.begin_transaction("read_committed")?;
            let result = self.call_model_upsert_values(
                contract,
                target_name,
                create.clone(),
                update.clone(),
            );
            let result = match result {
                Ok(value) => self.commit_transaction().map(|()| value),
                Err(error) => {
                    let _ = self.rollback_transaction();
                    Err(error)
                }
            };
            match result {
                Ok(value) => return Ok(value),
                Err(AppRuntimeError::Conflict(_)) if attempt + 1 < MAX_UPSERT_ATTEMPTS => {
                    let delay_ms = 1_u64 << attempt.min(5);
                    self.sleep(Duration::from_millis(delay_ms))?;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("bounded upsert retry loop always returns")
    }

    pub(crate) fn call_model_upsert_values(
        &mut self,
        contract: &ResourceContractV1,
        target_name: &str,
        create: Value,
        update: Value,
    ) -> Result<Value> {
        let target = contract
            .unique_targets
            .iter()
            .find(|target| target.target == target_name)
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "BicDB application resource `{}` has no signed unique target `{target_name}`",
                    contract.name
                ))
            })?;
        let create = create.as_object().cloned().ok_or_else(|| {
            AppRuntimeError::InvalidRequest(format!(
                "BicDB application `{}` upsert create value must be an object",
                contract.name
            ))
        })?;
        let mut update = update.as_object().cloned().ok_or_else(|| {
            AppRuntimeError::InvalidRequest(format!(
                "BicDB application `{}` upsert update value must be an object",
                contract.name
            ))
        })?;
        remove_immutable_upsert_fields(
            &mut update,
            &contract.primary_key,
            &contract.immutable_fields,
        );
        update.retain(|_, value| !value.is_null());
        let conflict_values = target
            .fields
            .iter()
            .map(|field| {
                create.get(field).cloned().ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(format!(
                        "BicDB application `{}` upsert create value lacks conflict field `{field}`",
                        contract.name
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let existing = if conflict_values.iter().any(Value::is_null) {
            None
        } else {
            let mut lookup = carrier_resource_request(ResourceOperation::List);
            lookup.scope = ResourceRecordScope::All;
            lookup.internal_model_call = true;
            lookup.limit = 2;
            lookup.filters = target
                .fields
                .iter()
                .cloned()
                .zip(conflict_values)
                .map(|(field, value)| FilterExpression::Eq { field, value })
                .collect();
            let transaction = self.current_transaction().ok_or_else(|| {
                AppRuntimeError::InvalidPackage(
                    "BicDB application upsert lookup requires a transaction".to_string(),
                )
            })?;
            let response = execute_resource_operation_in_transaction(
                self.host,
                contract,
                lookup,
                transaction,
            )?;
            let rows = response.body.as_array().ok_or_else(|| {
                AppRuntimeError::InvalidPackage(
                    "BicDB application upsert lookup did not return an array".to_string(),
                )
            })?;
            if rows.len() > 1 {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "BicDB application unique target `{}.{target_name}` matched multiple records",
                    contract.name
                )));
            }
            rows.first().cloned()
        };

        let mut request = carrier_resource_request(ResourceOperation::Upsert);
        request.scope = ResourceRecordScope::All;
        request.internal_model_call = true;
        if let Some(existing) = existing {
            request.id = Some(carrier_scalar_id(
                existing
                    .get(&contract.primary_key)
                    .cloned()
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidPackage(format!(
                            "BicDB application upsert result lacks primary key `{}`",
                            contract.primary_key
                        ))
                    })?,
                &contract.name,
                "upsert",
            )?);
            request.expected_version = contract
                .version_field
                .as_ref()
                .and_then(|field| existing.get(field))
                .and_then(Value::as_u64);
            request.body = Value::Object(update);
        } else {
            request.body = Value::Object(create);
        }
        self.execute_model_mutation(contract, request)
            .map(|response| response.body)
    }

    pub(crate) fn call_model_bulk(
        &mut self,
        contract: &ResourceContractV1,
        method: &str,
        mut positional: Vec<Value>,
        mut named: BTreeMap<String, Value>,
    ) -> Result<Value> {
        if self.current_transaction().is_none() {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "BicDB application `{method}` requires an enclosing transaction"
            )));
        }
        let records = match (positional.len(), named.remove("records")) {
            (1, None) => positional.pop().expect("one positional record batch"),
            (0, Some(records)) => records,
            _ => {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "BicDB application `{method}` requires exactly one records argument"
                )))
            }
        };
        let target = if method == "bulk_upsert" {
            Some(
                named
                    .remove("conflict")
                    .and_then(|value| value.as_str().map(str::to_string))
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidPackage(
                            "BicDB application bulk_upsert requires a literal conflict target"
                                .to_string(),
                        )
                    })?,
            )
        } else {
            None
        };
        if !named.is_empty() {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "BicDB application `{method}` contains unexpected arguments"
            )));
        }
        let records = records.as_array().cloned().ok_or_else(|| {
            AppRuntimeError::InvalidRequest(format!(
                "BicDB application `{method}` records value must be an array"
            ))
        })?;
        if records.len() > 10_000 {
            return Err(AppRuntimeError::ResourceExhausted(format!(
                "BicDB application `{method}` exceeds 10000 records"
            )));
        }
        let mut count = 0_i64;
        for record in records {
            if let Some(target) = target.as_deref() {
                let mut update = record.clone();
                if let Some(update) = update.as_object_mut() {
                    update.remove(&contract.primary_key);
                }
                self.call_model_upsert_values(contract, target, record, update)?;
            } else {
                let mut request = carrier_resource_request(ResourceOperation::Create);
                request.body = record;
                request.internal_model_call = true;
                self.execute_model_mutation(contract, request)?;
            }
            count = count.checked_add(1).ok_or_else(|| {
                AppRuntimeError::ResourceExhausted(
                    "BicDB application bulk mutation count exceeds signed Int range".to_string(),
                )
            })?;
        }
        Ok(Value::from(count))
    }

    pub(crate) fn execute_model_mutation(
        &mut self,
        contract: &ResourceContractV1,
        request: ResourceRequest,
    ) -> Result<ResourceResponse> {
        let transaction = self.current_transaction().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "BicDB application model mutation requires a transaction".to_string(),
            )
        })?;
        execute_resource_operation_in_transaction_with_mutation_hook(
            self.host,
            contract,
            request,
            transaction,
            |host, transaction, mutation| {
                execute_carrier_mutation_bindings(host, transaction, contract, mutation)
            },
        )
    }

    pub(crate) fn call_service(
        &mut self,
        alias: &str,
        arguments: Vec<(Option<String>, Value)>,
    ) -> Result<Value> {
        let binding = self.service_bindings.get(alias).cloned().ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(format!(
                "BicDB application action `{alias}` has no signed local callable or service binding"
            ))
        })?;
        let mut payload = serde_json::Map::new();
        let mut positional = arguments
            .iter()
            .filter(|(name, _)| name.is_none())
            .map(|(_, value)| value.clone());
        for parameter in &binding.parameters {
            let value = arguments
                .iter()
                .find(|(name, _)| name.as_deref() == Some(parameter))
                .map(|(_, value)| value.clone())
                .or_else(|| positional.next())
                .ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(format!(
                        "BicDB application service action `{alias}` is missing argument `{parameter}`"
                    ))
                })?;
            payload.insert(parameter.clone(), value);
        }
        if arguments.len() != binding.parameters.len() {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "BicDB application service action `{alias}` received an unexpected argument"
            )));
        }
        let transaction = binding
            .propagate_transaction
            .then(|| self.current_transaction())
            .flatten();
        let deadline_unix_ms = self.host.actor().deadline_unix_ms;
        match self.host_call(HostRequest::Service(ServiceRequest::Call {
            dependency: binding.dependency,
            service: binding.service,
            method: binding.method,
            payload: Value::Object(payload),
            transaction,
            deadline_unix_ms,
        }))? {
            HostValue::Json(value) => Ok(value),
            other => Err(AppRuntimeError::Invocation(format!(
                "BicDB application service action `{alias}` returned {other:?}, expected JSON"
            ))),
        }
    }

    pub(crate) fn call_queue(
        &mut self,
        queue: &str,
        method: &str,
        arguments: Vec<(Option<String>, Value)>,
    ) -> Result<Value> {
        let message = method.strip_prefix("publish_").ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(format!(
                "BicDB application queue method `{queue}.{method}` is unsupported"
            ))
        })?;
        if arguments.len() != 1 || arguments[0].0.is_some() {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "BicDB application queue publish `{queue}.{method}` requires one positional payload"
            )));
        }
        let payload = arguments[0].1.clone();
        let headers = BTreeMap::from([
            ("carrier_message".to_string(), message.to_string()),
            ("carrier_queue".to_string(), queue.to_string()),
        ]);
        let request = match self.current_transaction() {
            Some(transaction) => BrokerRequest::PublishOnCommit {
                transaction,
                queue: queue.to_string(),
                payload,
                headers,
                idempotency_key: None,
                delay_ms: None,
            },
            None => BrokerRequest::Publish {
                queue: queue.to_string(),
                payload,
                headers,
                idempotency_key: None,
                delay_ms: None,
            },
        };
        match self.host_call(HostRequest::Broker(request))? {
            HostValue::String(_) => Ok(Value::Null),
            other => {
                carrier_host_type_error(&format!("{queue}.{method}"), "message receipt", other)
            }
        }
    }

    pub(crate) fn call_client(
        &mut self,
        client: &str,
        method: &str,
        arguments: Vec<(Option<String>, Value)>,
    ) -> Result<Value> {
        let grpc_method = self
            .host
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.grpc.as_ref())
            .and_then(|grpc| grpc.clients.get(client))
            .and_then(|client| client.methods.get(method))
            .cloned();
        if let Some(grpc_method) = grpc_method {
            if arguments.len() != 1 {
                return Err(AppRuntimeError::InvalidRequest(format!(
                    "BicDB application gRPC call `{client}.{method}` requires one request object"
                )));
            }
            let payload = arguments.into_iter().next().expect("one gRPC argument").1;
            if !payload.is_object() {
                return Err(AppRuntimeError::InvalidRequest(format!(
                    "BicDB application gRPC call `{client}.{method}` requires an object payload"
                )));
            }
            let deadline_unix_ms =
                self.host
                    .actor()
                    .deadline_unix_ms
                    .min(crate::host::now_ms().saturating_add(
                        i64::try_from(grpc_method.deadline_ms).unwrap_or(i64::MAX),
                    ));
            return match self.host_call(HostRequest::Grpc(GrpcRequest::Unary {
                client: client.to_string(),
                method: method.to_string(),
                payload,
                deadline_unix_ms,
            }))? {
                HostValue::Json(value) => Ok(value),
                other => carrier_host_type_error(client, "gRPC response", other),
            };
        }
        let binding = self.client_bindings.get(client).cloned().ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(format!(
                "BicDB application client `{client}` has no signed egress binding"
            ))
        })?;
        let http_method = match method {
            "get" => "GET",
            "post" => "POST",
            _ => {
                return Err(AppRuntimeError::CapabilityDenied(format!(
                    "BicDB application client method `{client}.{method}` requires a plugin service binding"
                )))
            }
        };
        let mut positional = arguments
            .iter()
            .filter(|(name, _)| name.is_none())
            .map(|(_, value)| value.clone());
        let path = arguments
            .iter()
            .find(|(name, _)| name.as_deref() == Some("path"))
            .map(|(_, value)| value.clone())
            .or_else(|| positional.next())
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest(format!(
                    "BicDB application client call `{client}.{method}` requires a path"
                ))
            })?;
        let path = carrier_string(path, &format!("{client}.{method}"))?;
        let query = arguments
            .iter()
            .find(|(name, _)| name.as_deref() == Some("query"))
            .map(|(_, value)| value.clone());
        let body = arguments
            .iter()
            .find(|(name, _)| name.as_deref() == Some("body"))
            .map(|(_, value)| value.clone());
        for (name, _) in &arguments {
            if name
                .as_deref()
                .is_some_and(|name| !matches!(name, "path" | "query" | "body"))
            {
                return Err(AppRuntimeError::InvalidRequest(format!(
                    "BicDB application client call `{client}.{method}` has an unsupported argument"
                )));
            }
        }
        if positional.next().is_some() {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "BicDB application client call `{client}.{method}` has an unexpected positional argument"
            )));
        }
        if method == "get" && body.is_some() {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "BicDB application GET client `{client}` cannot send a body"
            )));
        }
        let url = if binding.base_url.is_empty() {
            carrier_client_relative_url(client, &path, query)?
        } else {
            carrier_client_url(client, &binding.base_url, &path, query)?
        };
        let mut headers = binding.headers.into_iter().collect::<Vec<_>>();
        let mut host_header = |name: &str, value: String| {
            headers.retain(|(candidate, _)| !candidate.eq_ignore_ascii_case(name));
            headers.push((name.to_string(), value));
        };
        host_header("x-carrier-trace-id", self.host.actor().trace_id.clone());
        host_header("traceparent", carrier_traceparent(self.host.actor()));
        if let Some(value) = self.host.actor().policy_attributes.get("w3c.tracestate") {
            host_header("tracestate", value.clone());
        }
        if let Some(value) = &self.host.actor().correlation_id {
            host_header("x-correlation-id", value.clone());
        }
        if let Some(value) = &self.host.actor().causation_id {
            host_header("x-causation-id", value.clone());
        }
        let body = match body {
            Some(body) => {
                if !headers
                    .iter()
                    .any(|(name, _)| name.eq_ignore_ascii_case("content-type"))
                {
                    headers.push(("content-type".to_string(), "application/json".to_string()));
                }
                serde_json::to_vec(&body)?
            }
            None => Vec::new(),
        };
        let deadline_unix_ms = self.host.actor().deadline_unix_ms.min(
            crate::host::now_ms()
                .saturating_add(i64::try_from(binding.timeout_ms).unwrap_or(i64::MAX)),
        );
        let response = match self.host_call(HostRequest::Egress(EgressRequest::Http {
            policy: binding.policy,
            method: http_method.to_string(),
            url,
            headers,
            body,
            deadline_unix_ms,
        }))? {
            HostValue::EgressResponse(value) => value,
            other => return carrier_host_type_error(client, "HTTP response", other),
        };
        if !(200..300).contains(&response.status) {
            return Err(AppRuntimeError::Invocation(format!(
                "BicDB application client `{client}` upstream returned HTTP {}",
                response.status
            )));
        }
        if response.body.is_empty() {
            Ok(Value::Null)
        } else {
            serde_json::from_slice(&response.body).map_err(|error| {
                AppRuntimeError::Invocation(format!(
                    "BicDB application client `{client}` returned invalid JSON: {error}"
                ))
            })
        }
    }

    pub(crate) fn call_builtin(
        &mut self,
        target: &str,
        arguments: Vec<(Option<String>, Value)>,
    ) -> Result<Value> {
        match target {
            "http.request" | "http.request_as" => self.test_http.ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(
                    "BicDB application authored HTTP requests are available only in the trusted test runner"
                        .to_string(),
                )
            })?(target, arguments),
            "encrypt" => self.field_crypto(target, &arguments, true),
            "decrypt" => self.field_crypto(target, &arguments, false),
            "auth.current_user" => self.current_user(),
            "auth.register" => self.auth_register(target, &arguments),
            "auth.login" => self.auth_login(target, &arguments),
            "auth.issue_tokens" => self.auth_issue_tokens(target, &arguments),
            "auth.password_policy" => self.auth_password_policy(target, &arguments),
            "auth.password_hash" => self.auth_password_hash(target, &arguments),
            "auth.password_verify" => self.auth_password_verify(target, &arguments),
            "auth.password_breach_digest" => self.auth_password_breach_digest(target, &arguments),
            "auth.totp_secret" => self.auth_totp_secret(target, &arguments),
            "auth.totp_code" => self.auth_totp_code(target, &arguments),
            "auth.totp_verify" => self.auth_totp_verify(target, &arguments),
            "auth.totp_uri" => self.auth_totp_uri(target, &arguments),
            "auth.magic_link_issue" => self.auth_magic_link_issue(target, &arguments),
            "auth.magic_link_verify" => self.auth_magic_link_verify(target, &arguments),
            "auth.oauth_authorize" => self.auth_oauth_authorize(target, &arguments),
            "auth.oauth_callback" => self.auth_oauth_callback(target, &arguments),
            "blob.put" => self.blob_put(target, &arguments),
            "blob.get" => self.blob_get(target, &arguments),
            "blob.signed_url" => self.blob_signed_url(target, &arguments),
            "blob.metadata" => self.blob_metadata_builtin(target, &arguments),
            "blob.hash" => self.blob_hash(target, &arguments),
            "blob.content_type" => self.blob_content_type(target, &arguments),
            "redis.publish" | "redis.incr" => self.redis_builtin(target, &arguments),
            "email.send" => self.email_builtin(target, &arguments),
            "time.now" => self.wall_time(false),
            "time.today" => self.wall_time(true),
            "uuid.new" => {
                let mut bytes: [u8; 16] =
                    self.random_bytes(target, 16)?.try_into().map_err(|_| {
                        AppRuntimeError::Invocation(
                            "BicDB random host returned the wrong UUID byte count".to_string(),
                        )
                    })?;
                bytes[6] = (bytes[6] & 0x0f) | 0x40;
                bytes[8] = (bytes[8] & 0x3f) | 0x80;
                Ok(Value::String(uuid::Uuid::from_bytes(bytes).to_string()))
            }
            "uuid.v7" => {
                let milliseconds = u64::try_from(self.wall_time_milliseconds()?).map_err(|_| {
                    AppRuntimeError::Invocation(
                        "BicDB clock returned a pre-epoch timestamp for uuid.v7".to_string(),
                    )
                })?;
                let random: [u8; 10] = self.random_bytes(target, 10)?.try_into().map_err(|_| {
                    AppRuntimeError::Invocation(
                        "BicDB random host returned the wrong UUID byte count".to_string(),
                    )
                })?;
                Ok(Value::String(
                    uuid::Builder::from_unix_timestamp_millis(milliseconds, &random)
                        .into_uuid()
                        .to_string(),
                ))
            }
            "id.token" => self.random_token(target, 32),
            "id.opaque" => {
                let len =
                    carrier_positive_size(carrier_argument(&arguments, 0, None, target)?, target)?;
                self.random_token(target, len)
            }
            "id.nanoid" => {
                let len =
                    carrier_positive_size(carrier_argument(&arguments, 0, None, target)?, target)?;
                let bytes = self.random_bytes(target, len)?;
                const ALPHABET: &[u8; 64] =
                    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz-";
                Ok(Value::String(
                    bytes
                        .into_iter()
                        .map(|byte| ALPHABET[usize::from(byte) % ALPHABET.len()] as char)
                        .collect(),
                ))
            }
            "crypto.random_bytes" => {
                let len =
                    carrier_positive_size(carrier_argument(&arguments, 0, None, target)?, target)?;
                Ok(Value::String(carrier_hex(&self.random_bytes(target, len)?)))
            }
            "crypto.token" => {
                let len = match arguments.first() {
                    Some(_) => carrier_positive_size(
                        carrier_argument(&arguments, 0, Some("bytes"), target)?,
                        target,
                    )?,
                    None => 32,
                };
                self.random_token(target, len)
            }
            "random.int" => self.random_int(target, &arguments),
            "random.float" => self.random_float(target, &arguments),
            "random.choice" => self.random_choice(target, &arguments),
            "logs.debug" | "logs.info" | "logs.warn" | "logs.error" => {
                self.observe_log(target, &arguments)
            }
            "trace.annotate" => self.observe_trace(target, &arguments),
            "metrics.counter" => Ok(json!({
                "kind": "counter",
                "name": carrier_string(carrier_argument(&arguments, 0, None, target)?, target)?,
            })),
            "metrics.gauge" => Ok(json!({
                "kind": "gauge",
                "name": carrier_string(carrier_argument(&arguments, 0, None, target)?, target)?,
            })),
            "metrics.counter.increment" => {
                self.observe_metric(target, MetricKind::Counter, &arguments)
            }
            "metrics.gauge.record" => self.observe_metric(target, MetricKind::Gauge, &arguments),
            "audit.record" => self.observe_audit(target, &arguments),
            "flags.enabled" => self.feature_flag_enabled(target, &arguments),
            "jobs.enqueue" => self.enqueue_job(target, &arguments),
            "cache.get_as" => self.runtime_cache_get(target, &arguments),
            "cache.set" => self.runtime_cache_set(target, &arguments),
            "cache.delete" => self.runtime_cache_delete(target, &arguments),
            "cache.exists" => self.runtime_cache_exists(target, &arguments),
            "tokenizer.count" => self.tokenizer_count(target, &arguments),
            "embeddings.embed" => self.embeddings_embed(target, &arguments),
            "sql.list_as" => self.call_declared_sql(target, &arguments, DeclaredSqlResult::List),
            "db.graph_list_as" => {
                self.call_declared_sql(target, &arguments, DeclaredSqlResult::List)
            }
            "sql.one_as" | "db.call_as" | "db.fn_one_as" => {
                self.call_declared_sql(target, &arguments, DeclaredSqlResult::One)
            }
            "db.graph_one_as" => self.call_declared_sql(target, &arguments, DeclaredSqlResult::One),
            "sql.scalar_as" | "db.fn_scalar_as" | "db.native_function" => {
                self.call_declared_sql(target, &arguments, DeclaredSqlResult::Scalar)
            }
            "sql.exec" => self.call_declared_sql(target, &arguments, DeclaredSqlResult::Affected),
            "workflows.start" => self.start_workflow(&arguments),
            "workflows.status" => self.workflow_status(&arguments),
            "workflows.cancel" => self.cancel_workflow(&arguments),
            "workflows.signal" => self.signal_workflow(&arguments),
            "workflows.evidence" => self.workflow_evidence(&arguments),
            "workflows.retry_compensation" => self.retry_workflow_compensation(&arguments),
            "workflows.result_as" => self.workflow_result(&arguments),
            _ => Err(AppRuntimeError::CapabilityDenied(format!(
                "BicDB application builtin `{target}` has no declared capability binding"
            ))),
        }
    }

    pub(crate) fn tokenizer_count(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        if arguments.len() != 1 || arguments[0].0.is_some() {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "builtin `{target}` requires exactly one positional string argument"
            )));
        }
        let text = carrier_string(arguments[0].1.clone(), target)?;
        let tokens = expect_carrier_u64(self.host_call(HostRequest::Tokenizer(
            TokenizerRequest::Count {
                provider: "default".to_string(),
                text,
            },
        ))?)?;
        let tokens = i64::try_from(tokens).map_err(|_| {
            AppRuntimeError::Provider(
                "tokenizer count exceeds BicDB application Int range".to_string(),
            )
        })?;
        Ok(Value::from(tokens))
    }

    pub(crate) fn embeddings_embed(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        if arguments.len() != 1 || arguments[0].0.is_some() {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "builtin `{target}` requires exactly one positional string argument"
            )));
        }
        let text = carrier_string(arguments[0].1.clone(), target)?;
        let dimensions = self
            .host
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.embeddings.as_ref())
            .and_then(|contract| contract.providers.get("default"))
            .map(|provider| provider.dimensions)
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(
                    "default embedding provider lacks exact signed authority".to_string(),
                )
            })?;
        match self.host_call(HostRequest::Embeddings(EmbeddingsRequest::Embed {
            provider: "default".to_string(),
            text,
            dimensions,
        }))? {
            HostValue::Json(value) => Ok(value),
            other => Err(AppRuntimeError::Invocation(format!(
                "BicDB application embedding host returned {other:?}, expected JSON"
            ))),
        }
    }

    pub(crate) fn call_llm(
        &mut self,
        client: &str,
        method: &str,
        arguments: Vec<(Option<String>, Value)>,
    ) -> Result<Value> {
        if arguments.iter().any(|(name, _)| name.is_some())
            || !matches!(
                (method, arguments.len()),
                ("respond" | "stream" | "stream_response", 1) | ("respond_as", 2)
            )
        {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "LLM call `{client}.{method}` does not match its signed positional shape"
            )));
        }
        let (output_type, payload) = if method == "respond_as" {
            (
                Some(carrier_string(arguments[0].1.clone(), method)?),
                arguments[1].1.clone(),
            )
        } else {
            (None, arguments[0].1.clone())
        };
        let payload = payload.as_object().ok_or_else(|| {
            AppRuntimeError::InvalidRequest("LLM request payload must be an object".to_string())
        })?;
        if payload.len() > 7
            || payload.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "user_prompt"
                        | "conversation_id"
                        | "__carrier_allowed_tools"
                        | "__carrier_max_turns"
                        | "__carrier_max_tool_calls"
                        | "__carrier_budget_tokens"
                        | "__carrier_deny_tools_after_output"
                )
            })
        {
            return Err(AppRuntimeError::InvalidRequest(
                "LLM request payload contains an unsupported field".to_string(),
            ));
        }
        let user_prompt = payload
            .get("user_prompt")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest(
                    "LLM request payload requires a non-empty user_prompt".to_string(),
                )
            })?
            .to_string();
        let conversation_id = match payload.get("conversation_id") {
            None | Some(Value::Null) => uuid::Uuid::new_v4().to_string(),
            Some(Value::String(value)) if !value.is_empty() && value.len() <= 256 => value.clone(),
            _ => {
                return Err(AppRuntimeError::InvalidRequest(
                    "LLM conversation_id must be a non-empty bounded string".to_string(),
                ));
            }
        };
        let contract = self
            .host
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.llm.as_ref())
            .and_then(|contract| contract.clients.get(client))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "LLM client `{client}` lacks exact signed authority"
                ))
            })?;
        let allowed_tools = payload
            .get("__carrier_allowed_tools")
            .map(|value| {
                value
                    .as_array()
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidPackage(
                            "agent tool filter must be an array".to_string(),
                        )
                    })?
                    .iter()
                    .map(|value| {
                        value.as_str().map(str::to_string).ok_or_else(|| {
                            AppRuntimeError::InvalidPackage(
                                "agent tool filter contains a non-string".to_string(),
                            )
                        })
                    })
                    .collect::<Result<BTreeSet<_>>>()
            })
            .transpose()?;
        if allowed_tools.as_ref().is_some_and(|allowed| {
            allowed
                .iter()
                .any(|name| !contract.tools.contains_key(name))
        }) {
            return Err(AppRuntimeError::CapabilityDenied(
                "agent tool filter exceeds signed LLM authority".to_string(),
            ));
        }
        let allowed_tools = Some(
            allowed_tools
                .unwrap_or_else(|| contract.tools.keys().cloned().collect::<BTreeSet<_>>()),
        );
        let execution_max_turns = payload
            .get("__carrier_max_turns")
            .and_then(Value::as_u64)
            .map(u32::try_from)
            .transpose()
            .map_err(|_| {
                AppRuntimeError::InvalidPackage("agent max turns exceeds u32".to_string())
            })?
            .unwrap_or(contract.max_turns)
            .min(contract.max_turns);
        let execution_max_tool_calls = payload
            .get("__carrier_max_tool_calls")
            .and_then(Value::as_u64)
            .unwrap_or(u64::MAX);
        let execution_budget_tokens = payload
            .get("__carrier_budget_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(u64::MAX);
        let deny_tools_after_output = payload
            .get("__carrier_deny_tools_after_output")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if execution_max_turns == 0 || execution_max_tool_calls == 0 || execution_budget_tokens == 0
        {
            return Err(AppRuntimeError::InvalidPackage(
                "agent execution bounds must be positive".to_string(),
            ));
        }
        let history_key = format!("__bicdb_llm_v1:{client}:{conversation_id}");
        let mut history = match self.runtime_cache_get(
            "llm.history",
            &[
                (None, Value::String("Json".to_string())),
                (None, Value::String(history_key.clone())),
            ],
        ) {
            Ok(Value::Array(history)) => history,
            Ok(_) => {
                return Err(AppRuntimeError::Invocation(
                    "persisted LLM history is not an array".to_string(),
                ));
            }
            Err(AppRuntimeError::NotFound(_)) => Vec::new(),
            Err(error) => return Err(error),
        };
        if history.len() > contract.max_history_messages as usize {
            history.drain(..history.len() - contract.max_history_messages as usize);
        }
        let candidates = self.llm_route_candidates(client, &contract, &history, &user_prompt)?;
        let initial = candidates.first().cloned().ok_or_else(|| {
            AppRuntimeError::CapabilityDenied("LLM route has no client".to_string())
        })?;
        let budget_selection = (|| -> Result<(String, Option<u64>)> {
            let Some(budget) = &contract.budget else {
                return Ok((initial.clone(), None));
            };
            let budget_candidates = if contract
                .routing
                .as_ref()
                .is_some_and(|routing| routing.on_budget_pressure)
            {
                candidates.clone()
            } else {
                vec![initial.clone()]
            };
            for candidate in budget_candidates {
                let projected = self.llm_projected_cost(&candidate, &history, &user_prompt)?;
                if self.llm_reserve_budget(client, budget, projected)? {
                    return Ok((candidate, Some(projected)));
                }
            }
            match &budget.over_budget {
                ApplicationLlmOverBudgetV1::Fail { error_code } => {
                    Err(AppRuntimeError::ApplicationFailure {
                        code: error_code.clone(),
                        message: format!("LLM client `{client}` exceeded its tenant daily budget"),
                        retryable: false,
                    })
                }
                ApplicationLlmOverBudgetV1::Downgrade { client: downgrade } => {
                    let projected = self.llm_projected_cost(downgrade, &history, &user_prompt)?;
                    if self.llm_reserve_budget(client, budget, projected)? {
                        Ok((downgrade.clone(), Some(projected)))
                    } else {
                        Err(AppRuntimeError::ResourceExhausted(format!(
                            "LLM client `{client}` and its signed downgrade exceeded the tenant daily budget"
                        )))
                    }
                }
            }
        })();
        let (selected, reserved_budget) = match budget_selection {
            Ok(selection) => selection,
            Err(error) => {
                if method == "stream" {
                    self.publish_llm_stream_failure(client, &contract, &conversation_id, &error)?;
                }
                return Err(error);
            }
        };
        let mut total_tool_calls = 0u64;
        let mut total_input_tokens = 0i64;
        let mut total_output_tokens = 0i64;
        let execution = (|| -> Result<Value> {
            let mut response = Value::Null;
            for turn in 0..execution_max_turns {
                if history.len() >= contract.max_history_messages as usize {
                    return Err(AppRuntimeError::ResourceExhausted(
                        "LLM tool conversation exceeded signed history bounds".to_string(),
                    ));
                }
                let continuation = turn != 0;
                response = self.llm_complete_with_fallback(
                    &selected,
                    client,
                    &candidates,
                    method,
                    if continuation { "" } else { &user_prompt },
                    continuation,
                    history.clone(),
                    conversation_id.clone(),
                    output_type.clone(),
                    allowed_tools.clone(),
                    &contract,
                )?;
                total_input_tokens = total_input_tokens.saturating_add(
                    response
                        .get("input_tokens")
                        .and_then(Value::as_i64)
                        .unwrap_or(0),
                );
                total_output_tokens = total_output_tokens.saturating_add(
                    response
                        .get("output_tokens")
                        .and_then(Value::as_i64)
                        .unwrap_or(0),
                );
                if !continuation {
                    history.push(json!({"role": "user", "content": user_prompt}));
                }
                let assistant = response.get("assistant_message").cloned().ok_or_else(|| {
                    AppRuntimeError::Invocation(
                        "BicDB application LLM response lacks assistant message".to_string(),
                    )
                })?;
                history.push(assistant);
                let tool_requests = response
                    .get("tool_requests")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                if deny_tools_after_output
                    && response
                        .get("structured_output")
                        .is_some_and(|value| !value.is_null())
                    && !tool_requests.is_empty()
                {
                    return Err(AppRuntimeError::CapabilityDenied(
                        "agent denied tools after producing output".to_string(),
                    ));
                }
                if tool_requests.is_empty() {
                    break;
                }
                let next_tool_calls = total_tool_calls.saturating_add(tool_requests.len() as u64);
                if next_tool_calls > execution_max_tool_calls {
                    return Err(AppRuntimeError::ResourceExhausted(
                        "agent exceeded its signed tool-call bound".to_string(),
                    ));
                }
                total_tool_calls = next_tool_calls;
                if u64::try_from(total_input_tokens.saturating_add(total_output_tokens))
                    .unwrap_or(u64::MAX)
                    > execution_budget_tokens
                {
                    return Err(AppRuntimeError::ResourceExhausted(
                        "agent exceeded its signed token budget".to_string(),
                    ));
                }
                for request in tool_requests {
                    let object = request.as_object().ok_or_else(|| {
                        AppRuntimeError::Invocation("LLM tool request is not an object".to_string())
                    })?;
                    let id = object.get("id").and_then(Value::as_str).ok_or_else(|| {
                        AppRuntimeError::Invocation("LLM tool request lacks id".to_string())
                    })?;
                    let name = object.get("name").and_then(Value::as_str).ok_or_else(|| {
                        AppRuntimeError::Invocation("LLM tool request lacks name".to_string())
                    })?;
                    let arguments = object
                        .get("arguments")
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    let result = self.execute_llm_tool(&contract, name, arguments)?;
                    if method == "stream" {
                        if let Some(stream) = &contract.stream {
                            self.emit(
                                &stream.tool_event,
                                json!({
                                    "conversation_id": conversation_id,
                                    "tenant_id": self.host.actor().tenant_id,
                                    "workspace_id": self.host.actor().workspace_id,
                                    "tool": name,
                                    "call_id": id,
                                }),
                            )?;
                        }
                    }
                    history.push(json!({
                        "role": "tool",
                        "tool_call_id": id,
                        "name": name,
                        "content": serde_json::to_string(&result)?,
                    }));
                }
                if turn + 1 == execution_max_turns {
                    return Err(AppRuntimeError::ResourceExhausted(format!(
                        "LLM client `{client}` exceeded its signed turn bound"
                    )));
                }
            }
            Ok(response)
        })();
        let mut response = match execution {
            Ok(response) => response,
            Err(error) => {
                if let (Some(budget), Some(reserved)) = (&contract.budget, reserved_budget) {
                    let actual = self
                        .llm_estimate(
                            &selected,
                            u64::try_from(total_input_tokens).unwrap_or(u64::MAX),
                            u64::try_from(total_output_tokens).unwrap_or(u64::MAX),
                        )
                        .unwrap_or(reserved);
                    self.llm_adjust_budget(client, budget, reserved, actual)?;
                }
                if method == "stream" {
                    self.publish_llm_stream_failure(client, &contract, &conversation_id, &error)?;
                }
                return Err(error);
            }
        };
        if let (Some(budget), Some(reserved)) = (&contract.budget, reserved_budget) {
            let actual = self.llm_estimate(
                &selected,
                u64::try_from(total_input_tokens).unwrap_or(u64::MAX),
                u64::try_from(total_output_tokens).unwrap_or(u64::MAX),
            )?;
            self.llm_adjust_budget(client, budget, reserved, actual)?;
        }
        response["tool_calls"] = Value::from(total_tool_calls);
        response["input_tokens"] = Value::from(total_input_tokens);
        response["output_tokens"] = Value::from(total_output_tokens);
        response["total_tokens"] =
            Value::from(total_input_tokens.saturating_add(total_output_tokens));
        response.as_object_mut().map(|object| {
            object.remove("tool_requests");
            object.remove("assistant_message");
        });
        let maximum = contract.max_history_messages as usize;
        if history.len() > maximum {
            history.drain(..history.len() - maximum);
        }
        self.runtime_cache_set(
            "llm.history",
            &[
                (None, Value::String(history_key)),
                (None, Value::Array(history)),
            ],
        )?;
        if method == "stream_response" {
            let handle = response
                .get("__carrier_stream_handle")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    AppRuntimeError::Invocation(
                        "live LLM response omitted its stream handle".to_string(),
                    )
                })?;
            return Ok(json!({"__carrier_sse_stream": handle}));
        }
        if method == "stream" {
            self.publish_llm_stream(client, &contract, &conversation_id, &mut response)?;
        }
        if method == "respond_as" {
            response.get("structured_output").cloned().ok_or_else(|| {
                AppRuntimeError::Invocation(
                    "BicDB application LLM response lacks structured output".to_string(),
                )
            })
        } else {
            Ok(response)
        }
    }

    pub(crate) fn execute_llm_tool(
        &mut self,
        contract: &ApplicationLlmClientV1,
        name: &str,
        arguments: Value,
    ) -> Result<Value> {
        let tool = contract.tools.get(name).ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(format!(
                "LLM tool `{name}` is outside signed authority"
            ))
        })?;
        let object = arguments.as_object().ok_or_else(|| {
            AppRuntimeError::InvalidRequest(format!(
                "LLM tool `{name}` arguments must be an object"
            ))
        })?;
        let arguments = tool
            .parameters
            .iter()
            .map(|parameter| {
                Ok((
                    Some(parameter.name.clone()),
                    object.get(&parameter.name).cloned().unwrap_or(Value::Null),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let program = self
            .host
            .application()
            .application_program
            .as_ref()
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage("BicDB application program is absent".to_string())
            })?;
        execute_application_program(&program, &tool.callable, BTreeMap::new(), arguments, self)
    }

    pub(crate) fn llm_complete_with_fallback(
        &mut self,
        selected: &str,
        logical_client: &str,
        candidates: &[String],
        method: &str,
        user_prompt: &str,
        continuation: bool,
        history: Vec<Value>,
        conversation_id: String,
        output_type: Option<String>,
        allowed_tools: Option<BTreeSet<String>>,
        logical_contract: &ApplicationLlmClientV1,
    ) -> Result<Value> {
        let mut ordered = vec![selected.to_string()];
        ordered.extend(
            candidates
                .iter()
                .filter(|candidate| candidate.as_str() != selected)
                .cloned(),
        );
        let mut last_error = None;
        for (index, candidate) in ordered.iter().enumerate() {
            let result = self.host_call(HostRequest::Llm(LlmRequest::Complete {
                client: candidate.clone(),
                method: method.to_string(),
                user_prompt: user_prompt.to_string(),
                continuation,
                history: history.clone(),
                conversation_id: Some(conversation_id.clone()),
                output_type: output_type.clone(),
                allowed_tools: allowed_tools.clone(),
                deadline_unix_ms: self.host.actor().deadline_unix_ms,
            }));
            match result {
                Ok(HostValue::Json(value)) => return Ok(value),
                Ok(other) => {
                    return Err(AppRuntimeError::Invocation(format!(
                        "BicDB application LLM host returned {other:?}, expected JSON"
                    )));
                }
                Err(error) => {
                    let may_fallback = logical_contract.routing.as_ref().is_some_and(|routing| {
                        matches!(error, AppRuntimeError::RateLimited(_)) && routing.on_rate_limit
                            || matches!(
                                error,
                                AppRuntimeError::Provider(_)
                                    | AppRuntimeError::Timeout(_)
                                    | AppRuntimeError::NotReady(_)
                                    | AppRuntimeError::CircuitOpen(_)
                            ) && routing.on_primary_outage
                    });
                    if index + 1 == ordered.len() || !may_fallback {
                        return Err(error);
                    }
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            AppRuntimeError::Provider(format!(
                "LLM route `{logical_client}` exhausted its signed candidates"
            ))
        }))
    }

    pub(crate) fn llm_route_candidates(
        &mut self,
        client: &str,
        contract: &ApplicationLlmClientV1,
        history: &[Value],
        prompt: &str,
    ) -> Result<Vec<String>> {
        let Some(routing) = &contract.routing else {
            return Ok(vec![client.to_string()]);
        };
        let mut candidates = vec![routing.primary.clone()];
        candidates.extend(routing.fallbacks.clone());
        if let Some(target) = routing.target_microusd_per_request {
            for index in 0..candidates.len() {
                if self.llm_projected_cost(&candidates[index], history, prompt)? <= target {
                    candidates.rotate_left(index);
                    break;
                }
            }
        }
        Ok(candidates)
    }

    pub(crate) fn llm_projected_cost(
        &mut self,
        client: &str,
        history: &[Value],
        prompt: &str,
    ) -> Result<u64> {
        let contract = self
            .host
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.llm.as_ref())
            .and_then(|llm| llm.clients.get(client))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "LLM route target `{client}` lacks signed authority"
                ))
            })?;
        let text = history
            .iter()
            .filter_map(|message| message.get("content").and_then(Value::as_str))
            .chain(std::iter::once(prompt))
            .collect::<Vec<_>>()
            .join("\n");
        let input_tokens =
            match self.host_call(HostRequest::Tokenizer(TokenizerRequest::Count {
                provider: contract.tokenizer_provider,
                text,
            }))? {
                HostValue::U64(value) => value,
                other => return carrier_host_type_error(client, "token count", other),
            };
        self.llm_estimate(client, input_tokens, contract.max_output_tokens)
    }

    pub(crate) fn llm_estimate(
        &mut self,
        client: &str,
        input_tokens: u64,
        output_tokens: u64,
    ) -> Result<u64> {
        match self.host_call(HostRequest::Llm(LlmRequest::Estimate {
            client: client.to_string(),
            input_tokens,
            output_tokens,
        }))? {
            HostValue::U64(value) => Ok(value),
            other => carrier_host_type_error(client, "cost estimate", other),
        }
    }

    pub(crate) fn llm_budget_key(&self, client: &str) -> Result<String> {
        if self
            .host
            .actor()
            .tenant_id
            .as_deref()
            .is_none_or(str::is_empty)
        {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "LLM client `{client}` requires trusted tenant identity for its budget"
            )));
        }
        let day = crate::host::now_ms().div_euclid(86_400_000);
        Ok(format!("__bicdb_llm_budget_v1:{client}:{day}"))
    }

    pub(crate) fn llm_reserve_budget(
        &mut self,
        client: &str,
        budget: &ApplicationLlmBudgetV1,
        projected: u64,
    ) -> Result<bool> {
        let storage_key = self.runtime_cache_storage_key(&self.llm_budget_key(client)?)?;
        let (transaction, owned) = self.runtime_cache_transaction()?;
        let result = (|| {
            let current = self
                .host
                .read_runtime_cache(transaction, &storage_key)?
                .and_then(|value| value.as_u64())
                .unwrap_or(0);
            let Some(next) = current.checked_add(projected) else {
                return Ok(false);
            };
            if next > budget.limit_microusd_per_tenant_day {
                return Ok(false);
            }
            self.host.write_runtime_cache(
                transaction,
                &storage_key,
                Some(crate::host::now_ms().saturating_add(172_800_000)),
                Value::from(next),
            )?;
            Ok(true)
        })();
        self.finish_runtime_cache_transaction(owned, result)
    }

    pub(crate) fn llm_adjust_budget(
        &mut self,
        client: &str,
        _budget: &ApplicationLlmBudgetV1,
        reserved: u64,
        actual: u64,
    ) -> Result<()> {
        if actual == reserved {
            return Ok(());
        }
        let storage_key = self.runtime_cache_storage_key(&self.llm_budget_key(client)?)?;
        let (transaction, owned) = self.runtime_cache_transaction()?;
        let result = (|| {
            let current = self
                .host
                .read_runtime_cache(transaction, &storage_key)?
                .and_then(|value| value.as_u64())
                .unwrap_or(0);
            let next = if actual < reserved {
                current.saturating_sub(reserved - actual)
            } else {
                current.checked_add(actual - reserved).ok_or_else(|| {
                    AppRuntimeError::ResourceExhausted(
                        "LLM tenant budget accounting overflowed".to_string(),
                    )
                })?
            };
            self.host.write_runtime_cache(
                transaction,
                &storage_key,
                Some(crate::host::now_ms().saturating_add(172_800_000)),
                Value::from(next),
            )
        })();
        self.finish_runtime_cache_transaction(owned, result)
    }

    pub(crate) fn publish_llm_stream(
        &mut self,
        client: &str,
        contract: &ApplicationLlmClientV1,
        conversation_id: &str,
        response: &mut Value,
    ) -> Result<()> {
        let stream = contract.stream.as_ref().ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(format!(
                "LLM stream `{client}` lacks exact signed streaming authority"
            ))
        })?;
        let text = response.get("text").and_then(Value::as_str).unwrap_or("");
        for (sequence, chunk) in text.as_bytes().chunks(1_024).enumerate() {
            self.emit(
                &stream.chunk_event,
                json!({
                    "conversation_id": conversation_id,
                    "tenant_id": self.host.actor().tenant_id,
                    "workspace_id": self.host.actor().workspace_id,
                    "sequence": sequence,
                    "chunk": String::from_utf8_lossy(chunk),
                }),
            )?;
        }
        self.emit(
            &stream.completed_event,
            json!({
                "conversation_id": conversation_id,
                "tenant_id": self.host.actor().tenant_id,
                "workspace_id": self.host.actor().workspace_id,
                "tool_calls": response.get("tool_calls").cloned().unwrap_or(Value::from(0)),
            }),
        )?;
        response["stream_path"] =
            Value::String(format!("{}?conversation_id={conversation_id}", stream.path));
        Ok(())
    }

    pub(crate) fn publish_llm_stream_failure(
        &mut self,
        client: &str,
        contract: &ApplicationLlmClientV1,
        conversation_id: &str,
        error: &AppRuntimeError,
    ) -> Result<()> {
        let stream = contract.stream.as_ref().ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(format!(
                "LLM stream `{client}` lacks exact signed streaming authority"
            ))
        })?;
        let (code, retryable) = match error {
            AppRuntimeError::ApplicationFailure {
                code, retryable, ..
            } => (code.as_str(), *retryable),
            AppRuntimeError::Timeout(_) | AppRuntimeError::ResilienceTimeout(_) => {
                ("deadline_exceeded", true)
            }
            AppRuntimeError::RateLimited(_) => ("rate_limited", true),
            AppRuntimeError::Provider(_) => ("provider_failure", true),
            AppRuntimeError::NotReady(_) | AppRuntimeError::CircuitOpen(_) => {
                ("dependency_unavailable", true)
            }
            AppRuntimeError::ResourceExhausted(_) => ("resource_exhausted", false),
            AppRuntimeError::CapabilityDenied(_) => ("capability_denied", false),
            AppRuntimeError::Authentication(_) => ("unauthenticated", false),
            _ => ("request_failed", false),
        };
        self.emit(
            &stream.failed_event,
            json!({
                "conversation_id": conversation_id,
                "tenant_id": self.host.actor().tenant_id,
                "workspace_id": self.host.actor().workspace_id,
                "code": code,
                "retryable": retryable,
            }),
        )
    }

    pub(crate) fn call_rag(
        &mut self,
        pipeline: &str,
        method: &str,
        arguments: Vec<(Option<String>, Value)>,
    ) -> Result<Value> {
        if !matches!(method, "respond" | "stream_response")
            || arguments.len() != 1
            || arguments[0].0.is_some()
        {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "RAG call `{pipeline}.{method}` requires one positional question"
            )));
        }
        let question = carrier_string(arguments[0].1.clone(), pipeline)?;
        let contract = self
            .host
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.rag.as_ref())
            .and_then(|rag| rag.pipelines.get(pipeline))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "RAG pipeline `{pipeline}` lacks exact signed authority"
                ))
            })?;
        let embedding = match (
            contract.embedding_provider.as_ref(),
            contract.embedding_callable.as_ref(),
        ) {
            (Some(provider), None) => {
                match self.host_call(HostRequest::Embeddings(EmbeddingsRequest::Embed {
                    provider: provider.clone(),
                    text: question.clone(),
                    dimensions: contract.dimensions,
                }))? {
                    HostValue::Json(value) => value,
                    other => return carrier_host_type_error(pipeline, "embedding", other),
                }
            }
            (None, Some(callable)) => {
                let program = self
                    .host
                    .application()
                    .application_program
                    .as_ref()
                    .cloned()
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidPackage(
                            "BicDB application program is absent".to_string(),
                        )
                    })?;
                execute_application_program(
                    &program,
                    callable,
                    BTreeMap::new(),
                    vec![(None, Value::String(question.clone()))],
                    self,
                )?
            }
            _ => {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "RAG pipeline `{pipeline}` has invalid embedding authority"
                )))
            }
        };
        let matches = self.call_model(
            &contract.retriever_model,
            "similar_with_scores",
            vec![
                (None, embedding),
                (Some("limit".to_string()), Value::from(contract.top_k)),
            ],
        )?;
        let matches = matches.as_array().ok_or_else(|| {
            AppRuntimeError::Invocation("RAG vector search did not return an array".to_string())
        })?;
        let mut documents = Vec::new();
        let max_context_bytes = u64::from(contract.context_window_tokens)
            .saturating_mul(4)
            .min(16 * 1024 * 1024) as usize;
        let mut context_bytes = 0usize;
        for item in matches {
            let score = item.get("score").and_then(Value::as_f64).ok_or_else(|| {
                AppRuntimeError::Invocation("RAG result omitted its score".to_string())
            })?;
            if contract
                .score_threshold_millionths
                .is_some_and(|threshold| score * 1_000_000.0 < f64::from(threshold))
            {
                continue;
            }
            let document = item.get("doc").cloned().ok_or_else(|| {
                AppRuntimeError::Invocation("RAG result omitted its document".to_string())
            })?;
            let encoded = serde_json::to_string(&document)?;
            if context_bytes.saturating_add(encoded.len()) > max_context_bytes {
                break;
            }
            context_bytes += encoded.len();
            documents.push(encoded);
        }
        let prompt = format!(
            "Answer the question using only the supplied retrieval context.\n\nQuestion:\n{question}\n\nContext:\n{}",
            documents.join("\n")
        );
        self.call_llm(
            &contract.llm_client,
            method,
            vec![(
                None,
                json!({
                    "user_prompt": prompt,
                    "conversation_id": Value::Null,
                }),
            )],
        )
    }

    pub(crate) fn call_agent(
        &mut self,
        agent_name: &str,
        method: &str,
        arguments: Vec<(Option<String>, Value)>,
    ) -> Result<Value> {
        if method != "run"
            || arguments.len() != 2
            || arguments.iter().any(|(name, _)| name.is_some())
        {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "agent `{agent_name}.{method}` requires positional input and prompt"
            )));
        }
        let input = arguments[0].1.clone();
        let prompt = carrier_string(arguments[1].1.clone(), agent_name)?;
        let contract = self
            .host
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.agents.as_ref())
            .and_then(|agents| agents.agents.get(agent_name))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "agent `{agent_name}` lacks exact signed authority"
                ))
            })?;
        let authenticated = self
            .host
            .actor()
            .user_id
            .as_deref()
            .is_some_and(|id| !id.is_empty())
            || self
                .host
                .actor()
                .service_id
                .as_deref()
                .is_some_and(|id| !id.is_empty());
        let tenant = self
            .host
            .actor()
            .tenant_id
            .as_deref()
            .is_some_and(|id| !id.is_empty());
        if contract.require_auth && !authenticated || contract.require_tenant && !tenant {
            if contract.emit_guard_failures {
                self.host_call(HostRequest::Observe(ObserveRequest::Evidence {
                    control: "agent.guard".to_string(),
                    outcome: "denied".to_string(),
                    fields: BTreeMap::from([(
                        "agent".to_string(),
                        Value::String(agent_name.to_string()),
                    )]),
                }))?;
            }
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "agent `{agent_name}` failed its signed identity guard"
            )));
        }
        let output_type = contract.structured_output.clone();
        let method = if output_type.is_some() {
            "respond_as"
        } else {
            "respond"
        };
        let mut payload = json!({
            "user_prompt": prompt,
            "conversation_id": Value::Null,
            "__carrier_allowed_tools": contract.tools,
            "__carrier_max_turns": contract.max_iterations,
            "__carrier_max_tool_calls": contract.max_tool_calls,
            "__carrier_budget_tokens": contract.budget_tokens,
            "__carrier_deny_tools_after_output": contract.deny_tools_after_output,
        });
        let mut call_arguments = Vec::new();
        if let Some(output_type) = output_type {
            call_arguments.push((None, Value::String(output_type)));
        }
        call_arguments.push((None, payload.take()));
        let mut last_error = None;
        for attempt in 0..=contract.retry_attempts {
            match self.call_llm(&contract.llm_client, method, call_arguments.clone()) {
                Ok(value) => return Ok(value),
                Err(error) if attempt < contract.retry_attempts => {
                    last_error = Some(error);
                    if contract.retry_backoff_ms > 0 {
                        ApplicationProgramHost::sleep(
                            self,
                            Duration::from_millis(contract.retry_backoff_ms),
                        )?;
                    }
                }
                Err(error) => last_error = Some(error),
            }
        }
        if let Some(fallback) = &contract.fallback_output {
            return evaluate_carrier_expression(
                self.host.application().application_program.as_ref(),
                fallback,
                BTreeMap::from([("input".to_string(), input)]),
            );
        }
        Err(last_error.unwrap_or_else(|| {
            AppRuntimeError::Provider(format!("agent `{agent_name}` failed without an error"))
        }))
    }

    pub(crate) fn blob_contract(&self, target: &str) -> Result<ApplicationBlobContractV1> {
        let contract = self
            .host
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.blob.as_ref())
            .filter(|contract| contract.helpers.contains(target))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "BicDB application blob helper `{target}` lacks its exact signed contract"
                ))
            })?;
        Ok(contract)
    }

    pub(crate) fn redis_builtin(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let expected = if target == "redis.publish" { 2 } else { 1 };
        if arguments.len() != expected || arguments.iter().any(|(name, _)| name.is_some()) {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "BicDB application Redis helper `{target}` requires exactly {expected} positional argument{}",
                if expected == 1 { "" } else { "s" }
            )));
        }
        let contract = self
            .host
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.redis.as_ref())
            .filter(|contract| contract.helpers.contains(target))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "BicDB application Redis helper `{target}` lacks its exact signed contract"
                ))
            })?;
        let request = match target {
            "redis.publish" => RedisRequest::Publish {
                provider: contract.provider,
                channel: carrier_string(carrier_argument(arguments, 0, None, target)?, target)?,
                message: carrier_string(carrier_argument(arguments, 1, None, target)?, target)?,
            },
            "redis.incr" => RedisRequest::Incr {
                provider: contract.provider,
                key: carrier_string(carrier_argument(arguments, 0, None, target)?, target)?,
            },
            _ => unreachable!("caller selects an exact Redis helper"),
        };
        match self.host_call(HostRequest::Redis(request))? {
            HostValue::I64(value) => Ok(Value::from(value)),
            other => carrier_host_type_error(target, "integer", other),
        }
    }

    pub(crate) fn email_builtin(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let contract = self
            .host
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.email.as_ref())
            .filter(|contract| contract.helper == target)
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "BicDB application email helper `{target}` lacks its exact signed contract"
                ))
            })?;
        let allowed = [
            "to", "from", "subject", "text", "html", "cc", "bcc", "reply_to",
        ];
        let mut names = BTreeSet::new();
        for (name, _) in arguments {
            let name = name.as_deref().ok_or_else(|| {
                AppRuntimeError::InvalidRequest(
                    "email.send accepts only named delivery arguments".to_string(),
                )
            })?;
            if !allowed.contains(&name) || !names.insert(name) {
                return Err(AppRuntimeError::CapabilityDenied(
                    "email.send contains an unsupported or repeated delivery argument".to_string(),
                ));
            }
        }
        let named = |name: &str| {
            arguments
                .iter()
                .find(|(candidate, _)| candidate.as_deref() == Some(name))
                .map(|(_, value)| value.clone())
        };
        let required_string = |name: &str| -> Result<String> {
            carrier_string(
                named(name).ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(format!(
                        "email.send requires named argument `{name}`"
                    ))
                })?,
                target,
            )
        };
        let recipients = |name: &str, required: bool| -> Result<Vec<String>> {
            let Some(value) = named(name) else {
                return if required {
                    Err(AppRuntimeError::InvalidRequest(format!(
                        "email.send requires named argument `{name}`"
                    )))
                } else {
                    Ok(Vec::new())
                };
            };
            match value {
                Value::String(value) => Ok(vec![value]),
                Value::Array(values) => values
                    .into_iter()
                    .map(|value| carrier_string(value, target))
                    .collect(),
                _ => Err(AppRuntimeError::InvalidRequest(format!(
                    "email.send named argument `{name}` requires a string or string array"
                ))),
            }
        };
        let optional_string = |name: &str| -> Result<Option<String>> {
            named(name)
                .filter(|value| !value.is_null())
                .map(|value| carrier_string(value, target))
                .transpose()
        };
        let request = EmailRequest::Send {
            provider: contract.provider,
            from: required_string("from")?,
            to: recipients("to", true)?,
            cc: recipients("cc", false)?,
            bcc: recipients("bcc", false)?,
            reply_to: optional_string("reply_to")?,
            subject: required_string("subject")?,
            text: optional_string("text")?,
            html: optional_string("html")?,
        };
        match self.host_call(HostRequest::Email(request))? {
            HostValue::Json(value) => Ok(value),
            other => carrier_host_type_error(target, "email delivery object", other),
        }
    }

    pub(crate) fn blob_put(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let contract = self.blob_contract(target)?;
        let source_path = carrier_string(carrier_argument(arguments, 0, None, target)?, target)?;
        let key = carrier_string(carrier_argument(arguments, 1, None, target)?, target)?;
        let file = self.virtual_files.get(&source_path).ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(format!(
                "BicDB application BicDB blob source `{source_path}` is not an invocation-local virtual file; ambient filesystem paths are unavailable"
            ))
        })?;
        if file.bytes.len() as u64 > contract.max_blob_bytes {
            return Err(AppRuntimeError::ResourceExhausted(
                "BicDB application blob source exceeds its signed size limit".to_string(),
            ));
        }
        let content_type = arguments
            .get(2)
            .map(|(_, value)| carrier_string(value.clone(), target))
            .transpose()?
            .or_else(|| file.content_type.clone())
            .or_else(|| mime_guess::from_path(&key).first_raw().map(str::to_string));
        let bytes = file.bytes.clone();
        let upload = match self.host_call(HostRequest::Blob(BlobRequest::CreateNamedUpload {
            namespace: contract.namespace.clone(),
            key: key.clone(),
            content_type,
            metadata: BTreeMap::new(),
        }))? {
            HostValue::Handle(value) => value,
            other => return carrier_host_type_error(target, "blob upload handle", other),
        };
        for chunk in bytes.chunks(256 * 1024) {
            match self.host_call(HostRequest::Blob(BlobRequest::Write {
                upload,
                bytes: chunk.to_vec(),
            }))? {
                HostValue::U64(written) if written == chunk.len() as u64 => {}
                other => return carrier_host_type_error(target, "blob write count", other),
            }
        }
        let metadata = match self.host_call(HostRequest::Blob(BlobRequest::Finish { upload }))? {
            HostValue::BlobMetadata(value) => value,
            other => return carrier_host_type_error(target, "blob metadata", other),
        };
        self.blob_evidence("carrier.blob.put", &key, Some(metadata.size))?;
        Ok(carrier_blob_metadata_value(&key, &metadata))
    }

    pub(crate) fn blob_get(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let contract = self.blob_contract(target)?;
        let key = carrier_string(carrier_argument(arguments, 0, None, target)?, target)?;
        let destination = carrier_string(carrier_argument(arguments, 1, None, target)?, target)?;
        validate_carrier_virtual_path(&destination)?;
        let metadata = self.named_blob_metadata(&contract, &key, target)?;
        let blob = match self.host_call(HostRequest::Blob(BlobRequest::OpenNamedRead {
            namespace: contract.namespace,
            key: key.clone(),
        }))? {
            HostValue::Handle(value) => value,
            other => return carrier_host_type_error(target, "blob read handle", other),
        };
        let mut bytes = Vec::with_capacity(metadata.size as usize);
        loop {
            let chunk = match self.host_call(HostRequest::Blob(BlobRequest::Read {
                blob,
                max_bytes: 256 * 1024,
            }))? {
                HostValue::Bytes(value) => value,
                other => return carrier_host_type_error(target, "blob bytes", other),
            };
            if chunk.is_empty() {
                break;
            }
            bytes.extend_from_slice(&chunk);
            if bytes.len() as u64 > contract.max_blob_bytes {
                return Err(AppRuntimeError::ResourceExhausted(
                    "BicDB application blob read exceeds its signed size limit".to_string(),
                ));
            }
        }
        if carrier_idempotency_sha256(&bytes) != metadata.sha256 {
            return Err(AppRuntimeError::Provider(
                "BicDB application blob read failed its signed SHA-256 boundary".to_string(),
            ));
        }
        self.virtual_files.insert(
            destination,
            ApplicationVirtualFile {
                bytes,
                content_type: metadata.content_type.clone(),
            },
        );
        self.blob_evidence("carrier.blob.get", &key, Some(metadata.size))?;
        Ok(carrier_blob_metadata_value(&key, &metadata))
    }

    pub(crate) fn blob_signed_url(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let contract = self.blob_contract(target)?;
        let key = carrier_string(carrier_argument(arguments, 0, None, target)?, target)?;
        let expires_seconds = arguments
            .iter()
            .find(|(name, _)| name.as_deref() == Some("expires_seconds"))
            .map(|(_, value)| carrier_integer(value.clone(), target))
            .transpose()?
            .unwrap_or(3_600);
        let expires_seconds = u32::try_from(expires_seconds)
            .ok()
            .filter(|value| (1..=86_400).contains(value))
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest(
                    "blob.signed_url expires_seconds must be 1..=86400".to_string(),
                )
            })?;
        let method = arguments
            .iter()
            .find(|(name, _)| name.as_deref() == Some("method"))
            .map(|(_, value)| carrier_string(value.clone(), target))
            .transpose()?
            .unwrap_or_else(|| "GET".to_string())
            .to_ascii_uppercase();
        if !contract.signed_methods.contains(&method) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "blob.signed_url method `{method}` is outside its signed method set"
            )));
        }
        let download_name = arguments
            .iter()
            .find(|(name, _)| name.as_deref() == Some("download_name"))
            .map(|(_, value)| carrier_string(value.clone(), target))
            .transpose()?;
        let url = match self.host_call(HostRequest::Blob(BlobRequest::NamedSignedUrl {
            namespace: contract.namespace,
            key: key.clone(),
            expires_seconds,
            method,
            download_name,
        }))? {
            HostValue::String(value) => value,
            other => return carrier_host_type_error(target, "signed blob URL", other),
        };
        self.blob_evidence("carrier.blob.signed_url", &key, None)?;
        Ok(Value::String(url))
    }

    pub(crate) fn blob_metadata_builtin(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let contract = self.blob_contract(target)?;
        let key = carrier_string(carrier_argument(arguments, 0, None, target)?, target)?;
        let metadata = self.named_blob_metadata(&contract, &key, target)?;
        self.blob_evidence("carrier.blob.metadata", &key, Some(metadata.size))?;
        Ok(carrier_blob_metadata_value(&key, &metadata))
    }

    pub(crate) fn blob_hash(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let contract = self.blob_contract(target)?;
        let key = carrier_string(carrier_argument(arguments, 0, None, target)?, target)?;
        let metadata = self.named_blob_metadata(&contract, &key, target)?;
        self.blob_evidence("carrier.blob.hash", &key, Some(metadata.size))?;
        Ok(Value::String(metadata.sha256))
    }

    pub(crate) fn blob_content_type(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let path = carrier_string(carrier_argument(arguments, 0, None, target)?, target)?;
        let content_type = self
            .virtual_files
            .get(&path)
            .and_then(|file| file.content_type.clone())
            .or_else(|| mime_guess::from_path(&path).first_raw().map(str::to_string))
            .unwrap_or_else(|| "application/octet-stream".to_string());
        Ok(Value::String(content_type))
    }

    pub(crate) fn named_blob_metadata(
        &mut self,
        contract: &ApplicationBlobContractV1,
        key: &str,
        target: &str,
    ) -> Result<BlobMetadata> {
        match self.host_call(HostRequest::Blob(BlobRequest::NamedMetadata {
            namespace: contract.namespace.clone(),
            key: key.to_string(),
        }))? {
            HostValue::BlobMetadata(value) => Ok(value),
            other => carrier_host_type_error(target, "blob metadata", other),
        }
    }

    pub(crate) fn blob_evidence(
        &mut self,
        action: &str,
        key: &str,
        size: Option<u64>,
    ) -> Result<()> {
        let mut fields = BTreeMap::from([(
            "subject".to_string(),
            Value::String(carrier_idempotency_sha256(key.as_bytes())),
        )]);
        if let Some(size) = size {
            fields.insert("size_bytes".to_string(), json!(size));
        }
        self.security_evidence(action, "success", fields)
    }

    pub(crate) fn enqueue_job(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let positional = arguments
            .iter()
            .filter(|(name, _)| name.is_none())
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        if positional.len() != 2
            || arguments.iter().any(|(name, _)| {
                name.as_deref()
                    .is_some_and(|name| name != "delay_seconds" && name != "baggage")
            })
        {
            return Err(AppRuntimeError::InvalidPackage(
                "jobs.enqueue requires a signed job name, payload, and optional delay_seconds/baggage"
                    .to_string(),
            ));
        }
        let job = carrier_string(positional[0].clone(), target)?;
        let queue = self
            .host
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.job_bindings.get(&job))
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "BicDB application job `{job}` has no signed queue binding"
                ))
            })?;
        let named_value = |name: &str| {
            arguments
                .iter()
                .find(|(argument_name, _)| argument_name.as_deref() == Some(name))
                .map(|(_, value)| value.clone())
        };
        let delay_ms = named_value("delay_seconds")
            .map(|value| {
                let seconds = carrier_integer(value, target)?;
                u64::try_from(seconds)
                    .map_err(|_| {
                        AppRuntimeError::InvalidRequest(
                            "jobs.enqueue delay_seconds cannot be negative".to_string(),
                        )
                    })?
                    .checked_mul(1_000)
                    .ok_or_else(|| {
                        AppRuntimeError::InvalidRequest(
                            "jobs.enqueue delay_seconds exceeds the durable broker range"
                                .to_string(),
                        )
                    })
                    .map(Some)
            })
            .transpose()?
            .flatten()
            .filter(|delay| *delay > 0);
        let baggage = named_value("baggage").unwrap_or_else(|| json!({}));
        if !baggage.is_object() {
            return Err(AppRuntimeError::InvalidRequest(
                "jobs.enqueue baggage must be an object".to_string(),
            ));
        }
        let baggage = serde_json::to_string(&baggage)?;
        if baggage.len() > 64 * 1024 {
            return Err(AppRuntimeError::InvalidRequest(
                "jobs.enqueue baggage exceeds 64 KiB".to_string(),
            ));
        }
        let headers = BTreeMap::from([
            ("carrier_job".to_string(), job),
            ("carrier_baggage".to_string(), baggage),
        ]);
        let request = match self.current_transaction() {
            Some(transaction) => BrokerRequest::PublishOnCommit {
                transaction,
                queue,
                payload: positional[1].clone(),
                headers,
                idempotency_key: None,
                delay_ms,
            },
            None => BrokerRequest::Publish {
                queue,
                payload: positional[1].clone(),
                headers,
                idempotency_key: None,
                delay_ms,
            },
        };
        match self.host_call(HostRequest::Broker(request))? {
            HostValue::String(receipt) => Ok(Value::String(receipt)),
            other => carrier_host_type_error(target, "message receipt", other),
        }
    }

    pub(crate) fn feature_flag_enabled(
        &self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        if arguments.len() != 1 || arguments[0].0.is_some() {
            return Err(AppRuntimeError::InvalidPackage(
                "flags.enabled requires one compiler-signed literal flag name".to_string(),
            ));
        }
        let name = carrier_string(arguments[0].1.clone(), target)?;
        let definition = self
            .host
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.flags.get(&name))
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "BicDB application feature flag `{name}` has no signed definition"
                ))
            })?;
        Ok(Value::Bool(carrier_feature_flag_enabled(
            &name,
            definition,
            self.host.actor(),
        )))
    }

    pub(crate) fn call_declared_sql(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
        mode: DeclaredSqlResult,
    ) -> Result<Value> {
        if arguments.is_empty() || arguments.iter().any(|(name, _)| name.is_some()) {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "BicDB application `{target}` requires a signed statement id and positional binds"
            )));
        }
        let statement_id = carrier_string(arguments[0].1.clone(), target)?;
        let parameters = arguments
            .iter()
            .skip(1)
            .map(|(_, value)| value.clone())
            .collect::<Vec<_>>();
        let owned = self.current_transaction().is_none();
        if owned {
            self.begin_transaction("read_committed")?;
        }
        let result = (|| {
            let transaction = self.current_transaction().ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "BicDB application `{target}` has no database transaction"
                ))
            })?;
            let value = self.host_call(HostRequest::Database(DatabaseRequest::RawSql {
                transaction,
                statement_id,
                parameters,
            }))?;
            match (mode, value) {
                (DeclaredSqlResult::List, HostValue::Rows(rows)) => Ok(Value::Array(rows)),
                (DeclaredSqlResult::One, HostValue::Rows(mut rows)) => {
                    rows.drain(..).next().ok_or_else(|| {
                        AppRuntimeError::NotFound(format!(
                            "BicDB application `{target}` returned no row"
                        ))
                    })
                }
                (DeclaredSqlResult::Scalar, HostValue::Rows(mut rows)) => {
                    let row = rows
                        .drain(..)
                        .next()
                        .and_then(|row| row.as_object().cloned())
                        .ok_or_else(|| {
                            AppRuntimeError::NotFound(format!(
                                "BicDB application `{target}` returned no scalar row"
                            ))
                        })?;
                    if row.len() != 1 {
                        return Err(AppRuntimeError::Invocation(format!(
                            "BicDB application `{target}` returned {} columns instead of one",
                            row.len()
                        )));
                    }
                    Ok(row.into_values().next().expect("one scalar column"))
                }
                (DeclaredSqlResult::Affected, HostValue::U64(count)) => {
                    i64::try_from(count).map(Value::from).map_err(|_| {
                        AppRuntimeError::ResourceExhausted(format!(
                            "BicDB application `{target}` affected-row count exceeds Int"
                        ))
                    })
                }
                (_, other) => carrier_host_type_error(target, "declared SQL result", other),
            }
        })();
        self.finish_owned_read_transaction(owned, result)
    }

    pub(crate) fn field_crypto(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
        encrypt: bool,
    ) -> Result<Value> {
        if arguments.len() != 2 || arguments.iter().any(|(name, _)| name.is_some()) {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "BicDB application `{target}` requires a field reference and value"
            )));
        }
        let field_reference = carrier_string(arguments[0].1.clone(), target)?;
        let secret_name = self
            .secret_bindings
            .get(&field_reference)
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "BicDB application encrypted field `{field_reference}` has no signed secret binding"
                ))
            })?;
        if arguments[1].1.is_null() {
            return Ok(Value::Null);
        }
        let value = carrier_string(arguments[1].1.clone(), target)?;
        let (version, legacy) = if encrypt {
            (None, false)
        } else if let Some(encoded) = value.strip_prefix("enc:v2:") {
            let (encoded_version, _) = encoded.split_once(':').ok_or_else(|| {
                AppRuntimeError::InvalidRequest(
                    "BicDB application encrypted field v2 envelope is malformed".to_string(),
                )
            })?;
            let version = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(encoded_version)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .filter(|version| !version.is_empty())
                .ok_or_else(|| {
                    AppRuntimeError::InvalidRequest(
                        "BicDB application encrypted field v2 key version is invalid".to_string(),
                    )
                })?;
            (Some(version), false)
        } else {
            (None, true)
        };
        let secret = self.secret_handle_version(&secret_name, version.as_deref())?;
        let request = if encrypt {
            CryptoRequest::Encrypt {
                secret,
                algorithm: "bicdb-aes-256-gcm-v2".to_string(),
                plaintext: value.into_bytes(),
                associated_data: Vec::new(),
            }
        } else {
            CryptoRequest::Decrypt {
                secret,
                algorithm: if legacy {
                    "bicdb-aes-256-gcm-v1"
                } else {
                    "bicdb-aes-256-gcm-v2"
                }
                .to_string(),
                ciphertext: value.into_bytes(),
                associated_data: Vec::new(),
            }
        };
        let bytes = match self.host_call(HostRequest::Crypto(request))? {
            HostValue::Bytes(value) => value,
            other => return carrier_host_type_error(target, "bytes", other),
        };
        let metadata = self.secret_metadata(secret, target)?;
        self.security_evidence(
            if encrypt {
                "bicdb.application.crypto.field.encrypt"
            } else {
                "bicdb.application.crypto.field.decrypt"
            },
            "success",
            BTreeMap::from([
                ("subject".to_string(), Value::String(field_reference)),
                ("secret".to_string(), Value::String(secret_name)),
                ("key_id".to_string(), Value::String(metadata.key_id)),
                ("key_version".to_string(), Value::String(metadata.version)),
                ("legacy_envelope".to_string(), Value::Bool(legacy)),
            ]),
        )?;
        String::from_utf8(bytes).map(Value::String).map_err(|_| {
            AppRuntimeError::Invocation(format!(
                "BicDB application `{target}` produced invalid UTF-8"
            ))
        })
    }

    pub(crate) fn secret_handle(&mut self, name: &str) -> Result<HostHandle> {
        self.secret_handle_version(name, None)
    }

    pub(crate) fn secret_handle_version(
        &mut self,
        name: &str,
        version: Option<&str>,
    ) -> Result<HostHandle> {
        let key = (name.to_string(), version.map(str::to_string));
        if let Some(handle) = self.secret_handles.get(&key) {
            return Ok(*handle);
        }
        let handle = match self.host_call(HostRequest::Secret(SecretRequest::Open {
            name: name.to_string(),
            version: version.map(str::to_string),
        }))? {
            HostValue::Handle(value) => value,
            other => return carrier_host_type_error(name, "secret handle", other),
        };
        self.secret_handles.insert(key, handle);
        Ok(handle)
    }

    pub(crate) fn security_contract(&self, target: &str) -> Result<ApplicationSecurityContractV1> {
        let contract = self
            .host
            .application()
            .application_program
            .as_ref()
            .and_then(|program| program.security.as_ref())
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::CapabilityDenied(format!(
                    "BicDB application security builtin `{target}` has no signed security contract"
                ))
            })?;
        if !contract.helpers.contains(target) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "BicDB application security builtin `{target}` is outside the signed helper set"
            )));
        }
        Ok(contract)
    }

    pub(crate) fn security_named(
        arguments: &[(Option<String>, Value)],
        name: &str,
        target: &str,
    ) -> Result<Value> {
        arguments
            .iter()
            .find(|(argument_name, _)| argument_name.as_deref() == Some(name))
            .map(|(_, value)| value.clone())
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest(format!(
                    "BicDB application `{target}` requires named argument `{name}`"
                ))
            })
    }

    pub(crate) fn security_optional_i64(
        &self,
        arguments: &[(Option<String>, Value)],
        name: &str,
        target: &str,
    ) -> Result<Option<i64>> {
        arguments
            .iter()
            .find(|(argument_name, _)| argument_name.as_deref() == Some(name))
            .map(|(_, value)| carrier_integer(value.clone(), target))
            .transpose()
    }

    pub(crate) fn security_now_seconds(
        &mut self,
        arguments: &[(Option<String>, Value)],
        target: &str,
    ) -> Result<i64> {
        self.security_optional_i64(arguments, "now_epoch_seconds", target)?
            .or(self.security_optional_i64(arguments, "at_epoch_seconds", target)?)
            .map(Ok)
            .unwrap_or_else(|| {
                self.wall_time_milliseconds()
                    .map(|milliseconds| milliseconds / 1_000)
            })
    }

    pub(crate) fn security_transaction(&mut self) -> Result<(HostHandle, bool)> {
        let owned = self.current_transaction().is_none();
        if owned {
            self.begin_transaction("serializable")?;
        }
        Ok((
            self.current_transaction()
                .expect("security transaction was opened"),
            owned,
        ))
    }

    pub(crate) fn finish_security_transaction<T>(
        &mut self,
        owned: bool,
        result: Result<T>,
    ) -> Result<T> {
        if !owned {
            return result;
        }
        match result {
            Ok(value) => {
                self.commit_transaction()?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.rollback_transaction();
                Err(error)
            }
        }
    }

    pub(crate) fn security_evidence(
        &mut self,
        action: &str,
        outcome: &str,
        mut fields: BTreeMap<String, Value>,
    ) -> Result<()> {
        fields.insert(
            "application".to_string(),
            Value::String(self.host.application_name().to_string()),
        );
        fields.insert(
            "trace_id".to_string(),
            Value::String(self.host.actor().trace_id.clone()),
        );
        self.host_call(HostRequest::Observe(ObserveRequest::Evidence {
            control: action.to_string(),
            outcome: outcome.to_string(),
            fields: fields.clone(),
        }))?;
        self.host_call(HostRequest::Observe(ObserveRequest::Audit {
            action: action.to_string(),
            subject: fields
                .get("subject")
                .and_then(Value::as_str)
                .unwrap_or("carrier-security")
                .to_string(),
            fields,
        }))?;
        Ok(())
    }

    pub(crate) fn security_user_value(user: &ApplicationSecurityUserV1) -> Value {
        json!({
            "id": user.id,
            "email": user.email,
            "name": user.name,
            "roles": user.roles,
            "scopes": [],
            "tenant_id": null,
            "workspace_id": null,
        })
    }

    pub(crate) fn security_user_email_key(email: &str) -> String {
        format!(
            "user-email:{}",
            carrier_idempotency_sha256(email.as_bytes())
        )
    }

    pub(crate) fn auth_register(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.security_contract(target)?;
        let email = carrier_string(Self::security_named(arguments, "email", target)?, target)?
            .trim()
            .to_ascii_lowercase();
        let name = carrier_string(Self::security_named(arguments, "name", target)?, target)?;
        let password =
            carrier_string(Self::security_named(arguments, "password", target)?, target)?;
        if email.is_empty() || name.trim().is_empty() {
            return Err(AppRuntimeError::InvalidRequest(
                "auth.register requires a non-empty email and name".to_string(),
            ));
        }
        carrier_validate_password(&password)?;
        let password_hash = self.security_password_hash(&password, target)?;
        let id = loop {
            let bytes: [u8; 8] = self.random_bytes(target, 8)?.try_into().map_err(|_| {
                AppRuntimeError::Invocation(
                    "BicDB random host returned an invalid user id".to_string(),
                )
            })?;
            let candidate = (u64::from_le_bytes(bytes) & i64::MAX as u64).max(1) as i64;
            let (transaction, owned) = self.security_transaction()?;
            let result = (|| {
                let email_key = Self::security_user_email_key(&email);
                if self
                    .host
                    .read_security_state(transaction, &email_key)?
                    .is_some()
                {
                    return Err(AppRuntimeError::Conflict(
                        "auth user already exists".to_string(),
                    ));
                }
                let id_key = format!("user-id:{candidate}");
                if self
                    .host
                    .read_security_state(transaction, &id_key)?
                    .is_some()
                {
                    return Ok(false);
                }
                let user = ApplicationSecurityUserV1 {
                    id: candidate,
                    email: email.clone(),
                    name: name.clone(),
                    roles: BTreeSet::from(["user".to_string()]),
                    password_hash: password_hash.clone(),
                };
                let value = serde_json::to_value(&user)?;
                self.host.create_security_state(
                    transaction,
                    &email_key,
                    "auth-user-email",
                    value.clone(),
                )?;
                self.host
                    .create_security_state(transaction, &id_key, "auth-user-id", value)?;
                Ok(true)
            })();
            if self.finish_security_transaction(owned, result)? {
                break candidate;
            }
        };
        let user = ApplicationSecurityUserV1 {
            id,
            email,
            name,
            roles: BTreeSet::from(["user".to_string()]),
            password_hash,
        };
        self.security_evidence(
            "carrier.auth.register",
            "success",
            BTreeMap::from([
                ("subject".to_string(), Value::String(id.to_string())),
                (
                    "email_sha256".to_string(),
                    Value::String(carrier_idempotency_sha256(user.email.as_bytes())),
                ),
            ]),
        )?;
        Ok(Self::security_user_value(&user))
    }

    pub(crate) fn auth_login(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.security_contract(target)?;
        let email = carrier_string(Self::security_named(arguments, "email", target)?, target)?
            .trim()
            .to_ascii_lowercase();
        let password =
            carrier_string(Self::security_named(arguments, "password", target)?, target)?;
        let (transaction, owned) = self.security_transaction()?;
        let result = self
            .host
            .read_security_state(transaction, &Self::security_user_email_key(&email))?
            .ok_or_else(|| AppRuntimeError::Authentication("invalid credentials".to_string()))
            .and_then(|value| {
                serde_json::from_value::<ApplicationSecurityUserV1>(value).map_err(Into::into)
            })
            .and_then(|user| {
                carrier_verify_password(&user.password_hash, &password)?
                    .then_some(user)
                    .ok_or_else(|| {
                        AppRuntimeError::Authentication("invalid credentials".to_string())
                    })
            });
        let user = self.finish_security_transaction(owned, result)?;
        self.security_evidence(
            "carrier.auth.login",
            "success",
            BTreeMap::from([("subject".to_string(), Value::String(user.id.to_string()))]),
        )?;
        Ok(Self::security_user_value(&user))
    }

    pub(crate) fn auth_issue_tokens(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let contract = self.security_contract(target)?;
        let user_id = carrier_integer(carrier_argument(arguments, 0, None, target)?, target)?;
        let (transaction, owned) = self.security_transaction()?;
        let user = self
            .host
            .read_security_state(transaction, &format!("user-id:{user_id}"))?
            .ok_or_else(|| AppRuntimeError::Authentication("unknown auth user".to_string()))
            .and_then(|value| serde_json::from_value(value).map_err(Into::into));
        let user: ApplicationSecurityUserV1 = self.finish_security_transaction(owned, user)?;
        let now = self.security_now_seconds(arguments, target)?;
        let session_id = match self.random_token(target, 24)? {
            Value::String(value) => value,
            _ => unreachable!("random_token returns a string"),
        };
        self.create_security_token_pair(&contract, &user, &session_id, now)
    }

    pub(crate) fn create_security_token_pair(
        &mut self,
        contract: &ApplicationSecurityContractV1,
        user: &ApplicationSecurityUserV1,
        session_id: &str,
        now: i64,
    ) -> Result<Value> {
        let token_id = match self.random_token("auth.issue_tokens", 24)? {
            Value::String(value) => value,
            _ => unreachable!("random_token returns a string"),
        };
        let access = self.security_jwt(
            contract,
            user,
            session_id,
            &token_id,
            "access",
            now.saturating_add(contract.access_ttl_seconds as i64),
        )?;
        let refresh_exp = now.saturating_add(contract.refresh_ttl_seconds as i64);
        let refresh = self.security_jwt(
            contract,
            user,
            session_id,
            &token_id,
            "refresh",
            refresh_exp,
        )?;
        let (transaction, owned) = self.security_transaction()?;
        let result = self.host.create_security_state(
            transaction,
            &format!("refresh:{}", carrier_idempotency_sha256(refresh.as_bytes())),
            "refresh-session",
            json!({
                "user_id": user.id,
                "session_id": session_id,
                "expires_at": refresh_exp,
                "created_at": now,
            }),
        );
        self.finish_security_transaction(owned, result)?;
        self.security_evidence(
            "carrier.auth.tokens.issue",
            "success",
            BTreeMap::from([
                ("subject".to_string(), Value::String(user.id.to_string())),
                (
                    "session_id".to_string(),
                    Value::String(session_id.to_string()),
                ),
            ]),
        )?;
        Ok(json!({"access_token": access, "refresh_token": refresh}))
    }

    pub(crate) fn security_jwt(
        &mut self,
        contract: &ApplicationSecurityContractV1,
        user: &ApplicationSecurityUserV1,
        session_id: &str,
        token_id: &str,
        token_type: &str,
        expires_at: i64,
    ) -> Result<String> {
        let scheme_name = contract.auth_scheme.as_deref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "security token contract has no auth scheme".to_string(),
            )
        })?;
        let scheme = self
            .host
            .application()
            .auth_schemes
            .get(scheme_name)
            .cloned()
            .ok_or_else(|| AppRuntimeError::InvalidPackage("auth scheme is absent".to_string()))?;
        let secret_name = contract.signing_secret.as_deref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "security token contract has no signing secret".to_string(),
            )
        })?;
        let secret = self.secret_handle(secret_name)?;
        let metadata = match self
            .host_call(HostRequest::Secret(SecretRequest::Metadata { secret }))?
        {
            HostValue::SecretMetadata(metadata) => metadata,
            other => return carrier_host_type_error("auth.issue_tokens", "secret metadata", other),
        };
        let algorithm = contract.signing_algorithm.as_deref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "security token contract has no signing algorithm".to_string(),
            )
        })?;
        let header = json!({
            "alg": if algorithm == "ed25519" { "EdDSA" } else { "HS256" },
            "typ": "JWT",
            "kid": metadata.key_id,
            "bicdb_key_version": metadata.version,
        });
        let claims = json!({
            "sub": user.id.to_string(),
            "email": user.email,
            "name": user.name,
            "policy": {"email": user.email, "name": user.name},
            "roles": user.roles,
            "scopes": [],
            "session_id": session_id,
            "jti": token_id,
            "iss": scheme.issuer,
            "aud": scheme.audience,
            "exp": expires_at,
            "token_type": token_type,
        });
        let encoded_header =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header)?);
        let encoded_claims =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
        let message = format!("{encoded_header}.{encoded_claims}");
        let signature = match self.host_call(HostRequest::Crypto(CryptoRequest::Sign {
            secret,
            algorithm: algorithm.to_string(),
            message: message.as_bytes().to_vec(),
        }))? {
            HostValue::Bytes(value) => value,
            other => return carrier_host_type_error("auth.issue_tokens", "signature bytes", other),
        };
        Ok(format!(
            "{message}.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature)
        ))
    }

    pub(crate) fn verify_security_refresh_token(
        &mut self,
        contract: &ApplicationSecurityContractV1,
        token: &str,
        now: i64,
    ) -> Result<crate::JwtClaims> {
        let mut parts = token.split('.');
        let (Some(encoded_header), Some(encoded_claims), Some(encoded_signature), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(AppRuntimeError::Authentication(
                "refresh token must contain three JWT segments".to_string(),
            ));
        };
        let decode = |value: &str| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(value)
                .map_err(|_| {
                    AppRuntimeError::Authentication(
                        "refresh token contains invalid base64url".to_string(),
                    )
                })
        };
        let header: Value = serde_json::from_slice(&decode(encoded_header)?)?;
        let algorithm = contract.signing_algorithm.as_deref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "security refresh contract has no signing algorithm".to_string(),
            )
        })?;
        let expected_jwt_algorithm = if algorithm == "ed25519" {
            "EdDSA"
        } else {
            "HS256"
        };
        if header.get("alg").and_then(Value::as_str) != Some(expected_jwt_algorithm)
            || header.get("typ").and_then(Value::as_str) != Some("JWT")
        {
            return Err(AppRuntimeError::Authentication(
                "refresh token algorithm or type is invalid".to_string(),
            ));
        }
        let key_version = header
            .get("bicdb_key_version")
            .or_else(|| header.get("carrier_key_version"))
            .and_then(Value::as_str)
            .filter(|version| !version.is_empty())
            .ok_or_else(|| {
                AppRuntimeError::Authentication(
                    "refresh token lacks a versioned signing key".to_string(),
                )
            })?;
        let secret_name = contract.signing_secret.as_deref().ok_or_else(|| {
            AppRuntimeError::InvalidPackage(
                "security refresh contract has no signing secret".to_string(),
            )
        })?;
        let secret = self.secret_handle_version(secret_name, Some(key_version))?;
        let message = format!("{encoded_header}.{encoded_claims}");
        let signature = decode(encoded_signature)?;
        let verified = match self.host_call(HostRequest::Crypto(CryptoRequest::Verify {
            secret,
            algorithm: algorithm.to_string(),
            message: message.into_bytes(),
            signature,
        }))? {
            HostValue::Bool(value) => value,
            other => return carrier_host_type_error("auth.refresh", "verification result", other),
        };
        if !verified {
            return Err(AppRuntimeError::Authentication(
                "refresh token signature is invalid".to_string(),
            ));
        }
        let claims: crate::JwtClaims = serde_json::from_slice(&decode(encoded_claims)?)?;
        let scheme = contract
            .auth_scheme
            .as_deref()
            .and_then(|scheme| self.host.application().auth_schemes.get(scheme))
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(
                    "security refresh contract references no auth scheme".to_string(),
                )
            })?;
        let audience_matches = match &claims.aud {
            crate::JwtAudience::One(value) => value == &scheme.audience,
            crate::JwtAudience::Many(values) => values.contains(&scheme.audience),
        };
        if claims.iss != scheme.issuer
            || !audience_matches
            || claims.token_type.as_deref() != Some("refresh")
            || claims.exp < now
            || claims.sub.trim().is_empty()
            || claims.session_id.as_deref().is_none_or(str::is_empty)
        {
            return Err(AppRuntimeError::Authentication(
                "refresh token claims violate the signed session policy".to_string(),
            ));
        }
        Ok(claims)
    }

    pub(crate) fn auth_refresh(&mut self, refresh_token: &str) -> Result<Value> {
        let contract = self.security_contract("auth.issue_tokens")?;
        let now = self.security_now_seconds(&[], "auth.refresh")?;
        let claims = self.verify_security_refresh_token(&contract, refresh_token, now)?;
        let user_id = claims.sub.parse::<i64>().map_err(|_| {
            AppRuntimeError::Authentication(
                "refresh token subject is not a BicDB application user".to_string(),
            )
        })?;
        let session_id = claims.session_id.ok_or_else(|| {
            AppRuntimeError::Authentication("refresh token has no session".to_string())
        })?;
        let (transaction, owned) = self.security_transaction()?;
        let result = (|| {
            let stored = self
                .host
                .consume_security_state(
                    transaction,
                    &format!(
                        "refresh:{}",
                        carrier_idempotency_sha256(refresh_token.as_bytes())
                    ),
                    "refresh-session",
                )?
                .ok_or_else(|| {
                    AppRuntimeError::Authentication(
                        "refresh token is revoked, expired, or already rotated".to_string(),
                    )
                })?;
            if stored.get("user_id").and_then(Value::as_i64) != Some(user_id)
                || stored.get("session_id").and_then(Value::as_str) != Some(session_id.as_str())
                || stored
                    .get("expires_at")
                    .and_then(Value::as_i64)
                    .unwrap_or_default()
                    < now
            {
                return Err(AppRuntimeError::Authentication(
                    "refresh token does not match its durable session".to_string(),
                ));
            }
            let user = self
                .host
                .read_security_state(transaction, &format!("user-id:{user_id}"))?
                .ok_or_else(|| {
                    AppRuntimeError::Authentication(
                        "refresh token user no longer exists".to_string(),
                    )
                })?;
            let user: ApplicationSecurityUserV1 = serde_json::from_value(user)?;
            self.create_security_token_pair(&contract, &user, &session_id, now)
        })();
        let tokens = self.finish_security_transaction(owned, result)?;
        self.security_evidence(
            "carrier.auth.session.refresh",
            "success",
            BTreeMap::from([
                ("subject".to_string(), Value::String(user_id.to_string())),
                ("session_id".to_string(), Value::String(session_id)),
            ]),
        )?;
        Ok(tokens)
    }

    pub(crate) fn auth_sessions(&mut self, actor: &ActorContext) -> Result<Value> {
        self.security_contract("auth.issue_tokens")?;
        let user_id = actor.user_id.as_deref().ok_or_else(|| {
            AppRuntimeError::Authentication("session listing requires a user".to_string())
        })?;
        let now = self.security_now_seconds(&[], "auth.sessions")?;
        let (transaction, owned) = self.security_transaction()?;
        let result = self
            .host
            .list_security_state(transaction, "refresh-session");
        let records = self.finish_security_transaction(owned, result)?;
        let mut sessions = BTreeMap::<String, Value>::new();
        for (_, record) in records {
            if record
                .get("user_id")
                .and_then(Value::as_i64)
                .map(|id| id.to_string())
                != Some(user_id.to_string())
                || record
                    .get("expires_at")
                    .and_then(Value::as_i64)
                    .unwrap_or_default()
                    < now
            {
                continue;
            }
            if let Some(session_id) = record.get("session_id").and_then(Value::as_str) {
                sessions.insert(
                    session_id.to_string(),
                    json!({
                        "id": session_id,
                        "created_at": record.get("created_at").cloned().unwrap_or(Value::Null),
                        "expires_at": record.get("expires_at").cloned().unwrap_or(Value::Null),
                        "current": actor.session_id.as_deref() == Some(session_id),
                    }),
                );
            }
        }
        Ok(Value::Array(sessions.into_values().collect()))
    }

    pub(crate) fn revoke_auth_session(
        &mut self,
        actor: &ActorContext,
        session_id: &str,
    ) -> Result<Value> {
        self.security_contract("auth.issue_tokens")?;
        let user_id = actor.user_id.as_deref().ok_or_else(|| {
            AppRuntimeError::Authentication("session revocation requires a user".to_string())
        })?;
        if session_id.trim().is_empty() {
            return Err(AppRuntimeError::InvalidRequest(
                "session revocation requires a session id".to_string(),
            ));
        }
        let (transaction, owned) = self.security_transaction()?;
        let result = (|| {
            let records = self
                .host
                .list_security_state(transaction, "refresh-session")?;
            let mut revoked = 0_u64;
            for (storage_id, record) in records {
                if record
                    .get("user_id")
                    .and_then(Value::as_i64)
                    .map(|id| id.to_string())
                    == Some(user_id.to_string())
                    && record.get("session_id").and_then(Value::as_str) == Some(session_id)
                    && self.host.delete_security_state_record(
                        transaction,
                        &storage_id,
                        "refresh-session",
                    )?
                {
                    revoked = revoked.saturating_add(1);
                }
            }
            if revoked == 0 {
                return Err(AppRuntimeError::NotFound(
                    "session was not found or does not belong to the actor".to_string(),
                ));
            }
            Ok(revoked)
        })();
        let revoked = self.finish_security_transaction(owned, result)?;
        self.security_evidence(
            "carrier.auth.session.revoke",
            "success",
            BTreeMap::from([
                ("subject".to_string(), Value::String(user_id.to_string())),
                (
                    "session_id".to_string(),
                    Value::String(session_id.to_string()),
                ),
                ("refresh_tokens".to_string(), json!(revoked)),
            ]),
        )?;
        Ok(json!({"ok": true, "revoked_refresh_tokens": revoked}))
    }

    pub(crate) fn auth_password_policy(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.security_contract(target)?;
        let password =
            carrier_string(Self::security_named(arguments, "password", target)?, target)?;
        Ok(carrier_password_policy(&password))
    }

    pub(crate) fn security_password_hash(
        &mut self,
        password: &str,
        target: &str,
    ) -> Result<String> {
        let random = self.random_bytes(target, 16)?;
        let salt = SaltString::encode_b64(&random)
            .map_err(|error| AppRuntimeError::Invocation(error.to_string()))?;
        Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            .map_err(|error| {
                AppRuntimeError::Invocation(format!("password hashing failed: {error}"))
            })
    }

    pub(crate) fn auth_password_hash(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.security_contract(target)?;
        let password =
            carrier_string(Self::security_named(arguments, "password", target)?, target)?;
        carrier_validate_password(&password)?;
        self.security_password_hash(&password, target)
            .map(Value::String)
    }

    pub(crate) fn auth_password_verify(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.security_contract(target)?;
        let password_hash = carrier_string(
            Self::security_named(arguments, "password_hash", target)?,
            target,
        )?;
        let password =
            carrier_string(Self::security_named(arguments, "password", target)?, target)?;
        Ok(Value::Bool(carrier_verify_password(
            &password_hash,
            &password,
        )?))
    }

    pub(crate) fn auth_password_breach_digest(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.security_contract(target)?;
        let password =
            carrier_string(Self::security_named(arguments, "password", target)?, target)?;
        let digest = Sha1::digest(password.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>();
        Ok(json!({"prefix": &digest[..5], "suffix": &digest[5..]}))
    }

    pub(crate) fn auth_totp_secret(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.security_contract(target)?;
        if !arguments.is_empty() {
            return Err(AppRuntimeError::InvalidRequest(
                "auth.totp_secret takes no arguments".to_string(),
            ));
        }
        Ok(Value::String(carrier_base32_encode(
            &self.random_bytes(target, 20)?,
        )))
    }

    pub(crate) fn auth_totp_code(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.security_contract(target)?;
        let secret = carrier_string(Self::security_named(arguments, "secret", target)?, target)?;
        let now = self.security_now_seconds(arguments, target)?;
        let period = self
            .security_optional_i64(arguments, "period_seconds", target)?
            .unwrap_or(30);
        let digits = self
            .security_optional_i64(arguments, "digits", target)?
            .unwrap_or(6);
        carrier_totp_code(&secret, now, period, digits).map(Value::String)
    }

    pub(crate) fn auth_totp_verify(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.security_contract(target)?;
        let secret = carrier_string(Self::security_named(arguments, "secret", target)?, target)?;
        let code = carrier_string(Self::security_named(arguments, "code", target)?, target)?;
        let now = self.security_now_seconds(arguments, target)?;
        let period = self
            .security_optional_i64(arguments, "period_seconds", target)?
            .unwrap_or(30);
        let digits = self
            .security_optional_i64(arguments, "digits", target)?
            .unwrap_or(6);
        let drift = self
            .security_optional_i64(arguments, "allowed_drift_windows", target)?
            .unwrap_or(1);
        if !(0..=10).contains(&drift) {
            return Err(AppRuntimeError::InvalidRequest(
                "auth.totp_verify allowed_drift_windows must be between 0 and 10".to_string(),
            ));
        }
        for offset in -drift..=drift {
            let candidate = carrier_totp_code(
                &secret,
                now.saturating_add(offset.saturating_mul(period)),
                period,
                digits,
            )?;
            if carrier_constant_time_equal(candidate.as_bytes(), code.as_bytes()) {
                return Ok(Value::Bool(true));
            }
        }
        Ok(Value::Bool(false))
    }

    pub(crate) fn auth_totp_uri(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.security_contract(target)?;
        let secret = carrier_string(Self::security_named(arguments, "secret", target)?, target)?;
        let account = carrier_string(
            Self::security_named(arguments, "account_name", target)?,
            target,
        )?;
        let issuer = carrier_string(Self::security_named(arguments, "issuer", target)?, target)?;
        let period = self
            .security_optional_i64(arguments, "period_seconds", target)?
            .unwrap_or(30);
        let digits = self
            .security_optional_i64(arguments, "digits", target)?
            .unwrap_or(6);
        carrier_validate_totp(period, digits)?;
        let mut url = Url::parse(&format!(
            "otpauth://totp/{}:{}",
            url::form_urlencoded::byte_serialize(issuer.as_bytes()).collect::<String>(),
            url::form_urlencoded::byte_serialize(account.as_bytes()).collect::<String>()
        ))
        .map_err(|error| AppRuntimeError::InvalidRequest(error.to_string()))?;
        url.query_pairs_mut()
            .append_pair("secret", &secret)
            .append_pair("issuer", &issuer)
            .append_pair("algorithm", "SHA1")
            .append_pair("digits", &digits.to_string())
            .append_pair("period", &period.to_string());
        Ok(Value::String(url.to_string()))
    }

    pub(crate) fn auth_magic_link_issue(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let contract = self.security_contract(target)?;
        let subject = carrier_string(Self::security_named(arguments, "subject", target)?, target)?;
        let email = carrier_string(Self::security_named(arguments, "email", target)?, target)?
            .trim()
            .to_ascii_lowercase();
        let secret_name =
            carrier_string(Self::security_named(arguments, "secret", target)?, target)?;
        if !contract.magic_link_secrets.contains(&secret_name) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "magic-link secret `{secret_name}` is outside the signed security contract"
            )));
        }
        let now = self.security_now_seconds(arguments, target)?;
        let ttl = self
            .security_optional_i64(arguments, "ttl_seconds", target)?
            .unwrap_or(900);
        if ttl <= 0 {
            return Err(AppRuntimeError::InvalidRequest(
                "auth.magic_link_issue ttl_seconds must be positive".to_string(),
            ));
        }
        let secret = self.secret_handle(&secret_name)?;
        let metadata = self.secret_metadata(secret, target)?;
        let jti = match self.random_token(target, 24)? {
            Value::String(value) => value,
            _ => unreachable!("random_token returns a string"),
        };
        let claims = ApplicationMagicLinkClaimsV1 {
            sub: subject.clone(),
            email,
            exp: now.saturating_add(ttl),
            iat: now,
            jti,
            token_type: "magic_link".to_string(),
            key_version: metadata.version.clone(),
        };
        let payload =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
        let signature = match self.host_call(HostRequest::Crypto(CryptoRequest::Hmac {
            secret,
            algorithm: "hmac-sha256".to_string(),
            message: payload.as_bytes().to_vec(),
        }))? {
            HostValue::Bytes(value) => value,
            other => return carrier_host_type_error(target, "HMAC bytes", other),
        };
        let token = format!(
            "{payload}.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature)
        );
        self.security_evidence(
            "carrier.auth.magic_link.issue",
            "success",
            BTreeMap::from([
                ("subject".to_string(), Value::String(subject)),
                ("key_id".to_string(), Value::String(metadata.key_id)),
                ("key_version".to_string(), Value::String(metadata.version)),
            ]),
        )?;
        Ok(json!({
            "token": token,
            "expires_at": carrier_time_string(claims.exp)?,
        }))
    }

    pub(crate) fn auth_magic_link_verify(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let contract = self.security_contract(target)?;
        let token = carrier_string(Self::security_named(arguments, "token", target)?, target)?;
        let secret_name =
            carrier_string(Self::security_named(arguments, "secret", target)?, target)?;
        if !contract.magic_link_secrets.contains(&secret_name) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "magic-link secret `{secret_name}` is outside the signed security contract"
            )));
        }
        let (payload, encoded_signature) = token
            .split_once('.')
            .ok_or_else(|| AppRuntimeError::Authentication("invalid magic link".to_string()))?;
        let claims: ApplicationMagicLinkClaimsV1 = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .ok_or_else(|| AppRuntimeError::Authentication("invalid magic link".to_string()))?;
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded_signature)
            .map_err(|_| AppRuntimeError::Authentication("invalid magic link".to_string()))?;
        let secret = self.secret_handle_version(&secret_name, Some(&claims.key_version))?;
        let valid = match self.host_call(HostRequest::Crypto(CryptoRequest::Verify {
            secret,
            algorithm: "hmac-sha256".to_string(),
            message: payload.as_bytes().to_vec(),
            signature,
        }))? {
            HostValue::Bool(value) => value,
            other => return carrier_host_type_error(target, "signature validity", other),
        };
        let now = self.security_now_seconds(arguments, target)?;
        if !valid || claims.token_type != "magic_link" || claims.exp < now {
            return Err(AppRuntimeError::Authentication(
                "invalid or expired magic link".to_string(),
            ));
        }
        let (transaction, owned) = self.security_transaction()?;
        let result = self.host.create_security_state(
            transaction,
            &format!("magic-used:{}", claims.jti),
            "magic-link-replay",
            json!({"expires_at": claims.exp}),
        );
        self.finish_security_transaction(owned, result)
            .map_err(|error| match error {
                AppRuntimeError::Conflict(_) | AppRuntimeError::OptimisticConflict(_) => {
                    AppRuntimeError::Authentication("magic link already used".to_string())
                }
                other => other,
            })?;
        let metadata = self.secret_metadata(secret, target)?;
        self.security_evidence(
            "carrier.auth.magic_link.verify",
            "success",
            BTreeMap::from([
                ("subject".to_string(), Value::String(claims.sub.clone())),
                ("key_id".to_string(), Value::String(metadata.key_id)),
                ("key_version".to_string(), Value::String(metadata.version)),
            ]),
        )?;
        Ok(json!({
            "subject": claims.sub,
            "email": claims.email,
            "expires_at": carrier_time_string(claims.exp)?,
        }))
    }

    pub(crate) fn secret_metadata(
        &mut self,
        secret: HostHandle,
        target: &str,
    ) -> Result<SecretMetadata> {
        match self.host_call(HostRequest::Secret(SecretRequest::Metadata { secret }))? {
            HostValue::SecretMetadata(value) => Ok(value),
            other => carrier_host_type_error(target, "secret metadata", other),
        }
    }

    pub(crate) fn auth_oauth_authorize(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.security_contract(target)?;
        let authorize_url = carrier_string(
            Self::security_named(arguments, "authorize_url", target)?,
            target,
        )?;
        let client_id = carrier_string(
            Self::security_named(arguments, "client_id", target)?,
            target,
        )?;
        let redirect_uri = carrier_string(
            Self::security_named(arguments, "redirect_uri", target)?,
            target,
        )?;
        let scopes = Self::security_named(arguments, "scopes", target)?
            .as_array()
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest("OAuth scopes must be an array".to_string())
            })?
            .iter()
            .map(|value| carrier_string(value.clone(), target))
            .collect::<Result<Vec<_>>>()?;
        let now = self.security_now_seconds(arguments, target)?;
        let ttl = self
            .security_optional_i64(arguments, "ttl_seconds", target)?
            .unwrap_or(600);
        if ttl <= 0 {
            return Err(AppRuntimeError::InvalidRequest(
                "auth.oauth_authorize ttl_seconds must be positive".to_string(),
            ));
        }
        let state = match self.random_token(target, 32)? {
            Value::String(value) => value,
            _ => unreachable!("random_token returns a string"),
        };
        let verifier = match self.random_token(target, 32)? {
            Value::String(value) => value,
            _ => unreachable!("random_token returns a string"),
        };
        let nonce = match self.random_token(target, 32)? {
            Value::String(value) => value,
            _ => unreachable!("random_token returns a string"),
        };
        let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(Sha256::digest(verifier.as_bytes()));
        let mut url = Url::parse(&authorize_url).map_err(|error| {
            AppRuntimeError::InvalidRequest(format!("invalid authorize_url: {error}"))
        })?;
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("response_type", "code")
                .append_pair("client_id", &client_id)
                .append_pair("redirect_uri", &redirect_uri)
                .append_pair("scope", &scopes.join(" "))
                .append_pair("state", &state)
                .append_pair("code_challenge", &challenge)
                .append_pair("code_challenge_method", "S256")
                .append_pair("nonce", &nonce);
            if let Some(extra) = arguments
                .iter()
                .find(|(name, _)| name.as_deref() == Some("extra_params"))
                .map(|(_, value)| value)
            {
                carrier_append_oauth_params(&mut query, extra)?;
            }
        }
        let expires_at = now.saturating_add(ttl);
        let record = ApplicationOauthStateV1 {
            pkce_verifier: verifier.clone(),
            nonce: nonce.clone(),
            expires_at,
        };
        let (transaction, owned) = self.security_transaction()?;
        let result = self.host.create_security_state(
            transaction,
            &format!("oauth-state:{state}"),
            "oauth-state",
            serde_json::to_value(record)?,
        );
        self.finish_security_transaction(owned, result)?;
        self.security_evidence(
            "carrier.auth.oauth.authorize",
            "success",
            BTreeMap::from([(
                "subject".to_string(),
                Value::String(carrier_idempotency_sha256(state.as_bytes())),
            )]),
        )?;
        Ok(json!({
            "url": url.to_string(),
            "state": state,
            "pkce_verifier": verifier,
            "nonce": nonce,
            "expires_at": carrier_time_string(expires_at)?,
        }))
    }

    pub(crate) fn auth_oauth_callback(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.security_contract(target)?;
        let query = Self::security_named(arguments, "query", target)?;
        let query = query.as_object().ok_or_else(|| {
            AppRuntimeError::InvalidRequest(
                "auth.oauth_callback query must be an object".to_string(),
            )
        })?;
        let state = query
            .get("state")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| AppRuntimeError::Authentication("missing oauth state".to_string()))?;
        let (transaction, owned) = self.security_transaction()?;
        let result = self
            .host
            .consume_security_state(transaction, &format!("oauth-state:{state}"), "oauth-state")?
            .ok_or_else(|| AppRuntimeError::Authentication("invalid oauth state".to_string()))
            .and_then(|value| serde_json::from_value(value).map_err(Into::into));
        let record: ApplicationOauthStateV1 = self.finish_security_transaction(owned, result)?;
        let now = self.security_now_seconds(arguments, target)?;
        if record.expires_at < now {
            return Err(AppRuntimeError::Authentication(
                "oauth state expired".to_string(),
            ));
        }
        if let Some(error) = query.get("error").and_then(Value::as_str) {
            let description = query
                .get("error_description")
                .and_then(Value::as_str)
                .unwrap_or("provider callback failed");
            return Err(AppRuntimeError::Authentication(format!(
                "oauth provider returned `{error}`: {description}"
            )));
        }
        let code = query
            .get("code")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| AppRuntimeError::Authentication("missing oauth code".to_string()))?
            .to_string();
        self.security_evidence(
            "carrier.auth.oauth.callback",
            "success",
            BTreeMap::from([(
                "subject".to_string(),
                Value::String(carrier_idempotency_sha256(state.as_bytes())),
            )]),
        )?;
        Ok(json!({
            "code": code,
            "pkce_verifier": record.pkce_verifier,
            "nonce": record.nonce,
        }))
    }

    pub(crate) fn runtime_cache_storage_key(&self, key: &str) -> Result<String> {
        let actor = self.host.actor();
        let scope = carrier_canonical_json(&json!({
            "application": self.host.application_name(),
            "tenant_id": actor.tenant_id.as_deref(),
            "workspace_id": actor.workspace_id.as_deref(),
            "key": key,
        }));
        Ok(carrier_idempotency_sha256(&serde_json::to_vec(&scope)?))
    }

    pub(crate) fn runtime_cache_transaction(&mut self) -> Result<(HostHandle, bool)> {
        let owned = self.current_transaction().is_none();
        if owned {
            self.begin_transaction("read_committed")?;
        }
        Ok((
            self.current_transaction()
                .expect("runtime cache transaction was opened"),
            owned,
        ))
    }

    pub(crate) fn finish_runtime_cache_transaction<T>(
        &mut self,
        owned: bool,
        result: Result<T>,
    ) -> Result<T> {
        if !owned {
            return result;
        }
        match result {
            Ok(value) => {
                self.commit_transaction()?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.rollback_transaction();
                Err(error)
            }
        }
    }

    pub(crate) fn runtime_cache_get(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        if arguments.len() != 2 || arguments.iter().any(|(name, _)| name.is_some()) {
            return Err(AppRuntimeError::InvalidRequest(
                "cache.get_as requires a target type and key".to_string(),
            ));
        }
        let key = carrier_string(arguments[1].1.clone(), target)?;
        let storage_key = self.runtime_cache_storage_key(&key)?;
        let (transaction, owned) = self.runtime_cache_transaction()?;
        let result = self
            .host
            .read_runtime_cache(transaction, &storage_key)
            .and_then(|value| {
                value.ok_or_else(|| AppRuntimeError::NotFound("cache entry not found".to_string()))
            });
        self.finish_runtime_cache_transaction(owned, result)
    }

    pub(crate) fn runtime_cache_set(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        if !(2..=3).contains(&arguments.len())
            || arguments[0].0.is_some()
            || arguments[1].0.is_some()
            || arguments
                .get(2)
                .is_some_and(|(name, _)| name.as_deref() != Some("ttl_seconds"))
        {
            return Err(AppRuntimeError::InvalidRequest(
                "cache.set requires key, value, and optional named ttl_seconds".to_string(),
            ));
        }
        let key = carrier_string(arguments[0].1.clone(), target)?;
        let value = arguments[1].1.clone();
        let ttl_seconds = arguments
            .get(2)
            .map(|(_, value)| carrier_integer(value.clone(), target))
            .transpose()?;
        let expires_at_ms = ttl_seconds.filter(|ttl| *ttl > 0).map(|ttl| {
            crate::host::now_ms().saturating_add(ttl.checked_mul(1_000).unwrap_or(i64::MAX))
        });
        let storage_key = self.runtime_cache_storage_key(&key)?;
        let (transaction, owned) = self.runtime_cache_transaction()?;
        let result = self
            .host
            .write_runtime_cache(transaction, &storage_key, expires_at_ms, value)
            .map(|_| Value::Null);
        self.finish_runtime_cache_transaction(owned, result)
    }

    pub(crate) fn runtime_cache_delete(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        if arguments.len() != 1 || arguments[0].0.is_some() {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "{target} requires one cache key"
            )));
        }
        let key = carrier_string(arguments[0].1.clone(), target)?;
        let storage_key = self.runtime_cache_storage_key(&key)?;
        let (transaction, owned) = self.runtime_cache_transaction()?;
        let result = self
            .host
            .delete_runtime_cache(transaction, &storage_key)
            .map(Value::Bool);
        self.finish_runtime_cache_transaction(owned, result)
    }

    pub(crate) fn runtime_cache_exists(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        if arguments.len() != 1 || arguments[0].0.is_some() {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "{target} requires one cache key"
            )));
        }
        let key = carrier_string(arguments[0].1.clone(), target)?;
        let storage_key = self.runtime_cache_storage_key(&key)?;
        let (transaction, owned) = self.runtime_cache_transaction()?;
        let result = self
            .host
            .read_runtime_cache(transaction, &storage_key)
            .map(|value| Value::Bool(value.is_some()));
        self.finish_runtime_cache_transaction(owned, result)
    }

    pub(crate) fn current_user(&self) -> Result<Value> {
        let actor = self.host.actor();
        let subject = actor.user_id.as_ref().ok_or_else(|| {
            AppRuntimeError::CapabilityDenied(
                "auth.current_user requires an authenticated user actor".to_string(),
            )
        })?;
        let id = subject.parse::<i64>().map_err(|_| {
            AppRuntimeError::InvalidRequest(
                "BicDB application CurrentUser.id requires a numeric BicDB user subject"
                    .to_string(),
            )
        })?;
        Ok(json!({
            "id": id,
            "email": actor.policy_attributes.get("email").cloned().unwrap_or_default(),
            "name": actor.policy_attributes.get("name").cloned().unwrap_or_default(),
            "roles": actor.roles,
            "scopes": actor.scopes,
            "tenant_id": actor.tenant_id,
            "workspace_id": actor.workspace_id,
        }))
    }

    pub(crate) fn wall_time(&mut self, date_only: bool) -> Result<Value> {
        let milliseconds = self.wall_time_milliseconds()?;
        let value = DateTime::<Utc>::from_timestamp_millis(milliseconds).ok_or_else(|| {
            AppRuntimeError::Invocation("BicDB clock returned an invalid timestamp".to_string())
        })?;
        Ok(Value::String(if date_only {
            value.date_naive().to_string()
        } else {
            value.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        }))
    }

    pub(crate) fn wall_time_milliseconds(&mut self) -> Result<i64> {
        match self.host_call(HostRequest::Clock(ClockRequest::WallTime))? {
            HostValue::I64(value) => Ok(value),
            other => carrier_host_type_error("time", "integer milliseconds", other),
        }
    }

    pub(crate) fn random_bytes(&mut self, target: &str, len: usize) -> Result<Vec<u8>> {
        let len = u32::try_from(len).map_err(|_| {
            AppRuntimeError::InvalidRequest(format!("builtin `{target}` size is too large"))
        })?;
        match self.host_call(HostRequest::Random(RandomRequest::Bytes { len }))? {
            HostValue::Bytes(value) => Ok(value),
            other => carrier_host_type_error(target, "bytes", other),
        }
    }

    pub(crate) fn random_token(&mut self, target: &str, len: usize) -> Result<Value> {
        Ok(Value::String(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(self.random_bytes(target, len)?),
        ))
    }

    pub(crate) fn random_u64(&mut self, target: &str) -> Result<u64> {
        let bytes: [u8; 8] = self.random_bytes(target, 8)?.try_into().map_err(|_| {
            AppRuntimeError::Invocation(
                "BicDB random host returned the wrong byte count".to_string(),
            )
        })?;
        Ok(u64::from_le_bytes(bytes))
    }

    pub(crate) fn random_int(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let min = carrier_integer(carrier_argument(arguments, 0, None, target)?, target)?;
        let max = carrier_integer(carrier_argument(arguments, 1, None, target)?, target)?;
        if min > max {
            return Err(AppRuntimeError::InvalidRequest(
                "random.int min must be less than or equal to max".to_string(),
            ));
        }
        let span = (i128::from(max) - i128::from(min) + 1) as u128;
        let sample = u128::from(self.random_u64(target)?) % span;
        Ok(Value::from((i128::from(min) + sample as i128) as i64))
    }

    pub(crate) fn random_float(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let unit = (self.random_u64(target)? >> 11) as f64 / (1u64 << 53) as f64;
        let value = match arguments {
            [] => unit,
            [_, _] => {
                let min = carrier_number(carrier_argument(arguments, 0, None, target)?, target)?;
                let max = carrier_number(carrier_argument(arguments, 1, None, target)?, target)?;
                if !min.is_finite() || !max.is_finite() || min >= max {
                    return Err(AppRuntimeError::InvalidRequest(
                        "random.float range bounds must be finite and min must be less than max"
                            .to_string(),
                    ));
                }
                min + unit * (max - min)
            }
            _ => {
                return Err(AppRuntimeError::InvalidRequest(
                    "random.float expects zero or two arguments".to_string(),
                ))
            }
        };
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest("random.float was non-finite".to_string())
            })
    }

    pub(crate) fn random_choice(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        let items = carrier_argument(arguments, 0, None, target)?
            .as_array()
            .cloned()
            .ok_or_else(|| {
                AppRuntimeError::InvalidRequest("random.choice requires an array".to_string())
            })?;
        if items.is_empty() {
            return Err(AppRuntimeError::InvalidRequest(
                "random.choice requires a non-empty array".to_string(),
            ));
        }
        Ok(items[(self.random_u64(target)? as usize) % items.len()].clone())
    }

    pub(crate) fn observe_log(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.require_observability_helper(target)?;
        let level = match target {
            "logs.debug" => LogLevel::Debug,
            "logs.info" => LogLevel::Info,
            "logs.warn" => LogLevel::Warn,
            _ => LogLevel::Error,
        };
        let message = carrier_string(carrier_argument(arguments, 0, None, target)?, target)?;
        let fields =
            carrier_optional_object(arguments.get(1).map(|argument| argument.1.clone()), target)?;
        self.host_call(HostRequest::Observe(ObserveRequest::Log {
            level,
            message,
            fields,
        }))?;
        Ok(Value::Null)
    }

    pub(crate) fn observe_trace(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.require_observability_helper(target)?;
        if !actor_trace_sampled(self.host.actor()) {
            return Ok(Value::Null);
        }
        let fields =
            carrier_optional_object(Some(carrier_argument(arguments, 0, None, target)?), target)?;
        self.host_call(HostRequest::Observe(ObserveRequest::TraceEvent {
            name: "carrier.annotate".to_string(),
            fields,
        }))?;
        Ok(Value::Null)
    }

    pub(crate) fn observe_metric(
        &mut self,
        target: &str,
        kind: MetricKind,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.require_observability_helper(target)?;
        let name = carrier_string(carrier_argument(arguments, 0, None, target)?, target)?;
        let contract = self.observability_contract()?;
        if !contract.dynamic_metric_names && !contract.metric_names.contains(&name) {
            return Err(AppRuntimeError::CapabilityDenied(format!(
                "BicDB application metric `{name}` is outside signed metric-name authority"
            )));
        }
        let (value, attributes) = match kind {
            MetricKind::Counter => match arguments.get(1).map(|argument| &argument.1) {
                None => (1.0, None),
                Some(Value::Object(_)) => {
                    (1.0, arguments.get(1).map(|argument| argument.1.clone()))
                }
                Some(value) => (
                    carrier_number(value.clone(), target)?,
                    arguments.get(2).map(|argument| argument.1.clone()),
                ),
            },
            MetricKind::Gauge => (
                carrier_number(carrier_argument(arguments, 1, None, target)?, target)?,
                arguments.get(2).map(|argument| argument.1.clone()),
            ),
            MetricKind::Histogram => unreachable!(),
        };
        if !value.is_finite() || (kind == MetricKind::Counter && value < 0.0) {
            return Err(AppRuntimeError::InvalidRequest(format!(
                "BicDB application metric `{name}` requires a finite value and counters cannot decrease"
            )));
        }
        let labels = carrier_optional_object(attributes, target)?
            .into_iter()
            .map(|(name, value)| Ok((name, carrier_display(&value))))
            .collect::<Result<BTreeMap<_, _>>>()?;
        self.host_call(HostRequest::Observe(ObserveRequest::Metric {
            name,
            kind,
            value,
            labels,
        }))?;
        Ok(Value::Null)
    }

    pub(crate) fn observe_audit(
        &mut self,
        target: &str,
        arguments: &[(Option<String>, Value)],
    ) -> Result<Value> {
        self.require_observability_helper(target)?;
        let action = carrier_string(carrier_argument(arguments, 0, None, target)?, target)?;
        let entity = carrier_string(carrier_argument(arguments, 1, None, target)?, target)?;
        let id = carrier_display(&carrier_argument(arguments, 2, None, target)?);
        let fields =
            carrier_optional_object(arguments.get(3).map(|argument| argument.1.clone()), target)?;
        self.host.observe_carrier_audit(
            self.current_transaction(),
            action,
            format!("{entity}:{id}"),
            fields,
        )?;
        Ok(Value::Null)
    }
}

fn remove_immutable_upsert_fields(
    update: &mut serde_json::Map<String, Value>,
    primary_key: &str,
    immutable_fields: &BTreeSet<String>,
) {
    update.remove(primary_key);
    for field in immutable_fields {
        update.remove(field);
    }
}

fn carrier_model_list_value(body: Value, page: u64, per_page: u64, total: u64) -> Result<Value> {
    let items = body.as_array().cloned().ok_or_else(|| {
        AppRuntimeError::InvalidPackage(
            "BicDB application model list did not return an item array".to_string(),
        )
    })?;
    Ok(json!({
        "items": items,
        "page_info": {
            "page": page,
            "per_page": per_page,
            "total": total,
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_updates_preserve_primary_and_immutable_scope_fields() {
        let mut update = json!({
            "id": "lead-1",
            "organization_id": "organization-1",
            "status": "qualified"
        })
        .as_object()
        .cloned()
        .unwrap();
        remove_immutable_upsert_fields(
            &mut update,
            "id",
            &BTreeSet::from(["organization_id".to_string()]),
        );

        assert_eq!(
            update,
            json!({"status": "qualified"}).as_object().cloned().unwrap()
        );
    }

    #[test]
    fn carrier_model_lists_retain_their_signed_page_envelope() {
        let value =
            carrier_model_list_value(Value::Array(vec![json!({"id": "line-1"})]), 2, 50, 51)
                .unwrap();
        assert_eq!(
            value,
            json!({
                "items": [{"id": "line-1"}],
                "page_info": {"page": 2, "per_page": 50, "total": 51}
            })
        );
        assert!(carrier_model_list_value(json!({"items": []}), 1, 50, 0).is_err());
    }
}
