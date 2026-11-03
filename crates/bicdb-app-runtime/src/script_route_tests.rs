//! Signed package scripts serving HTTP routes through the same per-caller
//! capability host, declared SQL, row-level security, and limits as WASM.
use crate::host::tests::actor;
use crate::{
    ActorContext, AppRuntimeError, ApplicationHostConfig, ApplicationHttpRequest,
    ApplicationPackage, ApplicationRuntime, EgressProvider, HttpHostPolicy, InMemorySecretProvider,
    InvocationServices, LocalBlobProvider, ObservabilityEvent, PackageVerifier, TrustedSigningKeys,
};
use base64::Engine;
use bicdb_core::{BicDb, CollectionPolicy, MutationPolicy, Record};
use bicdb_extension::abi_v2::{
    ApplicationManifestV2, DatabaseAction, EgressDeclaration, EgressResponse, FieldType,
    HttpRequestBodyV2, HttpResponseBodyV2, PackageMetadata, RawSqlDeclaration, RelationPermission,
    APPLICATION_COMPATIBILITY_PROFILE,
};
use bicdb_extension::{
    ExtensionCapability, ExtensionIdentity, ExtensionLimits, ExtensionManifest,
    ExtensionPermissions, HttpMethod,
};
use bicdb_sql::SqlSession;
use ed25519_dalek::{Signer, SigningKey};
use parking_lot::RwLock;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

const API_V1: &str = r#"
interface Note { id: string; body: string }
function listNotes(request: any) {
  const notes: Note[] = db.execute("list_notes", []);
  return { status: 200, body: { version: 1, tenant: request.actor.tenant_id, notes } };
}
function addNote(request: any) {
  const input = request.body.value;
  const inserted = db.transaction(tx => tx.execute("add_note", [input.id, input.body]));
  return { status: 201, headers: { "x-note-id": input.id }, body: { inserted } };
}
function undeclared() {
  return { status: 200, body: db.execute("DELETE FROM notes", []) };
}
function readMutation() {
  return { status: 200, body: db.one("add_note", ["x", "y"]) };
}
function spin() { for (;;) {} }
function echo(request: any) {
  return { status: 200, body: { headers: request.headers, cookies: request.cookies, path: request.path_parameters } };
}
function notify(request: any) {
  const response = http.post("https://api.example/hooks", { json: { note: request.path_parameters.id } });
  return { status: 202, body: response };
}
"#;

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn statement(id: &str, sql: &str, actions: &[DatabaseAction], parameters: usize) -> Value {
    json!({
        "id": id,
        "sql": sql,
        "sha256": digest(sql.as_bytes()),
        "relations": ["notes"],
        "routines": if sql.contains("current_setting") { vec!["current_setting"] } else { vec![] },
        "actions": actions,
        "parameters": vec!["string"; parameters],
    })
}

fn route(name: &str, method: &str, template: &str, export: &str) -> Value {
    json!({
        "name": name,
        "method": method,
        "template": template,
        "script": { "module": "api", "export": export },
        "roles": ["editor"],
        "max_request_bytes": 4096,
        "max_response_bytes": 65536
    })
}

fn package(version: &str, source: &str, key: &SigningKey) -> ApplicationPackage {
    let dependency_lock = br#"{"format":1,"packages":[]}"#.to_vec();
    let sbom = br#"{"bomFormat":"CycloneDX","specVersion":"1.5"}"#.to_vec();
    let provenance = br#"{"builder":"bicdb-script-route-test"}"#.to_vec();
    let raw_sql: Vec<RawSqlDeclaration> = serde_json::from_value(json!([
        statement("list_notes", "SELECT id, body FROM notes ORDER BY id", &[DatabaseAction::Select], 0),
        statement(
            "add_note",
            "INSERT INTO notes (id, body, tenant_id) VALUES ($1, $2, current_setting('bicdb.current_tenant'))",
            &[DatabaseAction::Insert, DatabaseAction::Execute],
            2,
        ),
    ]))
    .unwrap();
    assert!(raw_sql[1].parameters == vec![FieldType::String; 2]);
    let mut notify = route("notify", "POST", "/notes/{id}/notify", "notify");
    notify["script"]["egress"] = json!("hooks");
    let routes = serde_json::from_value(json!([
        route("list_notes", "GET", "/notes", "listNotes"),
        route("add_note", "POST", "/notes", "addNote"),
        route("undeclared", "GET", "/undeclared", "undeclared"),
        route("read_mutation", "GET", "/read-mutation", "readMutation"),
        route("spin", "GET", "/spin", "spin"),
        route("echo", "GET", "/echo/{id}", "echo"),
        notify,
        route(
            "notify_denied",
            "POST",
            "/notes/{id}/notify-denied",
            "notify"
        ),
    ]))
    .unwrap();
    let application = ApplicationManifestV2 {
        abi_version: 2,
        application_profile: APPLICATION_COMPATIBILITY_PROFILE.to_string(),
        package: PackageMetadata {
            application: "notes".to_string(),
            version: version.to_string(),
            package_sha256: "0".repeat(64),
            dependency_lock_sha256: digest(&dependency_lock),
            sbom_sha256: digest(&sbom),
            provenance_sha256: digest(&provenance),
            signature_key_id: "release".to_string(),
            signature_algorithm: "ed25519".to_string(),
            signature: "unsigned-placeholder".to_string(),
        },
        relation_permissions: vec![RelationPermission {
            relation: "notes".to_string(),
            actions: BTreeSet::from([DatabaseAction::Select, DatabaseAction::Insert]),
            readable_columns: BTreeSet::new(),
            writable_columns: ["id", "body", "tenant_id"].map(String::from).into(),
        }],
        raw_sql,
        service_imports: vec![],
        service_exports: vec![],
        secrets: vec![],
        egress: vec![EgressDeclaration {
            name: "hooks".into(),
            provider: None,
            required_provider_headers: BTreeSet::new(),
            schemes: BTreeSet::from(["https".into()]),
            hosts: BTreeSet::from(["api.example".into()]),
            ports: BTreeSet::from([443]),
            allow_redirects: false,
            allow_private_networks: false,
            mtls_secret: None,
            max_request_bytes: 4096,
            max_response_bytes: 4096,
            timeout_ms: 8000,
            max_concurrency: 2,
            requests_per_minute: 60,
        }],
        blobs: vec![],
        routes,
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
    let mut package = ApplicationPackage {
        manifest: ExtensionManifest {
            identity: ExtensionIdentity {
                name: "notes".to_string(),
                version: version.to_string(),
                abi_version: 2,
                description: "script route test".to_string(),
            },
            dependencies: vec![],
            capabilities: BTreeSet::from([
                ExtensionCapability::Database,
                ExtensionCapability::Transactions,
                ExtensionCapability::HttpRoutes,
                ExtensionCapability::NetworkEgress,
            ]),
            permissions: ExtensionPermissions {
                read_relations: BTreeSet::from(["notes".to_string()]),
                write_relations: BTreeSet::from(["notes".to_string()]),
                ..ExtensionPermissions::default()
            },
            limits: ExtensionLimits {
                timeout_ms: 1_000,
                ..ExtensionLimits::default()
            },
            functions: vec![],
            indexes: vec![],
            storage: vec![],
            routes: vec![],
            subscriptions: vec![],
            observability: vec![],
            application: Some(Box::new(application)),
        },
        modules: BTreeMap::new(),
        frontend_assets: BTreeMap::new(),
        components: Vec::new(),
        scripts: BTreeMap::from([("api".to_string(), source.to_string())]),
        dependency_lock,
        sbom,
        provenance,
    };
    sign(&mut package, key);
    package
}

fn sign(package: &mut ApplicationPackage, key: &SigningKey) {
    let metadata = &mut package.manifest.application.as_deref_mut().unwrap().package;
    metadata.package_sha256 = "0".repeat(64);
    metadata.signature = "unsigned-placeholder".to_string();
    let hash = digest(&crate::package::canonical_signing_payload(package).unwrap());
    let signature = key.sign(hash.as_bytes());
    let metadata = &mut package.manifest.application.as_deref_mut().unwrap().package;
    metadata.package_sha256 = hash;
    metadata.signature = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
}

struct Hooks;
impl EgressProvider for Hooks {
    fn execute(
        &self,
        _plugin: &str,
        policy: &EgressDeclaration,
        _secret: Option<&crate::SecretRecord>,
        method: &str,
        url: &str,
        _headers: &[(String, String)],
        body: &[u8],
        _timeout: u64,
    ) -> crate::Result<EgressResponse> {
        assert_eq!((policy.name.as_str(), method), ("hooks", "POST"));
        assert_eq!(url, "https://api.example/hooks");
        let request: Value = serde_json::from_slice(body).unwrap();
        Ok(EgressResponse {
            status: 200,
            headers: vec![],
            body: json!({"delivered": request["note"]})
                .to_string()
                .into_bytes(),
        })
    }
}

struct Harness {
    root: tempfile::TempDir,
    db: Arc<RwLock<BicDb>>,
    key: SigningKey,
}

impl Harness {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path().join("database")).unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT NOT NULL, tenant_id TEXT NOT NULL)")
            .unwrap();
        for (id, tenant) in [("a1", "tenant-1"), ("a2", "tenant-1"), ("b1", "tenant-2")] {
            db.insert(
                "notes",
                Record::new(id).with_metadata(
                    json!({"id": id, "body": format!("note {id}"), "tenant_id": tenant}),
                ),
            )
            .unwrap();
        }
        db.set_collection_policy(
            "notes",
            CollectionPolicy::tenant_field("tenant_id")
                .with_read_roles(["editor"])
                .with_write_roles(["editor"]),
        )
        .unwrap();
        db.set_mutation_policy(
            "notes",
            MutationPolicy::grants_required().with_tenant_field("tenant_id"),
        )
        .unwrap();
        Self {
            root,
            db: Arc::new(RwLock::new(db)),
            key: SigningKey::from_bytes(&[7; 32]),
        }
    }

    fn runtime(&self) -> Arc<ApplicationRuntime> {
        let mut trusted = TrustedSigningKeys::default();
        trusted
            .insert_ed25519("release", self.key.verifying_key().as_bytes())
            .unwrap();
        let root: &Path = self.root.path();
        let services = InvocationServices::new(
            Arc::new(InMemorySecretProvider::default()),
            Arc::new(Hooks),
            Arc::new(LocalBlobProvider::open(root.join("blobs"), vec![8; 32]).unwrap()),
            Arc::new(Mutex::<Vec<ObservabilityEvent>>::default()),
        );
        let mut config = ApplicationHostConfig::new(root.join("applications"), "script-node");
        config.max_package_bytes = 8 * 1024 * 1024;
        ApplicationRuntime::new_shared(
            self.db.clone(),
            config,
            PackageVerifier::new(trusted, 8 * 1024 * 1024).unwrap(),
            services,
        )
        .unwrap()
    }
}

fn caller(tenant: &str) -> ActorContext {
    let mut actor = actor(crate::host::now_ms() + 30_000);
    actor.roles.insert("editor".to_string());
    actor.tenant_id = Some(tenant.to_string());
    actor
}

fn call(
    runtime: &ApplicationRuntime,
    tenant: &str,
    method: HttpMethod,
    path: &str,
    body: HttpRequestBodyV2,
) -> crate::Result<crate::ApplicationHttpResponse> {
    runtime.dispatch_carrier_test_http(
        &HttpHostPolicy::default(),
        ApplicationHttpRequest {
            method,
            path: path.to_string(),
            query: vec![],
            headers: vec![
                (
                    "authorization".to_string(),
                    "Bearer caller-token".to_string(),
                ),
                ("x-client".to_string(), "test".to_string()),
            ]
            .into_iter()
            // Cookie-authenticated mutations need CSRF evidence; GETs carry one.
            .chain(
                (method == HttpMethod::Get)
                    .then(|| ("cookie".to_string(), "session=secret".to_string())),
            )
            .collect(),
            body,
            peer_address: None,
        },
        Some(caller(tenant)),
    )
}

fn json_body(response: &crate::ApplicationHttpResponse) -> &Value {
    match &response.body {
        HttpResponseBodyV2::Json(value) => value,
        other => panic!("expected JSON body, found {other:?}"),
    }
}

fn note_ids(runtime: &ApplicationRuntime, tenant: &str) -> (u64, Vec<String>) {
    let response = call(
        runtime,
        tenant,
        HttpMethod::Get,
        "/notes",
        HttpRequestBodyV2::Empty,
    )
    .unwrap();
    assert_eq!(response.status, 200);
    let body = json_body(&response);
    assert_eq!(body["tenant"], tenant);
    let ids = body["notes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|note| note["id"].as_str().unwrap().to_string())
        .collect();
    (body["version"].as_u64().unwrap(), ids)
}

#[test]
fn script_routes_run_declared_sql_under_each_callers_row_level_security() {
    let harness = Harness::new();
    let runtime = harness.runtime();
    runtime
        .install(package("1.0.0", API_V1, &harness.key))
        .unwrap();
    assert!(runtime.activate("notes").unwrap().ready);

    assert_eq!(
        note_ids(&runtime, "tenant-1"),
        (1, vec!["a1".into(), "a2".into()])
    );
    assert_eq!(note_ids(&runtime, "tenant-2"), (1, vec!["b1".into()]));

    let created = call(
        &runtime,
        "tenant-2",
        HttpMethod::Post,
        "/notes",
        HttpRequestBodyV2::Json(json!({"id": "b2", "body": "second"})),
    )
    .unwrap();
    assert_eq!(created.status, 201);
    assert!(created
        .headers
        .contains(&("x-note-id".to_string(), "b2".to_string())));
    assert_eq!(json_body(&created)["inserted"], 1);
    assert_eq!(note_ids(&runtime, "tenant-2").1, vec!["b1", "b2"]);
    assert_eq!(note_ids(&runtime, "tenant-1").1, vec!["a1", "a2"]);

    let error = call(
        &runtime,
        "tenant-1",
        HttpMethod::Get,
        "/undeclared",
        HttpRequestBodyV2::Empty,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("not declared in the signed manifest"),
        "{error}"
    );
    assert!(!error.to_string().contains('\n'));
    let error = call(
        &runtime,
        "tenant-1",
        HttpMethod::Get,
        "/read-mutation",
        HttpRequestBodyV2::Empty,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("read method cannot execute a mutation"),
        "{error}"
    );
    assert_eq!(note_ids(&runtime, "tenant-1").1, vec!["a1", "a2"]);

    let echoed = call(
        &runtime,
        "tenant-1",
        HttpMethod::Get,
        "/echo/n%201",
        HttpRequestBodyV2::Empty,
    )
    .unwrap();
    let echoed = json_body(&echoed);
    assert_eq!(echoed["path"], json!({"id": "n 1"}));
    assert_eq!(echoed["cookies"], json!([]));
    let headers = echoed["headers"].to_string().to_ascii_lowercase();
    assert!(headers.contains("x-client"));
    assert!(!headers.contains("authorization") && !headers.contains("caller-token"));
    assert!(!headers.contains("session=secret"));

    let notified = call(
        &runtime,
        "tenant-1",
        HttpMethod::Post,
        "/notes/a1/notify",
        HttpRequestBodyV2::Empty,
    )
    .unwrap();
    assert_eq!(notified.status, 202);
    assert_eq!(json_body(&notified)["status"], 200);
    assert_eq!(
        json_body(&notified)["body"],
        json!({"delivered": "a1"}).to_string()
    );
    let denied = call(
        &runtime,
        "tenant-1",
        HttpMethod::Post,
        "/notes/a1/notify-denied",
        HttpRequestBodyV2::Empty,
    )
    .unwrap_err();
    assert!(
        denied.to_string().contains("HTTP is not enabled"),
        "{denied}"
    );

    let mut outsider = caller("tenant-1");
    outsider.roles.clear();
    let forbidden = runtime
        .dispatch_carrier_test_http(
            &HttpHostPolicy::default(),
            ApplicationHttpRequest {
                method: HttpMethod::Get,
                path: "/notes".into(),
                query: vec![],
                headers: vec![],
                body: HttpRequestBodyV2::Empty,
                peer_address: None,
            },
            Some(outsider),
        )
        .unwrap_err();
    assert!(matches!(forbidden, AppRuntimeError::CapabilityDenied(_)));

    let openapi = runtime.openapi("notes").unwrap();
    assert_eq!(
        openapi["paths"]["/notes"]["get"]["x-bicdb-script"],
        json!({"module": "api", "export": "listNotes"})
    );
}

#[test]
fn script_route_infinite_loop_times_out_and_rolls_back() {
    let harness = Harness::new();
    let runtime = harness.runtime();
    let source = API_V1.replace(
        "function spin() { for (;;) {} }",
        "function spin() { db.transaction(tx => { tx.execute(\"add_note\", [\"loop\", \"x\"]); for (;;) {} }); }",
    );
    runtime
        .install(package("1.0.0", &source, &harness.key))
        .unwrap();
    runtime.activate("notes").unwrap();
    let started = std::time::Instant::now();
    let error = call(
        &runtime,
        "tenant-1",
        HttpMethod::Get,
        "/spin",
        HttpRequestBodyV2::Empty,
    )
    .unwrap_err();
    assert!(matches!(error, AppRuntimeError::Timeout(_)), "{error}");
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    assert_eq!(note_ids(&runtime, "tenant-1").1, vec!["a1", "a2"]);
}

#[test]
fn script_packages_are_signed_compiled_upgraded_rolled_back_and_restored() {
    let harness = Harness::new();
    let runtime = harness.runtime();
    let first = package("1.0.0", API_V1, &harness.key);

    let mut tampered = first.clone();
    tampered
        .scripts
        .insert("api".into(), API_V1.replace("version: 1", "version: 9"));
    let error = runtime.install(tampered).unwrap_err();
    assert!(
        error.to_string().contains("package hash mismatch"),
        "{error}"
    );

    let broken = package("1.0.0", "function listNotes( {", &harness.key);
    let error = runtime.install(broken).unwrap_err();
    assert!(
        error.to_string().contains("script `api` failed to compile"),
        "{error}"
    );

    let mut absent = package("1.0.0", API_V1, &harness.key);
    absent.manifest.application.as_deref_mut().unwrap().routes[0]
        .script
        .as_mut()
        .unwrap()
        .module = "missing".into();
    sign(&mut absent, &harness.key);
    let error = runtime.install(absent).unwrap_err();
    assert!(
        error.to_string().contains("absent script `missing`"),
        "{error}"
    );

    let mut doubled = package("1.0.0", API_V1, &harness.key);
    doubled.manifest.application.as_deref_mut().unwrap().routes[0].export = "listNotes".into();
    sign(&mut doubled, &harness.key);
    assert!(runtime.install(doubled).is_err());

    runtime.install(first).unwrap();
    runtime.activate("notes").unwrap();
    assert_eq!(note_ids(&runtime, "tenant-1").0, 1);

    let broken_upgrade = package("1.1.0", "function listNotes( {", &harness.key);
    assert!(runtime.upgrade(broken_upgrade).is_err());
    assert_eq!(note_ids(&runtime, "tenant-1").0, 1);

    let second = package(
        "1.1.0",
        &API_V1.replace("version: 1", "version: 2"),
        &harness.key,
    );
    assert!(runtime.upgrade(second).unwrap().ready);
    assert_eq!(runtime.inspect("notes").unwrap().version, "1.1.0");
    assert_eq!(note_ids(&runtime, "tenant-1").0, 2);

    assert!(runtime.rollback("notes").unwrap().ready);
    assert_eq!(note_ids(&runtime, "tenant-1").0, 1);
    drop(runtime);

    let restored = harness.runtime();
    assert!(restored.readiness().ready);
    assert_eq!(restored.inspect("notes").unwrap().version, "1.0.0");
    assert_eq!(note_ids(&restored, "tenant-2"), (1, vec!["b1".into()]));
}
