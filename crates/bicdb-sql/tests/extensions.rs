use std::collections::BTreeSet;

use bicdb_core::BicDb;
use bicdb_extension::{
    host::{
        ExtensionHttpRequest, ExtensionPackageStore, ExtensionRuntime, RouteAuthorization,
        WasmHostConfig,
    },
    DatabaseOperation, EventSource, EventSubscriptionRegistration, ExtensionCapability,
    ExtensionDependency, ExtensionIdentity, ExtensionLimits, ExtensionManifest,
    ExtensionPermissions, ExtensionState, HttpMethod, HttpRouteRegistration, RouteAuth,
    EXTENSION_ABI_VERSION,
};
use bicdb_sql::{
    list_event_bindings, list_installed_extensions, list_rest_resources, list_website_releases,
    list_websites, run_extension_event_once, sync_extension_runtime, ExtensionEventOutcome,
    SqlSession,
};

fn manifest() -> ExtensionManifest {
    ExtensionManifest {
        identity: ExtensionIdentity {
            name: "instant_rest".to_string(),
            version: "1.2.3".to_string(),
            abi_version: EXTENSION_ABI_VERSION,
            description: "test extension".to_string(),
        },
        dependencies: vec![],
        capabilities: BTreeSet::from([
            ExtensionCapability::HttpRoutes,
            ExtensionCapability::DatabaseEvents,
            ExtensionCapability::QueueEvents,
        ]),
        permissions: ExtensionPermissions {
            read_relations: BTreeSet::from(["public.patients".to_string()]),
            publish_queues: BTreeSet::from(["patients.changed".to_string()]),
            consume_queues: BTreeSet::from(["patients.work".to_string()]),
            ..ExtensionPermissions::default()
        },
        limits: ExtensionLimits::default(),
        functions: vec![],
        indexes: vec![],
        storage: vec![],
        routes: vec![HttpRouteRegistration {
            name: "resource".to_string(),
            method: HttpMethod::Get,
            path: "/resources".to_string(),
            export: "handle_resource".to_string(),
            auth: RouteAuth::RowLevelSecurity,
        }],
        subscriptions: vec![
            EventSubscriptionRegistration {
                name: "patient_changes".to_string(),
                source: EventSource::Database {
                    relation: "public.patients".to_string(),
                    operations: BTreeSet::from([
                        DatabaseOperation::Insert,
                        DatabaseOperation::Update,
                        DatabaseOperation::Delete,
                    ]),
                },
                export: "on_patient_changed".to_string(),
                max_attempts: 5,
                visibility_timeout_ms: 30_000,
            },
            EventSubscriptionRegistration {
                name: "patient_work".to_string(),
                source: EventSource::Queue {
                    queue: "patients.work".to_string(),
                    group: "instant-rest".to_string(),
                },
                export: "on_patient_work".to_string(),
                max_attempts: 5,
                visibility_timeout_ms: 30_000,
            },
        ],
        observability: vec![],
        application: None,
    }
}

fn module_bytes() -> Vec<u8> {
    module_bytes_for(&manifest())
}

fn module_bytes_for(extension_manifest: &ExtensionManifest) -> Vec<u8> {
    let manifest = serde_json::to_string(extension_manifest).unwrap();
    let escaped = manifest
        .as_bytes()
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<String>();
    let result = br#"{"status":200,"body":{"processed":true},"ack":true}"#;
    let result_escaped = result
        .iter()
        .map(|byte| format!("\\{byte:02x}"))
        .collect::<String>();
    let result_pointer = 16_384u32;
    wat::parse_str(format!(
        r#"(module
            (memory (export "memory") 2 1024)
            (data (i32.const 1024) "{escaped}")
            (data (i32.const {result_pointer}) "{result_escaped}")
            (func (export "bicdb_extension_abi_version") (result i32) i32.const 1)
            (func (export "bicdb_extension_manifest_ptr") (result i32) i32.const 1024)
            (func (export "bicdb_extension_manifest_len") (result i32)
                i32.const {manifest_len})
            (func (export "bicdb_extension_alloc") (param i32) (result i32)
                i32.const 32768)
            (func (export "bicdb_extension_dealloc") (param i32 i32))
            (func (export "bicdb_extension_invoke") (param i32 i32) (result i64)
                i64.const {packed}))"#,
        manifest_len = manifest.len(),
        packed = ((result_pointer as u64) << 32) | result.len() as u64,
    ))
    .unwrap()
}

fn install_sql(module_sha256: &str) -> String {
    install_sql_for(module_sha256, &manifest())
}

fn install_sql_for(module_sha256: &str, extension_manifest: &ExtensionManifest) -> String {
    let manifest = serde_json::to_string(extension_manifest)
        .unwrap()
        .replace('\'', "''");
    format!(
        "CREATE EXTENSION {} FROM MODULE '{}' MANIFEST '{}'",
        extension_manifest.identity.name, module_sha256, manifest
    )
}

fn upgrade_sql(module_sha256: &str, extension_manifest: &ExtensionManifest) -> String {
    let manifest = serde_json::to_string(extension_manifest)
        .unwrap()
        .replace('\'', "''");
    format!(
        "ALTER EXTENSION instant_rest UPDATE FROM MODULE '{}' MANIFEST '{}'",
        module_sha256, manifest
    )
}

#[test]
fn executable_extensions_have_transactional_durable_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let packages = ExtensionPackageStore::open(dir.path().join("extensions/packages")).unwrap();
        let hash = packages.install(&module_bytes(), 64 * 1024 * 1024).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
            .unwrap();
        session.execute(&install_sql(&hash)).unwrap();
        assert!(session
            .execute(
                "CREATE RESOURCE patients USING EXTENSION instant_rest \
                 FROM TABLE public.patients \
                 WITH (path = '/patients', methods = 'GET', auth = 'rls')"
            )
            .is_err());
        session
            .execute("ALTER EXTENSION instant_rest ACTIVATE SINGLE NODE")
            .unwrap();
        let mut upgraded_manifest = manifest();
        upgraded_manifest.identity.version = "1.2.4".to_string();
        let upgraded_hash = packages
            .install(&module_bytes_for(&upgraded_manifest), 64 * 1024 * 1024)
            .unwrap();
        session
            .execute(&upgrade_sql(&upgraded_hash, &upgraded_manifest))
            .unwrap();
        session
            .execute(
                "CREATE RESOURCE patients USING EXTENSION instant_rest \
                 FROM TABLE public.patients \
                 WITH (path = '/patients', methods = 'GET,POST', auth = 'rls')",
            )
            .unwrap();
        session
            .execute(
                "CREATE EVENT SUBSCRIPTION patient_changes USING EXTENSION instant_rest \
                 ON TABLE public.patients EVENTS (INSERT, UPDATE, DELETE) \
                 QUEUE 'patients.changed' EXECUTE 'on_patient_changed'",
            )
            .unwrap();
        session
            .execute(
                "CREATE EVENT SUBSCRIPTION patient_worker USING EXTENSION instant_rest \
                 ON QUEUE 'patients.work' GROUP 'instant-rest' EXECUTE 'on_patient_work'",
            )
            .unwrap();

        session
            .execute("INSERT INTO patients (id, name) VALUES ('p1', 'Ada')")
            .unwrap();
        session.execute("BEGIN").unwrap();
        session
            .execute(
                "CREATE RESOURCE transient USING EXTENSION instant_rest \
                 FROM TABLE public.patients \
                 WITH (path = '/transient', methods = 'GET')",
            )
            .unwrap();
        session
            .execute("INSERT INTO patients (id, name) VALUES ('p2', 'Rolled Back')")
            .unwrap();
        session.execute("ROLLBACK").unwrap();
        drop(session);

        let runtime = ExtensionRuntime::new(
            ExtensionPackageStore::open(dir.path().join("extensions/packages")).unwrap(),
            WasmHostConfig::default(),
        )
        .unwrap();
        sync_extension_runtime(&db, &runtime).unwrap();
        let binding = list_event_bindings(&db)
            .unwrap()
            .into_iter()
            .find(|binding| binding.name == "patient_changes")
            .unwrap();
        assert!(matches!(
            run_extension_event_once(&db, &runtime, &binding, "worker-1").unwrap(),
            ExtensionEventOutcome::Acked { .. }
        ));
        assert!(matches!(
            run_extension_event_once(&db, &runtime, &binding, "worker-1").unwrap(),
            ExtensionEventOutcome::Idle
        ));
    }

    {
        let mut db = BicDb::open(dir.path()).unwrap();
        assert_eq!(
            list_installed_extensions(&db).unwrap()[0].state,
            ExtensionState::Active
        );
        assert_eq!(list_rest_resources(&db).unwrap().len(), 1);
        assert_eq!(list_event_bindings(&db).unwrap().len(), 2);

        let mut session = SqlSession::new(&mut db);
        assert!(session
            .execute("DROP EXTENSION instant_rest RESTRICT")
            .is_err());
        session.execute("BEGIN").unwrap();
        session
            .execute("DROP EXTENSION instant_rest CASCADE")
            .unwrap();
        session.execute("ROLLBACK").unwrap();
    }

    {
        let db = BicDb::open(dir.path()).unwrap();
        assert_eq!(list_installed_extensions(&db).unwrap().len(), 1);
        assert_eq!(list_rest_resources(&db).unwrap().len(), 1);
        assert_eq!(list_event_bindings(&db).unwrap().len(), 2);
    }
}

#[test]
fn dependency_graph_activation_is_atomic_and_drop_is_protected() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let packages = ExtensionPackageStore::open(dir.path().join("extensions/packages")).unwrap();

    let mut renderer = manifest();
    renderer.identity.name = "renderer_core".to_string();
    renderer.identity.version = "1.5.0".to_string();
    renderer.dependencies.clear();

    let mut website = manifest();
    website.identity.name = "website_app".to_string();
    website.identity.version = "2.0.0".to_string();
    website.dependencies = vec![ExtensionDependency {
        name: "renderer_core".to_string(),
        version: "^1.0".to_string(),
        abi_version: EXTENSION_ABI_VERSION,
        optional: false,
        capabilities: BTreeSet::from([ExtensionCapability::HttpRoutes]),
        module_sha256: None,
    }];

    let renderer_hash = packages
        .install(&module_bytes_for(&renderer), 64 * 1024 * 1024)
        .unwrap();
    let website_hash = packages
        .install(&module_bytes_for(&website), 64 * 1024 * 1024)
        .unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(&install_sql_for(&renderer_hash, &renderer))
        .unwrap();
    session
        .execute(&install_sql_for(&website_hash, &website))
        .unwrap();
    session
        .execute("ALTER EXTENSION website_app ACTIVATE SINGLE NODE")
        .unwrap();
    drop(session);
    assert!(list_installed_extensions(&db)
        .unwrap()
        .iter()
        .all(|installation| installation.state == ExtensionState::Active));

    let mut session = SqlSession::new(&mut db);
    assert!(session
        .execute("ALTER EXTENSION renderer_core DISABLE RESTRICT")
        .unwrap_err()
        .to_string()
        .contains("website_app"));
    assert!(session
        .execute("DROP EXTENSION renderer_core RESTRICT")
        .unwrap_err()
        .to_string()
        .contains("website_app"));

    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER EXTENSION renderer_core DISABLE CASCADE")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    drop(session);
    assert!(list_installed_extensions(&db)
        .unwrap()
        .iter()
        .all(|installation| installation.state == ExtensionState::Active));
}

#[test]
fn websites_publish_version_and_rollback_through_host_owned_routes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let packages = ExtensionPackageStore::open(dir.path().join("extensions/packages")).unwrap();
    let hash = packages.install(&module_bytes(), 64 * 1024 * 1024).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session.execute(&install_sql(&hash)).unwrap();
        session
            .execute("ALTER EXTENSION instant_rest ACTIVATE SINGLE NODE")
            .unwrap();
        session
            .execute(
                "CREATE WEBSITE docs USING EXTENSION instant_rest \
                 WITH (host = 'docs.example.test', path = '/docs', \
                 export = 'handle_resource', auth = 'public')",
            )
            .unwrap();
        session
            .execute(
                r#"PUBLISH WEBSITE docs VERSION '1.0.0'
                   CONTENT '{"pages":{"/":{"title":"First","html":"<h1>First</h1>"}}}'
                   ACTIVATE"#,
            )
            .unwrap();
        session
            .execute(
                r#"PUBLISH WEBSITE docs VERSION '2.0.0'
                   CONTENT '{"pages":{"/":{"title":"Second","html":"<h1>Second</h1>"}}}'
                   ACTIVATE"#,
            )
            .unwrap();
        assert!(session
            .execute(
                r#"PUBLISH WEBSITE docs VERSION '2.0.0'
                   CONTENT '{"pages":{"/":{"title":"Duplicate"}}}'"#,
            )
            .is_err());
    }

    let website = list_websites(&db).unwrap().pop().unwrap();
    assert_eq!(website.active_version.as_deref(), Some("2.0.0"));
    assert_eq!(website.previous_version.as_deref(), Some("1.0.0"));
    assert_eq!(list_website_releases(&db, "docs").unwrap().len(), 2);

    let runtime = ExtensionRuntime::new(
        ExtensionPackageStore::open(dir.path().join("extensions/packages")).unwrap(),
        WasmHostConfig::default(),
    )
    .unwrap();
    sync_extension_runtime(&db, &runtime).unwrap();
    let response = runtime
        .dispatch_http(ExtensionHttpRequest {
            id: "website-request".to_string(),
            method: HttpMethod::Get,
            path: "/docs/guide".to_string(),
            headers: [("host".to_string(), "docs.example.test:443".to_string())]
                .into_iter()
                .collect(),
            query: Default::default(),
            body: serde_json::Value::Null,
            context: Default::default(),
            authorization: RouteAuthorization::Anonymous,
        })
        .unwrap()
        .unwrap();
    assert_eq!(response.body["processed"], true);

    {
        let mut session = SqlSession::new(&mut db);
        session.execute("ALTER WEBSITE docs ROLLBACK").unwrap();
    }
    let website = list_websites(&db).unwrap().pop().unwrap();
    assert_eq!(website.active_version.as_deref(), Some("1.0.0"));
    assert_eq!(website.previous_version.as_deref(), Some("2.0.0"));

    {
        let mut session = SqlSession::new(&mut db);
        assert!(session.execute("DROP WEBSITE docs RESTRICT").is_err());
        session.execute("BEGIN").unwrap();
        session.execute("DROP WEBSITE docs CASCADE").unwrap();
        session.execute("ROLLBACK").unwrap();
    }
    assert_eq!(list_websites(&db).unwrap().len(), 1);
    assert_eq!(list_website_releases(&db, "docs").unwrap().len(), 2);
}
