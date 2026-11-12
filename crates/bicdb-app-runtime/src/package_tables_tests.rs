use super::*;
use crate::script_workflow::route_tests::{caller, json_body, sign, Harness};
use crate::{ApplicationHttpRequest, ApplicationPackage, ApplicationRuntime, HttpHostPolicy};
use bicdb_extension::abi_v2::HttpRequestBodyV2;
use bicdb_extension::HttpMethod;

const DEAL_A: &str = "6f1c1b7e-4c1a-4bb1-9a53-0d9d0f0e0001";

fn script(version: u32) -> String {
    format!(
        r#"
function listDeals() {{
  return {{ status: 200, body: {{ version: {version}, deals: db.execute("list_deals", []) }} }};
}}
function addDeal(request: any) {{
  const input = request.body.value;
  const inserted = db.transaction(tx => tx.execute("add_deal", [input.id, input.title, input.amount, "lead"]));
  return {{ status: 201, body: {{ inserted }} }};
}}
function moveDeal(request: any) {{
  return {{ status: 200, body: {{ updated: db.execute("move_deal", [request.path_parameters.id, request.body.value.stage]) }} }};
}}
"#
    )
}

/// What a generator writes: compact tables, SQL without derived authority,
/// empty signature fields.
fn unsigned(version: &str, extra_column: bool) -> Value {
    let route = |name: &str, method: &str, template: &str, export: &str| {
        json!({"name": name, "method": method, "template": template,
               "script": {"module": "api", "export": export},
               "max_request_bytes": 4096, "max_response_bytes": 65536})
    };
    let mut columns = json!([
        {"name": "id", "type": "uuid"},
        {"name": "tenant_id", "type": "text"},
        {"name": "owner_id", "type": "text"},
        {"name": "title", "type": "text"},
        {"name": "amount", "type": "numeric"},
        {"name": "stage", "type": "text"},
        {"name": "created_at", "type": "timestamptz"}
    ]);
    if extra_column {
        columns
            .as_array_mut()
            .unwrap()
            .push(json!({"name": "probability", "type": "integer", "nullable": true}));
    }
    json!({
        "manifest": {
            "identity": {"name": "crm", "version": version, "abi_version": 2},
            "capabilities": ["http_routes"],
            "application": {
                "application_profile": "bicdb-application-v2",
                "package": {
                    "application": "crm", "version": version, "package_sha256": "",
                    "dependency_lock_sha256": "", "sbom_sha256": "", "provenance_sha256": "",
                    "signature_key_id": "release", "signature_algorithm": "ed25519", "signature": ""
                },
                "raw_sql": [
                    {"id": "list_deals", "sql": "SELECT id, owner_id, title, amount, stage FROM deals ORDER BY title"},
                    {"id": "add_deal",
                     "sql": "INSERT INTO deals (id, tenant_id, owner_id, title, amount, stage, created_at) VALUES ($1, current_setting('bicdb.current_tenant'), current_setting('bicdb.current_user'), $2, $3, $4, now())",
                     "parameters": ["uuid", "string", "decimal", "string"]},
                    {"id": "move_deal", "sql": "UPDATE deals SET stage = $2 WHERE id = $1",
                     "parameters": ["uuid", "string"]}
                ],
                "routes": [
                    route("list_deals", "GET", "/deals", "listDeals"),
                    route("add_deal", "POST", "/deals", "addDeal"),
                    route("move_deal", "POST", "/deals/{id}/stage", "moveDeal")
                ]
            }
        },
        "modules": {},
        "scripts": {"api": script(if extra_column { 2 } else { 1 })},
        "tables": [{
            "name": "deals",
            "columns": columns,
            "owner_column": "owner_id",
            "indexes": [{"name": "deals_stage_idx", "columns": ["stage"]}]
        }],
        "dependency_lock": br#"{"format":1,"packages":[]}"#.to_vec(),
        "sbom": br#"{"bomFormat":"CycloneDX"}"#.to_vec(),
        "provenance": br#"{"builder":"generator"}"#.to_vec()
    })
}

fn signed(harness: &Harness, version: &str, extra_column: bool) -> ApplicationPackage {
    let mut document = unsigned(version, extra_column);
    prepare_unsigned_package(&mut document).unwrap();
    let mut package: ApplicationPackage = serde_json::from_value(document).unwrap();
    let metadata = &mut package.manifest.application.as_deref_mut().unwrap().package;
    let digest = |bytes: &[u8]| format!("{:x}", Sha256::digest(bytes));
    metadata.dependency_lock_sha256 = digest(&package.dependency_lock);
    metadata.sbom_sha256 = digest(&package.sbom);
    metadata.provenance_sha256 = digest(&package.provenance);
    sign(&mut package, &harness.key);
    package
}

fn call(
    runtime: &ApplicationRuntime,
    tenant: &str,
    user: &str,
    method: HttpMethod,
    path: &str,
    body: HttpRequestBodyV2,
) -> crate::Result<Value> {
    let mut actor = caller(tenant);
    actor.user_id = Some(user.to_string());
    runtime
        .dispatch_carrier_test_http(
            &HttpHostPolicy::default(),
            ApplicationHttpRequest {
                method,
                path: path.to_string(),
                query: vec![],
                headers: vec![],
                body,
                peer_address: None,
            },
            Some(actor),
        )
        .map(|response| json_body(&response).clone())
}

fn deals(runtime: &ApplicationRuntime, tenant: &str) -> Value {
    call(
        runtime,
        tenant,
        "user-1",
        HttpMethod::Get,
        "/deals",
        HttpRequestBodyV2::Empty,
    )
    .unwrap()
}

#[test]
fn compact_tables_expand_to_tenant_and_owner_scoped_tables_that_evolve_additively() {
    let mut document = unsigned("1.0.0", false);
    prepare_unsigned_package(&mut document).unwrap();
    let application = &document["manifest"]["application"];
    let resource = &application["resources"][0];
    assert_eq!(resource["schema_only"], true);
    assert_eq!(resource["tenant_field"], "tenant_id");
    assert_eq!(resource["schema_version"], 8);
    assert_eq!(application["migrations"][0]["schema_version"], 8);
    assert_eq!(
        application["raw_sql"][1]["routines"],
        json!(["current_setting", "now"])
    );
    assert!(document.get("tables").is_none());

    let harness = Harness::new();
    let runtime = harness.runtime();
    runtime.install(signed(&harness, "1.0.0", false)).unwrap();
    assert!(runtime.activate("crm").unwrap().ready);

    let created = call(
        &runtime,
        "tenant-1",
        "user-1",
        HttpMethod::Post,
        "/deals",
        HttpRequestBodyV2::Json(json!({"id": DEAL_A, "title": "Renewal", "amount": 1250})),
    )
    .unwrap();
    assert_eq!(created["inserted"], 1);
    let listed = deals(&runtime, "tenant-1");
    assert_eq!(listed["deals"][0]["title"], "Renewal");
    assert_eq!(listed["deals"][0]["owner_id"], "user-1");
    assert_eq!(deals(&runtime, "tenant-2")["deals"], json!([]));

    // Owner policy: a colleague in the same tenant can read but not change it.
    let move_to = |user: &str| {
        call(
            &runtime,
            "tenant-1",
            user,
            HttpMethod::Post,
            &format!("/deals/{DEAL_A}/stage"),
            HttpRequestBodyV2::Json(json!({"stage": "won"})),
        )
    };
    assert_eq!(move_to("user-2").unwrap()["updated"], 0);
    assert_eq!(deals(&runtime, "tenant-1")["deals"][0]["stage"], "lead");
    assert_eq!(move_to("user-1").unwrap()["updated"], 1);
    assert_eq!(deals(&runtime, "tenant-1")["deals"][0]["stage"], "won");

    // An additive upgrade is accepted, but older code omitting its nullable
    // column is not a forward-compatible rollback contract.
    assert!(
        runtime
            .upgrade(signed(&harness, "1.1.0", true))
            .unwrap()
            .ready
    );
    assert_eq!(deals(&runtime, "tenant-1")["version"], 2);
    let error = runtime.rollback("crm").unwrap_err();
    assert!(
        error.to_string().contains("removes field `probability`"),
        "{error}"
    );
    assert_eq!(runtime.inspect("crm").unwrap().version, "1.1.0");
    let retained = deals(&runtime, "tenant-1");
    assert_eq!(retained["version"], 2);
    assert_eq!(retained["deals"][0]["title"], "Renewal");
    assert_eq!(retained["deals"][0]["owner_id"], "user-1");
    assert_eq!(retained["deals"][0]["stage"], "won");
    assert_eq!(deals(&runtime, "tenant-2")["deals"], json!([]));

    // Dropping a column is refused.
    assert!(
        runtime
            .upgrade(signed(&harness, "1.2.0", true))
            .unwrap()
            .ready
    );
    let mut dropped = unsigned("1.3.0", false);
    dropped["tables"][0]["columns"]
        .as_array_mut()
        .unwrap()
        .retain(|column| column["name"] != "created_at");
    dropped["manifest"]["application"]["raw_sql"]
        .as_array_mut()
        .unwrap()
        .truncate(1);
    prepare_unsigned_package(&mut dropped).unwrap();
    let mut dropped: ApplicationPackage = serde_json::from_value(dropped).unwrap();
    let metadata = &mut dropped.manifest.application.as_deref_mut().unwrap().package;
    let digest = |bytes: &[u8]| format!("{:x}", Sha256::digest(bytes));
    metadata.dependency_lock_sha256 = digest(&dropped.dependency_lock);
    metadata.sbom_sha256 = digest(&dropped.sbom);
    metadata.provenance_sha256 = digest(&dropped.provenance);
    sign(&mut dropped, &harness.key);
    let error = runtime.upgrade(dropped).unwrap_err();
    let error = error.to_string();
    assert!(
        error.contains("removes field") || error.contains("schema version regresses"),
        "{error}"
    );
}

#[test]
fn compact_declarations_reject_ambiguous_input() {
    let mut missing_types = unsigned("1.0.0", false);
    missing_types["manifest"]["application"]["raw_sql"][1]
        .as_object_mut()
        .unwrap()
        .remove("parameters");
    assert!(prepare_unsigned_package(&mut missing_types)
        .unwrap_err()
        .to_string()
        .contains("needs `parameters` types"));
    let mut bad_type = unsigned("1.0.0", false);
    bad_type["tables"][0]["columns"][3]["type"] = json!("blob-ish");
    assert!(prepare_unsigned_package(&mut bad_type).is_err());
    let mut bad_tenant = unsigned("1.0.0", false);
    bad_tenant["tables"][0]["columns"][1]["nullable"] = json!(true);
    assert!(prepare_unsigned_package(&mut bad_tenant).is_err());
    let mut unknown = unsigned("1.0.0", false);
    unknown["tables"][0]["rls"] = json!(false);
    assert!(prepare_unsigned_package(&mut unknown).is_err());
}
