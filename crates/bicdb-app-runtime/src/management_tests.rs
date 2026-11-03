use super::*;
use crate::script_workflow::route_tests::{note_ids, package, sign, Harness, API_V1};
use ed25519_dalek::SigningKey;

const TOKEN: &str = "management-token-0123456789abcdef0123";

fn request(
    method: HttpMethod,
    path: &str,
    token: Option<&str>,
    body: HttpRequestBodyV2,
) -> ApplicationHttpRequest {
    ApplicationHttpRequest {
        method,
        path: path.to_string(),
        query: vec![],
        headers: token
            .map(|token| vec![("Authorization".to_string(), format!("Bearer {token}"))])
            .unwrap_or_default(),
        body,
        peer_address: None,
    }
}

fn body(package: &crate::ApplicationPackage) -> HttpRequestBodyV2 {
    HttpRequestBodyV2::Json(serde_json::to_value(package).unwrap())
}

fn ok(handler: &ManagementHttpHandler, request: ApplicationHttpRequest) -> Value {
    match handler.handle(&request).unwrap().unwrap().body {
        HttpResponseBodyV2::Json(value) => value,
        other => panic!("unexpected body {other:?}"),
    }
}

#[test]
fn management_api_installs_upgrades_and_rolls_back_signed_packages_only() {
    let harness = Harness::new();
    let runtime = harness.runtime();
    assert!(ManagementHttpHandler::new(runtime.clone(), b"short").is_err());
    let handler =
        ManagementHttpHandler::new(runtime.clone(), format!("{TOKEN}\n").as_bytes()).unwrap();

    // Application traffic is not intercepted.
    assert!(handler
        .handle(&request(
            HttpMethod::Get,
            "/notes",
            None,
            HttpRequestBodyV2::Empty
        ))
        .unwrap()
        .is_none());
    assert!(handler
        .handle(&request(
            HttpMethod::Get,
            "/_bicdb/appsx",
            None,
            HttpRequestBodyV2::Empty
        ))
        .unwrap()
        .is_none());

    let first = package("1.0.0", API_V1, &harness.key);
    for token in [
        None,
        Some("wrong-token-0123456789abcdef0123456789"),
        Some(""),
    ] {
        let error = handler
            .handle(&request(
                HttpMethod::Post,
                "/_bicdb/apps/install",
                token,
                body(&first),
            ))
            .unwrap_err();
        assert!(
            matches!(error, AppRuntimeError::Authentication(_)),
            "{error}"
        );
    }
    assert!(runtime.list().is_empty());

    // A valid token never admits a package signed by an untrusted key or an
    // unsigned/tampered one.
    let mut foreign = first.clone();
    sign(&mut foreign, &SigningKey::from_bytes(&[9; 32]));
    let mut unsigned = first.clone();
    unsigned
        .manifest
        .application
        .as_deref_mut()
        .unwrap()
        .package
        .signature = "unsigned-placeholder".into();
    let mut tampered = first.clone();
    tampered.scripts.insert(
        "api".into(),
        "function listNotes() { return {status: 200}; }".into(),
    );
    for candidate in [&foreign, &unsigned, &tampered] {
        let error = handler
            .handle(&request(
                HttpMethod::Post,
                "/_bicdb/apps/install",
                Some(TOKEN),
                body(candidate),
            ))
            .unwrap_err();
        assert!(
            matches!(error, AppRuntimeError::InvalidRequest(_)),
            "{error}"
        );
    }
    assert!(runtime.list().is_empty());

    let installed = ok(
        &handler,
        request(
            HttpMethod::Post,
            "/_bicdb/apps/install",
            Some(TOKEN),
            body(&first),
        ),
    );
    assert_eq!(installed["application"], "notes");
    assert_eq!(installed["readiness"]["ready"], true);
    assert_eq!(note_ids(&runtime, "tenant-1").0, 1);

    let second = package(
        "1.1.0",
        &API_V1.replace("version: 1", "version: 2"),
        &harness.key,
    );
    let bytes = HttpRequestBodyV2::Binary(serde_json::to_vec(&second).unwrap());
    ok(
        &handler,
        request(HttpMethod::Post, "/_bicdb/apps/upgrade", Some(TOKEN), bytes),
    );
    assert_eq!(note_ids(&runtime, "tenant-1").0, 2);
    let listed = ok(
        &handler,
        request(
            HttpMethod::Get,
            "/_bicdb/apps",
            Some(TOKEN),
            HttpRequestBodyV2::Empty,
        ),
    );
    assert_eq!(listed[0]["version"], "1.1.0");
    assert_eq!(listed[0]["state"], "active");
    assert_eq!(listed[0]["history"], 1);

    ok(
        &handler,
        request(
            HttpMethod::Post,
            "/_bicdb/apps/notes/rollback",
            Some(TOKEN),
            HttpRequestBodyV2::Empty,
        ),
    );
    assert_eq!(note_ids(&runtime, "tenant-1").0, 1);
    drop(handler);
    drop(runtime);

    // Lifecycle changes persist across a restart.
    let runtime = harness.runtime();
    assert_eq!(runtime.inspect("notes").unwrap().version, "1.0.0");
    let handler = ManagementHttpHandler::new(runtime.clone(), TOKEN.as_bytes()).unwrap();
    ok(
        &handler,
        request(
            HttpMethod::Post,
            "/_bicdb/apps/notes/disable",
            Some(TOKEN),
            HttpRequestBodyV2::Empty,
        ),
    );
    assert!(matches!(
        runtime.dispatch_carrier_test_http(
            &crate::HttpHostPolicy::default(),
            request(HttpMethod::Get, "/notes", None, HttpRequestBodyV2::Empty),
            Some(crate::script_workflow::route_tests::caller("tenant-1")),
        ),
        Err(AppRuntimeError::NotFound(_))
    ));
    ok(
        &handler,
        request(
            HttpMethod::Delete,
            "/_bicdb/apps/notes",
            Some(TOKEN),
            HttpRequestBodyV2::Empty,
        ),
    );
    assert_eq!(
        ok(
            &handler,
            request(
                HttpMethod::Get,
                "/_bicdb/apps",
                Some(TOKEN),
                HttpRequestBodyV2::Empty
            )
        ),
        json!([])
    );
    let missing = handler
        .handle(&request(
            HttpMethod::Post,
            "/_bicdb/apps/notes/explode",
            Some(TOKEN),
            HttpRequestBodyV2::Empty,
        ))
        .unwrap_err();
    assert!(matches!(missing, AppRuntimeError::NotFound(_)));
}
