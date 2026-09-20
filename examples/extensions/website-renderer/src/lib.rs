use std::collections::BTreeMap;

use bicdb_extension::{export_bicdb_extension, ExtensionInvocation, ExtensionInvocationResult};
use serde_json::{json, Value};

const MANIFEST: &str = r#"{
  "identity": {
    "name": "website_renderer",
    "version": "1.0.0",
    "abi_version": 1,
    "description": "Renders versioned BicDB website bundles without network access"
  },
  "dependencies": [],
  "capabilities": ["http_routes"],
  "permissions": {
    "read_relations": [],
    "write_relations": [],
    "publish_queues": [],
    "consume_queues": [],
    "network_hosts": []
  },
  "limits": {
    "memory_bytes": 16777216,
    "fuel": 2000000,
    "timeout_ms": 1000,
    "max_input_bytes": 1048576,
    "max_output_bytes": 1048576,
    "max_concurrency": 16
  },
  "functions": [],
  "indexes": [],
  "storage": [],
  "routes": [{
    "name": "website",
    "method": "GET",
    "path": "/",
    "export": "render_website",
    "auth": "public"
  }],
  "subscriptions": [],
  "observability": []
}"#;

fn handle(input: &[u8]) -> Vec<u8> {
    let result = match serde_json::from_slice::<ExtensionInvocation>(input) {
        Ok(invocation) if invocation.target == "render_website" => render(&invocation.payload),
        Ok(invocation) => error_result(404, format!("unknown target `{}`", invocation.target)),
        Err(error) => error_result(400, format!("invalid invocation: {error}")),
    };
    serde_json::to_vec(&result).expect("website response is serializable")
}

fn render(payload: &Value) -> ExtensionInvocationResult {
    let relative_path = payload
        .pointer("/request/relative_path")
        .and_then(Value::as_str)
        .unwrap_or("/");
    let content = &payload["website"]["release"]["content"];
    let pages = content.get("pages").and_then(Value::as_object);
    let exact_page = pages.and_then(|pages| pages.get(relative_path));
    let status = if exact_page.is_some() { 200 } else { 404 };
    let page = exact_page.or_else(|| pages.and_then(|pages| pages.get("/404")));
    let Some(page) = page else {
        return html_result(
            404,
            "<!doctype html><html><body><h1>Not found</h1></body></html>".to_string(),
        );
    };
    let title = page
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("BicDB website");
    let html = page.get("html").and_then(Value::as_str).unwrap_or("");
    let stylesheet = content
        .get("stylesheet")
        .and_then(Value::as_str)
        .unwrap_or("");
    let body = format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
         <title>{}</title><style>{stylesheet}</style></head><body>{html}</body></html>",
        escape_html(title)
    );
    html_result(status, body)
}

fn html_result(status: u16, body: String) -> ExtensionInvocationResult {
    ExtensionInvocationResult {
        status,
        headers: BTreeMap::from([
            (
                "content-type".to_string(),
                "text/html; charset=utf-8".to_string(),
            ),
            (
                "content-security-policy".to_string(),
                "default-src 'none'; style-src 'unsafe-inline'; img-src data:; form-action 'self'"
                    .to_string(),
            ),
            ("x-content-type-options".to_string(), "nosniff".to_string()),
        ]),
        body: Value::String(body),
        ack: true,
        retry_after_ms: None,
        error: None,
    }
}

fn error_result(status: u16, error: String) -> ExtensionInvocationResult {
    ExtensionInvocationResult {
        status,
        headers: BTreeMap::new(),
        body: json!({"error": error}),
        ack: true,
        retry_after_ms: None,
        error: None,
    }
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

export_bicdb_extension!(MANIFEST, handle);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_a_requested_page_and_escapes_the_title() {
        let result = render(&json!({
            "website": {
                "release": {
                    "content": {
                        "stylesheet": "body{}",
                        "pages": {
                            "/": {
                                "title": "<Home>",
                                "html": "<h1>Trusted body</h1>"
                            }
                        }
                    }
                }
            },
            "request": {"relative_path": "/"}
        }));
        assert_eq!(result.status, 200);
        let html = result.body.as_str().unwrap();
        assert!(html.contains("&lt;Home&gt;"));
        assert!(html.contains("<h1>Trusted body</h1>"));
        assert_eq!(
            result.headers.get("content-type").map(String::as_str),
            Some("text/html; charset=utf-8")
        );
        assert!(result.headers["content-security-policy"].contains("form-action 'self'"));
    }

    #[test]
    fn unknown_pages_use_the_release_404_page() {
        let result = render(&json!({
            "website": {
                "release": {
                    "content": {
                        "pages": {
                            "/404": {"title": "Missing", "html": "<h1>Missing</h1>"}
                        }
                    }
                }
            },
            "request": {"relative_path": "/nope"}
        }));
        assert_eq!(result.status, 404);
        assert!(result.body.as_str().unwrap().contains("<h1>Missing</h1>"));
    }
}
