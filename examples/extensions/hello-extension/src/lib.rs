use bicdb_extension::{export_bicdb_extension, ExtensionInvocation, ExtensionInvocationResult};
use serde_json::json;

// Production build tooling may generate this JSON from a BicDbExtension
// implementation. Keeping it literal here makes the exported bytes static and
// keeps the example buildable without a custom build script.
const MANIFEST: &str = r#"{
  "identity": {
    "name": "hello_extension",
    "version": "1.0.0",
    "abi_version": 1,
    "description": "Minimal BicDB WASM extension"
  },
  "dependencies": [],
  "capabilities": ["functions", "http_routes", "queue_events"],
  "permissions": {
    "read_relations": [],
    "write_relations": [],
    "publish_queues": [],
    "consume_queues": ["hello.jobs"],
    "network_hosts": []
  },
  "limits": {
    "memory_bytes": 16777216,
    "fuel": 1000000,
    "timeout_ms": 1000,
    "max_input_bytes": 65536,
    "max_output_bytes": 65536,
    "max_concurrency": 4
  },
  "functions": [{
    "name": "hello",
    "export": "hello",
    "arguments": ["jsonb"],
    "returns": "jsonb",
    "volatility": "immutable"
  }],
  "indexes": [],
  "storage": [],
  "routes": [{
    "name": "hello_route",
    "method": "GET",
    "path": "/hello",
    "export": "hello",
    "auth": "public"
  }],
  "subscriptions": [{
    "name": "hello_jobs",
    "source": {
      "kind": "queue",
      "queue": "hello.jobs",
      "group": "hello-extension"
    },
    "export": "on_job",
    "max_attempts": 5,
    "visibility_timeout_ms": 30000
  }],
  "observability": []
}"#;

fn handle(input: &[u8]) -> Vec<u8> {
    let result = match serde_json::from_slice::<ExtensionInvocation>(input) {
        Ok(invocation) => match invocation.target.as_str() {
            "hello" => ExtensionInvocationResult {
                status: 200,
                body: json!({
                    "message": "hello from BicDB",
                    "input": invocation.payload,
                }),
                ..default_result()
            },
            "on_job" => ExtensionInvocationResult {
                status: 200,
                body: json!({"processed": invocation.id}),
                ack: true,
                ..default_result()
            },
            target => ExtensionInvocationResult {
                status: 404,
                error: Some(format!("unknown target `{target}`")),
                ..default_result()
            },
        },
        Err(error) => ExtensionInvocationResult {
            status: 400,
            error: Some(format!("invalid invocation: {error}")),
            ..default_result()
        },
    };
    serde_json::to_vec(&result).expect("extension result is serializable")
}

fn default_result() -> ExtensionInvocationResult {
    ExtensionInvocationResult {
        status: 200,
        headers: Default::default(),
        body: serde_json::Value::Null,
        ack: true,
        retry_after_ms: None,
        error: None,
    }
}

export_bicdb_extension!(MANIFEST, handle);
