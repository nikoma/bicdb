# Operating the BicDB application host

`bicdb app` manages signed ABI v2 applications and runs HTTP plus pgwire over
one shared in-process BicDB session. Application logic does not use a database
socket, connection pool, external PostgreSQL server, or backend service.

## Package input limits

`install`, `stage`, `validate`, `verify-signature`, `upgrade`, and restoring saved
packages use the same bounded file reader before JSON decoding. The reader
checks the open file's length, then limits the actual bytes read from that same
file, including trailing whitespace. Growing files and streams cannot bypass
the limit, and reaching it is not treated as an artificial end of a valid JSON
prefix.

The decoded limit is the lower of `ApplicationHostConfig.max_package_bytes` and
the `PackageVerifier` limit (both default to 256 MiB in the CLI). The encoded
ceiling is **16 × decoded limit + 64 KiB**, allowing ordinary pretty-printed JSON
byte arrays while imposing a finite representation-overhead budget. Unusually
padded JSON beyond this ceiling is rejected even if its decoded content would
fit. Decoder allocations also include JSON parser and collection overhead.

Decoded sizing includes payload bytes, module names, frontend paths and media
types, and compact-JSON sizes of the full manifest (including signature
metadata) and component declarations. Size arithmetic is checked. Modules are
limited to 4,096 entries with nonempty names of at most 1,024 UTF-8 bytes;
frontend assets retain their 4,096-entry and 1,024-byte-path limits. These map
limits also apply during decoding, before their associated values are read,
and duplicate module/asset keys are rejected. In-memory package
construction still faces the same metadata and size checks at verification.

`PackageVerifier::read_package` and `read_package_file`, and
`ApplicationRuntime::read_package_file`, perform bounded decoding only. A
successful read does **not** establish trust: signature, artifact-hash,
manifest, capability, and runtime checks still run when validating or staging.
The package format and signing envelope are unchanged.

## Trust and secret setup

Every management command that reads packages needs all signing keys used by
active snapshots:

```bash
bicdb app ./data \
  --trusted-key app-release=./app-release.pub \
  validate ./build/example-app.bicdb.json
```

Keys may be 32 raw Ed25519 bytes, lowercase hex, or base64. Secrets are loaded
into the trusted host, not the WASM environment:

```bash
--secret jwt-signing=2026-07=./secrets/jwt-signing.pem
```

The local blob signed-URL key is created with mode `0600` from the operating
system random source and remains under the application package root.
Application blob packages use application-isolated physical namespaces beneath the
same root. Back up the blob root with the package catalog and database; logical
keys and active signed URLs remain usable after restart when their package and
signing key remain active.

Production deployments can select the trusted `bicdb-blob-s3` provider module
with `--blob-config`. The module runs in the native host: S3 credentials and
network authority are never passed to application WASM. Paths in this JSON are
relative to the configuration file, and every credential file must contain one
non-empty line and have mode `0600` on Unix:

```json
{
  "kind": "s3_compatible",
  "endpoint": "https://objects.example.com",
  "bucket": "application-blobs",
  "key_prefix": "production/applications",
  "region": "us-west-2",
  "access_key_id_file": "secrets/s3-access-key-id",
  "secret_access_key_file": "secrets/s3-secret-access-key",
  "force_path_style": false,
  "max_object_bytes": 268435456,
  "server_side_encryption": "aws:kms",
  "sse_kms_key_id": "alias/application-blobs"
}
```

Use `force_path_style: true` for compatible stores such as MinIO when their DNS
configuration does not support bucket virtual hosts. Cleartext endpoints are
rejected unless `allow_insecure_http: true`; that exception is intended only
for loopback/private test infrastructure. Optional session credentials use
`session_token_file`. `max_object_bytes` must be at most 1 GiB because the
current provider ABI materializes one bounded object in host memory. The host
health-checks the configured bucket before every application management or
serve command and fails closed if it is unavailable.

Grant only `s3:ListBucket`/bucket inspection and `s3:GetObject`, `s3:PutObject`,
and `s3:DeleteObject` beneath the configured key prefix. Immutable generation
objects plus an atomic metadata pointer preserve the old value if an overwrite
cannot commit. Reads verify the signed metadata's size and SHA-256. Provider
responses and error bodies are bounded and discarded; guest-visible errors do
not contain credentials, endpoints, provider keys, or response bodies. Signed
client URLs remain host-proxied `/_bicdb/blob` capabilities, so object-store
credentials and physical keys never leave BicDB.

The local provider can also be selected explicitly:

```json
{ "kind": "local", "root": "./durable-blobs" }
```

## Integration providers

Application packages can bind dynamic HTTP/webhook clients, Redis-compatible
helpers, and SMTP email to operator-owned providers with
`--integration-config`. The package signs only the application/provider name,
exact required HTTP header names, schemes, relative request policy, Redis
helper set, email helper and message bounds, and scoping rules. Endpoints and
credentials remain exclusively in this versioned host config:

```json
{
  "version": 1,
  "http": [
    {
      "application": "example-orders",
      "provider": "PaymentsWebhook",
      "base_url": "https://hooks.example.com/applications/",
      "header_files": {
        "authorization": "secrets/webhook-authorization"
      },
      "healthcheck_path": "/health",
      "healthcheck_statuses": [200]
    }
  ],
  "redis": [
    {
      "application": "example-orders",
      "provider": "default",
      "endpoint": "rediss://cache.example.com:6379",
      "password_file": "secrets/redis-password",
      "database": 0,
      "key_prefix": "application-cache",
      "channel_prefix": "application-events",
      "pool_size": 16,
      "connect_timeout_ms": 5000,
      "request_timeout_ms": 5000
    }
  ],
  "email": [
    {
      "application": "example-notifications",
      "provider": "default",
      "endpoint": "smtps://smtp.example.com:465",
      "username_file": "secrets/smtp-username",
      "password_file": "secrets/smtp-password",
      "allowed_from": ["Care Team <care@example.com>"],
      "allowed_recipient_domains": ["example.com"],
      "max_recipients": 100,
      "max_message_bytes": 4194304,
      "pool_size": 16,
      "timeout_ms": 5000
    }
  ],
  "grpc": [
    {
      "application": "example-inventory",
      "provider": "InventoryGrpc",
      "endpoint": "https://inventory.example.com",
      "bearer_token_file": "secrets/inventory-token",
      "allowed_methods": ["/example.inventory.v1.Inventory/Reserve"],
      "max_request_bytes": 4194304,
      "max_response_bytes": 4194304,
      "connect_timeout_ms": 5000,
      "request_timeout_ms": 30000
    }
  ],
  "tokenizer": [
    {
      "application": "example-assistant",
      "provider": "default",
      "tokenizer_file": "models/tokenizer.json",
      "max_tokens": 1000000
    }
  ],
  "embeddings": [
    {
      "application": "example-assistant",
      "provider": "default",
      "model": "embeddinggemma-300m",
      "model_dir": "models/embeddinggemma-300m-ONNX",
      "dimensions": 768
    }
  ],
  "llm": [
    {
      "application": "example-assistant",
      "provider": "Primary",
      "endpoint": "https://llm.example.com/v1/chat/completions",
      "api_key_file": "secrets/llm-api-key",
      "wire_format": "openai",
      "model": "operator-model",
      "request_timeout_ms": 30000,
      "input_microusd_per_million_tokens": 2500000,
      "output_microusd_per_million_tokens": 10000000
    }
  ],
  "evaluations": [
    {
      "application": "example-assistant",
      "provider": "example_eval_0",
      "dataset_file": "evaluations/assistant.jsonl"
    }
  ]
}
```

Credential paths are relative to the config and must be mode `0600` on Unix.
Literal HTTP headers are intended only for non-secrets; use `header_files` for
credentials. Each HTTP binding is confined to its configured origin and base
path, supplies exactly the signed header-name set, strips credential response
headers, and must pass its startup health check. Cleartext or private-network
HTTP and cleartext Redis require their explicit `allow_insecure_http`,
`allow_private_networks`, or `allow_insecure_redis` flags. Cleartext SMTP
likewise requires `allow_insecure_smtp`; use it only for explicitly approved
test/private infrastructure. SMTP URLs cannot contain credentials, paths,
queries, or fragments. Username and password files must be configured
together. The provider enforces the sender allowlist, optional recipient-domain
allowlist, message/recipient bounds, deadline, and bounded pool, and records no
raw recipients or body in evidence. Redis channels are physically scoped by
application; keys are scoped by application and trusted tenant identity.
Missing bindings make the package unready rather than falling back to ambient
network access.

Unary gRPC bindings use the same application/provider identity rule. The
Application package signs every method path, protobuf tag and wire type, recursive
request/response schema, deadline, retry count, and size limit. Operator config
supplies only the credential-free HTTP/2 origin plus an optional mode-`0600`
bearer token, client certificate/key, and CA. `allowed_methods` can further
narrow the signed method set; it can never expand it. Cleartext h2c requires
`allow_insecure_http` and should remain loopback/private test infrastructure.
The native provider never returns credentials or raw protobuf frames to WASM.

Tokenizer, embedding, and LLM bindings follow the same exact
application/provider identity rule. Tokenizer and ONNX model files, concrete
LLM models, endpoints, credentials, optional operator system prompts, and
pricing are operator state. Pricing is integer micro-USD per million tokens;
BicDB rounds each input and output component upward before routing or budget
accounting. A routed logical client can select only physical clients named in
its signed package contract, while every request is filtered to that logical
client's exact tool set. Missing provider bindings make activation and
readiness fail closed.

Evaluation bindings point to JSON Lines datasets whose cases must match the
signed evaluation schema. Run one evaluation outside the serving process with:

```bash
bicdb app ./data \
  --trusted-key release=./release.pub \
  --integration-config ./integrations.json \
  evaluate example-assistant eval_0_quality --timeout-ms 300000
```

The command prints passed/total cases, pass rate, and requirement status, and
returns nonzero if the compiler-signed aggregate requirement fails. The
evaluation runs under its signed least-authority identity; it never inherits
operator tenant, workspace, client, service, role, or scope authority.

Signed plugin services can opt into `delegated_authority` on both their export
and exact import. This makes the typed, request-validated method the boundary
while the callee retains its own signed implementation authority—for example,
an SMS or payment plugin's operator-bound HTTP client. A one-sided or forged
delegation fails closed. Packages without the field retain legacy caller/callee
authority intersection.

## Lifecycle

```bash
bicdb app ./data --trusted-key release=./release.pub stage app.json
bicdb app ./data --trusted-key release=./release.pub activate my-app
bicdb app ./data --trusted-key release=./release.pub upgrade app-v2.json
bicdb app ./data --trusted-key release=./release.pub rollback my-app
bicdb app ./data --trusted-key release=./release.pub disable my-app
bicdb app ./data --trusted-key release=./release.pub remove my-app
```

Package files are content addressed and immutable. The active package set,
staged set, and rollback history are persisted together by fsync, temporary
file, rename, and parent-directory fsync. Lifecycle writers are serialized in
the process and by an OS file lock. Activation compiles modules and prepares
workers before publishing one new catalog generation. Existing requests retain
the old immutable snapshot. A failed activation leaves the previous snapshot
serving.

Useful inspection commands are `list`, `inspect`, `dependency-graph`, `routes`,
`services`, `workers`, `schedules`, `evaluate`, `doctor`, and
`export-diagnostics`.

## Serving

Production HTTP must use TLS:

```bash
bicdb app ./data \
  --trusted-key release=./release.pub \
  --secret application-key=current=./secrets/application-key \
  serve \
  --http-host 0.0.0.0 --http-port 8443 \
  --http-tls-cert ./tls/fullchain.pem \
  --http-tls-key ./tls/private-key.pem \
  --pg-host 127.0.0.1 --pg-port 5433 \
  --pg-require-auth --pg-require-tls \
  --pg-tls-cert ./tls/fullchain.pem \
  --pg-tls-key ./tls/private-key.pem \
  --oidc-jwks ./identity/jwks.json \
  --jwt-issuer https://identity.example \
  --jwt-audience example-app
```

Loopback development may use cleartext HTTP. Non-loopback cleartext is refused.
Choose exactly one JWT source: `--jwt-hs256-key` or an operator-pinned Ed25519
`--oidc-jwks`. Those legacy arguments configure one verifier. Applications
with signed per-route schemes use `--auth-config` instead:

```json
{
  "schemes": [
    {
      "kind": "jwt_hs256",
      "issuer": "https://internal.example",
      "audience": "internal-api",
      "keys": [
        { "key_id": "auth-2026-07", "key_file": "internal-current.key" },
        { "key_id": "auth-2026-06", "key_file": "internal-previous.key" }
      ]
    },
    {
      "kind": "oidc_ed25519",
      "issuer": "https://identity.example",
      "audience": "workforce-api",
      "jwks_file": "workforce-jwks.json"
    },
    {
      "kind": "oidc_rs256",
      "issuer": "https://auth.example",
      "audience": "business-api",
      "jwks_file": "business-jwks.json"
    }
  ]
}
```

HS256 schemes accept either the legacy single `key_file` or a non-empty `keys`
overlap set, but never both. Application-issued tokens carry the secret provider's
key id; keep the previous verification key for at least the maximum access-token
lifetime when rotating, then remove it from the operator configuration. Key
paths may be absolute or relative to the configuration file. The host
rejects empty or duplicate verifier contracts, and a protected route fails
closed unless an operator verifier exactly matches its signed kind, issuer,
and audience. OIDC JWKS files are pinned operator inputs; `oidc_rs256` accepts
only RSA signing keys with `alg: RS256`, a `sig` use when declared, unique
key ids, and a modulus of at least 2048 bits. Key material is never read from
an application package.

Application packages with a signed security contract also require the named
secret-provider versions through `--secret`. Add the new signing/HMAC/encryption
version before activating a package that selects it, retain the previous
version and verifier key for the maximum live token/link/ciphertext migration
window, and remove the old version only after that window or an explicit
revocation/migration. Refresh replay and session revocation are durable BicDB
state; restarting the host does not reset either boundary.

The host exposes `/livez`, `/readyz`, and `/openapi/{application}`. Readiness is
false if a required package, signature, dependency, migration, contract, route,
worker, schedule, secret/provider, or schema check is not healthy.

`/_bicdb/blob` is a host-owned signed transfer endpoint. Do not place it behind
middleware that rewrites its query string or HTTP method. A reverse proxy may
terminate TLS, but must preserve the complete encoded query and enforce a body
limit at least as strict as the application declaration. The host independently
checks signature, expiry, method, active namespace, size, content type, and
scan policy; no bearer token is required because the URL itself is the bounded
capability.

## Limits

Configure request/response byte limits, request timeout, HTTP admission,
per-minute rate limit, Wasmtime memory/fuel/time/input/output, package size,
per-plugin concurrency, host-call size, blob chunks, observability quota, and
idempotency capacity. The host uses a bounded process-wide observability ring;
required audit records remain durable BicDB rows.

Application OTLP packages require a matching operator exporter in the integration
configuration. A JSONL path is optional and can be combined with OTLP:

```json
{
  "version": 1,
  "observability": {
    "jsonl_path": "./observability/events.jsonl",
    "capacity": 100000,
    "otlp": {
      "endpoint": "https://collector.example",
      "protocol": "otlp_http",
      "header_files": {
        "authorization": "./secrets/otel-authorization"
      },
      "timeout_ms": 10000,
      "queue_capacity": 100000,
      "export_interval_ms": 1000
    }
  }
}
```

`protocol` is `otlp_http` or `otlp_grpc` and must match the signed application
contract. HTTP appends `/v1/traces`, `/v1/metrics`, and `/v1/logs`; gRPC uses
the standard OTLP services at the configured endpoint. Header files use the
same restricted-permission credential checks as other integration providers.
Set `allow_insecure_http: true` only for an explicitly accepted cleartext local
collector. Export queue pressure increments the host dropped-event diagnostic
without changing application transaction outcomes.

WASM receives fresh store/instance memory for every invocation. One application
runtime owns a shared Wasmtime engine, epoch watchdog, and bounded allocation
pool (256 instance slots by default). Parallel compilation is disabled so a
CPU-count-sized global compiler pool cannot remain resident. Compiled modules
are cached by SHA-256 with a 128-entry LRU ceiling; active, staged, and rollback
snapshots retain their own immutable `Arc` even after a cache eviction.

Application behavior programs are prepared once per immutable package snapshot.
The prepared plan reuses the already validated signed manifest, resource
contracts, and exact service/client/secret/event/realtime/workflow binding maps;
ordinary in-process calls do not clone or revalidate that graph per request.
Binary HTTP and signed-blob bodies remain raw byte vectors through the host
path, while JSON is encoded only at the protocol edge.

Inspect the live bounds and cache counters with:

```bash
bicdb app ./data --trusted-key release=./release.pub performance
```

The same snapshot is included in `export-diagnostics`. The release regression
gate is `scripts/application-runtime-performance-gate.sh`; on the reference
build host its 4,000-request, full-router zero-hop baseline is p50 10 µs and p95
12 µs, with 31 MiB peak RSS, six open file descriptors, and three threads. The
portable enforced ceilings are p50 500 µs, p95 2 ms, 512 MiB peak RSS, 64 file
descriptors, and eight threads.

## Recovery

On restart, BicDB re-verifies every referenced package and exact dependency,
recompiles modules, rebuilds routes, checks readiness, and starts supervisors.
Committed publish-on-commit entries replay through BicDB’s durable broker
outbox with stable IDs; uncommitted entries do not publish.

Do not edit package files or `snapshots/active.json`. Keep the package root and
database on durable local storage and include both in backups. Use
`export-diagnostics` before changing a failed installation.
