# BicDB cell application host preview

> Historical Phase 1 preview. BicDB 1.0.320-beta supersedes this construction
> with the callable Phase 3 boundary documented in
> [`cell-runtime-phase3.md`](cell-runtime-phase3.md). The compatibility path
> remains available for old Phase 1 manifests, but it is not the hardened
> Cell-native application profile.

Introduced in BicDB 1.0.316-beta, this is an **industry-agnostic** serving
extension to the callable Phase-1 cell boundary. A signed application package
can execute inside one cell and expose an authenticated HTTP API. BicDB does
not contain product schemas, domain workflows, or tenant-specific application
logic.

This preview remains fail-closed for regulated production workloads. Startup
reports `regulated_data_admitted = false`; the field is industry-neutral and
does not name an application model embedded in BicDB.

## Constructed capability graph

When a `CellManifest` pins applications, `CellRuntime::open` constructs:

```text
signed CellManifest
    │
    ├── exact application name/version/digest/schema generation
    ├── exact release-policy digest
    ├── exact identity-policy digest
    └── exact deny-all egress-policy digest
             │
             ▼
       CellApplicationHost
             │
             ├── one cell database handle
             ├── signed application runtime
             ├── authenticated HTTP routes
             ├── no package lifecycle API
             ├── no pgwire/database selector
             ├── no network egress
             ├── no blob filesystem fallback
             └── no cross-cell handle
```

Application release keys and CellManifest activation keys are independent.
Possessing a valid package signing key cannot authorize that package for a
cell. Possessing a manifest key cannot make a package signed by an untrusted
release root valid.

## Pinned policy files

All three files use strict JSON structures and their exact bytes must hash to
the digests in the signed `CellManifest`.

Release policy:

```json
{
  "format": "bicdb.cell-release-policy/v1",
  "required_package_signatures": 1,
  "roots": [
    {
      "root": "generic-suite",
      "signing_keys": {
        "application-release-2026-08": "<64 lowercase Ed25519 public-key hex>"
      }
    }
  ]
}
```

The current package format carries one signature, so the preview requires
exactly one. It does not pretend to implement threshold signing. Independent
CellManifest authorization is the second authority boundary.

Identity policy:

```json
{
  "format": "bicdb.cell-identity-policy/v1",
  "issuer": "https://identity.example.test",
  "audience": "generic-cell",
  "authentication_method": "oidc-ed25519",
  "maximum_lifetime_seconds": 3600,
  "clock_skew_seconds": 30,
  "oidc_ed25519_jwks": {
    "keys": [
      {
        "kty": "OKP",
        "crv": "Ed25519",
        "alg": "EdDSA",
        "use": "sig",
        "kid": "identity-2026-08",
        "x": "<base64url Ed25519 public key>"
      }
    ]
  }
}
```

The preview requires an HTTPS issuer, the exact `oidc-ed25519` authentication
method, a maximum access-token lifetime of one hour, and no more than 120
seconds of clock skew. Applications receive verified actor claims, never JWKS
key material or the bearer token.

Egress policy:

```json
{
  "format": "bicdb.cell-egress-policy/v1",
  "mode": "deny_all"
}
```

No permissive egress or blob provider is available in this release. An
application that requires unavailable capabilities fails activation/readiness;
the runtime does not substitute ambient host access.

## Application artifact and manifest pin

Store the exact package bytes as:

```text
<artifact-root>/<package-sha256-without-prefix>.bicdb-app
```

The manifest pin identifies the release root independently of the package key:

```json
{
  "root": "generic-suite",
  "name": "generic-ledger",
  "version": "1.0.0",
  "digest": "sha256:<exact package bytes>",
  "schema_generation": 1,
  "scope": "cell",
  "data_class": "sensitive"
}
```

Before the database opens, BicDB verifies the outer artifact digest, package
signature under the named release root, embedded package name/version, cell
scope, and derived schema generation. The preview rejects `regulated` (legacy alias `phi`) and
`regulated_local` (legacy alias `phi_local`) application pins because its regulated-workload admission gate is
still closed.

Every package route must be protected. A `public: true` route is refused, and
every named authentication scheme must select OIDC Ed25519 with the exact
issuer and audience from the pinned identity policy. Package activation and
readiness must finish before BicDB persists the manifest as the cell's
monotonic active state.

## Serve the cell application

Add the policy paths and an HTTP listener to the normal Phase-1 ceremony:

```bash
bicdb-cell serve \
  --manifest cell-manifest.cbor \
  --volume cell-volume \
  --expected-cell-id 018f7b30-4f4d-7b5c-a1f6-a183663e1240 \
  --expected-volume-id vol-7f92 \
  --guest-image-digest sha256:... \
  --trusted-key manifest-2026-08=manifest-signing.pub \
  --key-file cell.key \
  --artifact-root artifacts \
  --release-policy release-policy.json \
  --identity-policy identity-policy.json \
  --egress-policy egress-policy.json \
  --http-listen 127.0.0.1:8443
```

Cleartext HTTP is restricted to loopback. A non-loopback listener requires
both `--http-tls-cert` and `--http-tls-key`. `--check` cannot be combined with
a listener. Every application route is hosted behind the pinned JWT
authenticator; anonymous package routes are refused and undeclared routes
remain unavailable.

The environment-selected launcher adds:

```text
BICDB_CELL_RELEASE_POLICY=/absolute/path/release-policy.json
BICDB_CELL_IDENTITY_POLICY=/absolute/path/identity-policy.json
BICDB_CELL_EGRESS_POLICY=/absolute/path/egress-policy.json
BICDB_CELL_HTTP_LISTEN=127.0.0.1:8443
BICDB_CELL_HTTP_TLS_CERT=/absolute/path/server.crt
BICDB_CELL_HTTP_TLS_KEY=/absolute/path/server.key
```

The three policy variables are all-or-nothing. The TLS variables are also a
pair.

## Migration and fleet semantics

An application release that changes only code requires no schema migration.
When a signed release advances schema generation, there is still only **one
migration definition** to author and review. Each affected cell must execute
that transition against its own isolated storage:

```text
one signed application release
            │
            ├── Cell 001: generation 17 → 18
            ├── Cell 002: generation 17 → 18
            └── Cell 1000: generation 17 → 18
```

That is up to 1,000 local executions, not 1,000 independently maintained
migrations. This preview verifies and activates the exact package inside one
cell during startup. Batch cohort rollout, durable per-cell activation
receipts, lazy dormant-cell convergence, pause/rollback policy, and fleet-wide
progress are Phase-4 App Root/CellAgent work and are **not yet implemented**.
Until that controller lands, an operator must orchestrate cell manifest
transitions externally and must not claim one-command fleet convergence.

The intended fleet contract is additive expand/converge/contract migration,
bounded cohorts, temporary version skew, safe retry, and isolation of a failed
cell or cohort from the rest of the fleet.

## Remaining admission gates

The application host closes the missing callable application-execution and
cell-local identity gap. It does not close attested KMS release, complete
page/WAL/temp/index/blob/backup encryption, external anti-rollback state,
independently enforced process or microVM isolation, threshold release and
fleet activation, cell-scoped HA, device/cross-cell protocols, or independent
regulated-workload certification.

See [Phase 1 operator contract](cell-runtime-phase1.md) and
[BicDB Cell/Application Architecture](bicdb-cell-application-architecture.md).
