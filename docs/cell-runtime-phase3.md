# BicDB Phase 3 cell-native application runtime

Introduced in BicDB 1.0.320-beta, Phase 3 is the callable,
industry-agnostic application boundary for one Cell. It combines the Phase 2
cryptographic Cell with signed backend and frontend packages, cell-local
membership, device-bound session handoff, and exact-binary feature contracts.

Phase 3 does not admit regulated production data. Runtime status continues to
report `regulated_data_admitted = false`; later fleet, HA, device, grant,
deployment-attestation, and independent-review gates remain open.

## Constructed authority graph

```text
signed CellManifest
    │
    ├── exact CellId, volume, lineage, binary, image, profiles, epochs
    ├── one-shot attested Cell key lease
    ├── exact signed application packages
    ├── pinned release, identity, egress, authorization policies
    └── pinned exact-binary feature certification
             │
             ▼
       CellApplicationHost
             │
             ├── one encrypted Cell database
             ├── signed backend modules
             ├── signed same-origin frontend bytes
             ├── cell-local roles, scopes, and device keys
             ├── host-only Cell sessions
             └── no pgwire, database selector, cross-cell handle,
                 ambient egress, raw SQL, blob, or secret provider
```

The environment flag selects this construction path; it is not the security
boundary. `BICDB_RUNTIME_MODE=cell` permits only Cell commands, and the signed
manifest profiles determine which typed construction path can open storage.

## Phase 3 manifest profiles

A Phase 3 manifest uses:

```json
{
  "storage": {
    "encryption_profile": "cell-bound-xchacha20poly1305-v2"
  },
  "runtime": {
    "isolation_profile": "phase3-cell-native-process-isolated"
  },
  "policy": {
    "security_profile": "phase3-cell-native-deny-regulated",
    "authorization_policy_digest": "sha256:...",
    "feature_certification_digest": "sha256:..."
  }
}
```

It inherits the Phase 2 requirements for an attested, signed, one-shot key
lease. A development key file is refused.

## Signed component contract

Each Phase 3 package carries signed component and frontend fields in the
application package envelope:

```json
{
  "components": [
    {
      "name": "api",
      "kind": "backend",
      "scope": "cell",
      "data_class": "sensitive",
      "capabilities": ["database", "http_routes"],
      "egress": [],
      "database_features": ["row_level_security"]
    },
    {
      "name": "web",
      "kind": "frontend",
      "scope": "cell",
      "data_class": "sensitive",
      "capabilities": ["frontend_assets"],
      "egress": [],
      "database_features": []
    }
  ],
  "frontend_assets": {
    "index.html": {
      "content_type": "text/html; charset=utf-8",
      "bytes": [60, 33, 100, 111, 99, 116, 121, 112, 101, 32, 104, 116, 109, 108, 62]
    }
  }
}
```

The package signature covers exact frontend byte hashes and every component
contract. Startup fails before database open when:

- scope or data class differs from the Cell pin;
- a component requests regulated data or egress;
- the capability union differs from executable package surfaces;
- raw SQL, ambient secrets, blobs, or egress are requested;
- a frontend lacks a signed frontend component or `index.html`;
- an HTTP route lacks a signed backend component;
- the exact database-feature set differs from package behavior or is absent
  from the pinned binary certification; or
- a package selects its own authentication verifier.

Canonical output uses `regulated` and `regulated_local`. The old `phi` and
`phi_local` spellings are accepted only as deserialization aliases.

## Cell-local authorization policy

The manifest-pinned authorization policy owns final authority:

```json
{
  "format": "bicdb.cell-authorization-policy/v1",
  "cell_id": "018f7b30-4f4d-7b5c-a1f6-a183663e1240",
  "authorization_epoch": 7,
  "session_lifetime_seconds": 900,
  "minimum_assurance": "hardware-bound",
  "members": [
    {
      "user_id": "principal-a",
      "roles": ["record-reader"],
      "scopes": ["records.read"],
      "enabled": true,
      "devices": [
        {
          "device_id": "device-a",
          "public_key": "<64 lowercase Ed25519 public-key hex>",
          "enabled": true
        }
      ]
    }
  ]
}
```

External OIDC proves identity and assurance, not application authority. A
handoff assertion must be valid for at most 120 seconds and bind the CellId,
device id, nonce, and one-time assertion id. The device signs the
domain-separated handoff message. The Cell then loads roles and scopes only
from its local policy and issues a short-lived HS256 session derived with HKDF
from the Cell key, CellId, manifest digest, and authorization epoch.

The session is stored in a `Secure; HttpOnly; SameSite=Strict`
`__Host-bicdb-session` cookie. Supplying both that cookie and an Authorization
header, or duplicate session cookies, fails closed. Raw authorization, cookie,
proxy-authorization, and API-key values are withheld from both guest code and raw
WASM guest requests; guests receive only the verified actor context.

Consumed handoff ids are stored as HMAC-authenticated hashes in a bounded,
fsynced Cell-local journal and remain rejected after restart. The journal does
not yet have an external data-commit rollback root, which is one reason
regulated-data admission remains closed.

## Same-origin frontend

Frontend bytes are served only from the verified package. There is no
filesystem or CDN fallback. With one frontend package, `/` maps to its signed
`index.html`; immutable assets are also available at:

```text
/_bicdb/apps/<application>/<package_sha256>/<asset-path>
```

Responses use exact content types, `nosniff`, frame denial, no-referrer,
same-origin opener/resource policies, a restrictive permissions policy, and a
strict self-only CSP. Unversioned assets are `no-store`; digest paths are
immutable. Application code cannot emit `Set-Cookie` or weaken host security
headers.

## Exact-binary feature certification

The second new pinned policy is:

```json
{
  "format": "bicdb.cell-feature-certification/v1",
  "bicdb_binary_digest": "sha256:...",
  "database_format": 2,
  "conformance_suite_digest": "sha256:...",
  "certified_features": ["row_level_security", "stored_functions", "triggers", "jobs"]
}
```

The binary digest and database format must exactly match the signed Cell
manifest. Package-required features are derived from executable surfaces and
must exactly equal signed component declarations, then be a subset of the
certification. Phase 8 will add independent evidence signatures and admission
verification; Phase 3 treats this as manifest-authorized startup policy and
does not claim independent certification.

## Launch

The CLI construction path adds the two Phase 3 policies to the Phase 2 startup
ceremony:

```bash
bicdb cell serve \
  --manifest cell.cbor \
  --volume cell-volume \
  --expected-cell-id 018f7b30-4f4d-7b5c-a1f6-a183663e1240 \
  --expected-volume-id vol-a \
  --guest-image-digest sha256:... \
  --trusted-key manifest-a=manifest-a.pub \
  --key-lease-fd 3 \
  --kms-trusted-key kms-a=kms-a.pub \
  --attestation-nonce <64-lowercase-hex> \
  --artifact-root artifacts \
  --release-policy release-policy.json \
  --identity-policy identity-policy.json \
  --egress-policy egress-policy.json \
  --authorization-policy authorization-policy.json \
  --feature-certification feature-certification.json \
  --http-listen 127.0.0.1:8443
```

The environment launcher supports the equivalent paths:

```text
BICDB_RUNTIME_MODE=cell
BICDB_CELL_AUTHORIZATION_POLICY=/run/bicdb/authorization-policy.json
BICDB_CELL_FEATURE_CERTIFICATION=/run/bicdb/feature-certification.json
```

These are in addition to the Phase 2 manifest, volume, key-lease, artifact,
release, identity, egress, and listener variables. Policy variables are
all-or-nothing. Non-loopback listeners require a TLS certificate/key pair.

## Proven tests

The release test suite covers:

- signature coverage and tamper rejection for frontend bytes and component
  authority;
- exact capability/database-feature contract enforcement;
- full `CellRuntime::open` construction with an attested key lease and signed
  frontend/backend package;
- same-origin signed frontend delivery and security headers;
- device proof, local role/scope replacement, host-only session verification,
  duplicate/ambiguous credential rejection, and persistent handoff replay
  refusal; and
- continued denial of regulated data classes and admission.

The acceptance test opens a real encrypted Cell database, activates its signed
application, starts its HTTP host, and fetches its signed frontend. Phase 3 is
therefore a callable runtime, not merely a target API specification.
