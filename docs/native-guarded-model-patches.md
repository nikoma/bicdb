# Guarded single-record native model patches

`Model.patch(id:, changes:, expected_version:)` invokes the native resource host's ordinary
Update path inside an existing transaction. It does not use SQL or Upsert. Signed mutation
bindings, resource field/tenant/reference policies, optimistic counters and finance/lifecycle
validators therefore execute exactly as they do for a native resource update. A public CRUD
update route is not required; application-owned guarded actions remain the HTTP boundary.

The compiler requires an explicit expected version for versioned models. The host independently
checks that version and rejects forbidden fields or stale edits. Older hosts reject this unknown
model method before applying it; acceptance of a signed package alone is not runtime qualification.

Application failures with the canonical unauthorized, forbidden, not-found, conflict,
version-conflict, idempotency-conflict and rate-limit codes use their corresponding HTTP status.
Other domain validation failures retain 400 and their stable code/message keys.

The independent Node fixture lives in Carrier's `scripts/qualify-native-guarded-patch.mjs` and
`scripts/fixtures/native-guarded-patch`. It exercises a declared native journal with exact contra
reversal, rejected frozen memo edits, rejected missing/inexact contras, tenant denial, stale
patches, concurrency, crash/reopen and cold restore. Compiler preflight and signed output are
separate checks; native execution evidence must be attached before release.
