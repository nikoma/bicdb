Run the public native regression with Node22 or newer:

```sh
node crates/bicdb-app-runtime/tests/node-decimal-input/qualify.mjs /absolute/config.json
```

The configuration contains `binary`, `binarySha256`, `source`, `outputRoot` and
`expectExact`. Set `expectExact` to true for qualification. False records a
vulnerable predecessor: it requires the original input-rounding defect to occur,
and its evidence explicitly reports `actualExactAdmission:false`.

The original quoted Decimal input `7922816251426433759354395033.55` cannot be
represented by a 96-bit Decimal coefficient. Native488 accepted it and stored
`7922816251426433759354395034`. No caller-selected money scale can recover the
original amount after that input conversion.

This fixture uses an official Carrier2.3.92 package, retained byte-for-byte with
its authored source and public test signing key. `fixture.json` records exact
hashes. The repeated19 seed is public qualification material, never a deployment
key. Generated package output is not edited.

The Node oracle uses BigInt coefficient/scale arithmetic. Tests exercise quoted
and raw JSON numeric tokens, HTTP body and nested arrays, model writes, generated
CRUD, finance.decimal, query and path parameters, nullable/type boundaries,
explicit Float compatibility, late-batch rollback, role and tenant denial,
idempotent replay, actual resource exact/minimum filters, SIGKILL and cold restoration. New exact syntax admission for
redundant scientific mantissas and zero exponents is identified separately from
legacy-compatible values. No Rust test or helper is built or run.

The test uses a fresh synthetic OIDC verifier, isolated database and loopback
server. It qualifies ordinary native application input; Cell, real issuer, Hub,
and complete accounting product workflows require their own evidence.

`build-carrier-fixture.mjs COMPILER EVIDENCE_DIRECTORY` rebuilds with the exact
compiler hash in `fixture.json`, using the public test key. It runs the official
formatter/check/build, retains logs, copies generated package bytes verbatim,
and records the resulting source and artifact hashes. The native resource ABI
does not support Carrier's `max` filter; this fixture declares exact and minimum.

`build-sdk-filters.mjs` independently authors a signed public ABI-v2 resource
package from source, without reading or editing Carrier output. It intentionally
omits `ContractField.value_type` and covers both declared filter contracts and
the legacy bracket-filter API. Run its actual database regression with:

```sh
node crates/bicdb-app-runtime/tests/node-decimal-input/qualify-filters.mjs /absolute/config.json
```

It verifies exact equality across scales/scientific aliases, numeric minimum
bounds, unsupported-input refusal, unchanged Float behavior, role and tenant
boundaries, and acknowledged facts after SIGKILL/cold restoration. Its BigInt
oracle does not derive expected results from floating point or runtime queries.
False `expectExact` requires a reproduced defect and explicitly reports
`actualExactFilters:false`; it is a negative control, never qualification.

The retained `docs/evidence/decimal-input-2026-11-12/after22a-input.json` covers
313 input observations on the first combined private prototype. The separate
`before22a-filters.json` and frozen driver reproduce 54 filter failures in 87
observations on that same binary. Those failures include numeric comparisons,
not only admission. The successor therefore emits existing signed Decimal
`TypedEq`/`TypedGe` filters as well as parsing without rounding. The typed
comparison remains scoped to the resource's signed Decimal field; other field
domains keep their existing behavior. Successor execution and whole-product
qualification are still required.
