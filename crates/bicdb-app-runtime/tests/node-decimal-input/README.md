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
idempotent replay, SIGKILL and cold restoration. New exact syntax admission for
redundant scientific mantissas and zero exponents is identified separately from
legacy-compatible values. No Rust test or helper is built or run.

The test uses a fresh synthetic OIDC verifier, isolated database and loopback
server. It qualifies ordinary native application input; Cell, real issuer, Hub,
and complete accounting product workflows require their own evidence.
