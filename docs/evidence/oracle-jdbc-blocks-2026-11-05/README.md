# Oracle anonymous block JDBC qualification — 2026-11-05

The explicit reference adapter submits Oracle anonymous blocks over stock
PostgreSQL JDBC 42.7.8 without splitting their internal semicolons. It uses public
driver interfaces and an explicit preview-profile connection. The adapter is an
Apache-2.0 example, not a published Maven release or Oracle driver replacement.

## Executed validation

- Java unit tests: 10 passed.
- Generated provider tests: 4 passed, including compilation/execution of both
  transport variants and rejection of dirty/unpinned source before modification.
- Disposable protocol checks: 112 passed (37 each for extended, simple and
  extended-cache-everything modes; one default PostgreSQL profile rejection).
- The original pinned application provider compiled in both variants. A staged
  versioned provider passed exact numeric SQL and anonymous-block transport checks.
- Selected unchanged application test class `POInsertSQLTest`: 20 executed,
  19 passed, one failed, zero errors and zero skips. The adapter's actual runtime
  class origin was verified. The failure is unsupported `EMPTY_BLOB()` execution;
  it is retained as a failure, not excluded or converted to a passing assertion.

The application test ran on a disposable database clone with loopback-only
networking. Its database shut down cleanly. The original product and accepted
running database were not modified. A first same-version staging attempt loaded
an old provider; a fresh OSGi bundle version corrected class selection before the
reported adapter-active run. Applications must verify installed class versions.

## Artifact and source identities

- Backend source: `b0d9fb640a004f125fe3ddd8cdea10e3e045c8da`.
- Backend SHA-256: `d36dd27fda59def47510bdf24c0f7a8a3a311260caf56ee64e090ea006639086`.
- Adapter SHA-256: `9102b2ad40e5f9f1366e9d1a126556ec240c5e8423df5c30e02590506a3333ac`.
- PostgreSQL JDBC SHA-256: `2a32a9dcbc42d67a50ad3a0de5efd102c8d2be46720045f2cbd6689f160ab7c7`.
- External application source: `da0730aec42f3e9f5f671e81a70ff71e24d5e534`.
- Unchanged application test bundle SHA-256:
  `261afbb15ba933880ddf8cb9bee9f5fa07f9dc21b205ba424f7036ba6e7679e7`.

[Protocol results](protocol-results.json) record each bounded check and clean
shutdown. Reproduce with the [public runner](../../../../examples/oracle/jdbc/README.md).
Application compilation and runtime tests additionally require the separately
built upstream product; that product and its database are not included here.

Callable OUT parameters, raw statement block batches, complete LOB semantics,
arbitrary Oracle JDBC APIs and full application acceptance remain open. Default
PostgreSQL semantics are not replaced by the adapter. No production qualification,
full Oracle conformance or completion of the compatibility programme is claimed.

## Combined main validation

After merging main through `3d2d6d3b6c743e786d2feb14ff9f272d8aa68340`, the
combined source `3daf5794f1db9a508fbab7a204936987951d8111` passed the same ten
Java and four provider tests and [112 protocol checks](combined-protocol-results.json).
All [eight native Oracle transaction controls](combined-transaction-results.json)
matched as well. The combined backend SHA-256 is
`31340f36775b42d5f45ab5b0c03cd1be0fb7f06a48efd21bd139c69cee5c1b9e`.
Its guarded build maintained at least 11.8 GiB available host RAM. Licensing and
open-core boundary checks passed. This combined binary was frozen separately;
the previously accepted running application was not replaced. The original
application 19/20 result above belongs to the earlier backend identity, not an
application rerun on this combined build. CI results are separate from these
local validations.
