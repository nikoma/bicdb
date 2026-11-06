# Large native HA application packages

The actual four-package EHR rehearsal exposed a replication failure that small
counter packages did not: the provider package became locally durable but no
standby confirmed it before timeout. The prior frozen `d69b5da` supervisor also
fails the new 20 MiB incompressible-package regression with that same error.

Immutable release JSON now uses bounded 384 KiB pieces, each base64-encoded in
a private chunk table and individually confirmed by the configured native
quorum. The immutable size/count/hash header is published only after all pieces
are durable. Desired deployment state still follows package publication. Small
legacy JSON packages remain readable. Retries resume matching unpublished pieces;
conflicting, incomplete, oversized or corrupt pieces cannot become executable.
No clinical table, identity or retention policy changes.

At implementation commit `65901d1`, the complete app-host package passes 63 tests
with four opt-in tests ignored. This includes the 20 MiB package surviving two
successive host failovers without another upload, exact authenticated SQL/RLS,
COPY and transaction persistence, physical process fencing, metadata maintenance
stop/start, legacy package reads, partial-publication resume and corruption
refusals. The unrelated private-UID code and core HA code retain their earlier
separate checks; KVM is not used by this deployment.

Both new runtime regressions were executed against the original frozen runtime:
the package test fails on missing synchronous confirmation, and the quorum-error
response test fails because the old runtime returns HTTP 400. The fixed runtime
passes both. Unconfirmed locally durable commits now return HTTP 503, with no
automatic mutation replay. The operator upload test uses the same 120-second
request budget as the real deployment client; replication/fencing timeouts were
not relaxed.

The latest upstream storage-mode copy/verification and Oracle LIKE updates have
also been merged. The 63-test result identifies the tested implementation above;
qualification of the new unified CLI/supervisor and the actual EHR packages on
that merged source is separate and remains open. All state hosts must upgrade
before publishing chunked packages: old supervisors cannot interpret the new
header. This is source/runtime regression evidence, not production admission.
