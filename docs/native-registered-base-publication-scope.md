# Registered remote image: native-base publication scope

This document describes an unmerged source candidate, not a release,
deployment, physical expansion result or production qualification.

The preceding actual remote-history/row controls passed on source
`10d17149886e629c535b6b5523a89f87ae83ba81` in run `43484879958`
(five focused controls). The final-image controls on that source passed in
run `43484887734` (five focused controls). Those results do not test the
native-base changes described below. Original failed runs remain retained.

## Actual producer and durable profile

`crates/bicdb-core/src/db/range_image_install/registered_remote.rs` consumes
the original host's opaque final-image receipt, after its independent bounded
target-primary scan, under the same retained attempt, receiver hook,
admission/journal exclusion, native birth, DB open, Store and catalogue.
The caller supplies a fresh actual Role/OWN observation and an authenticated
Finished source frame. A decoded final pointer cannot construct the publisher.

The original host retains the exact proposed native transaction ID and bytes
before either put. One native transaction writes `install:base:<range>` and
the identical immutable `registered-remote-base/<attempt>/archive/<xid>`.
Only exact readback plus committed native transaction evidence can complete
publication. A canceled reply retains the same attempt and transaction;
retry never starts another publication transaction.

The common installed receipt has a distinct format-2 Registered profile with
a mandatory final-image pointer. Format 1 continues to require no Registered
pointer; its serialized bytes remain unchanged. The lower-bound reader checks
the actual native base/archive transaction and the immutable final-image
publication, including archived history and completed-row headers. It does
not invent the local source's staged-history proof. Builds without the actual
Registered validator refuse this profile.

Before publication, mutation admission still requires the unchanged original
NoDomain preimage. After this publisher's own transaction, its private retained
boundary accepts only that exact introduced base and forbids an unexpected
genesis, prefix or pending command. Other image operations stop once native
base publication has begun. This is not a general exception to the original
irreversible-history check.

## What this does not establish

An installed native base is a durable lower bound, independently of whether
the current image can serve reads. Image invalidation must make the current
image unavailable while preserving the historical range floor.

Publication leaves the external catalogue CLOSED/Installing. It does not
adopt the catalogue's range floor, activate NEW membership, commit a global
handoff decision, install a writer policy, or provide strong-read readiness.
It does not reconstruct owners after restart. These are separate required
steps before automatic relocation can finish. Independent index/FTS/vector
parity, second populated-range movement, source retirement and bounded
controller convergence also remain unqualified.

Focused controls exercise real publication, a missing-final-receipt refusal,
post-WAL cancellation with same-transaction retry, and an actual image
generation change retaining the lower bound. Their presence in source is not
proof they have passed. Record their exact source revision and run result
before claiming tested behavior.
