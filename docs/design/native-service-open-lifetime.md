# Native service actual-open lifetime fence

Status: implementation and regression source; execution on the combined candidate
is pending. This document supplies no physical or production qualification.

A service's cached storage mode and retained directory lock cannot establish
that its original `BicDb` is still open. The outer `Arc<RwLock<BicDb>>` can contain
a different database after replacement, while another owner keeps the original
directory lock alive. Treating either handle as the database lifetime would
leave an old listener's cached mode usable after that open ended.

`ClusterServiceOpenGuard` captures a weak token owned only by the actual database,
its original durability-health handle, canonical root and incarnation. The guard
cannot keep the database open. Expiry or failed durability health refuses the
request. A borrowed-database check additionally verifies the actual root and
incarnation; this guard is a necessary refusal check, never quorum authority,
catch-up evidence or permission to serve a strong read.

The listener captures the guard with its immutable identity, and checks it after
authentication before service or database latches. Storage-policy checks use the
same fence. Service-owned database reads and writes validate the borrowed
database identity. Shared checked-borrow helpers cover schema inspection,
schema mutations and ordinary snapshot export; image-owner rotation performs
the same check on its explicit nonwaiting borrow. The
legacy application-event and schema-bundle dispatches also validate the actual
borrowed database after obtaining its read guard, without holding the service
mutex. This prevents a check of one actual database from authorizing an event
against a replacement in the same outer handle.

Regression sources cover an actual from-birth enforced database replaced by
a genuine legacy database while retaining the original directory lock, and a
live-open check while both database and service latches are held. An actual authenticated TLS listener regression additionally
checks post-drop refusal while both latches remain held, and verifies the
replacement database canary and commit sequence remain unchanged. All three
controls await execution on the integrated candidate. They do not
establish that an acknowledged-write exploit existed before this change, nor
prove every RPC's replacement race or end-to-end failover semantics. Further acceptance must exercise replacement during in-flight ordinary RPCs,
including both original-open retention and original-open expiry. The actual
borrowed database must be checked before any use; the before-latch lifetime
preflight alone is not an atomic pin or a universal RPC replacement-race proof.
