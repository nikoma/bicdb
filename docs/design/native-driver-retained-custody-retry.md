# Native driver: retain original custody through fallible transitions

Source batch based on `1742776ab02957a053aded2ad4f665500d9325bf`
(1.0.512-beta). This is a same-process correction to the product handoff driver.
No cold recovery, new operator caller, remote owner proxy, production
qualification or successful test execution is claimed.

The old driver moved its finalized image out before target NEW validation,
round collection and decision preparation. A refusal dropped the only local
finalization handle, although the recovery lease's Drop kept the original
A-owned freeze in its runtime. That drop is safe exclusion, not recovery of the
completed finalization. A later drive could try the dropped-*partial*-image
restart path, which is not a constructor for an already completed image.

It also consumed its durable DECISION into local serving leases before fresh
OWN acquisition, native preparation and quorum aggregation. A refusal dropped
those local owners while metadata remained AwaitingActivation. A later call
skipped the earlier stages but had no retained durable decision to prepare.
Neither behavior establishes loss of acknowledged rows; both can strand the
selected range frozen.

## Ownership transitions

- The genuine finalized image and installer lease are wrapped once in `Arc`.
  A retained target NEW constructor performs every original Role, source OLD,
  native base, source seal, schema, restore, service and frozen-owner check.
  It receives only the same opaque pair, never a decoded receipt or generic
  Frozen. The consuming constructor delegates to that checked path.
- The driver retains the original pair until its publication round is stored.
  A failed pre-publication view drops its own read pin; it cannot erase the
  retained finalized image or create another A. Extra driver references drain
  immediately after successful transfer into the publication round.
- A durable decision can lend validated serving leases sharing the original
  owners. The decision stays on the driver through every fallible preparation
  operation. The complete prepared quorum is retained **before** the decision
  is cleared. Those extra references drain before eventual unique-owner release.
  Consuming APIs remain available by delegating and then dropping their input.
- Publication `try_finish` returns the exact original round and durable receipt
  map on refusal. The driver restores that round and its original completed
  member set; it does not replace a native transaction or decision body.
- The image release owner contains existing opaque `Arc` fields. A continuation
  builder receives a clone while the driver retains its original. Only after
  the complete continuation is stored is the original dropped. Existing unique
  `Arc` checks still prevent release while any extra owner exists.

The original Session remains the lifetime token. Fresh OWN and local metadata
points keep their actual request and native publication checks. Native
preparation still receives the original Session: canceling the current drive
request does not imply that no native fact was written. Every retry must read
and validate the original CURRENT/archive and native physical point. The native
preparation producer already reuses its existing same-fact record under fresh
nonregressing metadata. This patch changes no native commit, fsync, uncertain
outcome or replacement-XID rule.

## Three genuine fixture cuts, not yet executed

The existing five-host fixture uses real TCP/mTLS and native stores in one
process. Three new cases cancel the **actual current caller** at:

1. After actual target NEW exists, before its round/decision preparation.
2. After genuine serving leases are borrowed, before the first actual OWN call.
3. After one actual native preparation returns, before its aggregation.

The original Session is not canceled or replaced. The actual API result is
saved before assertions. The tests require cut reach, original session lifetime,
retained owner identities and full acknowledged payloads. They then use a fresh
caller with the **same driver**, requiring no new source cut, target A, native
base or DECISION receipt/XID. At cut 3, actual protected preparation CURRENT and
archive are read as DATA and must retain full-record equality and the same
native page XID through retry. These
readouts grant no permission and do not independently establish storage fsync.
The tests invoke the existing complete acceptance: full Strong reads, new ACKs,
and another ACK/Strong read after one original voter stops. Existing test bodies,
application assertions, request budgets, retry limits and stack configuration
remain unchanged. The two additional aggregation/continuation refusal paths are
source-reviewed here; no separate fault-control execution is claimed for them.

All checks so far are source/static only. Original 174 and earlier failing
cohorts remain at their exact revisions. Formatting is not Rust compilation or
a passing acceptance test. Independent source review and the repository's
required validation are still necessary for the coherent final head.

## Restart and operator boundary remains separate

A crash-safe operator protocol needs a protected durable intent before each
consuming/native side effect. It must bind the cluster/bootstrap, participant
birth and MStore/Store identities, range/old-new configurations, relocation and
session/nonce, exact source cut/history, target A/base, decision digest and phase.
Each attempted native XID and exact body must survive uncertainty, with immutable
archive plus CURRENT and commit/readback evidence; absence is not proof of abort.

After restart, intent bytes are historical DATA. A real cold-recovery producer
must obtain current native/database ownership and fresh Role/OWN, independently
validate protected history/decisions/activations and the required peer quorum,
and reestablish exclusion for the original attempt. It cannot deserialize an
Arc, revive an expired token, infer a freeze from a UUID, or start a replacement
handoff to hide an unknown outcome. Single-owner range coordination and bounded
retention/cleanup must follow actual committed release evidence.

Those producers and end-to-end crash controls are not added here. This patch
removes specific same-process ownership losses while preserving fail-closed
behavior on unknown durability, changed identity or unavailable evidence.

Draft PR #189, reviewed at `ea9e90a54fd57ac28e62d1dd2574fc3b2ceb942d`, changes
bounded collection-worker teardown and context drainage. It changes none of
these driver ownership transitions or persistent restart requirements. Its two
shared worker tests are not handoff restart qualification.
