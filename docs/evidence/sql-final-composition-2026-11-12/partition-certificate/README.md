# Atomic session schema and partition-certificate publication

The first combined positive at e2a49d96 failed before shipping any replica
frames: the session-owned schema path inserted its descriptor directly, leaving
the partition certificate stale. The unchanged original promoted undecided
rename test separately failed to resolve the donor backing. Both failures and
original assertions are retained.

At d7b6bfd3 the session retains its existing catalog-record mutation access,
then publishes schema and the affected completion certificate through the
existing atomic Native helper. Existing undo and creation-default behavior
remain in place. The actual partition replica/read/promotion test, complete
partition execution and child-schema targets, and rollback-index target all
passed. Original undecided rename promotion still fails with missing, replaced,
or ambiguous physical backing; this full batch is therefore failed, not green.

The new positive preserves exact Native acknowledged row hashes, reads both
parent and physical leaves on the live standby, reopens twice, invokes actual
local persisted promotion, reopens as primary and writes another routed row.
It runs both storage modes. This is a local Native control, not a network
election, physical HA, automatic capacity or authenticated portable-origin proof.

The explicit per-target summary distinguishes passing component evidence from
the still-failing mandatory original gate. Final combined broad/application/
fault qualification and the full original goal remain outstanding. No source
merge, CLI, release, deployment, new cloud spend or experimental promotion is
claimed.
