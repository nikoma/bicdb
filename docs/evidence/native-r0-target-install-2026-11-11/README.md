# Native empty-genesis target installer: focused software evidence

This is a source-level component result, not cluster qualification. Exact
candidate `dd45a8c3a29f1c3466c1824955f19baa4361371f` passed seven focused Rust
unit tests after a forced compilation from its own worktree. The tested package
metadata remains `1.0.466-beta` on an isolated, unpublished development branch.
No operator binary, release, deployment, physical failure campaign, throughput
result, RPO or RTO was produced by these checks.

The three storage tests exercise actual range-log state transitions, durable
refusal across reopen, and a real temporary-file sync/rename followed by an
injected directory-sync error. Persistence failure returns no success, retains
the conservative memory fence, and requires a new successful fsync on retry.
These tests use synthetic refusal DTOs strictly to test storage/serialization;
they do not manufacture image, metadata, quorum, or readiness authorities.

The four native tests use actual fsync-enabled `server_paged` databases, native
birth/genesis receipts, an actual one-voter data handoff promise, three durable
metadata stores, fresh metadata barriers/probes, and registered test participant
identities. The transport is an in-process protocol fixture. It does **not**
exercise sockets, mTLS, separate processes, VMs, hardware power loss or production.
The source is a verified **empty** image at consensus range index R=0. There is
no successful nonempty source-row copy result here.

They establish the following bounded component observations:

- An unacknowledged stale physical learner row is removed. The target keeps its
  own durable namespace UUID, which differs from the source UUID and survives
  reopen. The completed receipt does not release the catalog refusal or grant
  membership/read readiness.
- An unfinished attempt remains unavailable after actual database reopen. An
  authenticated replacement attempt under the same committed session restarts
  bounded clearing/copying and rejects the previous attempt's late chunk.
- After a native receipt commits but catalog activation has not occurred,
  orderly source and target close/reopen plus fresh role acquisition can
  revalidate the complete target and reissue only the same unfinished attempt.
  The resulting native receipt is freshly persisted. This is a lifecycle cut
  test, **not** an abrupt process crash or power-loss test.
- An outstanding owned controlled-writer lease prevents actual source capture;
  after that lease drains, a new actual capture succeeds. The fixture does not
  claim that a real concurrent WAL callback was fault-injected.

## Open gates

Nonzero R is explicitly refused before destructive replacement until the typed
common logical-history compatibility adapter is integrated. Numeric watermark
parity is insufficient to rule out an equal-index fork. The current-voter
repair cut has private fields and **no production constructor**; its enum and
purpose label are not implementation evidence. Post-relocation current-voter
repair, history-spine transfer, installed-anchor source use, and actual catalog/
configuration activation require their separate composed proofs and tests.
Unknown physical namespace aliases/rename-recovery contracts are refused rather
than silently normalized. General SQL, FTS, index, queue and audit failover are
not qualified by an empty-image test.

The catalog remains format 5 after image refusal is installed; earlier handoff
writers preserve the greater format instead of downgrading to 4. The feature
continues to require the explicit local namespace and image reader fences with
fsync enabled. This evidence does not independently repeat an old-binary
compatibility campaign for this candidate.

## Reproduction and retained failures

`validation.json` identifies the exact source, command, compiler, resource
monitor and test binary identity. The monitor touched only the candidate's
`lib.rs` timestamp (bytes unchanged), confirmed Cargo's actual compiling path,
and enforced one build job, no incremental compilation, 12 GiB pause/14 GiB
resume/8 GiB hard RAM limits and an 8 GiB disk floor. Tests ran with two threads.
The successful run used 74.01 seconds including compilation and 9.86 seconds of
unit execution; these durations are not database latency or recovery results.

The first candidate had compile errors (missing module imports and private
sibling field visibility). The next compiled but all four native fixtures failed
before entering the installer because newly provisioned namespace state had not
been reopened as paged state. The final fixture performs the same real
provision/reopen lifecycle as the pre-existing actual source-capture test;
assertions were preserved and no paged-state flag was forged. Both failed runs
and all resource receipts are retained separately. They must not be relabeled
as passes.

Portable source snapshots are the relevant exact files exported from the
successful commit, with hashes in `source-inputs.json`; the remaining repository
and pinned dependencies are still needed to build. Temporary fixture databases
were removed by the Rust test harness. The test executable SHA is an identity
observation, not a published artifact or a guarantee that the mutable shared
Cargo cache retains that executable. `manifest.json` covers all exported files.

Run `python3 scripts/verify-native-r0-target-evidence.py` for read-only source,
manifest, retained-failure and qualification-scope replay. Four Python regression
cases also reject rehashed scope upgrades, erased failures and cached-executable
output presented without the actual candidate compilation line.
