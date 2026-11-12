# Local native recovery readiness: component evidence

Status: 32 distinct local component tests passed at exact private composition
`10a10ba624a5f4d7ef0adab32c655ff2dac8e88b`. This is not production qualification,
a published release, or a tested new CLI binary. Historical package metadata is
1.0.466-beta; final integration must use the next unused current-main version.

Eight new tests exercise the actual local producer. Twenty-four affected controls
exercise the same compiled source: five receiver fence-quorum tests, nine owned
mutation-gate tests, seven native prefix/origin tests, and three history-stager tests.
The initial attempt at `0e8b1e2bb955fe694e354e200502073022bfc8f1` failed compilation
because a test called a private command constructor. The correction provides a
cfg(test)-only facade; the runtime constructor remains private. Failed output is
retained separately and does not count as an executed or passing test.

The new `LocalNativeRecoveryReady` is an opaque owned LOCAL physical proof. It
checks actual native factory completion, database-open identity, physical store
path, schema/namespace/reader/restore and voter credentials, actual native R/H,
and an actual fully resolved catalogue at exactly the same R. Every retained
Applied/Aborted decision must match the corresponding actual native history entry.
Its snapshot bound is the actual local durable database sequence; neither range R,
a source-node sequence nor a page-WAL XID is substituted for it. The owned source
read pin prevents every controlled/native writer for the proof lifetime.

New scenarios:

- Actual empty genesis, owned read pin, and controlled/ordinary writer exclusion.
- Actual RF1 origin writes on two ranges, R=2 with a higher local MVCC sequence,
  and successful ordinary record snapshot reads at that actual local sequence.
- A durable Applied label with no corresponding native materialization is refused.
- Equal R with a different actual command history is refused. The negative fixture
  explicitly rebuilds the logical catalogue through real prepare/commit/certify/
  mark-applied transitions; it does not establish that this occurred in Azure.
- Native history ahead of a rebuilt catalogue requires real adoption/recovery;
  no numeric floor is installed by this producer.
- Original parent cancellation invalidates an already issued local proof.
- A catalogue with the same node/cluster labels at another actual database path
  is refused.
- Actual close/reopen invalidates a proof from the previous open.

`passed` completed in 70.019 seconds including forced correct-worktree compilation;
minimum available RAM was 22,340,329,472 bytes. Controls reused only that unchanged
compiled source. There were no memory pauses or interruptions. All tests use local
fsynced server_paged databases; metadata identity transport is a simulated registered
fixture. This is not a network, independent-machine or power-loss test.

Local physical readiness is NOT leadership/serving-configuration readiness. The
physical anchor can survive an epoch/leader change only with the same physical
voters, span, schema and restore domain. This producer neither rebinds the active
serving configuration nor certifies a recovered cluster-wide history. Membership
activation still requires independently proved old/new quorum safety. Strong reads
still require current authority, a suitable read barrier, and final readiness checks.

Not tested/closed here: nonempty transferred-image publication and installed-anchor
reopen, ordinary RF3 replica Apply, current-session historical recovery and abort
resolution, post-WAL/pre-prefix unknown-outcome recovery, new-configuration activation,
StrongGet, SQL transparency, fault-safe rebalance convergence or physical throughput.
The historical Azure failure remains unchanged and is not causally explained by
these synthetic local failure fixtures.

Resource samples include available RAM, swap, PSI and disk headroom. Core test
optimization/debug are zero with release dependencies; these timings are not
production latency or throughput. The retained catalogue scan is capped at 1024
entries per range, but the existing catalogue checksum still covers global
catalogue state and has not been qualified as a constant-cost serving operation.
