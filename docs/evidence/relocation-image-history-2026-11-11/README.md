# Completed range relocation can leave a compacted-history recovery gap

**FAILED availability invariant on unmodified1.0.463-beta**,
`d49a391b3c6157f90f29e9f2f5214143c60f9f4b`: a supported relocation completed with
the correct record on its new leader, but that leader could not recover its
range history from a healthy current voter quorum once the old prefix had been
compacted. No data loss is demonstrated by this single-key fixture.

The [standalone fixture](../../../scripts/experiments/relocation-image-characterization.rs)
links the exact frozen optimized core
`dc714b001a0fe8758473c2c4ef80158474d7426e16630926edb65b6cc35f0356`.
It creates four local ServerPaged databases with8MiB pools, **fsync=false**, and
creates/closes/reopens the collection before service ownership so bounded paged
snapshot export is actually exercised. Its initial RF3 metadata topology is
scripted. Every transaction thereafter uses the real coordinator and three
replica services. The actual relocation driver invokes the same owned replica
services through an in-process forwarding transport: no parallel service/store
owners, hand-edited catalogs or fabricated progress counters are used.

Two cases update one fixed record8 and1,025 times. Every originating transaction
returns success. A real `n0→n3` relocation executes schema preparation, bounded
snapshot, catch-up, promotion and physical source cleanup. The final RF3 voter
set is `n1,n2,n3`; n3 is leader at routing epoch4. All its voters are reachable
through the scripted transport. The old source record is actually removed.

| Measured state | 8-command control | 1,025-command case |
| --- | --- | --- |
| Original voters' resolved range index | 8 | 1,025 |
| Original voters' compacted-through index | 0 | 1 |
| Snapshot/catch-up source DB watermark | 8 | 1,025 |
| Target range resolved index after completed relocation | 0 | 0 |
| New leader recovery | Succeeds; range index advances to8 | Refuses; remains0 |
| Target record after recovery/attempt | Exact latest value8 | Exact latest value1,025 |

The compacted case returns:
`range-write leader recovery for r1 requires a range snapshot at index 0; quorum
resolved through 1025`. Repeating the same relocation API alone has not supplied
the durable range-history/image base that recovery needs. Setting an integer
counter equal to a DB watermark would not establish that base safely.

The two characterization tests **pass because they assert these observations**.
The compacted healthy-quorum availability finding is **FAILED**. The
below-retention positive verifies the exact value after successful prefix
recovery, so perpetual refusal does not satisfy the test's healthy contract.

Controlled pauses retain both relevant phase boundaries. After one physical
copy batch, the row is present while learner `snapshot_complete=false`;
premature promotion refuses. After snapshot completion in `CatchingUp`, the
learner image is complete but target range history remains0; promotion still
refuses until the real catch-up step qualifies its metadata phase. Metadata and
driver handles are closed/reopened at both boundaries, while the **same live
data-service owners** remain. Their persisted cursor/watermark metadata resumes
and finishes correctly. This is control-plane checkpoint resume, not a database
process crash, interrupted physical I/O or power-loss test.

| Finding | Status |
| --- | --- |
| Healthy leader recovery when resolved history remains retained | PASSED |
| Completed relocation installs a recoverable range-image base | FAILED |
| Bounded recovery with compacted sources and healthy current quorum | FAILED |
| Correct current single-key data and premature promotion refusal | PASSED |
| Control-plane snapshot/catch-up checkpoint handle reopen | PASSED |
| Database crash recovery, physical HA, Azure causation | NOT TESTED |

Final helper link and both cases are bounded by active RAM/own-I/O monitoring.
The compacted case took31.11s test time; minimum available RAM26.85GB and sampled
process `write_bytes` peaked at6,493,872,128 bytes, below the16GiB guard. These
counters include all activity of that helper, have sampling gaps and are neither
isolated write amplification nor a physical throughput result. The short control
finished within one250ms resource sample, so its observed zero write-byte sample
does not mean zero writes. No guard triggered. Source and binary hashes, command
arguments and raw exact phase projections are adjacent.

An earlier fixture failed before relocation because its newly created collection
was still resident. That precondition failure remains in the original local
`resident-precondition` evidence directory; the final fixture follows the
repository's create/close/reopen paged-storage setup. A prior full observation run
is separately retained locally. The final run adds exact post-recovery reads and
real metadata/driver handle reopen observations.

The next correctness work needs a typed, authenticated, crash-consistent image
base binding a quorum-certified range cut to the installed record image,
namespace/schema/reader capabilities and source/destination incarnations. It
must survive learner cleanup and support suffix replay after compaction. Range
membership transition fencing must be proved independently of metadata quorum.
No new image API or snapshot field is implemented by this evidence commit.

Run `python3 scripts/verify-relocation-image-history-evidence.py` to check source
linkage, exported hashes and both retained observations. It reads portable
artifacts only and does not run an engine or cloud test. Local raw evidence is
preserved in `/tmp/bicdb-p0-image-characterization-evidence`; the portable export
replaces task build paths only, with distinct raw/export SHA256 values in
[export-provenance.json](export-provenance.json).
