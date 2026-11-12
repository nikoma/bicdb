# Local nonempty RF3 current-voter image repair — component evidence

At exact private source `33ffcf88a470a7f267838561d1b8f04d9b1f5820`
(`1.0.471-beta`), 10 distinct focused tests passed. The new nonempty repair test
uses actual fsynced data services, native databases, metadata consensus/probes,
recovery fences, source-history verification and image/catalogue receipts.
This is local component evidence, not physical HA or production qualification.
No Azure resources were allocated for these tests.

| Final-source scope | Passing tests |
| --- | ---: |
| Nonempty RF3 Normal CurrentVoterRepair choreography | 1 |
| Owned Handoff learner Runtime leases | 3 |
| Native historical recovery adapters | 2 |
| Loopback mTLS historical participant RPCs | 3 |
| Genuine two-range RF3 origin/genesis | 1 |

The repair fixture declares its initial two-range RF3 topology before birth and
native genesis. Four real acknowledged commits in the other range precede the
selected range's real acknowledged commit. Every replication acknowledgment
comes from an actual service; no supplied voter sets or fabricated counters are
used. Bare follower Apply is intentionally refused by the current contract, so
the target has the genuine retained quorum-certified command at index 1 while
its native selected-range image remains at index 0. This is behind-voter repair,
not a simulated online membership change.

Fresh metadata authority comes from metadata leader `n2`, distinct from data
leader `n0`. Three actual receivers persist epoch fences; distinct observations
from `n0` and `n1` form the current data quorum. The actual source history is
compared to all obligations in that quorum before producing the opaque repair
cut. Source and target operations use their existing service-owned catalogues;
no second live catalogue handle or broad store getter is introduced.

The target's 17 other-range sentinel imports are deliberately **unacknowledged
local diagnostic imports**, using the existing native importer with its actual
other-range mutation permit. They are not RF3 user commits or an authorization
proof. An ordinary target transaction is explicitly checked to remain refused.
The selected target's native index-0 history and receipt remain exactly
unchanged by those imports. These sentinels give independent MVCC sequence
values without assigning counters:

| Actual observation | Value |
| --- | ---: |
| Selected source range index | 1 |
| Source global DB sequence | 5 |
| Target global DB sequence before install | 20 |
| Installed target-local visibility bound | 21 |
| Cold reopened target global DB sequence | 21 |

The complete path consumes an actual Runtime-owned target freeze, persists the
install fence/dirty state, stages and audits complete history 0..1, replaces only
the selected range, copies and verifies actual rows/schema/local identities,
publishes the native page-WAL receipt, adopts its catalogue floor, and returns
only recovery control through the sealed finalization. Ordinary target MVCC
reads match the exact source record; all 17 other-range sentinels and the
original target namespace UUID survive. Every old target DB owner is verified
to be gone before reopening the physical files. Cold reads, history commitment
and the irreversible range floor still match.

The image intent and Active recovery refusal deliberately remain. Both generic
recovery inspection and the actual service's NativeReady producer refuse the
adopted image. The next gate is a typed image-finalization-to-Prepared transition;
this result does not claim that transition, cold repair resume, a subsequent
acknowledged write, serving authority, StrongGet, configuration activation or
image transfer over the wire. The selected record uses the fixture's ordinary
empty-metadata record; secondary indexes, FTS, queues, audits and arbitrary SQL
are not qualified here. R1025 was not rerun or established for this RF3 repair.
The earlier RF1 R1025 result remains separately source-scoped.

The RF3 repair case uses registered in-process trusted identities. The three
separate mTLS tests exercise actual loopback encrypted RPCs with disposable test
certificates; they do not make the repair case itself an authenticated network
experiment. No process crash, VM fault, physical host power loss or scale-out
capacity campaign occurred. RPO, RTO, latency percentiles and aggregate capacity
remain unqualified. No operator binary, release or deployment was produced.

Retained unsuccessful attempts are included: `061c2668` failed compilation due
to a missing existing scope argument; `1d5859a2` compiled but its fixture wrongly
expected an ordinary target transaction to succeed. The correct product guard
refused that transaction. The corrected fixture preserves the refusal and uses
the explicitly labeled local diagnostic importer. `36be6780` passed the initial
repair case; the final source adds lineage-isolation assertions and actual
measurement output. Only the final `33ffcf88` runs count toward the 10 above.

Raw logs, resource samples, exact commands and source snapshots are retained
with their hashes. The final initial compile took 73.05 seconds including a
5.29-second test; minimum available RAM was about 20.57 GiB, with no monitor
pause or interruption. Test runtimes are not database RTO or performance data.
The test databases were temporary and removed after assertions; the evidence
verifier checks retained bytes/results, not a second database reconciliation.
No CI run is claimed for this private candidate.

From the repository root:

```sh
python3 scripts/verify-native-rf3-current-repair-evidence.py
python3 -m unittest discover -s scripts/tests -p 'test_native_rf3_current_repair_evidence.py'
```

To repeat the new actual component test at the matching source revision:

```sh
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 RUST_TEST_THREADS=2 \
  cargo test --release --locked -p bicdb-core \
  --config profile.release.package.bicdb-core.opt-level=0 \
  --config profile.release.package.bicdb-core.debug=0 --lib \
  actual_rf3_nonempty_current_voter_repair_uses_same_catalogue_and_keeps_recovery_closed \
  -- --nocapture
```

The retained monitor records active memory, swap, pressure and disk headroom;
it pauses or terminates only its own process group. Reproduction uses fresh
temporary databases and does not read any retained Azure experiment database.
