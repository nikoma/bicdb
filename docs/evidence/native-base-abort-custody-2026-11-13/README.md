# Registered native-base recovery: two failed qualification cohorts

Both whole cohorts failed. This packet preserves source-pinned same-open
component evidence; it does not qualify HA, Serving or horizontal scaling.
The qualification branches retain source version **1.0.494-beta**. They are
separate from newer `main`, source merges, binaries and deployments.

| Exact source | Hosted run | Core result | Workspace/all-targets and consumer |
| --- | --- | --- | --- |
| `c1e4b3a4e8e7ad33c688436830f9af10a4203cc5` | [43510565054](https://github.com/nikoma/bicdb/actions/runs/43510565054) | **Failed: 5 passed / 5 failed**, 88.92 s | Passed |
| `81733c1fa601fea3bcac1ad5efac8435dd02b090` | [43514812736](https://github.com/nikoma/bicdb/actions/runs/43514812736) | **Failed: 8 passed / 2 failed**, 90.09 s | Passed |

Each selected and executed exactly ten tests under
`native_registered_fifth_remote_native_base_actual_`, with four Rust test
threads, one Cargo build job and incremental compilation disabled. The actual
metadata authority window remained about 1.5 s; the parent stayed 5 s and the
request order stayed Role → OWN → authenticated Finished → actual host.
No deadline extension, hidden retry or infrastructure experiment produced
these results. `817` changes test observers and diagnostics; the `c1e` native
publication repair is unchanged.

Each run contains **20 returned application event operations**, two per
fixture. The ledger records `ClusterEventClient.append`, payload hashes, full
records and logical operation identities. These are distinct from internal
native page transactions such as xid 20 (final image) and xid 21 (base
publication). Neither internal xid counts nor copied record counts establish
business throughput. The fixture stores these events in `__bic_cluster_events`;
it does not prove ordinary PostgreSQL table transactions or strong Serving
reads.

## Findings and issue boundaries

| Finding | `c1e` | `817` | Bounded conclusion |
| --- | --- | --- | --- |
| Healthy actual Role expiry before the first put | Passed | Passed | Original Aborted xid 21 retained; fresh same-A request committed xid 22. |
| Healthy expiry after one successful native metadata put | Passed | Passed | Original Aborted xid 21 retained; fresh same-A request committed xid 23 after structural closure. |
| Actual expiry after commit | Passed | Passed | Fresh same-A authority reconciled the same committed xid 21. |
| Finite attempt-history exhaustion | Passed | Passed | Nine real aborted requests; eight retained old owners plus the current owner; no tenth allocation. The 8 MiB byte exhaustion was not tested. |
| Missing actual final image | Passed | Passed | Refused without changing the retained image owner. |
| Failed native sync during abort closure | **Failed fixture assertion** | Passed | Actual sync fault fired once, xid 21 was Aborted but health remained poisoned; fresh authority could not replace the owner. |
| Unknown native commit after failed sync | **Failed fixture assertion** | Passed | Actual `CommitOutcomeUnknown`, same-open xid 21 InProgress, sticky health refusal and no replacement. No commit/rollback assumption. |
| Native-base cancellation/reconciliation | **Failed prerequisite** | Passed | `817` reached actual cancellation after commit and reconciled the same xid. This does not relabel `c1e`. |
| Normal native-base publication | **Failed timeout** | **Failed timeout** | `817` retained committed xid 21 when real Role expiry was observed before acceptance. |
| Image invalidation after base publication | **Failed prerequisite** | **Failed prerequisite** | `817` committed the base but timed out before the later invalidation assertions; that named guarantee was not tested in this run. |

The `c1e` sync controls stopped at the assertion that the generic ownership
observer's diagnostic error was null (`native_abort_tests.rs:284`). That
observer calls `lease.require_retained()` and correctly returned `WriteFailed`
on the intentionally fenced database. Later assertions were not reached.
Their retained observations separately show sticky health and an unchanged
publisher (one attempt, no retirement); they remain failed tests.

`0501804fec79664c9a4d9f0989b123b5327a35dc`, followed by
`81733c1fa601fea3bcac1ad5efac8435dd02b090`, corrected this test contract. The
generic observer still refuses. A separate `cfg(test)` memory-only observer
compares the actual host map entry, retained lease, `as_frozen().owner()`, exact
preflight Arc and original source/Store/MStore/open/birth IDs before, after the
fault and after a fresh request. It performs no health check, native read or
admission grant. Both controls then passed their original I/O/status and
no-replacement assertions. Payload visibility is not asserted through a
fenced handle.

The three old `c1e` failures occurred before their named acceptance checks:
normal/invalidation cases timed out at `PublishNativeBase`
(`native_base_tests.rs:113`), and cancellation timed out at `PublishFinalImage`
(`target_final_image_tests.rs:397`). Their exact native outcomes and failing
request checks were not captured then and remain inconclusive.

For the two `817` failures, the new report establishes actual commit-return,
not just bytes found in an archived WAL:

| Case | Host elapsed | Role before → after | OWN remaining after | Source remaining after | Parent remaining after |
| --- | --- | --- | --- | --- | --- |
| Normal base | 1,248,812 µs | 1,246,313 → −2,605 µs | 27,070 µs | 2,622,501 µs | 3,501,285 µs |
| Invalidation prerequisite | 1,294,785 µs | 1,282,177 → −12,713 µs | 24,945 µs | 2,597,563 µs | 3,491,175 µs |

Both post-error observations show actual native TxStatus Committed, exact
current base = immutable archive = retained receipt, commit-returned xid 21,
verified false, one native attempt, no retirement, unchanged actual A and no
diagnostic read errors. The Role was live before host invocation and expired
after it; OWN, source and parent remained live. This demonstrates a late
committed publication and contemporaneous Role expiry in these attempts.
The exact internal check returning the timeout is not instrumented. It does
**not** retroactively establish the cause of `c1e` or the earlier `e1ec` cohort.
Later floor/invalidation assertions were not tested in these failed cases.

Relevant runtime paths at the pinned revisions are:

- `crates/bicdb-core/src/db/range_image_install/registered_remote.rs`:
  `RegisteredRemoteBasePublication`, exact attempt retention and healthy
  same-open proven-abort classification.
- `crates/bicdb-core/src/db/range_image_native/registered_remote_image.rs` and
  `registered_remote_image/final_publication.rs`: original current-context
  constraints, actual final receipt and immutable archive readback.
- `crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/registered_replica/remote_attempt.rs`:
  health-gated observer and the distinct test-only memory custody observation.
- The corresponding `tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/remote_source_tests/target_final_image_tests/`
  subtree: `native_abort_tests.rs`, `native_expiry_tests.rs`,
  `native_base_tests.rs`, `catalogue_tests.rs` and `critical_diagnostics.rs`.

## Reproduction and the next smallest tests

On the exact sources and existing hosted test environment, retain the original
filter and configuration above. The portable commands below verify captured
results; they do not rerun Rust, issue network requests or open a database.

The smallest next continuation test starts from an **actual unforced late
commit** observed by the ordinary publication request. Retain its original
failed result, exact native owner/bytes/xid and live DB/A. After proving the
same-open committed current/archive mapping and unchanged image constraints,
issue exactly one genuinely fresh Role → OWN → Finished request for the same
A. Require reconciliation of the same xid, zero retired owners, no new logical
row transaction and unchanged application ledger. Keep the original timeout
visible even if recovery succeeds. If natural expiry is not reached, report
that prerequisite as not tested; do not manufacture a success by extending
its deadline. The separate deterministic after-commit expiry control already
passes, but does not substitute for this ordinary-path continuation.

In parallel, profile the existing publication's actual admission/current
checks, native receipt construction, first/second puts, WAL commit/sync,
CURRENT/archive readback and post-commit checks. Include source Finished-fetch
cost and lock wait separately. Record timings at real boundaries while keeping
owner custody before irreversible operations and all final checks. The current
reports locate a narrow Role margin; they do not identify which CPU, disk,
serialization, lock or repeated-validation cost consumes it. Measure before
changing persistence or authority lifetime.

## Evidence custody, hashes and verification

`runs.json` pins the complete source SHA, run/job/artifact IDs, whole result,
original raw archive hash/size and safe projection hash/size. Each run directory
contains the safe ZIP, its per-member inclusion/exclusion manifest, exact
whitelist inspection, source-pinned projector, captured reconciliation,
GitHub metadata and compressed Core job log. `SHA256SUMS.json` pins every
packet file except itself.

| Run | Private raw archive SHA-256 | Safe logical ZIP SHA-256 |
| --- | --- | --- |
| `c1e` | `587683f6ec471634e1f59baaab8c554504480b745d19b5af2e85e7660ef3ea1c` | `139d46a83da37bddb891106115f967207ce1d6b33c84f7059857d0ad14dda9bb` |
| `817` | `1e4e0f96ba36a3f075ad316a82cdf4d690aec1e7c789379e3896e9924c95dc87` | `2c8d349f1090c1f286bc9294b0e20884c18f21cf00ff8ddca37c1605c5c5583d` |

Run from any directory with Python 3's standard library:

```sh
python3 -B /path/to/this/packet/verify.py
```

The default verifier checks packet/member hashes, source and selected/executed
test identities, the exact failed result counts, application payload hashes,
actual source-frame filtering, OLD anchors/unchanged native records, healthy
abort retention, fault/status/custody observations and both `817` committed
Role-expiry reports. It cannot verify excluded raw bytes without the originals.
With separately authorized private custody files, additionally run:

```sh
python3 -B /path/to/this/packet/verify.py \
  --raw-c1e /private/c1e-native-base-ten-artifact14078871155.zip \
  --raw-817 /private/817-native-base-ten-artifact14080168310.zip
```

That mode verifies complete raw archive hashes and every included/omitted
member, then independently checks the 962 custody files per run: 35,509,198
bytes for `c1e`, 35,558,550 bytes for `817`. It only reads archive members;
it never opens an engine, runs recovery or writes to the archives.

Full raw ZIPs remain private with restricted permissions and are **not included**
in this leaf. Entire `/custody/` trees and custody manifests are omitted from
the public projections, including fixture database roots and mesh signing keys.
Custody was a **guarded live-owner stable file copy, followed by cleanup**:
all five service/DB/native/journal guard sets held, owners alive, listeners not
drained, engine not reopened. It was not an atomic stopped snapshot, backup,
cold recovery or physical power-loss test. Both captures reconciled cleanup
and evidence errors to empty; failing primary results remain retained.

No physical five-node expansion, sustained throughput gain, RPO/RTO,
p50/p95/p99 benchmark, general SQL, cold recovery, catalogue adoption, NEW,
decision/Open, Ready/Serving or source retirement is qualified here. The
source/OLD cap-eight lifetime gates and complete 64-range four-to-five-node
convergence remain open. No production gate changes follow from this packet.
