# Main500 integration: native-base success and broader remote failure

These are two separate hosted runs of exact candidate
`b6398fb075eed7c93a71ab25a52fad3f936421e5`, source **1.0.500-beta**. The
candidate preserves the native pipeline on actual main500, then adds a
test-only diagnostic continuation. Neither run qualifies production HA or
physical scale-out. This evidence does not establish that the candidate was
merged, released, built as a release binary or deployed.

| Run | Selected filter | Whole result | Core job | Artifact |
| --- | --- | --- | --- | --- |
| [43517610071](https://github.com/nikoma/bicdb/actions/runs/43517610071) | `native_registered_fifth_remote_native_base_actual_` | **10 passed / 0 failed**, 65.53 s | `132203519345` | `14082820921` |
| [43519728051](https://github.com/nikoma/bicdb/actions/runs/43519728051) | `native_registered_fifth_remote_` | **Failed: 44 passed / 7 failed**, 223.37 s | `132210791914` | `14082737655` |

Both hosted lists confirm the stated selected/executed counts. Workspace
all-target and workflow-consumer compilation passed in both runs. The broader
request reused the completed narrow run's immutable branch for its cache; it
was a separately authorized selector, not a retry or replacement. No standalone
catalogue rerun was dispatched.

## Environment and result boundaries

Both runs used four focused-test threads, one Cargo job, incremental compilation
disabled, the original 5 s parent, actual metadata authority windows of about
1.5 s and unchanged Role → OWN → Finished → host order. Main500's workflow
passes `profile.dev.package."*".opt-level=2` for the focused build/list/execution;
the earlier `817` workflow did not. The retained workflow diff and environment
request record that difference. Thus green results are not evidence that the
earlier `c1e` or `817` failures had any particular cause. The broader selector
also changes the concurrent test mix despite retaining four threads.

The candidate includes a diagnostic that can follow an ordinary timed-out but
observably committed native-base request with one genuinely fresh same-A
request, preserving the original error and failed test. **Neither run reached
that natural late-commit prerequisite.** No
`registered-native-base-unforced-timeout-continuation.json` was produced. The
continuation is **not tested**, even though the separate deterministic
post-commit expiry control passed.

The narrow run verifies healthy before-put and post-put native-metadata abort
recovery, committed-outcome reconciliation, actual sync-failure health fencing,
retained memory custody, finite attempt-history exhaustion, missing-final
refusal, ordinary base publication, post-WAL cancellation and current-image
invalidation with historical floor integrity. It contains **20 returned
application event operations**. The broader run contains **102 returned
application event operations**, two per fixture; their payload hashes and
logical operation IDs reconcile. They are `ClusterEventClient.append` records
in `__bic_cluster_events`, not ordinary SQL table transactions. Internal native
xids such as 20, 21, 22 and 23 represent metadata publications or structural
closure, not additional application commits or throughput measurements.

## Broader failed cohort

The 51 selected cases cover catalogue (4), final image (5), image rows (5),
Installing (3), native base (10), OLD (9), proposal (5), source (2), step (4)
and transfer (4). The dedicated cold-preservation selector
`native_registered_row_cold_sigkill_preserves_pending_before_generic_open`
was **not selected**. Shared prerequisites do not count as rerunning it.

All seven failures reported the same primary error at
`remote_source_tests.rs:1198`, `round.finish().unwrap()`:

```text
Cluster("metadata consensus: metadata observation request changed or expired")
```

This is the legacy local OLD seed, whose round is acquired before several
actual node-local close/WAL operations. The source establishes the early
finite observation and the failed finish; that run did not retain exact
pre/post observation deadline diagnostics. Do not infer a concurrency cause
or a production data-loss defect from this error alone.

| Failed case | Evidence reached | Later named acceptance |
| --- | --- | --- |
| `TargetInstalling` | Local OLD seed failure | Not tested |
| `TargetInstallingDirSyncRetry` | Local OLD seed failure | Directory fault not tested |
| `TargetNoDomain` | **Partial range-one NoDomain point** and source frames, then range-two seed failure | Complete two-range state assertions did not pass |
| `TargetNoDomainMemberChanged` | Local OLD seed failure | MStore-change refusal not tested |
| `TargetProposalPostCanceled` | Local OLD seed failure | Post-WAL cancellation not tested |
| `TargetProposalBeforeCanceled` | Local OLD seed failure | Pre-WAL revocation not tested |
| `TargetProposalReaderFenceRetry` | Local OLD seed failure | Reader-flag sync fault not tested |

The `TargetNoDomain` partial record is `no-domain-1-point.json`; its presence
must not be discarded or inflated into a passing two-range case. The other six
failures have no target-control outcome report. Exact failed test names,
primary errors, available report paths and all recorded ledger error operations
remain in the archive/captured reconciliation. No native close outcome is
reconstructed from absence or an expired local round.

Native-base 10 and catalogue 4 passed within this **failed** broader cohort.
The retained component reconciliation additionally checks actual native
outcomes/retired bytes, both poisoned-health refusals, exact same-candidate
catalogue bytes through two real directory-sync errors, post-fsync cancellation,
selected range R/H0/H and unchanged other catalogue data. This is component
coverage; it does not turn 44/7 into a success. Image-row 5 and final-image 5
also passed their selected tests.

All 51 cases retained empty cleanup/evidence-error lists. The complete raw
archive reconciles **5,155 custody files / 166,256,025 bytes**; the narrow run
reconciles **1,012 files / 35,634,695 bytes**. These counts describe captured
files, not independent machines or application data volume.

## Smallest next tests and open gates

The approved test-only successor `993dd8f72ddbd0ca7ecaccc9fa1c5d64fcc16052`
selects the existing actual remote OLD collector for twelve related target
cases: four NoDomain, five proposal and three Installing. It preserves all
target fault/acceptance assertions, dedicated legacy/source controls and all
four transfer lanes. It changes no product implementation, timeout, concurrency
or authority contract and is **uncompiled/unrun at this packet's freeze**.
The original 44/7 remains failed.

The smallest next target check exercises those twelve cases with actual
per-OLD Close, fresh donor Observe/final OWN, the real opaque RemoteOLD
certificate and an authenticated source Manifest. NoDomain happy must retain
**two distinct actual ranges**, source sessions, target attempts and Manifest
anchors; a range-one success cannot conceal a range-two failure. The successor
records bounded memory-only seed-boundary entries in existing journals without
extra file I/O inside authority windows. It does not cast DATA into local
quorums, SourceLineage, a role or a fabricated target fence.

For native-base liveness, independently retain an actual unforced late commit,
then one genuinely fresh same-A reconciliation with exact old bytes/xid and no
new logical row transaction. Preserve the first timeout. Profile existing
admission/current checks, native metadata writes/sync and post-commit validation
costs before extending any deadline. Natural expiry not reached means not
tested, not a passing continuation claim.

The integrated driver still needs final-head broad/native checks, real NEW /
decision / Open / Serving and source-retirement evidence. Complete 64-range
four-to-five-node convergence, independent physical faults, read consistency,
zero lost acknowledged commits and sustained throughput gain after convergence
remain separate production gates. These runs provide no physical RPO/RTO,
p50/p95/p99 results, SQL transparency, general cross-range atomicity or source
capacity certification.

## Custody access, hashes and portable verification

**Raw artifacts were uploaded to a public GitHub repository's Actions artifact
service, governed by GitHub access and retention. Local 0600 files and a 0700
directory after download do not prove owner-only GitHub confidentiality.** Full
raw archives include generated fixture database roots and mesh signing keys.
No production credentials are asserted to be present. Raw archives and their
bytes are not embedded in this packet. Safe projections omit entire custody
trees and custody manifests; the exact whitelist was inspected before use.
This also clarifies the scope of “private” wording in earlier frozen capture
notes: locally restricted copies, not a verified GitHub confidentiality claim.
Earlier evidence bytes remain unchanged.

Capture was a **guarded live-owner stable file copy, followed by cleanup**, with
all five service/DB/native/journal guard sets held, owners alive, listeners not
drained and no engine reopen. It was not an atomic stopped snapshot, backup,
cold recovery or physical power-loss test.

| Run | Complete raw SHA-256 | Safe logical ZIP SHA-256 |
| --- | --- | --- |
| Ten | `6c52a352b4850c704ed40254dfcccbdbd8076ea2a607609ada56dba615ac38a7` | `2f51b03e903b5297a31494e87db15ee870e5a27b27716f7e81a8acf0ee7c7b3d` |
| Remote51 | `badbb675c2dd6c28949be0bbee50a39a83d92b57632365a2ab142067704660aa` | `4a4f2f590c1c3277d9c3f2abb443a8e256239817fc928922ac0bc5544b3c13a2` |

`runs.json`, per-run GitHub metadata, captured reconciliation, exact whitelist,
projection manifests/projectors, environment diff and compressed Core logs
retain provenance. `SHA256SUMS.json` pins packet files. Run offline using only
Python's standard library:

```sh
python3 -B /path/to/packet/verify.py
```

This checks packet/projection/member hashes, exact source/test identity and
whole results, application ledger payloads, health/custody and bounded attempt
observations, catalogue fault records and seven failures including partial
NoDomain evidence. It cannot verify excluded raw bytes without the originals.
With separately authorized local raw custody, additionally run:

```sh
python3 -B /path/to/packet/verify.py \
  --raw-ten /local/b639-native-base-ten-artifact14082820921.zip \
  --raw-remote51 /local/b639-remote-fiftyone-artifact14082737655.zip
```

That verifies complete raw archive digests, all included/omitted member hashes
and every custody file. It reads archive bytes only: no database engine open,
recovery, network access, Rust rerun or modification of evidence.
