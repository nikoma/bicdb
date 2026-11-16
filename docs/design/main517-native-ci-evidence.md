# Main517 native CI: passed within its recorded contract

Main **1.0.517-beta / `bde74650c40934096e1cc6c6e75a3ea3c4643cf4`** merged
PR206 at November 15, 2026, 02:13:01 UTC. Its tree
`7ee0e1f37c310612165b56e3ca0c1dc10a8e0e9c` equals tested source
`23e2b7682ccbfd5db8fbca93e9fc2740f842aefc` and the actual pre-merge checkout
`9c740c3d07decd5efcbc1a2de66324018daa1f3a`.

The [required pre-merge run](https://github.com/nikoma/bicdb/actions/runs/43659430043)
and [Rails run](https://github.com/nikoma/bicdb/actions/runs/43659430112) passed.
The [native job](https://github.com/nikoma/bicdb/actions/runs/43659430043/job/132645298216)
reported **337 passed, zero failed and one ignored**, with 940 filtered tests.
There are 338 reported case names; the ignored internal child entry is not
another directly executed pass. The runner's 1,876.46 seconds is not an RTO,
throughput result or controlled performance comparison.

| Finding and scope | Classification | Evidence boundary |
| --- | --- | --- |
| Native suite and other required jobs on the exact merged source tree | Passed | Original complete logs and actual checkout/tree verified |
| Thirty hot-policy cases | Passed | Actual execution in this job |
| 131 named registered-fifth cases | Passed | Actual execution; overlaps the hot-policy group |
| 68 Strong-point component cases | Passed | Actual execution; groups overlap and are not additive |
| Original source881 acceptance contract | Not tested by this run | Different source, compilation settings and fixture OWN bound; nine source881 cases absent |
| Independent-machine automatic expansion, cold failures and sustained capacity gain | Not tested | Library fixtures do not prove these gates |
| Production qualification or verified deployment | Inconclusive | Neither follows from source delivery or CI |

This is a substantial actual component result. Its settings must remain
attached to it. The
[hot fixture](https://github.com/nikoma/bicdb/blob/23e2b7682ccbfd5db8fbca93e9fc2740f842aefc/crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline.rs#L218)
uses a four-second IO/OWN ceiling; source881 used 1.5 seconds. The
[native workflow](https://github.com/nikoma/bicdb/blob/23e2b7682ccbfd5db8fbca93e9fc2740f842aefc/.github/workflows/tests.yml#L244)
also uses workspace optimization level1, dependency optimization level2 and
one test thread. The hot SDK operation budget remains two seconds.

The exact source881 inventory has nine additional names: six collection
controls, two after-scope controls and one supervisor short-reader case. No
new names occur in this run relative to that inventory. This reflects the
different source line; it is not evidence of deliberate test deletion. Missing
cases cannot count as passes. The [failed source881 result](native-881-failed-acceptance.md)
remains 325/21/1. Changes in code, budgets, compilation and coverage prevent
attributing the different outcomes or elapsed times to one cause.

The complete original native raw log is 13,059,459 bytes, SHA-256
`c1d57d2eda0ed7c540072c2affe367b6bd89eae3cd08c5302127eab75cf2fac6`.
The 1,881,033-byte gzip has SHA-256
`903a8c442ac5bebb7046e00e930262cbd905e3d911853b6a2864e5987994268a`.
The actual execution/settings projection is
`b44d1aa51f42711fb75e7c0c49abd1944f294a0f74a033018d8e67f18059e79e`;
root independently checked every reported outcome and source setting, receipt
`f1af163fd9f468cf785be1daeea43e5dd2e80fb95d0d9797d87433712489af13`.

Complete capture retained 85 manifest files, 11 lossless original logs, eight
unique original ZIPs and 28 members, with zero missing captures or partials.
Root independently verified API artifact digests/sizes, every stored/raw hash
and ZIP CRC, and hashed every member without extraction or opening a DB.
Manifest SHA-256 is
`6a66e4d2e9ee90a641ec19328e462c5eeaf097036e5b4528f5cec073586d7bd0`;
capture verification is
`713755f2fc680b0800259ef19b8a75ae19860da03bf12ae04c666abfad7bce99`;
root whole-archive verification is
`998af01d91650aad85d0f2e428e2251a056cc4d82e02e9e191c62421c2562ff5`.

The [machine-readable result](main517-native-ci-evidence.json) preserves exact
source/settings identities and the nine unmatched source881 cases. It is an
audit artifact, not an admission grant or a qualified binary manifest. The
separate post-merge push CI has its own run identity and is not relabeled by
this pre-merge result. Automatic remote orchestration, cross-range SQL,
external-ledger physical reconciliation and the original failed Azure
qualification remain separate gates.

At the November 15, 03:03 UTC checkpoint, the separate
[post-merge native job](https://github.com/nikoma/bicdb/actions/runs/43666718926/job/132667643050)
had **failed**; the remaining-workspace job was still running. The job API
snapshot SHA-256 is
`5c9e9460e7986c13cf87d241d4fa1cd1d8996b6b736b63b392bf6a756c344154`.
Complete original post-merge capture and exact failure classification were
pending at that checkpoint. Main cannot be described as unconditionally
CI-green. The earlier pre-merge pass and this failure are both retained;
their difference alone establishes no timing, hardware or regression cause.

Subsequent original-log verification established **335 passed, two failed,
one ignored** in that native job, with 940 filtered and 1,866.98 seconds
reported. The post-merge Core job also failed a concurrent-backup target.

| Finding | Classification | Actual boundary and next deterministic test |
| --- | --- | --- |
| `native_writer_data_domain_actual_decided_closed_points_preserve_original_policy` | Failed | [Actual metadata lock inspection](https://github.com/nikoma/bicdb/blob/bde74650c40934096e1cc6c6e75a3ea3c4643cf4/crates/bicdb-core/src/db/range_image_native/rf3_fixture/native_historical_tls/native_joint_old_closure_tests/complete_joint_tests/writer_data_domain_fact_tests.rs#L102) unwrapped `None`; the current loop node was not retained. Synchronize the actual competing metadata operation and retain node/request state before inspection. Later fact checks were unreached; no Closed-point guarantee follows. |
| `native_registered_fifth_remote_new_actual_cancel_after_take_retains_exact_progress` | Failed | [Actual S2 retirement](https://github.com/nikoma/bicdb/blob/bde74650c40934096e1cc6c6e75a3ea3c4643cf4/crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/remote_source_tests/source_export_retirement_tests.rs#L696) returned an n2 RPC rejection: `source discovery metadata busy at boundary`. The later intended after-take exercise was unreached. Hold that actual metadata boundary, then release within the unchanged parent deadline; require actual retirement and the original after-take assertions with the same progress/owners and fresh authority. A separate hold-through-deadline variant must refuse within the original bound. |
| `a_backup_taken_while_writes_are_in_flight_restores_consistently` | Failed | Core's `backup_paged` target reported 3/1/0; [backup creation](https://github.com/nikoma/bicdb/blob/bde74650c40934096e1cc6c6e75a3ea3c4643cf4/crates/bicdb-core/tests/backup_paged.rs#L271) returned `source file paged/store.wal changed while the backup was streaming`. Restore/payload checks were unreached. Synchronize a real WAL change across the streaming copy and require the original successful backup/consistent restore assertion. The failed backup does not establish acknowledged-data loss. |

These reproduce from the exact post-merge run linked above and its
[Core job](https://github.com/nikoma/bicdb/actions/runs/43666718926/job/132667643037).
They remain open; no implementation fix is asserted here. Root verified both
complete original gzip/raw logs, actual checkout lines and failed case/summary
lines, receipt SHA-256
`32da7d2a48687cb107014eef91916073d8b719fd30ea04f5cc1656bd9dacb203`.
Native raw SHA-256 is
`85c48b06103445131bdd781dfb29bb6da030e35f37053958190e3790c19a141c`;
Core raw SHA-256 is
`24ff5f1bab02071168fdf81460026e00e533da802ebaa0e832971f17adc32fb7`.
Complete post-merge archive finalization remains pending while workspace CI
continues. Neither the earlier pass nor these failures provide physical RPO,
RTO, throughput or production qualification.

The [legacy concurrent-backup issue](legacy-standard-live-backup-defect.md)
records the cross-layer gate concern and eleven exact source pins. The separate
owned-capture target reported fourteen passes in pre-merge CI, including its
actual concurrent-commits/DDL exact-cut case. That passing protocol is distinct
from the failed legacy API and its unchanged acceptance assertions.
