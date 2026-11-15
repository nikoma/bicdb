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
