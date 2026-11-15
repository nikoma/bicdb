# Failed corrected ledger candidate: bb33

Source `bb33a0dcf948de053931b691fa6260e5bfa74c5b`, 1.0.516-beta,
was tested by [Tests 43655400089](https://github.com/nikoma/bicdb/actions/runs/43655400089)
and [Rails 43655400119](https://github.com/nikoma/bicdb/actions/runs/43655400119).
The actual GitHub merge checkout was
`eb258270e9819a755aacd976126811f84910767f`; its tree was exactly the candidate
tree `dffa13e01157a59e3be4587d3fff03e26b31dd71`. This head failed acceptance.
The earlier [3a failure](native-ledger-3a-failed-acceptance.md) remains unchanged.

| Evidence scope | Actual result on bb33 |
| --- | --- |
| All-target compilation and nondefault consumer | Passed. This compiles the Rust probe example. |
| Core and seven receipt/parser integration controls | Passed. The normal `tests/native_external_ledger_receipts.rs` target executed all seven controls: seven passed, zero failed/ignored. |
| Native serial library suite | **Failed**: 331 passed, 15 failed, one ignored, 941 filtered, 3,190.35 seconds. |
| SQL, PGwire, remaining workspace, Rails and repository checks | Passed. |
| Overall required Tests run | **Failed**. Passing parser or workspace targets do not repair Native. |
| SDK producer/collector against a database cluster, independent-machine scale-out, RPO/RTO, throughput or deployment | Not tested by this run. |

## Native failure boundaries

Twelve hot cases returned `QueryTimedOut` at the original
`hot_policy_tests.rs:876` publication call. No `NATIVE_POST_WAL_FAILURE` marker
was retained in this run. That absence does **not** establish that no native
write occurred, that an effect was rolled back, or that every timeout occurred
before WAL. Their native effects and timing cause remain inconclusive. Later
FIRST/Open/ALL-release/application assertions after the failing call were not
executed; earlier completed CLOSED or decision-helper assertions must not be
erased.

Two prepared-candidate cases actually refused counting with live OWN before and
after the call. They failed their exact error-message expectations at lines 149
and 158. Actual errors were `joint bridge preparation belongs to another service`
and `native range image: publication scope lost actual preparation pointer/archive`.
These are diagnostic-contract failures; the evidence does not show admission of
the wrong service or missing archive.

The middle-history control reached its actual publication scope on n2/range 1,
then refused with `CLOSED scope control lacks retained middle history`.
Its requested fault 2 did not complete; the line 113 assertion failed. This is
not a successful injected-corruption refusal. The retained selected preparation
was at range index 1, without the required interior history entry.

All six original issue 195 collection controls and all four remote native-decision
pipeline controls passed on bb33. Those scoped results do not prove the failed
hot positive Serving path. The two live preparation-refusal clock diagnostics
belong to passing explicit cancellation/expiry cases, not to the twelve failed
publication calls.

The actual 347 Native case names and every final outcome match the separate
c05 run. This correspondence does not establish identical internal clocks,
native effects or causes. The current combined source incorporates native
`881087806b3a5a9792b3e0ba46ba1ba1f2dd0617`; it needs its own normal CI.
Production Rust and workflows are byte-exact to 881, and the 20 ledger files are
byte-exact to bb33. No unchanged Python or earlier passing target substitutes
for acceptance of that combination.

## Frozen original evidence

The private local capture is
`/root/bicdb-ha-private-evidence/native516-pr204-bb33a0dcf-run43655400089`.
This is a custody pointer, not a public archive download or confidentiality
guarantee for GitHub's public-repository artifacts. Complete originals were
retained once; no database was opened or repaired. The eight original ZIPs
contain pressure records, Rails diagnostics and REST RPC journals. No Native
fixture/source-ledger/database custody bundle was uploaded for this run.

| Original or derived evidence | SHA256 |
| --- | --- |
| [Native job 132632707698](https://github.com/nikoma/bicdb/actions/runs/43655400089/job/132632707698), raw 13,612,369 bytes | `b6cc6e9aee839465cb290d78862ba57e5d95ade47f5c4f5d233ada1a225ef7a1` |
| Same Native log, lossless gzip | `addd04a5fddc69b2b0d1814d7a7e5836adb83b74961a39ff95c0fe1785261cf2` |
| [Core job 132632707689](https://github.com/nikoma/bicdb/actions/runs/43655400089/job/132632707689), raw 2,826,878 bytes | `5da4f4ad10be6675d97c396d64bceaf3d6946d58f3655de9c119e58f5d2d8986` |
| Same Core log, lossless gzip | `190374781f8769b286c9123d977cc999178c6e56ea9f557bbaa2a74e22352e48` |
| Actual seven-parser-case result projection | `dc7c430cebdf75992e2928cbe1b9e5abf2d9219c987a11b2174a547913f44b0c` |
| Native 347-case inventory, original diagnostics and failure classification | `589f5a1027d6f1e76d833fef310116c3093d9e0a90fb7dc3b30152a3aca90460` |
| Complete canonical manifest | `a1747ca6debdccdff9d09b7e59d3d859e11331cda36fa97c1b928e9788662f90` |
| Complete-capture verification | `9a4da5d305c8762e2780d583e06de1276c657f2fb5a1a0b92fe4181b1fa6ef3e` |
| Independent final rehash and corruption-control receipt | `ed6c2bdd95a564e4a8b612418ac0f71c083bf97dcb9d93220472e36c47bddb07` |
| Root independent whole-archive check | `acc8e340d5cbf12fcc27f8c68118ca5a48c624328b7ba7533b51bc06377188c5` |

Verification covered all 86 manifest files, 11 complete original gzip logs,
eight unique original ZIPs and 28 ZIP members. API digest/size/head, raw/stored
hashes and ZIP CRCs matched; zero captures were unavailable and no partial file
remained. In-memory copies of an actual original ZIP and Native gzip with
corrupted compressed bytes were rejected, as was a mismatched expected hash.
The canonical bytes were unchanged by these controls. Capture integrity is
separate from database correctness or successful acceptance.
