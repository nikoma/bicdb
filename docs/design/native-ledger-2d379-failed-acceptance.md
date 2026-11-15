# Historical PR204 ledger acceptance: 2d379 failed Native

Exact source `2d379154a67eca6d6ed5f8f9b5dbe8d7a9c987bd` (source version
1.0.516-beta) failed its normal required Native suite: **330 passed, 16 failed,
1 ignored**, 941 filtered, 4041.25 seconds. All other executed required jobs and
Rails passed. The normal Core `native_external_ledger_receipts` integration
target actually executed all seven parser controls: **7 passed, 0 failed**.
This supersedes compile-only wording for these seven controls, and does not
execute the external SDK workload or qualify physical durability or capacity.

The [claims and source pins](../evidence/native516-2d379-terminal-2026-11-15/claims.json)
retain the exact 16 names, failure boundaries, original hashes and comparison.
These are historical results, not current-main, release or deployment status.

## Exact executed source and failure boundaries

[Tests run 43662386840](https://github.com/nikoma/bicdb/actions/runs/43662386840)
and [Rails run 43662386744](https://github.com/nikoma/bicdb/actions/runs/43662386744)
used the candidate above. The actual GitHub checkout was
`5bd1cba6f47c037d095411131a9e911c69536b0d`; its tree exactly equals candidate
`adfbf752ac0ff5c2ce4aed0476b942d61354a6c7`. The four optional focused jobs were
skipped. They are not additional executed controls.

- Ten common publication timeouts at `hot_policy_tests.rs:876` have actual
  `NATIVE_POST_WAL_FAILURE` records: native commit returned, CURRENT/archive
  match the expected record, original Frozen remains, Controlled is zero, and
  the observed OWN has expired. Nine report XID36 and the follower case XID29.
  Recorded OWN overrun is 25,942–139,846 microseconds before forensic reads.
  These are internal native outcomes; the API failed. They are not APP ACKs,
  rollback evidence or a future authority grant.
- Two further timeouts at the same assertion have completed foreign and local
  journal-preflight traces and no post-WAL marker. Effects remain inconclusive;
  marker absence does not establish that no native write happened.
- The foreign-archive control reached real mutation and publication refusal,
  but its exact diagnostic assertion failed at `collection_binding_tests.rs:111`.
  The actual error was `publication scope lost actual preparation pointer/archive`.
  The fresh missing-archive case propagated that actual refusal at
  `fresh_collection_tests.rs:96`, rather than completing its later assertions.
  Both cases remain failed.
- The remote NEW positive case failed at `source_export_retirement_tests.rs:275`:
  actual S2 Start returned an RPC deadline error. The later intended independent
  NEW-fetch/pin-owner control was not reached; the source effect is not inferred.
- The remote native-D positive case failed its original success assertion at
  `remote_decision_tests.rs:118` with `QueryTimedOut`. The original compact
  post-API observation retains native XID22, `actual_commit_returned`, and healthy
  Core/page/WAL with native status Committed. This is not a successful full
  helper return or fresh proof. The later reader-fence controls were unreached.
  The larger original diagnostic was explicitly omitted at 1,486,461 bytes;
  the separately bounded 3,380-byte compact export passed its original hash check.

The common publication failures occurred before downstream Open, ALL release,
FIRST policy and Joined APP/Strong. Some earlier CLOSED and decision cuts had
already run; a failed later prerequisite does not erase those earlier facts.
Passing definitions elsewhere do not replace a missing intended cut here.

The actual 347 case names equal the earlier 881 inventory: six earlier failures
passed and one earlier pass failed, changing 325/21/1 to 330/16/1. These changes
establish neither a timing/CPU/IO cause nor an explanation of the original
Azure failure. No acknowledged-write loss is established by these failures.

## Complete original custody and independent verification

The frozen canonical archive is retained outside Git at
`/root/bicdb-ha-private-evidence/native516-pr204-2d379154a-run43662386840`:
85 files, 11 complete gzip logs, 8 original unique artifact ZIPs and 28 verified
members; 56,586,240 allocated bytes, zero unavailable captures and zero partials.
The directory is 0700 and files 0600. No DB was opened or repaired.

| Exact object | SHA256 |
| --- | --- |
| Canonical manifest | `54ec2007f1ec3526c060b6d8c7724341d463cdf9ecd3b8652e7c8b7a533fc463` |
| Complete verification | `fe15a8ee5d9f07f9a78f1f96be7ba9fb0812b43ff39dbb5ce2cfc8839fef7df7` |
| Native raw log, 13,595,331 bytes | `e525127ed9cb5dbf9e34047465bd8348e4a804f65679d2613eb9ee0c0f4c3314` |
| Original Native gzip | `0d1f729561864ec31ff2f5cf4c76665ae44bc61634efcc2cf7c19f1a85fe4de2` |
| Final failure classifier | `524efd661d0fe817d9702ca75273e55fca315d8570f5edbf73e20763db29cb5c` |
| Root independent whole-archive check | `c9b5d22ca0ff5a3cf497a2bfa3e7e1cd3d083f52432905ef232e96662bbfea08` |
| Root independent execution/classifier check | `17beca21550da694aa1f761bbe4c7ec2928959deb0690dd501708cfaadc23c89` |

Independent checks reconciled all original stored/raw hashes, API artifact
head/size/digest, gzip integrity and every ZIP member hash/CRC. RAM copies with
corrupted ZIP/gzip bytes and a mismatched expected hash were refused by the
same checks; original archive bytes were unchanged. The committed JSON is an
explicit logical projection, not a public copy of the complete raw archive.
Local restrictive custody does not guarantee confidentiality of artifacts
uploaded to a public GitHub repository. No blanket secret-absence claim is made.

## Subsequent source remains separate

Later exact diagnostic handling, same-candidate KnownRead/catalogue recovery,
owned activation and current-main composition are not tested by this run.
They cannot relabel these sixteen failures or inherit passing qualification
from an earlier revision. Normal validation of the final composed source is
still required. Remote Open/ALL release/Serving, automatic controller recovery,
second populated-range movement, cold self-healing, unavailable-OLD repair and
physical deployment/RPO/RTO/capacity remain separate gates.
