# Native82 original failure: eleven initiator fixtures and one refusal matcher

**Original result: FAILED — 70 passed, 12 failed, 0 ignored, 1088 filtered out,
941.30 seconds.** These are actual Core library cases, including eleven
macro-generated controls. The duration is neither RTO nor a throughput measure.
This packet preserves that result; it does not merge or test a correction.

Run [43544810818](https://github.com/nikoma/bicdb/actions/runs/43544810818), Core
job [132293554417](https://github.com/nikoma/bicdb/actions/runs/43544810818/job/132293554417),
checked out qualifier `88ffe19f40ffd4ee83fb63a062795380b385bb33`.
Its complete Core tree is `0472ff801146f2cdf51a6db769c0354315eb5b04`, identical
to runtime `5a85900f571c389ff97e278a0b9555048bdeadec` (`1.0.502-beta`). The
complete repository trees differ: qualifier workflow and memory-guard script
changes are retained, not disguised as an identical whole checkout. Original
source environment, Rust toolchain, compiler-produced list, job/run metadata,
raw logs and Git object membership proofs are retained. No binary hash was
measured/retained for this packet; no release or deployment is inferred.

## Findings and exact limits

| Finding | Classification | Actual scope |
| --- | --- | --- |
| Original selected cohort | **FAILED** | Actual 70/12; all 82 identities and logical fixture ledgers retained |
| Eleven hot fixtures use a donor as handoff initiator | **Confirmed fixture defect; cases FAILED** | Frozen code constructs `clients[donor]`; actual source ledgers identify n2, or n3 for follower case; copied metadata leader is n0 and selected range leader n1 |
| Those fixtures' later image/NEW/decision/policy/Joined fault assertions | **NOT TESTED** | Stop acquiring the required Role before those cuts; a broad fixture phase label is not proof of reaching its named milestone |
| Twelfth case's local metadata invalidation refusal | **PASSED, narrow observed refusal only** | Actual term mutation path reached; earlier fenced term1, copied target term2; exact actual-version-dirty error returned |
| Twelfth full NoDomain test | **FAILED** | Outdated exact refusal matcher panics; final state assertions/result file are not reached |
| Exact timing/generation or wire identity of rejected witness | **INCONCLUSIVE / unavailable** | Donor/leader correlation is raw evidence plus frozen source construction; SDK observer did not capture native witness RPC |
| ACK retention after arbitrary failover, physical scale-out, production qualification | **NOT TESTED by this packet** | No engine reopening, recovery or independent-host experiment |

The eleven failures all report:

```
metadata recovery-role identity, purpose or committed configuration changed
```

`hot_policy_tests.rs:252` constructs the actor from `clients[donor]`.
`distribution_consensus.rs:341` requires a `CommittedHandoff` initiator to be the
range leader **or** metadata leader. The retained collector and Manifest identify
the actual donor; copied MStore points identify n0 as metadata leader. These
recorded donors satisfy neither required leader identity. The construction is
source-derived and correlated with actual artifacts; it is **not** a newly
recorded or replayed witness request. Other witness checks also exist; this
packet is not an independently executed corrected-actor reproduction.

| Original case | Donor | Intended later gate not reached |
| --- | --- | --- |
| `TargetHotPolicy` | n2 | Original A through first DATA policy |
| `TargetHotEarlyPolicy` | n2 | Early ALL-released admission refusal |
| `TargetHotStoreReplacement` | n2 | Actual Store-slot replacement refusal |
| `TargetHotDecisionReplyLost` | n2 | Lost committed decision reply/same native attempt |
| `TargetHotData` | n2 | Original gateway post-join ACK and full Strong |
| `TargetHotDataReplyLost` | n2 | Lost decision reply followed by DATA acceptance |
| `TargetHotDataFirstCallerExpired` | n2 | Actual first-policy caller expiry/fresh same-xid continuation |
| `TargetHotDataFollower` | n3 | Follower Joined binding without invented leader readiness |
| `TargetHotDecisionTermBeforePut` | n2 | Actual term change before decision first put |
| `TargetHotDecisionTermAfterCommit` | n2 | Actual committed decision then term change/refused vote |
| `TargetHotFirstPolicyTermBeforeHook` | n2 | Actual policy readback then term change/refused hook |

The raw helper panic is `remote_source_tests.rs:283`, during Role acquisition.
These cases already have two source APP ACKs, actual remote OLD close/collection,
and genuine returned Manifest/History frames. They do not establish later target
rows/native materialization or the intended fault boundaries. The full original
names and statuses are in `case-observations.json` and the compiler/test logs.

The separate `TargetNoDomainMemberChanged` case reached the actual local term
mutation and received:

```
actual metadata authority version changed/dirty; retained native outcome is not a current grant
```

`target_no_domain_tests.rs:105–116` increments the actual MStore term, then
expects the older substring `actual owned metadata member state changed`.
The stronger refusal is printed in the assertion panic. Its final
`no-domain-1-member.json` and post-refusal state assertions are absent; do not
retrofit them. The retained earlier actual fence has term1 and the copied target
MStore has term2. An exact before/after gate-generation trace was not retained.
This is local metadata invalidation, not a partition/new-leader experiment.

The original `isolated-source-classification-original.json` includes generic
actor-construction fields for the twelfth record. Those fields are not applied
to that distinct NoDomain path here. Both original author reports are retained
byte-identically; their interpretation is bounded by their actual source paths.

## Acknowledgments and retained evidence

The 82 independent fixtures record **164 initial APP ACKs, zero initial APP
uncertain outcomes**. They are not 164 operations on one production cluster.
They precede the tested movements. No blanket post-failure Strong/cold-recovery
reconciliation follows from these counts. All logical attempt reports are in
the projection; deliberate native errors, retries and uncertain internal
transactions must remain distinct from public source APP acknowledgments.

Seventeen cases also retain the actual original SDK observer: eight records
per case, two APP exchanges, full synthetic request/response frames and sequence
numbers. Those journals cover the original APP calls, not the separate native
witness RPC that fails later. Planned descriptors and source-level actor
construction must not be called observed wire routing.

The full Core log is retained unchanged as gzip:

- Decompressed: 22,693,709 bytes, SHA256
  `241738e911657e0ed0e6d975f5ddf14ff10076f40e222699f859bf945a1181d0`.
- Original focused artifact14094258684: 23,829,148 bytes, SHA256
  `bdf53f0eb21bf88397a14099f6a971460db1497104bcb799ae078b3d85c03561`;
  9361 files, 379,438,111 uncompressed bytes. **Not in public Git.**
- Original pressure artifact14093402901: unchanged ZIP, SHA256
  `7676855d20070a02076c7a00b9d916c199a3f254e261fb7dc3fbb0e0fc3505d4`.

The full focused artifact contains database pages/WAL, seeds and **410
`mesh_signing_key.json` files**. Complete original bytes remain with the local
custodian outside Git, verified 0700 directory/0600 file; see
`private-original-custodian-receipt.json`. No signing-key contents were decoded
or rendered. The public `logical-projection.zip` includes exact bytes of 1075
explicitly whitelisted logical files; all 8286 database-custody members are
excluded with path/size/hash inventory in `projection-manifest.json`.

Selected MStore fields needed for correlation are derived separately from 24
omitted copied JSON files; their provenance/hashes are retained. They are not
engine-open, fsync or recovery evidence. Default verification checks published
correlation; optional original-ZIP verification independently rederives them.
The original root classifier's empty `failed_names` scan is preserved as an
incomplete initial scan; the actual twelve names come from final cargo output
and artifact case identities.

Public GitHub artifact access/retention is not a privacy guarantee, and a local
projection does not remove already-hosted originals. Literal/path scans are
recorded in `publication-scope.json`; they do not prove the absence of arbitrary
secrets. Synthetic complete frames and application payloads are intentionally
retained; this packet does not claim to exclude raw frames. Omitted database
bytes are not a public complete-database or cold-replay proof.

## Verify and reproduce

```
python3 verify.py
python3 verify.py --tamper-controls
python3 verify.py --original-zip /path/to/private/original-artifact-14094258684.zip
```

The first two are portable, read-only artifact checks requiring Python's
standard library, no Git/network or BicDB engine. Optional original verification
only reads ZIP bytes/selected metadata JSON and checks exact member/omission
hashes; it never opens or repairs a database. A verifier PASS does not change
the native **FAILED70/12** result.

For a separate authorized reproduction, use exact checkout88, its retained
workflow/helper settings and toolchain. Its original Core commands are:

```
cargo test --config 'profile.dev.package."*".opt-level=2' -p bicdb-core --lib --locked native_registered_fifth_ -- --list
cargo test --config 'profile.dev.package."*".opt-level=2' -p bicdb-core --lib --locked native_registered_fifth_ -- --test-threads=1 --show-output
```

Set `BICDB_TEST_SOURCE_SHA` to the actual checkout, and the existing
`BICDB_TEST_EVIDENCE_ROOT` to an isolated evidence directory as in the
retained workflow. Preserve original per-request budgets and fixture assertions.
Compare the actual complete compiler list; do not use the earlier incomplete
71-function scan. This documentation work did not run those commands.

See `issues.md` for the smallest next controls. Fixture corrections and 617/913
production successors are outside this evidence-only leaf. A narrower passing
transport-stall component is separate evidence, not an explanation or rerun of
these twelve failures. The original failed Azure qualification and production
gate remain unchanged.
