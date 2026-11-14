# Two populated source ranges: held-response progress component

**PASSED, narrow component:** one selected native library test passed in
12.29 seconds. An actual range1 Rows response was held after export while three
ordinary original-client calls on populated range2 completed. This is not a
completed relocation, second Joined-range movement, disk-stall experiment or
physical scale-out qualification. The original Azure qualification stays failed.

[Run43548089992](https://github.com/nikoma/bicdb/actions/runs/43548089992),
Core [job132304020107](https://github.com/nikoma/bicdb/actions/runs/43548089992/job/132304020107),
actually checked out qualifier `21669a25e0d95ff5b7a9ec8d367d5895c6d8e8fc`.
Its complete Core tree `9068ea94acc33d8645cc76c1d51d11c79eda0127` equals
runtime `c9680e82acf9ceb01ad8994b7bb0b67b25768782`, inherited prototype source
version1.0.502-beta. The qualifier differs in workflow/memory-guard settings;
whole repository trees are not described as identical. Git commit/tree/member
proofs and original source copies are retained. No binary SHA was measured or
retained by this packet; source version does not imply a release or deployment.

## Results and limits

| Evidence | Status | Meaning |
| --- | --- | --- |
| Exact component case | **PASSED** | Actual1passed/0failed/1170filtered,12.29s |
| Complete native-family inventory | **Listed/compiled only** |83 compiler-listed cases; only the new exact component executed |
| Original b01 attempt | **FAILED compilation** | Both jobs report E0603; zero runtime tests executed |
| Independent range2 progress during range1 response hold | **PASSED component** | Two full Strong reads and one new APP ACK, held at invocation and return |
| Same original held request resumes | **PASSED component** | Released handler returned a validated Rows reply without request expiry |
| Full range1 stream/target installation/Joined promotion | **NOT TESTED** | Rows.complete=false; no Finish or target install |
| Disk/native I/O stall, controller convergence, second Joined movement | **NOT TESTED** | Hold occurs after export and outside engine locks |
| Routing refresh/owner change/404 recovery | **NOT TESTED here** | No observed retry, refresh or404; range2 descriptor stays stable |
| Physical throughput, RPO/RTO, production/deployment | **NOT TESTED** | Same-process test fixture and file ledger;12.29s is test duration |

The original fixed gateway and original SDK request budget of2000ms remain in
use, with actual hashed keys and no manual shard hint or SDK refresh. Range1 is
selected for its image transfer; range2 is already populated. The real handler
exports the first range1 batch, then holds its TCP response. It is not blocked
inside a filesystem or database operation. Releasing the hold resumes the same
original request, rather than issuing a replacement.

The retained operation sequence is:

1. Two baseline APP ACKs populate ranges1 and2; a full Strong reads range2.
2. The genuine range1 Rows response is held after actual export.
3. Full Strong of the existing range2 value, a new range2 APP ACK, then full
   Strong of that new value all return while the hold remains active.
4. The original response resumes unexpired; a final full Strong reconciles the
   new range2 value after release.

That is **three APP ACKs, four full-value Strong returns, zero failed/uncertain
logical APP outcomes**, with every recorded intent/result retained. The five
additional API calls are in `two-range-stall-external-ledger.json`; the two
baseline APP calls are in `ack-ledger.json`. The three middle calls have actual
hold snapshots at both invocation and return. Their observed durations are
266954,310869 and294897 microseconds; these individual samples are not a
latency distribution, RTO or capacity measurement.

| Record | Role | SHA256 of expected full JSON payload |
| --- | --- | --- |
| `items/registered-fifth-ack-r1-8` | Baseline range1 ACK and actual exported batch | `a4123271a05a418aaccc67d998660f949209258545268e1f7573501ee0231555` |
| `items/registered-fifth-ack-r2-0` | Baseline range2 ACK/full Strong | `392cbf7c149065bb0e1c9d2cc7684c0c45b9ed228b5649e51e265629fe05b6a7` |
| `items/two-range-source-stall-6` | New range2 ACK/full Strong during hold | `222e0910059d42d1a36e723ad2f12146edf7395111f4990c9b99980218604c74` |

The returned batch contains one record from `__bic_cluster_events`, range1,
epoch2, snapshot sequence2. It matches the initial range1 acknowledged payload.
The baseline SDK append was actually dispatched to range1 epoch1 before the
pending placement. This exported-row match is not a post-movement range1 Strong
read or cold-recovery reconciliation. The transfer stream explicitly remains
incomplete; it does not install target rows or complete a relocation.

## Actual routing and source-journal custody

The original SDK observer was attached before the first APP. It retains28
contiguous records for seven primary attempts: three append and four Strong
calls. Exact complete synthetic request/response frames, destinations, IDs,
payloads, audit barriers and hashes are retained. There are no recorded retry,
refresh, error or404 responses. This is the captured application-call window,
not proof of global network/background quiescence; native phase RPCs are outside
that observer.

Range2 calls go to n2 at epoch1. The SDK's cached global generation remains4;
Strong responses report generation5. Range2's owner/epoch/replicas remain stable.
This generation difference does **not** demonstrate stale-owner routing,
automatic refresh or successful redirection. Fields prefixed `expected_` in the
API ledger come from the pending descriptor and are comparison DATA; actual
wire dispatch is recorded separately.

The worker takes the real original source journal, preserving its exact five
entries, including OLD facts and returned Manifest. The genuine Rows exchange
appends the sixth entry. The verifier checks exact prefix preservation, original
MF/A/session/request scope and validated response bytes. A new empty worker
journal would lose prior evidence and does not satisfy this check.

## Source map

Original qualifier source copies are under `sources/21669a25e0d95ff5b7a9ec8d367d5895c6d8e8fc/`.
Here `baseline/` abbreviates
`crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/`.
Every copied source has its immutable Git member proof in `source-index.json`.

| Source | Actual boundary |
| --- | --- |
| `baseline/remote_source_tests/two_range_stall_tests.rs:16`, `call` | Original SDK API, expected descriptor DATA and durable intent/result ledger |
| Same file:86, `exercise` | Genuine source exchange, hold/three middle calls/release/reconciliation and exact original journal prefix |
| `baseline/remote_source_tests.rs:88`, `uses_original_sdk_rpc_observer` | Existing bounded observer attached for this case before APP |
| `crates/bicdb-core/src/distribution_transport.rs:1636` | Test-only hold immediately after the actual authenticated source handler returns |
| `crates/bicdb-core/src/distribution_transport/native_image_response_stall.rs`, `ActualRowsReplyHold` / `hold_actual_rows_reply` | Bounded same-request hold, observed state and RAII release; no authority grant |

## Earlier compile failure remains separate

[Run43547094599](https://github.com/nikoma/bicdb/actions/runs/43547094599),
qualifier `b01ed646b476cbb5f76a495d0f71bc8d13b929d6`, has the exact Core tree of
runtime `c08fb440be78ff1b891dc9a5e4c6ec87228918cb`. Both workspace and focused
Core jobs failed Rust E0603: the new helper was `pub(super)` although its caller
was the grandparent module. No selected case or whole83 inventory ran there.
The corrected c968 leaf changes only that helper's visibility to
`pub(in super::super)`; no test body, request budget, assertion or source-journal
behavior changes. Original failed logs and safe pressure/list artifacts are
retained unchanged. This is a compile correction, not evidence that an engine
failure was repaired.

## Evidence custody and verification

The complete original artifact14095447597 is397780bytes, SHA256
`1637503905e7db9dc971e4adc1285f0c978b472d59cda5614b2e4fe26d217ad1`,
153members/5595680uncompressed bytes. It stays outside Git at the local private
custodian,0700directory/0600file. The public projection retains52 exact logical
members; all101 members under custody, including five `mesh_signing_key.json`
files, are omitted with original path/size/SHA inventory. Key contents are not
decoded or rendered. The original ZIP itself is not published in this packet.

The whole listener fixture dies together at cleanup; guarded live database
copies are not stopped-host/cold-replay evidence. No BicDB database was opened,
repaired or recovered while producing this packet. Public GitHub artifact
access/retention is not confidentiality, and this projection does not remove
already-hosted originals. Literal scans and whitelists do not establish the
absence of arbitrary secrets. Full synthetic RPC frames/payloads are retained.

```
python3 verify.py
python3 verify.py --tamper-controls
python3 verify.py --original-zip /path/to/private/original-artifact-14095447597.zip
```

The verifier needs only Python's standard library, no Git/network/native engine.
It derives the held-call, full-payload and actual-frame results from raw logical
members; validates exact log/source/job identity and both separate outcomes;
and optionally rederives the public projection from the complete private
original. A verifier PASS is evidence-integrity validation, not production
qualification. See `issues.md` for the next smallest acceptance tests.
