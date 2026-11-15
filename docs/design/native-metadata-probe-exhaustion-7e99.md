# Metadata quorum retries can exhaust before listener recovery

This issue and corrective source unit concern **1.0.520-beta source `7e99c9c96970b78a44402c975627c64e001adffa`**, tree `8022e9f6e6d51f9eb4691ca877c328229b29c3b1`. Normal CI used merge checkout `4c46808e3bbef80e5792cce2cbd9de8203839bbe` with that same tree. This is not evidence that the original Azure unreadable-record incident had the same cause.

## Reproduced failure and evidence

Native job `132883862065` in [Tests run `43737698638`](https://github.com/nikoma/bicdb/actions/runs/43737698638) finished with **396 passed, 55 failed and 1 ignored**. The selected case was:

```text
distribution_native_host::provisional_listener::tests::first_owned_tests::native_first_remote_catalogue_actual_foreign_disk_preimage_not_overwritten
```

Its initial four-node setup failed before the intended foreign-disk control. The original `OpenInitialData` for `n0` returned an error; the other three original node operations completed. No second `OpenInitialData` was attempted. Failure to reach the intended foreign-disk fault is not a passing refusal test and does not establish that the foreign preimage was overwritten.

The bounded `METADATA_PROBE_EXHAUSTED_FACTS` report at raw log line **29787** records the actual original quorum round:

| Field | Actual value |
| --- | --- |
| Range / epoch | `4` / `1` |
| Metadata term / read index | `1` / `1` |
| Context | `01a1e043-704f-7821-8033-8917f52c308b` |
| Read request | `01a1e043-704c-7463-8392-7b9721862031` |
| Acquisition | `01a1e043-704c-7463-8392-7ba212fdcf9a` |
| `n3` results | Reset, then three refused connections; attempts dequeued at 1625, 1727, 1830 and 1912 microseconds |
| `n2` results | Reset, then an accepted real ACK at 6541 microseconds; term, match, applied and read indexes all `1`, original context/request echoed |
| `n1` results | Reset, then three refused connections; attempts dequeued at 7831, 7928, 8002 and 8084 microseconds |
| Remaining context budget at last dequeue | **1,490,641 microseconds** |
| Omitted diagnostic facts | `0` |

These are coordinator result-dequeue times, not network-completion timestamps. The leader and `n2` provided only two of the four voters; the original majority remained three. Refusing to mint quorum authority was correct. Consuming all four attempts immediately, while the request still had time available, made this setup unable to tolerate the observed listener interruption.

A separate exhausted report at raw line **19433** concerns range `1`, epoch `2`, a different request and a deliberately unavailable peer. It must not be attributed to this setup failure.

The complete frozen archive is `visibility520-pr207-7e99c9c96-run43737698638`: 101 regular files, including 98 premanifest files, 11 complete gzip logs and 8 original ZIPs containing 28 members. Manifest SHA256 is `124bdadc431992c03a89035e8879944acd20e732b8c31d3e9995ba5775ccf9fa`. Native raw log SHA256 is `b59d517171ecf60921177338f5593bef6422806caeecdb72ecd005f2a866c8db`; stored gzip SHA256 is `7b7df7c518c498dd6409604b548791ae9fd62c1620865c829d3b906557d9542c`. Independent root whole-archive verification receipt SHA256 is `a08a618b81bee40e26723a5ee01e8f3309d39a6b1237b63e1c37bef017a2abe3`.

## Corrective source boundary

`distribution_metadata_read_provider.rs` retains the same actual quorum context, caller identity, admission predicate, consensus store, original cancellation/deadline, worker/voter limits and maximum attempts. Only existing retries of actual `Io(ConnectionReset)` or `Io(ConnectionRefused)` receive a short delay: 5, 10 and 20 milliseconds following attempts one, two and three. The retry eligibility time is clipped by the original context deadline. These constants are an implementation policy, not an established recovery bound.

The coordinator selects an eligible candidate from its bounded queue. A delayed peer does not block untried or healthy peers and does not occupy a sleeping worker. Pending delayed candidates are not reported as an exhausted voter set. Guard-free result waiting is bounded by ten milliseconds, the earliest eligible retry and the original deadline. Cancellation and expiry are checked through the existing original token. Accepted ACK validation, rejected-ACK catch-up, topology-change refusal and final authority checks remain unchanged. This does not retry a complete `OpenInitialData`, repeat source effects or weaken quorum intersection.

The short delays do not guarantee that an arbitrarily long listener interruption is recoverable in four attempts. The original failed case and unchanged caller windows must still pass in the integrated candidate. A subsequent failure must retain its original results and be investigated; it must not be hidden by extending the request, increasing the attempt cap or relabeling refusal as readiness.

## Finding classification and smallest next controls

| Finding | Classification | Evidence or next test |
| --- | --- | --- |
| Original FIRST setup | **Failed** | Original `n0` setup error; intended foreign-disk cut not reached |
| Four attempts exhausted rapidly with original time remaining | **Failed** (premature exhaustion confirmed) | Actual typed errors and complete bounded probe report above |
| Quorum refusal without three voters | **Passed at this boundary** | No accepted quorum authority with only leader plus `n2`; not a complete durability test |
| Exact cause of listener resets/refusals | **Inconclusive** | Concurrent listener replacement is a source-supported hypothesis; the original trace does not prove the cause or restoration event |
| Corrective pacing source | **Not tested** (implemented source only) | Standalone formatting and source review do not prove execution |
| Actual TLS listener recovery under the same context | **Not tested yet** | Four real voters; close two actual listeners while retaining their real stores, certificates and addresses; retain original socket failures; restore listeners; require genuine matching quorum ACKs and attempt-start intervals after the actual preceding returned errors |
| Cancellation and expiry while retries are pending | **Not tested yet** | Cancel or naturally expire the original parent during real socket failures; require refusal without a new request, renewed deadline or manufactured ACK |
| Original FIRST foreign-disk assertion and SDK/Strong tail | **Not tested after correction** | Rerun the unchanged full native case on the versioned integrated head |
| Azure scale-out, physical RPO/RTO and throughput gain | **Not tested by this change** | Independent physical experiment and complete external ledger remain required |

The runtime fix, component definitions, exact-head CI result, physical qualification and deployment must be reported separately. There is no production qualification claim here.
