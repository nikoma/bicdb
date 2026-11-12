# Actual INITIAL DATA discovery: healthy metadata, busy service latches

**Confirmed component availability failure, expected RED.** Executed source
`05979463ef312fc1128a42903f908de0189c2a63`, GitHub run `43278532377`,
Core job `131393398627`: **0 passed, 1 failed, 0 ignored, 931 filtered**,
5.07 seconds. The final positive healthy-discovery assertion failed at
`discovery_latch_baseline.rs:326`; every preceding prerequisite/assertion ran.
All-target and isolated no-default consumer job `131393334360` passed.
No production correction is part of this receipt.

## Deterministic reproduction and actual witness

The test is
`distribution_native_host::provisional_listener::tests::initial_data_tests::discovery_latch_baseline::native_initial_data_healthy_metadata_discovery_while_all_data_services_held`.
It creates three actual from-birth INITIAL factories, real enrolled metadata,
actual writer policies and real pinned mTLS listeners. Initial metadata
leader n0 is elected by the existing fixture; this is not an automatic-election
or independent-process qualification. All three real DATA owners open before
the first business ACK.

The same public client ACKs an actual n0-led range-3 value and Strong-reads its
complete nested payload. A separate holder thread then holds ALL THREE actual
DATA service mutexes; the RPC caller/observer owns no engine latch. Under that
cut, real metadata acquisition succeeds TWICE, before and after public
`ClusterEventClient::refresh()`, with distinct actual business/context IDs.
Both observations report metadata leader n0, term 1, read index 1 and the
unchanged committed generation-1 topology. No saved leader bit or caller DTO
substitutes for the real HostMQ producer.

The public client's six exact held requests/replies are retained:

| Destination | Request IDs | Actual responses |
| --- | --- | --- |
| n0 | 3 / 4 | BootstrapSnapshot: `native bootstrap service busy`; FetchTopology: immutable INITIAL unsupported-operation refusal |
| n1 | 5 / 6 | Same two actual refusals |
| n2 | 7 / 8 | Same two actual refusals |

Each complete envelope carries protocol 28 and its actual request ID; replies
include the actual per-node native birth. The dispatch observer separately
records generation-1 routing and the actual destination address. The
public refresh returns `no event cluster member is reachable` despite both
held metadata quorums succeeding. This is not a timeout or simulated reply.

After the holder releases the services, the SAME public client receives an
actual n1-led range-1 APP ACK for JSON null (request 9), then Strong reads
**present null**, not absence (request 10, R1/local B2). A final Strong read of
the original n0 ACK returns the exact original value/history (request 11,
R1/local B1, snapshot sequence 2). All three actual owners still report live
receivers/current writer policy/published dispatchers; the actual post-release
snapshot equals the original topology. Explicit owner stops occur before the
final healthy-discovery assertion reports RED. R, local B and business ACK
count are different quantities here: two ACKs in two different ranges, each
at R1. No R2 range or new recovery floor is asserted.

One server `unexpected end of file` diagnostic is preserved. The test does not
claim zero logged errors or attribute that diagnostic to a durable write.

## Custody and independent checks

`raw/059-core-job.log.gz` is the complete original GitHub job log, raw SHA-256
`752e11ffbf51c5e332d520029211750eec35769ed13c1f5e43ae597c6563d6e8`.
`raw/059-artifact13971398486.zip` is the exact downloaded workflow artifact,
SHA-256 `92835f330a6f30206ed723bf913ce892fac8d658c1b0bafb2bca4d45b65b7b91`.
All six original artifact files are reproduced byte-for-byte under `artifact/`:
source revision, rustc identity, test selection/logs, audit JSONL and RPC JSONL
(557,625 uncompressed bytes). Original requests/responses are full bounded
length-framed bytes, not hashes or truncated projections. The audit records
actual successful MQ constructors/context IDs; MQ internals are not separately
wire-journaled. `observations.json` supplies exact decoded diagnostic views,
not authority. No native database/page-WAL or private TLS roots were archived;
this receipt cannot perform independent database replay or ACK reconciliation.

`source.patch.gz` (lossless original patch) and `source-files.json` reconstruct/hash 1,766 tracked files in
the declared source roots from public read-foundation base
`b9394418bdf9d87864c2c12d6fe81bb7d4039d53`. Verification uses a temporary Git
index, never a checkout mutation, database open or Cargo invocation. The
reconstruction covers the selected crates, Cargo files, scripts, .cargo and
actual workflow; it does not reconstruct unrelated historical documentation
or create the original Git commit. The private source label is not a public
engine merge/release/deployment claim.

Run:

```sh
python3 verify.py --repo /root/bicdb
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v test_verify.py
```

The verifier checks complete package hashes, exact ZIP extraction, source/job
identity, the executed final failure, both held MQ prerequisites, all six
actual routes/replies, cross-leader APP/Strong values and distinct R/local B.
Six semantic tamper controls reject foreign source, a missing held member,
reused metadata context, wrong reply identity, a substituted fallback result,
and null-as-absence. These are evidence checks, not new engine tests or proof
constructors.

## Earlier prerequisite failure stays separate

Source `286ca38a86cdd175b1430b9d9570456eb94b916c`, run `43276644122`, job
`131386760357`: 0/1/931 filtered, 2.88 seconds. Its first held HostMQ refused
`metadata RPC destination differs from actual native birth/incarnation`.
The fixture used its stale pre-enrollment provisional transport. That run did
NOT reach public refresh or establish healthy held metadata. Its original
log/artifact ZIP and `prior-286.json` are preserved separately. The corrected
059 fixture constructs its real transport from the already authenticated
committed snapshot; the receiver's real MQ/birth/identity guards stay intact.

## Classification and next gate

This is an actual same-process TLS component failure of routing discovery
under deliberately held DATA service latches. It does not establish the cause
of the old `3ee` four-process CLI failure, an Azure/physical incident, lost ACK,
false absence, automatic 4-to-5 relocation or production HA qualification.
No other cohort's component passes are added to this one expected-RED result.

The next smallest gate is an INITIAL-only, actual-open/current-service-Store/
owned-MStore-bound producer of committed bootstrap **routing data** before the
DATA latch, followed by this same held-MQ/strict-client acceptance turning
green. Bootstrap data must mint no MQ, Ready, Strong or mutation permission.
Old control/blank-receiver allowlists and generic bootstrap stay unchanged;
FetchTopology must remain refused. Retained-old-Store replacement, ended owner
and wrong actual service must refuse even when an old object is still alive.
No implementation of that correction is included here.
