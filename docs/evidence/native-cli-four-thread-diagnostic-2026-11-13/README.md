# Four-thread native CLI diagnostic — November 13, 2026

The diagnostic is **failed: 5 passed, 1 failed in 45.39 seconds**. The original
first-ACK/full-value Strong case passed in this run; a different case failed
before its first APP, during initial metadata preparation. This is a diagnostic
observation, not a fix, an explanation of the original failure, or an HA/scale-out
qualification.

| Observation | Exact source / run | Classification |
| --- | --- | --- |
| Original first-ACK acceptance | `68b30c1bbe59b59345ec5a4d2f12354ae60d38ea`, run `43513943520`, job `132191138917` | **Failed.** Six-test target: 5/1, 46.91s. After four DATA ingress publications, the first positive append returned `no event cluster member is reachable`; no positive ACK returned. Its durable effect remains **inconclusive**. Positive Strong and fifth stage **not tested**. |
| Unchanged foreign broad retry | Same source/run, attempt 2, job `132226300840` | **Cancelled** at 23:34:38 UTC. No conclusion about the original failure follows; this diagnostic did not cancel or retry it. |
| New diagnostic target | Qualifier `03454e72cab0dea71b4783153fabf6716d9784fd`, run `43527079466`, job `132235428483` | **Failed**, 5/1, 45.39s. Compile/list passed. Separate community workspace all-targets/consumer job `132235387195` passed. |
| Original first-ACK case in diagnostic | Same qualifier | **Passed:** 3 positive returned APP ACKs; exact full-object, present JSON null and present empty-object Strong reads; true unwritten-key absence; same-owner Strong reread. No fifth stage or movement. |
| Fifth registry-addition health case | Same qualifier | **Passed:** 2 returned APP ACKs, 3 present Strong results, and 3 intended replica-only refusal probes. Fifth remains DATA-serving-false; range assignments unchanged; relocation not attempted. |
| Different receiver-case prerequisite | Same qualifier | **Failed:** command sequence 1, `prepare`, configured 5,000ms budget timed out. All 4 stopped diagnostic statuses remain sequence 0/provisional/DATA=false. This case reached no public client journal, APP, Strong or fifth process; those controls are **not tested**. Native completion after the timed-out action is **inconclusive**. |

The original first-write error masks an earlier error: `ClusterEventClient::request`
replaces the prior APP error when discovery returns no successful topology
observation. Discovery may exhaust the same request deadline before probing all,
or even any, members. Thus the text does not prove that every process was
unreachable. The original runner's journal was retained only in an unarchived
TempDir. Its low-level APP/refresh cause cannot be recovered from the available
original log. A later passing call does not fill that missing evidence.

The diagnostic's first positive APP has an actual retained frame: n0 gateway to
n0, generation 1, range 4, epoch 1, request 93. The single attempt returned a
validated ACK in 534,631,326ns of transport-observer elapsed time, followed by a
successful full-value Strong read. No refresh or APP retry occurred for that
positive call. This timing is neither application latency certification nor RTO.
The expected partial-startup negative APP did perform 5 refused attempts and 16
successful metadata snapshot probes. Its failed pre-open Strong performed 15 Get
attempts and 56 successful snapshot probes before timeout. These expected
negative operations are separate from the successful positive workload.

For the new preparation failure, the stopped allowlist contains metadata JSON
for n0/n1/n2 with matching envelope checksums, term 0, commit index 0, empty logs
and topology generation 1. The n3 metadata JSON is missing from the allowlist.
That is an observed partial preparation prefix, not a native-WAL absence,
rollback, fsync-return or recovery proof. All four children were still alive
before failure cleanup and were then killed and actually waited. Their recorded
stdout/stderr are empty. Command-file processing and host peer traffic are
outside the public-client RPC observer, so the reason preparation exceeded the
client budget remains inconclusive.

## Exact environment and custody boundaries

The qualifier changes only `.github/workflows/tests.yml`: focused CLI environment
and command use four test threads, with an accurate step label. Every other
tracked blob is identical to the original `68b` source. There is no runtime,
assertion, order, TTL, request-budget, source-version or `main` change. This packet
is a later documentation-only leaf; it is not a runtime release.

Both runs use the repository-pinned Rust 1.96.0 (`ac68faa20…`), one Cargo job,
no incremental compilation, debug info disabled and the default unoptimized CLI
dependency profile. Setup also installs `stable` 1.100.0; that setup printout is
not the active repository toolchain. The diagnostic's actual binary SHA256 is
`2b0c963fc72704940e1724d4ad4960c81c6d410d612ce8804ea7721045977304`;
the original was `f97fce3bd2ef09849e9ca07e7a5f8844068ae2c159e1a83288fe2bd1003d87a3`.
Both report 774,280,640 bytes. Byte identity is not claimed.

Differences include focused `--list` compilation/cache history, `--nocapture`,
runtime source SHA and externally retained journal paths, and the absence of the
original broad job's preceding workspace workload/services. The original already
installed the same fsyncing RPC observer; this run relocates its retention and
preserves its output. Four-thread pressure is reproduced, not the entire runner
history. The separate qualifier ref prevents the workflow's same-ref cancellation
from touching the foreign retry. Only one dispatch POST was issued.

The repository is public. Artifact `14086103789` was created by that public
workflow, scheduled to expire November 16 at 23:43:33 UTC. An unauthenticated REST
ZIP request returned HTTP 401 when inspected; this is not a confidentiality or
permanent-retention guarantee. Do not call the upstream artifact private. Local
copies have mode 0600 under a mode-0700 directory; that does not change upstream
access. The artifact includes the tracked-source archive, actual client journals
and the stopped diagnostics allowlist; the database-custody input was false and no raw native database tree was
uploaded. The failure allowlist excludes fresh fixture TLS key/certificate files;
the tracked-source archive is a repository snapshot, outside that fresh-custody
claim. This packet publishes no raw
custody files or raw RPC frames. Their exact member hashes remain in the explicit
projection manifest. The raw source/archive artifact must not be re-published as
an evidence shortcut.

The included full CI job logs were checked for generated PEM/key or token bodies;
paths and fixture application values remain. The original 85,450,921-byte log is
omitted with its SHA256, except exact lines 633560–633610. Public verification
checks retained projections/logs/source copies. It does **not** independently
re-read omitted original journals, raw stopped files, or raw frames. Optional ZIP
verification reproduces the whitelist and hashes without opening any database.

## Reproduce the evidence checks

```sh
python3 docs/evidence/native-cli-four-thread-diagnostic-2026-11-13/verify.py --self-test --repo /path/to/bicdb
python3 docs/evidence/native-cli-four-thread-diagnostic-2026-11-13/verify.py --artifact /path/to/focused-cli-03454-four-thread-artifact14086103789.zip
```

The second command requires the exact separately retained artifact. It does no
recovery, checkpoint, native API opening, RPC or grant construction. The source
index pins critical exact source blobs, the workflow-only diff, run/job metadata
and both raw diagnostic job logs. The original pre-dispatch forensic report is
retained as frozen, including its then-current “not dispatched yet” scope.

`issues.json` records the smallest next deterministic tests. No rerun or runtime
fix belongs to this packet. Independent-process startup and first-ACK availability
remain unresolved gates. Second populated range, cold DATA/Serving recovery,
controller/RPC relocation, fault-safe convergence, VM fault handling, sustained
capacity gain and general cross-range PostgreSQL SQL remain unqualified or not
tested here. No physical-machine RPO/RTO or p50/p95/p99 claim follows.
