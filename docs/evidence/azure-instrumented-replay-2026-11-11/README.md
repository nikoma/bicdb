# Instrumented Azure replay: 1.0.463-beta

Neither current-runtime attempt reached the fifth-node join. This evidence preserves two different failures: the 16-worker attempt exhausted its bounded observer queue; the subsequent 4-worker attempt captured unsuccessful database writes after a graceful restart. Neither is a successful scale-out experiment or production qualification.

Runtime source `d49a391b3c6157f90f29e9f2f5214143c60f9f4b`, version `1.0.463-beta`, was pinned before these runs. Later source fixes and version bumps are separate artifacts. The earlier `1.0.461-beta` experiment remains unchanged. [Artifact identities](artifact-identities.json) distinguish the actual engine/core/worker from the unchanged `b05385d4` event-policy reference.

| Artifact | SHA-256 |
| --- | --- |
| Optimized engine, stripped debug information | `840f0124f27a1a7695c4b2b0c72b05a262456802af65e063cff12242559e2f7d` |
| Member-side single-RPC worker | `b8cc2607220e32f705a23fd6b7d4f41c22298cd1b0ec92cc8e83847783ef8d2c` |
| Linked optimized core library | `dc714b001a0fe8758473c2c4ef80158474d7426e16630926edb65b6cc35f0356` |
| External controller | `a1ab418d1031ec409ce108aeb547cf5a2ceebff46f41811d991f7182259f66fb` |

Five separate Azure VMs were provisioned, with four database members running. RF3, 64 virtual ranges, `server_paged`, 256 MiB buffer pools, configured fsync, and 100,000,000,000 declared capacity bytes per member were used. Dedicated 128 GiB data disks used ext4 and no host caching. Distinct VM identities do not establish independent physical hosts. [Infrastructure, actual models, metrics and verified deletion](../azure-instrumented-repro-infrastructure-2026-11-11/README.md) are separate evidence. The fifth VM was staged but never joined: its final database, process record, database configuration and server log were absent.

## Recorded results

| Phase | Logical terminal observations | Available telemetry | Result |
| --- | --- | --- | --- |
| Initial cloud smoke, 4 workers | 319 ACKs, 315 exact Gets | Complete | Passed narrow smoke |
| Smoke reconciliation | 319/319 exact | Complete | Passed |
| Requested 900-second baseline, 16 workers | 2,939 ACKs, 2,923 exact Gets | One observer coverage gap | Incomplete; fenced after 715.142 seconds; no growth |
| Baseline read-only reconciliation | 2,939/2,939 exact | Complete | Every baseline ACK matched before stop |
| Explicit two-target fixture after restart | 2 ACKs, 2 exact Gets | Complete | Passed fixture |
| Requested 60-second steady-four phase, 4 workers | 1 ACK, 8 uncertain writes | Complete | Database write availability failed; no growth |
| Dedicated two-target read-only check | 2/2 exact | Complete | Both fixture values matched |
| Old baseline plus new steady-four write reconciliation | 2,947 exact, 1 absent of 2,948 | Complete | All 2,940 acknowledged targets matched; absent target was an uncertain write |
| Previous smoke after restart | 319/319 exact | Complete | Passed |

Across the latest separate checks, all **3,261 known acknowledged keys** returned the expected values. Of these, **3,258 were acknowledged before the graceful restart** (2,939 baseline+319 smoke) and crossed that restart; **3 new ACK keys were created after restart** (2 fixture+1 light workload) and were checked subsequently. The new three were not exercised across another restart. Of eight uncertain writes, seven returned the expected value and one returned `None`. Event Get is eventual/local-applied-on-named-leader; the absence does not establish rollback. A timeout never establishes that a commit failed. This observation is not a power-loss RPO result, a strong-read proof, or an atomic SQL transaction guarantee.

The first baseline contained 239 application transport RPC timeouts, 46 returned application errors, and 23 observer transport timeouts. Its 237 acknowledged logical writes with earlier unknown-response RPC history remain identifiable. Successful logical outcomes do not erase those attempts. Its queue-full observation gap is retained, the incomplete gate stopped new logical dispatch, and strict validation failed as expected. No fifth-node join was attempted.

The second steady-four phase had complete available client telemetry and no observer gap. Its eight unsuccessful writes remain explicitly uncertain rather than failed/rolled-back commits. Its strict trace validation passed while its workload result failed. The unchanged guard stopped before node five. The two targets were a **new explicit fixture** using the historical A/B keys and semantic payload, not a reproduction of the unknown original failed attempt order. Their automatic routes were A/r41 and B/r11. Semantic payload SHA-256 `9071a625c5fb2f7851c155ce4b013d80706162a8f712960ffe3e778dba194354` is distinct from a physical full-record or command checksum.

## Recovery evidence and limits

Full server logs, all-member status histories and live catalog captures were retained before shutdown. Original archived log prefixes match exactly; byte offsets separate appended post-restart errors. Server stderr has no per-line wall-clock timestamp, so cross-node chronological ordering and a timestamp-exact explanation of an individual client rejection are unavailable.

Post-restart messages include schema fences requiring active fingerprint `6b6a92f6380df10862dd1021b0bd5d38c2e1767c5cbeeaa9c41a698e9beaf5af` while local verification was stale/missing; leader recovery reaching only 1/2 voters; r2 tail recovery at index 49 reaching only 1/2 voters; and epochs changing before schema validation. For r18 the logs include an epoch-15 change and a schema-fence error, while the client earlier received an epoch-14 recovery-incomplete rejection. These are correlated observations, not proof that one selected message caused the request failure.

The independent live r18 catalogs showed n0/n1/n2 retained-command epoch 14 with last/resolved index 59 and 59 Applied entries; n3 retained epoch 9 with last/resolved index 58 and 58 Applied entries. Entry 59 included one uncertain write, demonstrating why uncertain does not mean aborted. Retained-command epoch can legitimately lag routing epoch. The full all-range analysis checked 10,829 command checksums and all four catalog checksums without a mismatch. The original offline extractor incorrectly indexed the topology token-start map with a RangeID, producing212 null route projections; its executed bytes and original output are preserved. A separate [corrected derivative](light-attempt/prestop-forensics/corrected-range-routing.json) joins descriptors by `id`, rejects missing/duplicate IDs and includes source digests/copy intervals. All212 rows join. The independently copied later r18 route was epoch17/leadern0/votersn0,n1,n2 on allfour captures, while the retained command epochs remained as above. This is a later independent file mapping, not request-time routing or a cause of the earlier epoch14/15 errors. Captures are not a coherent quorum snapshot, a page/index verification, or independent verification of voter signatures. The catalog's Applied/quorum-committed markers are local retained states; they are not a separately verified signed quorum certificate.

The sampled metadata health gate reported live members, RF3 and zero unavailable/under-replicated/active/failed ranges before application admission. Application writes subsequently received recovery-incomplete, stale-leader and transaction-conflict errors and transport timeouts. Metadata placement/liveness health therefore did not establish write readiness for this exact configuration. [Sampled restart-through-stop move histories](light-attempt/restart-through-stop-move-history.jsonl.gz), per-move cursor/byte/watermark/error fields, node terms/commit positions and routing epochs are preserved in the complete snapshot streams. Snapshots sample transitions; they do not observe every internal retry or prove continuous instantaneous state between polls.

## Latency and measurement scope

The incomplete first baseline's 2,939 acknowledged Append logical durations had p50/p95/p99 **2.977/7.566/10.534 seconds**. Its 2,923 exact Get durations had **0.404/0.827/1.220 seconds**. These include the controller's retry/journal path and are successful-terminal sample groups, not a clean service benchmark. The light phase's single ACK took22.587 seconds; its eight uncertain writes had p50/p95/p99 34.353/34.920/34.920 seconds (n=8). These are failure diagnostics, not SLO estimates. Every individual unsuccessful logical duration and available individual RPC duration is also retained. [Offline verification results](verification-results.json) retain the exact nanosecond distributions. The verifier independently recomputes logical percentiles and reports RPC distributions separately by purpose, operation and returned/transport status using nearest-rank percentiles.

Parent controller-host image characterizations wrote approximately 6.46 GB during Unix 1794388626.51–1794388659.79 and 6.49 GB during 1794388727.25–1794388758.52, overlapping the first baseline. Local Cargo/unit work also overlapped. These are controller I/O/CPU confounds. Heavy local work was drained before the light trial, while shared services remained. Controller and member CPU/I/O/pressure samples are diagnostic shared-host counters. No bottleneck cause or isolated latency effect is established.

The controller fsyncs each trace record and each counted ACK outside the cluster. Its Python policy replica uses member-side SSH/mTLS workers; TCP originates on those members, with application gateway n0 and authorized leader-local observer workers. It is neither the stock Rust CLI workload nor an unchanged PostgreSQL application, nor a dedicated independent-network client. Member TLS private keys were not exported by the bridge. Whole-database private forensic archives can contain task-generated mesh identity keys; restricted provider request bodies can contain deleted-test-host identity material.

Controller and member Unix timestamps are separate, uncalibrated clock domains. Member wire durations use monotonic clocks. Logical elapsed durations end before their own terminal record's fsync. Observer RPCs have separate timing and load. Full completed wire envelopes/errors are retained privately; partial network-frame bytes are not retained. Server-internal consensus/relocation retries remain unobserved by this client. All available application/event/discovery retries made by this policy are recorded.

## Reproduction and custody

Run the offline verifier from the repository:

```sh
python3 scripts/verify-azure-instrumented-replay-evidence.py
# Optional, only where the restricted durable originals are available:
python3 scripts/verify-azure-instrumented-replay-evidence.py --private-dir /root/.local/state/bicdb/ha-scaling-2026-11-11/private/instrumented-463-8186dd18
python3 -m unittest discover -s scripts/platform/instrumented-events -p test_project_evidence.py -v
python3 -m unittest discover -s scripts -p test_verify_azure_instrumented_replay_evidence.py -v
python3 -m unittest discover -s scripts/platform/instrumented-events -p test_correct_range_routing.py -v
```

`records.jsonl.gz` files retain **every source journal record**, without a 20-sample cap. Endpoint/task paths use stable aliases. Payload bytes, full raw bodies and selected repeated history fields are explicitly represented by hashes. Complete transformed error messages, expected/returned value hashes, stable IDs, all logical/RPC timings, routing and epoch/generation fields, unknown outcomes, retries and progress probes remain. These public projections are not equivalent verbatim wire captures, and source digest linkage cannot reconstruct omitted bodies. The private original journals remain authoritative. Public verification independently links ACK records to prior same-operation Appended responses, durable receipts and terminals; checks recorded request/caller/pin identities, every observer trigger/completion/gap and monotonic fence, explicit target sets, retained source bytes and numerical claims. The optional private comparison additionally validates original chains/full body hashes with the pinned checker, compares all107,961 canonical transformed rows, disables bytecode and checks unchanged private/public file sets, modes and sizes. Its16 verifier boundary tests and5 projection tests are retained here. Neither is external packet-origin authenticity or production attestation.

[Local readiness](local-readiness-receipt.json) records 32 passed instrumentation fixtures against the actual 463 engine/core, including 16 application channels and the 21-session ceiling. Those synthetic TLS fixtures are not HA qualification. Actual cloud startup verified all 16 n0 application workers plus current-leader observers before the first long window. The operational `run.executed.py` staging-v1 attempt stopped before init/server/traffic; its original bytes and abort receipt remain. The superseded client-v2 reconciliation-cap regression and superseded clients remain private; they were not used for physical traffic.

Executed sources are frozen in `executed/`. `host-tools.executed.py` supplied Host/TLS/storage helpers; its older native workload `main()` was not invoked. The actual operational runners were `run-v2.executed.py` (SHA `3346c4ab23b64b708e49c9b613cb61554e9ec0f4d920821724d3b5be217ffb7e`) and `light-run.executed.py` (SHA `cea96ba34d2de81814a6a395cdcf0a869c9203ff0e26bc1391eacdc7e245d34b`). The planned 600-second steady-five window was exploratory and never ran. No engine implementation was patched during either attempt.

[Durable private custody](durable-private-custody.json) identifies 1,401 files/933,356,662 bytes at `/root/.local/state/bicdb/ha-scaling-2026-11-11/private/instrumented-463-8186dd18`, with a canonical hash manifest. Nine original raw traces/bodies, eight original/light cold archives, complete forensic/config/log/history files and 900 allowlisted provider files were copied with source/destination parity rechecked after capture stopped. Directories are 0700/files0600. All controller/SSH/sidecar processes ended; all five VM task-process inventories were empty before teardown. The fifth database never existed. All 23 owned Azure resources were verified absent at **09:49:21 UTC**. No production resources were touched.

Twenty-one private files are flagged for possible task identity material: eight opaque DB archives and thirteen restricted provider files. Global Azure credential caches, explicit SSH private-key files, generated TLS private-key files, binaries and build caches are excluded from this curated copy. Runtime binaries/core libraries have separate custody. The canonical manifest is `9351cacbfc489c14b1af4635a4dd5c4bbbfd0c74f98c2b7c2d5ae318f5fa1fc0`. It excludes the custody copier's own actively opened output logs. Historical missing native failures from the old 461 trial remain missing; these new traces do not invent them.

## Open blockers and smallest next checks

1. Reproduce graceful restart followed by write readiness using the preserved schema/quorum/tail/epoch states on a pinned build. Assert readiness per current range leader before treating placement metrics as a write-health gate. Preserve authority and fail closed on ambiguity; do not disable schema, metadata or range quorum checks to obtain progress.
2. Exercise the error-triggered observer queue under the actual sustained timeout/retry rate. Keep raw application RPC coverage separate from watermark-probe completeness. Any capture gap must remain explicit; changing capture policy requires a separate frozen client and boundary tests.
3. After independent source fixes/checks, repeat a small four-to-five add under a bounded workload with an external ACK/uncertain ledger, actual moved-user-record proof, all-member convergence and a separately coordinated fault. No new VM resources are implied by this plan.
4. General cross-range SQL, durable multi-range business transactions, ordinary PostgreSQL routing and production qualification remain separate work. Point-event success would not establish them.

There is no current-463 scale-out, throughput-gain, fault-safe rebalance, RPO/RTO, transparent SQL or production qualification claim in this bundle.
