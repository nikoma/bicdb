# Authenticated native read observations and unadmitted candidates

Exact private source `daa77b42cd8f71a212de06596a249bfbce4d36e9` passed **15 focused tests**: six new read RPC/candidate cases and nine local read-observation controls. Earlier private source `453b86822bdc1a216e57b03d251dd42b573ad746` independently passed the first five RPC/candidate cases. These are 20 executions and 15 distinct tests; the earlier five do not increase the final source's count. Both runs compiled the intended worktree with a forced core source timestamp; neither paused or hit a resource guard.

The result is an **explicitly unadmitted factual candidate**. It cannot authorize a successful read, snapshot, absence, StrongGet, or old-voter disk continuation. Host enrollment, an actual snapshot covering target-local visibility, final fresh metadata, gateway/host integration, and broader qualification remain open. Legacy Get is unchanged. There is no incoming candidate-acquisition or StrongGet endpoint; the collector is a trusted local runtime method.

## Exact sources and executions

The retained engine patch reconstructs the actual source from public `b0e2498842be7b01e355bb8da1cc249ff16be056` / `1.0.471-beta`. This is the private engine source actually compiled, not a claim that a later public rebase, version, CI run, release binary or deployment was tested. The patch includes the preceding native catalogue durability guards (`6141729b` / `574ea560`); this cohort does not rerun their six dedicated configuration cases.

| Source | Selector | Result | Whole run | Unit execution | Minimum available RAM |
|---|---|---|---:|---:|---:|
| `453b8682` | `native_read_rpc_` | 5 passed | 109.08 s | 27.46 s | 20,829,548,544 B |
| `daa77b42` | `native_read_` | 15 passed | 119.09 s | 45.64 s | 20,617,637,888 B |

Both used release core opt-level 0/debug 0, one Cargo job, incremental disabled, two test threads, and `tls-replication`. Original stdout, compressed stderr/resources, exact command/environment/result and full source hashes are retained. Expected closed/canceled test connections produced broken-pipe server diagnostics; those remain verbatim. No initial compiler/runtime failure occurred in this RPC cohort. Earlier local-foundation compile and key-routing fixture failures remain in [their independent receipt](../native-read-observation-foundation-2026-11-11/README.md).

The shared test executable path was `/tmp/bicdb-azure-artifact-target/release/deps/bicdb_core-b0702f49611c59ed`. Its recorded SHA256/size were `7eca5958d8fc191597f3c8f0d04b48ea3e869bde75dbca70901c779d6ac98425` / 136,206,384 B at source 453, and `f7cd6056cc638b60b15e526cd584028412e213457146ee83d2d08d1e014e36cf` / 136,252,624 B at source daa. The shared path is mutable, and these are test executables, not release binaries or an immutable binary archive.

## Exercised behavior

The fixture creates genuine native RF3 origin ACKs through actual services, then performs actual mTLS historical recovery, prepare/completion, and local activation. Epoch 2/data leader n1 and metadata leader n2 are explicit fixture preconditions; this is not automatic election, physical failover, or a host deployment. The new path subsequently obtains an actual generic fresh metadata observation and actual mTLS read observations. Each receiver calls its same service's current native producer. A single-use actual-service bundle binds local proof to the original request; a verified transport wrapper binds remote facts to actual peer, destination, incarnation, pins, challenge, business request and requested metadata context. Requested metadata IDs are echoed request labels, not a claim that a receiver acquired metadata authority.

The six new cases exercise:

- actual fresh MQ plus local/remote matching native facts, returning only an unadmitted candidate;
- rejection of a genuine old response under a new challenge, deliberate challenge reuse under a new business/MQ context, wrong authenticated actor, wrong incarnation/pins, and remote-loopback substitution for the actual local proof;
- actual metadata acquisition followed by two fixture listener shutdowns, leaving no matching data majority;
- caller cancellation while a bounded test barrier holds the real local producer's return, retaining context capacity until that worker exits, with service/DB latches free;
- a genuine local/matching-peer majority returning while a minority worker is held after its actual TLS response, retaining its context until release;
- a healthy peer's genuine response deliberately held beyond its attempt slice, a retry within the original MQ/caller deadline, and expiration at the original deadline.

The final case proves injected attempt expiration and eventual matching candidate. It did not yet instrument the number of successful peer RPCs. Separate **test-only, uncompiled** follow-up `8ce4938c0ba44135f4d62745bdd414aa13e3a6c5` adds an explicit actual n2 successful-call count of at least two. Its patch and reconstructed source hashes are retained, but it contributes no passing test claim. The source implementation records expired authenticated points only as bounded constraints, never votes; no budget is renewed. Higher/genesis/same-index history conflicts veto, and lower points are compared with actual local native history. This cohort has no independent dynamic lower-peer-history or higher-minority fixture, so those rules retain source-review/compile scope.

The nine foundation controls additionally exercise current R2/local visibility after an immutable R1 activation anchor, lagging raw-CQ-only refusal, actual raw prepared/committed/applied versus physical state, reopen/lifetime/store replacement, scope/incarnation/pins, expiry/cancellation/health, and mutation-stamp invalidation/ABA. They do not upgrade the new candidate to permission.

## Boundaries and custody

Collectors use at most four workers and two shared exclusive collector contexts; readonly receivers use the same four-context budget. Local probing reuses the collector operation without a recursive reservation. Every actual local/remote/final-history IO job retains its operation through actual drain, including cancellation or partial spawn. Read observations allocate no recovery nonce, freeze, mutation permit or snapshot. Known returned conflicts are drained before early majority closure. Failed remote attempts are bounded and retain the original round budget. Schema/native/catalogue IO can be uncancellable: the five-second TTL rejects late verdicts but is **not** a hard OS-IO latency bound. The test barriers are controlled pauses after actual producers, not demonstrations of physical disk stalls.

Native birth facts are observations. A recreated empty store with an old voter identity must not gain continuity merely from R0, schema, bootstrap or current MQ membership. Actual quorum-committed birth enrollment and current-open host acceptance remain mandatory future gates. Neither a decoded facts DTO nor a candidate grants them.

Only the explicitly retained successful R2 foundation root from the final run was curated: **47 files / 824,217 B**, canonical private manifest SHA256 `9fb6d0825bf61bef1ed5835b65df114afdb57f0760799af2e016d952691905f7`. Source/destination hashes matched, private directories/files were fsynced and restricted to 0700/0600, and databases were never reopened for custody. Raw generated database keys stay private. Other successful temporary fixture databases were not archived. ACK/value results are application assertions; no external ACK ledger or packet archive was produced. This verifier checks retained integrity/source reconstruction, not independent provenance that packets originated on a particular host.

Run `python3 verify.py --repo /root/bicdb` to reconstruct all retained source states using an isolated Git index/object directory. It does not rebuild or rerun Rust. `PYTHONDONTWRITEBYTECODE=1 python3 test_verify.py` exercises six semantic tamper cases.
