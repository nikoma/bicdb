# Authenticated native completion participants — component evidence

**66 distinct Rust tests passed (67 executions) on `cad84207e8a4329db47449e43f658325d8f8cf55`.** This is a private, unmerged engine assembly whose package identity is `1.0.471-beta`, not a released binary or qualification of newer public `main`. The complete patch from public `1.0.471-beta` commit `b0e2498842be7b01e355bb8da1cc249ff16be056` and every core source-file hash are retained here.

The new incoming authenticated `CompleteNativeRecovery` request carries only scope, challenge and finite budget. Each recipient independently collects a closed native-ready quorum, obtains fresh metadata authority after that collection, and persists actual native-WAL configuration followed by catalogue completion. Readonly `ObserveNativeCompletion` uses a separate observer witness. Both preserve the owned physical pin and range freeze. **Completion remains `NativeCompletedFrozen`: it does not activate serving, admit ordinary writes or grant strong reads.** A decoded reply cannot construct the sealed observation or a quorum.

## Actual cases and limits

Three new real TLS cases exercise sequential completion of all three replicas, idempotent native-serving hashes, refusal of a wrong authenticated completion actor, readonly observation by a current voter, and cancellation of the real client socket after actual persistence/proof success but before the server sends its reply. A separately fresh readonly request resolves that unknown client outcome. Exact original records and native range index 1 remain unchanged; all ordinary mutation gates remain closed.

The TLS fixture starts real native databases and services. Its metadata leader 2, data leader 1, epoch 2 and physical epoch-1 lineage are declared preconditions; this is not an automatic-election or host-power-loss experiment. All three completion endpoints are alive. Metadata and participant evidence cross actual authenticated TLS; no synthetic successful participant response supplies authority.

The loss boundary is after the complete producer has validated and refreshed its retained proof. If persistence succeeds but proof refresh itself fails or expires, existing fresh `PrepareNativeReady` re-fsync/re-attestation remains the repair path; readonly status cannot clear uncertainty or manufacture a held proof.

Two floor controls cover the [original failed serving/image witness](../native-serving-image-baseline-2026-11-11/README.md). The coherent fix preserves historical serving A as an epoch/history floor after actual image B replaces its physical base. A historical-only record does not grant B ordinary writes. A new actual closed quorum produces B's grant while retaining freeze/refusal; immutable old/new native records remain. An actual native floor at epoch 2 rejects an authenticated copied logical rollback to epoch 1 and same-epoch placement drift. The rollback case is deliberately constructed local negative evidence, not an Azure cause or permission to roll metadata back.

The successful serving/image witness deletes its temporary databases on success. Retained GREEN evidence consists of assertions, logs and exact source; **there is no post-GREEN database archive**. The original failed baseline and its private custody are separate and unchanged.

Ready TLS controls check actual recipient metadata context/term/read-index provenance, duplicate refusal and an honest clean R0 minority alongside an R1 majority. The higher-observed-Prepared-index veto is compiled, but no separately constructed higher-minority/lower-majority negative fixture is claimed.

## Validation scope

| Group | Passing executions |
| --- | ---: |
| Incoming completion TLS | 3 |
| Ready TLS | 3 |
| Serving/image witness and floor negative | 1 + 1 |
| Local physical readiness/completed-frozen | 19 |
| Current-voter image repair | 4 |
| Historical participant/provider mTLS | 5 |
| Native origin, mutation gate, recovery state | 7 + 9 + 5 |
| Actual fenced quorum, owned image lease | 7 + 3 |

The serving/image witness also appears in the four current-repair tests: 67 executions therefore represent 66 unique test IDs. No full core suite, SQL/application acceptance, new application ACK after recovery, NativeActivated, StrongGet, audit/outbox recovery, physical HA, horizontal-capacity qualification, CI or deployment is established here.

All commands use default core features (including TLS), `--release`, core `opt-level=0/debug=0`, jobs 1, incremental 0 and two test threads. The first command forced a fresh compilation from the intended worktree, avoiding shared-target stale executable reuse. Its test phase took 15.00 seconds; subsequent groups reuse that exact source/executable. The first-start to final-end interval was about 251.44 seconds. Resource samples show minimum available RAM **21,899,538,432 bytes**, no guard pause/stop, and disk above the 8 GiB floor. The guard's complete source is retained. Cancellation/early-quorum broken-pipe logs are preserved, not reported as zero transport errors.

The captured test executable SHA256 is `21619959b59643ee14be56a0dd5ebd2c972408d71c534470407997f99d384f2e` (129,759,024 bytes). Its shared-target path is mutable; this receipt records its identity when tested and does not distribute a frozen release binary.

## Offline checks

```bash
python3 docs/evidence/native-completion-participants-2026-11-11/verify.py
python3 docs/evidence/native-completion-participants-2026-11-11/verify.py --repo /path/to/bicdb
python3 docs/evidence/native-completion-participants-2026-11-11/test_verify.py
```

The verifier checks exact manifest coverage, revision-scoped commands/results, raw test IDs/counts, forced compiler path, resource bounds and source patch. Optional `--repo` reconstructs every core source byte from the public base plus retained patch in a temporary directory; it does not compile or edit the repository. Four negative integrity cases reject manifest omissions, inflated counts, a failed result and a wrong compiler worktree even after rehashing. These checks establish custody and internal consistency of retained evidence, not independent production or packet-origin attestation.
