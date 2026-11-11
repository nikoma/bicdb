This is **55 distinct passing local focused controls** on experimental source `5d70669eec9fcea8f9963894ef73d243ef9d208f`, carrying package version **1.0.471-beta**. It is a private assembled source, not evidence that published 1.0.471 has these changes or that a release, deployment or production qualification exists.

The source compiles the new recipient-local historical recovery RPC collector and the owned recovery-to-installer lease. **These two new runtime paths were not exercised through actual participant RPCs or a recovery-owned target install in this bundle.** Their positive integration fixtures remain required. Passing compilation and adjacent controls must not be represented as that integration result.

| Focused group | Passing tests | Actual scope |
| --- | ---: | --- |
| Runtime | 1 | Actual handoff owner; shared context cap, concurrent readonly admission, retained background reservation, immediate refusal under an actual held DB writer |
| Historical native Apply | 2 | Actual local current-marker quorum excluding data leader; actual post-WAL fault, checkpoint cut, complete owner close/reopen, new physical WAL attempt retaining original command |
| Image installer | 14 | Actual native row/history copy, namespace identity, partial/reopen/late chunk refusal, local target visibility, actual catalogue adoption/finalization, R=1,025 after raw command compaction |
| Native size boundary | 1 | Complete native entry size boundary; legacy command construction remains independent |
| Metadata provider | 16 | Fresh quorum, identity/config/deadline/role limits; includes three actual loopback mTLS cases |
| Mutation gate | 9 | Actual controlled-writer drain, source read pin and owned freeze lifetime |
| Recovery state | 5 | Actual durable refusal/tail preservation, retired nonce, rename/directory-sync uncertainty and per-range catalogue identity |
| Current recovery quorum | 7 | Actual local metadata/native birth/store fences and source-history comparison controls |

Every invocation used the same clean source and executable. The initial successful invocation forced compilation from the recorded worktree; raw stderr contains that path. Rust was `1.96.0`, one Cargo job, incremental disabled, two test threads, release dependencies and **core opt-level 0/debug 0**. This is not an optimized performance experiment. The first build/test took 68.05 seconds, with minimum sampled available RAM 22,575,480,832 bytes. The complete 14-test installer invocation took 241.09 seconds; its default libtest output does not assign individual durations. No resource-guard pause or interruption occurred. Raw RAM/swap/pressure/disk samples and compiler warnings are retained.

The first attempted compile failed with two sibling-module privacy errors in the new read-only native journal observations. Its raw failure is retained in `prior-compile/`. The correction changes only the guard type/method visibility to the enclosing DB module; the tests above ran on that corrected committed tree. No failed invocation is counted as passing.

The historical tests independently construct genuine recipient-local role/fence/history/current-certification observations using actual temporary services and registered in-process transports. They are not proof of the newly added authenticated receiver collector. The existing metadata-provider mTLS cases do not expand the historical tests' transport scope. There was no VM/process power fault, node join, sustained scale-out measurement or hardware RPO/RTO measurement here. Physical recovery, complete suffix repair, native serving activation, queue/audit reconstruction and StrongGet remain separate gates.

`engine.patch.gz` reconstructs the complete engine delta from public source `b0e24988` (full base identifier in `claims.json`); `source-files.json` records the 150 core source files from the clean tested commit. The previously captured first-failed-compile source manifest is retained separately. The executed guard runner and full commands are included. The patch is a reproducibility artifact, not an instruction to apply experimental storage formats to production.

Run `python3 verify.py` to check exact file coverage, hashes, source identity, raw result summaries, resource guards and the 55 unique test identities. Add `--repo /path/to/bicdb` for offline reconstruction from that exact public Git object and byte-for-byte comparison of all captured core source files in a temporary directory. The optional check never edits the supplied repository. `PYTHONDONTWRITEBYTECODE=1 python3 test_verify.py` runs four evidence-verifier cases. These commands verify retained evidence; they do not rerun Rust or independently attest its execution.
