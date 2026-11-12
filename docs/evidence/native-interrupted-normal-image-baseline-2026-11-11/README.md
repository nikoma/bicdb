# Interrupted Normal image repair: unsupported cold resume

Exact private source `8707ffa42fc0b20ff30e0dd5ebd8989e20207a0d` passed **one characterization test**, whose exercised recovery scenario ended **`FAILED_UNSUPPORTED_COLD_RESUME`**. The passing test records the refusal and its preserved state; it does not establish recovery success. No native runtime fix, activation, StrongGet, Azure cause, acknowledged data loss, physical qualification, deployment or production readiness is claimed.

The source is the actual composed private engine (`1.0.471-beta`), including native catalogue fsync guards, local read observations, normal Apply and sparse-history integration. The retained patch reconstructs it from public `b0e2498842be7b01e355bb8da1cc249ff16be056` / 471. It does not qualify a later public rebase, source version, binary release or CI run.

## Preserved attempts

| Exact source | Attempt | Outcome | Whole execution |
|---|---|---|---:|
| `5c65eb9b` | original | Compiler E0308: assertion compared `Option<Arc<Record>>` with `Option<Record>`; no scenario assertions ran. | 20.05 s |
| `8707ffa4` | v2 | Own build stopped by disk guard before unit execution; no test result. Minimum free disk 8,541,470,720 B crossed the strict 8 GiB floor. | 47.05 s |
| `8707ffa4` | v3 | Characterization **PASS 1**, scenario **unsupported**. All scenario assertions reached; unit execution 5.00 s. | 83.05 s |

The correction between 5c65 and 8707 changes only the test's borrowed record comparison; it preserves the original value assertion and changes no engine guard. The resource interruption is retained separately from compiler failure and completed test execution. V3 compiled the intended `/tmp/bicdb-native-sparse-current-certification` core path in about 77 seconds, used one job/core opt-level 0/debug 0/incremental disabled/two test threads/`tls-replication`, and retained at least 20,884,803,584 B available RAM with no pause or guard stop.

Original stdout, stderr, resource samples, inputs/results, both exact source states and the executed resource monitor are retained. The v3 shared mutable test executable was 136,517,008 B, SHA256 `1e054519a4e62110b9770338eae6fe4cc2e7f107983936896ea78d1032936aa8`, at `/tmp/bicdb-azure-artifact-target/release/deps/bicdb_core-b0702f49611c59ed`. Its recorded modification time falls inside v3. This is a mutable test artifact, not a release binary or immutable executable archive.

## Actual schedule and first cold boundary

The in-process RF3 fixture obtains one genuine origin ACK, `interrupted-normal-ack-1`, with target n1 unavailable and delivery of Apply1 to n2 dropped. A second original operation, `interrupted-normal-uncertain-2`, returns `CommitOutcomeUnknown` after admission and before its original application WAL. Actual current-session sparse certification and native replay resolve command 2 on source n0. This is resolution of an uncertain original operation, **not a second new ACK**. The fsynced external controller ledger still contains exactly one acknowledged key/hash.

A genuine current-data quorum/source-history cut authorizes Normal current-voter image repair of n1 through source R2/H2. The target accepts the durable intent and staged history, then completes clearing before any row copy. It retains namespace identity, birth, current-session sparse decision/marker at index 2, and image attempt/stage bindings. Target raw last/resolved remain 0, and native base/applied are **unknown (`None`)**, with invalidation and an incomplete-image diagnostic. Its observed local DB sequence 0 is not a native R0 proof.

All original database owners really close: the test waits for every weak DB reference to expire. It copies the stopped tree and performs strict existing-state reopen, without creating new genesis. Target birth/hash and local namespace UID remain unchanged; its DB-open incarnation changes. Source n0 likewise retains birth/UID/R2/history/native receipt while its open identity changes. The copied source reconciles the exact original acknowledged value/hash and the resolved uncertain command's final mutations. The original three range catalogues remain byte-for-byte unchanged.

After reopen the target still has the accepted image intent and no published image base. Actual fresh metadata advances from term/read-index 1/1 to 2/2. Actual current data-fence collection obtains only **1 of the required 2 observations**: source n0 responds, target n1 refuses while durable native image installation is incomplete, and n2 remains unavailable in the fixture's data transport. A new current source cut cannot be constructed. The restarted target runtime also reports `no retained recovery source freeze`; the old process's ephemeral ownership is gone. NativeReady remains refused. The test does not deserialize old authority, rewrite counters, clear the intent, or pretend the target is ready.

This is a concrete cold-resume availability gap at the accepted-but-unfinished image boundary. It is not a demonstrated loss of the one acknowledged value, which remains present on the source. The fixture uses actual in-process protocol/storage/native producers and strict copied reopen; enabling the TLS feature does not turn this case into an authenticated network or physical fault test. It provides no application throughput, RTO, automatic election, host startup or scale-out qualification.

## Projections, custody and remaining work

`projections/controller-acknowledged.jsonl` is the exact fsynced one-line controller ledger. `ack-record.canonical.json` is the exact canonical record value derived from the original retained command; its SHA256 matches that ledger and the source's cold-read assertion. This projection is not an additional database read performed during packaging. The original source's two retained command entries, the pre-close state/cut/intent/history identifiers, and the final report are retained. Command 2 remains explicitly uncertain-at-origin, later resolved, and absent from the ACK ledger. The report embedded in v3 stderr exactly matches the standalone report.

Both the original and copied stopped trees are in restricted private custody: **107 files / 1,055,754 B**, manifest SHA256 `6f958adcf53821fbea5d7159b22c1a2695fbf7956b9d7bf8c0a2c1a9043e111a`. Source/destination hashes matched, directories/files were fsynced with 0700/0600 modes, and no database was opened for custody copying. Generated database/mesh keys remain private; no global credentials, compiler cache or binary is included in that custody count. Public projections contain test values and opaque identifiers, not those raw keys.

A possible next seam is a separately durable, validated accepted-cut receipt plus a narrowly scoped fresh Installing-fence observation for exact same-attempt cold continuation. Such a protocol must preserve current metadata/member/nonce/birth/UID/history/floor/refusal checks and must not derive authority from the raw intent, stage labels or a caller's counters. That repair is **not implemented or tested here**. This receipt freezes the original unsupported boundary for comparison with a future fix.

Run `python3 verify.py --repo /root/bicdb` to verify the retained claims and reconstruct both engine states using an isolated Git index/object directory. Optional `--private-dir /root/.local/state/bicdb/ha-scaling-2026-11-11/private/native-interrupted-normal-image-8707ffa4` checks the retained private exact file set, hashes and modes without opening a database. `PYTHONDONTWRITEBYTECODE=1 python3 test_verify.py` runs six semantic tamper cases. The verifier does not rebuild Rust, independently attest the original execution, or change the scenario result into success.
