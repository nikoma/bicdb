# Native history staging component evidence

Status: **component tested; composed image installation/recovery remains incomplete**.
This is private candidate source based on assembled `0ce7623f` (source version
1.0.466-beta), with the logical-history producer and controlled/read-pin changes.
The exact tested core source manifest is
`197a021d259df00de69394f8baf13f84f18ccb250a8587721f7037f852328057`.
Each receipt includes every core source path/hash and the actual command.

The 37 distinct passing local tests include three new staging cases. They create
actual fresh native databases/schema/genesis, and seed the R=1 case through the
ordinary RF1 coordinator, actual transaction/WAL callback and certificate path
before installing the permanent handoff promise. Actual source history is
exported from index zero and written into a new inactive target-local generation.
The stage checks contiguous links, bound source identity, native lower bounds and
all actual retained catalog CQ/Applied obligations. A separate bounded reread of
the stored spine is required before the opaque compatibility proof exists.

The new cases verify empty and nonempty history staging, identical begin/frame
retry availability, corrupt and wrong-attempt frame refusal, and refusal after a
missing stored inactive-spine page. The missing-page case deliberately deletes
one inactive metadata key; it is a corruption injection, not a crash simulation.
No data rows are copied or cleared by staging. Existing native factory, local
origin/pending/WAL, actual R0 target image, controlled callback drain and source
configuration controls also pass on this same source. The R0 target tests have an
empty certified source and do not demonstrate copying populated ranges.

Genesis now scans under a source read pin and publishes its actual native
receipt using a counted controlled writer after dropping the pin under the
exclusive database guard. Export under a live SourceCapture uses that capture's
existing exclusive pin. A private DB-open-bound journal guard permits immutable
safety revalidation while already holding native journal serialization.

The first attempt did not compile (serde tuple arity and a test typed-ID error).
The second compiled and all three new tests failed: append held the native
journal and nested safety validation attempted to reacquire it. Both attempts
are retained. The third source passes. A mistakenly selected source filter ran
zero tests; it contributes no coverage and was replaced by the actual four
target/Capture controls and the exact source-configuration control.

`summary.json` deduplicates passing test names across filters. Receipts and
lossless compressed output/resource samples retain failures, command flags,
source hashes, minimum RAM, pressure and timing. The build used release
externs with bicdb-core opt-level=0/debug=0, jobs=1, incremental disabled and an
active 12 GiB available-RAM pause guard. The intended worktree's lib.rs was
touched before compilation and output confirms that worktree compiled. No
resource pause/stop occurred; the successful compile/run minimum was 21.05 GiB.
Cached checks reused that exact source. This is not an optimized runtime result.

These tests do **not** demonstrate a conflicting retained CQ rejection schedule,
R>0 data installation, final atomic installed-base/history publication, recovery
Apply/abort, fresh data-quorum readiness, membership activation, surviving leader
loss, 1,025-command compaction, repaired current-voter availability, StrongGet,
physical scale-out, production qualification or deployment. R>0 row/final paths
remain closed pending actual staged-history composition. The earlier Azure
failures and their source revisions remain unchanged.
