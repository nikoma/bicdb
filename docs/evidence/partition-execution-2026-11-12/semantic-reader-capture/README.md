# Native semantic reader requirements during image capture

The original baseline at `73f18e76faaf631bf3e2d48239814be1e788eb9a`
failed all three new product assertions: actual active SQL partition, Decimal
comparison and application uniqueness requirements were absent from
`required_reader_features`. Each baseline test stopped at EmbeddedMemory; no
ServerPaged baseline outcome is claimed. Original failing logs and assertions
are retained here.

Implementation `e8c426a73a14c1d97851082b90b815da60d4ef82` includes those
three implemented readers in image contracts. Local namespace, storage and
image-authority flags remain local. The change supplies no new authority.

Eight distinct focused tests passed (nine invocations, one repeated binding
test), zero failed/ignored. The three unchanged capture regressions each run
EmbeddedMemory and ServerPaged with real Native ACKs and two reopens. A new
protected-binding regression checks all three flags invalidate an old capture,
keep the new requirement across two reopens, preserve the acknowledged row
hash and local namespace, and exclude local authority/storage flags. The actual
Native installer also refuses a target whose Decimal or application uniqueness
reader contract differs, before writes, preserving its sequence, namespace and
format bytes. Three existing binding, gate and snapshot controls passed too.

The complete existing format and image-install modules also passed, with
35 tests, zero failed/ignored. Across the focused and module runs,
39 distinct tests passed (44 total invocations). Full current Core
regression is queued after the preceding broader run and owned artifact
cleanup; no passing broad result is claimed for this changed contract. The
source is on the private partition branch; it is not part of standalone
1.0.490-beta PR165 and has no independently reserved delivery version.

The installer requires an exact preinstalled contract; this change does not
automatically install requirements at a destination. Legacy logical propagation,
portable collection-origin creation/import and the original promoted undecided
rename positive remain open. Old-reader, complete application, physical HA and
automatic multi-node capacity qualification are not established by these tests.
No production CLI, release or deployment is produced. The original full goal
remains active, experimental production behavior remains disabled, and there
is no new cloud spend or promotion.

The manifest records stored and original log hashes. Receipts and controller
specifications retain exact source, command, result and memory-monitor scope.
