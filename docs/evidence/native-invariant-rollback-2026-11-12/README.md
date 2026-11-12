# Same-schema native invariant rollback

`before-669.json` is an actual Node-driven negative control against executable
SHA-256 `e070a73f523241d6d7becaffc32a4063e902733af5654ba187a7fd6364bc13f6`,
source `669b024476ac5fdbf1bdf6efb8734eab8a5157dc` (1.0.488-beta candidate).
It records unsafe behavior, not a qualification of that candidate.

Four official-Carrier signed fixtures have identical resource contracts except
release-derived schema versions and no application dependencies. Both removal
of an active invariant and rollback to an older expression were accepted. The
subsequent invalid writes returned 201, despite returning 409 before rollback.
The acknowledged original rows and new invalid rows survived restart and cold
copy. A compatible rollback retained its invariant and continued rejecting the
invalid write.

The regression driver, fixture sources, signed bytes and public test key are in
`crates/bicdb-app-runtime/tests/native-invariant-rollback/`. The corrected head
must reject both unsafe rollbacks, retain compatible rollback, preserve all
acknowledged data, and pass its own native and mandatory CI runs. No application
resource or field was invented to manufacture an unrelated schema fence.
