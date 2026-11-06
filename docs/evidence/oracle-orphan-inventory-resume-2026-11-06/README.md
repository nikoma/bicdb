# Resumable orphan inventory qualification

The read-only inventory accepts `--resume` for an exact ordered count prefix.
It requires the same manifest/profile/target and completed migration-report
digest. Full current catalog/enforcement/projection and row-baseline checks run
before count reuse, and final catalog/fingerprint checks remain mandatory.
See the [operator instructions](../../oracle-foreign-key-orphan-inventory.md).

All **21 focused tests passed: eight pure and thirteen native, zero skips**.
The full importer/tool suite ran **178 tests: 174 passed, four skipped, zero
failures**. The four skips require separately configured live Oracle owners or
replay connections and were not counted as passing. [validation.json](validation.json)
retains their exact identities, source/executable hashes and log digests.
The executable was the retained build from
`5e5fecc3ca279f2e3ea3e37af0c244e11bbb69ea`; no SQL engine rebuild was needed.

The restart control interrupts a three-relationship inventory after one count,
restarts its disposable native database, and verifies that only the two pending
relationships are recounted. The previous implementation failed this assertion
with three calls instead of two. A separate CLI control invokes the actual
`--resume` command. The completed resumed report is accepted by the existing
strict offline comparison reader, including the intentional legacy orphan.

Negative controls cover changed endpoint/migration/source identity, reordered
or duplicate declarations, boolean/fractional/negative counts, validated-key
orphans, incomplete/changed fingerprints, and integer-versus-boolean declaration
changes. Native controls refuse changed rows or removed relationships before
count reuse. Identity rejection preserves the supplied journal. Even a complete
prior inventory must repeat its catalog and fingerprint checks.

This is bounded recovery/tooling evidence. It does not repair rows, qualify all
application orphan counts or source-data equality, acquire a writer lease,
authenticate report authors or complete application/cutover/production acceptance.
