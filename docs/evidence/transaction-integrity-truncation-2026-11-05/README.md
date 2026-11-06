# Integrity reservations during core transaction cleanup

The core write-only truncation API could discard integrity reservations even
when a buffered write survived. A standalone reproduction retained one write,
truncated to its existing length and observed that another transaction could
reserve both its parent-reference and unique keys. The failed behavior and
input/archive digests are retained in the [validation record](validation.json).
The [exact same standalone input](probe.rs.txt) passes against the rebuilt core after the fix,
with both reference and uniqueness protection retained. That diagnostic consumes
and removes an owned disposable database path. Current regressions run with
`cargo test -p bicdb-core --test integrity_truncation`.

Write-only truncation now retains integrity reservations conservatively while
any write remains. Removing all writes releases them. Exact savepoint cleanup
continues to use rollback marks, which track both writes and acquisitions.
Read-lock cleanup rejects a suffix containing integrity reservations atomically,
including when there are no writes. Three focused regressions verify retained
protection, full release, new-owner protection and rejection without partial
cleanup. Shared references and exclusive uniqueness use the same lifecycle rule.

The validation record lists actual per-target core, SQL, protocol and tool test
counts, skipped/ignored cases, candidate identity and log hashes. Two initial
native-tool failures identified the absent JDBC build in the isolated worktree.
The offline Maven build passed all ten tests, and all six SDK classes match the
staged runtime byte-for-byte. The complete native-tool suite was then rerun;
passing Rust results were reused only after checking the unchanged source and
frozen binary. Independent
Oracle comparisons retain all [seven foreign-key cases](foreign-key-reference.json)
and [six BLOB-read cases](blob-reference.json); each native comparison database
shuts down cleanly and is removed. The disposable Oracle owner and reference
container are cleaned up by the source-fixture harness.

This qualifies the exercised core cleanup paths and bounded regression cases.
It does not qualify an accepted application upgrade, the whole application,
general admission progress under saturated lock waits, distributed reservation
ownership, production operation or complete Oracle compatibility. See the
[reservation contract](../../foreign-key-reservations.md) for supported key moves
and remaining boundaries. The separately interrupted application batch still
requires all 89 original tests on a fresh full-schema copy.
