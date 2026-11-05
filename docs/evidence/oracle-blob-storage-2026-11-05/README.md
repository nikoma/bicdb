# Oracle BLOB storage checkpoint

This checkpoint adds identity-bearing BLOB cells, transactional owner metadata
and PostgreSQL prepared direct-column origin metadata. It does not implement
persistent APPEND or qualify a complete application. See the
[storage contract and integration limits](../../oracle-blob-storage.md).

The [validation record](validation-results.json) identifies the tested source,
file hashes, checks and resource limits. Tests cover independent copies,
equal-byte replacements, self-assignment, unrelated-column updates, deletion,
rollback, savepoints, primary-key moves, reopen, malformed identity envelopes,
private collection denial and protection from the legacy typed-storage rewriter.
Driver checks cover BYTEA payloads, prepared origin metadata and rollback in the
Oracle profile, with a separate PostgreSQL quoted-identifier control. Existing
Oracle and general protocol suites protect their respective behavior.

Native storage tests disable fsync; reopen and transactional rollback are not
power-loss qualification. The existing native Oracle
[ownership observations](../oracle-persistent-blob-ownership-2026-11-05/README.md)
and [acquisition observations](../oracle-lob-read-versions-2026-11-05/README.md)
remain the source contracts for future runtime integration. This checkpoint does
not rerun those probes or establish persistent locator equivalence.

The first broad run exposed an assertion expecting the old internal String
representation; its binary-value assertion was corrected. New driver tests then
exposed missing prepared-column origins: persisted identifier lookup folded
quoted names, and the metadata-only Describe fast path dropped origins entirely.
Both paths were corrected without executing a query during preparation.

No standalone server binary, release or application deployment is delivered by
this source checkpoint. Stored read-version binding, later-transaction rebinding,
actual stored identities for RETURNING, authorized APPEND, DDL lifecycle and
owner-registry cleanup, legacy source-manifest migration, index/constraint and
trigger semantics, and complete application acceptance remain open. GitHub
workflows were neither added nor invoked.
