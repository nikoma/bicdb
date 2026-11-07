# Literal session settings without database-writer admission

Literal session SET assignments now run through the normal SQL session under
shared database access, without consuming a durable-writer slot or creating an
implicit database transaction. They can progress past a queued writer while an
existing reader is still active. An actively held exclusive database guard still
serializes access; this is not a general query-timeout or availability guarantee.

The AST gate accepts one SET statement with literal/identifier values, including
simple numeric signs, assignment lists and time-zone settings. Casts, functions,
transaction commands and statement batches retain their existing paths. Ordinary
SQL validation, authorization, cancellation, GUC scope and catalog handling remain
in the SQL session. No application, tenant or table name selects this behavior.

The retained previous-code control reproduces the blocked setting in both the
PostgreSQL and Oracle-preview profiles. Its reader is released before assertions
and thread joins, so the expected failures do not strand a writer. The final
focused controls also verify outside-transaction SET LOCAL behavior, transaction
rollback/commit scopes, and literal settings on a read-only standby while data
writes remain fenced. This is a controlled liveness regression, not a production
latency, throughput or HA qualification.

See [the check provenance](summary.json) for exact revisions and log hashes,
including reuse of the unchanged SQL/core inputs and immutable driver/probe
classes. Original application acceptance, the remaining Oracle programme,
production qualification, releases and deployments remain separate gates.

Focused checks:

```sh
cargo test --locked -p bicdb-pgwire --lib literal_session_setting -- --test-threads=1
```

The completed batch passes 139 pgwire library, 183 protocol, 48 Oracle HR,
44 authorization/JSON, 259 native-tooling and 250 live JDBC checks. Four
native-tooling checks require external references and are skipped. Four
provider-preparation checks also pass. The focused seven checks are included
in the library run, not additional independent acceptance tests.

The unchanged SQL/core/dependency sources retain the earlier 760-pass,
four-ignore SQL library result. Thirty unchanged JDBC unit checks retain their
separate metadata-identity checkpoint. Compiled adapter and live probe classes
are reused only after source and byte-identity checks; the changed backend is
used for the live probes. An initial authorization compilation was interrupted
by the resource guard. Its log is retained separately; all remaining checks
subsequently completed with the production dependency feature graph.
