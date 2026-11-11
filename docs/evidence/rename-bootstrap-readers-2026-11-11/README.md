# Rename recovery roles and bootstrap reader requirements

This is focused evidence for an **unmerged prototype**, not application,
mixed-version live-wire, VM-fault or production qualification. The original
application assertions and ten-second timeout are unchanged.

The tests-only source `59f55d87` fails both new bootstrap controls: range
snapshot copying and an equal-structural-schema installation publish data
without the source's `sql_catalog_intents_v1` requirement. Each command stops
at its first EmbeddedMemory target failure; this does not establish failure in
every intended matrix cell. Both failed receipts are retained.

Runtime source `c3ce4ebf092595573302d054ed3dd95fabf9dac3` passes all six commands
(68 tests across the selected targets). Snapshot exports carry sorted portable
reader requirements. Mutable and shared snapshot apply validate and persist
them before publishing rows. Nonempty requirements use
`reader_protected_records_v1` instead of the mandatory legacy `records` field,
so a frozen legacy decoder rejects them. Unprotected snapshot bytes remain
identical. Unknown requirements, missing journal requirements and inconsistent
envelopes fail closed.

Schema bundles checksum the requirements and use format 2 when requirements
are present; the unprotected format-1 checksum payload is unchanged. Semantic
compatibility fingerprints include these requirements, and installing a new
requirement invalidates cached readiness identity. Bootstrap handles an equal
structural schema; signed activation installs the requirements before catalog
changes and resumes after reopening. Sticky requirements cannot be removed by
an additive rollout. A first implementation at `237edc53` passed the snapshot
and legacy-wire controls but failed schema export because its canonical digest
verifier omitted the requirements. That failed receipt is retained; the
verifier was corrected without changing the assertion.

The earlier role-aware source `546ac4a6` passes its six startup/rename/recycle
and SQL regression commands. A standby retains the primary's live intent
without independently undoing it; the configured primary initializer recovers
it. The additional `81fe1ca7` control passes both stores and PostgreSQL/Oracle-HR
persisted profiles using the real persisted promotion/restart APIs: an
unfinished replicated rename restores the prior acknowledged value and allows
new writes that survive another reopen. These are native host-operation
prefixes, not Oracle client implicit-DDL-commit conformance or network-election
proof. The temporary global standby SQL readiness fence is still a scoped
admission limitation.

Helper-only source `2262454a38e25c14e0bcecc95b86c8afccff4383` retains real WAL,
snapshot and schema-plus-snapshot receiver stores in both destination storage
modes. Its production core source and Cargo files are identical to `c3ce4ebf`.
The standalone fixture executable is retained on the build host under
`qualification-artifacts-debug/2262454a38e25c14e0bcecc95b86c8afccff4383/`;
it is not a standard server binary or a release. The first helper attempt
`fa5d1d4c` failed compilation before any fixture or older-process probe ran.

Separate, SHA256-pinned server executables from `31a65686` and `a6839bc4` then
consume isolated copies. All **16 cases pass**: four unflagged controls read
`retained-source-value`; all twelve protected receivers refuse the unknown
`sql_rename_intents_v1` flag with no changes to any file bytes. The result
records executable identities and each before/after file digest. Database
copies and generated signing-key material remain private on the build host.
This is actual older-process store admission evidence; the live transport
decoder check above is a frozen-decoder control, not an actual mixed-version
network rollout test.

Remaining gates include scoped relation/type/routine/Describe admission and
concurrent visibility; fresh Oracle standby bootstrap; mixed-version live
transport and fault cuts; resident-scan total-work/performance qualification;
and final source integration, broad CI, standard binary, independent ACK/VM
and application acceptance. These results do not establish stored materialized
view semantics or Oracle implicit-DDL behavior.

The next tests-only source `ce3dc430` establishes a concrete remaining scoped
admission failure. Its first EmbeddedMemory/rollback case refuses ALTER because
a different native transaction has a pending write and lock in an unrelated
collection. Later intended matrix cells are not reached. Native PostgreSQL
16.13 passes both corresponding schedules: the unrelated peer commits before
the rename owner's COMMIT or ROLLBACK, and its row survives either decision.
The native control uses and drops a private schema in the dedicated control
database. These receipts are retained separately; the failing regression must
be fixed before the rename prototype can be delivered. The completed bootstrap
checks do not satisfy this gate.

`sha256s.json` identifies the retained receipts and old-reader result.
