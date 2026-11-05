# Public legacy BLOB migration phase

The application snapshot importer now exposes `--phase blob-types` with explicit
source declarations, read-only preflight, stopped-writer confirmation, complete
target table fingerprints and resumable conversion. The
[migration guide](../../oracle-blob-migration.md) describes operator commands and
limits. This is a general manifest/schema-driven operation; no application table
name selects engine behavior.

The local Python importer suite passed 41 tests with zero skips, including a
real BicDB server fixture. That fixture covers NULL, empty and nonempty binary
values, preservation of unrelated native BYTEA, quoted schema names, real
persistent APPEND after conversion, apply/resume, refusal after target rows
change, and preserving an existing journal on source-identity mismatch. A missing server TimeZone status is rejected before catalog or row decoding.
The fixture removes its entire owned native database, including on failures.

Pure checks cover source/view boundaries, duplicate or virtual declarations,
ordered journal prefixes, target/source mismatches, explicit writer confirmation,
and a capability probe that verifies stored bytes and cleans up on failure.
The initial real-server attempt reached fixture cleanup and failed on unsupported
`DROP SCHEMA`; cleanup was corrected to remove the owned native database, and
the complete suite was rerun. No DROP SCHEMA support is claimed.

Read-only preflight does not establish persistent-locator capability. Apply
checks that separately with a nonce table, then rewrites source BLOB columns and
checks complete target table values. Those checks establish target preservation,
not equality with source export rows, writer fencing, complete Oracle DDL/LOB
semantics or production qualification. The separate importer `verify` phase and
a consistent source/cutover policy remain required.


Full-snapshot checks initially exposed a missing PostgreSQL TimeZone status:
the installed psycopg C client crashed while preparing a timestamptz result,
including a NULL-only reproduction without application tables. The
[general pgwire fix](../pgwire-timezone-status-2026-11-05/README.md) reports startup
and session timezone status. The final 41-test batch uses the default C driver
and a real nullable/non-null timestamptz field. A pure-Python control was used
for diagnosis, not substituted for final C-driver acceptance. An earlier private
wrapper readiness check also omitted the required profile and was corrected
before importer execution; it made no source-column conversions.


The final default C-driver check completed read-only preflight, apply and resume
against a fresh clean clone of the exported application snapshot. All 16 source
BLOB columns completed, and complete fingerprints of all 16 affected tables
matched before/after in both apply and resume. The owned database exited with
code 0, with no mutations to the accepted application's database. The resource
guard completed with its 12 GiB headroom floor intact. This proves target-value
preservation for this export, not source-row equality after application writes
or full application acceptance.


The later delivery merge passed 12 JSON regression tests, 181 general protocol
tests and 48 Oracle protocol tests, plus workspace/all-target checking and the
server build. Its source and binary hashes are recorded separately from the
preceding full-snapshot C-driver qualification. No new accepted deployment is
implied by the delivery build.
