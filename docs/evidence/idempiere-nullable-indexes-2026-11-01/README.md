# Native nullable unique-key and expression-index evidence

The original iDempiere Oracle snapshot's index phase now verifies 266 of its 267
additional logical indexes. The 24 nullable composite unique indexes and five
COALESCE unique indexes that were outstanding in the previous run are installed.
The remaining datetime truncation index is reported unsupported; the phase stays
incomplete. Nullable composite UNIQUE table constraints, deferred foreign keys,
stored programs and views still need qualification.

Nine ordered insert attempts against Oracle's ordinary composite unique index
and BicDB's explicit NULLS NOT DISTINCT partial index produced the same outcomes:
six accepted inserts and three duplicate rejections. The cases include duplicate
all-NULL keys and partially NULL duplicates. Oracle reports uniqueness code 1;
BicDB reports SQLSTATE 23505. Scratch tables were removed after the comparison.

Native index tests cover backfill, canonical numeric fallback keys, pending keys,
updates, ON CONFLICT, concurrent commits and reopening in both storage modes.
COALESCE's supported scalar/literal subset excludes volatile or value-changing
expressions and untyped fractional floating defaults. This does not establish
Oracle implicit-conversion or linguistic-collation equivalence.

An older development binary refused an isolated format-metadata fixture with the
new capability flag, without changing that metadata. Extended index uniqueness
also has a non-boolean wire encoding, which an older boolean reader rejects.
Legacy/default unique-index JSON retains its boolean representation.

The existing PostgreSQL composite-expression catalog test now uses actual native
index backing. Its `indkey` has two zero entries, one for each expression, as
specified by the [PostgreSQL catalog documentation](https://www.postgresql.org/docs/current/catalog-pg-index.html).
The regression additionally checks both attribute counts and actual duplicate
rejection. Catalog placeholders are not reported as valid physical indexes.

A separate Oracle reference copy applied the 24 missing release migrations.
Registration was deferred until the script's SQL effects committed and schema
recompilation succeeded. The copy's collection type received a fresh OID during
schema cloning. The upgraded reference has 915 tables, 185,471 rows and zero
invalid objects. A separate native BicDB development instance imported all 915
tables and reconciled every row multiset. That import used the preceding
`b016db4` development build and persistent development storage. It does not
qualify migration SQL execution on BicDB or the complete application.

The [summary](summary.json) separates the original index-development copy from
the upgraded reference and the new BicDB import. Credentials, row exports,
application binaries and environment-specific paths remain outside the repository.

Validation passed: 439 core unit tests (three ignored), 20 storage conformance
tests, four new nullable-unique tests, 338 SQL unit tests (three ignored), all 416
tests in the main SQL integration suite, 165 protocol tests, ten Oracle-profile
driver integration tests and 13 Python migration-policy tests. Formatting,
licensing and open-core boundary checks passed. Workspace/all-target compilation
was checked separately. No ignored test was enabled or disabled by this change.
