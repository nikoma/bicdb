# iDempiere native index development evidence

The pinned Oracle seed contains 267 additional logical indexes after excluding
primary/unique constraint-backed indexes. This run verified native, ready index
backing for 237. The index phase is **incomplete**: 24 indexes need Oracle's
nullable composite unique-key policy, five need additional unique expression
support, and one needs executable datetime truncation indexing. Oracle's 48 LOB
storage indexes are physical storage details, separately recorded by the importer.
The source tables also have outstanding nullable composite UNIQUE constraints.

The importer checks ready/valid native backing and proves that the target rejects
a non-executable placeholder as invalid. It separately probes duplicate enforcement
for nullable composite uniqueness. Unsupported expressions cannot create unique
catalog placeholders. Non-unique metadata-only indexes are described as neither
ready nor valid. Journals retain failure reasons and never claim application
qualification.

Native `UPPER` fields now support index backfill, lookups, uniqueness, updates,
deletes, reopening and the owned/borrowed cell key paths. Their case
conversion matches the engine's existing ASCII `upper()` behavior; this is not
Oracle linguistic collation conformance. The datetime overload resolver also
recognizes CREATE INDEX's source table, but that does not supply an executable
index field for datetime truncation.

After rebuilding the source `AD_COLUMN_NAME` index, the actual application
preference query returned the same sorted 49 rows as Oracle in approximately
1.97 seconds, compared with 0.039 seconds on the local Oracle reference. The
separate diagnostic lookup index had been removed. These are single development
measurements, not a throughput benchmark or a performance guarantee.

The Java application accepts credentials and reaches desktop initialization. It
then reports the missing `AD_UserDef_Form` table from an unapplied release
migration. Business workflows, complete schema compatibility and production
operation remain unqualified. The BicDB development copy uses volatile storage.
The preserved reference seed is separate from that copy.

Validation: 336 SQL unit tests passed (three ignored), 165 protocol tests, nine
Oracle-profile driver integration tests, two core cell-key regressions and 12
Python migration-policy tests passed. Formatting, licensing and the open-core
boundary checks passed. The [machine-readable summary](summary.json) contains
aggregate results; credentials, exported row data and application binaries are
kept outside this repository.
