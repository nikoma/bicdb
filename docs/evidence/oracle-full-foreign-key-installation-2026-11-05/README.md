# Full application foreign-key installation checkpoint

The public resumable migration phase installed **all 3,826 exported iDempiere
foreign-key relationships** in a native Oracle-profile candidate database.
The [bounded installation record](installation-summary.json) records the source
manifest, complete private migration-report digest, candidate source and binary
digests, and explicit qualification limits. It includes no application row values
or connection credentials.

The completed report contains 3,826 distinct table/constraint pairs: **912
validated and 2,914 not validated**, referencing 3,823 primary-key and three
unique-key parents. Source validation state was retained. Before/after row
multiset fingerprints match for all **888 participating tables**. Definition
checks preserved **62 routines and 160 views**. The native database shut down
cleanly with exit status zero after verification.

This checkpoint used an already exercised target database. Matching pre/post
fingerprints establish that installing relationships preserved that target's
values; they do not establish equality with the original Oracle export. No rows
were repaired to obtain this installation result.

Full target orphan counts and comparison with the independent
[Oracle replay](../oracle-export-reference-2026-11-05/README.md), pristine source
value equality, original application business tests with every relationship
installed, and recovery/cutover remain separate gates. In particular, this
checkpoint does not turn NOT VALIDATED relationships into validated ones or
qualify the entire application. The candidate is separate from the accepted
application deployment. No production qualification or Oracle certification is
claimed.

The [migration guide](../../oracle-foreign-key-migration.md) documents the public
commands and writer-quiescence contract. The earlier
[migration-tool validation](../oracle-foreign-key-migration-2026-11-05/README.md)
records the independent native fixtures and PostgreSQL regression coverage;
this installation checkpoint adds full relationship-count evidence rather than
claiming a new engine test run.
