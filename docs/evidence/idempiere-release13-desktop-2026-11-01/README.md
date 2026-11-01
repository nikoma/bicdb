# Upgraded application fixture and desktop probe

The separate release-13 Oracle reference copy has 915 tables and 185,471 rows.
All table data reconciled after import into a native BicDB development instance
before application writes and test configuration changes. The reference and
original seed remain separate from the application copy.

The upgraded snapshot has 268 additional logical indexes. Its native index pass
verified 267 executable indexes; `FACT_ACCT_TRUNC_DATEACCT` remains unsupported.
The source `GETDATE` and `CHARAT` routines were installed and checked. This is
not a completed routines, views or constraint migration.

The actual release-13 application used its Oracle SQL provider with an explicit
PostgreSQL JDBC transport adaptation. Browser login accepted the public sample
credentials and role selection and reached the desktop shell. The previous
missing `AD_UserDef_Form` error was absent. Dashboard/background errors remain,
including missing views, date arithmetic and unsupported Oracle expressions.
The probe did not establish a fully loaded, error-free desktop or business
workflow acceptance. No application validation was disabled.

After that probe, `RV_UNPROCESSED` and `RV_C_INVOICE` were installed using their
exported Oracle query definitions. Complete row-multiset comparisons matched the
isolated Oracle reference: five and eight rows respectively. The UI was not
rerun after installing those two views. The other views and routines remain
outside this evidence.

Driver regressions cover selected-schema creation and removal of tables,
indexes, sequences and views. Named and ALL `DEALLOCATE` cleanup is checked
through the wire protocol under PostgreSQL, Oracle HR and Oracle preview
profiles, including quoted mixed-case prepared names.

Validation passed: 338 SQL unit tests (three ignored), ten Oracle-profile driver
integration tests and 167 protocol integration tests. Formatting, licensing and
open-core boundary checks passed.

Credentials, exported application rows, application binaries and local process
details remain outside the repository. These checks use development builds and
do not qualify migration SQL execution on BicDB, a release, production operation
or the complete application.
