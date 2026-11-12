# Backup controls after Oracle identifier normalization

A real browser/online-backup drill on merged `1.0.481-beta`
(`3cce710c689ba8ec7a820538113e389481e04669`) reached four ready browser
sessions and completed a read-only inventory of all 915 application tables,
18,857 declared columns and complete binary values. Starting the backup then
failed with SQLSTATE `0A000`: the Oracle frontend rewrote the call to
`SELECT "BICDB_BACKUP_START"(...)`, but the server dispatcher compared the
rendered, quoted object name against an unquoted spelling. No backup or
400-action qualification follows from that failed attempt.

The dispatcher now matches a single identifier's value using the SQL profile's
case rules. PostgreSQL quoted uppercase remains distinct; quoted lowercase and
Oracle-normalized names reach the existing dispatcher. Qualified names remain
ordinary SQL calls. The strict standalone-call shape, literal arguments,
superuser check, transaction refusal and delegation refusal remain required.

The negative regression at `4950d69df7cc5d78cf3b469f4d60902f743cabdf`
uses the unchanged source481 implementation and reproduces the actual dispatch
failure after a valid Oracle startup. At
`62da2e6e1fb3280011185ba1a35758d45e43d6e2`, all five online-backup tests
passed: four existing controls and one new regression exercising both Oracle
and PostgreSQL. The latter checks start/status/cancel, encryption and restore
of an acknowledged value, quoted/qualified name boundaries, ordinary-user
refusal and transaction refusal. Its server uses fsync; restore retains the
actual source storage mode and Oracle frontend.

Earlier agent fixture failures are retained in the manifest and compressed
logs: an omitted required Oracle startup option, a mismatched restore storage
mode, and an embedded restore reader missing Oracle name lowering. They are
not product test passes. Production code is identical from `74c024cc` through
`62da2e6e`; those later changes correct only the new agent regression.

The versioned delivery head requires its own version-identity, protocol and
online-backup validation and repository CI. Focused controls do not qualify a
full application run, a browser workload, physical fault recovery, release or
deployment. The larger SQL rename/partition campaign remains separate and open.
