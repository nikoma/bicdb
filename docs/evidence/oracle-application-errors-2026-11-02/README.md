# Oracle application errors and exception context

Compiler version 14 (`bicdb_oracle_preview_v14`, OID 999413) adds bounded
RAISE_APPLICATION_ERROR and current SQLCODE/SQLERRM to the Oracle preview profile.
Existing stored compiler versions retain their earlier contracts; recreate a
routine from source to adopt this version. No engine rule depends on application
object names.

The original Oracle-provider iDempiere application is **not fully operational**
on BicDB. Definition acceptance remains **57/62**. These checks use disposable
databases and do not upgrade the running development application database.
No production release or deployment is established by this qualification.

## Rules covered

The source reference is Oracle 23.26.3.0.0 under American diagnostic language,
AL32UTF8 and the fixed preview numeric/date NLS settings. RAISE_APPLICATION_ERROR
accepts the observed -20999 through -20000 range, rounds numeric inputs through
its binary-integer formal, and distinguishes invalid ranges, numeric overflow
and numeric-text conversion errors. Character, NUMBER and logical DATE messages
use the preview conversion contract. Trailing spaces and linefeeds are removed;
leading characters, tabs and carriage returns are preserved. SQLERRM is limited
to 512 UTF-8 bytes, and the observed unhandled message limit is 2045 bytes.

Omitted keeperrorstack defaults to FALSE. Explicit NULL behaves like TRUE in the
reference. Replacing the stack invalidates saved messages in enclosing handlers,
while their SQLCODE remains accessible. Reraising a cleared error produces
ORA-21002. Input conversion errors occur before that replacement. Direct PL/SQL
calls inherit exception context; SQL-invoked routines start with normal diagnostics
and cannot clear a parent PL/SQL handler's saved message. Diagnostic reads remain
dynamic within expressions and parameter defaults. Local declarations shadow
standard names according to Oracle identifier case rules.

PostgreSQL transport uses SQLSTATE P0001 for application errors, 22023 for bad
ranges, 22003 for numeric overflow and 22P02 for numeric-text conversion. Primary
messages contain the logical ORA text. DETAIL contains JSON with an
`oracle_error_stack` array of logical codes/messages. Oracle call-site backtraces
are excluded from comparisons and are not implemented. Generic native causes
may retain native SQLSTATE/message entries; this is not a full Oracle catalog.
Native bare RAISE preserves PostgreSQL state, message, DETAIL and constraint fields.

## Executable checks

Run the new Oracle comparison with dedicated source and target databases:

```sh
python examples/oracle/reference/routine_namespaces.py \
  --suite errors --oracle-config /private/reference.json \
  --output /private/application-errors.json
```

Set BICDB_PG_DSN privately to the Oracle-preview target. The runner checks frozen
source expectations against actual Oracle and BicDB, then removes its objects.

```sh
python examples/oracle/reference/postgres_errors.py \
  --output /private/native-errors.json
```

Set ORACLE_CONTROL_PG_DSN and BICDB_PG_DSN privately to PostgreSQL and native
BicDB databases. Error fields, trailing whitespace and recovery are compared
independently of the Oracle profile.

The original AD_SEQUENCE_NEXT body is also compared against Oracle with a minimal
synthetic dependency table in fresh schemas. This covers successful allocations,
state, rollback and the missing-sequence application error. A separate disposable
copy of the application records NULL-argument diagnostics only. Such calls do
not establish valid-input business behavior; the source application procedures
are not executed by that copy-only diagnostic runner.

## Reference results

[application-error-comparisons.json](application-error-comparisons.json) records
**108/108** new matches against actual Oracle. Together with
[retained-oracle-controls.json](retained-oracle-controls.json), all **1,086/1,086**
generated comparisons pass on the final candidate. These counts cover the listed
fixtures and do not measure overall Oracle compatibility.

[postgres-error-controls.json](postgres-error-controls.json) records **9/9** new
independent PostgreSQL 18.4 controls. Including retained suites,
[native-controls.json](native-controls.json) records **163/163** matches.
[reader-compatibility.json](reader-compatibility.json) verifies that the version-13
reader rejects version-14 calls with 0A000 and remains usable on an isolated DB.
No old reader opens the running application database.

[original-sequence-comparisons.json](original-sequence-comparisons.json) records
**9/9** controlled original procedure comparisons. Its source body is unchanged;
only its declaration is namespace-qualified. The dependency table is synthetic
and minimal. This does not establish concurrent allocation or complete schema
behavior. [original-procedure-call-diagnostics.json](original-procedure-call-diagnostics.json)
records **1/13** successful NULL-input calls on the disposable application copy.
Two others now raise application business errors. Definitions remain **57/62**;
NULL-input success is not valid-input business acceptance.

[existing-semantic-gap-observations.json](existing-semantic-gap-observations.json)
separately retains seven prior NVL/type/conversion probes: two match and five
still differ. Those mismatches are excluded from the passing generated corpus.
[message-whitespace-source.json](message-whitespace-source.json) records additional
source observations; unsupported NUL and invalid packed CHR transport are not
counted as passing checks.

The separate [imported-data sequence comparison](original-application-sequence-comparisons.json)
records **2/2** valid calls against the actual imported application AD_SEQUENCE
data. Both existing-row branches use the unchanged original procedure; outputs,
NUMERIC metadata, counter changes and explicit rollback match Oracle. Both source
and target counter states are checked after rollback. The target is a disposable
copy of the 915-table import, with its existing migration limitations. This does
not qualify all constraints, concurrency, other routines or end-to-end workflows.
These two calls are separate from the nine minimal-dependency checks and the
1,086 generated cases above.

[validation.json](validation.json) records the final source/binary identities,
local Rust and Python checks and monitored resource limits. The running
application process retains its previous binary, independently recorded in
[running-application-unchanged.json](running-application-unchanged.json); its full
query values are not requalified by this milestone.

## Remaining scope

SQLERRM(code), EXCEPTION_INIT, complete predefined errors, error localization,
Oracle backtraces, packages and autonomous transactions remain open. Generic
SQLCODE mappings cover no-data, too-many-rows, division and unique violations;
SQLERRM text is only supplied for the explicitly tested supported subset.
Other unsupported mappings fail explicitly. Separate existing NVL constant/type
and conversion mismatches remain recorded outside the passing corpus. Complete
application workflows, security, concurrency, migration and operational acceptance
remain required by the broader programme.
