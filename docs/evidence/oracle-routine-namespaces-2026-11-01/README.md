# Static function relation namespaces

The preview compiler version `bicdb_oracle_preview_v2` binds static relation
references to the namespace in each stored function declaration. The existing
session relation visitor is shared with the routine compiler. It retains CTE
names, explicit qualifications and visible table aliases, including correlated
references. Preview session creation qualifies an unqualified function name
with the selected application schema. Exact namespace spelling is stored in
routine metadata and used by qualified DROP checks. Native access checks still
apply to the resulting tables.

The rule applies to arbitrary declarations and relations. No application names,
table names, routine names or query fingerprints select this behavior. The
application probe below is separate acceptance evidence. Default PostgreSQL and
the older Oracle compiler versions have independent regression coverage.

The [namespace comparison](namespace-comparison.json) records twenty-four values
matched with Oracle Free 23.26.3.0.0. Generated functions exercise bound cursors,
CTEs, SELECT INTO, query FOR loops, nested cursors and explicit qualifications.
The BicDB database also contains different-valued public shadow tables. Those
values must never replace the owner-schema values returned by Oracle. The
[datetime comparison](datetime-comparison.json) adds sixteen DATE/timestamp and
numeric/NULL values under version 2. Temporary test objects were removed from
both databases.
The public [reference runner](../../../examples/oracle/reference/routine_namespaces.py)
replays the namespace cases with generated names, explicit typed expectations
and cleanup failure reporting. Its [usage](../../../examples/oracle/reference/README.md#generated-routine-namespaces)
is independent of any application schema. The artifact above was produced by
that runner.

Engine regressions separately cover a default PostgreSQL database, native
LANGUAGE plpgsql in the preview database and the retained version-1 compiler.
They preserve the previous native lookup behavior. Additional regressions cover
a quoted namespace containing a dot, implicit function creation through the
selected schema, CTEs, exact qualified deletion, rollback and native table
privileges. EXECUTE and schema USAGE alone do not grant table SELECT permission.
The first focused run found an unquoted dotted search_path in a test fixture;
the fixture was corrected to use valid quoted search_path syntax.

Version 1 remains supported with its previous behavior; existing definitions
are not silently reinterpreted. The [older-reader check](older-reader.json)
stores a version-2 function in a separate database, then verifies that the
previous version-1-capable runtime rejects its execution with SQLSTATE 0A000.
The application database was never opened by that older binary.

The [original application probe](application-probe.json) uses the unchanged
PAYMENTTERMDUEDATE source with only its destination function namespace specified.
It now reaches arithmetic after the cursor query, failing with numeric-operand
SQLSTATE 42601 on datetime arithmetic. Its Oracle result is recorded separately;
no matching BicDB result is claimed. The failed probe was removed, its absence
checked, and existing GETDATE/CHARAT still execute. Previously qualified nullable
UNIQUE constraints and their source values passed read-only verification after
the development server upgrade.

This does not establish Oracle AUTHID rights, schema-isolated routine overloads,
full qualified routine-call resolution, source DATE/TIMESTAMP provenance or
general routine SQL overload resolution. Version-2 procedures are unsupported;
compilation rejects dynamic SQL namespace binding rather than running it in an
unintended namespace. PL/SQL completion and application workflows remain open.

Validation passed: 359 SQL unit tests (three ignored), 416 SQL integration tests,
117 pgwire unit tests, sixteen Oracle-profile driver tests, 167 protocol tests,
fourteen Python migration-tool tests and nine reference-harness tests. Prepared
calls survive database reopen with the public shadow tables present. Formatting,
licensing and open-core
boundary checks passed. The development example server was rebuilt and the
isolated fixture upgraded. No release or production deployment was performed.
