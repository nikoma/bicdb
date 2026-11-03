# Bounded session context

This checkpoint adds general Oracle-preview USERENV inspection and immutable
host-installed transport facts. It does not qualify complete application
execution or Oracle conformance.

The read-only public [source fixture](../../../examples/oracle/reference/session_context.py)
compares identity relationships, optional length behavior, NULL handling,
case-insensitive names and error precedence. Its **33/33** cases match the
Oracle reference; the prior frozen binary matched **0/33**. Physical usernames,
database names and client endpoints differ across installations and are not
claimed to have identical literal values. Separate [PostgreSQL controls](../../../examples/oracle/reference/postgres_session_context_controls.py)
retain native session and text behavior (**8/8**).

## Supported behavior and limits

Supported attributes are CURRENT_USER, SESSION_USER, CURRENT_SCHEMA, DB_NAME,
DB_DOMAIN, HOST and IP_ADDRESS. Identity uses the effective role and session
authorization respectively, and schema selection follows preview resolution.
Actual role catalog spelling is retained. The preview has no configured Oracle
database domain, so DB_DOMAIN is NULL. Accepted-peer IP metadata is populated by
the PostgreSQL server; missing transport metadata is NULL. Client hostname is
unavailable through PostgreSQL startup and HOST remains NULL on that transport.
The server's hostname is never presented as the client's hostname.

Namespace and attribute matching are case-insensitive without trimming their
names. Unknown namespaces return NULL. Unsupported/invalid USERENV parameters
are refused. The implementation is a bounded subset, not an implementation of
all valid USERENV names or application-defined contexts. Optional numeric length
truncates toward zero; values outside 1..4000 use 256. NULL arguments yield NULL.
UTF8 truncation retains complete codepoints; partial-byte Oracle character
behavior, NLS conversion and extended authentication contexts are unqualified.

Startup options cannot inject client address/hostname metadata. SQL SET,
set_config and RESET refuse writes. RESET ALL and DISCARD ALL preserve the
host-installed facts. Prepared driver checks cover client forgery attempts,
VARCHAR result metadata, reset behavior and actual accepted-peer addresses.

## Persistence and application state

Compiler version 22 uses OID 999421; version 21 retains OID 999420. Legacy
routines retain their prior semantics. Thirteen reopen/reader checks cover old
routines, new routines and views, old-reader refusals, and current reopen after
refusal. Durable metadata format remains 5.

The local compatibility server accepts optional explicit connection and active
query limits for JDBC application testing. This changes neither production server
defaults nor the full CLI's controls. Development binaries are not published
releases or verified application deployments. See [validation.json](validation.json)
for the source/binary identity and final local validation evidence.

Contract reference: [Oracle SYS_CONTEXT SQL reference](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/SYS_CONTEXT.html).
