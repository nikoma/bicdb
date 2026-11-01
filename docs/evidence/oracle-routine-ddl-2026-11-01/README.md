# Routine deletion through PostgreSQL drivers

Only CREATE routine declarations enter the Oracle procedural-body lowerer.
DROP FUNCTION and DROP PROCEDURE use ordinary DDL processing and no longer
require an IS/AS body. Their lookup uses the routine store's existing normalized
keys. An explicit namespace must match the routine's recorded namespace.

Routine deletion requires ownership through the effective role, inherited
ownership or superuser privileges. EXECUTE permission does not permit deletion.
The existing DDL undo path now restores removed routines on transaction rollback;
a failed DROP naming several objects also restores earlier removals. Function
type dependencies retain the existing RESTRICT/CASCADE behavior. An absent or
mismatched IF EXISTS target does not cascade into another routine's dependencies.

Engine regressions cover native PostgreSQL and preview function deletion, native
procedure deletion, qualified names, wrong namespaces, owner checks, missing
objects and rollback. A bounded HR regression also creates and removes an
Oracle-authored procedure. Driver regressions use separate owner/other logins.
Their administrative roles and procedure are provisioned through the native
backend API: preview role-management DDL and Oracle procedure creation remain
outstanding. These tests do not qualify those creation paths.

The routine store still uses global normalized names. Schema-isolated overloads,
full quoted-name semantics, complete dependency tracking, Oracle AUTHID/errors and
general PL/SQL execution remain separate requirements. This is not complete
application acceptance.

The [application cleanup](application-cleanup.json) removes the unqualified
PAYMENTTERMDUEDATE probe through the actual application database's PostgreSQL
driver connection. The definition remains absent after restart, while the
previously working GETDATE/CHARAT functions still execute. The three source
nullable-UNIQUE constraints and their source values were also rechecked without
repair after the new runtime restarted.

The [known cursor gap](known-cursor-gap.json) remains explicit: the original
Oracle function definition was accepted, but its call failed on the cursor loop.
Removing this failed development probe does not make that routine work. Its
original Oracle source was retained outside the repository.

Validation passed: 347 SQL unit tests (three ignored), all 416 main SQL integration
tests, 117 pgwire unit tests, fourteen Oracle-profile driver tests and 167 protocol
tests. Formatting, licensing and open-core boundary checks passed. The development
server was rebuilt and the isolated fixture restarted. No release was published
or production deployment performed.
