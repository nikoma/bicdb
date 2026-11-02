# Bound cursor loops

The native routine interpreter executes `FOR record IN cursor_name LOOP` using
an unparameterized bound cursor declaration. The Oracle preview lowers
`CURSOR name IS SELECT ...` to that declaration. Opening a cursor binds the
current parameters and local values, captures result columns and materializes
rows. Completion, EXIT, RETURN, cancellation and body errors close the loop's
cursor. A failed attempt to open an already-open cursor preserves that cursor.
CONTINUE, empty results, subsequent reopening and nested loops are covered.

Embedded queries now resolve quoted routine variables after actual row columns,
using the same frame name normalization as bound procedural expressions. Native
PostgreSQL tests cover quoted parameters and column precedence. This does not
extend the routine store's existing normalized-name contract to full quoted-name
or schema-isolated overload semantics. Per-call cursor query clones avoid the
address-keyed statement caches reserved for persistent IR nodes.

The [reference comparison](reference-comparison.json) records twenty values
compared with Oracle Free 23.26.3.0.0 through separate Oracle and PostgreSQL
drivers. Five synthetic functions exercise sums, CONTINUE/EXIT, RETURN inside a
cursor loop, local values and nested cursors. Arguments alternate between two
nonempty buckets and an empty bucket, then repeat an earlier call. Nested Oracle
cursors use a declared local to capture the outer row's bucket. Temporary
functions and tables were removed from both databases.

The [original application probe](application-probe.json) calls iDempiere's
PAYMENTTERMDUEDATE with only an explicit destination namespace adaptation.
Its cursor syntax now parses, but execution fails while initializing a DATE
local: `TRUNC` on the DATE parameter still selects native numeric TRUNC. No
application result is qualified. The failed definition was removed and its
absence checked; existing GETDATE and CHARAT definitions still execute. Original
application source remains outside this repository. The three previously
qualified nullable UNIQUE constraints and their source values were rechecked
without repair after the development server upgrade.

Parameterized cursor declarations, REF CURSOR interfaces, cursor attributes,
bulk operations, streaming results, full cursor repositioning, Oracle error
contracts and general PL/SQL remain outstanding. Typed routine expressions and
application namespace resolution require further work. This evidence does not
qualify the complete iDempiere schema or its business workflows.

Validation passed: 352 SQL unit tests (three ignored), 416 SQL integration tests,
117 pgwire unit tests, fifteen Oracle-profile driver tests, 167 protocol tests
and fourteen Python migration-tool tests. Driver tests run the native PostgreSQL
and Oracle preview profiles, including alternating prepared arguments after
database reopen. Formatting, license and open-core boundary checks passed. The
development example server was rebuilt and the isolated application fixture
upgraded. No release was published or production deployment performed.
