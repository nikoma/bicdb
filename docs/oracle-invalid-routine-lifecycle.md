# Oracle invalid return declarations

The Oracle preview profile retains a function whose declared return type is
unknown as an INVALID object. CREATE reports compilation errors; calls reject
with ORA-06575. Replacement by a valid declaration clears the diagnostics, and
DROP removes the object. PostgreSQL independently rejects an unknown return type
with SQLSTATE 42704. CREATE OR REPLACE checks the existing routine's owner.

Invalid state, the original return declaration and diagnostics live in the same
routine metadata record. A sticky `oracle_invalid_routines_v1` core format flag
is persisted before storing invalid metadata. Older readers must refuse this
store rather than execute the physical placeholder return type. Executable-cache
lookup and compilation both reject invalid routines.

The preview USER_OBJECTS fallback provides OBJECT_NAME, OBJECT_TYPE and STATUS
for owned supported catalog objects. USER_ERRORS provides NAME, TYPE, SEQUENCE,
ATTRIBUTE, TEXT and MESSAGE_NUMBER. Other roles' objects are filtered. Local
relations and CTEs take precedence; explicit SYS names select the fallback.
These are bounded dictionary views, not complete Oracle dictionary emulation.

This change covers unresolved return declarations. Arbitrary PL/SQL compiler
errors, ALTER COMPILE, dependency-driven invalidation, automatic recompilation
and exact native source line/column locations are outside this implementation.

## Evidence

The isolated native Oracle 23.26.3 control fixture is checked in as
`crates/bicdb-sql/tests/fixtures/oracle/invalid_routine_lifecycle_oracle_control.json`.
Its CREATE warning, INVALID state, two diagnostics, ORA-06575, valid replacement
and DROP were observed against the disposable BICDBAPPCTRL control database.

Source e6b2f06a failed all four initial focused controls. Source 448e703d passed
six focused controls, including dictionary ownership and name shadowing. Source
664ca3c0 passed the same six with an optional closed-store capture hook. The
capture hook initially failed to compile at 9d4077be because the storage mode was
moved; that run executed no tests.

An actual reader built from fccdf8230f675d03d8216d152dd1dfdd5e803e57,
SHA256 c91308f11dae618d83570323e1c364830686c79b9f38e256792ef140c08ee3ee,
refused the captured 664ca3c0 store specifically for
`oracle_invalid_routines_v1`; hashes of all store files were unchanged. This is
format compatibility evidence. The semantic fixture disables fsync and is not
power-loss durability evidence. The current reader's flush/reopen lifecycle is
covered in both storage modes by the focused regression.

These checks are separate from original unchanged iDempiere application tests,
the two original 10-second timeout cases, and final-artifact VM qualification.
