# Dynamic timezone expressions in routines and triggers

Source `acfc830f24aa33d3873d579fb59ce2ddeef9d0aa` fixes session evaluation of
`AT TIME ZONE`. Timestamp and zone operands now resolve active routine values
and retain the declared timestamp/timestamptz direction. The existing timezone
implementation still handles offsets, DST, nulls and invalid-zone refusals.
The evaluator arm is outlined to preserve the ordinary interpreter stack.

Before the fix, literal-zone SQL works but routine variables and `NEW.zone`
fall through to constant-only evaluation. The public CLI reduction and two
failed integration tests reproduce that. After the change both tests pass:
summer/winter offsets, both typed timestamp directions, null propagation,
actual row-trigger output and rejected invalid-zone writes are covered.

The actual EHR development runtime subsequently creates and approves a
monitoring template **and starts its enrollment**, without changing clinical
eligibility, approval or timezone validation. The worker then fails with a
separate `42601` parser error (`open_h` is mistaken for an OPEN command); its
full monitoring/reminder contract remains failed and unadmitted. Patient/meal
API persistence, tenant refusals and acknowledged owned-sink mail pass. These
partial application passes are not full release or production acceptance.

Source checks all pass with one Cargo job, incremental compilation disabled
and the active 12 GiB RAM / 8 GiB disk headroom guard:

- SQL library: 763 passed, four existing ignored, zero failures.
- Eleven routine/domain integration targets: 35 passed, zero failures.
- Protocol integration: 188 passed, plus the procedural migration test.
- Protocol library: 140 passed, zero failures.
- Formatting and whitespace checks pass.

The 1,410 source/test/fixture inputs match the immutable source manifest
`8190e0ebbb527dc4aa6274dee34f9503ff7e880d92ac0acef4d4ff70905b3957`.
Raw failing and passing logs are retained here. No clinical data or production
keys were copied to the build host. Production remains on PostgreSQL; the
owner deferred HA and database-host admin access is still unresolved.
