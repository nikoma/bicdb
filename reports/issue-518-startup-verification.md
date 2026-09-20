# Issue 518: provisioning and generated API startup

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](../docs/source-distribution.md).

Runtime fixes are in `7ce6d8fd08f5fc5c6a6537f982562943ca15fbc5` (PR #894).
The validation used an isolated database and copied application artifacts on the
existing build VM. Application sources and migrations were not modified.

## Findings and changes

The original missing `HEALTH_AI_LLM_API_KEY` was a smoke-environment omission,
not a SQL engine failure. Every required generated configuration variable was
supplied with a test-only value for this run.

The reported `DECLARE` / `FOREACH ... SLICE 1` form already executes on current
BicDB. A new pgwire regression runs a two-relation compatibility migration twice
and verifies preserved data through the renamed tables and their old views.

The complete current migration chain exposed another parser failure in migration
`0060_public_program_enrollment_page.sql`: a function REVOKE with a newline before
FROM returned SQLSTATE 42601. The fix recognizes SQL keyword boundaries instead
of literal spaces. SQL regressions verify actual revoked/granted execution rights
with newlines, tabs, CRLF, repeated spaces, and quoted role names.

## Validation

| Check | Result |
| --- | --- |
| Original fixture, first and second pgwire pass | Passed |
| Current Carrier migrations | 80 applied |
| Repeat Carrier migration command | 0 applied; passed |
| Generated API with a restricted runtime SQL role | Started |
| GET `/health` | HTTP 200, `{"ok":true}` |
| GET `/patient-portal/invites/nonexistent-token` | HTTP 200, `status: invalid` |
| Full `bicdb-sql`, `bicdb-pgwire`, and `bicdb-cli` suites | Passed |
| Formatting, diff, and production-name guard | Clean diff; same 50 pre-existing guard matches |

The unoptimized binary completed all 147 relation checks and 2,733 required-column
checks, then became ready in 62.63 seconds. A 300-second diagnostic allowance
established that startup was progressing rather than blocked. Optimized-build
verification with the original 60-second startup allowance passed: **9.03 seconds**.
All 80 migrations, their repeat run, and both HTTP checks also passed against a
fresh database with the optimized binary.

Compiler: Rust 1.96.0. Carrier: 2.3.75. Node: 22.23.1. Compilation ran only on the
existing VM, with `CARGO_BUILD_JOBS=2`. The standalone original migration was
recovered from `832c649d^:fixtures/walknorth_ehr_20260620/0001_initial.sql`;
SHA-256 `f77fdb0fd45cf3806bce0d56a96e40ad00f38d36a7cf2ebeb906714498ab150b`.
The product-specific fixture and runtime remain outside the BicDB repository.

Coverage is provisioning, authenticated pgwire login integration, schema readiness, and
these two HTTP smoke routes. GitHub-hosted checks were unavailable; the VM suites above actually executed.

Optimized binary: `/home/benchmark/bicdb-transaction-delegation/target/release/bicdb` on the
existing build VM, built from the runtime source at the commit above. SHA-256:
`481b3fa3eb4f53f5d01b0b765e9e7e61182b8e3565712998c08d7ebdc07daf41`.
This is a built and tested artifact, not a published release or deployment.
