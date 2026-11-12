# Generated columns, physical keys and partition updates

This evidence covers private SQL runtime `5aa92dcecb29b639251acc81a5d60428dfae96f7`,
composed with main489. It is not a merged version, standard CLI, release,
deployment, iDempiere acceptance or distributed HA qualification.

Actual PostgreSQL18.4 controls, using pinned image
`sha256:a02db8cac496f15b094798a38254f14d6e00741f709360e5e00bb6668ea31636`,
confirm physical leaf generation on parent UPDATE, destination recomputation on
movement, source-expression error precedence, generated primary keys, BEFORE
trigger NEW values, computed-key upsert, foreign-key actions and JSONB null keys.
All six reference runs passed; each disposable container was removed.

The valid test-only baseline `d8f8d00e` over unchanged `6c16e66e` passed two tests
and failed ten. Earlier harness failures (E0507, E0308 and a missing Git bundle
prerequisite) remain distinct from executed product tests. Candidate `c7c21f97`
passed ten and failed two because computed-key UPDATE lacked its implicit
statement transaction. Source `5aa92dce` corrects that shared admission rule and
retains the preceding generated-column fixes and all original assertions.

At exact `5aa92dce`, all **123 focused tests passed**, with zero failed or ignored:
thirteen generated-column/key controls, ninety-seven related integration tests,
and thirteen upgrade-admission units. Tests cover both storage modes, RETURNING,
trigger base/key changes, SQL UPDATE permissions, uniqueness and null failure
rollback, physical old-key removal, foreign-key actions and reopens. These do
not independently prove signed Native mutation authority.

Recovery was independently rerun with source-pinned probes:

| Scope | Passed | Failed |
| --- | ---: | ---: |
| Genuine old482 default, explicit operator and entry/startup controls | 30 | 0 |
| Process cuts, including specified volatile-tail/directory emulation | 152 | 0 |
| Actual one-shot and persistent EIO injections | 96 | 0 |

An independent verifier checks exact case coverage, binary SHA, all fourteen
original ACKs and immutable old-file manifests, three stable opens, Native index
checks, acknowledged-new recovery, and old-reader refusal without byte changes.
These are overlapping case invocations over fourteen original ACKs, not hundreds
of independent application ACKs. The crash probe SHA is
`7aa88ec9b9de0181f529937d25d977e6a472cb2a0e457390cef9aa910fe035c4`.

Full Core/SQL/protocol suites at earlier `6c16e66e` passed 1,699/2,573/491 tests
respectively, with 17/4/2 ignored and no failures. Only unchanged Core coverage
carries forward to `5aa92dce`; the changed SQL and protocol must be requalified.
The full `5aa92dce` SQL/protocol batch was live at this evidence checkpoint.
The separate [VM evidence](../vm-recovery/README.md) covers actual guest RAM loss.

`original-source-scoped-results.tar.gz` preserves original reports, stdout/stderr,
reference SQL/results and controller scripts. `archived-files.json` records each
member's size and SHA256. Scratch databases and binaries are excluded. Source
files for the implementation and thirteen controls are tracked in this branch.
Protected Native authority, remaining lifecycle/replication gates, application
acceptance and final versioned merge remain open; no production promotion follows.

The subsequent test-only Native-authority probe `3545a445` executed four tests:
two passed and two failed. The original mutation-authority regression and the
new signed same-leaf UPDATE positive passed. A computed-key change incorrectly
attempts Native DELETE with an UPDATE grant; a grantless parent UPDATE correctly
refuses but reports XX000 instead of 42501. The grantless case stops at that
first mismatch, so later cases in its loop are not yet proved. These failures
are retained as open delivery gates, not waived or counted as passing. The
positive computed-key fixture will explicitly include `id` column authority so
future Native identity movement is covered by its grant as well as `k` and `b`.
