# Private local namespace consumer qualification

This evidence belongs to private runtime `0d020d5c8c11e8456dd0b23841ae64d97b0c4434`, integrated from private catalog reader `3f755fd8` and public namespace candidate `a700710b`. It is not a main delivery, release, application acceptance, or replicated promotion qualification. The source version in this development integration is inherited from the namespace candidate; it is not a new version reservation.

Rename images now carry both the physical storage alias and the actual persistent local namespace UUID. Recovery validates every remaining image before changing any backing or row. Drop/recreate with the same logical name, physical storage alias, and row key cannot authorize replay into the replacement. Missing and nil identities refuse recovery. A trusted SQL initializer checks SQL journal quiescence before calling Core's namespace activation, and profile initialization runs it before accepting application transactions. Completed identity initialization requires no catalog mutation. Oracle's existing startup-only case-index migration still requires native quiescence.

## Actual outcomes

| Source | Result and scope |
| --- | --- |
| `35e5839d` | Integration compile stopped: SSI expected `Arc<()>`, while the private catalog registry now stores `CatalogBinding`. No tests ran. |
| `f2c25a66` | SSI uses the existing binding's incarnation; test compilation then stopped because the new fixture moved its storage-mode enum before formatting the failure. No tests ran. |
| `72da2fe2` | Unchanged storage-string recovery failed both new refusal assertions: **0 passed, 2 failed**. Both stopped in EmbeddedMemory before reaching the second mode. |
| `aee80da2` | UUID consumer: **6 focused tests passed**, including positive recovery, business-commit decisions, missing/nil images and activation refusal. Tests iterate both storage modes where specified. |
| `6fa6b58b` | **All 13 journal tests passed**. New profile controls: **1 passed, 1 failed** because the test incorrectly assumed the complete Oracle startup initializer admits live native transactions. The existing case-index migration explicitly requires quiescence. |
| `65e54d42` | **13 journal + 2 profile tests passed** after preserving that existing Oracle restriction. Core test compilation then stopped at a newly integrated namespace fixture missing the private `required_reader_features` field. |
| `0d020d5c` | Added the explicit empty capability set to that fixture only. **15 SSI + 1 catalog unit + 27 SQL catalog integration tests passed**. A second focused batch passed **25 native catalog integration + 11 namespace foundation + 9 TCP/metadata tests**, with **one existing ignored frozen-reader test**. |

The 65 and 0d production runtime is identical; their difference is a Core test fixture field. These results provide 103 passing focused executions across those scopes, with one existing ignored test. They are not full Core/SQL regression suites or application acceptance. Earlier full suites cover their own recorded sources only.

All eight batch specifications, original receipts and complete compressed logs are retained. `manifest.json` records source revisions and receipt/spec/raw/compressed hashes. Compiler failures and incorrect fixture assumptions remain visible. No upstream application assertion, bytecode, original 10-second wait, or fsync guarantee was changed.

## Open completion gates

A source node's local UUID is not a portable logical object identity. Native logical imports deliberately mint or retain the target's actual local UUID. Streamed rename images therefore require an authenticated logical-origin/backing-generation to actual-target binding established at creation/import and included in recovery's durability domain. That binding is not implemented here. Local replay now refuses a donor identity that cannot be proved locally; replicated undecided-DDL promotion remains unqualified.

The SQL producer and recovery still retain their conservative global transaction gates. Full dependency reservation, all catalog readers and metadata paths, live concurrent rename, replicated recovery, broad regressions, crash/VM and final application qualification remain required before this private campaign can be delivered. The observed original PostgreSQL MTable partition failure has not been claimed fixed by these component checks.
