# Test-only observer cost spans

Base: `d7376271e823e2c945fbde682dd386953f0cce7e`, source `1.0.489-beta`. This patch adds only `cfg(test)` instrumentation to `db/range_image_native/serving_configuration/observer_scope.rs`. It does not implement an optimization or demonstrate which inner check dominates the exact68 prerequisite failure.

The existing `NativeProbeTraceSpan::current` supplies the original phase, node, challenge, session, scope and deadline. Its existing RAII destructor records unfinished spans on early error or unwind. Successful calls consume the trace with `finish`; no result is swallowed or reconstructed. With no active trace context, the existing helper returns None. There is no new context, role, token, deadline, worker, journal, database operation, network call, or validation helper.

| Span suffix after `observer-` | Exactly bracketed existing work |
| --- | --- |
| `identity-full-writer` | Actual writer `require_database` on every existing scope identity call |
| `identity-owned-native-pin` | Existing owned native pin/current admission check |
| `identity-native-fields` | Existing observer-to-actual-native identity fields check |
| `constructor-full-writer` | Initial full writer/database check before journal acquisition |
| `constructor-journal-acquisition` | Existing actual journal try-lock |
| `constructor-full-physical` | Full owned physical materialization validation |
| `constructor-actual-lineage` | Actual lineage construction under that journal |
| `constructor-initial-identity` | Existing initial scope identity check, including its nested three identity spans |
| `constructor-joint-fact` | Existing protected joint-domain construction, including any nested identity calls |
| `constructor-prepared-binding` | Existing prepared/current-domain/saved catalogue/serving checks, including nested identity and serving spans |
| `serving-current-readback` | Existing native serving current read and its validation |
| `serving-archive-readback` | Existing archive read plus historical-floor validation |
| `exit-initial-identity` | Existing first full exit identity check |
| `exit-actual-lineage` | Existing actual post-callback lineage validation |
| `exit-full-joint-saved-binding` | Existing full protected joint and saved-floor revalidation |
| `exit-prepared-binding` | Existing full prepared/binding revalidation |

The final unchanged exit identity call emits the same three identity subspans. All existing outer entry/exit, recipient role, scope/challenge, saved/archive floors, current/archive readback, writer, physical, pin, slot, schema, health and original-parent checks remain in their original order. The original callback and fault hook positions are unchanged. Standalone and non-CurrentJoint paths are unchanged.

Spans are **inclusive**: constructor/joint/prepared/exit parents contain nested identity and serving children. Do not add their durations as independent physical costs. Each repeated identity check has its own emitted begin/end pair on the same actual request; count repetitions before attributing cost. Logging overhead is included. The measurements do not establish a disk-stall bound, isolation, capacity or a performance improvement.

Static audit removed the one newly added cfg(test) import, 16 newly added cfg(test) trace starts and 16 finishes; the remaining file equals the base after blank-line normalization (SHA-256 `1331aa4425e35d3b809b865606ce5f9a042988af0851785002ba72ba4c5f633b`). This comparison includes every original validation, return and signature. Rustfmt and diff checks passed; no Cargo or runtime test ran for this instrumentation patch. The next comparable runtime selection is the unchanged four-control `native_current_joint_activation_scope_actual_` group, preserving its actual missing-archive prerequisite failure and original budgets if it recurs.
