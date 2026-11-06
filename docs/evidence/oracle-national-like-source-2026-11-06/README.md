# National LIKE source observations

The read-only Oracle reference corpus records 82 predicates and two filtering
contexts on Oracle 23.26.3.0.0, with AL32UTF8 database text, AL16UTF16 national
text and BINARY comparison/sort settings. Every observation retains its exact
SQL, binds, truth result or Oracle error code in [observations.json](observations.json).
The original application objects were unchanged and the reference container was
restored to its previous stopped state.

| Source character family | Supplementary character LIKE `_` | LIKE `__` |
| --- | --- | --- |
| Ordinary database text | true | false |
| National text | false | true |

The first operand's family also governs mixed-family patterns and escapes in
this corpus. Ordinary text rejects the tested multibyte escapes with ORA-01425;
national text accepts a single BMP UTF-16 unit, while a supplementary escape is
two units and is rejected. Fixed-width padding, NULL/empty escapes, typed NULL
operands and an empty input relation have separate recorded controls. These
results do not qualify other national character sets, linguistic collations,
LIKEC/LIKE2/LIKE4 or general Oracle conformance.

The source observer now exposes `--contract national`. Its default ordinary
contract remains 22 predicates and five contexts. Seven unit controls verify
corpus selection, read-only execution, cleanup and error classification. They
use a fake driver and are not source-database acceptance. ORA-12704 is classified
as a semantic error; authentication, cancellation, TNS and driver failures abort
observation and retain rollback/close behavior.

The [previous compiler control](previous-compiler-control.json) independently
runs the retained v29 backend on a disposable database. Its national routine
uses Unicode-scalar matching (one underscore matches, two do not), and standalone
national LIKE is refused. This is evidence of an existing compatibility gap,
not successful Oracle equivalence. A new versioned execution contract and durable
national column provenance remain implementation and qualification work. Current
runtime work must preserve the behavior of persisted older routine definitions.

See [summary.json](summary.json) for exact source/tool identities. This checkpoint
publishes reference evidence only; it does not claim national LIKE support,
complete application acceptance, a release, deployment or production qualification.
