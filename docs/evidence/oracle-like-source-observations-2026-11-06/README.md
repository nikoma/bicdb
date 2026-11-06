# Read-only Oracle LIKE escape observations

The [nineteen-case source record](summary.json) captures read-only observations
from Oracle 23.26.3.0.0. It is reference evidence, not BicDB qualification. The
owned reference service was restored to its original stopped state and no
original application objects were changed.

In the recorded CASE predicates, explicit NULL and empty escape values,
including bound and typed NULLs, return ORA-01425. The error also occurs with NULL source or pattern operands and an
invalid escape length. Default backslash is literal; explicit backslash and pipe
escapes match. Trailing escapes and invalid escape pairs return ORA-01424.

The Unicode case returned ORA-01425 on this reference. Its character-set and
client-encoding dependencies require additional investigation; do not interpret
that observation as a general restriction on Unicode escapes. Expression-valued
escapes and full pattern/error behavior require separate implementation and
transport acceptance. PostgreSQL NULL escape semantics remain independent.

The [Oracle pattern-matching reference](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Pattern-matching-Conditions.html)
describes NULL operands as producing UNKNOWN. The recorded source behavior
therefore needs version and expression-context qualification. Additional row
filter, empty-input and encoding controls are required before extending these
CASE observations to all source statements.

The [follow-up environment and context record](environment.json) adds source
NLS metadata, literal/bind byte dumps, three Unicode variants and five filtering
contexts. It confirms AL32UTF8, AL16UTF16, BINARY comparison and BYTE length
defaults. The ordinary literal and bound `§` values are each one character and
two UTF-8 bytes; their explicit escapes return ORA-01425. The national-literal
variant matches. Row predicates with explicit NULL/empty escapes return
ORA-01425, including a NULL input row; an empty input relation returns count
zero. The reference service was again restored to stopped state.

These observations justify separate ordinary and national execution contracts
for this source version. They do not qualify BicDB's new helper or implement
national LIKE, arbitrary collations, LIKEC, LIKE2 or LIKE4. The typed lowering
continues to reject national text patterns until that contract is implemented.
