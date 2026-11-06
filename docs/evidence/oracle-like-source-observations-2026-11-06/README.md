# Read-only Oracle LIKE escape observations

The [nineteen-case source record](summary.json) captures read-only observations
from Oracle 23.26.3.0.0. It is reference evidence, not BicDB qualification. The
owned reference service was restored to its original stopped state and no
original application objects were changed.

Explicit NULL and empty escape values, including bound and typed NULLs, return
ORA-01425. The error also occurs with NULL source or pattern operands and an
invalid escape length. Default backslash is literal; explicit backslash and pipe
escapes match. Trailing escapes and invalid escape pairs return ORA-01424.

The Unicode case returned ORA-01425 on this reference. Its character-set and
client-encoding dependencies require additional investigation; do not interpret
that observation as a general restriction on Unicode escapes. Expression-valued
escapes and full pattern/error behavior require separate implementation and
transport acceptance. PostgreSQL NULL escape semantics remain independent.
