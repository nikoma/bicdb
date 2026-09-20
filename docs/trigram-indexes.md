# Trigram substring indexes

`CREATE INDEX ... USING GIN (text_column gin_trgm_ops)` creates a real inverted
index using BicDB's transactional posting storage. One non-unique text
expression is supported. Its persisted source expression drives index maintenance
on insert and update; deletes, transaction rollback, and restart use the ordinary
inverted-index lifecycle.

Positive LIKE and ILIKE predicates can use a literal three-character ASCII
substring to select candidates. The full SQL predicate, including RLS and other
WHERE conditions, still checks the records. Every non-ASCII value remains a
candidate to avoid incorrect exclusions from Unicode case folding. Short or
non-ASCII patterns, explicit ESCAPE clauses, negation, and other unsupported
candidate shapes use the ordinary scan path. Partial-index declarations are
accepted with conservative postings for all rows; filtering still applies the
query's predicate.

This implements a subset of PostgreSQL's
[trigram index interface](https://www.postgresql.org/docs/18/pgtrgm.html).
It does not implement its similarity functions/operators, GiST variant, or
regular-expression index acceleration. The internal tokens are substring
candidates, not PostgreSQL's `show_trgm` representation. No Hub capacity or
substring-search throughput claim follows from this compatibility support.
