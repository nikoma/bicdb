# Invalidated pre-hardening smoke

This VU2 campaign ran before the release-validity contract was hardened. Its
raw runtime artifacts were intentionally removed during publication curation,
so HammerDB exit/completion, failed-query, and requested profiling evidence
cannot be revalidated.

The current schema-v2 summarizer therefore marks every trial invalid. The
retained metadata and measurements are historical harness-screening evidence
only; they are not a performance baseline and cannot support a keeper or reject
decision.
