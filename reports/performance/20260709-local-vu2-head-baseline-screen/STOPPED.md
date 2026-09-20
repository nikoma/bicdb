# Stopped local 2-VU baseline screen

This local screening campaign was stopped after the benchmark-host mismatch was
identified. It did not execute any measured trials and is not a current-HEAD
baseline.

- The discarded `default-durable` warmup produced 41,824 DSUM NOPM.
- The discarded `tuned-durable` warmup produced 43,975.33 DSUM NOPM.
- The `cpu-ceiling` warmup was interrupted before it produced a result.
- All runs used two VUs on `development-host`, not the canonical VU32 configuration on
  `benchmark-primary`.

The generated 0/0 aggregate summary was removed because zero is not a valid
substitute for unavailable measured-trial metrics. Curated trial metadata and
the two completed warmup results are retained for provenance; runtime logs,
HammerDB state, and other raw scratch artifacts are excluded from Git.
