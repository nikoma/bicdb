# Original-run forensic projections

See [the status matrix and issue reports](../../azure-original-run-forensics-2026-11-11.md).
These projections read only original retained controller files and five original
pre-diagnosis cold archive members. No database open/replay/extraction, VM action
or new workload occurred. The failed 1.0.461 run remains failed.

- `two-ack-range-forensics.json`: hashed key joins, exact range/epoch/leader and
  local watermarks, later routed-read times, untimestamped range-log fragments.
- `diagnostic-progress.json`: all 68 numeric polls, bounds and failed move IDs.
- `failure-sample-classification.json`: 404 logical failures versus only 20
  retained samples; absent evidence explicitly labeled.
- `finding-statuses.json`: machine-readable passed/failed/inconclusive/not-tested
  classifications, with narrow criteria defined in the forensic report.
- `input-provenance.json`: original private-input hashes and exact projector
  identity. `manifest.json` hashes newly exported public bytes separately.

Raw keys, endpoints, certificates, account IDs and database archives are excluded.
Command UUIDs in log fragments are replaced by their SHA-256. Source logs lack
per-key response correlation; fragments establish no original-call root cause.

Reproduce the projections against retained **private originals** without opening
BicDB (output to a new disposable directory):

```sh
python3 scripts/project-azure-original-run-forensics.py --run-dir /path/to/original-long-comparison --output-dir /tmp/bicdb-forensic-recheck
```

Compare all output JSON with these projections, and verify manifest digests.
This is static forensic evidence, not an engine recovery test or qualification.

The portable checker does not read private inputs unless explicitly supplied:

```sh
python3 scripts/verify-azure-original-forensics.py
python3 scripts/verify-azure-original-forensics.py --run-dir /path/to/original-long-comparison
```
