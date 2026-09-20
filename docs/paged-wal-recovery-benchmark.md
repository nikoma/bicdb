# Paged WAL recovery certification

Status: clean-process benchmark shipped in BicDB 1.0.82-beta; attributable,
checksummed evidence and offline verification shipped in 1.0.83-beta.

`bicdb bench paged-recovery` measures the startup path implemented in
[`paged-wal-recovery.md`](paged-wal-recovery.md). It creates a database at a new
path, makes a configurable amount of row data durable behind a checkpoint,
creates a configurable post-checkpoint WAL suffix, exits that preparation
context, and launches a fresh BicDB process to recover the suffix.

Preparation and measurement are separate processes. This prevents memory
retained by fixture generation from being reported as recovery memory.

## Safety

The target passed through `--path` must not exist. The command fails rather
than deleting, reusing, or overwriting an existing directory. A successful run
retains the fixture and its WAL so the artifact can be inspected or reproduced;
the operator decides when to remove it.

The internal probe is not a general database command. It is hidden from CLI
help and is launched by the benchmark only after fixture creation succeeds.

## Release-gate example

Build the release binary, choose a new path on the storage device being
certified, and set limits from the deployment's recovery objective and memory
envelope:

```bash
cargo build --release -p bicdb-cli --features bench

./target/release/bicdb bench paged-recovery \
  --checkpointed-data-bytes 10737418240 \
  --wal-bytes 1073741824 \
  --record-bytes 1200 \
  --buffer-pool-bytes 536870912 \
  --page-size 8192 \
  --batch 1000 \
  --fsync true \
  --sample-interval-ms 2 \
  --max-recovery-ms 30000 \
  --max-peak-rss-bytes 1073741824 \
  --max-rss-growth-bytes 805306368 \
  --source-revision 0123456789abcdef0123456789abcdef01234567 \
  --cache-state cold \
  --cache-preparation 'host page cache dropped before process launch' \
  --require-release-evidence \
  --path /mnt/certification/recovery-10g-1g \
  --json-out reports/recovery-10g-1g.json \
  --csv-out reports/recovery-10g-1g.csv
```

If a declared limit is exceeded, the JSON and CSV evidence is written and the
command exits unsuccessfully. If RSS is unavailable on the platform, declaring
an RSS limit also fails closed rather than silently passing.

`--fsync true` is required for durability certification. `--fsync false` is
useful only for algorithmic smoke tests.

`--require-release-evidence` performs its environment preflight before the
database path is created. It requires durable I/O, a full 40- or 64-hex source
revision, an explicit `cold` or `warm` cache declaration, bounded cache
preparation details, and available kernel, CPU, memory, filesystem, mount,
source, and device identity. The exact executable is streamed through a fixed
1 MiB buffer and recorded by byte size and SHA-256. A source revision is an
operator/build-pipeline assertion; the executable digest is the authoritative
identity of the measured bytes.

## Proving database-size independence

One run cannot prove that recovery is independent of total database bytes. Run
a matrix that holds the WAL suffix and all other options fixed while increasing
`--checkpointed-data-bytes`, then a second matrix that holds checkpointed data
fixed while increasing `--wal-bytes`.

Minimum release evidence:

| Matrix | Checkpointed logical row bytes | WAL suffix bytes |
| --- | ---: | ---: |
| database size | 0, 1 GiB, 10 GiB, largest available fixture | fixed at the production checkpoint target |
| suffix size | fixed at 10 GiB or the largest available fixture | 64 MiB, 256 MiB, 1 GiB, maximum allowed suffix |

The gate passes only when:

- every report has `passed = true` under the declared limits;
- recovery performs exactly two WAL passes;
- `wal_bytes_scanned` equals the generated suffix;
- `peak_record_bytes` remains below the hard one-record bound;
- sampled checkpointed and recovered suffix rows are readable;
- time and RSS track suffix work within the declared tolerance rather than
  checkpointed database size; and
- results record binary hash, hardware, kernel, filesystem, mount options,
  storage device, durability settings, and cache state.

The command does not evict the operating system page cache. Cold-cache
certification must control cache state externally and record how it was done.

## Artifact schema

The JSON artifact has `format_version = 2` and four sections:

- `environment`: BicDB version and source revision, exact executable path,
  byte size and SHA-256, database path, OS/architecture, kernel, CPU, logical
  CPUs, host memory, filesystem type/source/mount/options/device, declared
  cache state and preparation, and capture time;

- `fixture`: requested and actual checkpointed bytes, checkpointed file bytes,
  requested and actual WAL bytes, row and transaction counts, page/pool
  configuration, durability mode, and generation time;
- `probe`: clean-process open time, RSS before/peak/after, RSS growth, sampled
  row verification, and the strict engine `RecoveryReport`; and
- `limits`: the operator-declared ceilings plus `passed` and concrete failure
  reasons.

The top-level `checksum_sha256` covers the complete normalized report with the
checksum field empty. Verification recomputes the environment/fixture/probe
bindings, every structural and operator gate, the `passed` value, failure list,
and checksum; it does not merely trust the serialized result:

```bash
./target/release/bicdb bench paged-recovery-verify \
  --report reports/recovery-10g-1g.json
```

The verifier refuses symlinks, empty files, non-regular files, reports larger
than 16 MiB, unknown JSON fields, schema mismatches, pass/failure outcomes that
disagree with their measurements, and checksum mismatches. The checksum
provides artifact integrity, not signer identity; signed release-bundle
publication remains a separate outer control.

Record contents, keys, SQL text, and other unbounded-cardinality values are not
included in the report.

## Development smoke evidence

The implementation was smoke-tested on 2026-08-04 with an unoptimized binary,
an AMD Ryzen 9 7900X host, Linux 6.8, and an ext4 filesystem. Durability was
disabled, so these are not production performance claims.

| Checkpointed logical data | Actual WAL | Recovery open | Peak RSS | RSS growth | Result |
| ---: | ---: | ---: | ---: | ---: | --- |
| 0 | 68,985,480 B | 450.3 ms | 108,789,760 B | 46,047,232 B | pass |
| 67,109,724 B | 68,170,512 B | 520.8 ms | 130,363,392 B | 71,581,696 B | pass |

Both runs used two scans, an 8,232-byte peak record buffer, and verified all
selected rows. The small two-point smoke confirms the harness and evidence
schema, not the production-size independence gate.
