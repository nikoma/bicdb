# BicDB TPC-C campaign harness

See `docs/performance-campaign.md` for the campaign workflow and
`docs/benchmarking-tpcc.md` for workload-specific background.

```bash
cargo build --release --bin bicdb

# One-time immutable seed.
SEED=/dev/shm/bicdb-tpcc-seed WAREHOUSES=16 \
  bench/tpcc/build_seed.sh

# Full baseline: three warmups, six balanced permutations (18 unprofiled
# throughput trials), three latency profiles, and three allocation profiles.
CAMPAIGN_ID=20260709-head-baseline \
SEED=/dev/shm/bicdb-tpcc-seed \
DATA_ROOT=/srv/bicdb-bench \
  bench/tpcc/run_matrix.sh
```

`run_trial.sh` is the single-trial primitive. `run_matrix.sh` is the normal entry
point. Every trial emits a versioned `result.json`; `summarize.py` produces the
campaign-level `summary.json` and `summary.md`.

Compare a candidate with its baseline using the durable lane it targets:

```bash
python3 bench/tpcc/summarize.py compare \
  reports/performance/<baseline> reports/performance/<candidate> tuned-durable
```

`dev9_trial.sh` and `lockab.sh` remain as historical drivers. New performance
decisions must use the campaign harness.
