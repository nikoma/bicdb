# Step 0: can a tiny model beat BicDB's access-path choices?

Measurement only. No model was trained. Harness: `crates/bicdb-bench/src/bin/plan-regret.rs` and
`bench/plan-regret/analyze.py` (see `bench/plan-regret/README.md`). Host: dev9, in-process engine,
default (memory-resident) storage, release build without LTO, run serially with nothing else
measuring on the host.

## Headline

On realistic workloads, BicDB's **executed** chooser is within measurement noise of the best legal
plan. There is no regret for a 100–1,000-parameter model to recover. The only real regret appears
on adversarial shapes built to break the rule (see *Challenge*). Those cases are fixable with a
deterministic rule, not a learned cost model.

## Architecture finding: three choosers, and EXPLAIN described one that didn't run

| SELECT shape | chooser that executes |
|---|---|
| ordinary read (projection, LIMIT, aggregates, GROUP BY, DISTINCT, ranges, OR) | rule-based **row-set locator**: most matched B-tree index columns wins; ties broken by running the tied probes; full scan only when no index matches |
| `WHERE prefix = … ORDER BY next_col LIMIT k` | bounded ordered index path |
| any read in a transaction with pending writes on the table | **pending-writes** record-id chooser (same rule, but probes every candidate) |
| EXPLAIN / EXPLAIN ANALYZE (before the follow-up fix) | the **cost-based planner** (`plan_query`), executed through its own loader |

The cost-based planner also runs twice per ordinary SELECT, and its result is discarded. Before
the follow-up fix, EXPLAIN disagreed with execution on **23 of 48** shape × condition checks
(`tests/explain_matches_execution.rs`).

## Workloads

`orders` table (Zipf customers, skewed status with region correlation, time-correlated days),
five B-tree indexes including two composites. Nine query templates: equality, composite equality,
ranges, BETWEEN, equality + range, ORDER BY … LIMIT, and an unindexed predicate. Parameters span
selectivity from about 10⁻⁵ to 0.8.

| dataset | rows | row width | queries | reps |
|---|---|---|---|---|
| narrow | 100k | 16–64 B note | 300 | 5 |
| wide | 100k | 1.2–2.4 KB note | 150 | 5 |
| narrow-large | 400k | 16–64 B | 100 | 3 |
| challenge | 100k | 16–64 B, extra `(status, created_day)` index | 150 | 5 |

Each legal candidate of the executing chooser, plus a full scan, was forced. Candidate order was
reshuffled on every repetition, under two cache conditions: warm, and CPU caches flushed (256 MiB
written before each run). Every forced plan's result was hashed and compared with the full-scan
result: **0 mismatches** across all datasets. Medians of the repetitions are used. Median noise
(MAD/median) is 0.7% warm and 0.8% flushed.

## Results (three realistic datasets, 450 queries, held-out test split, warm)

| policy | p50 ms | p95 ms | workload regret | p95 query regret | >2× | >10× |
|---|---:|---:|---:|---:|---:|---:|
| **rule (executed today)** | 8.7 | 582 | **0.0%** | 1.33× | 8 | **0** |
| cost-based planner, no stats | 13.3 | 596 | 8.6% | 13.29× | 44 | 19 |
| per-index fitted constants | 13.3 | 596 | 8.6% | 13.29× | 42 | 19 |
| fitted constants + latency calibration | 11.1 | 582 | 6.9% | 5.02× | 30 | 9 |
| cost-based planner after ANALYZE | 12.9 | 582 | 8.8% | 12.53× | 42 | 17 |
| oracle | 8.3 | 582 | 0.0% | 1.00× | 0 | 0 |

Flushed-cache results are the same to within 0.1 percentage point.

Per dataset (warm workload regret / >10× count), rule vs. ANALYZE: narrow 0.1% / 0 vs. 8.7% / 9;
wide 0.0% / 0 vs. 4.7% / 5; narrow-large 0.0% / 0 vs. 10.0% / 3.

### Cardinality estimation (planner, q-error against rows actually fetched)

| estimator | median | p90 | p95 | share >10× |
|---|---:|---:|---:|---:|
| per-index, no stats | 4.67 | 3,333 | 10,000 | 37% |
| per-index, fitted constants | 4.44 | 50 | 285 | 38% |
| per-index, after ANALYZE | 1.11 | 51 | 162 | 23% |
| query output (full-scan estimate), with or without stats | 476 | 100,000 | 100,000 | 89% |

ANALYZE fixes per-index estimates, yet plan regret does not improve (8.6% → 8.8%). The cost
model's error is in **pricing**, not cardinality:
- An index fetch is priced at 0.1–2 per row against a full scan's 1 per row. In this storage mode
  per-row decode dominates and is the same for either path, so even at 80% selectivity the index
  is as fast as the scan, or faster.
- The planner's query-output estimate ignores statistics entirely.

Planning overhead is about 0.06 ms p50, against a 7 ms p50 oracle query.

## Challenge (adversarial)

Templates aimed at the rule's weak spots (status restricted to the common values 0 and 1):

- `trap_longer_prefix`: `region = r AND status = s AND customer_id = c`. The 2-column
  `(region, status)` prefix beats the 1-column but far more selective `customer_id`.
- `trap_tied_huge_probe`: `status = s AND customer_id = c`, a tie where one probe returns about
  80% of the table.
- `trap_tied_range_probe`: `status = s AND created_day >= d AND region = r`, a tie between
  `(region, status)` and `(status, created_day)`, where the latter is huge.

| policy (challenge, test split, warm) | p50 ms | workload regret | max regret | >10× |
|---|---:|---:|---:|---:|
| rule (executed today) | 6.48 | **28.9%** | 30.1× | **15** |
| cost-based planner, no stats | 6.48 | 44.0% | 30.1× | 15 |
| per-index fitted constants | 1.24 | 43.4% | 9.0× | 0 |
| cost-based planner after ANALYZE | 1.24 | 2.1% | 2.6× | 0 |
| **narrow hybrid** (below) | 1.24 | **2.1%** | 2.6× | **0** |

Over all queries in each template, the losses come entirely from `trap_longer_prefix`:
165.7% regret, with 23 of 51 queries more than 10× slower. Tied probes cost almost nothing (0.9%
and 0.0%), so the rule's tie-probing is cheap enough. ANALYZE estimates rank the trap correctly
even though the composite estimate is value-blind: it says 263 rows for `(region, status)`
whether `status` matches 80% of rows or 10%.

### The narrow hybrid

Keep the executed rule, its candidates, and its exact tie probes. With ANALYZE statistics, let a
candidate with **fewer** matched columns (one the rule discarded without probing) replace the
rule's choice when its estimated rows are at least *k*× smaller. Never introduce a full scan.

| all four datasets (600 queries, test split, warm) | workload regret | max regret | >10× |
|---|---:|---:|---:|
| rule | 0.4% | 30.1× | 15 |
| narrow hybrid, any *k* from 1 to 8 | **0.1%** | **2.7×** | **0** |
| cost-based planner after ANALYZE | 5.2% | 51.7× | 13 |

The realistic datasets are unchanged under the hybrid (0.0–0.1%, no catastrophic choices), and
the result does not depend on *k* in that range. A naive hybrid that ranks *all* candidates by
estimate, overriding the tie probes too, is worse on realistic data: narrow 2.6% with 5
catastrophic, wide 5.8% with 7. Exact probes beat estimates wherever the rule already has them.

## Follow-up: the hybrid in the engine, measured end to end

The hybrid is implemented in the locator behind an internal, default-off gate
(`BICDB_LOCATOR_HYBRID`, ratio `BICDB_LOCATOR_HYBRID_RATIO`, default 2; #31). The forced-candidate
scores above leave out the chooser's own probe cost, so this run times the **unforced** statement,
as a client sees it. It also records planner, decision and wasted-probe time separately.

**Validation on held-out data (`challenge2`).** This dataset uses a different index layout
(`(status, region)`, `(created_day, status)`), fresh values, a second trap shape and a control
template. In the control, region and status are each common, but together rare, so the long prefix
is the selective one. The ratio was not retuned.

The first form of the hybrid let the estimate override the rule outright. It fixed the traps, but
regressed the control from 9.0% to 30.1% regret. A composite estimate is the minimum of the
per-column most-common-value counts, capped at rows ÷ distinct keys. It cannot see correlation
between columns, so for this control it looks exactly like the trap.

The shipped form fixes that: **estimates nominate, exact probes decide.** The nominee is probed
alongside the tied winners, and the smallest row-id count wins. Probes only collect row IDs; the
expensive record fetch happens afterwards.

End to end, on the merged code (560 queries; the noise figure is the median of MAD ÷ median, 1.1% warm):

| test split | rule total | hybrid total | >10× rule → hybrid | worst query, rule → hybrid |
|---|---:|---:|---:|---:|
| warm | 17,887 ms | 17,822 ms | 11 → **0** | 29.6× → 4.7× |
| flushed | 18,012 ms | 17,826 ms | 10 → **0** | 20.3× → 4.0× |

| template (all queries, warm) | nominated | overridden | hybrid vs. rule time |
|---|---:|---:|---:|
| `trap_longer_prefix` | 30 | 29 | −72.9% |
| `c2_status_region_customer` (held out) | 25 | 25 | −61.9% |
| `c2_control_rare_status` (held out) | 22 | 6 | +1.4% |
| every realistic template | 0 | 0 | within noise |

The worst hybrid query (4.7×) is a tie the rule handles the same way (`trap_tied_huge_probe`).

**Where the time goes** (unforced, warm): the rule's decision, including its probes, has a p50 of
14 µs and a p95 of 0.58 ms, about 0.19% of end-to-end time. Probes that are then discarded take
about 0.13%. Planner time is 0. Before #31, the planner ran twice per ordinary SELECT and its result
was discarded: 37 µs p50, 0.06%. The streaming fast paths now check the cheap disqualifiers first.

**Paged storage.** EXPLAIN there was wrong on 8 of 13 shapes. Whenever the planner chose a full scan,
that scan streamed. EXPLAIN now runs the streaming routes in a dry-run mode, so it uses execution's
own eligibility checks. Truth tests for paged storage are vacuous unless the fixture is reopened and
uses only durable index kinds, because otherwise nothing streams.

## Follow-up: cold paged storage

Same 560 queries and seeds, on server-paged storage (`--storage paged`), on a production host
shared with other services (one core, lowest CPU and I/O priority). The dataset is reopened after
loading, so rows come from pages. The cold condition closes the database, evicts its own files
from the OS page cache and reopens. Code: main at `fc1b283`. Noise (median MAD ÷ median): 7.8%
warm, 5.3% cold, so per-template differences under about 40% are not signal here.

**Bug found first (#35).** The first cold run was invalid: after a reopen, `ANALYZE` recorded every
paged table as empty, because it sampled only resident rows. The planner then priced a full scan
at zero, and the streaming routes turned all 560 queries into full scans. Fixed: ANALYZE reads the
page store. Any code that reads resident shards directly has the same blind spot after a reopen.

**Forced-candidate scores** (test split; excludes the chooser's own cost):

| policy | warm regret | warm >10× | cold regret | cold >10× | worst, cold |
|---|---:|---:|---:|---:|---:|
| rule | 3.1% | 11 | 17.8% | 18 | 132× |
| analyze (planner + stats) | 4.9% | 6 | 20.2% | 13 | 73× |
| hybrid | 1.6% | **0** | 9.7% | **0** | 9.1× |

On cold storage, the access path matters a lot more than in memory: the rule's regret goes from
3.1% to 17.8%. The hybrid removes every query over 10× in both conditions.

**Unforced, end to end** (all 560 queries; total ms):

| template | nominated / overridden | warm rule → hybrid | cold rule → hybrid |
|---|---:|---:|---:|
| `trap_longer_prefix` | 30 / 29 | 457 → 154 (−66%) | 10,812 → 5,303 (−51%) |
| `c2_status_region_customer` (held out) | 25 / 25 | 348 → 178 (−49%) | 14,113 → 9,917 (−30%) |
| `c2_control_rare_status` (held out) | 22 / 6 | 14 → 15 | **307 → 652 (+112%)** |
| all other templates | 0 / 0 | within noise | within noise (+1.3% summed) |
| total | 77 / 60 | 47,193 → 47,602 (+0.9%) | 219,418 → 212,564 (−3.1%) |

**Open problem: the held-out control regresses on cold storage.** In the 16 nominated control
queries the hybrid did not override, it kept the rule's plan, yet the query took 2–10× longer
(about 5 → 25 ms). The hybrid's decision, including the nominee probe, took 0.1–0.2 ms of that.
The extra time is spent outside the measured decision, and only when the hybrid is on. Each
unforced cold run is a single sample, but all 16 move the same way. The cause is not yet known.
**The hybrid stays default-off** until a profiled reproduction explains this.

**The bigger cost on paged storage is the rule's own tie-break probes.** Probes that are then
discarded take 27% of unforced end-to-end time (0.13% in memory). The rule probes every tied
candidate's full row-id list, which is I/O on paged storage, and keeps the smallest. This hits
realistic templates, not just traps:

| template | discarded-probe share of time |
|---|---:|
| `trap_tied_huge_probe` | 93% |
| `trap_tied_range_probe` | 83% |
| `status_day_range` | 54% |
| `customer_status_eq` | 52% |
| `c2_status_region_day` | 51% |

This also explains why unforced regret (rule: 41% warm, 86% cold) is far above the forced scores.
A bounded probe would recover most of it without changing any choice: stop counting a candidate
once it exceeds the current best. Like the hybrid, it is deterministic and needs no model.

## Is the 0.1% evidence of an excellent chooser, or of an easy benchmark?

Both, and the data says which part is which:

1. **Exact cardinality where it matters.** When candidates tie on matched columns, the rule runs
   the tied probes and keeps the smallest. Most multi-index templates are ties, so they get exact
   counts. The probe cost is in the measured latency, and it is the only source of the rule's
   small regret on realistic data (`customer_status_eq`, about 6%).
2. **Access path rarely matters in this storage mode.** Data is memory-resident and per-row decode
   dominates either way, so "index whenever possible" is almost never punished. On cold paged storage
   this no longer holds: the rule's forced regret rises to 17.8% (see above).
3. **The rule has two real traps**, which the realistic workload did not set and the challenge
   dataset targets: a longer but unselective prefix beating a short selective index, and ties
   whose losing probe is huge.

## Recommendation

**A learned 100–1,000-parameter cost model is not justified.** Keep neural-model work blocked.

- On realistic workloads there is no regret to recover (0.0–0.1%, no catastrophic choices).
- The one real failure mode (longest-prefix wins over a selective shorter index) is removed by
  the deterministic narrow hybrid above: 600 queries, 0.1% regret, worst case 2.7×. That leaves
  under 0.1% for any model.
- Every estimate-driven policy a model would sit on is worse than the rule on realistic data
  (5–17% regret, up to 30 catastrophic choices). Its dominant error is pricing, not cardinality.
- Cold paged storage leaves more room (hybrid 9.7% forced regret), but every estimate-driven
  policy is still worse than the rule there (calibrated 27.5%, planner with stats 20.2%). The
  largest cost found, the rule's discarded tie-break probes (27% of end-to-end time), needs a
  bounded probe, not a model.

Follow-ups, in order:
1. **Truthful EXPLAIN** (done in the follow-up PR). The row-set executor also now uses GiST/GIN
   indexes it used to ignore.
2. **Implement the narrow hybrid in the locator,** behind the same forced-candidate harness: with
   statistics present, compare the winner's estimated rows against the shorter candidates it
   discarded without probing.
3. **Remove the planner's discarded double invocation** from the ordinary SELECT path, and move
   toward one authoritative chooser.
4. **Cold paged storage** (done, above). It found the ANALYZE bug (#35); the hybrid
   removes every >10× choice but regresses the held-out control for an unexplained reason.
5. **Next:** bound the tie-break probes (27% of paged end-to-end time), and profile the
   control regression before any decision on enabling the hybrid. Neither needs a model.
