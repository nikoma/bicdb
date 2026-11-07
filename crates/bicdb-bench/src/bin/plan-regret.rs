//! Step-0 optimizer measurement: cardinality-estimation error and plan regret
//! for BicDB's single-table access-path chooser.
//!
//! For every generated query the harness
//!   1. asks the planner (no ANALYZE statistics) for its legal candidates and
//!      its choice,
//!   2. forces every candidate in a freshly shuffled order per repetition, under
//!      two cache conditions (warm; CPU caches flushed before each run), and
//!      records latency, rows fetched by the access path, and rows matched,
//!   3. verifies each forced plan returns exactly the full-scan result,
//!   4. after `ANALYZE`, records the planner's estimates and choice again
//!      (measuring the chosen plan if it was not already a candidate).
//!
//! Output is JSON Lines (one object per query) for `bench/plan-regret/analyze.py`.
//! Every value written is synthetic: the harness generates its own data and
//! parameters, and the engine-side trace (`bicdb_sql::plan_diagnostics`) never
//! carries literal values.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::plan_diagnostics::{
    with_plan_diagnostics, DecisionTrace, DiagnosticsOptions, PlanSignature,
};
use bicdb_sql::{SqlResult, SqlSession};
use serde_json::{json, Value};

// ---------------------------------------------------------------- RNG

/// SplitMix64: tiny, seedable, reproducible.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }
    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.below(i as u64 + 1) as usize;
            items.swap(i, j);
        }
    }
    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

/// Zipf(s) sampler over 1..=n via a precomputed CDF.
struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(n: usize, s: f64) -> Self {
        let mut cdf = Vec::with_capacity(n);
        let mut total = 0.0;
        for k in 1..=n {
            total += 1.0 / (k as f64).powf(s);
            cdf.push(total);
        }
        for value in &mut cdf {
            *value /= total;
        }
        Self { cdf }
    }
    fn sample(&self, rng: &mut Rng) -> usize {
        let u = rng.unit();
        self.cdf.partition_point(|&p| p < u) + 1
    }
}

// ---------------------------------------------------------------- config

#[derive(Clone, Debug)]
struct Dataset {
    name: String,
    rows: usize,
    note_min: usize,
    note_max: usize,
}

struct Config {
    out: PathBuf,
    data_root: PathBuf,
    datasets: Vec<Dataset>,
    queries: usize,
    reps: usize,
    seed: u64,
    flush_bytes: usize,
}

fn parse_args() -> Result<Config> {
    let mut out = PathBuf::from("plan-regret-out");
    let mut data_root = std::env::temp_dir();
    let mut datasets =
        "narrow:100000:16:64,narrow-large:400000:16:64,wide:100000:1200:2400".to_string();
    let mut queries = 300usize;
    let mut reps = 5usize;
    let mut seed = 20261106u64;
    let mut flush_mb = 256usize;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().with_context(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--out" => out = PathBuf::from(value()?),
            "--data-root" => data_root = PathBuf::from(value()?),
            "--datasets" => datasets = value()?,
            "--queries" => queries = value()?.parse()?,
            "--reps" => reps = value()?.parse()?,
            "--seed" => seed = value()?.parse()?,
            "--flush-mb" => flush_mb = value()?.parse()?,
            "-h" | "--help" => {
                println!(
                    "plan-regret [--out DIR] [--data-root DIR] [--datasets name:rows:note_min:note_max,...]\n\
                     \t[--queries N] [--reps R] [--seed S] [--flush-mb MB]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument {other}"),
        }
    }
    let datasets = datasets
        .split(',')
        .map(|spec| {
            let parts = spec.split(':').collect::<Vec<_>>();
            let [name, rows, note_min, note_max] = parts.as_slice() else {
                bail!("dataset spec must be name:rows:note_min:note_max, got {spec}");
            };
            Ok(Dataset {
                name: name.to_string(),
                rows: rows.parse()?,
                note_min: note_min.parse()?,
                note_max: note_max.parse()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Config {
        out,
        data_root,
        datasets,
        queries,
        reps,
        seed,
        flush_bytes: flush_mb << 20,
    })
}

// ---------------------------------------------------------------- data

const STATUS_WEIGHTS: [f64; 8] = [0.80, 0.10, 0.05, 0.02, 0.01, 0.01, 0.005, 0.005];
const REGIONS: u64 = 50;
const DAYS: usize = 1000;

fn customers(rows: usize) -> usize {
    (rows / 20).max(10)
}

fn sample_status(rng: &mut Rng, region: u64) -> u64 {
    // Correlation: the first five regions are dominated by rare statuses, so
    // the independence assumption behind multi-column estimates is wrong there.
    if region < 5 {
        return 4 + rng.below(4);
    }
    let u = rng.unit();
    let mut acc = 0.0;
    for (status, weight) in STATUS_WEIGHTS.iter().enumerate() {
        acc += weight;
        if u < acc {
            return status as u64;
        }
    }
    7
}

fn build_dataset(session: &mut SqlSession<'_>, dataset: &Dataset, seed: u64) -> Result<()> {
    session.execute(
        "CREATE TABLE orders (
            id int8 PRIMARY KEY,
            customer_id int4 NOT NULL,
            status int4 NOT NULL,
            region int4 NOT NULL,
            created_day int4 NOT NULL,
            amount float8 NOT NULL,
            note text NOT NULL
        )",
    )?;
    let mut rng = Rng(seed);
    let zipf = Zipf::new(customers(dataset.rows), 1.1);
    let mut id = 1usize;
    while id <= dataset.rows {
        let mut sql = String::from("INSERT INTO orders VALUES ");
        let end = (id + 999).min(dataset.rows);
        for row in id..=end {
            let customer = zipf.sample(&mut rng);
            let region = rng.below(REGIONS);
            let status = sample_status(&mut rng, region);
            // Time-ordered: created_day tracks insertion order with jitter.
            let day = (row * DAYS / dataset.rows + rng.below(10) as usize).min(DAYS - 1);
            let amount = (rng.unit() * 6.0).exp();
            let width = dataset.note_min
                + rng.below((dataset.note_max - dataset.note_min + 1) as u64) as usize;
            let fill = (b'a' + (row % 26) as u8) as char;
            if row > id {
                sql.push(',');
            }
            sql.push_str(&format!(
                "({row},{customer},{status},{region},{day},{amount:.4},'{}')",
                fill.to_string().repeat(width)
            ));
        }
        session.execute(&sql)?;
        id = end + 1;
    }
    if is_challenge2(dataset) {
        // Held-out layout: different composite orders and members than the
        // datasets the hybrid was evaluated on.
        session.execute(
            "CREATE INDEX orders_customer ON orders (customer_id);
             CREATE INDEX orders_status ON orders (status);
             CREATE INDEX orders_day ON orders (created_day);
             CREATE INDEX orders_status_region ON orders (status, region);
             CREATE INDEX orders_day_status ON orders (created_day, status);",
        )?;
        return Ok(());
    }
    session.execute(
        "CREATE INDEX orders_customer ON orders (customer_id);
         CREATE INDEX orders_status ON orders (status);
         CREATE INDEX orders_day ON orders (created_day);
         CREATE INDEX orders_region_status ON orders (region, status);
         CREATE INDEX orders_customer_day ON orders (customer_id, created_day);",
    )?;
    if is_challenge(dataset) {
        // A long composite over unselective columns: competes with the
        // selective single-column indexes in the adversarial templates.
        session.execute("CREATE INDEX orders_status_day ON orders (status, created_day);")?;
    }
    Ok(())
}

/// Adversarial datasets target the rule-based chooser's two weak spots: a
/// longer but unselective index prefix beating a short selective one, and
/// ties resolved by executing every tied (possibly huge) probe.
fn is_challenge(dataset: &Dataset) -> bool {
    dataset.name.starts_with("challenge") && !is_challenge2(dataset)
}

fn is_challenge2(dataset: &Dataset) -> bool {
    dataset.name.starts_with("challenge2")
}

// ---------------------------------------------------------------- queries

struct Query {
    template: &'static str,
    sql: String,
}

fn generate_queries(dataset: &Dataset, count: usize, rng: &mut Rng) -> Vec<Query> {
    let zipf = Zipf::new(customers(dataset.rows), 1.1);
    let customers = customers(dataset.rows) as u64;
    let customer = |rng: &mut Rng| -> u64 {
        // Half heavy hitters (Zipf), half uniform over the long tail.
        if rng.unit() < 0.5 {
            zipf.sample(rng) as u64
        } else {
            1 + rng.below(customers)
        }
    };
    let widths = [0u64, 1, 5, 50, 300, 900];
    let projection = "SELECT id, amount FROM orders";
    (0..count)
        .map(|_| {
            let day = rng.below(DAYS as u64);
            let width = *rng.pick(&widths);
            let status = rng.below(8);
            let region = rng.below(REGIONS);
            if is_challenge2(dataset) {
                let common = *rng.pick(&[0u64, 0, 0, 1]);
                return match rng.below(4) {
                    0 => Query {
                        template: "c2_status_region_customer",
                        sql: format!(
                            "{projection} WHERE status = {common} AND region = {} AND customer_id = {}",
                            5 + rng.below(REGIONS - 5),
                            customer(rng)
                        ),
                    },
                    1 => Query {
                        template: "c2_status_region_day",
                        sql: format!(
                            "{projection} WHERE status = {common} AND region = {} AND created_day = {}",
                            5 + rng.below(REGIONS - 5),
                            rng.below(DAYS as u64)
                        ),
                    },
                    2 => Query {
                        // Control: here the longer prefix IS the selective one
                        // (rare statuses), so an override would be a mistake.
                        template: "c2_control_rare_status",
                        sql: format!(
                            "{projection} WHERE status = {} AND region = {} AND customer_id = {}",
                            6 + rng.below(2),
                            5 + rng.below(REGIONS - 5),
                            zipf.sample(rng)
                        ),
                    },
                    _ => Query {
                        template: "c2_customer_eq",
                        sql: format!("{projection} WHERE customer_id = {}", customer(rng)),
                    },
                };
            }
            if is_challenge(dataset) {
                // Common statuses make the long composite prefixes unselective.
                let common = *rng.pick(&[0u64, 0, 0, 1]);
                return match rng.below(3) {
                    0 => Query {
                        template: "trap_longer_prefix",
                        sql: format!(
                            "{projection} WHERE region = {} AND status = {common} AND customer_id = {}",
                            5 + rng.below(REGIONS - 5),
                            customer(rng)
                        ),
                    },
                    1 => Query {
                        template: "trap_tied_huge_probe",
                        sql: format!(
                            "{projection} WHERE status = {common} AND customer_id = {}",
                            customer(rng)
                        ),
                    },
                    _ => Query {
                        template: "trap_tied_range_probe",
                        sql: format!(
                            "{projection} WHERE status = {common} AND created_day >= {} AND region = {}",
                            rng.below(50),
                            5 + rng.below(REGIONS - 5)
                        ),
                    },
                };
            }
            match rng.below(9) {
                0 => Query {
                    template: "customer_eq",
                    sql: format!("{projection} WHERE customer_id = {}", customer(rng)),
                },
                1 => Query {
                    template: "status_eq",
                    sql: format!("{projection} WHERE status = {status}"),
                },
                2 => Query {
                    template: "region_status_eq",
                    sql: format!("{projection} WHERE region = {region} AND status = {status}"),
                },
                3 => Query {
                    template: "day_range",
                    sql: format!(
                        "{projection} WHERE created_day BETWEEN {day} AND {}",
                        day + width
                    ),
                },
                4 => Query {
                    template: "customer_day_gt",
                    sql: format!(
                        "{projection} WHERE customer_id = {} AND created_day > {day}",
                        customer(rng)
                    ),
                },
                5 => Query {
                    template: "status_day_range",
                    sql: format!(
                        "{projection} WHERE status = {status} AND created_day BETWEEN {day} AND {}",
                        day + width
                    ),
                },
                6 => Query {
                    template: "customer_status_eq",
                    sql: format!(
                        "{projection} WHERE customer_id = {} AND status = {status}",
                        customer(rng)
                    ),
                },
                7 => Query {
                    template: "status_order_limit",
                    sql: format!(
                        "{projection} WHERE status = {status} ORDER BY created_day, id LIMIT 10"
                    ),
                },
                _ => Query {
                    template: "amount_gt_unindexed",
                    sql: format!("{projection} WHERE amount > {:.2}", rng.unit() * 400.0),
                },
            }
        })
        .collect()
}

// ---------------------------------------------------------------- execution

fn result_fingerprint(result: &SqlResult) -> (usize, u64) {
    // Order-insensitive multiset hash of rendered rows.
    let mut sum = 0u64;
    for row in &result.rows {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for value in row {
            value.to_cell().hash(&mut hasher);
            0xffu8.hash(&mut hasher);
        }
        sum = sum.wrapping_add(hasher.finish());
    }
    (result.rows.len(), sum)
}

/// The decision whose access path actually executed: the row-set executor's
/// rule-based locator when it ran, else the planner decision that fetched
/// rows, else the last planner decision.
fn executed_decision(decisions: &[DecisionTrace]) -> Option<&DecisionTrace> {
    decisions
        .iter()
        .rev()
        .find(|decision| decision.chooser == "locator")
        .or_else(|| {
            decisions
                .iter()
                .rev()
                .find(|decision| decision.rows_fetched.is_some())
        })
        .or_else(|| decisions.last())
}

/// The last cost-based planner decision (what EXPLAIN reports).
fn planner_decision(decisions: &[DecisionTrace]) -> Option<&DecisionTrace> {
    decisions
        .iter()
        .rev()
        .find(|decision| decision.chooser == "planner")
}

struct Run {
    elapsed_ms: f64,
    fingerprint: (usize, u64),
    decision: Option<DecisionTrace>,
    planner: Option<DecisionTrace>,
    forced: bool,
    decisions: Vec<DecisionTrace>,
}

fn run_once(session: &mut SqlSession<'_>, sql: &str, force: Option<&PlanSignature>) -> Result<Run> {
    run_with(session, sql, force, Some(false))
}

fn run_with(
    session: &mut SqlSession<'_>,
    sql: &str,
    force: Option<&PlanSignature>,
    hybrid: Option<bool>,
) -> Result<Run> {
    let options = DiagnosticsOptions {
        force: force.cloned(),
        locator_hybrid: hybrid,
        ..DiagnosticsOptions::default()
    };
    let started = Instant::now();
    let (result, decisions) = with_plan_diagnostics(options, || session.execute(sql));
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let result = result.with_context(|| format!("executing {sql}"))?;
    let decision = executed_decision(&decisions).cloned();
    let planner = planner_decision(&decisions).cloned();
    // Forced when the executing chooser honored the force: every locator
    // decision when the locator ran (it may run more than once), otherwise
    // every planner decision.
    let executing = if decisions.iter().any(|d| d.chooser == "locator") {
        "locator"
    } else {
        "planner"
    };
    let forced = force.is_some()
        && decisions
            .iter()
            .filter(|d| d.chooser == executing)
            .all(|d| d.forced)
        && decisions.iter().any(|d| d.chooser == executing);
    Ok(Run {
        elapsed_ms,
        fingerprint: result_fingerprint(&result),
        decision,
        planner,
        forced,
        decisions,
    })
}

fn time_plan_only(session: &mut SqlSession<'_>, sql: &str, reps: usize) -> Result<f64> {
    let explain = format!("EXPLAIN {sql}");
    let mut samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let started = Instant::now();
        session.execute(&explain)?;
        samples.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    Ok(median(&mut samples))
}

struct CacheFlusher {
    buffer: Vec<u8>,
    tick: u8,
}

impl CacheFlusher {
    fn new(bytes: usize) -> Self {
        Self {
            buffer: vec![0; bytes.max(4096)],
            tick: 0,
        }
    }
    /// Write one byte per cache line of a buffer larger than the LLC, evicting
    /// the engine's working set from CPU caches (data stays in RAM).
    fn flush(&mut self) {
        self.tick = self.tick.wrapping_add(1);
        for index in (0..self.buffer.len()).step_by(64) {
            self.buffer[index] = self.buffer[index].wrapping_add(self.tick);
        }
        std::hint::black_box(&self.buffer);
    }
}

fn median(samples: &mut [f64]) -> f64 {
    if samples.is_empty() {
        return f64::NAN;
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = samples.len() / 2;
    if samples.len() % 2 == 1 {
        samples[mid]
    } else {
        (samples[mid - 1] + samples[mid]) / 2.0
    }
}

#[derive(Default)]
struct CandidateMeasurement {
    warm: Vec<f64>,
    flushed: Vec<f64>,
    rows_fetched: Option<usize>,
    rows_matched: Option<usize>,
    semantics_ok: bool,
    forced_ok: bool,
}

const CONDITIONS: [&str; 2] = ["warm", "flushed"];

fn measure_candidates(
    session: &mut SqlSession<'_>,
    sql: &str,
    signatures: &[PlanSignature],
    reference: (usize, u64),
    total_rows: usize,
    reps: usize,
    rng: &mut Rng,
    flusher: &mut CacheFlusher,
) -> Result<BTreeMap<PlanSignature, CandidateMeasurement>> {
    let mut out: BTreeMap<PlanSignature, CandidateMeasurement> = BTreeMap::new();
    // Untimed warm-up + semantics check for every candidate.
    for signature in signatures {
        let run = run_once(session, sql, Some(signature))?;
        let measurement = out.entry(signature.clone()).or_default();
        measurement.semantics_ok = run.fingerprint == reference;
        measurement.forced_ok = run.forced;
        if let Some(decision) = &run.decision {
            measurement.rows_fetched = decision.rows_fetched;
            measurement.rows_matched = decision.rows_matched;
        }
        if signature.kind == "FullScan" && measurement.rows_fetched.is_none() {
            measurement.rows_fetched = Some(total_rows);
        }
    }
    let mut conditions = CONDITIONS.to_vec();
    rng.shuffle(&mut conditions);
    for condition in conditions {
        for _ in 0..reps {
            let mut order = signatures.to_vec();
            rng.shuffle(&mut order);
            for signature in &order {
                if condition == "flushed" {
                    flusher.flush();
                }
                let run = run_once(session, sql, Some(signature))?;
                let measurement = out.get_mut(signature).expect("measured");
                if run.fingerprint != reference {
                    measurement.semantics_ok = false;
                }
                let samples = if condition == "warm" {
                    &mut measurement.warm
                } else {
                    &mut measurement.flushed
                };
                samples.push(run.elapsed_ms);
            }
        }
    }
    Ok(out)
}

/// End-to-end timing of the UNFORCED statement, i.e. what a client sees:
/// planning, the chooser's own probes (including tie probes it discards),
/// and execution of its choice. Reports the decision-time split per run.
#[allow(clippy::too_many_arguments)]
fn measure_natural(
    session: &mut SqlSession<'_>,
    sql: &str,
    reference: (usize, u64),
    hybrid: bool,
    reps: usize,
    rng: &mut Rng,
    flusher: &mut CacheFlusher,
) -> Result<Value> {
    run_with(session, sql, None, Some(hybrid))?;
    let mut warm = Vec::new();
    let mut flushed = Vec::new();
    let mut locator_us = Vec::new();
    let mut wasted_us = Vec::new();
    let mut planner_us = Vec::new();
    let mut semantics_ok = true;
    let mut last: Option<Run> = None;
    let mut conditions = CONDITIONS.to_vec();
    rng.shuffle(&mut conditions);
    for condition in conditions {
        for _ in 0..reps {
            if condition == "flushed" {
                flusher.flush();
            }
            let run = run_with(session, sql, None, Some(hybrid))?;
            semantics_ok &= run.fingerprint == reference;
            if condition == "warm" {
                warm.push(run.elapsed_ms);
                let rule = run
                    .decisions
                    .iter()
                    .filter(|d| d.chooser != "planner")
                    .collect::<Vec<_>>();
                locator_us.push(rule.iter().filter_map(|d| d.decision_micros).sum::<u64>() as f64);
                wasted_us.push(
                    rule.iter()
                        .filter_map(|d| d.wasted_probe_micros)
                        .sum::<u64>() as f64,
                );
                planner_us.push(
                    run.decisions
                        .iter()
                        .filter(|d| d.chooser == "planner")
                        .filter_map(|d| d.decision_micros)
                        .sum::<u64>() as f64,
                );
            } else {
                flushed.push(run.elapsed_ms);
            }
            last = Some(run);
        }
    }
    let last = last.expect("at least one repetition");
    let chosen = last
        .decision
        .as_ref()
        .map(|d| d.candidates[d.chosen].signature.label());
    let overridden = last.decisions.iter().any(|d| d.hybrid_override);
    let nominated = last.decisions.iter().any(|d| d.hybrid_nominated);
    Ok(json!({
        "hybrid": hybrid,
        "warm_ms": median(&mut warm),
        "flushed_ms": median(&mut flushed),
        "chosen": chosen,
        "hybrid_override": overridden,
        "hybrid_nominated": nominated,
        "rule_decision_us": median(&mut locator_us),
        "wasted_probe_us": median(&mut wasted_us),
        "planner_us": median(&mut planner_us),
        "semantics_ok": semantics_ok,
    }))
}

fn signature_json(signature: &PlanSignature) -> Value {
    json!({
        "label": signature.label(),
        "kind": signature.kind,
        "index": signature.index,
        "prefix_len": signature.prefix_len,
    })
}

fn run_dataset(config: &Config, dataset: &Dataset, out: &mut impl Write) -> Result<()> {
    let dir = config.data_root.join(format!(
        "plan-regret-{}-{}",
        dataset.name,
        std::process::id()
    ));
    if dir.exists() {
        std::fs::remove_dir_all(&dir)?;
    }
    std::fs::create_dir_all(&dir)?;
    let result = (|| -> Result<()> {
        let mut db = BicDb::open_with_config(&dir, DbConfig::default().with_fsync(false))?;
        let mut session = SqlSession::new(&mut db);
        let started = Instant::now();
        build_dataset(&mut session, dataset, config.seed ^ 0xD47A)?;
        eprintln!(
            "[{}] built {} rows in {:.1}s",
            dataset.name,
            dataset.rows,
            started.elapsed().as_secs_f64()
        );

        let mut rng = Rng(config.seed ^ dataset.rows as u64);
        let queries = generate_queries(dataset, config.queries, &mut rng);
        let mut order = (0..queries.len()).collect::<Vec<_>>();
        rng.shuffle(&mut order);
        let mut flusher = CacheFlusher::new(config.flush_bytes);

        // Phase A: no statistics.
        let mut records: Vec<Value> = vec![Value::Null; queries.len()];
        let mut measured: Vec<BTreeMap<PlanSignature, CandidateMeasurement>> =
            (0..queries.len()).map(|_| BTreeMap::new()).collect();
        let mut references = vec![(0usize, 0u64); queries.len()];
        for (done, &qi) in order.iter().enumerate() {
            let query = &queries[qi];
            let natural = run_once(&mut session, &query.sql, None)?;
            let Some(decision) = natural.decision.clone() else {
                bail!("no planning decision recorded for {}", query.sql);
            };
            let reference_run =
                run_once(&mut session, &query.sql, Some(&PlanSignature::full_scan()))?;
            references[qi] = reference_run.fingerprint;
            let mut signatures = decision
                .candidates
                .iter()
                .map(|candidate| candidate.signature.clone())
                .collect::<BTreeSet<_>>();
            signatures.insert(PlanSignature::full_scan());
            let signatures = signatures.into_iter().collect::<Vec<_>>();
            let plan_ms = time_plan_only(&mut session, &query.sql, 5)?;
            measured[qi] = measure_candidates(
                &mut session,
                &query.sql,
                &signatures,
                references[qi],
                dataset.rows,
                config.reps,
                &mut rng,
                &mut flusher,
            )?;
            let natural_nostats = measure_natural(
                &mut session,
                &query.sql,
                references[qi],
                false,
                config.reps,
                &mut rng,
                &mut flusher,
            )?;
            records[qi] = json!({
                "natural_nostats": natural_nostats,
                "dataset": dataset.name,
                "rows": dataset.rows,
                "note_bytes": [dataset.note_min, dataset.note_max],
                "query_id": qi,
                "template": query.template,
                "sql": query.sql,
                "result_rows": references[qi].0,
                "natural_result_matches": natural.fingerprint == references[qi],
                "reference_forced": reference_run.forced,
                "plan_only_ms": plan_ms,
                "executed_chooser": decision.chooser,
                "executed": decision_json(&decision),
                "nostats": natural.planner.as_ref().map(decision_json),
            });
            if done % 25 == 0 {
                eprintln!("[{}] phase A {}/{}", dataset.name, done, queries.len());
            }
        }

        // Phase B: fresh ANALYZE statistics.
        let started = Instant::now();
        session.execute("ANALYZE orders")?;
        let analyze_ms = started.elapsed().as_secs_f64() * 1000.0;
        for (done, &qi) in order.iter().enumerate() {
            let query = &queries[qi];
            let natural = run_once(&mut session, &query.sql, None)?;
            let Some(decision) = natural.decision.clone() else {
                bail!(
                    "no planning decision recorded after ANALYZE for {}",
                    query.sql
                );
            };
            let missing = decision
                .candidates
                .iter()
                .map(|candidate| candidate.signature.clone())
                .chain(std::iter::once(
                    decision.candidates[decision.chosen].signature.clone(),
                ))
                .filter(|signature| !measured[qi].contains_key(signature))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                let extra = measure_candidates(
                    &mut session,
                    &query.sql,
                    &missing,
                    references[qi],
                    dataset.rows,
                    config.reps,
                    &mut rng,
                    &mut flusher,
                )?;
                measured[qi].extend(extra);
            }
            let natural_stats = measure_natural(
                &mut session,
                &query.sql,
                references[qi],
                false,
                config.reps,
                &mut rng,
                &mut flusher,
            )?;
            let natural_hybrid = measure_natural(
                &mut session,
                &query.sql,
                references[qi],
                true,
                config.reps,
                &mut rng,
                &mut flusher,
            )?;
            // Make sure the hybrid's choice also has a forced measurement.
            let hybrid_run = run_with(&mut session, &query.sql, None, Some(true))?;
            if let Some(hybrid_decision) = &hybrid_run.decision {
                let signature = hybrid_decision.candidates[hybrid_decision.chosen]
                    .signature
                    .clone();
                if !measured[qi].contains_key(&signature) {
                    let extra = measure_candidates(
                        &mut session,
                        &query.sql,
                        &[signature],
                        references[qi],
                        dataset.rows,
                        config.reps,
                        &mut rng,
                        &mut flusher,
                    )?;
                    measured[qi].extend(extra);
                }
            }
            let plan_ms = time_plan_only(&mut session, &query.sql, 5)?;
            let record = records[qi].as_object_mut().expect("record");
            record.insert("natural_stats".into(), natural_stats);
            record.insert("natural_hybrid".into(), natural_hybrid);
            record.insert("executed_after_analyze".into(), decision_json(&decision));
            record.insert(
                "stats".into(),
                json!(natural.planner.as_ref().map(decision_json)),
            );
            record.insert("stats_plan_only_ms".into(), json!(plan_ms));
            record.insert(
                "stats_natural_result_matches".into(),
                json!(natural.fingerprint == references[qi]),
            );
            record.insert("analyze_ms".into(), json!(analyze_ms));
            if done % 25 == 0 {
                eprintln!("[{}] phase B {}/{}", dataset.name, done, queries.len());
            }
        }

        for qi in 0..queries.len() {
            let measurements = measured[qi]
                .iter_mut()
                .map(|(signature, m)| {
                    let mut warm = m.warm.clone();
                    let mut flushed = m.flushed.clone();
                    json!({
                        "signature": signature_json(signature),
                        "warm_ms": median(&mut warm),
                        "flushed_ms": median(&mut flushed),
                        "warm_samples": m.warm,
                        "flushed_samples": m.flushed,
                        "rows_fetched": m.rows_fetched,
                        "rows_matched": m.rows_matched,
                        "semantics_ok": m.semantics_ok,
                        "forced_ok": m.forced_ok,
                    })
                })
                .collect::<Vec<_>>();
            let record = records[qi].as_object_mut().expect("record");
            record.insert("measurements".into(), Value::Array(measurements));
            writeln!(out, "{}", serde_json::to_string(&records[qi])?)?;
        }
        out.flush()?;
        Ok(())
    })();
    let _ = std::fs::remove_dir_all(&dir);
    result
}

fn decision_json(decision: &DecisionTrace) -> Value {
    json!({
        "chooser": decision.chooser,
        "forced": decision.forced,
        "rows_fetched": decision.rows_fetched,
        "total_rows": decision.total_rows,
        "has_statistics": decision.has_statistics,
        "chosen": signature_json(&decision.candidates[decision.chosen].signature),
        "candidates": decision.candidates.iter().map(|candidate| json!({
            "signature": signature_json(&candidate.signature),
            "estimated_rows": candidate.estimated_rows,
            "estimated_cost": candidate.estimated_cost,
        })).collect::<Vec<_>>(),
    })
}

fn main() -> Result<()> {
    let config = parse_args()?;
    std::fs::create_dir_all(&config.out)?;
    let path = config.out.join("measurements.jsonl");
    let mut out = BufWriter::new(File::create(&path)?);
    let meta = json!({
        "seed": config.seed,
        "queries_per_dataset": config.queries,
        "reps": config.reps,
        "flush_bytes": config.flush_bytes,
        "datasets": config.datasets.iter().map(|d| json!({
            "name": d.name, "rows": d.rows, "note_min": d.note_min, "note_max": d.note_max,
        })).collect::<Vec<_>>(),
        "git_rev": std::env::var("PLAN_REGRET_GIT_REV").ok(),
    });
    std::fs::write(
        config.out.join("meta.json"),
        serde_json::to_string_pretty(&meta)?,
    )?;
    for dataset in &config.datasets {
        run_dataset(&config, dataset, &mut out)?;
    }
    eprintln!("wrote {}", path.display());
    Ok(())
}
