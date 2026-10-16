//! Reproducible traffic for the explicit immutable cluster-event contract.
use anyhow::{ensure, Result};
use bicdb_core::{ClusterEventClient, DistributionStore};
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Recheck a previous workload's acknowledged writes after later failures.
    Verify { path: PathBuf, ledger: PathBuf },
    Append {
        path: PathBuf,
        namespace: String,
        key: String,
        value: String,
    },
    Get {
        path: PathBuf,
        namespace: String,
        key: String,
    },
    Scan {
        path: PathBuf,
        namespace: String,
        /// JSON continuation returned by an earlier page.
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long, default_value_t = 128)]
        limit: usize,
    },
    /// Bounded 50/50 append/read traffic, followed by checking every acknowledged write.
    Workload {
        path: PathBuf,
        #[arg(long)]
        namespace: String,
        #[arg(long, default_value_t = 60)]
        seconds: u64,
        #[arg(long, default_value_t = 16)]
        concurrency: usize,
        #[arg(long, default_value_t = 100_000)]
        max_operations: u64,
        #[arg(long, default_value_t = 1024)]
        payload_bytes: usize,
        #[arg(long, default_value_t = 1000)]
        rpc_timeout_ms: u64,
        /// Create a new ledger for checks after later failure/recovery phases.
        #[arg(long)]
        ledger_out: Option<PathBuf>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    namespace: String,
    run: String,
    payload_bytes: usize,
    keys: Vec<String>,
}

fn client(path: &Path, timeout: Option<u64>) -> Result<ClusterEventClient> {
    let mut config = bicdb_core::load_distribution_config(path)?;
    let store = DistributionStore::open(path, config.clone(), true)?;
    if let Some(timeout) = timeout {
        config.transport.io_timeout_ms = timeout;
        config.transport.connect_timeout_ms = timeout.min(500);
    }
    Ok(ClusterEventClient::new(&config, store.topology().clone())?)
}

pub fn run(command: Command) -> Result<()> {
    let result = match command {
        Command::Verify { path, ledger } => {
            use std::io::Read;
            let file = std::fs::File::open(ledger)?;
            ensure!(
                file.metadata()?.len() <= 128 * 1024 * 1024,
                "workload ledger exceeds 128 MiB"
            );
            let mut bytes = Vec::new();
            file.take(128 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
            let ledger: Ledger = serde_json::from_slice(&bytes)?;
            ensure!(
                ledger.keys.len() <= 1_000_000 && ledger.payload_bytes <= 32 * 1024,
                "invalid ledger bounds"
            );
            let client = client(&path, Some(1000))?;
            client.refresh()?;
            let expected = json!({"run":ledger.run,"payload":"x".repeat(ledger.payload_bytes)});
            let mut failed = 0;
            let mut errors = Vec::new();
            for key in &ledger.keys {
                if !matches!(client.get(&ledger.namespace, key), Ok(Some(value)) if value == expected)
                {
                    failed += 1;
                    if errors.len() < 20 {
                        errors.push(key);
                    }
                }
            }
            json!({"checked":ledger.keys.len(),"unverified":failed,"unverified_key_samples":errors,
                "verification_passed":failed == 0 && !ledger.keys.is_empty()})
        }
        Command::Append {
            path,
            namespace,
            key,
            value,
        } => {
            json!({"inserted": client(&path, None)?.append(&namespace, &key, serde_json::from_str(&value)?)?})
        }
        Command::Get {
            path,
            namespace,
            key,
        } => json!({"value": client(&path, None)?.get(&namespace, &key)?}),
        Command::Scan {
            path,
            namespace,
            cursor,
            limit,
        } => serde_json::to_value(client(&path, None)?.scan(
            &namespace,
            cursor.map(|s| serde_json::from_str(&s)).transpose()?,
            limit,
        )?)?,
        Command::Workload {
            path,
            namespace,
            seconds,
            concurrency,
            max_operations,
            payload_bytes,
            rpc_timeout_ms,
            ledger_out,
        } => {
            ensure!(
                (1..=3600).contains(&seconds)
                    && (1..=256).contains(&concurrency)
                    && (1..=1_000_000).contains(&max_operations)
                    && payload_bytes <= 32 * 1024
                    && (100..=5000).contains(&rpc_timeout_ms),
                "invalid workload bounds"
            );
            bicdb_core::cluster_event_record_id(&namespace, "validation")?;
            let client = Arc::new(client(&path, Some(rpc_timeout_ms))?);
            client.refresh()?;
            let run = uuid::Uuid::new_v4().simple().to_string();
            let started = Instant::now();
            let deadline = started + Duration::from_secs(seconds);
            let count = AtomicU64::new(0);
            let payload = "x".repeat(payload_bytes);
            let mut samples = Vec::new();
            let mut acknowledged = Vec::new();
            let mut errors = Vec::new();
            let mut failures = 0u64;
            std::thread::scope(|scope| -> Result<()> {
                let mut workers = Vec::new();
                for worker in 0..concurrency {
                    let client = &client;
                    let count = &count;
                    let namespace = &namespace;
                    let run = &run;
                    let payload = &payload;
                    workers.push(scope.spawn(move || {
                        let mut times = Vec::new();
                        let mut acknowledged = Vec::new();
                        let mut errors = Vec::new();
                        let mut failures = 0u64;
                        let mut last_key: Option<String> = None;
                        while Instant::now() < deadline {
                            let index = count.fetch_add(1, Ordering::Relaxed);
                            if index >= max_operations {
                                break;
                            }
                            let before = Instant::now();
                            let write = index % 2 == 0 || last_key.is_none();
                            let key = if write {
                                format!("{run}-{worker}-{index}")
                            } else {
                                last_key.clone().unwrap()
                            };
                            let value = json!({"run":run,"payload":payload});
                            let result = if write {
                                client.append(namespace, &key, value.clone()).map(|_| ())
                            } else {
                                client.get(namespace, &key).and_then(|actual| {
                                    if actual == Some(value) {
                                        Ok(())
                                    } else {
                                        Err(bicdb_core::BicDbError::Cluster(
                                            "acknowledged event read mismatch".into(),
                                        ))
                                    }
                                })
                            };
                            let elapsed_us =
                                before.elapsed().as_micros().min(u64::MAX as u128) as u64;
                            match result {
                                Ok(()) => {
                                    times.push((write, elapsed_us));
                                    if write {
                                        last_key = Some(key.clone());
                                        acknowledged.push(key);
                                    }
                                }
                                Err(error) => {
                                    failures += 1;
                                    if errors.len() < 20 {
                                        errors.push(error.to_string());
                                    }
                                }
                            }
                        }
                        (times, acknowledged, errors, failures)
                    }));
                }
                for worker in workers {
                    let (times, keys, messages, failed) = worker
                        .join()
                        .map_err(|_| anyhow::anyhow!("workload worker panicked"))?;
                    samples.extend(times);
                    acknowledged.extend(keys);
                    failures += failed;
                    errors.extend(
                        messages
                            .into_iter()
                            .take(20usize.saturating_sub(errors.len())),
                    );
                }
                Ok(())
            })?;
            let traffic_seconds = started.elapsed().as_secs_f64();
            if let Some(path) = ledger_out {
                use std::io::Write;
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)?;
                serde_json::to_writer(
                    &mut file,
                    &Ledger {
                        namespace: namespace.clone(),
                        run: run.clone(),
                        payload_bytes,
                        keys: acknowledged.clone(),
                    },
                )?;
                file.flush()?;
                file.sync_all()?;
            }
            // Verification is deliberately outside the measured traffic period.
            let expected = json!({"run":run,"payload":payload});
            let mut unverified = Vec::new();
            for key in &acknowledged {
                match client.get(&namespace, key) {
                    Ok(Some(value)) if value == expected => {}
                    _ => unverified.push(key.clone()),
                }
            }
            fn latency(mut values: Vec<u64>) -> Value {
                values.sort_unstable();
                let at = |percent: usize| -> Option<u64> {
                    if values.is_empty() {
                        None
                    } else {
                        Some(values[(values.len() * percent).div_ceil(100).saturating_sub(1)])
                    }
                };
                json!({"samples":values.len(),"p50_us":at(50),"p95_us":at(95),"p99_us":at(99),"max_us":values.last()})
            }
            json!({"format":1,"workload":"immutable_events_append_read","run":run,
                "namespace":namespace,"concurrency":concurrency,"payload_bytes":payload_bytes,
                "encoded_value_bytes":serde_json::to_vec(&expected)?.len(),
                "rpc_timeout_ms":rpc_timeout_ms,"traffic_seconds":traffic_seconds,
                "successful_operations":samples.len(),"failed_operations":failures,
                "successful_operations_per_second":samples.len() as f64 / traffic_seconds,
                "write_latency":latency(samples.iter().filter(|(write,_)| *write).map(|(_,us)| *us).collect()),
                "read_latency":latency(samples.iter().filter(|(write,_)| !*write).map(|(_,us)| *us).collect()),
                "acknowledged_writes":acknowledged.len(),"unverified_writes":unverified,
                "error_samples":errors,"verification_passed":unverified.is_empty() && !acknowledged.is_empty()})
        }
    };
    println!("{}", serde_json::to_string(&result)?);
    ensure!(
        result.get("verification_passed") != Some(&Value::Bool(false)),
        "acknowledged event verification failed"
    );
    Ok(())
}
