//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

/// Host-side policy for starting and resuming durable page checkpoint work.
///
/// The page cursor and all retry state remain in BicDB's checksummed schedule;
/// this controller retains only two conservative trigger deadlines. Losing
/// those deadlines on process failure may start a checkpoint earlier after
/// restart, but cannot skip or replace an active operation.
#[derive(Debug)]
pub(crate) struct AutomaticPagedCheckpointDriver {
    pub(crate) start_interval_ms: u64,
    pub(crate) pressure_retry_ms: u64,
    pub(crate) next_periodic_at_ms: u64,
    pub(crate) next_pressure_at_ms: u64,
    pub(crate) limits: PagedCheckpointScheduleLimits,
}

impl AutomaticPagedCheckpointDriver {
    pub(crate) fn new(
        now_ms: u64,
        start_interval: Duration,
        poll_interval: Duration,
        limits: PagedCheckpointScheduleLimits,
    ) -> Result<Self> {
        let start_interval_ms = duration_millis_u64(start_interval, "checkpoint interval")?;
        let poll_interval_ms = duration_millis_u64(poll_interval, "checkpoint poll interval")?;
        limits.validate()?;
        Ok(Self {
            start_interval_ms,
            // A permanently unfrozen transaction may intentionally retain WAL.
            // Do not spin complete checkpoint generations at the host poll rate.
            pressure_retry_ms: poll_interval_ms.max(1_000).min(start_interval_ms),
            next_periodic_at_ms: now_ms
                .checked_add(start_interval_ms)
                .ok_or_else(|| PgWireError::Server("checkpoint due time overflow".to_string()))?,
            // WAL already above its trigger on restart must be serviced now.
            next_pressure_at_ms: now_ms,
            limits,
        })
    }

    pub(crate) fn tick(
        &mut self,
        handle: &PagedCheckpointMaintenanceHandle,
        governor: &ResourceGovernor,
        now_ms: u64,
    ) -> Result<AutomaticPagedCheckpointTick> {
        let snapshot = handle.storage_snapshot()?;

        if let Some(schedule) = handle.checkpoint_maintenance_status()? {
            if !schedule.completed {
                if schedule.paused_reason.is_some() {
                    return Ok(AutomaticPagedCheckpointTick::Paused {
                        operation_id: schedule.operation_id,
                    });
                }
                governor.validate_demand(ResourceLane::Compaction, schedule.limits.demand)?;
                let advance =
                    handle.tick_checkpoint_maintenance(schedule.operation_id, governor, now_ms)?;
                if matches!(advance, PagedCheckpointScheduleAdvance::Complete { .. }) {
                    self.defer_new_operation(now_ms)?;
                }
                return Ok(AutomaticPagedCheckpointTick::Advanced(advance));
            }
        }

        let has_checkpoint_work = snapshot.wal_bytes != 0 || snapshot.buffer_pool.dirty_pages != 0;
        let under_wal_pressure =
            snapshot.wal_bytes > snapshot.wal_max_bytes && now_ms >= self.next_pressure_at_ms;
        let periodic_due = has_checkpoint_work && now_ms >= self.next_periodic_at_ms;
        if !under_wal_pressure && !periodic_due {
            return Ok(AutomaticPagedCheckpointTick::Idle);
        }

        governor.validate_demand(ResourceLane::Compaction, self.limits.demand)?;
        let schedule = handle.start_checkpoint_maintenance(now_ms, self.limits)?;
        self.defer_new_operation(now_ms)?;
        let advance =
            handle.tick_checkpoint_maintenance(schedule.operation_id, governor, now_ms)?;
        Ok(AutomaticPagedCheckpointTick::Advanced(advance))
    }

    pub(crate) fn defer_new_operation(&mut self, now_ms: u64) -> Result<()> {
        self.next_periodic_at_ms = now_ms
            .checked_add(self.start_interval_ms)
            .ok_or_else(|| PgWireError::Server("checkpoint due time overflow".to_string()))?;
        self.next_pressure_at_ms = now_ms
            .checked_add(self.pressure_retry_ms)
            .ok_or_else(|| PgWireError::Server("checkpoint retry time overflow".to_string()))?;
        Ok(())
    }
}

pub(crate) fn duration_millis_u64(duration: Duration, label: &str) -> Result<u64> {
    u64::try_from(duration.as_millis())
        .ok()
        .filter(|millis| *millis != 0)
        .ok_or_else(|| PgWireError::Server(format!("{label} must fit in nonzero milliseconds")))
}

pub(crate) fn start_background_tasks(server: Arc<PgWireServer>) -> Result<()> {
    start_background_tasks_for_scope(server.clone(), true)?;
    if let Some(cluster) = server.cluster.as_ref().and_then(Weak::upgrade) {
        cluster.enable_database_background_tasks();
    }
    Ok(())
}

pub(crate) fn start_database_background_tasks(server: Arc<PgWireServer>) -> Result<()> {
    start_background_tasks_for_scope(server, false)
}

pub(crate) fn start_background_tasks_for_scope(
    server: Arc<PgWireServer>,
    include_host_tasks: bool,
) -> Result<()> {
    let start_database_tasks = !server
        .database_background_started
        .swap(true, Ordering::SeqCst);
    let start_host_tasks =
        include_host_tasks && !server.host_background_started.swap(true, Ordering::SeqCst);

    if start_host_tasks {
        start_host_services(server.clone())?;
    }

    if start_database_tasks {
        let flush_server = server.clone();
        thread::spawn(move || {
            let worker = BackgroundWorker::register(flush_server.clone());
            while worker.sleep_until_shutdown(flush_server.config.flush_interval) {
                if let Err(error) = flush_server.flush() {
                    eprintln!("bicdb server flush loop error: {error}");
                }
            }
        });

        if server.config.automatic_paged_checkpoint {
            let _ = start_automatic_paged_checkpoint_worker(server.clone());
            let _ = start_automatic_paged_vacuum_worker(server.clone());
            let _ = start_automatic_fts_fold_worker(server.clone());
        }
        if server.config.automatic_paged_read_ahead {
            let _ = start_automatic_paged_read_ahead_worker(server.clone());
        }
    }

    // Self-protective memory guard + optional MVCC/memory leak trace. The guard
    // aborts the process before the host slides into kswapd thrash (which on a
    // tmpfs data dir requires a power-cycle to recover). BICDB_MEM_GUARD_MB sets
    // the floor of host MemAvailable (default 2048; 0 disables). BICDB_MEM_TRACE
    // enables a periodic structural dump for leak profiling.
    if start_host_tasks {
        let guard_server = server.clone();
        thread::spawn(move || {
            let worker = BackgroundWorker::register(guard_server.clone());
            let guard_mb: u64 = std::env::var("BICDB_MEM_GUARD_MB")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2048);
            let trace = std::env::var("BICDB_MEM_TRACE").is_ok();
            let interval = Duration::from_secs(
                std::env::var("BICDB_MEM_TRACE_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(2),
            );
            let sweep = std::env::var("BICDB_VERSION_SWEEP")
                .map(|v| v != "0" && !v.eq_ignore_ascii_case("off"))
                .unwrap_or(true);
            // Sweep eagerly only under memory pressure (host MemAvailable
            // below this floor, default 4x the guard or 8 GiB, whichever is
            // larger); otherwise at a slow hygiene cadence. Reclaiming
            // millions of dead versions is real work — cross-thread frees and
            // shard write locks — that a host with memory to spare should not
            // pay every two seconds (it measured -4% on such a host), nor
            // once a minute inside a short benchmark window (-1% on one VM);
            // five minutes keeps a long-running write-once workload's
            // resident set bounded without touching a two-minute run.
            let sweep_floor_mb: u64 = std::env::var("BICDB_VERSION_SWEEP_MB")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| (guard_mb * 4).max(8192));
            let sweep_every = Duration::from_secs(
                std::env::var("BICDB_VERSION_SWEEP_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(300),
            );
            let mut last_sweep = Instant::now();
            while worker.sleep_until_shutdown(interval) {
                let avail_mb = mem_available_mb();
                // Dead row versions the write path leaves behind (see
                // `BicDb::sweep_dead_versions`): reclaimed here, a bounded
                // batch per tick under memory pressure, else once per
                // `sweep_every`, so a write-once workload's resident set stops
                // growing by one dead copy per write.
                if sweep {
                    let pressed = avail_mb > 0 && avail_mb < sweep_floor_mb;
                    if pressed || last_sweep.elapsed() >= sweep_every {
                        if let Ok(db) = guard_server.read_db() {
                            let freed = db.sweep_dead_versions(4_000_000);
                            if trace && freed > 0 {
                                eprintln!("MEMTRACE version_sweep freed={freed} pressed={pressed}");
                            }
                        }
                        last_sweep = Instant::now();
                    }
                }
                let rss_mb = self_rss_mb();
                if trace {
                    if let Ok(db) = guard_server.read_db() {
                        let s = db.mvcc_debug_stats();
                        eprintln!(
                            "MEMTRACE avail_mb={avail_mb} rss_mb={rss_mb} wal_mb={} \
                         tx_states={} active_snap={} min_snap={} commit_seq={} last_seq={} \
                         records={} versions={} max_chain={}",
                            s.wal_bytes / (1024 * 1024),
                            s.tx_states,
                            s.active_snapshots,
                            s.min_active_snapshot,
                            s.commit_seq,
                            s.last_committed_seq,
                            s.total_records,
                            s.total_versions,
                            s.max_chain,
                        );
                    }
                }
                if guard_mb > 0 && avail_mb > 0 && avail_mb < guard_mb {
                    let extra = guard_server
                        .read_db()
                        .ok()
                        .map(|db| {
                            let s = db.mvcc_debug_stats();
                            format!(
                                " wal_mb={} tx_states={} records={} versions={}",
                                s.wal_bytes / (1024 * 1024),
                                s.tx_states,
                                s.total_records,
                                s.total_versions
                            )
                        })
                        .unwrap_or_default();
                    eprintln!(
                        "bicdb MEMORY GUARD TRIPPED: host MemAvailable {avail_mb} MB < {guard_mb} MB \
                     (server rss {rss_mb} MB).{extra} Aborting to protect the host."
                    );
                    std::process::abort();
                }
            }
        });
    }

    // WAL auto-compaction (checkpointer). The transaction log on disk only
    // shrinks in BicDb::compact(); nothing else truncates it, so a long-running
    // write workload grows the WAL without bound (on a tmpfs data dir that is RAM).
    // A background (fuzzy) checkpointer fires once the WAL crosses
    // BICDB_AUTO_COMPACT_WAL_MB, materializing committed state into segments and
    // truncating the log (which also prunes non-committed tx_states). Default is
    // 4096 MB — the PostgreSQL max_wal_size ballpark for a write-heavy box (PG's
    // own 1 GB default is the first thing raised on such workloads, and here it
    // measured −16% on a sustained write benchmark vs −6% at 4 GB) — so the
    // workload is
    // bounded out of the box; set BICDB_AUTO_COMPACT_WAL_MB=0 to disable (the
    // old always-off behavior).
    //
    // COST: this is an INCREMENTAL checkpoint -- phase 1 appends only the per-collection
    // dirty delta to segments off the exclusive write lock (compacting a segment fully
    // only when its garbage exceeds ~2x live), so the exclusive lock is held only by the
    // two short bookend phases (boundary capture + WAL tail copy), not for a full-dataset
    // rewrite. It is therefore a low-pause online checkpointer, not just an OOM valve;
    // a smaller threshold checkpoints more often (lower WAL peak, more append overhead).
    if start_database_tasks {
        let compact_server = server.clone();
        thread::spawn(move || {
            let worker = BackgroundWorker::register(compact_server.clone());
            let limit_mb: u64 = std::env::var("BICDB_AUTO_COMPACT_WAL_MB")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(4096);
            if limit_mb == 0 {
                return;
            }
            let limit_bytes = limit_mb * 1024 * 1024;
            while worker.sleep_until_shutdown(Duration::from_secs(2)) {
                let wal_bytes = match compact_server.read_db() {
                    Ok(db) => db.wal_bytes(),
                    Err(_) => continue,
                };
                if wal_bytes < limit_bytes {
                    continue;
                }
                let checkpoint_started = Instant::now();
                // Background (fuzzy) checkpoint, three phases, only the bookends hold the
                // exclusive lock and both are short. This replaces the old full-compaction
                // checkpoint that held the write lock for the entire ~74s dataset rewrite.
                //
                // Phase 0 (brief write lock): capture the WAL boundary + dirty delta.
                let phase0_started = Instant::now();
                let plan = {
                    let Ok(admission) = compact_server.try_admit_write() else {
                        continue;
                    };
                    match compact_server.write_db_with_admission(&admission) {
                        Ok(db) => match db.checkpoint_begin() {
                            Ok(plan) => plan,
                            Err(error) => {
                                eprintln!("bicdb auto-checkpoint phase-0 error: {error}");
                                continue;
                            }
                        },
                        Err(_) => continue,
                    }
                };
                let phase0_ms = phase0_started.elapsed().as_millis();
                // Phase 1 (shared read lock): append the dirty delta to segments
                // concurrently with commits. Runs on a short-lived thread at
                // minimum CPU priority (nice 19): on a CPU-saturated box the bulk
                // segment appends otherwise compete head-on with foreground commits
                // (measured −13..−16% throughput at normal priority); deprioritized
                // they only soak up cycles the workload isn't using. A separate
                // thread (rather than re-nicing this one) because an unprivileged
                // process cannot lower its nice back down, and the phase-0/2
                // bookends hold the exclusive write lock — a starved holder of that
                // lock would stall every commit (priority inversion).
                let phase1_started = Instant::now();
                let seg_ok = thread::scope(|scope| {
                    scope
                        .spawn(|| {
                            #[cfg(target_os = "linux")]
                            unsafe {
                                let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
                                libc::setpriority(libc::PRIO_PROCESS, tid, 19);
                            }
                            match compact_server.read_db() {
                                Ok(db) => match db.checkpoint_write_segments(&plan) {
                                    Ok(()) => true,
                                    Err(error) => {
                                        eprintln!("bicdb auto-checkpoint phase-1 error: {error}");
                                        false
                                    }
                                },
                                Err(_) => false,
                            }
                        })
                        .join()
                        .unwrap_or(false)
                });
                let phase1_ms = phase1_started.elapsed().as_millis();
                if !seg_ok {
                    // checkpoint_begin drained the per-collection dirty sets into the
                    // plan; phase 1 did not durably append them, so the dirty ids must
                    // be restored or the next (larger) keep_from would truncate their
                    // WAL frames while they are absent from segments -> data loss.
                    if let Ok(db) = compact_server.read_db() {
                        db.checkpoint_abort(plan);
                    }
                    continue;
                }
                // Phase 2 (brief write lock): drop the WAL prefix below the boundary.
                let Ok(admission) = compact_server.try_admit_write() else {
                    continue;
                };
                let acquire_started = Instant::now();
                match compact_server.write_db_with_admission(&admission) {
                    Ok(mut db) => {
                        let acquire_ms = acquire_started.elapsed().as_millis();
                        let work_started = Instant::now();
                        let dirty = plan.dirty_count();
                        match db.checkpoint_truncate_wal(plan.keep_from()) {
                            Ok((before, after)) => eprintln!(
                                "bicdb auto-checkpoint: wal {} MB -> {} MB \
                             (delta {dirty} records, phase-0 {phase0_ms} ms, \
                             phase-1 {phase1_ms} ms, lock-acquire {acquire_ms} ms, \
                             truncate {} ms, total {} ms)",
                                before / (1024 * 1024),
                                after / (1024 * 1024),
                                work_started.elapsed().as_millis(),
                                checkpoint_started.elapsed().as_millis(),
                            ),
                            Err(error) => eprintln!("bicdb auto-checkpoint phase-2 error: {error}"),
                        }
                    }
                    Err(error) => eprintln!("bicdb auto-checkpoint write-lock error: {error}"),
                }
            }
        });
    }

    if start_host_tasks {
        thread::spawn(move || {
            let worker = BackgroundWorker::register(server.clone());
            while worker.sleep_until_shutdown(server.config.metrics_interval) {
                let stats = server.stats_snapshot();
                if proc_mix_trace_enabled() {
                    let routine_exceptions = routine_exception_counts_snapshot();
                    eprintln!(
                        "bicdb server metrics: active_connections={} queries={} failed_queries={} uptime={}s proc_mix neword={} payment={} delivery={} ostat={} slev={} routine_exceptions serialization_failure={} deadlock_detected={} no_data_found={} other={}",
                        stats.active_connections,
                        stats.queries_executed,
                        stats.failed_queries,
                        stats.uptime_seconds,
                        server.proc_neword.load(Ordering::Relaxed),
                        server.proc_payment.load(Ordering::Relaxed),
                        server.proc_delivery.load(Ordering::Relaxed),
                        server.proc_orderstatus.load(Ordering::Relaxed),
                        server.proc_stocklevel.load(Ordering::Relaxed),
                        routine_exceptions.serialization_failure,
                        routine_exceptions.deadlock_detected,
                        routine_exceptions.no_data_found,
                        routine_exceptions.other,
                    );
                } else {
                    eprintln!(
                        "bicdb server metrics: active_connections={} queries={} failed_queries={} uptime={}s",
                        stats.active_connections,
                        stats.queries_executed,
                        stats.failed_queries,
                        stats.uptime_seconds
                    );
                }
            }
        });
    }
    Ok(())
}

pub(crate) fn start_host_services(server: Arc<PgWireServer>) -> Result<()> {
    let services = server
        .host_services
        .lock()
        .map_err(|_| PgWireError::Server("pgwire host service registry lock poisoned".to_string()))?
        .clone();
    for service in services {
        service.start(PgWireHostContext {
            server: server.clone(),
        })?;
    }
    Ok(())
}

/// Registers a background worker for the lifetime of its thread, so shutdown
/// can tell whether the database is still held.
pub(crate) struct BackgroundWorker {
    server: Arc<PgWireServer>,
}

impl BackgroundWorker {
    pub(crate) fn register(server: Arc<PgWireServer>) -> Self {
        server.background_workers.fetch_add(1, Ordering::SeqCst);
        Self { server }
    }

    /// Sleeps up to `total`, waking early once shutdown is requested.
    ///
    /// The loops used to sleep their whole poll interval before re-checking,
    /// so a server with the default five-second flush interval stayed open for
    /// seconds after it had reported a clean shutdown.
    pub(crate) fn sleep_until_shutdown(&self, total: Duration) -> bool {
        let slice = Duration::from_millis(25);
        let mut slept = Duration::ZERO;
        while slept < total {
            if self.server.is_shutdown_requested() {
                return false;
            }
            let step = slice.min(total - slept);
            thread::sleep(step);
            slept += step;
        }
        !self.server.is_shutdown_requested()
    }
}

impl Drop for BackgroundWorker {
    fn drop(&mut self) {
        self.server
            .background_workers
            .fetch_sub(1, Ordering::SeqCst);
    }
}

pub(crate) fn start_automatic_paged_checkpoint_worker(
    server: Arc<PgWireServer>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let _worker = BackgroundWorker::register(server.clone());
        let poll_interval = server.config.automatic_paged_checkpoint_poll_interval;
        let mut driver = match AutomaticPagedCheckpointDriver::new(
            cluster_now_ms(),
            server.config.checkpoint_interval,
            poll_interval,
            server.config.automatic_paged_checkpoint_limits,
        ) {
            Ok(driver) => driver,
            Err(error) => {
                log_operational_event(
                    "paged_checkpoint.worker_invalid",
                    "error",
                    json!({ "error": error.to_string() }),
                );
                server.request_shutdown();
                return;
            }
        };
        // Clone the intentionally narrow checkpoint capability once, then
        // release the database guard. A bounded checkpoint step performs real
        // I/O (writeback, fsync); holding the database-wide read lock across
        // it starves every `db.write()` caller behind the reader-preferring
        // lock for the whole step. The page store's own writer gate already
        // serializes the step against foreground commits.
        let handle = match server.read_db() {
            Ok(db) => db.paged_checkpoint_maintenance_handle(),
            Err(error) => {
                log_operational_event(
                    "paged_checkpoint.worker_invalid",
                    "error",
                    json!({ "error": error.to_string() }),
                );
                return;
            }
        };
        let Some(handle) = handle else {
            // Not server_paged: nothing this worker will ever have to do.
            return;
        };
        let mut consecutive_failures = 0u32;
        let mut reported_pause = None;
        while !server.is_shutdown_requested() {
            let now_ms = cluster_now_ms();
            let result = driver.tick(&handle, &server.resource_governor, now_ms);
            match result {
                Ok(AutomaticPagedCheckpointTick::Advanced(advance)) => {
                    consecutive_failures = 0;
                    reported_pause = None;
                    if let PagedCheckpointScheduleAdvance::Complete {
                        operation_id,
                        totals,
                        ..
                    } = advance
                    {
                        if let Ok(mut checkpoint) = server.last_checkpoint.lock() {
                            *checkpoint = Some(unix_timestamp());
                        }
                        log_operational_event(
                            "paged_checkpoint.completed",
                            "info",
                            json!({
                                "operation_id": operation_id,
                                "steps": totals.successful_steps,
                                "pages_written": totals.pages_written,
                                "logical_writeback_bytes": totals.logical_writeback_bytes,
                                "transactions_frozen": totals.transactions_frozen,
                                "wal_truncations": totals.wal_truncations,
                                "tail_pages_truncated": totals.tail_pages_truncated,
                            }),
                        );
                    }
                }
                Ok(AutomaticPagedCheckpointTick::Paused { operation_id }) => {
                    consecutive_failures = 0;
                    if reported_pause != Some(operation_id) {
                        reported_pause = Some(operation_id);
                        log_operational_event(
                            "paged_checkpoint.paused",
                            "warn",
                            json!({ "operation_id": operation_id }),
                        );
                    }
                }
                Ok(AutomaticPagedCheckpointTick::Idle) => {
                    consecutive_failures = 0;
                    reported_pause = None;
                }
                Err(error) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if consecutive_failures.is_power_of_two() {
                        log_operational_event(
                            "paged_checkpoint.worker_error",
                            "error",
                            json!({
                                "error": error.to_string(),
                                "consecutive_failures": consecutive_failures,
                            }),
                        );
                    }
                }
            }
            let error_backoff = if consecutive_failures == 0 {
                poll_interval
            } else {
                let shift = consecutive_failures.saturating_sub(1).min(16);
                poll_interval
                    .saturating_mul(1_u32 << shift)
                    .min(MAX_BACKGROUND_TASK_POLL_INTERVAL)
            };
            sleep_background_task_interruptibly(&server, error_backoff);
        }
    })
}

/// Autovacuum: periodically starts a durable, resource-governed vacuum
/// pass and drives its bounded steps — the dead-version counterpart of the
/// automatic checkpoint worker. Cursor and retry state live in BicDB's
/// checksummed schedule; losing this thread's deadline merely starts the
/// next pass earlier after restart.
pub(crate) fn start_automatic_paged_vacuum_worker(
    server: Arc<PgWireServer>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let _worker = BackgroundWorker::register(server.clone());
        let poll_interval = server.config.automatic_paged_vacuum_poll_interval;
        let start_interval = server.config.automatic_paged_vacuum_interval;
        let limits = server.config.automatic_paged_vacuum_limits;
        let handle = match server.read_db() {
            Ok(db) => db.paged_vacuum_maintenance_handle(),
            Err(error) => {
                log_operational_event(
                    "paged_vacuum.worker_invalid",
                    "error",
                    json!({ "error": error.to_string() }),
                );
                return;
            }
        };
        let Some(handle) = handle else {
            // Not server_paged: nothing this worker will ever have to do.
            return;
        };
        let start_interval_ms = start_interval.as_millis().min(u128::from(u64::MAX)) as u64;
        // An incomplete pass on restart resumes immediately; otherwise the
        // first pass begins one interval after boot.
        let mut next_start_at_ms = cluster_now_ms().saturating_add(start_interval_ms);
        let mut consecutive_failures = 0_u32;
        let mut reported_pause = None;
        while !server.is_shutdown_requested() {
            let now_ms = cluster_now_ms();
            let result = (|| -> Result<bool> {
                if let Some(schedule) = handle.vacuum_maintenance_status_optional()? {
                    if !schedule.completed {
                        if schedule.paused_reason.is_some() {
                            if reported_pause != Some(schedule.operation_id) {
                                reported_pause = Some(schedule.operation_id);
                                log_operational_event(
                                    "paged_vacuum.paused",
                                    "warn",
                                    json!({ "operation_id": schedule.operation_id }),
                                );
                            }
                            return Ok(false);
                        }
                        reported_pause = None;
                        let advance = handle.tick_vacuum_maintenance(
                            schedule.operation_id,
                            &server.resource_governor,
                            now_ms,
                        )?;
                        if let bicdb_core::PagedVacuumScheduleAdvance::Complete {
                            operation_id,
                            totals,
                            ..
                        } = &advance
                        {
                            next_start_at_ms = now_ms.saturating_add(start_interval_ms);
                            let punched = handle.punch_free_pages(u64::MAX);
                            log_operational_event(
                                "paged_vacuum.completed",
                                "info",
                                json!({
                                    "operation_id": operation_id,
                                    "pages_scanned": totals.pages_scanned,
                                    "versions_reclaimed": totals.versions_reclaimed,
                                    "bytes_reclaimed": totals.bytes_reclaimed,
                                    "pages_freed": totals.pages_freed,
                                    "hole_punched_bytes": punched
                                        .as_ref()
                                        .map(|report| report.bytes_punched)
                                        .unwrap_or(0),
                                }),
                            );
                        }
                        return Ok(true);
                    }
                }
                if now_ms >= next_start_at_ms {
                    let schedule = handle.start_vacuum_maintenance(now_ms, limits)?;
                    next_start_at_ms = now_ms.saturating_add(start_interval_ms);
                    handle.tick_vacuum_maintenance(
                        schedule.operation_id,
                        &server.resource_governor,
                        now_ms,
                    )?;
                    return Ok(true);
                }
                Ok(false)
            })();
            match result {
                Ok(_) => consecutive_failures = 0,
                Err(error) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if consecutive_failures.is_power_of_two() {
                        log_operational_event(
                            "paged_vacuum.worker_error",
                            "error",
                            json!({
                                "error": error.to_string(),
                                "consecutive_failures": consecutive_failures,
                            }),
                        );
                    }
                }
            }
            let error_backoff = if consecutive_failures == 0 {
                poll_interval
            } else {
                let shift = consecutive_failures.saturating_sub(1).min(16);
                poll_interval
                    .saturating_mul(1_u32 << shift)
                    .min(MAX_BACKGROUND_TASK_POLL_INTERVAL)
            };
            sleep_background_task_interruptibly(&server, error_backoff);
        }
    })
}

/// Fold hygiene: when an FTS index's unfolded write-layer tail crosses the
/// configured threshold, fold it — before the tail degrades ranked search
/// and bloats the entry keyspace. The fold runs under a database read
/// guard exactly like the SQL statement an operator would otherwise issue.
pub(crate) fn start_automatic_fts_fold_worker(server: Arc<PgWireServer>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let _worker = BackgroundWorker::register(server.clone());
        let threshold = server.config.automatic_fts_fold_threshold_entries;
        let poll_interval = server.config.automatic_fts_fold_poll_interval;
        if threshold == 0 {
            return;
        }
        let mut consecutive_failures = 0_u32;
        while !server.is_shutdown_requested() {
            let result = (|| -> Result<()> {
                let db = server.read_db()?;
                let fts_indexes: Vec<String> = db
                    .index_definitions()
                    .into_iter()
                    .filter(|definition| definition.kind == bicdb_core::IndexKind::FullText)
                    .map(|definition| definition.name)
                    .collect();
                for index in fts_indexes {
                    let over = match db.full_text_unfolded_entries_capped(&index, threshold) {
                        Ok(Some(_)) => false,
                        Ok(None) => true,
                        // Non-read-through / non-paged indexes have no tail.
                        Err(_) => false,
                    };
                    if !over {
                        continue;
                    }
                    let started = Instant::now();
                    let (terms, blocks) = db.compact_full_text_index(&index)?;
                    log_operational_event(
                        "fts_fold.completed",
                        "info",
                        json!({
                            "index": index,
                            "terms_folded": terms,
                            "blocks_written": blocks,
                            "elapsed_ms": started.elapsed().as_millis() as u64,
                        }),
                    );
                }
                Ok(())
            })();
            match result {
                Ok(()) => consecutive_failures = 0,
                Err(error) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if consecutive_failures.is_power_of_two() {
                        log_operational_event(
                            "fts_fold.worker_error",
                            "error",
                            json!({
                                "error": error.to_string(),
                                "consecutive_failures": consecutive_failures,
                            }),
                        );
                    }
                }
            }
            let backoff = if consecutive_failures == 0 {
                poll_interval
            } else {
                let shift = consecutive_failures.saturating_sub(1).min(16);
                poll_interval
                    .saturating_mul(1_u32 << shift)
                    .min(MAX_BACKGROUND_TASK_POLL_INTERVAL)
            };
            sleep_background_task_interruptibly(&server, backoff);
        }
    })
}

pub(crate) fn start_automatic_paged_read_ahead_worker(
    server: Arc<PgWireServer>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let _worker = BackgroundWorker::register(server.clone());
        let poll_interval = server.config.automatic_paged_read_ahead_poll_interval;
        let limits = server.config.automatic_paged_read_ahead_limits;
        let demand = server.config.automatic_paged_read_ahead_demand;
        // Clone the intentionally narrow page-worker capability once, then
        // release the database guard. Speculative I/O must never extend a
        // database-wide read-lock hold or delay foreground writers.
        let Some(read_ahead) = server.db.read().paged_read_ahead_handle() else {
            return;
        };
        let mut consecutive_failures = 0_u32;
        while !server.is_shutdown_requested() {
            let result = match read_ahead.queue_depth() {
                depth if depth != 0 => read_ahead
                    .step_governed(limits, &server.resource_governor, demand, cluster_now_ms())
                    .map(Some)
                    .map_err(PgWireError::from),
                _ => Ok(None),
            };

            let sleep_for = match result {
                Ok(Some(report)) => {
                    consecutive_failures = 0;
                    if report.read_errors != 0 {
                        log_operational_event(
                            "paged_read_ahead.read_errors",
                            "warn",
                            json!({
                                "read_errors": report.read_errors,
                                "logical_io_bytes": report.logical_io_bytes,
                            }),
                        );
                    }
                    if report.queue_depth_remaining == 0 {
                        poll_interval
                    } else {
                        // Continue promptly; the node governor still fences CPU,
                        // I/O tokens, and concurrent background work per step.
                        Duration::ZERO
                    }
                }
                Ok(None) => {
                    consecutive_failures = 0;
                    poll_interval
                }
                Err(error) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if consecutive_failures.is_power_of_two() {
                        log_operational_event(
                            "paged_read_ahead.worker_error",
                            "error",
                            json!({
                                "error": error.to_string(),
                                "consecutive_failures": consecutive_failures,
                            }),
                        );
                    }
                    let shift = consecutive_failures.saturating_sub(1).min(16);
                    poll_interval
                        .saturating_mul(1_u32 << shift)
                        .min(MAX_BACKGROUND_TASK_POLL_INTERVAL)
                }
            };
            sleep_background_task_interruptibly(&server, sleep_for);
        }
    })
}

/// Sleep in bounded slices so a long poll interval or failure backoff cannot
/// outlive the server's shutdown grace period.
pub(crate) fn sleep_background_task_interruptibly(server: &PgWireServer, duration: Duration) {
    const SHUTDOWN_POLL_SLICE: Duration = Duration::from_millis(100);

    let started = Instant::now();
    while !server.is_shutdown_requested() {
        let Some(remaining) = duration.checked_sub(started.elapsed()) else {
            break;
        };
        if remaining.is_zero() {
            break;
        }
        thread::sleep(remaining.min(SHUTDOWN_POLL_SLICE));
    }
}

pub(crate) fn start_distribution_supervisor(
    server: Arc<PgWireServer>,
    supervisor: ClusterSupervisor,
) {
    thread::spawn(move || {
        let _worker = BackgroundWorker::register(server.clone());
        let mut distribution = match load_distribution_config(&server.path) {
            Ok(config) => config,
            Err(error) => {
                eprintln!("bicdb cluster supervisor config error: {error}");
                server.request_shutdown();
                return;
            }
        };
        if distribution.node_tls_certificate_sha256.is_none()
            && !distribution.transport.dev_localhost_plaintext
        {
            let fingerprint = distribution
                .transport
                .tls
                .as_ref()
                .ok_or_else(|| {
                    BicDbError::Cluster(
                        "production cluster transport is missing TLS configuration".to_string(),
                    )
                })
                .and_then(|tls| {
                    replication_transport::replication_certificate_sha256(&tls.cert_path)
                });
            match fingerprint {
                Ok(fingerprint) => {
                    distribution.node_tls_certificate_sha256 = Some(fingerprint);
                    if let Err(error) =
                        save_distribution_config(&server.path, &distribution, server.config.fsync)
                    {
                        eprintln!(
                            "bicdb cluster supervisor could not persist legacy TLS identity binding: {error}"
                        );
                        server.request_shutdown();
                        return;
                    }
                }
                Err(error) => {
                    eprintln!(
                        "bicdb cluster supervisor could not load local TLS identity: {error}"
                    );
                    server.request_shutdown();
                    return;
                }
            }
        }
        let store = match DistributionStore::open(
            &server.path,
            distribution.clone(),
            server.config.fsync,
        ) {
            Ok(store) => store,
            Err(error) => {
                eprintln!("bicdb cluster supervisor topology error: {error}");
                server.request_shutdown();
                return;
            }
        };
        let initial_topology = store.topology().clone();
        {
            let db = server.db.read();
            let replication = db.replication_config();
            if !replication.enabled
                || replication.cluster_id != distribution.cluster_id.as_str()
                || replication.node_id != distribution.node_id.as_str()
            {
                eprintln!(
                    "bicdb cluster relocation WAL is not configured for {}/{}",
                    distribution.cluster_id, distribution.node_id
                );
                server.request_shutdown();
                return;
            }
        }
        let shared_topology = Arc::new(RwLock::new(initial_topology));
        let store = Arc::new(ParkingMutex::new(store));
        let metadata_consensus = match MetadataConsensusStore::open(
            &server.path,
            distribution.cluster_id.clone(),
            distribution.node_id.clone(),
            shared_topology.read().clone(),
            server.config.fsync,
        ) {
            Ok(consensus) => Arc::new(ParkingMutex::new(consensus)),
            Err(error) => {
                eprintln!("bicdb metadata consensus error: {error}");
                server.request_shutdown();
                return;
            }
        };
        let service = match ClusterDataNodeService::open(
            distribution.cluster_id.clone(),
            distribution.node_id.clone(),
            &server.path,
            Arc::clone(&server.db),
            server.config.fsync,
        ) {
            Ok(service) => Arc::new(ParkingMutex::new(service)),
            Err(error) => {
                eprintln!("bicdb cluster data service error: {error}");
                server.request_shutdown();
                return;
            }
        };
        let data_server = match start_cluster_data_server(
            &distribution.node_address,
            distribution.transport.clone(),
            Arc::clone(&shared_topology),
            Arc::clone(&service),
            None,
            Some(Arc::clone(&metadata_consensus)),
            Some(Arc::clone(&store)),
        ) {
            Ok(handle) => handle,
            Err(error) => {
                eprintln!("bicdb cluster data listener error: {error}");
                server.request_shutdown();
                return;
            }
        };
        eprintln!(
            "bicdb cluster data RPC listening on {} node={} metadata=quorum",
            data_server.local_addr(),
            distribution.node_id,
        );
        let transport = match TcpClusterRelocationTransport::new(
            distribution.cluster_id.clone(),
            distribution.node_id.clone(),
            Arc::clone(&shared_topology),
            distribution.transport.clone(),
        ) {
            Ok(transport) => transport,
            Err(error) => {
                eprintln!("bicdb cluster transport error: {error}");
                server.request_shutdown();
                return;
            }
        };
        let range_write_transport = match TcpClusterRelocationTransport::new(
            distribution.cluster_id.clone(),
            distribution.node_id.clone(),
            Arc::clone(&shared_topology),
            distribution.transport.clone(),
        ) {
            Ok(transport) => Arc::new(transport),
            Err(error) => {
                eprintln!("bicdb range-write transport error: {error}");
                server.request_shutdown();
                return;
            }
        };
        let range_write_coordinator = match RangeWriteCoordinator::new_schema_fenced(
            distribution.cluster_id.clone(),
            distribution.node_id.clone(),
            Arc::clone(&shared_topology),
            Arc::clone(&service),
            range_write_transport,
        ) {
            Ok(coordinator) => Arc::new(coordinator),
            Err(error) => {
                eprintln!("bicdb range-write coordinator error: {error}");
                server.request_shutdown();
                return;
            }
        };
        let cancellation = CancellationToken::uncancelable();
        let mut driver = match TransportClusterRelocationDriver::new(
            transport,
            ClusterRelocationTransportConfig::default(),
        ) {
            Ok(driver) => driver,
            Err(error) => {
                eprintln!("bicdb cluster relocation driver error: {error}");
                server.request_shutdown();
                return;
            }
        };
        let anti_entropy_limits = server.config.automatic_anti_entropy_limits;
        // All first-party background work on this database shares the server's
        // node governor. Repair and checkpoint I/O therefore cannot each spend
        // the full background envelope independently.
        let anti_entropy_governor = server.resource_governor.clone();
        let anti_entropy_first_attempt = cluster_now_ms()
            .checked_add(anti_entropy_limits.sweep_interval_ms)
            .unwrap_or(u64::MAX);
        let mut anti_entropy = match AutomaticRangeAntiEntropyController::open(
            &server.path,
            distribution.cluster_id.clone(),
            distribution.node_id.clone(),
            anti_entropy_first_attempt,
            anti_entropy_limits,
            server.config.fsync,
        ) {
            Ok(controller) => controller,
            Err(error) => {
                eprintln!("bicdb automatic anti-entropy controller error: {error}");
                server.request_shutdown();
                return;
            }
        };
        let mut last_seen_contact_ms = metadata_consensus.lock().last_leader_contact_ms();
        let mut election_deadline_ms =
            last_seen_contact_ms.saturating_add(metadata_election_delay_ms(
                &distribution.node_id,
                metadata_consensus.lock().status().current_term,
                distribution.metadata_election_timeout_ms,
            ));
        let mut last_quorum_ms = cluster_now_ms();
        let mut last_metadata_round = Instant::now()
            .checked_sub(Duration::from_millis(
                distribution.metadata_heartbeat_interval_ms,
            ))
            .unwrap_or_else(Instant::now);
        let mut last_supervisor_tick = Instant::now()
            .checked_sub(Duration::from_millis(supervisor.config().tick_interval_ms))
            .unwrap_or_else(Instant::now);
        let mut last_member_heartbeat = Instant::now()
            .checked_sub(Duration::from_millis(supervisor.config().tick_interval_ms))
            .unwrap_or_else(Instant::now);
        let mut last_range_recovery_attempt = Instant::now()
            .checked_sub(Duration::from_millis(supervisor.config().tick_interval_ms))
            .unwrap_or_else(Instant::now);
        let mut last_anti_entropy_tick = Instant::now()
            .checked_sub(Duration::from_millis(supervisor.config().tick_interval_ms))
            .unwrap_or_else(Instant::now);

        // Install only after every fallible supervisor component is ready.
        // The coordinator deliberately owns the node data service, which owns
        // the shared database handle; clearing it on exit is therefore also
        // the shutdown boundary that breaks that ownership cycle.
        server
            .db
            .read()
            .install_commit_admission(range_write_coordinator.clone());

        while !server.is_shutdown_requested() {
            let now_ms = cluster_now_ms();
            let (status, contact_ms) = {
                let consensus = metadata_consensus.lock();
                (consensus.status(), consensus.last_leader_contact_ms())
            };
            if contact_ms > last_seen_contact_ms {
                last_seen_contact_ms = contact_ms;
                election_deadline_ms = contact_ms.saturating_add(metadata_election_delay_ms(
                    &distribution.node_id,
                    status.current_term,
                    distribution.metadata_election_timeout_ms,
                ));
            }

            match status.role {
                MetadataConsensusRole::Leader => {
                    if last_metadata_round.elapsed()
                        >= Duration::from_millis(distribution.metadata_heartbeat_interval_ms)
                    {
                        match replicate_metadata_round(&metadata_consensus, driver.transport()) {
                            Ok(acks) => {
                                if metadata_voter_quorum(&status.voters, &acks) {
                                    last_quorum_ms = now_ms;
                                } else if now_ms.saturating_sub(last_quorum_ms)
                                    >= distribution.metadata_election_timeout_ms
                                {
                                    eprintln!(
                                        "bicdb metadata leader {} lost quorum and stepped down",
                                        distribution.node_id
                                    );
                                    let _ = metadata_consensus.lock().relinquish_leadership();
                                }
                            }
                            Err(error) => {
                                eprintln!("bicdb metadata replication round error: {error}");
                            }
                        }
                        if let Err(error) = publish_committed_metadata(
                            &server,
                            &store,
                            &metadata_consensus,
                            driver.transport(),
                        ) {
                            eprintln!("bicdb metadata publication error: {error}");
                            server.request_shutdown();
                            break;
                        }
                        last_metadata_round = Instant::now();
                    }

                    let clean_leader = {
                        let status = metadata_consensus.lock().status();
                        status.role == MetadataConsensusRole::Leader
                            && status.commit_index == status.last_log_index
                    };
                    if clean_leader
                        && last_supervisor_tick.elapsed()
                            >= Duration::from_millis(supervisor.config().tick_interval_ms)
                    {
                        let local = store
                            .lock()
                            .topology()
                            .nodes
                            .get(&distribution.node_id)
                            .cloned();
                        let Some(local) = local else {
                            eprintln!(
                                "bicdb cluster leader {} disappeared from topology",
                                distribution.node_id
                            );
                            server.request_shutdown();
                            break;
                        };
                        let labels = match local_schema_heartbeat_labels(&server, local.labels) {
                            Ok(labels) => labels,
                            Err(error) => {
                                eprintln!("bicdb schema compatibility fingerprint failed: {error}");
                                last_supervisor_tick = Instant::now();
                                continue;
                            }
                        };
                        if let Err(error) = store.lock().queue_heartbeat(
                            &distribution.node_id,
                            local.incarnation,
                            local.used_bytes,
                            local.capacity_bytes,
                            labels,
                            now_ms,
                        ) {
                            eprintln!("bicdb local cluster heartbeat failed: {error}");
                            last_supervisor_tick = Instant::now();
                            continue;
                        }
                        let staging = {
                            let store = store.lock();
                            (
                                store.topology().generation,
                                store.fork_ephemeral_with_pending_metadata_mutations(),
                            )
                        };
                        let (committed_generation, mut staged, pending_mutations) = match staging {
                            (generation, Ok((staged, pending))) => (generation, staged, pending),
                            (_, Err(error)) => {
                                eprintln!("bicdb cluster heartbeat staging error: {error}");
                                last_supervisor_tick = Instant::now();
                                continue;
                            }
                        };
                        match supervisor.tick(&mut staged, &mut driver, now_ms, &cancellation) {
                            Ok(report) => {
                                let staged_generation = staged.topology().generation;
                                if staged_generation != committed_generation {
                                    let proposal = {
                                        let mut consensus = metadata_consensus.lock();
                                        consensus.propose_topology(staged.topology().clone())
                                    };
                                    match proposal {
                                        Ok(_) => {
                                            store.lock().acknowledge_pending_metadata_mutations(
                                                &pending_mutations,
                                            );
                                            if let Ok(acks) = replicate_metadata_round(
                                                &metadata_consensus,
                                                driver.transport(),
                                            ) {
                                                if metadata_voter_quorum(&status.voters, &acks) {
                                                    last_quorum_ms = now_ms;
                                                }
                                            }
                                            if let Err(error) = publish_committed_metadata(
                                                &server,
                                                &store,
                                                &metadata_consensus,
                                                driver.transport(),
                                            ) {
                                                eprintln!(
                                                    "bicdb metadata publication error: {error}"
                                                );
                                                server.request_shutdown();
                                                break;
                                            }
                                        }
                                        Err(error) => {
                                            eprintln!("bicdb metadata proposal error: {error}");
                                        }
                                    }
                                } else {
                                    store
                                        .lock()
                                        .acknowledge_pending_metadata_mutations(&pending_mutations);
                                }
                                if staged_generation != committed_generation
                                    || !report.relocations_started.is_empty()
                                    || !report.relocation_failures.is_empty()
                                {
                                    eprintln!(
                                        "bicdb cluster supervisor staged generation {} -> {}, started={}, progressed={}, deferred={}, failures={} {:?}",
                                        committed_generation,
                                        staged_generation,
                                        report.relocations_started.len(),
                                        report.relocations_progressed.len(),
                                        report.relocations_deferred.len(),
                                        report.relocation_failures.len(),
                                        report.relocation_failures,
                                    );
                                }
                            }
                            Err(error) => {
                                eprintln!("bicdb cluster supervisor tick error: {error}");
                            }
                        }
                        last_supervisor_tick = Instant::now();
                    }
                }
                MetadataConsensusRole::Follower | MetadataConsensusRole::Candidate => {
                    if status.role == MetadataConsensusRole::Follower
                        && last_member_heartbeat.elapsed()
                            >= Duration::from_millis(supervisor.config().tick_interval_ms)
                    {
                        if let Some(leader_id) = status
                            .leader_id
                            .as_ref()
                            .filter(|leader_id| **leader_id != distribution.node_id)
                        {
                            let local = store
                                .lock()
                                .topology()
                                .nodes
                                .get(&distribution.node_id)
                                .cloned();
                            match local {
                                Some(mut local) => {
                                    local.labels = match local_schema_heartbeat_labels(
                                        &server,
                                        local.labels,
                                    ) {
                                        Ok(labels) => labels,
                                        Err(error) => {
                                            eprintln!(
                                                "bicdb schema compatibility fingerprint failed: {error}"
                                            );
                                            last_member_heartbeat = Instant::now();
                                            continue;
                                        }
                                    };
                                    if let Err(error) = driver.transport().heartbeat(
                                        leader_id,
                                        local.incarnation,
                                        local.used_bytes,
                                        local.capacity_bytes,
                                        local.labels,
                                        now_ms,
                                    ) {
                                        eprintln!(
                                            "bicdb cluster heartbeat to metadata leader {leader_id} failed: {error}"
                                        );
                                    }
                                    if status.learners.contains(&distribution.node_id)
                                        && status.commit_index == status.last_log_index
                                    {
                                        match driver.transport().promote_metadata_learner(
                                            leader_id,
                                            status.commit_index,
                                            status.topology_generation,
                                            now_ms,
                                        ) {
                                            Ok(topology) => {
                                                if let Err(error) =
                                                    driver.transport().install_topology(topology)
                                                {
                                                    eprintln!(
                                                        "bicdb metadata learner topology refresh failed: {error}"
                                                    );
                                                }
                                            }
                                            Err(error) => {
                                                eprintln!(
                                                    "bicdb metadata learner promotion request to {leader_id} failed: {error}"
                                                );
                                            }
                                        }
                                    }
                                }
                                None => {
                                    eprintln!(
                                        "bicdb cluster member {} disappeared from topology",
                                        distribution.node_id
                                    );
                                    server.request_shutdown();
                                    break;
                                }
                            }
                        }
                        last_member_heartbeat = Instant::now();
                    }
                    if !status.learners.contains(&distribution.node_id)
                        && now_ms >= election_deadline_ms
                    {
                        match run_metadata_election_round(
                            &metadata_consensus,
                            driver.transport(),
                            &status,
                            contact_ms,
                        ) {
                            Ok(true) => {
                                let elected = metadata_consensus.lock().status();
                                eprintln!(
                                    "bicdb metadata node {} became leader for term {}",
                                    distribution.node_id, elected.current_term
                                );
                                last_quorum_ms = now_ms;
                                // Establish leadership with followers in the
                                // same scheduling slice as the successful vote.
                                // Waiting for the next supervisor iteration
                                // leaves a newly elected leader vulnerable to a
                                // long deschedule: a voter can reach its own
                                // timeout before seeing the leadership barrier
                                // and needlessly advance the term again.
                                match replicate_metadata_round(
                                    &metadata_consensus,
                                    driver.transport(),
                                ) {
                                    Ok(acks) => {
                                        if metadata_voter_quorum(&elected.voters, &acks) {
                                            last_quorum_ms = cluster_now_ms();
                                        }
                                    }
                                    Err(error) => {
                                        eprintln!(
                                            "bicdb metadata leader barrier replication error: {error}"
                                        );
                                    }
                                }
                                last_metadata_round = Instant::now()
                                    .checked_sub(Duration::from_millis(
                                        distribution.metadata_heartbeat_interval_ms,
                                    ))
                                    .unwrap_or_else(Instant::now);
                            }
                            Ok(false) => {}
                            Err(error) => {
                                eprintln!("bicdb metadata election error: {error}");
                            }
                        }
                        let term = metadata_consensus.lock().status().current_term;
                        election_deadline_ms = now_ms.saturating_add(metadata_election_delay_ms(
                            &distribution.node_id,
                            term,
                            distribution.metadata_election_timeout_ms,
                        ));
                    }
                    if let Err(error) = publish_committed_metadata(
                        &server,
                        &store,
                        &metadata_consensus,
                        driver.transport(),
                    ) {
                        eprintln!("bicdb metadata follower publication error: {error}");
                        server.request_shutdown();
                        break;
                    }
                }
            }
            if last_range_recovery_attempt.elapsed()
                >= Duration::from_millis(supervisor.config().tick_interval_ms)
            {
                if let Err(error) = range_write_coordinator.recover_local_leadership(16) {
                    eprintln!("bicdb range leadership recovery deferred: {error}");
                }
                last_range_recovery_attempt = Instant::now();
            }
            if last_anti_entropy_tick.elapsed()
                >= Duration::from_millis(supervisor.config().tick_interval_ms)
                && anti_entropy.state().next_attempt_at_ms <= now_ms
            {
                let topology = shared_topology.read().clone();
                let stats = server.stats_snapshot();
                let foreground_pressure = stats.active_queries > 0
                    || stats.queued_queries > 0
                    || stats.active_reads > 0
                    || stats.queued_reads > 0
                    || stats.active_writes > 0
                    || stats.queued_writes > 0;
                match anti_entropy.tick_with_foreground_pressure(
                    &topology,
                    range_write_coordinator.as_ref(),
                    driver.transport(),
                    &anti_entropy_governor,
                    foreground_pressure,
                    now_ms,
                ) {
                    Ok(bicdb_core::AutomaticRangeAntiEntropyAdvance::RepairRequired(report)) => {
                        eprintln!(
                            "bicdb anti-entropy range {} requires certified repair: {:?}",
                            report.range_id, report.outcome
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        eprintln!("bicdb automatic anti-entropy tick deferred: {error}");
                    }
                }
                last_anti_entropy_tick = Instant::now();
            }
            thread::sleep(Duration::from_millis(25));
        }
        // Fail closed for any request still draining and release the
        // coordinator -> service -> database cycle before the server handle is
        // dropped or this database directory is reopened.
        server.db.read().clear_commit_admission();
        data_server.request_shutdown();
    });
}

pub(crate) fn metadata_election_delay_ms(
    node_id: &ClusterNodeId,
    term: u64,
    base_timeout_ms: u64,
) -> u64 {
    let digest = Sha256::digest(format!("{}:{term}", node_id.as_str()).as_bytes());
    let jitter = u64::from_be_bytes(digest[..8].try_into().unwrap()) % base_timeout_ms.max(1);
    base_timeout_ms.saturating_add(jitter)
}

pub(crate) fn metadata_voter_quorum(
    voters: &[ClusterNodeId],
    acknowledgements: &BTreeSet<ClusterNodeId>,
) -> bool {
    !voters.is_empty()
        && voters
            .iter()
            .filter(|node_id| acknowledgements.contains(*node_id))
            .count()
            > voters.len() / 2
}

pub(crate) fn run_metadata_election_round(
    consensus: &Arc<ParkingMutex<MetadataConsensusStore>>,
    transport: &TcpClusterRelocationTransport,
    observed_status: &bicdb_core::MetadataConsensusStatus,
    observed_contact_ms: u64,
) -> Result<bool> {
    let Some(request) = consensus
        .lock()
        .start_election_if_unchanged(observed_status, observed_contact_ms)?
    else {
        return Ok(false);
    };
    let voters = consensus.lock().status().voters;
    let mut votes = BTreeSet::from([request.candidate_id.clone()]);
    for voter in voters
        .iter()
        .filter(|node_id| **node_id != request.candidate_id)
    {
        match transport.request_metadata_vote(voter, request.clone()) {
            Ok(response) if response.term > request.term => {
                consensus.lock().observe_remote_term(response.term)?;
                return Ok(false);
            }
            Ok(response) if response.term == request.term && response.vote_granted => {
                votes.insert(response.voter_id);
            }
            Ok(_) => {}
            Err(error) => {
                eprintln!("bicdb metadata vote request to {voter} failed: {error}");
            }
        }
    }
    if !metadata_voter_quorum(&voters, &votes) {
        return Ok(false);
    }
    consensus.lock().become_leader(&votes)?;
    Ok(true)
}

pub(crate) fn replicate_metadata_round(
    consensus: &Arc<ParkingMutex<MetadataConsensusStore>>,
    transport: &TcpClusterRelocationTransport,
) -> Result<BTreeSet<ClusterNodeId>> {
    let status = consensus.lock().status();
    if status.role != MetadataConsensusRole::Leader {
        return Ok(BTreeSet::new());
    }
    let mut acknowledgements = BTreeSet::from([status.node_id.clone()]);
    let followers = status
        .voters
        .iter()
        .chain(status.learners.iter())
        .filter(|node_id| **node_id != status.node_id)
        .cloned()
        .collect::<BTreeSet<_>>();
    for follower in &followers {
        // Starting at one deliberately offers a snapshot to an ordinary
        // lagging follower. A failover survivor can instead have compacted a
        // commit that the newly elected leader has in its log but did not yet
        // know was committed. Such a follower returns its retained boundary
        // as `conflict_index`; retry there rather than offering the same stale
        // snapshot forever. Bound retries so a faulty peer cannot monopolize
        // the supervisor.
        let mut next_index = 1_u64;
        for _ in 0..4 {
            let request = consensus.lock().append_request_from(next_index, 16)?;
            match transport.append_metadata(follower, request) {
                Ok(response) => {
                    let success = response.success && response.term == status.current_term;
                    consensus
                        .lock()
                        .record_append_response(follower, &response)?;
                    if success {
                        acknowledgements.insert(follower.clone());
                        break;
                    }
                    if response.term != status.current_term {
                        break;
                    }
                    let leader_next = consensus.lock().status().last_log_index.saturating_add(1);
                    let retry_index = response.conflict_index.clamp(1, leader_next);
                    if retry_index == next_index {
                        break;
                    }
                    next_index = retry_index;
                }
                Err(error) => {
                    eprintln!("bicdb metadata append to {follower} failed: {error}");
                    break;
                }
            }
        }
    }
    Ok(acknowledgements)
}

pub(crate) fn publish_committed_metadata(
    server: &PgWireServer,
    store: &Arc<ParkingMutex<DistributionStore>>,
    consensus: &Arc<ParkingMutex<MetadataConsensusStore>>,
    transport: &TcpClusterRelocationTransport,
) -> Result<()> {
    let topology = consensus.lock().committed_topology().clone();
    store
        .lock()
        .install_authoritative_topology(topology.clone())?;
    transport.install_topology(topology.clone())?;
    install_server_cluster_topology(server, topology)
}

pub(crate) fn cluster_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub(crate) fn local_schema_heartbeat_labels(
    server: &PgWireServer,
    mut labels: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    let fingerprint = server.db.read().schema_compatibility_fingerprint()?;
    if fingerprint.user_collection_count == 0 && fingerprint.index_count == 0 {
        labels.insert(SCHEMA_BOOTSTRAP_NODE_LABEL.to_string(), "true".to_string());
        // An empty member is a schema recipient, never schema authority. If it
        // wins metadata leadership before its first range copy, publishing the
        // empty fingerprint as active would pin the cluster to that schema and
        // reject the real range leader's heartbeat. The one-time bootstrap
        // claim is sufficient until an authenticated relocation installs and
        // certifies the leader's schema bundle.
        labels.remove(SCHEMA_COMPATIBILITY_NODE_LABEL);
    } else {
        labels.remove(SCHEMA_BOOTSTRAP_NODE_LABEL);
        labels.insert(
            SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(),
            fingerprint.sha256,
        );
    }
    Ok(labels)
}

pub(crate) fn install_server_cluster_topology(
    server: &PgWireServer,
    topology: bicdb_core::ClusterTopology,
) -> Result<()> {
    if let Some(router) = server.distribution_router.as_ref() {
        router.install_topology(topology)?;
    }
    Ok(())
}

/// Host `MemAvailable` in MB from /proc/meminfo, or 0 if unavailable (non-Linux
/// or read error) — callers treat 0 as "unknown, do not trip the guard".
pub(crate) fn mem_available_mb() -> u64 {
    let Ok(text) = fs::read_to_string("/proc/meminfo") else {
        return 0;
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            // Format: "MemAvailable:   12345678 kB"
            if let Some(kb) = rest
                .split_whitespace()
                .next()
                .and_then(|v| v.parse::<u64>().ok())
            {
                return kb / 1024;
            }
        }
    }
    0
}

/// This process's resident set size in MB from /proc/self/statm, or 0.
pub(crate) fn self_rss_mb() -> u64 {
    let Ok(text) = fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    // Fields: size resident shared ... (in pages).
    text.split_whitespace()
        .nth(1)
        .and_then(|v| v.parse::<u64>().ok())
        .map(|pages| pages * 4096 / (1024 * 1024))
        .unwrap_or(0)
}

pub(crate) fn validate_server_config(config: &PgWireConfig) -> Result<()> {
    let derived_version_num = derive_postgres_server_version_num(&config.postgres_server_version)
        .map_err(PgWireError::Server)?;
    if config.postgres_server_version_num != derived_version_num {
        return Err(PgWireError::Server(format!(
            "postgres_server_version_num {} does not match postgres_server_version {} (expected {derived_version_num})",
            config.postgres_server_version_num, config.postgres_server_version
        )));
    }
    config.resource_governor.validate()?;
    if config.automatic_paged_checkpoint {
        config.automatic_paged_checkpoint_limits.validate()?;
        if config.automatic_paged_checkpoint_poll_interval.is_zero()
            || config.automatic_paged_checkpoint_poll_interval > MAX_BACKGROUND_TASK_POLL_INTERVAL
            || config.checkpoint_interval.is_zero()
            || config.checkpoint_interval > MAX_AUTOMATIC_CHECKPOINT_INTERVAL
        {
            return Err(PgWireError::Server(
                "automatic paged checkpoint polling must be 1ms..=60s and its start interval must be 1ms..=24h"
                    .to_string(),
            ));
        }
        config.resource_governor.validate_demand(
            ResourceLane::Compaction,
            config.automatic_paged_checkpoint_limits.demand,
        )?;
    }
    if config.automatic_paged_read_ahead {
        config
            .automatic_paged_read_ahead_limits
            .validate(bicdb_core::MIN_PAGE_SIZE)
            .map_err(|error| PgWireError::Server(error.to_string()))?;
        config.automatic_paged_read_ahead_demand.validate()?;
        if config.automatic_paged_read_ahead_poll_interval.is_zero()
            || config.automatic_paged_read_ahead_poll_interval > MAX_BACKGROUND_TASK_POLL_INTERVAL
            || config.automatic_paged_read_ahead_demand.memory_bytes < 64 * 1024
            || config.automatic_paged_read_ahead_demand.io_bytes
                < config.automatic_paged_read_ahead_limits.max_io_bytes
            || config.automatic_paged_read_ahead_demand.io_charge_bytes
                < config.automatic_paged_read_ahead_limits.max_io_bytes
        {
            return Err(PgWireError::Server(
                "automatic paged read-ahead polling and resource bounds are inconsistent"
                    .to_string(),
            ));
        }
        config.resource_governor.validate_demand(
            ResourceLane::Compaction,
            config.automatic_paged_read_ahead_demand,
        )?;
    }
    if config.max_connections == 0 {
        return Err(PgWireError::Server(
            "max_connections must be greater than zero".to_string(),
        ));
    }
    if config.max_connections_per_ip == 0 {
        return Err(PgWireError::Server(
            "max_connections_per_ip must be greater than zero".to_string(),
        ));
    }
    if config.authentication_timeout.is_zero() {
        return Err(PgWireError::Server(
            "authentication_timeout must be greater than zero".to_string(),
        ));
    }
    if config.max_pending_accepts == 0 {
        return Err(PgWireError::Server(
            "max_pending_accepts must be greater than zero".to_string(),
        ));
    }
    if config.max_active_queries == 0 {
        return Err(PgWireError::Server(
            "max_active_queries must be greater than zero".to_string(),
        ));
    }
    if config.max_queued_queries == 0 {
        return Err(PgWireError::Server(
            "max_queued_queries must be greater than zero".to_string(),
        ));
    }
    if config.max_active_reads == 0 {
        return Err(PgWireError::Server(
            "max_active_reads must be greater than zero".to_string(),
        ));
    }
    if config.max_queued_reads == 0 {
        return Err(PgWireError::Server(
            "max_queued_reads must be greater than zero".to_string(),
        ));
    }
    if config.max_active_writes == 0 {
        return Err(PgWireError::Server(
            "max_active_writes must be greater than zero".to_string(),
        ));
    }
    if config.max_queued_writes == 0 {
        return Err(PgWireError::Server(
            "max_queued_writes must be greater than zero".to_string(),
        ));
    }
    if config.max_request_bytes < 8 {
        return Err(PgWireError::Server(
            "max_request_bytes must be at least 8".to_string(),
        ));
    }
    if config.max_result_rows == 0 {
        return Err(PgWireError::Server(
            "max_result_rows must be greater than zero".to_string(),
        ));
    }
    if config.tls_cert.is_some() != config.tls_key.is_some() {
        return Err(PgWireError::Server(
            "both tls_cert and tls_key must be provided".to_string(),
        ));
    }
    if config.require_tls && config.tls_cert.is_none() {
        return Err(PgWireError::Server(
            "require_tls needs both tls_cert and tls_key".to_string(),
        ));
    }
    if config.channel_binding.is_some() && config.auth_method != AuthMethod::ScramSha256 {
        return Err(PgWireError::Server(
            "channel_binding policy requires auth_method scram-sha-256".into(),
        ));
    }
    if config.channel_binding == Some(ChannelBindingPolicy::Require) {
        if config.tls_cert.is_none() {
            return Err(PgWireError::Server(
                "channel_binding=require needs both tls_cert and tls_key".into(),
            ));
        }
        if !config.require_auth {
            return Err(PgWireError::Server(
                "channel_binding=require needs require_auth".into(),
            ));
        }
    }
    if config.tls_client_ca.is_some() {
        return Err(PgWireError::Server(
            "client certificate authentication is not supported".to_string(),
        ));
    }
    if !config.require_auth && !config.allow_remote_no_auth && !is_local_host(&config.host) {
        return Err(PgWireError::Server(format!(
            "refusing no-auth server on non-local host {}; pass --require-auth or --allow-remote-no-auth",
            config.host
        )));
    }
    if !config.require_auth && !is_local_host(&config.host) {
        eprintln!(
            "WARNING: BicDB server is running without authentication on non-local host {}",
            config.host
        );
    }
    Ok(())
}
