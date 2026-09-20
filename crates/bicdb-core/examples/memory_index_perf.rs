use std::env;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bicdb_core::{BicDb, DbConfig, MemoryIndexMode, Record};
use serde_json::json;

struct PerfArgs {
    rows: usize,
    mode: MemoryIndexMode,
    model_dir: PathBuf,
    db_path: PathBuf,
    fsync: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = PerfArgs::parse()?;
    let mut db =
        BicDb::open_with_config(&args.db_path, DbConfig::default().with_fsync(args.fsync))?;

    let started = Instant::now();
    db.register_local_onnx_embedding_model("embeddinggemma-300m", &args.model_dir, 768)?;
    let register_ms = started.elapsed().as_secs_f64() * 1000.0;

    db.create_collection("patient_notes")?;

    let started = Instant::now();
    db.create_memory_index(
        "idx_patient_notes_text_memory",
        "patient_notes",
        "text",
        "embeddinggemma-300m",
        args.mode,
    )?;
    let create_index_ms = started.elapsed().as_secs_f64() * 1000.0;

    let records = sample_notes(args.rows);
    let started = Instant::now();
    db.batch_insert("patient_notes", records)?;
    let insert_ms = started.elapsed().as_secs_f64() * 1000.0;

    let started = Instant::now();
    let report = db.process_memory_index_jobs(usize::MAX)?;
    let process_ms = started.elapsed().as_secs_f64() * 1000.0;

    let started = Instant::now();
    let hits = db.search_memory_index(
        "patient_notes",
        "text",
        "persistent cough after covid with chest tightness",
        5,
    )?;
    let search_ms = started.elapsed().as_secs_f64() * 1000.0;

    let top = hits
        .first()
        .map(|hit| format!("{}:{:.6}", hit.record.id, hit.score))
        .unwrap_or_else(|| "none".to_string());

    println!("db_path={}", args.db_path.display());
    println!("model_dir={}", args.model_dir.display());
    println!("mode={:?}", args.mode);
    println!("rows={}", args.rows);
    println!("fsync={}", args.fsync);
    println!("register_model_ms={register_ms:.3}");
    println!("create_index_ms={create_index_ms:.3}");
    println!("insert_ms={insert_ms:.3}");
    println!(
        "process_ms={process_ms:.3} processed={} failed={} pending={}",
        report.processed, report.failed, report.pending
    );
    println!("search_ms={search_ms:.3} hits={} top={top}", hits.len());
    Ok(())
}

impl PerfArgs {
    fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        let mut rows = 20;
        let mut mode = MemoryIndexMode::Async;
        let mut model_dir = PathBuf::from("models/embeddinggemma-300m-ONNX");
        let mut db_path = None;
        let mut fsync = true;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--rows" => {
                    rows = args
                        .next()
                        .ok_or("--rows requires a value")?
                        .parse::<usize>()?;
                }
                "--mode" => {
                    mode = match args.next().ok_or("--mode requires a value")?.as_str() {
                        "async" => MemoryIndexMode::Async,
                        "sync" => MemoryIndexMode::Sync,
                        other => return Err(format!("unsupported --mode `{other}`").into()),
                    };
                }
                "--model-dir" => {
                    model_dir = PathBuf::from(args.next().ok_or("--model-dir requires a value")?);
                }
                "--db" => {
                    db_path = Some(PathBuf::from(args.next().ok_or("--db requires a value")?));
                }
                "--no-fsync" => fsync = false,
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument `{other}`").into()),
            }
        }

        Ok(Self {
            rows,
            mode,
            model_dir,
            db_path: db_path.unwrap_or_else(default_db_path),
            fsync,
        })
    }
}

fn default_db_path() -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    env::temp_dir().join(format!("bicdb-memory-perf-{}-{now}", std::process::id()))
}

fn print_help() {
    println!(
        "Usage: cargo run -p bicdb-core --example memory_index_perf -- [--rows N] [--mode async|sync] [--model-dir PATH] [--db PATH] [--no-fsync]"
    );
}

fn sample_notes(rows: usize) -> Vec<Record> {
    const NOTES: &[&str] = &[
        "Persistent cough after covid with fatigue and chest tightness",
        "Yoga plan for chronic low back pain and hip mobility",
        "Rural clinic follow-up for tuberculosis exposure and night sweats",
        "Medication reconciliation after hospital discharge for hypertension",
        "Nutrition counseling for gestational diabetes with glucose logs",
        "Physical therapy plan for shoulder impingement and range of motion",
        "Behavioral health check-in for insomnia and anxiety symptoms",
        "Asthma action plan review with rescue inhaler education",
    ];

    (0..rows)
        .map(|idx| {
            let note = NOTES[idx % NOTES.len()];
            Record::new(format!("note-{idx:05}")).with_metadata(json!({
                "text": format!("{note}. Visit number {idx}."),
                "source": "memory_index_perf",
                "ordinal": idx
            }))
        })
        .collect()
}
