use std::{fs::File, io::Read, path::PathBuf};

use anyhow::{bail, Context, Result};
use bicdb_sql::oracle::assessment::{assess_source, MAX_SOURCE_BYTES};
use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub enum OracleCommand {
    /// Produce a conservative lexical inventory of raw Oracle SQL/PLSQL files.
    Assess {
        #[arg(required = true, num_args = 1..)]
        paths: Vec<PathBuf>,
        /// Emit the complete structured inventory as JSON.
        #[arg(long)]
        json: bool,
        /// Return an error if known blockers or an incomplete scan are found.
        /// A successful check is not a migration readiness certification.
        #[arg(long)]
        fail_on_blockers: bool,
    },
}

pub fn run(command: OracleCommand) -> Result<()> {
    let OracleCommand::Assess {
        paths,
        json,
        fail_on_blockers,
    } = command;
    let mut reports = Vec::new();
    let mut total_bytes = 0usize;
    for path in paths {
        let metadata = path
            .metadata()
            .with_context(|| format!("inspect {}", path.display()))?;
        if !metadata.is_file() {
            bail!("{} is not a regular source file", path.display());
        }
        if metadata.len() > MAX_SOURCE_BYTES as u64 {
            bail!(
                "{} exceeds the 16 MiB per-file limit; split the source before assessment",
                path.display()
            );
        }
        let mut source = String::new();
        File::open(&path)
            .with_context(|| format!("open {}", path.display()))?
            .take(MAX_SOURCE_BYTES as u64 + 1)
            .read_to_string(&mut source)
            .with_context(|| format!("read UTF-8 source {}", path.display()))?;
        total_bytes = total_bytes
            .checked_add(source.len())
            .context("source size overflow")?;
        if total_bytes > 64 * 1024 * 1024 {
            bail!("assessment exceeds the 64 MiB total source limit; split the invocation");
        }
        reports.push(assess_source(&path.display().to_string(), &source));
    }
    let blocked = reports.iter().any(|r| {
        !r.lexical_scan_complete
            || r.findings_truncated
            || r.findings.iter().any(|f| f.severity == "blocker")
    });
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "format": 1, "analysis": "lexical-inventory-v1", "migration_ready": false,
                "runtime_assessment_required": true, "sources": reports
            }))?
        );
    } else {
        println!(
            "Oracle source inventory; runtime and migration qualification are still required."
        );
        for report in &reports {
            println!(
                "{}: {} objects, {} dependency leads, {} findings; scan {}",
                report.source,
                report.objects.len(),
                report.dependencies.len(),
                report.findings.len(),
                if report.lexical_scan_complete {
                    "complete"
                } else {
                    "incomplete"
                }
            );
            for finding in &report.findings {
                println!(
                    "  {}:{}:{} [{}] {}: {}",
                    report.source,
                    finding.line,
                    finding.column,
                    finding.severity,
                    finding.id,
                    finding.action
                );
            }
        }
    }
    if fail_on_blockers && blocked {
        bail!("Oracle assessment found blockers or incomplete source; inspect the inventory");
    }
    Ok(())
}
