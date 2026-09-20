//! Regression: a mostly-conflicting multi-row `ON CONFLICT DO UPDATE` inside
//! a transaction must stay proportional to the BATCH, not to the batch
//! squared or the table size.
//!
//! The crawl-ingestion incident hit this: conflict detection did the right
//! O(log n) unique-index lookup and then merged the transaction's entire
//! pending write set per input row, cloning a full record (payload JSON
//! included) per candidate. Every processed row appends a write, so the cost
//! grew with the square of the batch and the write blew past its 30s timeout.

use std::time::Instant;

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn session_db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    (dir, db)
}

fn seed(sql: &mut SqlSession, rows: usize) {
    sql.execute(
        "CREATE TABLE landings (\
           id TEXT PRIMARY KEY, \
           source_message_id TEXT UNIQUE, \
           broker_message_id TEXT, \
           payload_sha256 TEXT, \
           payload TEXT)",
    )
    .unwrap();
    for chunk in (0..rows).collect::<Vec<_>>().chunks(500) {
        let values: Vec<String> = chunk
            .iter()
            .map(|index| {
                format!(
                    "('id-{index:06}', 'src-{index:06}', 'b-{index:06}', 'sha-{index:06}', '{}')",
                    "x".repeat(500)
                )
            })
            .collect();
        sql.execute(&format!(
            "INSERT INTO landings VALUES {}",
            values.join(", ")
        ))
        .unwrap();
    }
}

/// One batch, all rows conflicting, inside an explicit transaction — the
/// exact drain shape.
fn upsert_batch(sql: &mut SqlSession, batch: usize) -> std::time::Duration {
    let values: Vec<String> = (0..batch)
        .map(|index| {
            format!("('id-{index:06}', 'src-{index:06}', 'nb-{index:06}', 'sha-{index:06}', 'p')")
        })
        .collect();
    let statement = format!(
        "INSERT INTO landings (id, source_message_id, broker_message_id, payload_sha256, payload) \
         VALUES {} \
         ON CONFLICT (source_message_id) DO UPDATE \
           SET broker_message_id = EXCLUDED.broker_message_id \
           WHERE landings.payload_sha256 = EXCLUDED.payload_sha256 \
         RETURNING source_message_id",
        values.join(", ")
    );
    sql.execute("BEGIN").unwrap();
    let started = Instant::now();
    let result = sql.execute(&statement).unwrap();
    let elapsed = started.elapsed();
    assert_eq!(result.rows.len(), batch, "every row must be returned");
    sql.execute("COMMIT").unwrap();
    elapsed
}

#[test]
fn conflicting_upsert_scales_with_batch_not_batch_squared() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    seed(&mut sql, 5_000);

    // Warm any lazy structures so the first measurement isn't the outlier.
    upsert_batch(&mut sql, 10);

    let small = upsert_batch(&mut sql, 25);
    let large = upsert_batch(&mut sql, 100);

    // 4x the rows must not cost ~16x the time. Quadratic growth would put
    // `large` at >= 16x `small`; linear is ~4x. Allow generous slack for
    // timer noise on a loaded machine while still failing on quadratic.
    let ratio = large.as_secs_f64() / small.as_secs_f64().max(1e-6);
    assert!(
        ratio < 9.0,
        "4x batch cost {ratio:.1}x time ({small:?} -> {large:?}) — conflict \
         detection is scaling super-linearly in the batch"
    );

    // Correctness: the conditional update applied, and non-matching
    // payload_sha256 rows are left alone.
    let rows = sql
        .execute("SELECT broker_message_id FROM landings WHERE source_message_id = 'src-000005'")
        .unwrap();
    assert_eq!(
        rows.rows[0][0],
        SqlValue::String("nb-000005".to_string()),
        "the DO UPDATE must have applied"
    );
}

/// Read-your-writes must survive the fast path: a row inserted earlier in the
/// SAME transaction has to be seen as a conflict.
#[test]
fn pending_transaction_writes_still_conflict() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    seed(&mut sql, 50);

    sql.execute("BEGIN").unwrap();
    // Not committed yet: only visible through the transaction's pending set.
    sql.execute("INSERT INTO landings VALUES ('id-900000', 'src-900000', 'b-900', 'sha-900', 'p')")
        .unwrap();
    // Upserting the same source_message_id must conflict with that pending row.
    let result = sql
        .execute(
            "INSERT INTO landings (id, source_message_id, broker_message_id, payload_sha256, payload) \
             VALUES ('id-900001', 'src-900000', 'nb-900', 'sha-900', 'p') \
             ON CONFLICT (source_message_id) DO UPDATE \
               SET broker_message_id = EXCLUDED.broker_message_id \
             RETURNING source_message_id",
        )
        .unwrap();
    assert_eq!(
        result.rows.len(),
        1,
        "pending row must be found as a conflict"
    );
    sql.execute("COMMIT").unwrap();

    // Exactly one row for that key, carrying the updated value.
    let rows = sql
        .execute(
            "SELECT id, broker_message_id FROM landings WHERE source_message_id = 'src-900000'",
        )
        .unwrap();
    assert_eq!(
        rows.rows.len(),
        1,
        "the upsert must not have created a duplicate"
    );
    assert_eq!(rows.rows[0][1], SqlValue::String("nb-900".to_string()));
}

/// A row deleted in this transaction must NOT be reported as a conflict even
/// though the committed index still lists it.
///
/// KNOWN PRE-EXISTING BUG (fails identically before this change): the unique
/// constraint validation that runs on the insert does not honour the
/// transaction's pending DELETE, so this raises 23505 instead of inserting.
/// Recorded here rather than silently omitted; unignore when the constraint
/// path learns to consult pending deletes.
#[ignore = "pre-existing: unique validation ignores a pending delete (23505)"]
#[test]
fn row_deleted_in_transaction_is_not_a_conflict() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    seed(&mut sql, 50);

    sql.execute("BEGIN").unwrap();
    sql.execute("DELETE FROM landings WHERE source_message_id = 'src-000010'")
        .unwrap();
    sql.execute(
        "INSERT INTO landings (id, source_message_id, broker_message_id, payload_sha256, payload) \
         VALUES ('id-new10', 'src-000010', 'fresh', 'sha-new', 'p') \
         ON CONFLICT (source_message_id) DO UPDATE \
           SET broker_message_id = EXCLUDED.broker_message_id",
    )
    .unwrap();
    sql.execute("COMMIT").unwrap();

    let rows = sql
        .execute(
            "SELECT id, broker_message_id FROM landings WHERE source_message_id = 'src-000010'",
        )
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], SqlValue::String("id-new10".to_string()));
    assert_eq!(rows.rows[0][1], SqlValue::String("fresh".to_string()));
}

/// The per-statement arbiter memo must never answer from stale catalog
/// state: an index created between statements has to be seen.
#[test]
fn arbiter_memo_does_not_survive_index_ddl() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE m (id TEXT PRIMARY KEY, k TEXT, note TEXT)")
        .unwrap();
    sql.execute("INSERT INTO m VALUES ('a', 'k1', 'first')")
        .unwrap();

    // Populate the memo for (m, [k]) while NO unique index on k exists.
    sql.execute(
        "INSERT INTO m (id, k, note) VALUES ('b', 'k2', 'second') \
         ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note",
    )
    .unwrap();

    // Now add the unique index and conflict on it: the memo must be gone.
    sql.execute("CREATE UNIQUE INDEX m_k ON m (k)").unwrap();
    sql.execute(
        "INSERT INTO m (id, k, note) VALUES ('c', 'k1', 'updated') \
         ON CONFLICT (k) DO UPDATE SET note = EXCLUDED.note",
    )
    .unwrap();

    // 'k1' must still be a single row (the conflict was detected), not a
    // duplicate that a stale "no arbiter" answer would have allowed.
    let rows = sql
        .execute("SELECT count(*) FROM m WHERE k = 'k1'")
        .unwrap();
    assert_eq!(
        rows.rows[0][0],
        SqlValue::Int(1),
        "stale arbiter memo let a duplicate through"
    );
}
