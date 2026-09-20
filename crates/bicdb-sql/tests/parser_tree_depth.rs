//! Iterative parsers that build recursive trees.
//!
//! The parse budget was introduced to stop recursive-descent parsers blowing
//! the stack on nested input, and it charges depth where the PARSER recurses.
//! But several loops build a left-leaning tree without recursing at all:
//!
//!     $.a.a.a…                    N-deep Expr::Member
//!     $[0][0][0]…                 N-deep Expr::Array
//!     $?(1 || 1 || 1 …)           N-deep Expr::Binary
//!     to_tsquery('a | b | c …')   N-deep PgTsQueryNode::Or
//!
//! Each step wraps the accumulated node. The nesting budget never fired
//! (nothing recursed) and the node budget is a million, so a few thousand
//! steps passed both — and then the stack overflowed in evaluation and in the
//! tree's own recursive `Drop`.
//!
//! That is not a catchable panic. A stack overflow is a hardware fault:
//! `catch_unwind` cannot contain it, so one read-only query from any
//! authenticated role aborts the process and every tenant in it.
//!
//! Depth is now charged where the depth is created — per tree-deepening step,
//! with no matching `leave`, because the level persists in the tree.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

fn db() -> (tempfile::TempDir, BicDb) {
    let directory = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    (directory, db)
}

/// Each of these aborted the process before the fix.
#[test]
fn flat_accessor_and_operator_chains_are_refused_not_fatal() {
    let (_directory, mut db) = db();
    let mut session = SqlSession::new(&mut db);
    let cases = [
        format!(
            "SELECT jsonb_path_query('{{\"a\":1}}'::jsonb, '$.{}')",
            "a.".repeat(5_000)
        ),
        format!(
            "SELECT jsonb_path_query('[1]'::jsonb, '${}')",
            "[0]".repeat(3_000)
        ),
        format!(
            "SELECT jsonb_path_query('{{\"a\":1}}'::jsonb, '$?({})')",
            vec!["1"; 50_000].join(" || ")
        ),
        format!(
            "SELECT to_tsquery('{}')",
            (0..200_000)
                .map(|index| format!("t{index}"))
                .collect::<Vec<_>>()
                .join(" | ")
        ),
    ];
    for sql in cases {
        let error = session
            .execute(&sql)
            .expect_err("a runaway chain must be refused by the budget");
        let rendered = format!("{error}");
        assert!(
            rendered.contains("nested deeper")
                || rendered.contains("depth")
                || rendered.contains("nodes"),
            "expected a budget refusal, got: {rendered}"
        );
    }
}

/// The budget must not have swallowed ordinary expressions. Real jsonpath and
/// tsquery are a handful of steps deep, far under the limit.
#[test]
fn ordinary_expressions_still_parse() {
    let (_directory, mut db) = db();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "SELECT jsonb_path_query('{\"a\":{\"b\":{\"c\":42}}}'::jsonb, '$.a.b.c')",
        "SELECT jsonb_path_query('[[[7]]]'::jsonb, '$[0][0][0]')",
        "SELECT jsonb_path_query('{\"a\":5}'::jsonb, '$?(@.a > 1 && @.a < 9 || @.a == 5)')",
        "SELECT to_tsquery('alpha | beta & gamma | delta')",
        "SELECT to_tsquery('(alpha | beta) & (gamma | delta)')",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|error| panic!("{sql} must still parse: {error}"));
    }
}

/// A chain just under the limit still works, so the cap is a real boundary
/// rather than an accidental rejection of anything repetitive.
#[test]
fn a_chain_within_the_budget_still_parses() {
    let (_directory, mut db) = db();
    let mut session = SqlSession::new(&mut db);
    let nested = format!("{}42{}", "[".repeat(50), "]".repeat(50));
    let path = format!("${}", "[0]".repeat(50));
    session
        .execute(&format!(
            "SELECT jsonb_path_query('{nested}'::jsonb, '{path}')"
        ))
        .expect("a 50-deep chain is inside the budget and must still work");
}
