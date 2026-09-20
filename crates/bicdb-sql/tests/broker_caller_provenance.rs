//! Broker authorization: provenance is stated, never inferred (H-14).
//!
//! `Broker::authorize` used to take `Option<&SecurityContext>`, where `None`
//! was documented as a trusted caller. That single value carried two unrelated
//! facts — "which identity is this" and "did this arrive over the network" —
//! and the embedded Rust API and an unauthenticated pgwire session both
//! produced `None`. `sql_session_for_server` derives the context from a
//! SERVER-WIDE config option, so a deployment that never set one handed `None`
//! to every ordinary authenticated client and every queue ACL went inert.
//!
//! The observable symptom was an inverted privilege ladder: on a correctly
//! configured queue, an IDENTIFIED user without the role was refused while an
//! UNIDENTIFIED caller was allowed. Absence of identity granted more authority
//! than presence of an unprivileged identity.
//!
//! Provenance is now a separate input (`BrokerCaller`). These tests pin every
//! rung of that ladder, including that admin cannot be used to escape the gate.

use bicdb_core::{BicDb, DbConfig, SecurityContext, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

const ACL: &str = r#"{"publish_roles":["ops"],"consume_roles":["ops"],"admin_roles":["ops"]}"#;

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

/// The bootstrap role is a superuser, which is how an ACL gets created in the
/// first place. Kept as a helper so each test states its own setup.
fn configure_locked_queue(db: &mut BicDb) {
    let mut admin = SqlSession::new(db);
    admin
        .execute(&format!(
            "SELECT broker_configure_queue('locked', '{ACL}') FROM generate_series(1,1)"
        ))
        .expect("a superuser SQL session must be able to declare an ACL");
}

fn context(roles: &[&str]) -> SecurityContext {
    let mut ctx = SecurityContext::new("intruder", "tenant-x");
    ctx.roles = roles.iter().map(|role| role.to_string()).collect();
    ctx
}

fn publish(session: &mut SqlSession<'_>) -> Result<bicdb_sql::SqlResult, bicdb_sql::SqlError> {
    session.execute("SELECT broker_publish('locked', '{}'::json) FROM generate_series(1,1)")
}

/// The rung that was inverted: no broker identity must not out-rank an
/// identity holding no roles.
#[test]
fn a_sql_caller_without_a_broker_identity_is_refused() {
    let (_directory, mut db) = db();
    configure_locked_queue(&mut db);
    let mut session = SqlSession::new_unprivileged(&mut db, "ordinary");
    let error = publish(&mut session).expect_err("an unidentified SQL caller must be refused");
    assert!(
        format!("{error}").contains("lacks Publish access"),
        "unexpected refusal: {error}"
    );
}

#[test]
fn an_identified_caller_without_the_role_is_refused() {
    let (_directory, mut db) = db();
    configure_locked_queue(&mut db);
    let mut session = SqlSession::new_secure(&mut db, context(&["reader"]));
    let error = publish(&mut session).expect_err("a role-less identity must be refused");
    assert!(
        format!("{error}").contains("lacks Publish access"),
        "unexpected refusal: {error}"
    );
}

#[test]
fn an_identified_caller_with_the_granted_role_succeeds() {
    let (_directory, mut db) = db();
    configure_locked_queue(&mut db);
    let mut session = SqlSession::new_secure(&mut db, context(&["ops"]));
    publish(&mut session).expect("the granted role must still be able to publish");
}

/// The escape route the narrower patch left open: rewrite the ACL, then walk
/// in. Admin is part of the gate, so it must be refused too.
#[test]
fn admin_cannot_be_used_to_remove_the_acl_and_walk_in() {
    let (_directory, mut db) = db();
    configure_locked_queue(&mut db);
    let mut session = SqlSession::new_unprivileged(&mut db, "ordinary");
    let error = session
        .execute("SELECT broker_configure_queue('locked', '{}') FROM generate_series(1,1)")
        .expect_err("rewriting another role's ACL must be refused");
    assert!(
        format!("{error}").contains("lacks Admin access"),
        "unexpected refusal: {error}"
    );
    publish(&mut session).expect_err("and the queue must still be closed afterwards");
}

/// Enumeration is part of the boundary: `broker_stats` filtered only when a
/// context was present, so an unidentified caller listed every queue.
#[test]
fn queue_enumeration_is_filtered_for_an_unidentified_caller() {
    let (_directory, mut db) = db();
    configure_locked_queue(&mut db);
    let mut session = SqlSession::new_unprivileged(&mut db, "ordinary");
    let stats = session
        .execute("SELECT broker_stats() FROM generate_series(1,1)")
        .expect("broker_stats itself is not gated");
    assert!(
        !format!("{:?}", stats.rows).contains("locked"),
        "an unidentified caller must not enumerate a queue it cannot consume: {:?}",
        stats.rows
    );
}

/// A queue with no ACL stays open, so the change does not silently close
/// deployments that never configured one.
#[test]
fn a_queue_without_an_acl_remains_open() {
    let (_directory, mut db) = db();
    let mut session = SqlSession::new_unprivileged(&mut db, "ordinary");
    session
        .execute("SELECT broker_publish('open', '{}'::json) FROM generate_series(1,1)")
        .expect("an unconfigured queue must remain publishable");
}

/// The superuser rung: an ACL must not lock the bootstrap role out of the
/// queue it configured. This is what broke the first attempt at this fix.
#[test]
fn a_superuser_sql_role_can_still_administer_a_queue_it_locked_down() {
    let (_directory, mut db) = db();
    configure_locked_queue(&mut db);
    let mut admin = SqlSession::new(&mut db);
    let config = admin
        .execute("SELECT broker_queue_config('locked') FROM generate_series(1,1)")
        .expect("a superuser must not be locked out by an ACL it declared");
    assert!(format!("{:?}", config.rows).contains("ops"));
    assert!(matches!(
        admin
            .execute("SELECT broker_publish('locked', '{}'::json) FROM generate_series(1,1)")
            .map(|result| result.rows.first().cloned()),
        Ok(Some(_))
    ));
    let _ = SqlValue::Null;
}
