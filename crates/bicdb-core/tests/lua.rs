#![cfg(feature = "lua")]
use bicdb_core::{BicDb, DbConfig, LuaLimits, LuaReply, Record};

#[test]
fn lua_native_commit_rollback_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let config = DbConfig::default().with_fsync(true);
    let mut db = BicDb::open_with_config(directory.path(), config.clone()).unwrap();
    db.create_collection("appointments").unwrap();
    db.create_collection("tasks").unwrap();
    let appointment = serde_json::to_vec(
        &Record::new("a1").with_metadata(serde_json::json!({"status":"booked", "revision": 1})),
    )
    .unwrap();
    let task =
        serde_json::to_vec(&Record::new("t1").with_metadata(serde_json::json!({"status":"open"})))
            .unwrap();
    let args = [appointment, task];
    assert_eq!(db.eval_lua(b"db.call('PUT', 'appointments', ARGV[1]); db.call('PUT', 'tasks', ARGV[2]); return db.call('EXISTS', 'appointments', 'a1')", &[], &args, &LuaLimits::default()).unwrap(), LuaReply::Integer(1));
    assert!(db
        .eval_lua(
            b"db.call('DELETE', 'appointments', 'a1'); error('abort')",
            &[],
            &[],
            &LuaLimits::default()
        )
        .is_err());
    assert!(db.get("appointments", "a1").unwrap().is_some());
    assert_eq!(db.eval_lua(b"local r=cjson.decode(db.call('GET','appointments','a1')); r.timestamp=123; r.metadata.revision=r.metadata.revision+1; db.call('PUT','appointments',cjson.encode(r)); return r.metadata.revision", &[], &[], &LuaLimits::default()).unwrap(), LuaReply::Integer(2));
    assert_eq!(
        db.get("appointments", "a1").unwrap().unwrap().timestamp,
        Some(123)
    );
    let mut limits = LuaLimits::default();
    limits.instructions = 2000;
    assert!(db
        .eval_lua(
            b"db.call('DELETE', 'tasks', 't1'); while true do end",
            &[],
            &[],
            &limits
        )
        .is_err());
    assert!(db.get("tasks", "t1").unwrap().is_some());
    drop(db);
    let db = BicDb::open_with_config(directory.path(), config).unwrap();
    assert!(db.get("appointments", "a1").unwrap().is_some());
    assert!(db.get("tasks", "t1").unwrap().is_some());
}
