use bicdb_lua::{execute_workflow, Limits};
use serde_json::json;

#[test]
fn workflow_returns_json_and_uses_same_host_contract_as_javascript() {
    let mut calls = Vec::new();
    let value = execute_workflow(
        r#"
        local row = db.one('appointment', {event.id})
        return db.transaction(function(tx)
            tx.execute('update', {row.id, row.revision + 1})
            return {status='completed', revision=row.revision+1}
        end)
    "#,
        &json!({"id":"appointment-1"}),
        &Limits::default(),
        |method, args| {
            calls.push((method.to_string(), args));
            Ok(if method == "db.one" {
                json!({"id":"appointment-1", "revision":2})
            } else {
                json!(null)
            })
        },
    )
    .unwrap();
    assert_eq!(value, json!({"status":"completed","revision":3}));
    assert_eq!(
        calls.iter().map(|(m, _)| m.as_str()).collect::<Vec<_>>(),
        ["db.one", "db.begin", "tx.execute", "db.commit"]
    );
    assert_eq!(calls[2].1, json!(["update", ["appointment-1", 3]]));
}

#[test]
fn workflow_rolls_back_on_failure() {
    let mut calls = Vec::new();
    assert!(execute_workflow(
        "return db.transaction(function(tx) tx.execute('update'); error('abort') end)",
        &json!({}),
        &Limits::default(),
        |method, _| {
            calls.push(method.to_string());
            Ok(json!(null))
        }
    )
    .is_err());
    assert_eq!(calls, ["db.begin", "tx.execute", "db.rollback"]);
}
