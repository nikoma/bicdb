use bicdb_script::{compile_typescript, execute, Limits};
use serde_json::json;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

#[test]
fn typescript_compiles_and_async_workflow_calls_host_with_bound_parameters() {
    let source = r#"
      interface Booking { id: string; revision: number }
      enum Status { Completed = "completed" }
      async function workflow(event: Booking): Promise<object> {
        const current = db.one("appointment", [event.id]);
        return db.transaction(tx => {
          tx.execute("update", [current.id, event.revision + 1]);
          return {status: Status.Completed, revision: event.revision + 1};
        });
      }
    "#;
    let script = compile_typescript(source, &Limits::default()).unwrap();
    assert!(!script.javascript.contains("interface Booking"));
    assert_eq!(script.source_sha256.len(), 64);
    let calls = Rc::new(RefCell::new(Vec::new()));
    let recorded = calls.clone();
    let value = execute(
        &script.javascript,
        &json!({"id":"a' OR 1=1", "revision":2}),
        &Limits::default(),
        move |method, args| {
            recorded.borrow_mut().push((method.to_owned(), args));
            Ok(if method == "db.one" {
                json!({"id":"a' OR 1=1"})
            } else {
                json!(null)
            })
        },
    )
    .unwrap();
    assert_eq!(value, json!({"status":"completed", "revision":3}));
    assert_eq!(
        calls
            .borrow()
            .iter()
            .map(|(m, _)| m.as_str())
            .collect::<Vec<_>>(),
        ["db.one", "db.begin", "tx.execute", "db.commit"]
    );
    assert_eq!(calls.borrow()[2].1, json!(["update", ["a' OR 1=1", 3]]));
}

#[test]
fn rejects_syntax_modules_and_async_transactions_and_rolls_back_errors() {
    assert!(compile_typescript("function workflow( {", &Limits::default()).is_err());
    assert!(compile_typescript(
        "import value from 'fs'; function workflow() {}",
        &Limits::default()
    )
    .is_err());
    for source in [
        "function workflow() { return db.transaction(tx => {throw new Error('abort')}); }",
        "function workflow() { return db.transaction(async tx => 1); }",
    ] {
        let calls = Rc::new(RefCell::new(Vec::new()));
        let recorded = calls.clone();
        assert!(
            execute(source, &json!({}), &Limits::default(), move |method, _| {
                recorded.borrow_mut().push(method.to_owned());
                Ok(json!(null))
            })
            .is_err()
        );
        assert_eq!(*calls.borrow(), ["db.begin", "db.rollback"]);
    }
}

#[test]
fn bounds_execution_microtasks_results_and_host_calls_without_os_access() {
    let limits = Limits {
        timeout: Duration::from_millis(30),
        pending_jobs: 100,
        result_bytes: 256,
        host_calls: 2,
        ..Limits::default()
    };
    let start = Instant::now();
    assert!(execute(
        "function workflow() { while(true) {} }",
        &json!({}),
        &limits,
        |_, _| Ok(json!(null))
    )
    .is_err());
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(execute(
        "function workflow() { return new Promise(() => {}); }",
        &json!({}),
        &limits,
        |_, _| Ok(json!(null))
    )
    .is_err());
    assert!(execute(
        "function workflow() { return 'a'.repeat(1000); }",
        &json!({}),
        &limits,
        |_, _| Ok(json!(null))
    )
    .is_err());
    assert!(execute(
        "function workflow() { for(let i=0;i<3;i++) secrets.get('x'); }",
        &json!({}),
        &limits,
        |_, _| Ok(json!(null))
    )
    .is_err());
    let result = execute("function workflow() { return [typeof process, typeof require, typeof fetch, typeof std]; }", &json!({}), &limits, |_,_| Err("denied".into())).unwrap();
    assert_eq!(
        result,
        json!(["undefined", "undefined", "undefined", "undefined"])
    );
}
