use bicdb_lua::{execute, Limits, Reply};

#[test]
fn binary_arguments_json_and_redis_reply_shapes() {
    let reply = execute(b"local t=cjson.decode(ARGV[1]); local s=redis.call('TYPE',KEYS[1]); return {KEYS[1],t.count,s.ok,redis.pcall('BAD').err ~= nil,false,redis.status_reply('OK')}", &[vec![0, 255]], &[br#"{"count":3}"#.to_vec()], &Limits::default(), |args| {
        if args[0] == b"TYPE" { Ok(Reply::Status("hash".into())) } else { Err("bad command".into()) }
    }).unwrap();
    assert_eq!(
        reply,
        Reply::Array(vec![
            Reply::Bulk(vec![0, 255]),
            Reply::Integer(3),
            Reply::Bulk(b"hash".to_vec()),
            Reply::Integer(1),
            Reply::Nil,
            Reply::Status("OK".into())
        ])
    );
}

#[test]
fn isolated_sandbox_and_execution_limits() {
    let limits = Limits {
        instructions: 2000,
        ..Limits::default()
    };
    let host = |_| Err("unexpected host call".into());
    assert!(execute(b"while true do end", &[], &[], &limits, host).is_err());
    assert_eq!(execute(b"return {os == nil, io == nil, package == nil, debug == nil, loadfile == nil, loadstring == nil}", &[], &[], &limits, host).unwrap(), Reply::Array(vec![Reply::Integer(1); 6]));
    execute(b"leaked = 'secret'; return 1", &[], &[], &limits, host).unwrap();
    assert_eq!(
        execute(b"return leaked", &[], &[], &limits, host).unwrap(),
        Reply::Nil
    );
    assert!(execute(b"local t={}; t[1]=t; return t", &[], &[], &limits, host).is_err());
    assert!(execute(
        b"local s=string.rep('x',1000000); local t={}; for i=1,80 do t[i]=s end; return t",
        &[],
        &[],
        &Limits::default(),
        host
    )
    .is_err());
    assert!(execute(&[0x1b, b'L', b'u', b'a'], &[], &[], &limits, host).is_err());
    let limits = Limits {
        memory_bytes: 128 * 1024,
        ..limits
    };
    assert!(execute(b"return string.rep('x',1000000)", &[], &[], &limits, host).is_err());
}

#[test]
fn json_preserves_integer_shapes_empty_arrays_and_null() {
    let result = execute(
        b"return cjson.encode(cjson.decode(ARGV[1]))",
        &[],
        &[br#"{"number":2,"fraction":2.5,"empty":[],"null":null}"#.to_vec()],
        &Limits::default(),
        |_| Err("unexpected host call".into()),
    )
    .unwrap();
    let Reply::Bulk(bytes) = result else {
        panic!("expected JSON string")
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["number"].as_u64(), Some(2));
    assert_eq!(json["fraction"].as_f64(), Some(2.5));
    assert!(json["empty"].as_array().unwrap().is_empty());
    assert!(json["null"].is_null());
}
