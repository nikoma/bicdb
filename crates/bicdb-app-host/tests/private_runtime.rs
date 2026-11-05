use base64::{engine::general_purpose::STANDARD, Engine};
use bicdb_app_host::{
    manifest::Release,
    runtime::{find_node, Process},
};
use serde_json::{json, Value};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

fn package(source: &str) -> Release {
    serde_json::from_value(json!({"manifest":{"format":1,"name":"fixture-http","version":"1","runtime":"node_http","entrypoint":"app.mjs","files":["app.mjs"]},"files":{"app.mjs":STANDARD.encode(source)}})).unwrap()
}
fn directories(root: &Path, source: &str) -> (PathBuf, PathBuf) {
    let app = root.join("private");
    let release = app.join("release");
    fs::create_dir_all(&release).unwrap();
    fs::set_permissions(&app, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(release.join("app.mjs"), source).unwrap();
    (app, release)
}
fn configure(app: &Path, value: Value) {
    let path = app.join("runtime-private.json");
    fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}
fn start(release: &Release, directory: &Path, app: &Path) -> Process {
    Process::start(
        release,
        directory,
        app,
        "http://127.0.0.1:1/internal/fixture-http",
        "fixture-platform-capability-0123456789abcdef",
        &find_node(None).unwrap(),
    )
    .unwrap()
}

#[test]
fn private_configuration_refuses_public_files_symlinks_and_reserved_bindings() {
    use bicdb_app_host::app_config::Config;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("runtime-private.json");
    configure(
        root.path(),
        json!({"format":1,"environment":{"GREETING":"private"}}),
    );
    assert_eq!(
        Config::read(root.path()).unwrap().environment["GREETING"],
        "private"
    );
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(Config::read(root.path()).is_err());
    fs::rename(&path, root.path().join("other.json")).unwrap();
    std::os::unix::fs::symlink("other.json", &path).unwrap();
    assert!(Config::read(root.path()).is_err());
    fs::remove_file(&path).unwrap();
    for name in [
        "PATH",
        "PORT",
        "HOSTNAME",
        "BICDB_DATABASE_TOKEN",
        "NODE_OPTIONS",
        "1INVALID",
    ] {
        let mut environment = serde_json::Map::new();
        environment.insert(name.into(), json!("sensitive-fixture-value"));
        configure(root.path(), json!({"format":1,"environment":environment}));
        let error = Config::read(root.path())
            .err()
            .expect("reserved binding refused")
            .to_string();
        assert!(!error.contains("sensitive-fixture-value"));
    }
    fs::write(
        &path,
        r#"{"format":1,"environment":{"SECRET":1234567890123456789012345678901234567890}}"#,
    )
    .unwrap();
    let error = Config::read(root.path()).err().unwrap().to_string();
    assert!(!error.contains("1234567890"));
    assert!(!error.contains("SECRET"));
}

#[test]
fn private_configuration_refuses_unknown_fields_and_invalid_bounds() {
    use bicdb_app_host::app_config::Config;
    let root = tempfile::tempdir().unwrap();
    for invalid in [
        json!({"format":2}),
        json!({"format":1,"unknown":true}),
        json!({"format":1,"startup_timeout_ms":0}),
        json!({"format":1,"startup_timeout_ms":300001}),
        json!({"format":1,"request_body_limit_bytes":0}),
        json!({"format":1,"response_body_limit_bytes":134217729}),
        json!({"format":1,"environment":{"GREETING":"a".repeat(16385)}}),
        json!({"format":1,"identity":{"uid":0,"gid":0,"execution_root":"/tmp/private/execution"}}),
    ] {
        configure(root.path(), invalid);
        assert!(Config::read(root.path()).is_err());
    }
    configure(
        root.path(),
        json!({"format":1,"environment":{"GREETING":"private"}}),
    );
    let config = Config::read(root.path()).unwrap();
    let mut release = package("console.log('fixture')");
    release.manifest.isolation = bicdb_app_host::manifest::Isolation::Microvm;
    assert!(config.validate_release(&release).is_err());
    release.manifest.isolation = bicdb_app_host::manifest::Isolation::Process;
    release.manifest.placement = Some(bicdb_app_host::manifest::Placement {
        region: "fixture".into(),
    });
    assert!(config.validate_release(&release).is_err());
}

#[test]
fn private_bindings_and_an_operator_startup_deadline_apply_to_http_execution() {
    let root = tempfile::tempdir().unwrap();
    let source="import http from 'node:http'; setTimeout(()=>http.createServer((req,res)=>{res.setHeader('content-type','application/json');res.end(JSON.stringify({greeting:process.env.GREETING}));}).listen(Number(process.env.PORT),process.env.HOST),1200);";
    let release = package(source);
    let (app, directory) = directories(root.path(), source);
    configure(
        &app,
        json!({"format":1,"environment":{"GREETING":"configured"},"startup_timeout_ms":1000}),
    );
    let mut process = start(&release, &directory, &app);
    assert!(
        process
            .wait_ready(&release, "fixture-platform-capability-0123456789abcdef")
            .is_err(),
        "the configured short deadline must be honored"
    );
    process.stop();
    configure(
        &app,
        json!({"format":1,"environment":{"GREETING":"configured"},"startup_timeout_ms":3000}),
    );
    let mut process = start(&release, &directory, &app);
    process
        .wait_ready(&release, "fixture-platform-capability-0123456789abcdef")
        .unwrap();
    let value: Value = reqwest::blocking::get(format!("http://{}/", process.address))
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(value["greeting"], "configured");
    assert_eq!(
        release.files.len(),
        1,
        "operator bindings are outside the release"
    );
    process.stop();
}

#[test]
#[ignore = "requires Linux root and a dedicated free fixture UID; run explicitly with --ignored"]
fn isolated_http_cannot_read_private_state_or_receive_platform_capabilities() {
    assert_eq!(
        unsafe { libc::geteuid() },
        0,
        "run this qualification as root"
    );
    const UID: u32 = 64977;
    for entry in fs::read_dir("/proc").unwrap().flatten() {
        if entry.file_name().to_string_lossy().parse::<u32>().is_ok() {
            use std::os::unix::fs::MetadataExt;
            if let Ok(metadata) = entry.metadata() {
                assert_ne!(
                    metadata.uid(),
                    UID,
                    "fixture UID is already in use; preserve that process"
                );
            }
        }
    }
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o711)).unwrap();
    let private_path = root.path().join("private/operator-only.txt");
    let source=r#"import http from 'node:http'; import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
let privateDenied=false;try{fs.readFileSync(__PRIVATE__);}catch{privateDenied=true;}
let writeDenied=false;try{fs.writeFileSync(new URL('./app.mjs',import.meta.url),'fixture mutation');}catch{writeDenied=true;}
fs.writeFileSync(path.join(os.tmpdir(),'work-proof'),'ok');
let capabilitySeen=false;
http.createServer((req,res)=>{capabilitySeen ||= Boolean(req.headers['x-bic-internal-token']);res.setHeader('content-type','application/json');res.end(JSON.stringify({uid:process.getuid(),gid:process.getgid(),groups:process.getgroups(),kernelGroups:fs.readFileSync('/proc/self/status','utf8').match(/^Groups:(.*)$/m)?.[1].trim(),privateDenied,writeDenied,platformEnv:Boolean(process.env.BICDB_DATABASE_TOKEN),capabilitySeen,greeting:process.env.GREETING,noNewPrivileges:/^NoNewPrivs:\s+1$/m.test(fs.readFileSync('/proc/self/status','utf8'))}));}).listen(Number(process.env.PORT),process.env.HOST);"#
 .replace("__PRIVATE__",&serde_json::to_string(private_path.to_str().unwrap()).unwrap());
    let release = package(&source);
    let (app, directory) = directories(root.path(), &source);
    fs::write(&private_path, "owner-only fixture data").unwrap();
    let execution = root.path().join("execution");
    fs::create_dir(&execution).unwrap();
    fs::set_permissions(&execution, fs::Permissions::from_mode(0o711)).unwrap();
    configure(
        &app,
        json!({"format":1,"environment":{"GREETING":"configured"},"identity":{"uid":UID,"gid":UID,"execution_root":execution},"startup_timeout_ms":10000}),
    );
    let mut process = start(&release, &directory, &app);
    process
        .wait_ready(&release, "fixture-platform-capability-0123456789abcdef")
        .unwrap();
    let value: Value = reqwest::blocking::get(format!("http://{}/", process.address))
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(value["uid"], UID);
    assert_eq!(value["gid"], UID);
    // Node includes the effective primary GID in getgroups(). The kernel
    // supplementary group list itself must be empty.
    assert_eq!(value["groups"], json!([UID]));
    assert_eq!(value["kernelGroups"], "");
    for key in ["privateDenied", "writeDenied", "noNewPrivileges"] {
        assert_eq!(value[key], true, "{key}");
    }
    assert_eq!(value["platformEnv"], false);
    assert_eq!(value["capabilitySeen"], false);
    assert_eq!(value["greeting"], "configured");
    process.stop();
    assert_eq!(
        fs::read_dir(execution.join("work")).unwrap().count(),
        0,
        "stopped execution work is removed"
    );
}
