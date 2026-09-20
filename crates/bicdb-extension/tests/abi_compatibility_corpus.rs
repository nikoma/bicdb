use std::fs;
use std::path::{Path, PathBuf};

use bicdb_extension::abi_v2::{HostCall, HostCallResult};
use bicdb_extension::ExtensionManifest;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Corpus {
    corpus_version: u32,
    abi_version: u32,
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    name: String,
    kind: Kind,
    file: String,
    expect: Expectation,
    canonical: bool,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Manifest,
    HostCall,
    HostResult,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Expectation {
    Accept,
    RejectDecode,
    RejectValidate,
}

fn artifact_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../abi/application-v2")
        .canonicalize()
        .expect("canonical ABI artifact directory")
}

#[test]
fn versioned_compatibility_corpus_matches_the_canonical_rust_abi() {
    let root = artifact_root().join("compatibility/v2");
    let index = fs::read(root.join("index.json")).expect("read compatibility index");
    let corpus: Corpus = serde_json::from_slice(&index).expect("decode compatibility index");
    assert_eq!(corpus.corpus_version, 2);
    assert_eq!(corpus.abi_version, 2);
    assert!(!corpus.cases.is_empty());

    for case in corpus.cases {
        let bytes = fs::read(root.join(&case.file))
            .unwrap_or_else(|error| panic!("read case `{}`: {error}", case.name));
        let actual = exercise(case.kind, &bytes, case.canonical);
        assert_eq!(actual, case.expect, "compatibility case `{}`", case.name);
    }
}

fn exercise(kind: Kind, bytes: &[u8], canonical: bool) -> Expectation {
    match kind {
        Kind::Manifest => match serde_json::from_slice::<ExtensionManifest>(bytes) {
            Err(_) => Expectation::RejectDecode,
            Ok(value) => match value.validate() {
                Err(_) => Expectation::RejectValidate,
                Ok(()) => {
                    assert_canonical(canonical, bytes, &value);
                    Expectation::Accept
                }
            },
        },
        Kind::HostCall => match serde_json::from_slice::<HostCall>(bytes) {
            Err(_) => Expectation::RejectDecode,
            Ok(value) => {
                assert_canonical(canonical, bytes, &value);
                Expectation::Accept
            }
        },
        Kind::HostResult => match serde_json::from_slice::<HostCallResult>(bytes) {
            Err(_) => Expectation::RejectDecode,
            Ok(value) => match value.validate() {
                Err(_) => Expectation::RejectValidate,
                Ok(()) => {
                    assert_canonical(canonical, bytes, &value);
                    Expectation::Accept
                }
            },
        },
    }
}

fn assert_canonical<T: serde::Serialize>(enabled: bool, bytes: &[u8], value: &T) {
    if enabled {
        let actual = serde_json::to_vec(value).expect("encode canonical compatibility value");
        assert_eq!(actual, trim_newline(bytes));
    }
}

fn trim_newline(bytes: &[u8]) -> &[u8] {
    bytes
        .strip_suffix(b"\n")
        .or_else(|| bytes.strip_suffix(b"\r\n"))
        .unwrap_or(bytes)
}

#[test]
fn wit_projection_pins_the_v2_transport_and_core_wasm_lowering() {
    let root = artifact_root();
    let wit = fs::read_to_string(root.join("application.wit")).expect("read ABI v2 WIT");
    let readme = fs::read_to_string(root.join("README.md")).expect("read ABI v2 mapping");

    assert!(wit.contains("package bicdb:application@2.0.0;"));
    assert!(wit.contains("call: func(request: list<u8>, response-capacity: u32)"));
    assert!(wit.contains("world application-v2"));
    assert!(readme.contains("bicdb:app/host.call"));
    assert!(readme.contains("bicdb_extension_invoke"));
}
