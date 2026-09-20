use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::path::{Path, PathBuf};
#[cfg(feature = "embeddings")]
use std::sync::{Arc, OnceLock};

#[cfg(feature = "embeddings")]
use ort::session::Session;
#[cfg(feature = "embeddings")]
use ort::value::Tensor;
#[cfg(feature = "embeddings")]
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Digest;
#[cfg(feature = "embeddings")]
use tokenizers::{Tokenizer, TruncationParams};
use uuid::Uuid;

use crate::error::{BicDbError, Result};
use crate::record::Record;
use crate::vector::{self, VectorMetric, VectorSearchResult};

#[cfg(feature = "embeddings")]
const EMBEDDINGGEMMA_PREFIX: &str = "title: none | text: ";
#[cfg(feature = "embeddings")]
const EMBEDDING_MAX_TOKENS: usize = 2048;
#[cfg(feature = "embeddings")]
const ONNX_SENTENCE_EMBEDDING_OUTPUT: &str = "sentence_embedding";

#[cfg(feature = "embeddings")]
static ONNX_EMBEDDERS: OnceLock<Mutex<BTreeMap<String, Arc<Mutex<OnnxEmbeddingModel>>>>> =
    OnceLock::new();

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum EmbeddingProviderKind {
    Local,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum EmbeddingRuntime {
    Deterministic,
    Onnx,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelRegistryEntry {
    pub name: String,
    pub provider: EmbeddingProviderKind,
    pub runtime: EmbeddingRuntime,
    pub dimension: usize,
    pub model_dir: Option<PathBuf>,
    pub model_file: Option<PathBuf>,
    pub external_data_file: Option<PathBuf>,
    pub tokenizer_file: Option<PathBuf>,
    #[serde(default)]
    pub checksums: BTreeMap<String, String>,
}

impl ModelRegistryEntry {
    pub fn local_test(name: impl Into<String>, dimension: usize) -> Self {
        Self {
            name: name.into(),
            provider: EmbeddingProviderKind::Local,
            runtime: EmbeddingRuntime::Deterministic,
            dimension,
            model_dir: None,
            model_file: None,
            external_data_file: None,
            tokenizer_file: None,
            checksums: BTreeMap::new(),
        }
    }

    pub fn local_onnx(
        name: impl Into<String>,
        model_dir: impl AsRef<Path>,
        dimension: usize,
    ) -> Result<Self> {
        let name = name.into();
        let model_dir = model_dir.as_ref().to_path_buf();
        let model_file = model_dir.join("onnx").join("model_q4.onnx");
        let external_data_file = model_dir.join("onnx").join("model_q4.onnx_data");
        let tokenizer_file = model_dir.join("tokenizer.json");
        if name.trim().is_empty() || dimension == 0 {
            return Err(memory_error(
                "embedding model name and dimension must be configured",
            ));
        }
        for path in [&model_file, &external_data_file, &tokenizer_file] {
            if !path.is_file() {
                return Err(memory_error(format!(
                    "embedding model file not found: {}",
                    path.display()
                )));
            }
        }
        let checksums = BTreeMap::from([
            (
                "onnx/model_q4.onnx".to_string(),
                embedding_file_sha256(&model_file)?,
            ),
            (
                "onnx/model_q4.onnx_data".to_string(),
                embedding_file_sha256(&external_data_file)?,
            ),
            (
                "tokenizer.json".to_string(),
                embedding_file_sha256(&tokenizer_file)?,
            ),
        ]);
        Ok(Self {
            name,
            provider: EmbeddingProviderKind::Local,
            runtime: EmbeddingRuntime::Onnx,
            dimension,
            model_dir: Some(model_dir),
            model_file: Some(model_file),
            external_data_file: Some(external_data_file),
            tokenizer_file: Some(tokenizer_file),
            checksums,
        })
    }

    pub fn embed(&self, text: &str) -> Result<Vec<f32>> {
        embed_text(self, text)
    }
}

fn embedding_file_sha256(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut digest = sha2::Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryIndexMode {
    Async,
    Sync,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryIndexConsistency {
    Eventual,
    Immediate,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryIndexDefinition {
    pub name: String,
    pub collection: String,
    pub field: String,
    pub model: String,
    pub mode: MemoryIndexMode,
    pub consistency: MemoryIndexConsistency,
    pub created_at: i64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryJobStatus {
    Pending,
    Indexed,
    Failed,
    ModelMissing,
    Stale,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryIndexJob {
    pub job_id: Uuid,
    pub index_name: String,
    pub collection: String,
    pub record_id: String,
    pub field: String,
    pub model: String,
    pub source_hash: String,
    pub status: MemoryJobStatus,
    pub error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryIndexProcessReport {
    pub processed: usize,
    pub failed: usize,
    pub pending: usize,
    /// Why jobs failed, most recent last.
    ///
    /// The counts alone are not actionable: "indexing failed for 1 job(s)" gives
    /// an operator nothing to act on, while the underlying cause is often
    /// specific and fixable ("ONNX Runtime shared library not found ..."). The
    /// reasons are already recorded on each job; carrying them here stops them
    /// being discarded on the way out.
    #[serde(default)]
    pub failures: Vec<String>,
}

impl MemoryIndexProcessReport {
    /// A one-line summary naming the distinct causes, for an error message.
    pub fn failure_summary(&self) -> String {
        let mut distinct: Vec<&str> = Vec::new();
        for failure in &self.failures {
            if !distinct.contains(&failure.as_str()) {
                distinct.push(failure);
            }
        }
        if distinct.is_empty() {
            return "no reason recorded".to_string();
        }
        distinct.join("; ")
    }
}

pub(crate) fn memory_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Memory(message.into())
}

pub(crate) fn record_memory_text(record: &Record, field: &str) -> Option<String> {
    match field {
        "id" => Some(record.id.clone()),
        "payload" => record
            .payload
            .as_deref()
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .map(str::to_string),
        field => record
            .metadata
            .as_object()
            .and_then(|metadata| metadata.get(field))
            .and_then(json_text),
    }
}

fn json_text(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Null => None,
        Value::Bool(_) | Value::Number(_) => Some(value.to_string()),
        Value::Array(_) | Value::Object(_) => Some(value.to_string()),
    }
}

pub(crate) fn source_hash(text: &str) -> String {
    hex::encode(sha2::Sha256::digest(text.as_bytes()))
}

pub(crate) fn deterministic_embedding(text: &str, dimension: usize) -> Result<Vec<f32>> {
    if dimension == 0 {
        return Err(memory_error(
            "embedding dimension must be greater than zero",
        ));
    }
    let mut vector = vec![0.0f32; dimension];
    for token in tokenize(text) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        token.hash(&mut hasher);
        let hash = hasher.finish();
        let idx = (hash as usize) % dimension;
        let sign = if (hash >> 63) == 0 { 1.0 } else { -1.0 };
        vector[idx] += sign;
    }
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm == 0.0 {
        vector[0] = 1.0;
        return Ok(vector);
    }
    for value in &mut vector {
        *value /= norm;
    }
    vector::validate_vector(&vector)?;
    Ok(vector)
}

pub(crate) fn embed_text(model: &ModelRegistryEntry, text: &str) -> Result<Vec<f32>> {
    match model.runtime {
        EmbeddingRuntime::Deterministic => deterministic_embedding(text, model.dimension),
        EmbeddingRuntime::Onnx => onnx_embedding(model, text),
    }
}

// Same runtime contract as a missing onnxruntime .so under load-dynamic:
// the model registry still round-trips, only inference is unavailable.
#[cfg(not(feature = "embeddings"))]
fn onnx_embedding(model: &ModelRegistryEntry, _text: &str) -> Result<Vec<f32>> {
    Err(memory_error(format!(
        "embedding model `{}` requires ONNX inference, but this build has no `embeddings` support",
        model.name
    )))
}

/// Where the ONNX Runtime shared library is expected, under `load-dynamic`.
#[cfg(feature = "embeddings")]
const ORT_DYLIB_ENV: &str = "ORT_DYLIB_PATH";

/// Confirm the ONNX Runtime shared library is present before calling into `ort`.
///
/// # Why this exists
///
/// `ort` is built with `load-dynamic`, so it resolves `libonnxruntime` at
/// runtime. When that load FAILS, `ort` builds its error by calling
/// `ort::api()` — which re-enters the very `OnceLock` it is already inside, and
/// blocks on it forever:
///
/// ```text
/// ort::load_dylib_from_path        <- load fails
///   .map_err(|e| Error::new(..))   <- constructing the error
///     ort::api()
///       G_ORT_API.get_or_init(..)
///         Once::call_once_force    <- waits on the Once this thread holds
/// ```
///
/// So a missing runtime library does not produce an error, it produces a
/// **hang** — in a default-featured build, in any process that tries local
/// embedding. That is an upstream `ort` bug, but BicDB is what deadlocks, so
/// BicDB checks first and returns something an operator can act on.
///
/// The check is deliberately a file-existence test rather than a trial load:
/// attempting the load is exactly the operation that hangs.
#[cfg(feature = "embeddings")]
fn ensure_onnx_runtime_available() -> Result<()> {
    if let Ok(path) = std::env::var(ORT_DYLIB_ENV) {
        if std::path::Path::new(&path).is_file() {
            return Ok(());
        }
        return Err(memory_error(format!(
            "{ORT_DYLIB_ENV} points at `{path}`, which is not a file; ONNX inference cannot start"
        )));
    }

    // Default search locations for the runtime under load-dynamic.
    const CANDIDATES: &[&str] = &[
        "/usr/local/lib/libonnxruntime.so",
        "/usr/lib/libonnxruntime.so",
        "/usr/lib/x86_64-linux-gnu/libonnxruntime.so",
        "/usr/local/lib/libonnxruntime.dylib",
        "/opt/homebrew/lib/libonnxruntime.dylib",
    ];
    if CANDIDATES
        .iter()
        .any(|candidate| std::path::Path::new(candidate).is_file())
    {
        return Ok(());
    }

    Err(memory_error(format!(
        "ONNX Runtime shared library not found. Local embedding models need libonnxruntime \
installed, or {ORT_DYLIB_ENV} set to its path. Checked: {}. Proceeding would hang rather than \
fail: `ort` deadlocks in its own error path when the library cannot be loaded.",
        CANDIDATES.join(", ")
    )))
}

#[cfg(feature = "embeddings")]
fn onnx_embedding(model: &ModelRegistryEntry, text: &str) -> Result<Vec<f32>> {
    ensure_onnx_runtime_available()?;
    let model_file = model.model_file.as_ref().ok_or_else(|| {
        memory_error(format!("embedding model `{}` has no ONNX file", model.name))
    })?;
    let tokenizer_file = model.tokenizer_file.as_ref().ok_or_else(|| {
        memory_error(format!(
            "embedding model `{}` has no tokenizer file",
            model.name
        ))
    })?;
    let key = format!(
        "{}:{}:{}:{}:{}:{}",
        model.name,
        model_file.display(),
        model
            .checksums
            .get("onnx/model_q4.onnx")
            .map(String::as_str)
            .unwrap_or(""),
        model
            .checksums
            .get("tokenizer.json")
            .map(String::as_str)
            .unwrap_or(""),
        model
            .checksums
            .get("onnx/model_q4.onnx_data")
            .map(String::as_str)
            .unwrap_or(""),
        model.dimension
    );
    let embedder = {
        let mut embedders = ONNX_EMBEDDERS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock();
        if let Some(embedder) = embedders.get(&key) {
            Arc::clone(embedder)
        } else {
            let embedder = Arc::new(Mutex::new(OnnxEmbeddingModel::load(
                model_file.clone(),
                tokenizer_file.clone(),
                model.dimension,
            )?));
            embedders.insert(key, Arc::clone(&embedder));
            embedder
        }
    };
    let embedding = embedder.lock().embed(text);
    embedding
}

#[cfg(feature = "embeddings")]
struct OnnxEmbeddingModel {
    tokenizer: Tokenizer,
    session: Session,
    dimension: usize,
}

#[cfg(feature = "embeddings")]
impl OnnxEmbeddingModel {
    fn load(model_file: PathBuf, tokenizer_file: PathBuf, dimension: usize) -> Result<Self> {
        let mut tokenizer = Tokenizer::from_file(&tokenizer_file).map_err(|error| {
            memory_error(format!(
                "failed to load tokenizer `{}`: {error}",
                tokenizer_file.display()
            ))
        })?;
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: EMBEDDING_MAX_TOKENS,
                ..Default::default()
            }))
            .map_err(|error| memory_error(format!("failed to configure tokenizer: {error}")))?;
        let session = Session::builder()
            .and_then(|mut builder| builder.commit_from_file(&model_file))
            .map_err(|error| {
                memory_error(format!(
                    "failed to load ONNX embedding model `{}`: {error}",
                    model_file.display()
                ))
            })?;
        if !session
            .outputs()
            .iter()
            .any(|output| output.name() == ONNX_SENTENCE_EMBEDDING_OUTPUT)
        {
            return Err(memory_error(format!(
                "ONNX embedding model `{}` does not expose `{}`",
                model_file.display(),
                ONNX_SENTENCE_EMBEDDING_OUTPUT
            )));
        }
        Ok(Self {
            tokenizer,
            session,
            dimension,
        })
    }

    fn embed(&mut self, text: &str) -> Result<Vec<f32>> {
        let input = format!("{EMBEDDINGGEMMA_PREFIX}{text}");
        let encoding = self
            .tokenizer
            .encode(input, true)
            .map_err(|error| memory_error(format!("failed to tokenize embedding text: {error}")))?;
        let input_ids = encoding
            .get_ids()
            .iter()
            .map(|id| i64::from(*id))
            .collect::<Vec<_>>();
        if input_ids.is_empty() {
            return Err(memory_error("tokenizer produced no input ids"));
        }
        let attention_mask = encoding
            .get_attention_mask()
            .iter()
            .map(|id| i64::from(*id))
            .collect::<Vec<_>>();
        let token_count = input_ids.len();
        let input_ids = Tensor::<i64>::from_array(([1usize, token_count], input_ids))
            .map_err(|error| memory_error(format!("failed to create input_ids tensor: {error}")))?;
        let attention_mask = Tensor::<i64>::from_array(([1usize, token_count], attention_mask))
            .map_err(|error| {
                memory_error(format!("failed to create attention_mask tensor: {error}"))
            })?;
        let mut outputs = self
            .session
            .run(ort::inputs! {
                "input_ids" => input_ids,
                "attention_mask" => attention_mask,
            })
            .map_err(|error| memory_error(format!("ONNX embedding inference failed: {error}")))?;
        let output: Tensor<f32> = outputs
            .remove(ONNX_SENTENCE_EMBEDDING_OUTPUT)
            .ok_or_else(|| {
                memory_error(format!(
                    "ONNX embedding output `{ONNX_SENTENCE_EMBEDDING_OUTPUT}` missing"
                ))
            })?
            .downcast()
            .map_err(|error| memory_error(format!("invalid ONNX embedding output: {error}")))?;
        let (_shape, data) = output.extract_tensor();
        let vector = data.to_vec();
        if vector.len() != self.dimension {
            return Err(memory_error(format!(
                "ONNX embedding dimension mismatch: expected {}, got {}",
                self.dimension,
                vector.len()
            )));
        }
        vector::validate_vector(&vector)?;
        Ok(vector)
    }
}

fn tokenize(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split(|ch: char| !ch.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
}

pub(crate) fn score_memory_results(
    records: Vec<Record>,
    query_vector: &[f32],
    top_k: usize,
) -> Result<Vec<VectorSearchResult>> {
    if top_k == 0 {
        return Err(BicDbError::InvalidTopK);
    }
    let mut hits = Vec::new();
    for record in records {
        let Some(vector) = record.vector.as_deref() else {
            continue;
        };
        if vector.len() != query_vector.len() {
            continue;
        }
        hits.push(VectorSearchResult {
            score: vector::score(VectorMetric::Cosine, query_vector, vector),
            record,
        });
    }
    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.record.id.cmp(&right.record.id))
    });
    hits.truncate(top_k);
    Ok(hits)
}

#[cfg(all(test, feature = "embeddings"))]
mod onnx_preflight_tests {
    use super::*;

    /// The pre-flight check must FAIL rather than hang when the runtime is
    /// absent. Before it existed, this path deadlocked inside `ort` and took the
    /// whole test suite with it.
    #[test]
    fn a_missing_runtime_library_is_an_error_not_a_hang() {
        // Only meaningful where the library genuinely is not installed; where it
        // is, the check should pass instead.
        let result = ensure_onnx_runtime_available();
        match result {
            Ok(()) => {
                // A runtime is present. Then the check must not be spuriously
                // failing for anyone who has one installed.
            }
            Err(error) => {
                let message = error.to_string();
                assert!(
                    message.contains("ONNX Runtime shared library not found"),
                    "the error must name the actual problem: {message}"
                );
                assert!(
                    message.contains(ORT_DYLIB_ENV),
                    "the error must say how to fix it: {message}"
                );
            }
        }
    }

    #[test]
    fn a_bad_dylib_path_names_the_path_it_rejected() {
        let previous = std::env::var(ORT_DYLIB_ENV).ok();
        // SAFETY-adjacent: this test is single-threaded with respect to the env
        // var it sets, and restores it before returning.
        std::env::set_var(ORT_DYLIB_ENV, "/nonexistent/libonnxruntime.so");
        let error = ensure_onnx_runtime_available().unwrap_err().to_string();
        match previous {
            Some(value) => std::env::set_var(ORT_DYLIB_ENV, value),
            None => std::env::remove_var(ORT_DYLIB_ENV),
        }
        assert!(
            error.contains("/nonexistent/libonnxruntime.so"),
            "an operator needs to see which path was rejected: {error}"
        );
    }
}
