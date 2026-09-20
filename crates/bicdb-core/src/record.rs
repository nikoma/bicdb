use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::error::Result;
use crate::geometry::Geometry;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Record {
    pub id: String,
    pub vector: Option<Vec<f32>>,
    pub metadata: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub geometry: Option<Geometry>,
    pub timestamp: Option<i64>,
    pub payload: Option<Vec<u8>>,
}

impl AsRef<Record> for Record {
    fn as_ref(&self) -> &Record {
        self
    }
}

impl Record {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            vector: None,
            metadata: Value::Object(Map::new()),
            geometry: None,
            timestamp: None,
            payload: None,
        }
    }

    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_vector(mut self, vector: Vec<f32>) -> Self {
        self.vector = Some(vector);
        self
    }

    pub fn with_geometry(mut self, geometry: Geometry) -> Self {
        self.geometry = Some(geometry);
        self
    }

    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn with_payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = Some(payload);
        self
    }

    pub fn content_hash(&self) -> Result<String> {
        let bytes = serde_json::to_vec(self)?;
        let digest = Sha256::digest(bytes);
        Ok(hex::encode(digest))
    }
}

/// The compact, long-lived in-memory form of a [`Record`].
///
/// The engine keeps every live record (and every version-chain entry) resident in
/// RAM. A `serde_json::Value` metadata tree costs ~10x its raw JSON size (per-field
/// `String` keys, boxed enum nodes, map overhead), which dominates resident memory
/// for large datasets. `StoredRecord` keeps `metadata` as the raw JSON text
/// (`Box<RawValue>`) instead, so storage costs ~the serialized size, and the heavy
/// `Value` tree is materialized only at API boundaries (reads, index key extraction,
/// filters) via [`StoredRecord::to_record`] / [`StoredRecord::metadata_value`].
///
/// Its serde representation is byte-identical to [`Record`] (same field names/order;
/// `RawValue` emits its metadata verbatim), so segments and the WAL are unchanged on
/// disk and it can be (de)serialized interchangeably with `Record`.
#[derive(Debug, Deserialize)]
pub struct StoredRecord {
    pub id: String,
    pub vector: Option<Vec<f32>>,
    pub metadata: Box<RawValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub geometry: Option<Geometry>,
    pub timestamp: Option<i64>,
    pub payload: Option<Vec<u8>>,
    /// Lazily-parsed flat cell view of `metadata` (see [`TypedRow`]), built at
    /// most once per resident record and shared by every reader. In-memory
    /// only (serde skips it); a record update replaces the whole
    /// `StoredRecord`, which naturally invalidates the cache.
    #[serde(skip)]
    pub(crate) typed: std::sync::OnceLock<Option<Arc<TypedRow>>>,
    /// `Some` when this record's heavy bytes live in the page store rather than
    /// in memory — see [`EvictedPayload`]. In-memory only; never serialized.
    #[serde(skip)]
    pub(crate) evicted: Option<Box<EvictedPayload>>,
}

/// Where an evicted record's bytes actually are.
///
/// In `server_paged` mode the resident copy of a row keeps only its cheap
/// identity fields (`id`, `timestamp`, `vector`, `geometry`); `metadata` and
/// `payload` — the kilobytes — are represented by this reference and fetched
/// from the page store on demand through [`StoredRecord::to_record`] /
/// [`StoredRecord::metadata_value`] / [`StoredRecord::typed_row`], the choke
/// points every consumer already goes through.
///
/// # Why a pinned snapshot rather than "latest"
///
/// A superseded version's stub must keep returning the value it had when it was
/// superseded, or a reader holding an old core snapshot would see a *newer*
/// value through an *older* version — a snapshot-isolation violation that no
/// test of the happy path would catch. `as_of` pins the paged read to the
/// moment this version was current, and versions above it are invisible to
/// that snapshot by construction.
///
/// The fetch trait object breaks what would otherwise be a cyclic type
/// dependency (`record` → `paged_collection` → `record`), and keeps this module
/// ignorant of the page engine.
pub(crate) struct EvictedPayload {
    /// Fetches the full record this stub stands for. Must resolve as of the
    /// pinned snapshot, not as of now.
    pub(crate) fetch: Arc<dyn Fn(&str) -> Result<Option<Record>> + Send + Sync>,
    /// The record id to fetch (same as `StoredRecord::id`; kept here so the
    /// fetch closure needs no access to the stub itself).
    pub(crate) pk: Arc<str>,
}

impl std::fmt::Debug for EvictedPayload {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EvictedPayload")
            .field("pk", &self.pk)
            .finish_non_exhaustive()
    }
}

/// Serialization must never write a stub's placeholder bytes anywhere.
///
/// Every serialize of a `StoredRecord` today lands somewhere that matters — a
/// segment frame, a replication delta, a stats measurement. If a stub slipped
/// through, its placeholder metadata would be recorded as if it were the row,
/// and the corruption would surface much later with no trail. Refusing loudly
/// here turns "a site we missed" from silent corruption into an immediate,
/// attributable error. Sites that legitimately need the bytes materialize via
/// [`StoredRecord::to_record`] first.
impl Serialize for StoredRecord {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        if self.evicted.is_some() {
            return Err(serde::ser::Error::custom(
                "attempted to serialize an evicted record stub; materialize it \
                 with to_record() first (this is a bug: some code path treats a \
                 paged-mode stub as if it held the row bytes)",
            ));
        }
        #[derive(Serialize)]
        struct Wire<'a> {
            id: &'a str,
            vector: &'a Option<Vec<f32>>,
            metadata: &'a RawValue,
            #[serde(skip_serializing_if = "Option::is_none")]
            geometry: &'a Option<Geometry>,
            timestamp: &'a Option<i64>,
            payload: &'a Option<Vec<u8>>,
        }
        Wire {
            id: &self.id,
            vector: &self.vector,
            metadata: &self.metadata,
            geometry: &self.geometry,
            timestamp: &self.timestamp,
            payload: &self.payload,
        }
        .serialize(serializer)
    }
}

impl StoredRecord {
    /// A resident row from its id and metadata JSON text (a JSON object in
    /// the canonical `Value` serialization) with no vector, geometry,
    /// timestamp or payload: the row an INSERT template writes without ever
    /// building the `Value` tree.
    pub fn from_parts(id: String, metadata_text: String) -> Result<StoredRecord> {
        Ok(StoredRecord {
            id,
            vector: None,
            metadata: RawValue::from_string(metadata_text)?,
            geometry: None,
            timestamp: None,
            payload: None,
            typed: std::sync::OnceLock::new(),
            evicted: None,
        })
    }

    /// This row with its metadata replaced by `text` (a JSON object, in the
    /// canonical `Value` serialization); identity fields are shared with
    /// `self`. The resident row an UPDATE splices together without ever
    /// parsing the old row into a `Value` tree.
    pub fn with_metadata_text(&self, text: String) -> Result<StoredRecord> {
        self.with_metadata_raw(text, RawValue::from_string)
    }

    /// `with_metadata_text` for text that is well-formed JSON by
    /// construction — the output of [`splice_json_object_text`], whose
    /// surviving bytes were validated when the old row was built and whose
    /// spliced values are serde-rendered. Skips the full re-parse
    /// `RawValue::from_string` performs only to validate.
    pub fn with_metadata_text_spliced(&self, text: String) -> Result<StoredRecord> {
        self.with_metadata_raw(text, |text| {
            debug_assert!(
                serde_json::from_str::<&RawValue>(&text).is_ok(),
                "spliced metadata must be well-formed JSON"
            );
            // SAFETY: `RawValue` is `#[repr(transparent)]` over `str` (this is
            // how serde_json itself builds one from validated text), so a
            // `Box<str>` has the same layout as `Box<RawValue>`.
            Ok(unsafe { std::mem::transmute::<Box<str>, Box<RawValue>>(text.into_boxed_str()) })
        })
    }

    fn with_metadata_raw(
        &self,
        text: String,
        raw: impl FnOnce(String) -> serde_json::Result<Box<RawValue>>,
    ) -> Result<StoredRecord> {
        if self.evicted.is_some() {
            return Err(crate::error::BicDbError::Corruption {
                path: std::path::PathBuf::from("paged"),
                message: format!(
                    "record `{}` is an evicted stub; materialize it before patching",
                    self.id
                ),
            });
        }
        Ok(StoredRecord {
            id: self.id.clone(),
            vector: self.vector.clone(),
            metadata: raw(text)?,
            geometry: self.geometry.clone(),
            timestamp: self.timestamp,
            payload: self.payload.clone(),
            typed: std::sync::OnceLock::new(),
            evicted: None,
        })
    }
}

/// Splice `patches` into the JSON object `text`: `Some(value_text)` replaces
/// (or adds) the member, `None` removes it. Every other member keeps its
/// exact text, and the result is the canonical `Value` serialization (members
/// in ascending key order), so it equals what parsing `text` into a `Value`,
/// applying the patches and serializing would produce. `None` when `text` is
/// not a JSON object.
pub fn patch_json_object_text(text: &str, patches: &[(&str, Option<&str>)]) -> Option<String> {
    if let Some(patched) = splice_json_object_text(text, patches) {
        return Some(patched);
    }
    let members: std::collections::BTreeMap<String, &RawValue> = serde_json::from_str(text).ok()?;
    let mut out = String::with_capacity(text.len() + 64);
    out.push('{');
    let mut first = true;
    let mut push = |out: &mut String, key: &str, value: &str| {
        if !first {
            out.push(',');
        }
        first = false;
        let mut bytes = std::mem::take(out).into_bytes();
        serde_json::to_writer(&mut bytes, key).expect("a string serializes");
        *out = String::from_utf8(bytes).expect("JSON is UTF-8");
        out.push(':');
        out.push_str(value);
    };
    let mut patch = patches.iter().peekable();
    for (key, raw) in &members {
        while let Some((patch_key, patch_value)) = patch.peek() {
            match (*patch_key).cmp(key.as_str()) {
                std::cmp::Ordering::Less => {
                    if let Some(value) = patch_value {
                        push(&mut out, patch_key, value);
                    }
                    patch.next();
                }
                std::cmp::Ordering::Equal => break,
                std::cmp::Ordering::Greater => break,
            }
        }
        match patch.peek() {
            Some((patch_key, patch_value)) if *patch_key == key.as_str() => {
                if let Some(value) = patch_value {
                    push(&mut out, key, value);
                }
                patch.next();
            }
            _ => push(&mut out, key, raw.get()),
        }
    }
    for (patch_key, patch_value) in patch {
        if let Some(value) = patch_value {
            push(&mut out, patch_key, value);
        }
    }
    out.push('}');
    Some(out)
}

/// `patch_json_object_text` for the compact serde-written shape every
/// resident row has: one byte scan records where each member's key and
/// value sit, and the output is spliced from those spans and the patches —
/// no map, no per-key `String`, no re-escaping. `None` (take the parsing
/// path) on whitespace, escaped keys, members out of key order or anything
/// the scan does not fully recognise.
pub fn splice_json_object_text(text: &str, patches: &[(&str, Option<&str>)]) -> Option<String> {
    let mut members: Vec<(std::ops::Range<usize>, std::ops::Range<usize>)> = Vec::new();
    if !scan_compact_member_spans(text, &mut members) {
        return None;
    }
    // Members must already be in ascending key order (the canonical
    // serialization) for the merge below to keep the output canonical.
    if members
        .windows(2)
        .any(|pair| text[pair[0].0.clone()] >= text[pair[1].0.clone()])
    {
        return None;
    }
    fn push_patch(out: &mut String, first: &mut bool, key: &str, value: &str) {
        if !*first {
            out.push(',');
        }
        *first = false;
        let mut bytes = std::mem::take(out).into_bytes();
        serde_json::to_writer(&mut bytes, key).expect("a string serializes");
        *out = String::from_utf8(bytes).expect("JSON is UTF-8");
        out.push(':');
        out.push_str(value);
    }
    let mut out = String::with_capacity(text.len() + 64);
    out.push('{');
    let mut first = true;
    let mut patch = patches.iter().peekable();
    for (key_span, value_span) in &members {
        let key = &text[key_span.clone()];
        while let Some((patch_key, patch_value)) = patch.peek() {
            match (*patch_key).cmp(key) {
                std::cmp::Ordering::Less => {
                    if let Some(value) = patch_value {
                        push_patch(&mut out, &mut first, patch_key, value);
                    }
                    patch.next();
                }
                _ => break,
            }
        }
        match patch.peek() {
            Some((patch_key, patch_value)) if *patch_key == key => {
                if let Some(value) = patch_value {
                    push_patch(&mut out, &mut first, key, value);
                }
                patch.next();
            }
            _ => {
                // Verbatim: the quoted key, the colon and the value bytes.
                if !first {
                    out.push(',');
                }
                first = false;
                out.push_str(&text[key_span.start - 1..value_span.end]);
            }
        }
    }
    for (patch_key, patch_value) in patch {
        if let Some(value) = patch_value {
            push_patch(&mut out, &mut first, patch_key, value);
        }
    }
    out.push('}');
    Some(out)
}

/// Copy only `keys` from a compact JSON object, retaining each selected
/// value's original JSON bytes. The result is another canonical JSON object.
///
/// This is primarily useful for compact update logging: an UPDATE already has
/// the exact set of top-level columns it changed, so its WAL record can carry
/// the old/new values of those columns instead of copying the complete row.
/// `None` is returned for non-compact input, duplicate requested keys, or a key
/// that is absent from the object; callers can safely fall back to a full row.
pub(crate) fn project_json_object_text(text: &str, keys: &[impl AsRef<str>]) -> Option<String> {
    if keys.iter().enumerate().any(|(idx, key)| {
        keys[..idx]
            .iter()
            .any(|other| other.as_ref() == key.as_ref())
    }) {
        return None;
    }
    let mut members = Vec::new();
    if !scan_compact_member_spans(text, &mut members) {
        return None;
    }
    let mut out = String::with_capacity(keys.len().saturating_mul(64).saturating_add(2));
    out.push('{');
    let mut found = 0usize;
    for (key_span, value_span) in members {
        let key = &text[key_span.clone()];
        if !keys.iter().any(|wanted| wanted.as_ref() == key) {
            continue;
        }
        if found != 0 {
            out.push(',');
        }
        // Include the opening quote, key, closing quote, colon, and raw value.
        out.push_str(&text[key_span.start - 1..value_span.end]);
        found += 1;
    }
    out.push('}');
    (found == keys.len()).then_some(out)
}

/// Locate the closing quote, recording whether JSON unescaping is needed.
#[inline]
fn json_string_end(bytes: &[u8], mut position: usize) -> Option<(usize, bool)> {
    // Skip eight ordinary string bytes at a time without architecture-specific
    // intrinsics or unaligned unsafe loads. The zero-byte test can conservatively
    // flag a neighbouring byte after a borrow; we only use it to enter the exact
    // scalar path, never to choose a string boundary.
    const LOW: u64 = 0x0101_0101_0101_0101;
    const HIGH: u64 = 0x8080_8080_8080_8080;
    let has_zero = |word: u64| word.wrapping_sub(LOW) & !word & HIGH;
    let mut escaped = false;
    loop {
        while bytes.len().saturating_sub(position) >= 8 {
            let word = u64::from_ne_bytes(bytes[position..position + 8].try_into().ok()?);
            let special = has_zero(word ^ 0x2222_2222_2222_2222)
                | has_zero(word ^ 0x5c5c_5c5c_5c5c_5c5c)
                | (word.wrapping_sub(0x2020_2020_2020_2020) & !word & HIGH);
            if special != 0 {
                break;
            }
            position += 8;
        }
        loop {
            match bytes.get(position) {
                Some(b'"') => return Some((position, escaped)),
                Some(b'\\') => {
                    escaped = true;
                    position += 2;
                    break;
                }
                Some(byte) if *byte < 0x20 => return None,
                Some(_) => position += 1,
                None => return None,
            }
        }
    }
}

/// The key span (inside the quotes) and value span (whole value text) of
/// each member of a compact JSON object, in text order; the structural twin
/// of `scan_compact_cells` that classifies nothing. `false` on anything the
/// compact scan does not fully recognise.
fn scan_compact_member_spans(
    text: &str,
    out: &mut Vec<(std::ops::Range<usize>, std::ops::Range<usize>)>,
) -> bool {
    let bytes = text.as_bytes();
    if bytes.first() != Some(&b'{') {
        return false;
    }
    let mut i = 1;
    if bytes.get(i) == Some(&b'}') {
        return i + 1 == bytes.len();
    }
    loop {
        if bytes.get(i) != Some(&b'"') {
            return false;
        }
        i += 1;
        let key_start = i;
        let Some((end, false)) = json_string_end(bytes, i) else {
            return false;
        };
        i = end;
        let key_end = i;
        i += 1;
        if bytes.get(i) != Some(&b':') {
            return false;
        }
        i += 1;
        let value_start = i;
        match bytes.get(i) {
            Some(b'"') => {
                i += 1;
                let Some((end, _)) = json_string_end(bytes, i) else {
                    return false;
                };
                i = end + 1;
            }
            Some(b't') if bytes[i..].starts_with(b"true") => i += 4,
            Some(b'f') if bytes[i..].starts_with(b"false") => i += 5,
            Some(b'n') if bytes[i..].starts_with(b"null") => i += 4,
            Some(b'{') | Some(b'[') => {
                let mut depth = 0usize;
                loop {
                    match bytes.get(i) {
                        None => return false,
                        Some(b'"') => {
                            i += 1;
                            let Some((end, _)) = json_string_end(bytes, i) else {
                                return false;
                            };
                            i = end + 1;
                        }
                        Some(b'{') | Some(b'[') => {
                            depth += 1;
                            i += 1;
                        }
                        Some(b'}') | Some(b']') => {
                            depth -= 1;
                            i += 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        Some(_) => i += 1,
                    }
                }
            }
            Some(b'-') | Some(b'0'..=b'9') => {
                while matches!(
                    bytes.get(i),
                    Some(b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
                ) {
                    i += 1;
                }
            }
            _ => return false,
        }
        if i > bytes.len() {
            return false;
        }
        out.push((key_start..key_end, value_start..i));
        match bytes.get(i) {
            Some(b',') => i += 1,
            Some(b'}') => {
                i += 1;
                break;
            }
            _ => return false,
        }
    }
    i == bytes.len()
}

impl crate::residency::ResidentBytes for StoredRecord {
    fn heap_bytes(&self) -> u64 {
        use crate::residency::{string_bytes, vec_bytes};

        let mut bytes = string_bytes(&self.id)
            // `metadata` is raw JSON text, which is the bulk of a typical row.
            + self.metadata.get().len() as u64
            + self
                .vector
                .as_ref()
                .map_or(0, |v| vec_bytes::<f32>(v.capacity()))
            + self
                .payload
                .as_ref()
                .map_or(0, |p| vec_bytes::<u8>(p.capacity()))
            + self.geometry.as_ref().map_or(0, |g| {
                std::mem::size_of::<Geometry>() as u64 + g.heap_bytes()
            });

        // The lazily-built typed cell view is resident once populated, and is a
        // real per-record cost even though it never reaches disk.
        if let Some(Some(typed)) = self.typed.get() {
            bytes += std::mem::size_of::<(Box<str>, TypedCell)>() as u64 * typed.len() as u64;
            for (name, cell) in typed.iter() {
                bytes += name.len() as u64;
                bytes += match cell {
                    TypedCell::Number(text) | TypedCell::Str(text) | TypedCell::Raw(text) => {
                        text.len() as u64
                    }
                    TypedCell::Null
                    | TypedCell::Bool(_)
                    | TypedCell::Int(_)
                    | TypedCell::Float(_) => 0,
                };
            }
        }

        bytes
    }
}

impl Clone for StoredRecord {
    fn clone(&self) -> Self {
        let typed = std::sync::OnceLock::new();
        if let Some(cached) = self.typed.get() {
            let _ = typed.set(cached.clone());
        }
        Self {
            id: self.id.clone(),
            vector: self.vector.clone(),
            metadata: self.metadata.clone(),
            geometry: self.geometry.clone(),
            timestamp: self.timestamp,
            payload: self.payload.clone(),
            typed,
            evicted: self.evicted.clone(),
        }
    }
}

impl Clone for EvictedPayload {
    fn clone(&self) -> Self {
        Self {
            fetch: Arc::clone(&self.fetch),
            pk: Arc::clone(&self.pk),
        }
    }
}

/// [`CellRef`] without the borrow: what an index key needs from a cell (the
/// envelope's ordered key and raw text; nested values are `Raw`, which
/// only a parse reproduces).
#[derive(Clone, Debug)]
pub enum OwnedCell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Envelope {
        index_key: Option<String>,
        raw: String,
    },
    Raw,
}

pub type OwnedCellRow = Vec<(String, OwnedCell)>;

/// One top-level cell of a stored row, borrowed from the row's raw JSON text
/// by [`StoredRecord::cells_into`]. Unlike [`TypedCell`] nothing here is
/// cached or owned: the pass is meant to run once per read of a row and cost
/// about one scan of the text, no `serde_json::Value` tree.
#[derive(Clone, Debug)]
pub enum CellRef<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    /// A JSON string; borrowed when it has no escapes.
    Str(std::borrow::Cow<'a, str>),
    /// A text-only engine typed-storage envelope
    /// (`{"$bicdb_typed": {"version": 1, "pg_type": .., "text": .., "index_key": ..}}`),
    /// the storage form of NUMERIC and temporal columns. `index_key` is the
    /// lowercase-hex ordered key the SQL layer persisted for indexing (absent
    /// on some older rows); `raw` is the whole envelope's JSON text. Envelopes
    /// carrying a structured `value` or a user type are left as `Raw`.
    Envelope {
        pg_type: &'a str,
        text: &'a str,
        index_key: Option<&'a str>,
        raw: &'a str,
    },
    /// Anything else (nested object/array, unusual envelope) as raw JSON text.
    Raw(&'a str),
}

/// The cells of one stored row as borrowed by [`StoredRecord::cells_into`].
pub type CellRow<'a> = Vec<(std::borrow::Cow<'a, str>, CellRef<'a>)>;

/// The serde-derive reading of a typed-storage envelope: the authority that
/// [`scan_text_envelope`] must agree with wherever it answers.
fn classify_envelope_via_serde(text: &str) -> CellRef<'_> {
    #[derive(Deserialize)]
    struct EnvelopeLite<'a> {
        version: u32,
        #[serde(borrow)]
        pg_type: &'a str,
        #[serde(borrow, default)]
        text: Option<&'a str>,
        #[serde(borrow, default)]
        index_key: Option<&'a str>,
        #[serde(default)]
        value: Option<serde::de::IgnoredAny>,
        #[serde(default)]
        type_oid: Option<serde::de::IgnoredAny>,
    }
    #[derive(Deserialize)]
    struct EnvelopeOuter<'a> {
        #[serde(rename = "$bicdb_typed", borrow)]
        typed: EnvelopeLite<'a>,
    }
    match serde_json::from_str::<EnvelopeOuter<'_>>(text) {
        Ok(EnvelopeOuter {
            typed:
                EnvelopeLite {
                    version: 1,
                    pg_type,
                    text: Some(value),
                    index_key,
                    value: None,
                    type_oid: None,
                },
        }) => CellRef::Envelope {
            pg_type,
            text: value,
            index_key,
            raw: text,
        },
        _ => CellRef::Raw(text),
    }
}

fn classify_cell_ref(text: &str) -> CellRef<'_> {
    match text.as_bytes().first() {
        Some(b'"') => match serde_json::from_str::<&str>(text) {
            Ok(value) => CellRef::Str(std::borrow::Cow::Borrowed(value)),
            Err(_) => match serde_json::from_str::<String>(text) {
                Ok(value) => CellRef::Str(std::borrow::Cow::Owned(value)),
                Err(_) => CellRef::Raw(text),
            },
        },
        Some(b't') => CellRef::Bool(true),
        Some(b'f') => CellRef::Bool(false),
        Some(b'n') => CellRef::Null,
        Some(b'{') if text.starts_with("{\"$bicdb_typed\"") => {
            scan_text_envelope(text).unwrap_or_else(|| classify_envelope_via_serde(text))
        }
        Some(b'{') | Some(b'[') => CellRef::Raw(text),
        _ => {
            if !text.contains(['.', 'e', 'E']) {
                if let Ok(value) = text.parse::<i64>() {
                    return CellRef::Int(value);
                }
            }
            match text.parse::<f64>() {
                Ok(value) => CellRef::Float(value),
                Err(_) => CellRef::Raw(text),
            }
        }
    }
}

/// Fast classification of a compact text-only typed-storage envelope
/// (`{"$bicdb_typed":{...}}` with scalar members in any order). `None` when
/// the envelope has anything the scan does not recognise — escaped strings,
/// unknown members, whitespace — and the serde derive decides instead.
fn scan_text_envelope(text: &str) -> Option<CellRef<'_>> {
    if let Some((cell, consumed)) = scan_standard_text_envelope_prefix(text) {
        if consumed == text.len() {
            return Some(cell);
        }
    }
    const OUTER: &str = "{\"$bicdb_typed\":";
    let inner = text.strip_prefix(OUTER)?.strip_suffix('}')?;
    let mut version = None;
    let mut pg_type = None;
    let mut value_text = None;
    let mut index_key = None;
    let mut structured = false;
    // Do not materialize the four-member inner object. This function runs for
    // every NUMERIC and temporal cell in every borrowed row scan, so the old
    // temporary `Vec` was several heap allocations per TPC-C row. The compact
    // scanner already visits the members in one pass; collect only the four
    // references the envelope decoder needs.
    if !scan_compact_cells_with(inner, |key, cell| {
        match (key, cell) {
            ("version", CellRef::Int(v)) => version = Some(v),
            ("pg_type", CellRef::Str(std::borrow::Cow::Borrowed(v))) => pg_type = Some(v),
            ("text", CellRef::Str(std::borrow::Cow::Borrowed(v))) => value_text = Some(v),
            ("index_key", CellRef::Str(std::borrow::Cow::Borrowed(v))) => index_key = Some(v),
            ("value", _) | ("type_oid", _) => structured = true,
            _ => return false,
        }
        true
    }) {
        return None;
    }
    let pg_type = pg_type?;
    if version != Some(1) || structured {
        return Some(CellRef::Raw(text));
    }
    let value_text = value_text?;
    Some(CellRef::Envelope {
        pg_type,
        text: value_text,
        index_key,
        raw: text,
    })
}

/// The common serde-written envelope has a fixed member order. Recognize it
/// while locating its end, avoiding the outer row scanner's full brace/string
/// walk followed by another complete envelope scan. Other layouts retain the
/// generic scanner/serde path; no persistent row cache or format change.
fn scan_standard_text_envelope_prefix(text: &str) -> Option<(CellRef<'_>, usize)> {
    fn string_end(text: &str) -> Option<(&str, &str)> {
        let end = text.find('"')?;
        let value = &text[..end];
        if value.bytes().any(|byte| byte == b'\\' || byte < 0x20) {
            return None;
        }
        Some((value, &text[end + 1..]))
    }
    let rest = text.strip_prefix("{\"$bicdb_typed\":{\"version\":1,\"pg_type\":\"")?;
    let (pg_type, rest) = string_end(rest)?;
    let rest = rest.strip_prefix(",\"text\":\"")?;
    let (value_text, rest) = string_end(rest)?;
    let (index_key, rest) = if let Some(rest) = rest.strip_prefix(",\"index_key\":\"") {
        let (index_key, rest) = string_end(rest)?;
        (Some(index_key), rest)
    } else {
        (None, rest)
    };
    let rest = rest.strip_prefix("}}")?;
    let consumed = text.len() - rest.len();
    Some((
        CellRef::Envelope {
            pg_type,
            text: value_text,
            index_key,
            raw: &text[..consumed],
        },
        consumed,
    ))
}

/// Single-pass cell scan of a compact (serde-written, no whitespace) JSON
/// object: each key is borrowed, each escape-free string cell is borrowed
/// directly, and only envelopes, escaped strings and numbers go through
/// [`classify_cell_ref`]. Returns `false` on anything it does not fully
/// recognise (whitespace, escaped keys, malformed text); the caller then
/// takes the serde path, which is the authority on what the text means.
fn scan_compact_cells<'a>(text: &'a str, out: &mut CellRow<'a>) -> bool {
    scan_compact_cells_with(text, |key, cell| {
        out.push((std::borrow::Cow::Borrowed(key), cell));
        true
    })
}

/// Callback form of [`scan_compact_cells`]. Keeping the visitor on the stack
/// lets nested typed-envelope classification inspect its handful of members
/// without allocating a temporary cell row.
fn scan_compact_cells_with<'a>(
    text: &'a str,
    mut visit: impl FnMut(&'a str, CellRef<'a>) -> bool,
) -> bool {
    let bytes = text.as_bytes();
    if bytes.first() != Some(&b'{') {
        return false;
    }
    let mut i = 1;
    if bytes.get(i) == Some(&b'}') {
        return i + 1 == bytes.len();
    }
    loop {
        if bytes.get(i) != Some(&b'"') {
            return false;
        }
        i += 1;
        let key_start = i;
        let Some((end, false)) = json_string_end(bytes, i) else {
            return false;
        };
        i = end;
        let key = &text[key_start..i];
        i += 1;
        if bytes.get(i) != Some(&b':') {
            return false;
        }
        i += 1;
        let value_start = i;
        let cell = match bytes.get(i) {
            Some(b'"') => {
                i += 1;
                let str_start = i;
                let Some((str_end, escaped)) = json_string_end(bytes, i) else {
                    return false;
                };
                i = str_end + 1;
                if escaped {
                    classify_cell_ref(&text[value_start..i])
                } else {
                    CellRef::Str(std::borrow::Cow::Borrowed(&text[str_start..str_end]))
                }
            }
            Some(b't') if bytes[i..].starts_with(b"true") => {
                i += 4;
                CellRef::Bool(true)
            }
            Some(b'f') if bytes[i..].starts_with(b"false") => {
                i += 5;
                CellRef::Bool(false)
            }
            Some(b'n') if bytes[i..].starts_with(b"null") => {
                i += 4;
                CellRef::Null
            }
            Some(b'{') | Some(b'[') => {
                if let Some((cell, consumed)) = scan_standard_text_envelope_prefix(&text[i..]) {
                    i += consumed;
                    cell
                } else {
                    let mut depth = 0usize;
                    loop {
                        match bytes.get(i) {
                            None => return false,
                            Some(b'"') => {
                                i += 1;
                                let Some((end, _)) = json_string_end(bytes, i) else {
                                    return false;
                                };
                                i = end + 1;
                            }
                            Some(b'{') | Some(b'[') => {
                                depth += 1;
                                i += 1;
                            }
                            Some(b'}') | Some(b']') => {
                                depth -= 1;
                                i += 1;
                                if depth == 0 {
                                    break;
                                }
                            }
                            Some(_) => i += 1,
                        }
                    }
                    classify_cell_ref(&text[value_start..i])
                }
            }
            Some(b'-') | Some(b'0'..=b'9') => {
                while matches!(
                    bytes.get(i),
                    Some(b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
                ) {
                    i += 1;
                }
                classify_cell_ref(&text[value_start..i])
            }
            _ => return false,
        };
        if !visit(key, cell) {
            return false;
        }
        match bytes.get(i) {
            Some(b',') => i += 1,
            Some(b'}') => {
                i += 1;
                break;
            }
            _ => return false,
        }
    }
    i == bytes.len()
}

/// One scalar cell of a [`TypedRow`]. Nested objects/arrays stay as raw JSON
/// text (`Raw`) and are parsed only if actually projected.
#[derive(Clone, Debug)]
pub enum TypedCell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Number(Box<str>),
    Str(Box<str>),
    Raw(Box<str>),
}

/// Flat (key, cell) view of a record's top-level metadata object, in JSON
/// order. Costs about the raw JSON size — roughly a tenth of the equivalent
/// `serde_json::Value` tree — which is what makes caching it per resident
/// record affordable where caching `Value` was not.
pub type TypedRow = Box<[(Box<str>, TypedCell)]>;

fn classify_raw_cell(raw: &RawValue) -> TypedCell {
    let text = raw.get();
    match text.as_bytes().first() {
        Some(b'"') => match serde_json::from_str::<String>(text) {
            Ok(value) => TypedCell::Str(value.into_boxed_str()),
            Err(_) => TypedCell::Raw(text.into()),
        },
        Some(b't') => TypedCell::Bool(true),
        Some(b'f') => TypedCell::Bool(false),
        Some(b'n') => TypedCell::Null,
        Some(b'{') | Some(b'[') => TypedCell::Raw(text.into()),
        _ => {
            if !text.contains(['.', 'e', 'E']) {
                if let Ok(value) = text.parse::<i64>() {
                    return TypedCell::Int(value);
                }
            }
            TypedCell::Number(text.into())
        }
    }
}

fn parse_typed_row(metadata: &RawValue) -> Option<Arc<TypedRow>> {
    #[derive(Deserialize)]
    #[serde(transparent)]
    struct RawMap(std::collections::BTreeMap<(), ()>);
    // Deserialize the top-level object as (String, Box<RawValue>) pairs; any
    // non-object metadata opts the record out of the typed view.
    struct TopLevel(Vec<(Box<str>, TypedCell)>);
    impl<'de> serde::Deserialize<'de> for TopLevel {
        fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct V;
            impl<'de> serde::de::Visitor<'de> for V {
                type Value = TopLevel;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("a JSON object")
                }
                fn visit_map<A>(self, mut map: A) -> std::result::Result<TopLevel, A::Error>
                where
                    A: serde::de::MapAccess<'de>,
                {
                    let mut cells = Vec::with_capacity(map.size_hint().unwrap_or(8));
                    while let Some(key) = map.next_key::<String>()? {
                        let raw: Box<RawValue> = map.next_value()?;
                        cells.push((key.into_boxed_str(), classify_raw_cell(&raw)));
                    }
                    Ok(TopLevel(cells))
                }
            }
            deserializer.deserialize_map(V)
        }
    }
    let _ = RawMap; // silence unused helper on some toolchains
    serde_json::from_str::<TopLevel>(metadata.get())
        .ok()
        .map(|top| Arc::new(top.0.into_boxed_slice()))
}

impl PartialEq for StoredRecord {
    fn eq(&self, other: &Self) -> bool {
        // `RawValue` has no `PartialEq`; compare its raw JSON text. Metadata is
        // always built from `Value`'s canonical serialization, so equal text means
        // equal `Value` and vice versa.
        self.id == other.id
            && self.vector == other.vector
            && self.metadata.get() == other.metadata.get()
            && self.geometry == other.geometry
            && self.timestamp == other.timestamp
            && self.payload == other.payload
    }
}

impl StoredRecord {
    /// Compact an owned [`Record`], serializing its metadata `Value` to raw JSON
    /// once. The raw text is exactly `Value`'s own serialization, so a later
    /// `to_record()` reparses to an equal `Value` and `content_hash` is unchanged.
    pub fn from_record(record: &Record) -> Result<Self> {
        Ok(Self {
            id: record.id.clone(),
            vector: record.vector.clone(),
            metadata: serde_json::value::to_raw_value(&record.metadata)?,
            geometry: record.geometry.clone(),
            timestamp: record.timestamp,
            payload: record.payload.clone(),
            typed: std::sync::OnceLock::new(),
            evicted: None,
        })
    }

    /// A resident stub for a record whose bytes live in the page store.
    ///
    /// Keeps the fields that are cheap and that iteration sites actually use
    /// (`id`, `timestamp`, `vector`, `geometry`); replaces `metadata` and
    /// `payload` with a fetch-on-demand reference. The placeholder metadata is
    /// unreadable through any legitimate path: [`Self::metadata_value`],
    /// [`Self::to_record`], and [`Self::typed_row`] all detect the stub and
    /// fetch, and [`Serialize`] refuses outright.
    pub(crate) fn evicted_stub(record: &Record, payload: EvictedPayload) -> Self {
        Self {
            id: record.id.clone(),
            vector: record.vector.clone(),
            metadata: RawValue::from_string("null".to_string())
                .expect("the literal null is valid JSON"),
            geometry: record.geometry.clone(),
            timestamp: record.timestamp,
            payload: None,
            typed: std::sync::OnceLock::new(),
            evicted: Some(Box::new(payload)),
        }
    }

    /// Fetch the full record this stub stands for, as of its pinned snapshot.
    fn fetch_evicted(&self, evicted: &EvictedPayload) -> Result<Record> {
        match (evicted.fetch)(&evicted.pk)? {
            Some(record) => Ok(record),
            // The version chain says this version exists; the page store must
            // agree. Absence means the pinned snapshot no longer resolves —
            // which is corruption or a vacuum that ran too eagerly, and either
            // way silent None would be read as "row deleted".
            None => Err(crate::error::BicDbError::Corruption {
                path: std::path::PathBuf::from("paged"),
                message: format!(
                    "record `{}` has a resident stub but its bytes are missing \
                     from the page store at the pinned snapshot",
                    evicted.pk
                ),
            }),
        }
    }

    /// The row's top-level cells, borrowed from the raw metadata text in one
    /// streaming pass into `out` (cleared first). Returns `false` — with `out`
    /// left empty — when the row is an eviction stub or its metadata is not a
    /// JSON object, in which case callers take the `Record` path.
    ///
    /// This is the read primitive for schema-typed rows: no `Value` tree, no
    /// per-key `String`, no envelope re-parse, nothing cached on the record.
    pub fn cells_into<'a>(&'a self, out: &mut CellRow<'a>) -> bool {
        out.clear();
        if self.evicted.is_some() {
            return false;
        }
        if scan_compact_cells(self.metadata.get(), out) {
            return true;
        }
        out.clear();
        struct Sink<'a, 'b>(&'b mut CellRow<'a>);
        impl<'de, 'b> serde::de::DeserializeSeed<'de> for Sink<'de, 'b> {
            type Value = ();
            fn deserialize<D>(self, deserializer: D) -> std::result::Result<(), D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                deserializer.deserialize_map(self)
            }
        }
        impl<'de, 'b> serde::de::Visitor<'de> for Sink<'de, 'b> {
            type Value = ();
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A>(self, mut map: A) -> std::result::Result<(), A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                if let Some(hint) = map.size_hint() {
                    self.0.reserve(hint);
                }
                while let Some(key) = map.next_key::<std::borrow::Cow<'de, str>>()? {
                    let raw: &'de RawValue = map.next_value()?;
                    self.0.push((key, classify_cell_ref(raw.get())));
                }
                Ok(())
            }
        }
        let mut deserializer = serde_json::Deserializer::from_str(self.metadata.get());
        match serde::de::DeserializeSeed::deserialize(Sink(out), &mut deserializer) {
            Ok(()) => true,
            Err(_) => {
                out.clear();
                false
            }
        }
    }

    /// The cached flat cell view of the metadata (parsed at most once; `None`
    /// when the metadata is not a JSON object).
    /// The row's top-level cells in an owned form (see [`OwnedCell`]): one
    /// parse whose result outlives the borrow of the JSON text, so a consumer
    /// that reads several cells at different times (index keys for every
    /// index of a collection at commit) parses once. `None` when the row is
    /// an eviction stub or its metadata is not an object.
    pub fn owned_cells(&self) -> Option<OwnedCellRow> {
        let mut cells: CellRow<'_> = Vec::with_capacity(32);
        if !self.cells_into(&mut cells) {
            return None;
        }
        Some(
            cells
                .into_iter()
                .map(|(key, cell)| {
                    let cell = match cell {
                        CellRef::Null => OwnedCell::Null,
                        CellRef::Bool(value) => OwnedCell::Bool(value),
                        CellRef::Int(value) => OwnedCell::Int(value),
                        CellRef::Float(value) => OwnedCell::Float(value),
                        CellRef::Str(text) => OwnedCell::Str(text.into_owned()),
                        CellRef::Envelope { index_key, raw, .. } => OwnedCell::Envelope {
                            index_key: index_key.map(str::to_string),
                            raw: raw.to_string(),
                        },
                        CellRef::Raw(_) => OwnedCell::Raw,
                    };
                    (key.into_owned(), cell)
                })
                .collect(),
        )
    }

    pub fn typed_row(&self) -> Option<Arc<TypedRow>> {
        if let Some(evicted) = self.evicted.as_deref() {
            // Fetched fresh each call, deliberately NOT cached in `typed`:
            // caching would re-materialize the very bytes eviction exists to
            // keep out of memory, one hot row at a time, with no budget.
            let record = match self.fetch_evicted(evicted) {
                Ok(record) => record,
                Err(_) => return None,
            };
            let raw = match serde_json::value::to_raw_value(&record.metadata) {
                Ok(raw) => raw,
                Err(_) => return None,
            };
            return parse_typed_row(&raw);
        }
        self.typed
            .get_or_init(|| parse_typed_row(&self.metadata))
            .clone()
    }

    /// Materialize the heavy [`Record`] form (parses the metadata JSON). Use only
    /// where a `Value` is actually needed (API reads, index/filter evaluation).
    pub fn to_record(&self) -> Result<Record> {
        if let Some(evicted) = self.evicted.as_deref() {
            return self.fetch_evicted(evicted);
        }
        Ok(Record {
            id: self.id.clone(),
            vector: self.vector.clone(),
            metadata: self.metadata_value()?,
            geometry: self.geometry.clone(),
            timestamp: self.timestamp,
            payload: self.payload.clone(),
        })
    }

    /// Parse just the metadata into a `Value`.
    pub fn metadata_value(&self) -> Result<Value> {
        if let Some(evicted) = self.evicted.as_deref() {
            return Ok(self.fetch_evicted(evicted)?.metadata);
        }
        Ok(serde_json::from_str(self.metadata.get())?)
    }

    /// The record's serialized (wire) size in bytes. For an evicted stub this
    /// fetches and measures the real bytes — the caller asked how big the row
    /// is, not how big the resident representation is.
    pub fn wire_bytes_len(&self) -> Result<u64> {
        if let Some(evicted) = self.evicted.as_deref() {
            let record = self.fetch_evicted(evicted)?;
            return Ok(serde_json::to_vec(&record)?.len() as u64);
        }
        Ok(serde_json::to_vec(self)?.len() as u64)
    }

    /// Content hash, identical to [`Record::content_hash`] for the same record:
    /// `StoredRecord` serializes byte-for-byte like `Record`, so the digest matches
    /// without materializing the heavy `Value`.
    pub fn content_hash(&self) -> Result<String> {
        if let Some(evicted) = self.evicted.as_deref() {
            return self.fetch_evicted(evicted)?.content_hash();
        }
        let bytes = serde_json::to_vec(self)?;
        let digest = Sha256::digest(bytes);
        Ok(hex::encode(digest))
    }
}

impl From<Record> for StoredRecord {
    fn from(record: Record) -> Self {
        Self::from_record(&record).expect("Value metadata always serializes to JSON")
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CollectionMode {
    #[default]
    Standard,
    TimeSeries,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CollectionMeta {
    pub name: String,
    pub vector_dim: Option<usize>,
    #[serde(default)]
    pub mode: CollectionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<CollectionPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_policy: Option<crate::MutationPolicy>,
    /// Explicit allow-list bit for mesh export/import. False by default so
    /// SQL RLS tables and newly-created namespaces cannot enter the mesh by
    /// accident; operators enable it only for provisioned shared schemas.
    #[serde(default)]
    pub mesh_sync_enabled: bool,
}

impl CollectionMeta {
    pub fn standard(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            vector_dim: None,
            mode: CollectionMode::Standard,
            policy: None,
            mutation_policy: None,
            mesh_sync_enabled: false,
        }
    }

    pub fn time_series(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            vector_dim: None,
            mode: CollectionMode::TimeSeries,
            policy: None,
            mutation_policy: None,
            mesh_sync_enabled: false,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CollectionPolicy {
    pub tenant_field: String,
    #[serde(default)]
    pub read_roles: BTreeSet<String>,
    #[serde(default)]
    pub write_roles: BTreeSet<String>,
    #[serde(default)]
    pub delete_roles: BTreeSet<String>,
    #[serde(default)]
    pub columns: BTreeMap<String, ColumnSecurity>,
    #[serde(default)]
    pub protected: bool,
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub vector_non_sensitive: bool,
}

impl CollectionPolicy {
    pub fn tenant_field(field: impl Into<String>) -> Self {
        Self {
            tenant_field: field.into(),
            ..Self::default()
        }
    }

    pub fn with_read_roles<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.read_roles = roles.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_write_roles<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.write_roles = roles.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_delete_roles<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.delete_roles = roles.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = columns
            .into_iter()
            .map(|column| (column.into(), ColumnSecurity::default()))
            .collect();
        self
    }

    pub fn with_column_security(
        mut self,
        column: impl Into<String>,
        security: ColumnSecurity,
    ) -> Self {
        self.columns.insert(column.into(), security);
        self.protected = true;
        self
    }

    pub fn with_schema_version(mut self, schema_version: u32) -> Self {
        self.schema_version = schema_version;
        self
    }

    pub fn with_non_sensitive_vectors(mut self, non_sensitive: bool) -> Self {
        self.vector_non_sensitive = non_sensitive;
        self
    }
}

fn default_schema_version() -> u32 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnSecurity {
    #[serde(default)]
    pub encrypted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pii_category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blind_index: Option<BlindIndexPolicy>,
    #[serde(default)]
    pub redaction: RedactionPolicy,
    #[serde(default)]
    pub decrypt_roles: BTreeSet<String>,
}

impl Default for ColumnSecurity {
    fn default() -> Self {
        Self {
            encrypted: false,
            pii_category: None,
            key_ref: None,
            blind_index: None,
            redaction: RedactionPolicy::Null,
            decrypt_roles: BTreeSet::new(),
        }
    }
}

impl ColumnSecurity {
    pub fn encrypted(category: impl Into<String>) -> Self {
        Self {
            encrypted: true,
            pii_category: Some(category.into()),
            ..Self::default()
        }
    }

    pub fn with_key_ref(mut self, key_ref: impl Into<String>) -> Self {
        self.key_ref = Some(key_ref.into());
        self
    }

    pub fn with_blind_index(mut self, namespace: impl Into<String>) -> Self {
        self.blind_index = Some(BlindIndexPolicy {
            namespace: namespace.into(),
            version: 1,
        });
        self
    }

    pub fn with_redaction(mut self, redaction: RedactionPolicy) -> Self {
        self.redaction = redaction;
        self
    }

    pub fn with_decrypt_roles<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.decrypt_roles = roles.into_iter().map(Into::into).collect();
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlindIndexPolicy {
    pub namespace: String,
    #[serde(default = "default_blind_index_version")]
    pub version: u32,
}

fn default_blind_index_version() -> u32 {
    1
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RedactionPolicy {
    #[default]
    Null,
    Fixed(String),
    Ciphertext,
}

#[cfg(test)]
mod stored_record_tests {
    use super::*;

    #[test]
    fn standard_envelope_prefix_consumes_only_its_cell() {
        for envelope in [
            r#"{"$bicdb_typed":{"version":1,"pg_type":"numeric","text":"12.50","index_key":"0a0b"}}"#,
            r#"{"$bicdb_typed":{"version":1,"pg_type":"timestamp","text":"2026-09-04 12:00:00"}}"#,
            r#"{"$bicdb_typed":{"version":1,"pg_type":"text","text":"héllo"}}"#,
        ] {
            let input = format!("{envelope},\"next\":42}}");
            let (cell, consumed) = scan_standard_text_envelope_prefix(&input).unwrap();
            assert_eq!(consumed, envelope.len());
            assert_eq!(
                format!("{cell:?}"),
                format!("{:?}", classify_envelope_via_serde(envelope))
            );
            let object = format!("{{\"first\":{input}");
            let mut cells = Vec::new();
            assert!(scan_compact_cells(&object, &mut cells));
            assert_eq!(cells.len(), 2);
            assert!(matches!(cells[1].1, CellRef::Int(42)));
        }
        for not_standard in [
            r#"{"$bicdb_typed":{"version":1,"pg_type":"numeric","text":"a\"b"}}"#,
            r#"{"$bicdb_typed":{"version":2,"pg_type":"numeric","text":"1"}}"#,
            r#"{"$bicdb_typed":{"version":1,"pg_type":"numeric","text":"1","extra":2}}"#,
            r#"{"$bicdb_typed":{"version":1,"pg_type":"numeric","text":"1"}"#,
        ] {
            assert!(scan_standard_text_envelope_prefix(not_standard).is_none());
        }
    }
    use serde_json::json;

    #[test]
    fn word_string_scan_matches_scalar_boundaries() {
        fn scalar(bytes: &[u8], mut position: usize) -> Option<(usize, bool)> {
            let mut escaped = false;
            loop {
                match bytes.get(position) {
                    Some(b'"') => return Some((position, escaped)),
                    Some(b'\\') => {
                        escaped = true;
                        position += 2;
                    }
                    Some(byte) if *byte < 0x20 => return None,
                    Some(_) => position += 1,
                    None => return None,
                }
            }
        }
        for length in 0..96 {
            let mut bytes = vec![b'x'; length + 1];
            bytes[length] = b'"';
            for position in 0..=length {
                let old = bytes[position];
                for byte in 0..=255u8 {
                    bytes[position] = byte;
                    for start in [0, length / 2, length] {
                        assert_eq!(
                            json_string_end(&bytes, start),
                            scalar(&bytes, start),
                            "length={length} position={position} byte={byte} start={start}"
                        );
                    }
                }
                bytes[position] = old;
            }
        }
        for text in [
            "é漢字🦀 mixed \" end",
            "escaped\\\"quote\\\\slash\"",
            "trailing\\",
            "unterminated",
        ] {
            assert_eq!(
                json_string_end(text.as_bytes(), 0),
                scalar(text.as_bytes(), 0)
            );
        }
    }

    fn sample() -> Record {
        Record::new("w1_d2_o3")
            .with_metadata(json!({
                "ol_amount": 123.45,
                "ol_quantity": 5,
                "ol_dist_info": "abcdefghijklmnopqrstuvwx",
                "nested": {"a": [1, 2, 3], "b": null},
                "flag": true,
            }))
            .with_timestamp(42)
    }

    fn serde_cells<'a>(text: &'a str) -> Option<Vec<(String, String)>> {
        serde_json::from_str::<&RawValue>(text).ok()?;
        let mut deserializer = serde_json::Deserializer::from_str(text);
        let map: BTreeMap<String, Box<RawValue>> =
            serde::Deserialize::deserialize(&mut deserializer).ok()?;
        // serde_json's map preserves first-seen order only through preserve_order;
        // compare as (key, classified cell) in text order instead.
        let mut ordered = Vec::new();
        let mut deserializer = serde_json::Deserializer::from_str(text);
        struct Keys(Vec<String>);
        impl<'de> serde::de::Visitor<'de> for Keys {
            type Value = Vec<String>;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                mut self,
                mut map: A,
            ) -> std::result::Result<Vec<String>, A::Error> {
                while let Some(key) = map.next_key::<String>()? {
                    let _: &RawValue = map.next_value()?;
                    self.0.push(key);
                }
                Ok(self.0)
            }
        }
        let keys =
            serde::Deserializer::deserialize_map(&mut deserializer, Keys(Vec::new())).ok()?;
        for key in keys {
            let raw = map.get(&key)?;
            ordered.push((key, format!("{:?}", classify_cell_ref(raw.get()))));
        }
        Some(ordered)
    }

    #[test]
    fn envelope_scan_matches_the_serde_derive_on_every_shape() {
        let shapes = [
            json!({"$bicdb_typed": {"version": 1, "pg_type": "numeric", "text": "12.50", "index_key": "0a0b"}}),
            json!({"$bicdb_typed": {"index_key": "0a0b", "text": "12.50", "pg_type": "numeric", "version": 1}}),
            json!({"$bicdb_typed": {"version": 1, "pg_type": "timestamp", "text": "2026-09-02 05:28:05"}}),
            json!({"$bicdb_typed": {"version": 1, "pg_type": "float8", "value": {"Float8": 1.5}}}),
            json!({"$bicdb_typed": {"version": 1, "pg_type": "point", "text": "(1,2)", "type_oid": 600}}),
            json!({"$bicdb_typed": {"version": 2, "pg_type": "numeric", "text": "1"}}),
            json!({"$bicdb_typed": {"version": 1, "pg_type": "text", "text": "a \"q\""}}),
            json!({"$bicdb_typed": {"version": 1, "pg_type": "nu\"m", "text": "1"}}),
            json!({"$bicdb_typed": {"version": 1, "pg_type": "numeric"}}),
            json!({"$bicdb_typed": {"version": 1, "text": "1"}}),
            json!({"$bicdb_typed": {"version": 1, "pg_type": "numeric", "text": "1", "extra": 5}}),
            json!({"$bicdb_typed": {"version": 1, "pg_type": "numeric", "text": "1", "index_key": null}}),
            json!({"$bicdb_typed": 5}),
            json!({"$bicdb_typed": {"version": 1, "pg_type": "numeric", "text": "1"}, "more": 1}),
        ];
        for shape in &shapes {
            let text = shape.to_string();
            let fast = scan_text_envelope(&text).map(|cell| format!("{cell:?}"));
            let derive = format!("{:?}", classify_envelope_via_serde(&text));
            if let Some(fast) = fast {
                assert_eq!(fast, derive, "envelope scan differs from serde on {text}");
            }
            assert_eq!(
                format!("{:?}", classify_cell_ref(&text)),
                derive,
                "classification on {text}"
            );
        }
        let text = shapes[0].to_string();
        assert!(matches!(
            scan_text_envelope(&text),
            Some(CellRef::Envelope {
                pg_type: "numeric",
                text: "12.50",
                index_key: Some("0a0b"),
                ..
            })
        ));
        assert!(matches!(
            scan_text_envelope(&shapes[3].to_string()),
            Some(CellRef::Raw(_))
        ));
        assert!(
            scan_text_envelope(&shapes[6].to_string()).is_none(),
            "escaped text declines"
        );
    }

    #[test]
    fn compact_cell_scan_matches_the_serde_path_on_every_shape() {
        let shapes: Vec<String> = vec![
            json!({}).to_string(),
            json!({"a": 1}).to_string(),
            json!({"s": "", "t": "plain", "u": "h\u{e9}llo \u{2713} \u{1F600}", "q": "a \"q\" b\\n", "c": "tab\tx"}).to_string(),
            json!({"i": 0, "n": -7, "big": i64::MAX, "huge": u64::MAX, "f": 1.5, "e": 1e300, "z": -0.0, "d": 12.0}).to_string(),
            json!({"t": true, "f": false, "n": null}).to_string(),
            json!({"amount": {"$bicdb_typed": {"version": 1, "pg_type": "numeric", "text": "12.50", "index_key": "0a0b"}},
                   "seen": {"$bicdb_typed": {"version": 1, "pg_type": "timestamp", "text": "2026-09-02 05:28:05"}},
                   "money": {"$bicdb_typed": {"version": 1, "pg_type": "float8", "value": {"Float8": 1.5}}},
                   "shape": {"$bicdb_typed": {"version": 1, "pg_type": "point", "text": "(1,2)", "type_oid": 600}},
                   "esc": {"$bicdb_typed": {"version": 1, "pg_type": "text", "text": "a \"}\" b"}}}).to_string(),
            json!({"nested": {"k": [1, {"x": "]}"}], "s": "{"}, "arr": [[], {}, "\"", 3], "empty": {}, "e2": []}).to_string(),
            json!({"k\"ey": 1, "k\\ey": 2}).to_string(),
            "{\"a\": 1, \"b\": \"x\"}".to_string(),
            "{\"a\":1,}".to_string(),
            "{\"a\":tru}".to_string(),
            "{\"a\":\"x}".to_string(),
            "{\"a\":[1,2}".to_string(),
            "{\"a\":1}x".to_string(),
            "[1,2]".to_string(),
        ];
        for (idx, text) in shapes.iter().enumerate() {
            let expected = serde_cells(text);
            let mut fast: CellRow<'_> = Vec::new();
            let fast_ok = scan_compact_cells(text, &mut fast);
            assert_eq!(
                fast_ok,
                idx < 7,
                "serde-written compact objects take the fast path, the rest decline: {text}"
            );
            if fast_ok {
                let got = fast
                    .iter()
                    .map(|(key, cell)| (key.to_string(), format!("{cell:?}")))
                    .collect::<Vec<_>>();
                assert_eq!(
                    Some(got),
                    expected,
                    "fast scan differs from serde on {text}"
                );
                assert!(
                    fast.iter()
                        .all(|(key, _)| matches!(key, std::borrow::Cow::Borrowed(_))),
                    "keys are borrowed on {text}"
                );
            } else {
                // Declined shapes are the ones the serde path either rejects or
                // handles itself (whitespace, escaped keys): never a wrong answer.
                let Ok(metadata) = RawValue::from_string(text.clone()) else {
                    assert!(expected.is_none(), "unstorable text {text}");
                    continue;
                };
                let stored = StoredRecord {
                    id: "r".into(),
                    metadata,
                    ..StoredRecord::from_record(&Record::new("r")).unwrap()
                };
                let mut cells = Vec::new();
                let ok = stored.cells_into(&mut cells);
                assert_eq!(ok, expected.is_some(), "serde fallback on {text}");
            }
        }
    }

    #[test]
    fn cells_into_borrows_every_top_level_cell_shape() {
        let record = Record::new("r").with_metadata(json!({
            "amount": {"$bicdb_typed": {"version": 1, "pg_type": "numeric", "text": "12.50", "index_key": "0a0b"}},
            "seen": {"$bicdb_typed": {"version": 1, "pg_type": "timestamp", "text": "2026-09-02 05:28:05"}},
            "money": {"$bicdb_typed": {"version": 1, "pg_type": "float8", "value": {"Float8": 1.5}}},
            "shape": {"$bicdb_typed": {"version": 1, "pg_type": "point", "text": "(1,2)", "type_oid": 600}},
            "name": "plain",
            "quoted": "a \"q\" b",
            "count": 42,
            "ratio": 0.25,
            "flag": true,
            "nothing": null,
            "nested": {"k": [1, 2]},
        }));
        let stored = StoredRecord::from_record(&record).unwrap();
        let mut cells = Vec::new();
        assert!(stored.cells_into(&mut cells));
        let get = |name: &str| {
            cells
                .iter()
                .find(|(key, _)| key.as_ref() == name)
                .map(|(_, cell)| cell.clone())
                .unwrap_or_else(|| panic!("cell {name}"))
        };
        assert!(matches!(
            get("amount"),
            CellRef::Envelope {
                pg_type: "numeric",
                text: "12.50",
                index_key: Some("0a0b"),
                ..
            }
        ));
        assert!(matches!(
            get("seen"),
            CellRef::Envelope {
                pg_type: "timestamp",
                text: "2026-09-02 05:28:05",
                index_key: None,
                ..
            }
        ));
        assert!(
            matches!(get("money"), CellRef::Raw(_)),
            "structured value stays raw"
        );
        assert!(
            matches!(get("shape"), CellRef::Raw(_)),
            "user/special type stays raw"
        );
        assert!(matches!(
            get("name"),
            CellRef::Str(std::borrow::Cow::Borrowed("plain"))
        ));
        assert!(
            matches!(get("quoted"), CellRef::Str(std::borrow::Cow::Owned(ref v)) if v == "a \"q\" b")
        );
        assert!(matches!(get("count"), CellRef::Int(42)));
        assert!(matches!(get("ratio"), CellRef::Float(v) if v == 0.25));
        assert!(matches!(get("flag"), CellRef::Bool(true)));
        assert!(matches!(get("nothing"), CellRef::Null));
        assert!(matches!(get("nested"), CellRef::Raw(_)));
        // Keys arrive in the record's order and the scratch vector is
        // reused: a second pass over a different row replaces the contents.
        let keys = cells
            .iter()
            .map(|(key, _)| key.as_ref())
            .collect::<Vec<_>>();
        let expected_keys = record
            .metadata
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        assert_eq!(keys, expected_keys);
        let other =
            StoredRecord::from_record(&Record::new("s").with_metadata(json!({"only": 1}))).unwrap();
        assert!(other.cells_into(&mut cells));
        assert_eq!(cells.len(), 1);
        let scalar = StoredRecord::from_record(&Record::new("t").with_metadata(json!(7))).unwrap();
        assert!(
            !scalar.cells_into(&mut cells),
            "non-object metadata is not a cell row"
        );
        assert!(cells.is_empty());
    }

    #[test]
    fn stored_record_serializes_byte_identically_to_record() {
        let record = sample();
        let stored = StoredRecord::from_record(&record).unwrap();
        // Disk format (segments + WAL) must be unchanged: the compact form must
        // serialize to exactly the same bytes as the heavy Record form.
        let record_bytes = serde_json::to_vec(&record).unwrap();
        let stored_bytes = serde_json::to_vec(&stored).unwrap();
        assert_eq!(
            record_bytes, stored_bytes,
            "StoredRecord must serialize byte-identically to Record"
        );
    }

    #[test]
    fn record_round_trips_through_stored_form() {
        let record = sample();
        let back = StoredRecord::from_record(&record)
            .unwrap()
            .to_record()
            .unwrap();
        assert_eq!(
            record, back,
            "Record -> StoredRecord -> Record must be identity"
        );
        assert_eq!(
            record.content_hash().unwrap(),
            back.content_hash().unwrap(),
            "content hash must be stable across compaction"
        );
    }

    #[test]
    fn stored_record_deserializes_from_record_bytes() {
        let record = sample();
        let bytes = serde_json::to_vec(&record).unwrap();
        // Recovery reads on-disk Record bytes; the compact form must decode them too.
        let stored: StoredRecord = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(stored.to_record().unwrap(), record);
    }

    #[test]
    fn empty_metadata_matches_record_new() {
        let record = Record::new("x");
        let stored = StoredRecord::from_record(&record).unwrap();
        assert_eq!(stored.metadata.get(), "{}");
        assert_eq!(
            serde_json::to_vec(&record).unwrap(),
            serde_json::to_vec(&stored).unwrap()
        );
    }
}

#[cfg(test)]
mod patch_tests {
    #[test]
    fn patched_object_text_equals_the_value_path() {
        use serde_json::{json, Value};
        let base = json!({"a": 1, "c": {"x": [1, 2]}, "d": "d\"q", "b": 2.50, "n": null});
        let text = base.to_string();
        let cases: Vec<Vec<(&str, Option<Value>)>> = vec![
            vec![("a", Some(json!(7)))],
            vec![("b", Some(json!("s"))), ("d", None)],
            vec![("aa", Some(json!(true))), ("zz", Some(json!({"k": 1})))],
            vec![("a", None), ("c", None), ("n", Some(json!(3)))],
            vec![],
        ];
        for case in cases {
            let rendered = case
                .iter()
                .map(|(key, value)| (*key, value.as_ref().map(|value| value.to_string())))
                .collect::<Vec<_>>();
            let patches = rendered
                .iter()
                .map(|(key, value)| (*key, value.as_deref()))
                .collect::<Vec<_>>();
            let spliced = super::patch_json_object_text(&text, &patches).unwrap();
            let mut expected = base.clone();
            for (key, value) in &case {
                match value {
                    Some(value) => {
                        expected[*key] = value.clone();
                    }
                    None => {
                        expected.as_object_mut().unwrap().remove(*key);
                    }
                }
            }
            // The patcher emits ascending top-level keys even when SQL's
            // preserve_order serde feature is unified into this test build.
            expected.as_object_mut().unwrap().sort_keys();
            assert_eq!(spliced, expected.to_string(), "{case:?}");
        }
        assert!(super::patch_json_object_text("[1]", &[]).is_none());
    }
}

#[cfg(test)]
mod splice_tests {
    use super::*;

    /// The parsing patcher, as the oracle for the splice.
    fn reference(text: &str, patches: &[(&str, Option<&str>)]) -> Option<String> {
        let members: std::collections::BTreeMap<String, &RawValue> =
            serde_json::from_str(text).ok()?;
        let mut merged: std::collections::BTreeMap<String, String> = members
            .iter()
            .map(|(key, raw)| (key.clone(), raw.get().to_string()))
            .collect();
        for (key, value) in patches {
            match value {
                Some(value) => {
                    merged.insert((*key).to_string(), (*value).to_string());
                }
                None => {
                    merged.remove(*key);
                }
            }
        }
        let mut out = String::from("{");
        for (idx, (key, value)) in merged.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            out.push_str(&serde_json::to_string(key).unwrap());
            out.push(':');
            out.push_str(value);
        }
        out.push('}');
        Some(out)
    }

    fn compact(mut value: serde_json::Value) -> String {
        value.as_object_mut().unwrap().sort_keys();
        serde_json::to_string(&value).unwrap()
    }

    #[test]
    fn projection_preserves_raw_values_and_requires_exact_keys() {
        let text = compact(serde_json::json!({
            "a": 1,
            "balance": {
                "$bicdb_typed": {
                    "version": 1,
                    "pg_type": "numeric",
                    "text": "-10.00",
                    "index_key": "0a0b"
                }
            },
            "nested": {"quoted": "a \"value\"", "items": [1, null, true]},
            "z": "last"
        }));
        let keys = [Box::<str>::from("balance"), Box::<str>::from("nested")];
        let projected = project_json_object_text(&text, &keys).unwrap();
        assert_eq!(
            projected,
            format!(
                "{{\"balance\":{},\"nested\":{}}}",
                serde_json::to_string(&serde_json::json!({
                    "$bicdb_typed": {
                        "version": 1,
                        "pg_type": "numeric",
                        "text": "-10.00",
                        "index_key": "0a0b"
                    }
                }))
                .unwrap(),
                serde_json::to_string(&serde_json::json!({
                    "quoted": "a \"value\"",
                    "items": [1, null, true]
                }))
                .unwrap()
            )
        );
        assert!(project_json_object_text(&text, &[Box::<str>::from("missing")]).is_none());
        assert!(
            project_json_object_text(&text, &[Box::<str>::from("a"), Box::<str>::from("a")])
                .is_none()
        );
        assert!(project_json_object_text("{ \"a\": 1 }", &[Box::<str>::from("a")]).is_none());
    }

    #[test]
    fn splice_matches_the_parsing_patcher_and_declines_non_compact_text() {
        let rows = [
            compact(serde_json::json!({
                "c_balance": {"$bicdb_typed": {"version": 1, "pg_type": "numeric", "text": "-10.00", "index_key": "0a0b"}},
                "c_d_id": 3, "c_data": "some \"quoted\" data\nline", "c_first": "Fname", "c_id": 42,
                "c_since": {"$bicdb_typed": {"version": 1, "pg_type": "timestamp", "text": "2026-09-03 12:00:00"}},
                "c_w_id": 1, "flag": true, "nothing": null, "ratio": 2.5, "neg": -7, "tags": ["a", "b"]
            })),
            compact(serde_json::json!({"a": 1})),
            "{}".to_string(),
            compact(
                serde_json::json!({"z": "last", "m": {"nested": [1, {"x": null}]}, "a": "first"}),
            ),
        ];
        let patch_sets: Vec<Vec<(&str, Option<&str>)>> = vec![
            vec![(
                "c_balance",
                Some(
                    r#"{"$bicdb_typed":{"version":1,"pg_type":"numeric","text":"5.25","index_key":"0c0d"}}"#,
                ),
            )],
            vec![("c_d_id", Some("4")), ("c_first", Some("\"New\\nName\""))],
            vec![
                ("added", Some("\"x\"")),
                ("nothing", None),
                ("zzz", Some("1")),
            ],
            vec![("a", None)],
            vec![],
            vec![("c_data", Some("null")), ("flag", Some("false"))],
        ];
        for text in &rows {
            for patches in &patch_sets {
                let mut patches = patches.clone();
                patches.sort_by(|a, b| a.0.cmp(b.0));
                let expected = reference(text, &patches);
                assert_eq!(
                    patch_json_object_text(text, &patches),
                    expected,
                    "{text} {patches:?}"
                );
                // The splice itself agrees wherever it answers, and every
                // compact serde-written row with ascending keys is answered.
                if let Some(spliced) = splice_json_object_text(text, &patches) {
                    assert_eq!(Some(spliced), expected, "{text} {patches:?}");
                }
            }
        }
        let row: serde_json::Value = serde_json::from_str(&rows[0]).unwrap();
        assert!(splice_json_object_text(&compact(row), &[("c_id", Some("7"))]).is_some());
        // Non-compact or non-canonical text takes the parsing path.
        assert!(splice_json_object_text("{ \"a\": 1 }", &[("a", Some("2"))]).is_none());
        assert!(splice_json_object_text("{\"b\":1,\"a\":2}", &[("a", Some("3"))]).is_none());
        assert!(splice_json_object_text("{\"k\\u0041\":1}", &[("a", Some("3"))]).is_none());
        assert_eq!(
            patch_json_object_text("{ \"a\": 1 }", &[("a", Some("2"))]),
            Some("{\"a\":2}".to_string())
        );
        assert!(patch_json_object_text("[1,2]", &[]).is_none());
    }

    #[test]
    fn spliced_rows_read_back_like_validated_ones() {
        let stored = StoredRecord {
            id: "r".to_string(),
            vector: None,
            metadata: serde_json::value::RawValue::from_string(compact(
                serde_json::json!({"a": 1, "b": "two", "c": null}),
            ))
            .unwrap(),
            geometry: None,
            timestamp: Some(3),
            payload: None,
            typed: std::sync::OnceLock::new(),
            evicted: None,
        };
        let patches = [("b", Some("\"three\"")), ("d", Some("4"))];
        let text = splice_json_object_text(stored.metadata.get(), &patches).unwrap();
        let spliced = stored.with_metadata_text_spliced(text.clone()).unwrap();
        let validated = stored.with_metadata_text(text).unwrap();
        assert_eq!(spliced.metadata.get(), validated.metadata.get());
        assert_eq!(
            spliced.to_record().unwrap().metadata,
            validated.to_record().unwrap().metadata
        );
        let mut a = Vec::new();
        let mut b = Vec::new();
        assert!(spliced.cells_into(&mut a) && validated.cells_into(&mut b));
        assert_eq!(format!("{a:?}"), format!("{b:?}"));
    }
}
