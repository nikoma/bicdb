//! Bounded ingestion of unverified application packages.
use crate::package::{
    MAX_APPLICATION_COMPONENTS, MAX_FRONTEND_ASSETS, MAX_FRONTEND_ASSET_PATH_BYTES,
};
use crate::{AppRuntimeError, ApplicationPackage, FrontendAsset, PackageVerifier, Result};
use serde::{
    de::{self, MapAccess, Visitor},
    Deserialize, Deserializer,
};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, BufReader, Read, Write},
    marker::PhantomData,
    path::Path,
};

pub const MAX_PACKAGE_MODULES: usize = 4_096;
pub const MAX_MODULE_NAME_BYTES: usize = 1_024;
const ENCODED_OVERHEAD_BYTES: u64 = 64 * 1024;

/// Accommodate compact JSON and the ordinary pretty-printed byte arrays used
/// by package producers. This is a hard input ceiling, not an allowance for
/// more decoded data; callers still enforce the independent decoded limit.
pub fn encoded_package_byte_limit(decoded_limit: usize) -> Result<u64> {
    if decoded_limit == 0 {
        return Err(AppRuntimeError::InvalidPackage(
            "maximum package bytes must be positive".into(),
        ));
    }
    u64::try_from(decoded_limit)
        .ok()
        .and_then(|limit| limit.checked_mul(16))
        .and_then(|limit| limit.checked_add(ENCODED_OVERHEAD_BYTES))
        .ok_or_else(|| {
            AppRuntimeError::InvalidPackage("encoded package byte limit overflows".into())
        })
}

fn encoded_limit_error(limit: u64) -> AppRuntimeError {
    AppRuntimeError::InvalidPackage(format!("encoded package exceeds byte limit {limit}"))
}

struct LimitedPackageReader<R> {
    inner: R,
    remaining: u64,
    exceeded: bool,
}
impl<R: Read> Read for LimitedPackageReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            // A Take reader would turn the limit into EOF and could accept a
            // valid JSON prefix while silently ignoring the rest of the file.
            let mut extra = [0];
            if self.inner.read(&mut extra)? == 0 {
                return Ok(0);
            }
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "encoded package byte limit exceeded",
            ));
        }
        let length = self.remaining.min(buffer.len() as u64) as usize;
        let read = self.inner.read(&mut buffer[..length])?;
        self.remaining -= read as u64;
        Ok(read)
    }
}

impl PackageVerifier {
    /// Decode an untrusted file under this verifier's size policy. This does
    /// not establish trust: verify/install/stage must still verify its signature.
    pub fn read_package_file(&self, path: impl AsRef<Path>) -> Result<ApplicationPackage> {
        let file = File::open(path)?;
        let limit = encoded_package_byte_limit(self.max_package_bytes())?;
        if file.metadata()?.len() > limit {
            return Err(encoded_limit_error(limit));
        }
        // Use the same open file for the metadata check and bounded read.
        // Concurrent growth or a non-regular reader cannot bypass the ceiling.
        self.read_package(file)
    }

    /// Decode a bounded stream; JSON must consume the entire input, including
    /// trailing whitespace, without accepting a prefix truncated at the limit.
    pub fn read_package(&self, reader: impl Read) -> Result<ApplicationPackage> {
        let limit = encoded_package_byte_limit(self.max_package_bytes())?;
        let mut reader = LimitedPackageReader {
            inner: reader,
            remaining: limit,
            exceeded: false,
        };
        let decoded = serde_json::from_reader::<_, ApplicationPackage>(BufReader::new(&mut reader));
        if reader.exceeded {
            return Err(encoded_limit_error(limit));
        }
        let package = decoded?;
        package.check_decoded_size(self.max_package_bytes())?;
        Ok(package)
    }
}

pub(crate) fn deserialize_modules<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, Vec<u8>>, D::Error> {
    bounded_map(
        deserializer,
        MAX_PACKAGE_MODULES,
        MAX_MODULE_NAME_BYTES,
        "module",
    )
}

pub(crate) fn deserialize_frontend_assets<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, FrontendAsset>, D::Error> {
    bounded_map(
        deserializer,
        MAX_FRONTEND_ASSETS,
        MAX_FRONTEND_ASSET_PATH_BYTES,
        "frontend asset",
    )
}

fn bounded_map<'de, D: Deserializer<'de>, V: Deserialize<'de>>(
    deserializer: D,
    count: usize,
    name_bytes: usize,
    label: &'static str,
) -> std::result::Result<BTreeMap<String, V>, D::Error> {
    struct BoundedMap<V> {
        count: usize,
        name_bytes: usize,
        label: &'static str,
        marker: PhantomData<V>,
    }
    impl<'de, V: Deserialize<'de>> Visitor<'de> for BoundedMap<V> {
        type Value = BTreeMap<String, V>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a bounded {} map", self.label)
        }
        fn visit_map<M: MapAccess<'de>>(
            self,
            mut map: M,
        ) -> std::result::Result<Self::Value, M::Error> {
            let mut values = BTreeMap::new();
            while let Some(name) = map.next_key::<String>()? {
                if values.len() == self.count {
                    return Err(de::Error::custom(format!(
                        "package exceeds {} {} entries",
                        self.count, self.label
                    )));
                }
                if name.is_empty() || name.len() > self.name_bytes {
                    return Err(de::Error::custom(format!(
                        "{} name must contain 1..={} bytes",
                        self.label, self.name_bytes
                    )));
                }
                if values.contains_key(&name) {
                    return Err(de::Error::custom(format!("duplicate {} name", self.label)));
                }
                // Reject invalid metadata before decoding the associated value.
                values.insert(name, map.next_value()?);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_map(BoundedMap {
        count,
        name_bytes,
        label,
        marker: PhantomData,
    })
}

struct SizeCounter {
    bytes: usize,
    limit: usize,
}
impl SizeCounter {
    fn metadata(&mut self, value: &impl serde::Serialize) -> Result<()> {
        let limit = self.limit;
        serde_json::to_writer(self, value).map_err(|error| {
            AppRuntimeError::InvalidPackage(format!(
                "package metadata cannot be counted within decoded byte limit {limit}: {error}"
            ))
        })
    }

    fn add(&mut self, bytes: usize) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(format!(
                    "decoded package exceeds byte limit {}",
                    self.limit
                ))
            })?;
        Ok(())
    }
}
impl Write for SizeCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.add(buffer.len())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        Ok(buffer.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ApplicationPackage {
    pub(crate) fn check_decoded_size(&self, limit: usize) -> Result<usize> {
        if self.modules.len() > MAX_PACKAGE_MODULES {
            return Err(AppRuntimeError::InvalidPackage(format!(
                "package exceeds {MAX_PACKAGE_MODULES} module entries"
            )));
        }
        if self.frontend_assets.len() > MAX_FRONTEND_ASSETS
            || self.components.len() > MAX_APPLICATION_COMPONENTS
        {
            return Err(AppRuntimeError::InvalidPackage(
                "package has too many frontend assets or components".into(),
            ));
        }
        let mut counter = SizeCounter { bytes: 0, limit };
        for value in self
            .modules
            .values()
            .map(Vec::len)
            .chain(self.frontend_assets.values().map(|asset| asset.bytes.len()))
            .chain([
                self.dependency_lock.len(),
                self.sbom.len(),
                self.provenance.len(),
            ])
        {
            counter.add(value)?;
        }
        if counter.bytes == 0 {
            return Err(AppRuntimeError::InvalidPackage(
                "package contains no payload bytes".into(),
            ));
        }
        for name in self.modules.keys() {
            if name.is_empty() || name.len() > MAX_MODULE_NAME_BYTES {
                return Err(AppRuntimeError::InvalidPackage(format!(
                    "module name must contain 1..={MAX_MODULE_NAME_BYTES} bytes"
                )));
            }
            counter.add(name.len())?;
        }
        for (path, asset) in &self.frontend_assets {
            counter.add(path.len())?;
            counter.add(asset.content_type.len())?;
        }
        // Count all manifest/signature and component metadata without cloning
        // it or allocating a serialized copy. Overflow/limit errors stop early.
        counter.metadata(&self.manifest)?;
        counter.metadata(&self.components)?;
        Ok(counter.bytes)
    }
}
