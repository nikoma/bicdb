//! The database owns desired deployment state; host files are verified caches.
use super::*;
use crate::store::Deployment;
use base64::{engine::general_purpose::STANDARD, Engine};

const RELEASES: &str = "__bic_platform_releases";
const DEPLOYMENT: &str = "__bic_platform_deployment";
const CHUNKS: &str = "__bic_platform_release_chunks";
const CHUNK_BYTES: usize = 384 * 1024;
const MAX_PAYLOAD_BYTES: usize = crate::manifest::MAX_RELEASE_BYTES * 4 / 3 + 2 * 1024 * 1024;
const CHUNK_PREFIX: &str = "@bicdb-release-chunks-v1:";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChunkedRelease {
    parts: usize,
    bytes: usize,
    sha256: String,
}

fn write(session: &mut SqlSession<'_>, sql: &str) -> Result<()> {
    session.execute("BEGIN")?;
    let result = session.execute(sql).and_then(|_| session.execute("COMMIT"));
    if result.is_err() {
        let _ = session.execute("ROLLBACK");
    }
    result?;
    Ok(())
}

impl Database {
    pub(crate) fn deployment(&self) -> Result<Option<Deployment>> {
        self.named_deployment("current")
    }

    pub(crate) fn companion_deployment(&self, application: &str) -> Result<Option<Deployment>> {
        self.named_deployment(&format!("companion:{application}"))
    }

    fn named_deployment(&self, key: &str) -> Result<Option<Deployment>> {
        let db = self.db.read();
        if !db
            .collections()
            .iter()
            .any(|table| table.name == DEPLOYMENT)
        {
            return Ok(None);
        }
        let rows = SqlSession::new_shared(&db)
            .execute(&format!(
                "SELECT payload FROM {DEPLOYMENT} WHERE id = {}",
                quote(key)
            ))?
            .rows;
        rows.first()
            .map(|row| match &row[0] {
                SqlValue::String(value) => Ok(serde_json::from_str(value)?),
                _ => bail!("invalid deployment state"),
            })
            .transpose()
    }

    pub(crate) fn deployment_release(&self, digest: &str) -> Result<Release> {
        let db = self.db.read();
        let rows = SqlSession::new_shared(&db)
            .execute(&format!(
                "SELECT payload FROM {RELEASES} WHERE digest = {}",
                quote(digest)
            ))?
            .rows;
        let Some(SqlValue::String(payload)) = rows.first().and_then(|row| row.first()) else {
            bail!("deployment release is unavailable");
        };
        let bytes = if let Some(header) = payload.strip_prefix(CHUNK_PREFIX) {
            let header: ChunkedRelease = serde_json::from_str(header)?;
            ensure!(
                header.bytes > CHUNK_BYTES
                    && header.bytes <= MAX_PAYLOAD_BYTES
                    && header.parts == header.bytes.div_ceil(CHUNK_BYTES)
                    && header.sha256.len() == 64,
                "invalid chunked release bounds"
            );
            let rows =
                SqlSession::new_shared(&db)
                    .execute(&format!(
                "SELECT part,payload FROM {CHUNKS} WHERE digest = {} ORDER BY part LIMIT {}",
                quote(digest), header.parts + 1))?
                    .rows;
            ensure!(
                rows.len() == header.parts,
                "deployment release is incomplete"
            );
            let mut bytes = Vec::with_capacity(header.bytes);
            for (part, row) in rows.into_iter().enumerate() {
                ensure!(
                    row.len() == 2 && row[0].to_cell().parse::<usize>()? == part,
                    "invalid release chunk ordering"
                );
                let SqlValue::String(encoded) = &row[1] else {
                    bail!("invalid release chunk");
                };
                ensure!(
                    encoded.len() <= CHUNK_BYTES * 4 / 3,
                    "oversized release chunk"
                );
                let decoded = STANDARD.decode(encoded)?;
                ensure!(
                    decoded.len() == CHUNK_BYTES.min(header.bytes - bytes.len()),
                    "invalid release chunk size"
                );
                bytes.extend(decoded);
            }
            ensure!(
                format!("{:x}", Sha256::digest(&bytes)) == header.sha256,
                "deployment release chunk integrity failure"
            );
            bytes
        } else {
            ensure!(
                payload.len() <= MAX_PAYLOAD_BYTES,
                "oversized legacy release"
            );
            payload.as_bytes().to_vec()
        };
        let release: Release = serde_json::from_slice(&bytes)?;
        release.validate()?;
        ensure!(
            release.digest()? == digest,
            "deployment release integrity failure"
        );
        Ok(release)
    }

    pub(crate) fn publish_deployment(
        &mut self,
        deployment: &Deployment,
        releases: &[&Release],
    ) -> Result<()> {
        self.publish_named_deployment("current", deployment, releases)
    }

    pub(crate) fn publish_companion_deployment(
        &mut self,
        deployment: &Deployment,
        releases: &[&Release],
    ) -> Result<()> {
        self.publish_named_deployment(
            &format!("companion:{}", deployment.application),
            deployment,
            releases,
        )
    }

    fn store_replicated_release(&self, release: &Release) -> Result<()> {
        let digest = release.digest()?;
        {
            let db = self.db.read();
            if !SqlSession::new_shared(&db)
                .execute(&format!(
                    "SELECT digest FROM {RELEASES} WHERE digest = {}",
                    quote(&digest)
                ))?
                .rows
                .is_empty()
            {
                return Ok(());
            }
        }
        let payload = serde_json::to_vec(release)?;
        ensure!(
            payload.len() <= MAX_PAYLOAD_BYTES,
            "oversized serialized release"
        );
        let stored = if payload.len() > CHUNK_BYTES {
            self.initialize_table(CHUNKS,
                "digest TEXT NOT NULL, part BIGINT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(digest,part)")?;
            // A package-sized WAL value stalls the replication control channel.
            // Confirm each bounded piece before offering the next, and publish
            // the immutable header only after every piece is quorum durable.
            for (part, bytes) in payload.chunks(CHUNK_BYTES).enumerate() {
                self.ensure_authority()?;
                let encoded = STANDARD.encode(bytes);
                let result = (|| -> Result<()> {
                    let db = self.db.read();
                    let mut session = SqlSession::new_shared(&db);
                    let rows = session
                        .execute(&format!(
                            "SELECT payload FROM {CHUNKS} WHERE digest = {} AND part = {part}",
                            quote(&digest)
                        ))?
                        .rows;
                    if let Some(row) = rows.first() {
                        ensure!(
                            row.first() == Some(&SqlValue::String(encoded.clone())),
                            "conflicting immutable release chunk"
                        );
                    } else {
                        write(
                            &mut session,
                            &format!(
                                "INSERT INTO {CHUNKS} VALUES ({}, {part}, {})",
                                quote(&digest),
                                quote(&encoded)
                            ),
                        )?;
                    }
                    Ok(())
                })();
                self.acknowledge(result)?;
            }
            format!(
                "{CHUNK_PREFIX}{}",
                serde_json::to_string(&ChunkedRelease {
                    parts: payload.len().div_ceil(CHUNK_BYTES),
                    bytes: payload.len(),
                    sha256: format!("{:x}", Sha256::digest(&payload)),
                })?
            )
        } else {
            String::from_utf8(payload)?
        };
        let result = (|| -> Result<()> {
            let db = self.db.read();
            write(
                &mut SqlSession::new_shared(&db),
                &format!(
                    "INSERT INTO {RELEASES} VALUES ({}, {})",
                    quote(&digest),
                    quote(&stored)
                ),
            )
        })();
        self.acknowledge(result)
    }

    fn publish_named_deployment(
        &mut self,
        key: &str,
        deployment: &Deployment,
        releases: &[&Release],
    ) -> Result<()> {
        self.ensure_authority()?;
        let key = quote(key);
        let result = (|| -> Result<()> {
            for (table, columns) in [
                (RELEASES, "digest TEXT PRIMARY KEY, payload TEXT NOT NULL"),
                (DEPLOYMENT, "id TEXT PRIMARY KEY, payload TEXT NOT NULL"),
            ] {
                self.initialize_table(table, columns)?;
            }
            for release in releases {
                release.validate()?;
                ensure!(
                    release.manifest.name == deployment.application,
                    "deployment release belongs to another application"
                );
                self.validate_replication_sync(&release.manifest.sync)?;
                self.store_replicated_release(release)?;
            }
            let db = self.db.read();
            let mut session = SqlSession::new_shared(&db);
            for digest in std::iter::once(&deployment.release).chain(deployment.previous.iter()) {
                ensure!(
                    !session
                        .execute(&format!(
                            "SELECT digest FROM {RELEASES} WHERE digest = {}",
                            quote(digest)
                        ))?
                        .rows
                        .is_empty(),
                    "deployment references a missing release"
                );
            }
            let payload = quote(&serde_json::to_string(deployment)?);
            if session
                .execute(&format!("SELECT id FROM {DEPLOYMENT} WHERE id = {key}"))?
                .rows
                .is_empty()
            {
                write(
                    &mut session,
                    &format!("INSERT INTO {DEPLOYMENT} VALUES ({key}, {payload})"),
                )?;
            } else {
                write(
                    &mut session,
                    &format!("UPDATE {DEPLOYMENT} SET payload = {payload} WHERE id = {key}"),
                )?;
            }
            Ok(())
        })();
        self.acknowledge(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn release(large: bool) -> Release {
        let padding = if large {
            vec![73u8; CHUNK_BYTES * 3]
        } else {
            vec![73u8; 16]
        };
        serde_json::from_value(json!({"manifest":{"format":1,"name":"chunk-fixture","version":"1","runtime":"node_http","entrypoint":"app.mjs","files":["app.mjs","padding.bin"]},"files":{"app.mjs":STANDARD.encode("export default {};"),"padding.bin":STANDARD.encode(padding)}})).unwrap()
    }
    #[test]
    fn bounded_release_chunks_round_trip_and_legacy_payloads_still_read() {
        for large in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut db = Database::open(directory.path()).unwrap();
            let release = release(large);
            let digest = release.digest().unwrap();
            let desired = Deployment {
                application: release.manifest.name.clone(),
                release: digest.clone(),
                previous: None,
                generation: 1,
                enabled: true,
            };
            db.publish_deployment(&desired, &[&release]).unwrap();
            let actual = db.deployment_release(&digest).unwrap();
            assert_eq!(
                serde_json::to_value(actual).unwrap(),
                serde_json::to_value(&release).unwrap()
            );
            let guard = db.db.read();
            let mut session = SqlSession::new_shared(&guard);
            let payload = session
                .execute(&format!("SELECT payload FROM {RELEASES}"))
                .unwrap()
                .rows[0][0]
                .to_cell();
            assert_eq!(payload.starts_with(CHUNK_PREFIX), large);
            if large {
                let rows = session
                    .execute(&format!("SELECT payload FROM {CHUNKS}"))
                    .unwrap()
                    .rows;
                assert!(rows.len() > 1);
                assert!(rows
                    .iter()
                    .all(|r| r[0].to_cell().len() <= CHUNK_BYTES * 4 / 3));
            }
        }
    }
    #[test]
    fn incomplete_or_corrupt_chunks_cannot_become_executable_code() {
        for mutation in [
            format!("DELETE FROM {CHUNKS} WHERE part=0"),
            format!("UPDATE {CHUNKS} SET payload='broken' WHERE part=0"),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let mut db = Database::open(directory.path()).unwrap();
            let release = release(true);
            let digest = release.digest().unwrap();
            let desired = Deployment {
                application: release.manifest.name.clone(),
                release: digest.clone(),
                previous: None,
                generation: 1,
                enabled: true,
            };
            db.publish_deployment(&desired, &[&release]).unwrap();
            SqlSession::new(&mut db.db.write())
                .execute(&mutation)
                .unwrap();
            assert!(db.deployment_release(&digest).is_err());
        }
    }
    #[test]
    fn interrupted_package_publication_resumes_only_matching_immutable_chunks() {
        for matching in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            let db = Database::open(directory.path()).unwrap();
            let release = release(true);
            let digest = release.digest().unwrap();
            let payload = serde_json::to_vec(&release).unwrap();
            db.initialize_table(RELEASES, "digest TEXT PRIMARY KEY,payload TEXT NOT NULL")
                .unwrap();
            db.initialize_table(CHUNKS,"digest TEXT NOT NULL,part BIGINT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(digest,part)").unwrap();
            let first = if matching {
                STANDARD.encode(&payload[..CHUNK_BYTES])
            } else {
                "corrupt".into()
            };
            SqlSession::new(&mut db.db.write())
                .execute(&format!(
                    "INSERT INTO {CHUNKS} VALUES({},0,{})",
                    quote(&digest),
                    quote(&first)
                ))
                .unwrap();
            let published = db.store_replicated_release(&release);
            assert_eq!(published.is_ok(), matching);
            if matching {
                assert_eq!(
                    db.deployment_release(&digest).unwrap().digest().unwrap(),
                    digest
                );
            } else {
                assert!(
                    db.deployment_release(&digest).is_err(),
                    "an interrupted conflicting package cannot become executable"
                );
            }
        }
    }
}
