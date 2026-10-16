//! Encrypted transport grants travel with state; their operator key does not.
use super::*;

const PEERS: &str = "__bic_platform_peer_grants";

impl Database {
    pub(crate) fn peer_grants(&self) -> Result<Option<String>> {
        let db = self.db.read();
        if !db.collections().iter().any(|table| table.name == PEERS) {
            return Ok(None);
        }
        let rows = SqlSession::new_shared(&db)
            .execute(&format!(
                "SELECT ciphertext FROM {PEERS} WHERE id = 'current'"
            ))?
            .rows;
        rows.first()
            .map(|row| match &row[0] {
                SqlValue::String(value) => Ok(value.clone()),
                _ => bail!("invalid encrypted peer grants"),
            })
            .transpose()
    }

    pub(crate) fn save_peer_grants(&self, ciphertext: &str) -> Result<()> {
        ensure!(ciphertext.len() <= 2 * 1024 * 1024, "peer grants too large");
        self.initialize_table(PEERS, "id TEXT PRIMARY KEY, ciphertext TEXT NOT NULL")?;
        let result = (|| -> Result<()> {
            let db = self.db.read();
            let mut session = SqlSession::new_shared(&db);
            session.execute("BEGIN")?;
            let result = (|| -> Result<()> {
                session.execute(&format!("DELETE FROM {PEERS} WHERE id = 'current'"))?;
                session.execute(&format!(
                    "INSERT INTO {PEERS} VALUES ('current', {})",
                    quote(ciphertext)
                ))?;
                session.execute("COMMIT")?;
                Ok(())
            })();
            if result.is_err() {
                let _ = session.execute("ROLLBACK");
            }
            result
        })();
        self.acknowledge(result)
    }

    pub(crate) fn clear_peer_grants(&self) -> Result<()> {
        if self.peer_grants()?.is_some() {
            self.ensure_authority()?;
            let result = (|| -> Result<()> {
                let mut db = self.db.write();
                SqlSession::new(&mut db).execute(&format!("DELETE FROM {PEERS}"))?;
                Ok(())
            })();
            self.acknowledge(result)?;
        }
        Ok(())
    }

    /// Call while the host's DatabaseHandle is locked, but without an engine
    /// guard. No unacknowledged suffix may escape to independent edge peers.
    pub(crate) fn edge_barrier(&self) -> Result<()> {
        self.ensure_authority()?;
        self.acknowledge(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recovery_clears_replicated_transport_grants_before_publication() {
        let root = tempfile::tempdir().unwrap();
        let release = Release::read(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/platform/counter/bic.json"),
        )
        .unwrap();
        let mut database = Database::open(&root.path().join("source")).unwrap();
        database.prepare(&release).unwrap();
        database
            .save_peer_grants("encrypted-grant-fixture")
            .unwrap();
        assert_eq!(
            database.peer_grants().unwrap().as_deref(),
            Some("encrypted-grant-fixture")
        );
        assert_eq!(
            database
                .db
                .write()
                .export_sync_bundle_delta(&Default::default())
                .unwrap()
                .event_count,
            0
        );
        let deployment = crate::store::Deployment {
            application: release.manifest.name.clone(),
            release: release.digest().unwrap(),
            previous: None,
            generation: 1,
            enabled: true,
        };
        let archive = root.path().join("application.bicbak");
        let password = "recovery-unit-test-passphrase-000000000000000000";
        crate::recovery::snapshot(&mut database, deployment, release, None, &archive, password)
            .unwrap();
        let store = crate::store::Store::open(&root.path().join("target")).unwrap();
        let restored = crate::recovery::restore(&store, &archive, password).unwrap();
        assert!(!restored.enabled);
        let recovered =
            Database::open(&store.app_dir("counter").unwrap().join("database")).unwrap();
        assert!(recovered.peer_grants().unwrap().is_none());
        assert!(
            database.peer_grants().unwrap().is_some(),
            "restoring cannot revoke the live source's grants"
        );
    }
}
