//! A preview imports logical SQL data into a fresh application database.
use super::*;

impl Database {
    pub(crate) fn materialize_preview(
        &self,
        path: &Path,
        release: &Release,
    ) -> Result<bicdb_sql::ApplicationCopyReport> {
        self.edge_barrier()?;
        let source = self.db.read();
        let receipts = SqlSession::new_shared(&source)
            .execute(&format!(
                "SELECT position, name, digest FROM {MIGRATIONS} ORDER BY position"
            ))?
            .rows;
        ensure!(
            receipts.len() == release.manifest.migrations.len(),
            "preview requires a fully applied release migration history"
        );
        for (position, name) in release.manifest.migrations.iter().enumerate() {
            let row = &receipts[position];
            ensure!(
                row[0] == SqlValue::Int(position as i64)
                    && row[1] == SqlValue::String(name.clone())
                    && row[2]
                        == SqlValue::String(format!("{:x}", Sha256::digest(release.decode(name)?))),
                "preview migration history differs from the saved release"
            );
        }
        let target = Database::open(path)?;
        let report = {
            let mut engine = target.db.write();
            let report = bicdb_sql::copy_application_tables(&source, &mut engine)?;
            // Receipts prevent replaying migration INSERTs or defaults while
            // creating a preview. Jobs, effect receipts and outboxes stay empty.
            engine.batch_insert(MIGRATIONS, source.scan_collection(MIGRATIONS)?)?;
            engine.flush()?;
            ensure!(
                engine.node_id() != source.node_id()
                    && engine.mesh_verifying_key() != source.mesh_verifying_key(),
                "preview database identity must be independent"
            );
            report
        };
        drop(target);
        drop(source);
        self.edge_barrier()?;
        Ok(report)
    }
}
