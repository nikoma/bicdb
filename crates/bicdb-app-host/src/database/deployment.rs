//! The database owns desired deployment state; host files are verified caches.
use super::*;
use crate::store::Deployment;

const RELEASES: &str = "__bic_platform_releases";
const DEPLOYMENT: &str = "__bic_platform_deployment";

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
                "SELECT payload FROM {DEPLOYMENT} WHERE id = 'current'"
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
        let release: Release = serde_json::from_str(payload)?;
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
        self.ensure_authority()?;
        let result = (|| -> Result<()> {
            for (table, columns) in [
                (RELEASES, "digest TEXT PRIMARY KEY, payload TEXT NOT NULL"),
                (DEPLOYMENT, "id TEXT PRIMARY KEY, payload TEXT NOT NULL"),
            ] {
                self.initialize_table(table, columns)?;
            }
            // One package per transaction keeps each WAL record bounded even
            // when the active and previous packages are both near the limit.
            for release in releases {
                release.validate()?;
                ensure!(
                    release.manifest.name == deployment.application,
                    "deployment release belongs to another application"
                );
                self.validate_replication_sync(&release.manifest.sync)?;
                let digest = release.digest()?;
                let payload = serde_json::to_string(release)?;
                let db = self.db.read();
                let mut session = SqlSession::new_shared(&db);
                if session
                    .execute(&format!(
                        "SELECT digest FROM {RELEASES} WHERE digest = {}",
                        quote(&digest)
                    ))?
                    .rows
                    .is_empty()
                {
                    write(
                        &mut session,
                        &format!(
                            "INSERT INTO {RELEASES} VALUES ({}, {})",
                            quote(&digest),
                            quote(&payload)
                        ),
                    )?;
                }
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
                .execute(&format!("SELECT id FROM {DEPLOYMENT} WHERE id = 'current'"))?
                .rows
                .is_empty()
            {
                write(
                    &mut session,
                    &format!("INSERT INTO {DEPLOYMENT} VALUES ('current', {payload})"),
                )?;
            } else {
                write(
                    &mut session,
                    &format!("UPDATE {DEPLOYMENT} SET payload = {payload} WHERE id = 'current'"),
                )?;
            }
            Ok(())
        })();
        self.acknowledge(result)
    }
}
