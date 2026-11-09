//! Split out of the parent module to keep files digestible; behavior
//! unchanged — a separate `impl` block on the same type.
use super::*;

// CTEs have already been executed by the session. Do not clone their often
// much larger UPDATE/RETURNING trees merely to discard them before SELECT.
// List every Query field explicitly so parser additions require review.
fn clone_query_without_with(query: &Query) -> Query {
    Query {
        with: None,
        body: query.body.clone(),
        order_by: query.order_by.clone(),
        limit_clause: query.limit_clause.clone(),
        fetch: query.fetch.clone(),
        locks: query.locks.clone(),
        for_clause: query.for_clause.clone(),
        settings: query.settings.clone(),
        format_clause: query.format_clause.clone(),
        pipe_operators: query.pipe_operators.clone(),
    }
}

#[cfg(test)]
mod query_clone_tests {
    use super::*;

    #[test]
    fn without_with_preserves_query_body_and_modifiers() {
        use sqlparser::{dialect::GenericDialect, parser::Parser};

        for sql in [
            "WITH x AS (SELECT 1 AS n) SELECT n FROM x ORDER BY n DESC LIMIT 2 OFFSET 1",
            "WITH x AS (SELECT 1 AS n) SELECT n FROM x FETCH FIRST 2 ROWS ONLY",
            "WITH x AS (SELECT 1 AS n) SELECT n FROM x FOR UPDATE",
            "WITH x AS (SELECT 1 AS n) SELECT n FROM x UNION ALL SELECT 2 ORDER BY n",
            "WITH x AS (UPDATE t SET n = n + 1 RETURNING n) SELECT array_agg(n) FROM x",
            "WITH x AS (SELECT 1 AS n) SELECT * FROM (WITH y AS (SELECT 2 AS m) SELECT m FROM y) AS nested",
        ] {
            let Statement::Query(query) = Parser::parse_sql(&GenericDialect {}, sql)
                .unwrap()
                .remove(0)
            else {
                panic!("expected query: {sql}");
            };
            let before = query.clone();
            let mut expected = (*query).clone();
            expected.with = None;
            assert_eq!(clone_query_without_with(&query), expected, "{sql}");
            assert_eq!(query, before, "input must remain untouched: {sql}");
        }
    }
}

impl<'db> SqlSession<'db> {
    pub(crate) fn execute_raw_create_memory_index(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some(statement) = parse_raw_create_memory_index(sql)? else {
            return Ok(None);
        };
        let model = statement.model.ok_or_else(|| {
            SqlError::InvalidSql("CREATE MEMORY INDEX requires WITH (model = '...')".to_string())
        })?;
        let mode = match statement.mode.as_deref().unwrap_or("async") {
            "async" => MemoryIndexMode::Async,
            "sync" => MemoryIndexMode::Sync,
            other => {
                return Err(SqlError::InvalidSql(format!(
                    "unsupported memory index mode `{other}`; expected async or sync"
                )));
            }
        };
        self.db_mut()?.create_memory_index(
            &statement.index_name,
            &statement.table,
            &statement.field,
            &model,
            mode,
        )?;
        Ok(Some(SqlResult::command("CREATE MEMORY INDEX")))
    }

    pub(crate) fn execute_raw_create_memory_table(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some(statement) = parse_raw_create_memory_table(sql)? else {
            return Ok(None);
        };
        let model = self.default_memory_index_model()?;
        let result = self.execute_inner(&statement.table_sql)?;
        for field in statement.fields {
            let index_name = format!("idx_{}_{}_memory", statement.table, field);
            self.db_mut()?.create_memory_index(
                &index_name,
                &statement.table,
                &field,
                &model,
                MemoryIndexMode::Async,
            )?;
        }
        Ok(Some(result))
    }

    pub(crate) fn default_memory_index_model(&self) -> Result<String> {
        let models = self.db_ref().embedding_models();
        if models
            .iter()
            .any(|model| model.name == "embeddinggemma-300m")
        {
            return Ok("embeddinggemma-300m".to_string());
        }
        match models.as_slice() {
            [model] => Ok(model.name.clone()),
            [] => Err(SqlError::InvalidSql(
                "CREATE TABLE ... MEMORY requires a registered embedding model".to_string(),
            )),
            _ => Err(SqlError::InvalidSql(
                "CREATE TABLE ... MEMORY requires embeddinggemma-300m or exactly one registered embedding model"
                    .to_string(),
            )),
        }
    }

    pub(crate) fn execute_raw_process_memory_jobs(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some(limit) = parse_raw_process_memory_jobs(sql)? else {
            return Ok(None);
        };
        let report = self.db_mut()?.process_memory_index_jobs(limit)?;
        Ok(Some(SqlResult::new(
            vec!["bicdb_process_memory_jobs".to_string()],
            vec![vec![SqlValue::Int(report.processed as i64)]],
        )))
    }

    pub(crate) fn execute_raw_similar_to_select(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some(statement) = parse_raw_similar_to_select(sql)? else {
            return Ok(None);
        };
        let schema = load_schema(self.db_ref(), &statement.table)?
            .ok_or_else(|| SqlError::InvalidCollection(statement.table.clone()))?;
        let columns = if statement.columns.trim() == "*" {
            schema
                .columns
                .iter()
                .filter(|column| !column.hidden)
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
        } else {
            statement
                .columns
                .split(',')
                .map(str::trim)
                .filter(|column| !column.is_empty())
                .map(normalize_select_column_identifier)
                .collect::<Vec<_>>()
        };
        for column in &columns {
            ensure_schema_column(&statement.table, &schema, column)?;
        }
        let hits = self.db_ref().search_memory_index(
            &statement.table,
            &statement.field,
            &statement.query,
            statement.limit,
        )?;
        let rows = hits
            .iter()
            .map(|hit| {
                columns
                    .iter()
                    .map(|column| record_column_value(&hit.record, &schema, column))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let column_types = columns
            .iter()
            .map(|column| schema.column(column).map(|column| column.pg_type.clone()))
            .collect::<Vec<_>>();
        Ok(Some(
            SqlResult::new(columns, rows).with_column_types(column_types),
        ))
    }

    pub(crate) fn execute_raw_create_spatial_index(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some((index_name, table, field)) = parse_raw_create_spatial_index(sql)? else {
            return Ok(None);
        };
        self.require_table_ownership(&table, "create an index on it")?;
        let mut schema = load_schema(self.db_ref(), &table)?
            .ok_or_else(|| SqlError::InvalidCollection(table.clone()))?;
        if field.eq_ignore_ascii_case("geometry")
            && schema.column(&field).is_some_and(|column| !column.hidden)
        {
            return Err(SqlError::Unsupported(
                "BicDB Spatial indexes target the intrinsic geometry field, not a declared SQL column; use the PostgreSQL geometric type's supported index operator class"
                    .to_string(),
            ));
        }
        if schema.indexes.iter().any(|index| index.name == index_name) {
            return Err(SqlError::InvalidSql(format!(
                "index `{index_name}` already exists"
            )));
        }
        let index_field = spatial_index_field_from_name(&field)?;
        self.create_session_index(IndexDefinition {
            nulls_not_distinct: false,
            name: index_name.clone(),
            collection: table.clone(),
            fields: vec![index_field],
            unique: false,
            kind: IndexKind::Spatial,
            predicate: None,
            exclusion: None,
        })?;
        schema.indexes.push(IndexSchema {
            name: index_name,
            expression: format!("SPATIAL {field}"),
            source_expressions: Vec::new(),
            operator_classes: Vec::new(),
            internal_index_names: Vec::new(),
            collations: Vec::new(),
            unique: false,
            access_method: "gist".to_string(),
            metadata_only: false,
            predicate: None,
        });
        self.save_session_schema(&schema)?;
        Ok(Some(SqlResult::command("CREATE INDEX")))
    }

    /// `PACK SPATIAL INDEX <name> [USING HILBERT | STR]`: rebuild the index
    /// as a bulk-packed immutable node tree in the durable keyspace
    /// (`BicDb::pack_spatial_index_with_strategy`). Returns the pack report
    /// as one row. Requires an exclusive session (the pack holds `&mut
    /// BicDb`) and refuses to run inside an explicit transaction — the pack
    /// manages its own paged transactions and publishes atomically on its
    /// own.
    /// `VACUUM`: run the bounded, cursor-resumable dead-version reclaimer
    /// to completion synchronously, honoring the session's cancellation
    /// token between steps. Returns the totals as one row. The automatic
    /// background counterpart runs on the serve loop; this is the operator
    /// hammer.
    pub(crate) fn execute_raw_vacuum(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        if trimmed.eq_ignore_ascii_case("VACUUM") {
            // BicDB's vacuum is store-wide, so it cannot be scoped to the
            // tables a caller owns the way PostgreSQL's is. It is a heavy
            // global operation and takes the administrative gate.
            self.require_superuser_for_admin_operation("VACUUM")?;
        }
        if !trimmed.eq_ignore_ascii_case("VACUUM") {
            if trimmed.len() >= 6 && trimmed[..6].eq_ignore_ascii_case("VACUUM") {
                return Err(SqlError::Unsupported(
                    "only plain `VACUUM` is supported: BicDB vacuum is store-wide (per-table \
                     and FULL/ANALYZE options do not apply)"
                        .to_string(),
                ));
            }
            return Ok(None);
        }
        if self.tx.is_some() {
            return Err(SqlError::InvalidSql(
                "VACUUM cannot run inside a transaction".to_string(),
            ));
        }
        let cancellation = self.cancellation.clone();
        let report = self
            .db_ref()
            .run_vacuum_to_completion(&cancellation)
            .map_err(SqlError::from)?;
        Ok(Some(SqlResult::new(
            vec![
                "pages_scanned".to_string(),
                "versions_examined".to_string(),
                "versions_reclaimed".to_string(),
                "bytes_reclaimed".to_string(),
                "pages_freed".to_string(),
                "elapsed_ms".to_string(),
            ],
            vec![vec![
                SqlValue::Int(report.pages_scanned as i64),
                SqlValue::Int(report.versions_examined as i64),
                SqlValue::Int(report.versions_reclaimed as i64),
                SqlValue::Int(report.bytes_reclaimed as i64),
                SqlValue::Int(report.pages_freed as i64),
                SqlValue::Int(report.elapsed_millis as i64),
            ]],
        )))
    }

    /// `TRIM AUDIT HISTORY [ACKNOWLEDGED <offset>]`: drop superseded
    /// record-audit events (retention with winner preservation). Winning
    /// deletes are kept unless the caller supplies its peer-acknowledged
    /// horizon. Mesh-safe: retained local events freeze their origin
    /// positions first, so peer vectors survive the rewrite without
    /// resyncs.
    pub(crate) fn execute_raw_trim_audit_history(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_ascii_uppercase();
        if !upper.starts_with("TRIM AUDIT HISTORY") {
            return Ok(None);
        }
        // Destroys the forensic record, and with a crafted acknowledged offset
        // can drop winning deletes. Anti-forensics plus data loss.
        self.require_superuser_for_admin_operation("trim audit history")?;
        let rest = trimmed["TRIM AUDIT HISTORY".len()..].trim();
        let acknowledged = if rest.is_empty() {
            0
        } else {
            let upper_rest = rest.to_ascii_uppercase();
            let Some(value) = upper_rest.strip_prefix("ACKNOWLEDGED") else {
                return Err(SqlError::InvalidSql(
                    "usage: TRIM AUDIT HISTORY [ACKNOWLEDGED <offset>]".to_string(),
                ));
            };
            value.trim().parse::<u64>().map_err(|_| {
                SqlError::InvalidSql(
                    "TRIM AUDIT HISTORY ACKNOWLEDGED expects a non-negative integer".to_string(),
                )
            })?
        };
        if self.tx.is_some() {
            return Err(SqlError::InvalidSql(
                "TRIM AUDIT HISTORY cannot run inside a transaction".to_string(),
            ));
        }
        let report = self
            .db_mut()?
            .trim_event_horizon(bicdb_core::SyncCheckpoint::new(acknowledged))
            .map_err(SqlError::from)?;
        Ok(Some(SqlResult::new(
            vec![
                "events_before".to_string(),
                "events_after".to_string(),
                "superseded_dropped".to_string(),
                "deletes_dropped".to_string(),
                "bytes_before".to_string(),
                "bytes_after".to_string(),
            ],
            vec![vec![
                SqlValue::Int(report.events_before as i64),
                SqlValue::Int(report.events_after as i64),
                SqlValue::Int(report.superseded_dropped as i64),
                SqlValue::Int(report.deletes_dropped as i64),
                SqlValue::Int(report.bytes_before as i64),
                SqlValue::Int(report.bytes_after as i64),
            ]],
        )))
    }

    /// `EXPLAIN MATERIALIZED AGGREGATE <name>` and
    /// `RECONCILE MATERIALIZED AGGREGATE <name>`.
    ///
    /// EXPLAIN answers "what would this grain cost" before anything is
    /// materialized — the measured extremes on a million rows were a 7,700x
    /// reduction and a 1.18x one from grain choice alone, which is why this
    /// exists rather than letting users discover it afterwards.
    ///
    /// RECONCILE promotes the correctness check from a test helper to an
    /// operator command: it recomputes from the base table and reports drift.
    pub(crate) fn execute_raw_projection_admin(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_ascii_uppercase();
        let (explain, rest) =
            if let Some(rest) = upper.strip_prefix("EXPLAIN MATERIALIZED AGGREGATE") {
                (true, &trimmed[trimmed.len() - rest.len()..])
            } else if let Some(rest) = upper.strip_prefix("RECONCILE MATERIALIZED AGGREGATE") {
                (false, &trimmed[trimmed.len() - rest.len()..])
            } else {
                return Ok(None);
            };
        let name = rest.trim().trim_matches('"');
        if name.is_empty() {
            return Err(SqlError::InvalidSql(
                "MATERIALIZED AGGREGATE requires a projection name".to_string(),
            ));
        }
        let directory = self
            .db_ref()
            .data_path()
            .join(bicdb_core::aggregate_projection::PROJECTIONS_DIR);
        let mut projection =
            bicdb_core::aggregate_projection::AggregateProjection::load(&directory, name)
                .map_err(SqlError::from)?
                .ok_or_else(|| {
                    SqlError::InvalidSql(format!("materialized aggregate `{name}` does not exist"))
                })?;
        // EXPLAIN and RECONCILE both read the cube's SOURCE table — reconcile
        // recomputes from it — so they take the same ownership gate the other
        // cube commands already use.
        self.require_table_ownership(projection.collection_name(), "inspect a cube on it")?;

        if explain {
            let estimate = projection.estimate(self.db_ref()).map_err(SqlError::from)?;
            let warning = if estimate.warnings.is_empty() {
                "none".to_string()
            } else {
                estimate.warnings.join(" | ")
            };
            return Ok(Some(SqlResult::new(
                vec![
                    "source_rows".into(),
                    "estimated_cells".into(),
                    "reduction".into(),
                    "dimension_cardinality".into(),
                    "input_state_bytes".into(),
                    "cell_bytes".into(),
                    "warnings".into(),
                ],
                vec![vec![
                    SqlValue::Int(estimate.source_rows as i64),
                    SqlValue::Int(estimate.estimated_cells as i64),
                    SqlValue::Float((estimate.reduction * 100.0).round() / 100.0),
                    SqlValue::String(
                        estimate
                            .dimension_cardinality
                            .iter()
                            .map(|count| count.to_string())
                            .collect::<Vec<_>>()
                            .join(", "),
                    ),
                    SqlValue::Int(estimate.input_state_bytes as i64),
                    SqlValue::Int(estimate.cell_bytes as i64),
                    SqlValue::String(warning),
                ]],
            )));
        }

        // RECONCILE: catch up first so drift reflects the current stream, not
        // a stale watermark.
        projection.catch_up(self.db_ref()).map_err(SqlError::from)?;
        let drift = projection
            .reconcile(self.db_ref())
            .map_err(SqlError::from)?;
        let status = if drift.is_clean() { "EXACT" } else { "DRIFT" };
        Ok(Some(SqlResult::new(
            vec![
                "cells_checked".into(),
                "missing".into(),
                "extra".into(),
                "measure_mismatches".into(),
                "status".into(),
            ],
            vec![vec![
                SqlValue::Int(drift.cells_compared as i64),
                SqlValue::Int(drift.cells_only_authoritative as i64),
                SqlValue::Int(drift.cells_only_incremental as i64),
                SqlValue::Int(drift.cells_differing as i64),
                SqlValue::String(status.to_string()),
            ]],
        )))
    }

    /// `ROLLUP MATERIALIZED AGGREGATE <name> TO (<dim> <level>, ...)`
    ///
    /// A rollup is an engine operation rather than a `GROUP BY` because
    /// `COUNT(DISTINCT)` and percentiles do not add: two cells that share a
    /// value would double-count it. Rolling up merges their sketches.
    pub(crate) fn execute_raw_projection_rollup(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_ascii_uppercase();
        let Some(rest) = upper.strip_prefix("ROLLUP MATERIALIZED AGGREGATE") else {
            return Ok(None);
        };
        let rest = &trimmed[trimmed.len() - rest.len()..];
        let (name, spec) = match rest.to_ascii_uppercase().find(" TO ") {
            Some(at) => (rest[..at].trim(), rest[at + 4..].trim()),
            None => (rest.trim(), ""),
        };
        let name = name.trim().trim_matches('"');
        if name.is_empty() {
            return Err(SqlError::InvalidSql(
                "ROLLUP MATERIALIZED AGGREGATE requires a projection name".to_string(),
            ));
        }
        let directory = self
            .db_ref()
            .data_path()
            .join(bicdb_core::aggregate_projection::PROJECTIONS_DIR);
        let mut projection =
            bicdb_core::aggregate_projection::AggregateProjection::load(&directory, name)
                .map_err(SqlError::from)?
                .ok_or_else(|| {
                    SqlError::InvalidSql(format!("materialized aggregate `{name}` does not exist"))
                })?;
        // Read current state, not the watermark the snapshot was taken at.
        projection.catch_up(self.db_ref()).map_err(SqlError::from)?;

        let levels = parse_rollup_levels(spec, projection.dimension_names())?;
        let rolled = projection.rollup_with(|index, value| match levels.get(index) {
            Some(RollupSpec::Level(level)) => level.coarsen_public(value),
            // H3 parents need `h3o`, which lives in this crate rather than in
            // bicdb-core — hence the caller-supplied coarsening hook.
            Some(RollupSpec::H3(resolution)) => Some(h3_parent(value, *resolution)),
            None => Some(value.clone()),
        });

        // WHOLE removes a dimension from the key, so it must also be removed
        // from the header — otherwise the columns and the rows disagree.
        let kept: Vec<String> = projection
            .dimension_names()
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                !matches!(
                    levels.get(*index),
                    Some(RollupSpec::Level(
                        bicdb_core::aggregate_projection::RollupLevel::Whole
                    ))
                )
            })
            .map(|(_, name)| name.clone())
            .collect();
        Ok(Some(render_aggregate_cells(
            &kept,
            projection.measure_names(),
            projection.sketch_specs(),
            rolled
                .into_iter()
                .map(|(key, cell)| (key, cell.state, cell.sketches)),
        )))
    }

    /// `CREATE CUBE` / `DROP CUBE` / `REFRESH CUBE`.
    ///
    /// The DDL the campaign deliberately withheld until the engine underneath
    /// it was real, so the syntax describes something that exists rather than
    /// something aspirational.
    /// A cube identifier must survive becoming a path component.
    ///
    /// Delegates to the core validator so the SQL layer and every other
    /// caller enforce one alphabet; the SQL layer merely reports it as a
    /// statement error.
    pub(crate) fn validate_cube_name(&self, name: &str) -> Result<()> {
        bicdb_core::aggregate_projection::validate_projection_name(name).map_err(SqlError::from)
    }

    pub(crate) fn execute_raw_cube_ddl(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_ascii_uppercase();
        let directory = self
            .db_ref()
            .data_path()
            .join(bicdb_core::aggregate_projection::PROJECTIONS_DIR);

        if let Some(rest) = upper.strip_prefix("DROP CUBE") {
            let rest = &trimmed[trimmed.len() - rest.len()..];
            let if_exists = rest
                .to_ascii_uppercase()
                .trim_start()
                .starts_with("IF EXISTS");
            let name = if if_exists {
                rest.trim()[9..].trim()
            } else {
                rest.trim()
            }
            .trim_matches('"');
            // Validate BEFORE any path is built: an unchecked name here was
            // a cross-tenant `remove_dir_all` primitive.
            self.validate_cube_name(name)?;
            let path = directory.join(format!("{name}.projection"));
            if !path.exists() {
                if if_exists {
                    return Ok(Some(SqlResult::command("DROP CUBE")));
                }
                return Err(SqlError::InvalidSql(format!(
                    "cube `{name}` does not exist"
                )));
            }
            // A cube is derived from its source table, so dropping it
            // requires the authority to alter that table.
            if let Some(projection) =
                bicdb_core::aggregate_projection::AggregateProjection::load(&directory, name)
                    .map_err(SqlError::from)?
            {
                self.require_table_ownership(projection.collection_name(), "drop a cube on it")?;
            }
            std::fs::remove_dir_all(&path).map_err(|error| {
                SqlError::InvalidSql(format!("could not drop cube `{name}`: {error}"))
            })?;
            return Ok(Some(SqlResult::command("DROP CUBE")));
        }

        if let Some(rest) = upper.strip_prefix("REFRESH CUBE") {
            let name = trimmed[trimmed.len() - rest.len()..]
                .trim()
                .trim_matches('"');
            self.validate_cube_name(name)?;
            let mut projection =
                bicdb_core::aggregate_projection::AggregateProjection::load(&directory, name)
                    .map_err(SqlError::from)?
                    .ok_or_else(|| SqlError::InvalidSql(format!("cube `{name}` does not exist")))?;
            self.require_table_ownership(projection.collection_name(), "refresh a cube on it")?;
            let applied = projection.catch_up(self.db_ref()).map_err(SqlError::from)?;
            projection.save(&directory, true).map_err(SqlError::from)?;
            return Ok(Some(SqlResult::new(
                vec!["events_applied".into(), "cells".into()],
                vec![vec![
                    SqlValue::Int(applied as i64),
                    SqlValue::Int(projection.cell_count() as i64),
                ]],
            )));
        }

        let Some(definition) = parse_create_cube(trimmed)? else {
            return Ok(None);
        };
        self.validate_cube_name(&definition.name)?;
        self.require_table_ownership(&definition.collection, "create a cube on it")?;
        if directory
            .join(format!("{}.projection", definition.name))
            .exists()
        {
            return Err(SqlError::InvalidSql(format!(
                "cube `{}` already exists",
                definition.name
            )));
        }
        let projection = bicdb_core::aggregate_projection::AggregateProjection::new(
            definition.name.clone(),
            definition.collection.clone(),
            definition.dimensions.clone(),
            definition.measures.clone(),
        )
        .map_err(SqlError::from)?
        .with_sketches(definition.sketches.clone())
        .map_err(SqlError::from)?;

        // Estimate BEFORE materializing. A cube whose grain is nearly as fine
        // as the table is a second copy of the table; building it and then
        // reporting the memory it took is not a warning, it is an incident.
        let estimate = projection.estimate(self.db_ref()).map_err(SqlError::from)?;
        if !estimate.warnings.is_empty() && !definition.force {
            return Err(SqlError::InvalidSql(format!(
                "refusing to build cube `{}`: {}. Re-run with WITH FORCE to build it anyway.",
                definition.name,
                estimate.warnings.join(" | ")
            )));
        }

        let mut projection = projection;
        projection
            .rebuild_from_base(self.db_ref())
            .map_err(SqlError::from)?;
        std::fs::create_dir_all(&directory).map_err(|error| {
            SqlError::InvalidSql(format!(
                "could not create the projections directory: {error}"
            ))
        })?;
        projection.save(&directory, true).map_err(SqlError::from)?;
        Ok(Some(SqlResult::new(
            // Report the relation name explicitly. Projections live in their
            // own namespace ON PURPOSE, so a cube can never shadow a real
            // table; telling the caller where it landed is the right way to
            // close that ergonomic gap, rather than relaxing the rule.
            vec![
                "cube".into(),
                "relation".into(),
                "cells".into(),
                "source_rows".into(),
                "reduction".into(),
            ],
            vec![vec![
                SqlValue::String(definition.name.clone()),
                SqlValue::String(format!(
                    "{}{}",
                    crate::select_exec::PROJECTION_RELATION_PREFIX,
                    definition.name
                )),
                SqlValue::Int(projection.cell_count() as i64),
                SqlValue::Int(estimate.source_rows as i64),
                SqlValue::Float((estimate.reduction * 100.0).round() / 100.0),
            ]],
        )))
    }

    /// `MERGE MATERIALIZED AGGREGATE <a>, <b>, ...`
    ///
    /// Combines independently-maintained shard projections. Additive measures
    /// add; sketches MERGE, so a value two shards both saw is counted once.
    pub(crate) fn execute_raw_projection_merge(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_ascii_uppercase();
        let Some(rest) = upper.strip_prefix("MERGE MATERIALIZED AGGREGATE") else {
            return Ok(None);
        };
        let rest = &trimmed[trimmed.len() - rest.len()..];
        let names: Vec<&str> = rest
            .split(',')
            .map(|name| name.trim().trim_matches('"'))
            .filter(|name| !name.is_empty())
            .collect();
        if names.is_empty() {
            return Err(SqlError::InvalidSql(
                "MERGE MATERIALIZED AGGREGATE requires at least one projection name".to_string(),
            ));
        }
        let directory = self
            .db_ref()
            .data_path()
            .join(bicdb_core::aggregate_projection::PROJECTIONS_DIR);
        let mut partials = Vec::with_capacity(names.len());
        for name in &names {
            let mut projection =
                bicdb_core::aggregate_projection::AggregateProjection::load(&directory, name)
                    .map_err(SqlError::from)?
                    .ok_or_else(|| {
                        SqlError::InvalidSql(format!(
                            "materialized aggregate `{name}` does not exist"
                        ))
                    })?;
            self.require_table_ownership(projection.collection_name(), "merge a cube on it")?;
            projection.catch_up(self.db_ref()).map_err(SqlError::from)?;
            partials.push(projection.export_partial());
        }
        let merged =
            bicdb_core::aggregate_projection::AggregateProjection::merge_partials(&partials)
                .map_err(SqlError::from)?;
        Ok(Some(render_aggregate_cells(
            &merged.dimensions,
            &merged.measures,
            &merged.sketches,
            merged.cells,
        )))
    }

    pub(crate) fn execute_raw_pack_spatial_index(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some((index_name, strategy)) = parse_raw_pack_spatial_index(sql)? else {
            return Ok(None);
        };
        let strategy = match strategy.as_deref() {
            None | Some("HILBERT") => bicdb_core::SpatialPackStrategy::Hilbert,
            Some("STR") => bicdb_core::SpatialPackStrategy::Str,
            Some(other) => {
                return Err(SqlError::InvalidSql(format!(
                    "PACK SPATIAL INDEX supports USING HILBERT or USING STR, not `{other}`"
                )));
            }
        };
        if self.in_transaction() {
            return Err(SqlError::Unsupported(
                "PACK SPATIAL INDEX cannot run inside a transaction".to_string(),
            ));
        }
        // The rest of the index DDL surface resolves names
        // case-insensitively (drop_session_index); match it here rather
        // than surface a "not found" for a name DROP would accept.
        let index_name = self
            .db_ref()
            .index_definitions()
            .into_iter()
            .map(|definition| definition.name)
            .find(|candidate| candidate.eq_ignore_ascii_case(&index_name))
            .unwrap_or(index_name);
        // An index belongs to its table, so the table's authority governs
        // repacking it — the same rule DROP INDEX follows.
        if let Some(table) = self.table_owning_index(&index_name)? {
            self.require_table_ownership(&table, "pack an index on it")?;
        }
        let report = self
            .db_mut()?
            .pack_spatial_index_with_strategy(&index_name, strategy)?;
        Ok(Some(SqlResult::new(
            vec![
                "index_name".to_string(),
                "strategy".to_string(),
                "generation".to_string(),
                "entry_count".to_string(),
                "node_count".to_string(),
                "height".to_string(),
            ],
            vec![vec![
                SqlValue::String(report.index_name),
                SqlValue::String(report.strategy.label().to_string()),
                SqlValue::Int(report.generation as i64),
                SqlValue::Int(report.entry_count as i64),
                SqlValue::Int(report.node_count as i64),
                SqlValue::Int(i64::from(report.height)),
            ]],
        )))
    }

    pub(crate) fn execute_raw_create_index_on_only(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some(index) = parse_raw_create_index_on_only(sql)? else {
            return Ok(None);
        };
        self.require_table_ownership(&index.table, "create an index on it")?;
        let mut schema = load_schema(self.db_ref(), &index.table)?
            .ok_or_else(|| SqlError::InvalidCollection(index.table.clone()))?;
        if schema
            .indexes
            .iter()
            .any(|candidate| candidate.name.eq_ignore_ascii_case(&index.name))
        {
            if index.if_not_exists {
                return Ok(Some(SqlResult::command("CREATE INDEX")));
            }
            return Err(SqlError::InvalidSql(format!(
                "index `{}` already exists",
                index.name
            )));
        }
        schema.indexes.push(IndexSchema {
            name: index.name,
            expression: index.expression,
            source_expressions: Vec::new(),
            operator_classes: Vec::new(),
            internal_index_names: Vec::new(),
            collations: Vec::new(),
            unique: index.unique,
            access_method: index.access_method,
            metadata_only: true,
            predicate: None,
        });
        self.save_session_schema(&schema)?;
        Ok(Some(SqlResult::command("CREATE INDEX")))
    }

    pub(crate) fn execute_query(&mut self, query: &Query) -> Result<SqlResult> {
        self.ensure_read_only_query(query)?;
        // Every relation of the FROM tree needs SELECT before any row is read,
        // whether or not the executor ends up scanning it.
        self.sql_engine().require_from_relations_readable(query)?;
        validate_comparison_signatures(&self.sql_engine(), ComparisonStatement::Query(query))?;
        if self.oracle_autonomous && query_has_row_locks(query) {
            // A locking read starts an Oracle transaction even if SELECT INTO
            // subsequently raises NO_DATA_FOUND and its handler catches it.
            self.oracle_autonomous_active = true;
            self.oracle_autonomous_lock_epoch = self.oracle_autonomous_lock_epoch.wrapping_add(1);
        }
        let previous = std::mem::replace(&mut self.in_routine_expression, false);
        let mut result = self.execute_query_in_sql(query);
        if result
            .as_ref()
            .is_err_and(|error| error.to_string() == super::row_routine_write_error().to_string())
            && !self.row_routine_call
            && !self
                .session_gucs
                .get("bicdb.sql_profile")
                .is_some_and(|profile| oracle::is_oracle_profile(profile))
            && !oracle::preview_routine_language_enabled(self.db_ref())?
        {
            if let Some(rows) = self.execute_writing_projection(query)? {
                result = Ok(rows);
            }
        }
        self.in_routine_expression = previous;
        result
    }

    /// `SELECT f(..) FROM one_relation [WHERE ..]` where `f` modifies data:
    /// the per-row expression host only reads, so read the source rows first
    /// and evaluate the select list once per row in this session, as
    /// PostgreSQL evaluates a volatile select list once per row.
    fn execute_writing_projection(&mut self, query: &Query) -> Result<Option<SqlResult>> {
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(None);
        };
        if query.with.is_some()
            || query.order_by.is_some()
            || query.limit_clause.is_some()
            || query.fetch.is_some()
            || !query.locks.is_empty()
            || select.distinct.is_some()
            || select.having.is_some()
            || !matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if exprs.is_empty())
            || select.into.is_some()
        {
            return Ok(None);
        }
        let [TableWithJoins { relation, joins }] = select.from.as_slice() else {
            return Ok(None);
        };
        let TableFactor::Table { name, alias, .. } = relation else {
            return Ok(None);
        };
        if !joins.is_empty()
            || select.projection.iter().any(|item| {
                !matches!(
                    item,
                    SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. }
                )
            })
        {
            return Ok(None);
        }
        let qualifier = alias
            .as_ref()
            .map(|alias| ident_value(&alias.name))
            .or_else(|| object_name_parts(name).last().cloned())
            .unwrap_or_default();
        // Built as ASTs, never re-rendered text: rendering is not a faithful
        // round trip for every literal.
        let mut source_query = query.clone();
        if let SetExpr::Select(source_select) = source_query.body.as_mut() {
            source_select.projection = vec![SelectItem::Wildcard(
                sqlparser::ast::WildcardAdditionalOptions::default(),
            )];
        }
        let source = self.execute_query_in_sql(&source_query)?;
        let mut output: Option<SqlResult> = None;
        for row in &source.rows {
            let mut projection = select.projection.clone();
            let _ = sqlparser::ast::visit_expressions_mut(&mut projection, |expr| {
                let column = match expr {
                    Expr::Identifier(ident) => Some(ident_value(ident)),
                    Expr::CompoundIdentifier(parts)
                        if parts.len() == 2 && ident_value(&parts[0]) == qualifier =>
                    {
                        Some(ident_value(&parts[1]))
                    }
                    _ => None,
                };
                if let Some(index) = column.and_then(|column| {
                    source
                        .columns
                        .iter()
                        .position(|name| name.eq_ignore_ascii_case(&column))
                }) {
                    *expr = sql_value_literal_expr(&row[index]);
                }
                ControlFlow::<()>::Continue(())
            });
            let mut row_query = query.clone();
            if let SetExpr::Select(row_select) = row_query.body.as_mut() {
                row_select.projection = projection;
                row_select.from.clear();
                row_select.selection = None;
            }
            let produced = self.execute_query_in_sql(&row_query)?;
            match &mut output {
                Some(output) => output.rows.extend(produced.rows),
                None => output = Some(produced),
            }
        }
        Ok(Some(output.unwrap_or_else(|| SqlResult::empty(Vec::new()))))
    }

    fn execute_query_in_sql(&mut self, query: &Query) -> Result<SqlResult> {
        if let Some(rewritten) = normalize_fetch_clause(query)? {
            return self.execute_temporary_query(&rewritten);
        }
        if let Some(rewritten) = rewrite_ordinal_order_by(query, &|relation| {
            table_factor_column_names(self.db_ref(), relation)
        }) {
            return self.execute_temporary_query(&rewritten);
        }
        if let Some(rewritten) = rewrite_row_type_constructs(self.db_ref(), query)? {
            return self.execute_temporary_query(&rewritten);
        }
        if let Some(folded) = fold_statement_constants(query, &self.routine_vars) {
            return self.execute_temporary_query(&folded);
        }
        // Own the entire statement, including CTEs and its final DML. A
        // correlated function must never commit independently of that DML.
        if (!self.row_routine_call && self.query_has_postgres_scalar_routines(query)?)
            || query_may_write(query)
                && (self.tx.is_some() || self.mutation_needs_atomic_effects(query)?)
        {
            return self.with_statement_transaction(|session| session.execute_query_inner(query));
        }
        if self.row_routine_call && query_has_row_locks(query) {
            return Err(super::row_routine_write_error());
        }
        // Read-only row hosts already borrow the outer statement's snapshot.
        // A nested table function must not start/commit an independent unit.
        if !self.row_routine_call
            && self.tx.is_none()
            && (query_has_row_locks(query) || self.query_has_plpgsql_set_functions(query)?)
        {
            self.execute("BEGIN")?;
            let result = self.execute_query_inner(query).and_then(|result| {
                self.fire_deferred_row_triggers()?;
                Ok(result)
            });
            return match result {
                Ok(result) => {
                    if !self.defer_commit {
                        self.commit()?;
                    }
                    Ok(result)
                }
                Err(error) => {
                    self.rollback_current_transaction()?;
                    Err(error)
                }
            };
        }
        if self.tx.is_some() && query_has_row_locks(query) {
            // READ COMMITTED FOR UPDATE: after waiting for a row another
            // transaction changed, lock and return its latest version.
            let mut attempts = 0;
            loop {
                match self.execute_query_inner(query) {
                    Err(error)
                        if crate::engine::is_read_committed_lock_refresh(&error)
                            && attempts < 1000 =>
                    {
                        attempts += 1;
                        if let Some(tx) = self.tx.as_mut() {
                            tx.refresh_read_committed_snapshot()?;
                        }
                        if attempts > 1 {
                            std::thread::yield_now();
                        }
                    }
                    result => return result,
                }
            }
        }
        self.execute_query_inner(query)
    }

    fn execute_query_inner(&mut self, query: &Query) -> Result<SqlResult> {
        // Resolve named routine variables and positional parameters in every
        // nested row-count clause before CTE materialization.
        if let Some(bound) = self.bind_limit_clause_placeholders(query) {
            return self.execute_temporary_query(&bound);
        }
        if let Some(with) = &query.with {
            if with.recursive {
                if let Some(update) = set_expr_update(query.body.as_ref()) {
                    if let Some(result) =
                        self.execute_odoo_parent_store_update_with_recursive_cte(update, with)?
                    {
                        return Ok(result);
                    }
                }
            }
            if with.recursive {
                return self.sql_engine().execute_query(query);
            }
            let ctes = self.materialize_ctes(with)?;
            if let Some(insert) = set_expr_insert(query.body.as_ref()) {
                return self.execute_insert_with_ctes(insert, ctes);
            }
            if let Some(update) = set_expr_update(query.body.as_ref()) {
                return self.execute_update_with_ctes(update, ctes);
            }
            if let Some(delete) = set_expr_delete(query.body.as_ref()) {
                return self.execute_delete_with_ctes(delete, ctes);
            }
            let body = clone_query_without_with(query);
            if let Some(result) = self.try_execute_set_function_query(&body, ctes.clone())? {
                return Ok(result);
            }
            return self.sql_engine_with_ctes(ctes).execute_query(&body);
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return self.sql_engine().execute_query(query);
        };
        if select.from.is_empty() && select_has_window_functions(select, query)? {
            return self.sql_engine().execute_query(query);
        }
        if select.from.is_empty() {
            return self.execute_session_select_without_from(select, query);
        }
        if let Some(result) = self.try_execute_scalar_derived_command(query)? {
            return Ok(result);
        }
        if let Some(result) = self.try_execute_set_function_query(query, BTreeMap::new())? {
            return Ok(result);
        }
        self.sql_engine().execute_query(query)
    }

    /// Rewritten nodes have no stable identity in a stored routine. Their
    /// addresses must not enter expression-type or routine plan memos, even
    /// when a rewrite recursively reaches another routine or query rewrite.
    fn execute_temporary_query(&mut self, query: &Query) -> Result<SqlResult> {
        let _scope = crate::eval::suspend_expr_type_scope();
        let previous_ir = self.current_routine_ir.take();
        let previous_owned = std::mem::replace(&mut self.ir_owned_statement, false);
        let result = self.execute_query(query);
        self.ir_owned_statement = previous_owned;
        self.current_routine_ir = previous_ir;
        result
    }

    /// Bind row-count expressions at every query level before a CTE or
    /// subquery reaches the engine's constant LIMIT/OFFSET evaluator. Other
    /// expressions keep normal column/variable resolution and the stored IR
    /// remains unchanged between calls.
    fn bind_limit_clause_placeholders(&self, query: &Query) -> Option<Query> {
        use sqlparser::ast::{
            visit_expressions, visit_expressions_mut, LimitClause, Visit, VisitMut, Visitor,
            VisitorMut,
        };
        use std::ops::ControlFlow;
        let vars = &self.routine_vars;
        if vars.is_empty() {
            return None;
        }
        fn value(expr: &Expr, vars: &BTreeMap<String, SqlValue>) -> Option<SqlValue> {
            match expr {
                Expr::Identifier(ident) => crate::select_exec::routine_var_from_ident(vars, ident),
                Expr::Value(value) => match &value.value {
                    Value::Placeholder(name) => {
                        crate::select_exec::routine_var_from_name(vars, name)
                    }
                    _ => None,
                },
                _ => None,
            }
        }
        fn count_expressions(query: &Query) -> Vec<&Expr> {
            let mut counts = Vec::new();
            match &query.limit_clause {
                Some(LimitClause::LimitOffset { limit, offset, .. }) => {
                    counts.extend(limit.as_ref());
                    counts.extend(offset.as_ref().map(|o| &o.value));
                }
                Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
                    counts.push(offset);
                    counts.push(limit);
                }
                None => {}
            }
            counts.extend(query.fetch.as_ref().and_then(|f| f.quantity.as_ref()));
            counts
        }
        struct Finder<'a>(&'a BTreeMap<String, SqlValue>);
        impl Visitor for Finder<'_> {
            type Break = ();
            fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
                let found = count_expressions(query).into_iter().any(|count| {
                    visit_expressions(count, |expr| {
                        if value(expr, self.0).is_some() {
                            ControlFlow::Break(())
                        } else {
                            ControlFlow::Continue(())
                        }
                    })
                    .is_break()
                });
                if found {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            }
        }
        if query.visit(&mut Finder(vars)).is_continue() {
            return None;
        }
        struct Binder<'a>(&'a BTreeMap<String, SqlValue>);
        impl VisitorMut for Binder<'_> {
            type Break = ();
            fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
                let mut bind = |expr: &mut Expr| {
                    if let Some(bound) = value(expr, self.0) {
                        *expr = sql_value_to_literal_expr(&bound);
                    }
                    ControlFlow::<()>::Continue(())
                };
                match &mut query.limit_clause {
                    Some(LimitClause::LimitOffset { limit, offset, .. }) => {
                        let _ = visit_expressions_mut(limit, &mut bind);
                        if let Some(offset) = offset {
                            let _ = visit_expressions_mut(&mut offset.value, &mut bind);
                        }
                    }
                    Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
                        let _ = visit_expressions_mut(offset, &mut bind);
                        let _ = visit_expressions_mut(limit, &mut bind);
                    }
                    None => {}
                }
                if let Some(fetch) = &mut query.fetch {
                    let _ = visit_expressions_mut(&mut fetch.quantity, &mut bind);
                }
                ControlFlow::Continue(())
            }
        }
        let mut rewritten = query.clone();
        let _ = VisitMut::visit(&mut rewritten, &mut Binder(vars));
        Some(rewritten)
    }

    pub(crate) fn execute_odoo_parent_store_update_with_recursive_cte(
        &mut self,
        update: &sqlparser::ast::Update,
        with: &With,
    ) -> Result<Option<SqlResult>> {
        if with.cte_tables.len() != 1 {
            return Ok(None);
        }
        let cte_name = with.cte_tables[0].alias.name.value.as_str();
        if !cte_name.eq_ignore_ascii_case("__parent_store_compute") {
            return Ok(None);
        }
        let (table, _target_alias) = table_with_joins_name_and_alias(&update.table)?;
        let table = resolve_session_relation_name_if_exists(self.db_ref(), &table);
        let Some(schema) = load_schema(self.db_ref(), &table)? else {
            return Ok(None);
        };
        if schema.column("id").is_none()
            || schema.column("parent_id").is_none()
            || schema.column("parent_path").is_none()
        {
            return Ok(None);
        }

        let records = self.sql_engine().scan_records(&table)?;
        let mut parent_by_id = BTreeMap::<i64, Option<i64>>::new();
        for record in &records {
            let Some(id) = sql_value_i64(&record_column_value(record, &schema, "id")) else {
                continue;
            };
            let parent_id = match record_column_value(record, &schema, "parent_id") {
                SqlValue::Null => None,
                value => sql_value_i64(&value),
            };
            parent_by_id.insert(id, parent_id);
        }

        let mut path_cache = BTreeMap::<i64, String>::new();
        for id in parent_by_id.keys().copied().collect::<Vec<_>>() {
            let mut visiting = BTreeSet::new();
            let path = parent_store_path_for_id(id, &parent_by_id, &mut path_cache, &mut visiting)?;
            path_cache.insert(id, path);
        }

        let mut updated = 0usize;
        for (idx, record) in records.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.cancellation.check()?;
            }
            let Some(id) = sql_value_i64(&record_column_value(&record, &schema, "id")) else {
                continue;
            };
            let Some(path) = path_cache.get(&id) else {
                continue;
            };
            let mut record = record.as_ref().clone();
            set_record_column(
                &mut record,
                Some(&schema),
                "parent_path",
                SqlValue::String(path.clone()),
            )?;
            self.update_session_record(&table, record)?;
            updated += 1;
        }
        Ok(Some(SqlResult::command(format!("UPDATE {updated}"))))
    }

    pub(crate) fn materialize_ctes(&mut self, with: &With) -> Result<BTreeMap<String, CteResult>> {
        if with.recursive {
            return Err(SqlError::Unsupported(
                "recursive CTEs are not supported".to_string(),
            ));
        }
        let mut ctes = BTreeMap::new();
        for cte in &with.cte_tables {
            let name = cte.alias.name.value.clone();
            let key = cte_key(&name);
            let result = if let Some(insert) = set_expr_insert(cte.query.body.as_ref()) {
                self.execute_insert_with_ctes(insert, ctes.clone())?
            } else if let Some(update) = set_expr_update(cte.query.body.as_ref()) {
                self.execute_update_with_ctes(update, ctes.clone())?
            } else if let Some(delete) = set_expr_delete(cte.query.body.as_ref()) {
                self.execute_delete_with_ctes(delete, ctes.clone())?
            } else if let Some(result) =
                self.try_execute_set_function_query(&cte.query, ctes.clone())?
            {
                result
            } else {
                let engine = self.sql_engine_with_ctes(ctes.clone());
                engine.execute_query(&cte.query)?
            };
            let columns = cte_columns(&name, &cte.alias.columns, &result.columns)?;
            let column_types = result.column_types.clone();
            ctes.insert(
                key,
                CteResult::new(name, columns, result.rows).with_column_types(column_types),
            );
        }
        Ok(ctes)
    }

    pub(crate) fn execute_session_select_without_from(
        &mut self,
        select: &Select,
        query: &Query,
    ) -> Result<SqlResult> {
        if let Some(mut result) = self
            .sql_engine()
            .execute_projection_array_set_functions(select)?
        {
            apply_row_limit(&mut result.rows, query)?;
            return Ok(result);
        }
        if let Some(result) = self
            .sql_engine()
            .execute_projection_json_set_function(select)?
        {
            let mut result = result;
            apply_row_limit(&mut result.rows, query)?;
            return Ok(result);
        }
        let mut candidates = vec![()];
        apply_row_limit(&mut candidates, query)?;
        let include_row = !candidates.is_empty()
            && match &select.selection {
                Some(selection) => {
                    sql_value_truth(self.eval_session_expr(selection)?)?.unwrap_or(false)
                }
                None => true,
            };
        let mut columns = Vec::new();
        let mut column_types = Vec::new();
        let mut row = Vec::new();
        for item in &select.projection {
            let (expr, alias) = select_item_expr_and_alias(item)?;
            columns.push(alias.unwrap_or_else(|| select_expr_column_name(expr)));
            validate_common_type_expr(self.db_ref(), expr)?;
            column_types.push(projected_expr_pg_type_with_db(self.db_ref(), expr));
            if include_row {
                row.push(self.eval_session_expr(expr)?);
            }
        }
        let rows = if include_row { vec![row] } else { Vec::new() };
        validate_integer_result_types(SqlResult::new(columns, rows).with_column_types(column_types))
    }

    pub(crate) fn execute_raw_create_user(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_ascii_uppercase();
        if !upper.starts_with("CREATE USER ") {
            return Ok(None);
        }
        self.require_role_management_privilege("create roles")?;
        let rest = trimmed["CREATE USER ".len()..].trim();
        let Some(first) = rest.split_whitespace().next() else {
            return Err(SqlError::InvalidSql(
                "CREATE USER requires a role name".to_string(),
            ));
        };
        let name = normalize_role_name(first.trim_matches('"'));
        let mut role = default_role_schema(&name);
        role.can_login = true;
        let options = rest[first.len()..].trim().to_ascii_uppercase();
        role.superuser = options.contains(" SUPERUSER") || options == "SUPERUSER";
        role.create_db = options.contains(" CREATEDB") || options == "CREATEDB";
        role.create_role = options.contains(" CREATEROLE") || options == "CREATEROLE";
        role.password_set = options.contains(" PASSWORD ");
        if role.superuser || role.create_role {
            self.require_superuser_for_privileged_role_attributes("create a role with")?;
        }
        create_role_record(self.db_mut()?, role, false)?;
        Ok(Some(SqlResult::command("CREATE ROLE")))
    }

    pub(crate) fn execute_raw_database_ddl(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let Some(command) = parse_database_ddl(sql)? else {
            return Ok(None);
        };
        match command.clone() {
            DatabaseDdl::Create {
                template: Some(template),
                ..
            } => Err(SqlError::Unsupported(format!(
                "CREATE DATABASE ... TEMPLATE {template} (a branch) needs a server in cluster mode (bicdb serve --cluster)"
            ))),
            DatabaseDdl::Create { name, owner, .. } => {
                let roles = list_roles(self.db_ref())?;
                let current_role = current_user_from_gucs(&self.session_gucs);
                let permitted = current_role.eq_ignore_ascii_case(&current_role_name())
                    || roles.iter().any(|role| {
                        role.name.eq_ignore_ascii_case(&current_role)
                            && (role.superuser || role.create_db)
                    });
                if !permitted {
                    return Err(BicDbError::Authorization(format!(
                        "permission denied to create database \"{name}\""
                    ))
                    .into());
                }
                let owner = owner.unwrap_or(current_role);
                ensure_known_role(&roles, &owner)?;
                save_database_if_missing(
                    self.db_mut()?,
                    DatabaseSchema {
                        name,
                        owner,
                        settings: BTreeMap::new(),
                    },
                )?;
                Ok(Some(SqlResult::command("CREATE DATABASE")))
            }
            DatabaseDdl::AlterOwner { name, owner } => {
                ensure_known_role(&list_roles(self.db_ref())?, &owner)?;
                // Had no authorization at all: any authenticated role could
                // reassign the database.
                let current_owner = list_databases(self.db_ref())?
                    .into_iter()
                    .find(|database| database.name.eq_ignore_ascii_case(&name))
                    .map(|database| database.owner)
                    .unwrap_or_else(|| BOOTSTRAP_ROLE_NAME.to_string());
                let object = format!("database {name}");
                self.require_object_ownership(&current_owner, &object, "change its owner")?;
                self.require_settable_new_owner(&owner, &object)?;
                save_database_owner(self.db_mut()?, &name, &owner)?;
                Ok(Some(SqlResult::command("ALTER DATABASE")))
            }
            DatabaseDdl::SetTablespace { name } => {
                ensure_database_exists(self.db_ref(), &name)?;
                Ok(Some(SqlResult::command("ALTER DATABASE")))
            }
            DatabaseDdl::SetSetting { name, .. } | DatabaseDdl::ResetAllSettings { name } => {
                // PostgreSQL: the database owner or a superuser may set its session defaults.
                let owner = list_databases(self.db_ref())?
                    .into_iter()
                    .find(|database| database.name.eq_ignore_ascii_case(&name))
                    .map(|database| database.owner)
                    .ok_or_else(|| {
                        SqlError::InvalidSql(format!("database \"{name}\" does not exist"))
                    })?;
                self.require_object_ownership(&owner, &format!("database {name}"), "alter it")?;
                match &command {
                    DatabaseDdl::SetSetting { setting, value, .. } => {
                        if is_protected_security_setting(setting) {
                            return Err(protected_security_setting_error(setting));
                        }
                        save_database_setting(
                            self.db_mut()?,
                            &name,
                            Some(setting),
                            value.as_deref(),
                        )?;
                    }
                    _ => save_database_setting(self.db_mut()?, &name, None, None)?,
                }
                Ok(Some(SqlResult::command("ALTER DATABASE")))
            }
            DatabaseDdl::Drop { name, if_exists, .. } => {
                let Some(database) = list_databases(self.db_ref())?
                    .into_iter()
                    .find(|database| database.name.eq_ignore_ascii_case(&name))
                else {
                    if if_exists {
                        return Ok(Some(SqlResult::command("DROP DATABASE")));
                    }
                    return Err(SqlError::InvalidSql(format!(
                        "database \"{name}\" does not exist"
                    )));
                };
                self.require_object_ownership(
                    &database.owner,
                    &format!("database {name}"),
                    "drop it",
                )?;
                delete_database(self.db_mut()?, &name)?;
                Ok(Some(SqlResult::command("DROP DATABASE")))
            }
        }
    }

    pub(crate) fn execute_raw_role_membership_ddl(
        &mut self,
        sql: &str,
    ) -> Result<Option<SqlResult>> {
        let Some(mut command) = parse_raw_role_membership_ddl(sql)? else {
            return Ok(None);
        };
        self.require_role_management_privilege("manage role memberships")?;
        let (roles, members) = match &mut command {
            RawRoleMembershipDdl::Grant { roles, members, .. }
            | RawRoleMembershipDdl::Revoke { roles, members } => (roles, members),
        };
        for role in roles.iter_mut().chain(members.iter_mut()) {
            *role = match role.as_str() {
                "CURRENT_USER" | "CURRENT_ROLE" => current_user_from_gucs(&self.session_gucs),
                "SESSION_USER" => session_user_from_gucs(&self.session_gucs),
                _ => role.clone(),
            };
        }
        let existing_memberships = list_role_memberships(self.db_ref())?;
        let grantor = if self.current_user_is_superuser()? {
            current_role_name()
        } else {
            current_user_from_gucs(&self.session_gucs)
        };

        let known_roles = list_roles(self.db_ref())?;
        match command {
            RawRoleMembershipDdl::Grant {
                roles: granted_roles,
                members,
                admin_option,
                inherit_option,
                set_option,
            } => {
                for role in &granted_roles {
                    ensure_known_role(&known_roles, role)?;
                    let granted = known_roles
                        .iter()
                        .find(|candidate| candidate.name.eq_ignore_ascii_case(role))
                        .ok_or_else(|| SqlError::UndefinedRole { name: role.clone() })?;
                    self.ensure_role_target_manageable(granted, "grant")?;
                }
                for member in &members {
                    ensure_known_role(&known_roles, member)?;
                }
                for role in &granted_roles {
                    for member in &members {
                        if role == member {
                            return Err(SqlError::InvalidSql(format!(
                                "role \"{role}\" cannot be a member of itself"
                            )));
                        }
                        let previous = existing_memberships.iter().find(|membership| {
                            membership.role.eq_ignore_ascii_case(role)
                                && membership.member.eq_ignore_ascii_case(member)
                        });
                        let default_inherit = known_roles
                            .iter()
                            .find(|role| role.name.eq_ignore_ascii_case(member))
                            .is_none_or(|role| role.inherit);
                        self.capture_membership_undo(role, member)?;
                        save_role_membership(
                            self.db_mut()?,
                            &RoleMembership {
                                role: role.clone(),
                                member: member.clone(),
                                grantor: grantor.clone(),
                                admin_option: admin_option
                                    .unwrap_or_else(|| previous.is_some_and(|m| m.admin_option)),
                                inherit_option: Some(
                                    inherit_option
                                        .or_else(|| previous.and_then(|m| m.inherit_option))
                                        .unwrap_or(default_inherit),
                                ),
                                set_option: set_option
                                    .unwrap_or_else(|| previous.is_none_or(|m| m.set_option)),
                            },
                        )?;
                    }
                }
                Ok(Some(SqlResult::command("GRANT ROLE")))
            }
            RawRoleMembershipDdl::Revoke { roles, members } => {
                for role in &roles {
                    ensure_known_role(&known_roles, role)?;
                    let revoked = known_roles
                        .iter()
                        .find(|candidate| candidate.name.eq_ignore_ascii_case(role))
                        .ok_or_else(|| SqlError::UndefinedRole { name: role.clone() })?;
                    self.ensure_role_target_manageable(revoked, "revoke")?;
                }
                for member in &members {
                    ensure_known_role(&known_roles, member)?;
                }
                for role in &roles {
                    for member in &members {
                        self.capture_membership_undo(role, member)?;
                        delete_role_membership(self.db_mut()?, role, member)?;
                    }
                }
                Ok(Some(SqlResult::command("REVOKE ROLE")))
            }
        }
    }

    pub(crate) fn scan_session_records_for_action(
        &self,
        table: &str,
        action: PolicyAction,
    ) -> Result<Vec<Record>> {
        let records = match self.security_context.as_ref() {
            Some(ctx) => match self.read_tx() {
                Some(tx) => tx
                    .scan_collection_with_context(ctx, table)
                    .map_err(SqlError::from),
                None => self
                    .db_ref()
                    .scan_collection_with_context(ctx, table)
                    .map_err(SqlError::from),
            },
            None => match self.read_tx() {
                Some(tx) => tx.scan_collection(table).map_err(SqlError::from),
                None => self.db_ref().scan_collection(table).map_err(SqlError::from),
            },
        }?;
        self.filter_rls_records(table, action, records)
    }

    pub(crate) fn get_session_record_for_action_with_schema(
        &self,
        table: &str,
        action: PolicyAction,
        id: &str,
        schema: Option<&TableSchema>,
    ) -> Result<Option<Arc<Record>>> {
        let record = match self.security_context.as_ref() {
            Some(ctx) => match self.tx.as_ref() {
                Some(tx) => tx.get_with_context(ctx, table, id).map_err(SqlError::from),
                None => self
                    .db_ref()
                    .get_with_context(ctx, table, id)
                    .map_err(SqlError::from),
            },
            None => match self.tx.as_ref() {
                Some(tx) => tx.get(table, id).map_err(SqlError::from),
                None => self.db_ref().get(table, id).map_err(SqlError::from),
            },
        }?;
        let Some(record) = record else {
            return Ok(None);
        };
        let engine = self.sql_engine();
        if rls_allows_record_with_schema(&engine, table, action, &record, schema)? {
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn session_records_for_action_ids_with_schema(
        &self,
        table: &str,
        action: PolicyAction,
        ids: Vec<String>,
        schema: Option<&TableSchema>,
    ) -> Result<Vec<Record>> {
        let mut records = Vec::new();
        for (idx, id) in ids.into_iter().enumerate() {
            if idx % 1024 == 0 {
                self.cancellation.check()?;
            }
            if let Some(record) =
                self.get_session_record_for_action_with_schema(table, action, &id, schema)?
            {
                // The Arc is usually fresh from the read (refcount 1): take the
                // record instead of deep-cloning its metadata tree.
                records.push(Arc::try_unwrap(record).unwrap_or_else(|record| (*record).clone()));
            }
        }
        Ok(records)
    }

    /// Candidate records for an UPDATE, and whether they came from an index
    /// lookup (`false` means a full scan the caller must filter itself).
    pub(crate) fn update_candidate_records(
        &self,
        table: &str,
        target_alias: &str,
        schema: Option<&TableSchema>,
        selection: Option<&Expr>,
        ctes: &BTreeMap<String, CteResult>,
    ) -> Result<(Vec<Record>, bool)> {
        let row_engine = self.sql_engine_with_ctes(ctes.clone());
        if let Some(ids) = row_engine.indexed_record_ids_for_table_selection(
            table,
            target_alias,
            schema,
            selection,
        )? {
            sql_profile_index_lookup();
            let records = self.session_records_for_action_ids_with_schema(
                table,
                PolicyAction::Update,
                ids,
                schema,
            )?;
            sql_profile_records_materialized(&records);
            return Ok((records, true));
        }
        let records = self.scan_session_records_for_action(table, PolicyAction::Update)?;
        sql_profile_full_scan();
        sql_profile_records_materialized(&records);
        Ok((records, false))
    }

    pub(crate) fn filter_rls_records(
        &self,
        table: &str,
        action: PolicyAction,
        records: Vec<Record>,
    ) -> Result<Vec<Record>> {
        let schema = load_schema(self.db_ref(), table)?;
        let prepared = prepare_rls_with_schema(
            self.db_ref(),
            table,
            action,
            schema.as_ref(),
            &self.session_gucs,
            self.security_context.as_ref(),
            false,
        )?;
        let PreparedRls::Filter(filter) = prepared else {
            return Ok(records);
        };
        let schema = schema.as_ref().expect("RLS filter requires a table schema");
        let engine = self.sql_engine();
        let mut filtered = Vec::with_capacity(records.len());
        for record in records {
            if filter.allows(&engine, schema, &record)? {
                filtered.push(record);
            }
        }
        Ok(filtered)
    }

    pub(crate) fn retain_rls_select_visible<T>(
        &self,
        table: &str,
        schema: Option<&TableSchema>,
        items: Vec<T>,
        record_of: impl Fn(&T) -> &Record,
    ) -> Result<Vec<T>> {
        let prepared = prepare_rls_with_schema(
            self.db_ref(),
            table,
            PolicyAction::Select,
            schema,
            &self.session_gucs,
            self.security_context.as_ref(),
            false,
        )?;
        let PreparedRls::Filter(filter) = prepared else {
            return Ok(items);
        };
        let schema = schema.expect("RLS filter requires a table schema");
        let engine = self.sql_engine();
        let mut retained = Vec::with_capacity(items.len());
        for item in items {
            if filter.allows(&engine, schema, record_of(&item))? {
                retained.push(item);
            }
        }
        Ok(retained)
    }

    pub(crate) fn enforce_rls_check(
        &self,
        table: &str,
        action: PolicyAction,
        record: &Record,
    ) -> Result<()> {
        let schema = load_schema(self.db_ref(), table)?;
        self.enforce_rls_checks(table, schema.as_ref(), action, std::slice::from_ref(record))
    }

    pub(crate) fn enforce_rls_checks(
        &self,
        table: &str,
        schema: Option<&TableSchema>,
        action: PolicyAction,
        records: &[Record],
    ) -> Result<()> {
        self.enforce_rls_write_checks(table, schema, action, false, records)
    }

    /// The new-row checks PostgreSQL runs for a write (its WITH CHECK
    /// options): the command's own `WITH CHECK` policies (falling back to
    /// `USING`), then, when the statement reads the target relation
    /// (`select_required`: a `RETURNING` or an `ON CONFLICT` arbiter for
    /// INSERT, any read of the existing row for UPDATE), the SELECT
    /// policies' `USING`. A new row the writer could not read back is then
    /// an error rather than a row silently written and returned.
    pub(crate) fn enforce_rls_write_checks(
        &self,
        table: &str,
        schema: Option<&TableSchema>,
        action: PolicyAction,
        select_required: bool,
        records: &[Record],
    ) -> Result<()> {
        match self.first_rls_write_violation(table, schema, action, select_required, records)? {
            Some((_, error)) => Err(error),
            None => Ok(()),
        }
    }

    /// The index of the first record failing the write checks of
    /// `enforce_rls_write_checks`, with its error. Callers that check
    /// constraints in a batch use the index to report an earlier row's
    /// constraint error first, as PostgreSQL's row-at-a-time order does.
    pub(crate) fn first_rls_write_violation(
        &self,
        table: &str,
        schema: Option<&TableSchema>,
        action: PolicyAction,
        select_required: bool,
        records: &[Record],
    ) -> Result<Option<(usize, SqlError)>> {
        if records.is_empty() || schema.is_none_or(|schema| !schema.rls_enabled) {
            return Ok(None);
        }
        let prepare = |action, with_check| {
            prepare_rls_with_schema(
                self.db_ref(),
                table,
                action,
                schema,
                &self.session_gucs,
                self.security_context.as_ref(),
                with_check,
            )
        };
        let mut filters = Vec::with_capacity(2);
        if let PreparedRls::Filter(filter) = prepare(action, true)? {
            filters.push(filter);
        }
        if select_required && action != PolicyAction::Select {
            if let PreparedRls::Filter(filter) = prepare(PolicyAction::Select, false)? {
                filters.push(filter);
            }
        }
        if filters.is_empty() {
            return Ok(None);
        }
        let schema = schema.expect("RLS filter requires a table schema");
        let engine = self.sql_engine();
        for (index, record) in records.iter().enumerate() {
            for filter in &filters {
                let verdict = filter.verdict(&engine, schema, record)?;
                if !matches!(verdict, RlsRowVerdict::Allowed) {
                    return Ok(Some((index, rls_check_denied(table, &verdict))));
                }
            }
        }
        Ok(None)
    }

    /// The existing-row check of `INSERT ... ON CONFLICT DO UPDATE`: the row
    /// must pass the UPDATE policies' `USING` and, since the arbiter reads
    /// it, the SELECT policies' `USING`. PostgreSQL raises an error here
    /// (never a silent skip), so a row the writer cannot see is never
    /// modified or returned.
    pub(crate) fn enforce_rls_conflict_check(
        &self,
        table: &str,
        schema: &TableSchema,
        existing: &Record,
    ) -> Result<()> {
        if !schema.rls_enabled {
            return Ok(());
        }
        let engine = self.sql_engine();
        for action in [PolicyAction::Update, PolicyAction::Select] {
            if let PreparedRls::Filter(filter) = prepare_rls_with_schema(
                self.db_ref(),
                table,
                action,
                Some(schema),
                &self.session_gucs,
                self.security_context.as_ref(),
                false,
            )? {
                let verdict = filter.verdict(&engine, schema, existing)?;
                if !matches!(verdict, RlsRowVerdict::Allowed) {
                    return Err(rls_using_denied(table, &verdict));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn insert_session_records(
        &mut self,
        table: &str,
        records: Vec<Record>,
    ) -> Result<()> {
        self.insert_session_records_with_blob_mode(table, records, None)
    }

    pub(crate) fn insert_session_records_for_key_move<'record>(
        &mut self,
        table: &str,
        records: impl IntoIterator<Item = (&'record Record, Record)>,
    ) -> Result<()> {
        let mut prepared = Vec::new();
        let mut events = Vec::new();
        for (before, mut after) in records {
            events.extend(self.prepare_blob_records(
                table,
                std::slice::from_mut(&mut after),
                Some(before),
            )?);
            prepared.push(after);
        }
        self.insert_session_records_with_blob_mode(table, prepared, Some(events))
    }

    fn insert_session_records_with_blob_mode(
        &mut self,
        table: &str,
        mut records: Vec<Record>,
        prepared: Option<Vec<oracle::lobs::WriteEvent>>,
    ) -> Result<()> {
        let events = match prepared {
            Some(events) => events,
            None => self.prepare_blob_records(table, &mut records, None)?,
        };
        self.materialize_full_text_projections(table, &mut records)?;
        let record_count = records.len();
        // Resolve the protection check before borrowing the transaction so the
        // shared db read does not overlap the `self.tx` mutable borrow.
        let protected = self.db_ref().collection_policy(table)?.is_some()
            || self
                .db_ref()
                .mutation_policy(table)?
                .is_some_and(|policy| policy.grants_required);
        let signed_grant = protected
            .then(|| self.signed_mutation_grant(table))
            .flatten();
        if let Some(tx) = self.tx.as_mut() {
            if protected {
                let grant = signed_grant.ok_or_else(|| {
                    SqlError::BicDb(BicDbError::Authorization(
                        "protected SQL writes inside transactions require a signed mutation grant"
                            .to_string(),
                    ))
                })?;
                let started = Instant::now();
                for record in records {
                    tx.upsert_with_grant(grant, table, record)?;
                }
                sql_profile_write_elapsed(started);
                sql_profile_write_batch(record_count);
                self.record_blob_writes(events)?;
                return Ok(());
            }
            let started = Instant::now();
            tx.batch_insert(table, records)?;
            sql_profile_write_elapsed(started);
            sql_profile_write_batch(record_count);
            self.record_blob_writes(events)?;
            return Ok(());
        }
        let started = Instant::now();
        let result = match self.security_context.clone() {
            Some(ctx) => self.db_mut()?.secure(&ctx).batch_insert(table, records),
            None => self.db_mut()?.batch_insert(table, records),
        }
        .map_err(SqlError::from);
        sql_profile_write_elapsed(started);
        if result.is_ok() {
            sql_profile_write_batch(record_count);
        }
        result
    }

    pub(crate) fn insert_reserved_session_record(
        &mut self,
        table: &str,
        reservation: bicdb_core::ReadCommittedRowReservation,
        mut record: Record,
        explicit_columns: &[String],
    ) -> Result<()> {
        let grant = self.signed_mutation_grant(table).ok_or_else(|| {
            SqlError::BicDb(BicDbError::Authorization(
                "reserved signed mutation requires its actual grant".into(),
            ))
        })?;
        let events = self.prepare_blob_records(table, std::slice::from_mut(&mut record), None)?;
        self.materialize_full_text_projections(table, std::slice::from_mut(&mut record))?;
        let started = Instant::now();
        self.tx
            .as_mut()
            .ok_or_else(|| SqlError::BicDb(BicDbError::TransactionNotPending))?
            .upsert_reserved_with_grant_columns(grant, reservation, record, explicit_columns)?;
        sql_profile_write_elapsed(started);
        sql_profile_write_batch(1);
        self.record_blob_writes(events)
    }

    pub(crate) fn insert_session_records_with_statement_snapshots<I>(
        &mut self,
        table: &str,
        records: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = (Record, u64)>,
    {
        let mut records = records.into_iter().collect::<Vec<_>>();
        let mut events = Vec::new();
        for (record, _) in &mut records {
            events.extend(self.prepare_blob_records(table, std::slice::from_mut(record), None)?);
            self.materialize_full_text_projections(table, std::slice::from_mut(record))?;
        }
        let record_count = records.len();
        let protected = self.db_ref().collection_policy(table)?.is_some()
            || self
                .db_ref()
                .mutation_policy(table)?
                .is_some_and(|policy| policy.grants_required);
        let signed_grant = protected
            .then(|| self.signed_mutation_grant(table))
            .flatten();
        if let Some(tx) = self.tx.as_mut() {
            if protected {
                let grant = signed_grant.ok_or_else(|| {
                    SqlError::BicDb(BicDbError::Authorization(
                        "protected SQL writes inside transactions require a signed mutation grant"
                            .to_string(),
                    ))
                })?;
                let started = Instant::now();
                for (record, _) in records {
                    tx.upsert_with_grant(grant, table, record)?;
                }
                sql_profile_write_elapsed(started);
                sql_profile_write_batch(record_count);
                self.record_blob_writes(events)?;
                return Ok(());
            }
            let started = Instant::now();
            tx.write_upserts_with_statement_snapshots(table, records)?;
            sql_profile_write_elapsed(started);
            sql_profile_write_batch(record_count);
            self.record_blob_writes(events)?;
            return Ok(());
        }
        self.insert_session_records(
            table,
            records.into_iter().map(|(record, _)| record).collect(),
        )
    }

    /// Repair-aware sibling of
    /// [`Self::insert_session_records_with_statement_snapshots`]; writes with a
    /// `RepairPlan` skip the statement-time row lock (commit repairs instead).
    pub(crate) fn insert_session_records_with_repairs<I>(
        &mut self,
        table: &str,
        records: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = (Record, u64, Option<RepairPlan>)>,
    {
        let mut records = records.into_iter().collect::<Vec<_>>();
        let mut events = Vec::new();
        for (record, _, _) in &mut records {
            events.extend(self.prepare_blob_records(table, std::slice::from_mut(record), None)?);
            self.materialize_full_text_projections(table, std::slice::from_mut(record))?;
        }
        let record_count = records.len();
        let protected = self.db_ref().collection_policy(table)?.is_some()
            || self
                .db_ref()
                .mutation_policy(table)?
                .is_some_and(|policy| policy.grants_required);
        let signed_grant = protected
            .then(|| self.signed_mutation_grant(table))
            .flatten();
        if let Some(tx) = self.tx.as_mut() {
            if protected {
                let grant = signed_grant.ok_or_else(|| {
                    SqlError::BicDb(BicDbError::Authorization(
                        "protected SQL writes inside transactions require a signed mutation grant"
                            .to_string(),
                    ))
                })?;
                let started = Instant::now();
                for (record, _, _) in records {
                    tx.upsert_with_grant(grant, table, record)?;
                }
                sql_profile_write_elapsed(started);
                sql_profile_write_batch(record_count);
                self.record_blob_writes(events)?;
                return Ok(());
            }
            let started = Instant::now();
            tx.write_upserts_with_repairs(table, records)?;
            sql_profile_write_elapsed(started);
            sql_profile_write_batch(record_count);
            self.record_blob_writes(events)?;
            return Ok(());
        }
        self.insert_session_records(
            table,
            records.into_iter().map(|(record, _, _)| record).collect(),
        )
    }

    /// Buffer rows already in their stored form (see
    /// `try_execute_update_stored`): the caller has established that the
    /// table has no full-text projections or protection policy and that a
    /// transaction is open.
    pub(crate) fn insert_session_stored_records(
        &mut self,
        table: &str,
        rows: Vec<(Arc<bicdb_core::StoredRecord>, u64)>,
        optimistic_insert: bool,
    ) -> Result<()> {
        let record_count = rows.len();
        let Some(tx) = self.tx.as_mut() else {
            return Err(SqlError::Unsupported(
                "stored-form writes require a transaction".to_string(),
            ));
        };
        let started = Instant::now();
        if optimistic_insert {
            tx.write_stored_inserts_with_statement_snapshots(table, rows)?;
        } else {
            tx.write_stored_upserts_with_statement_snapshots(table, rows)?;
        }
        sql_profile_write_elapsed(started);
        sql_profile_write_batch(record_count);
        Ok(())
    }

    /// [`Self::insert_session_stored_records`] for rows spliced from their
    /// pre-image: `changed` names exactly the top-level keys the statement
    /// assigned, so commit skips every index that reads none of them.
    pub(crate) fn update_session_stored_records(
        &mut self,
        table: &str,
        rows: Vec<(
            Arc<bicdb_core::StoredRecord>,
            Arc<bicdb_core::StoredRecord>,
            u64,
            Option<RepairPlan>,
        )>,
        changed: Arc<[Box<str>]>,
    ) -> Result<()> {
        let record_count = rows.len();
        let Some(tx) = self.tx.as_mut() else {
            return Err(SqlError::Unsupported(
                "stored-form writes require a transaction".to_string(),
            ));
        };
        let started = Instant::now();
        tx.write_stored_updates_with_changes(
            table,
            rows.into_iter()
                .map(|(previous, stored, snapshot, repair)| {
                    (
                        Some(previous),
                        stored,
                        snapshot,
                        repair,
                        Some(Arc::clone(&changed)),
                    )
                }),
        )?;
        sql_profile_write_elapsed(started);
        sql_profile_write_batch(record_count);
        Ok(())
    }

    pub(crate) fn update_session_record(&mut self, table: &str, record: Record) -> Result<()> {
        self.insert_session_records(table, vec![record])
    }

    pub(crate) fn materialize_full_text_projections(
        &self,
        table: &str,
        records: &mut [Record],
    ) -> Result<()> {
        let Some(schema) = load_schema_shared(self.db_ref(), table)? else {
            return Ok(());
        };
        let definitions = index_definitions_shared(self.db_ref());
        for definition in definitions
            .iter()
            .filter(|index| {
                matches!(
                    index.kind,
                    IndexKind::FullText | IndexKind::Jsonb | IndexKind::Array
                ) && index.collection.eq_ignore_ascii_case(table)
                    || index.kind == IndexKind::Spatial
                        && index.collection.eq_ignore_ascii_case(table)
                        && index.fields.first().is_some_and(|field| {
                            matches!(field, IndexField::MetadataPath(path) if path.first().is_some_and(|name| name.starts_with("$bicdb_geometric_")))
                        })
            })
        {
            let index_and_source = if definition.kind == IndexKind::Spatial {
                schema.indexes.iter().find_map(|index| {
                    index
                        .internal_index_names
                        .iter()
                        .position(|name| name == &definition.name)
                        .and_then(|position| {
                            index
                                .source_expressions
                                .get(position)
                                .map(|source| (index, source.as_str()))
                        })
                })
            } else {
                schema
                .indexes
                .iter()
                .find(|index| index.name == definition.name)
                    .map(|index| (index, index.source_expressions.first().map(String::as_str).unwrap_or(index.expression.as_str())))
            };
            let Some((_index, source_expression)) = index_and_source else {
                continue;
            };
            let Some(IndexField::MetadataPath(path)) = definition.fields.first() else {
                continue;
            };
            let Some(projection) = path.first() else {
                continue;
            };
            let expression = parse_routine_expr(source_expression)?;
            if definition.kind == IndexKind::Spatial {
                let pg_type = projected_expr_pg_type(&expression, Some(&schema)).ok_or_else(|| {
                    SqlError::undefined_object(format!(
                        "cannot resolve geometric index expression type: {expression}"
                    ))
                })?;
                self.materialize_geometric_projection(
                    table,
                    &schema,
                    projection,
                    &expression,
                    &pg_type,
                    records,
                )?;
            } else {
            self.materialize_inverted_projection(
                table,
                &schema,
                projection,
                &expression,
                definition.kind,
                records,
            )?;
        }
        }
        Ok(())
    }

    pub(crate) fn materialize_geometric_projection(
        &self,
        table: &str,
        schema: &TableSchema,
        projection: &str,
        expression: &Expr,
        pg_type: &str,
        records: &mut [Record],
    ) -> Result<()> {
        for record in records {
            let value = self
                .eval_target_record_expr(table, table, schema, record, expression)?
                .ok_or_else(|| {
                    SqlError::Unsupported(format!(
                        "geometric index expression {expression} cannot be evaluated"
                    ))
                })?;
            if let Some(value) = geometric_index_projection(&value, pg_type)? {
                set_json_object_value(&mut record.metadata, projection, value);
            } else if let Some(metadata) = record.metadata.as_object_mut() {
                metadata.remove(projection);
            }
        }
        Ok(())
    }

    pub(crate) fn materialize_full_text_projection(
        &self,
        table: &str,
        schema: &TableSchema,
        projection: &str,
        expression: &Expr,
        records: &mut [Record],
    ) -> Result<()> {
        self.materialize_inverted_projection(
            table,
            schema,
            projection,
            expression,
            IndexKind::FullText,
            records,
        )
    }

    pub(crate) fn parallel_full_text_doc_blobs(
        &self,
        table: &str,
        schema: &TableSchema,
        expression: &Expr,
        records: &[Record],
        workers: usize,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let workers = workers.clamp(1, records.len());
        let chunk_rows = records.len().div_ceil(workers);
        let db = self.db_ref();
        let settings = self.settings;
        let routine_vars = Arc::clone(&self.routine_vars);
        let session_gucs = self.session_gucs.clone();
        let security_context = self.security_context.clone();
        let cancellation = self.cancellation.clone();
        let chunks = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for chunk in records.chunks(chunk_rows) {
                let routine_vars = Arc::clone(&routine_vars);
                let session_gucs = session_gucs.clone();
                let security_context = security_context.clone();
                let cancellation = cancellation.clone();
                handles.push(scope.spawn(move || {
                    let mut worker = match security_context {
                        Some(context) => SqlSession::new_shared_secure(db, context),
                        None => SqlSession::new_shared(db),
                    };
                    worker.settings = settings;
                    worker.routine_vars = routine_vars;
                    worker.session_gucs = session_gucs;
                    worker.cancellation = cancellation;
                    let mut blobs = Vec::with_capacity(chunk.len());
                    for record in chunk {
                        worker.cancellation.check()?;
                        let value = worker
                            .eval_target_record_expr(table, table, schema, record, expression)?
                            .ok_or_else(|| {
                                SqlError::Unsupported(format!(
                                    "inverted index expression {expression} cannot be evaluated"
                                ))
                            })?;
                        let (doc_length, doc_distinct, terms) =
                            crate::fts::fts_doc_terms_parts(&value)?;
                        blobs.push((
                            record.id.clone(),
                            bicdb_core::BicDb::encode_fts_doc_terms(
                                doc_length,
                                doc_distinct,
                                &terms,
                            ),
                        ));
                    }
                    Ok::<_, SqlError>(blobs)
                }));
            }
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().map_err(|_| {
                        SqlError::Unsupported(
                            "parallel full-text tokenization worker panicked".to_string(),
                        )
                    })?
                })
                .collect::<Result<Vec<_>>>()
        })?;
        Ok(chunks.into_iter().flatten().collect())
    }

    pub(crate) fn materialize_inverted_projection(
        &self,
        table: &str,
        schema: &TableSchema,
        projection: &str,
        expression: &Expr,
        kind: IndexKind,
        records: &mut [Record],
    ) -> Result<()> {
        for record in records {
            let value = self
                .eval_target_record_expr(table, table, schema, record, expression)?
                .ok_or_else(|| {
                    SqlError::Unsupported(format!(
                        "inverted index expression {expression} cannot be evaluated"
                    ))
                })?;
            let projected = match kind {
                // Positions included: ranking reads the index, not the text.
                IndexKind::FullText => crate::fts::fts_index_projection(&value)?,
                IndexKind::Jsonb => serde_json::json!(jsonb_index_terms(&value)?),
                IndexKind::Array if projection.starts_with(TRIGRAM_PROJECTION_PREFIX) => {
                    serde_json::json!(trigram_index_terms(&value)?)
                }
                IndexKind::Array => serde_json::json!(array_index_terms(&value)?),
                _ => unreachable!("only inverted indexes materialize token projections"),
            };
            set_json_object_value(&mut record.metadata, projection, projected);
        }
        Ok(())
    }

    pub(crate) fn delete_session_record(&mut self, table: &str, id: &str) -> Result<bool> {
        Ok(self.delete_session_records(table, &[id.to_string()])? > 0)
    }

    pub(crate) fn delete_session_records(&mut self, table: &str, ids: &[String]) -> Result<usize> {
        self.delete_session_records_with_statement_snapshots(
            table,
            ids.iter().map(|id| (id.clone(), 0)),
        )
    }

    pub(crate) fn delete_session_records_with_statement_snapshots<I>(
        &mut self,
        table: &str,
        ids: I,
    ) -> Result<usize>
    where
        I: IntoIterator<Item = (String, u64)>,
    {
        let ids = ids.into_iter().collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(0);
        }
        let events = self.remove_blob_owners(table, &ids)?;
        let protected = self.db_ref().collection_policy(table)?.is_some()
            || self
                .db_ref()
                .mutation_policy(table)?
                .is_some_and(|policy| policy.grants_required);
        let signed_grant = protected
            .then(|| self.signed_mutation_grant(table))
            .flatten();
        if let Some(tx) = self.tx.as_mut() {
            if protected {
                let grant = signed_grant.ok_or_else(|| {
                    SqlError::BicDb(BicDbError::Authorization(
                        "protected SQL deletes inside transactions require a signed mutation grant"
                            .to_string(),
                    ))
                })?;
                let started = Instant::now();
                for (id, _) in &ids {
                    tx.delete_with_grant(grant, table, id)?;
                }
                sql_profile_write_elapsed(started);
                sql_profile_write_batch(ids.len());
                self.record_blob_writes(events)?;
                return Ok(ids.len());
            }
            let started = Instant::now();
            tx.delete_many_with_statement_snapshots(
                table,
                ids.iter()
                    .map(|(id, statement_snapshot)| (id.as_str(), *statement_snapshot)),
            )?;
            sql_profile_write_elapsed(started);
            sql_profile_write_batch(ids.len());
            self.record_blob_writes(events)?;
            return Ok(ids.len());
        }
        let ids = ids.into_iter().map(|(id, _)| id).collect::<Vec<_>>();
        let started = Instant::now();
        let deleted = match self.security_context.clone() {
            Some(ctx) => self
                .db_mut()?
                .secure(&ctx)
                .batch_delete(table, ids.iter().map(String::as_str)),
            None => self
                .db_mut()?
                .batch_delete(table, ids.iter().map(String::as_str)),
        }
        .map_err(SqlError::from)?;
        sql_profile_write_elapsed(started);
        if deleted > 0 {
            sql_profile_write_batch(deleted);
        }
        Ok(deleted)
    }

    pub(crate) fn capture_role_undo(&mut self, name: &str) -> Result<()> {
        if self.tx.is_some() {
            let previous = load_role_schema(self.db_ref(), name)?;
            self.ddl_undo.push(DdlUndo::RestoreRole {
                name: name.to_owned(),
                previous,
            });
        }
        Ok(())
    }

    pub(crate) fn capture_membership_undo(&mut self, role: &str, member: &str) -> Result<()> {
        if self.tx.is_some() {
            let previous = list_role_memberships(self.db_ref())?
                .into_iter()
                .find(|m| m.role == role && m.member == member);
            self.ddl_undo.push(DdlUndo::RestoreMembership {
                role: role.to_owned(),
                member: member.to_owned(),
                previous,
            });
        }
        Ok(())
    }

    pub(crate) fn execute_create_role(&mut self, create_role: &CreateRole) -> Result<SqlResult> {
        self.require_role_management_privilege("create roles")?;
        if create_role.authorization_owner.is_some()
            || !create_role.in_role.is_empty()
            || !create_role.in_group.is_empty()
            || !create_role.role.is_empty()
            || !create_role.user.is_empty()
            || !create_role.admin.is_empty()
        {
            return Err(SqlError::Unsupported(
                "role membership options are not supported".to_string(),
            ));
        }
        if create_role.superuser.unwrap_or(false)
            || create_role.bypassrls.unwrap_or(false)
            || create_role.create_role.unwrap_or(false)
            || create_role.replication.unwrap_or(false)
        {
            self.require_superuser_for_privileged_role_attributes("create a role with")?;
        }
        for name in &create_role.names {
            let name = normalize_role_name(&object_name(name)?);
            let mut role = default_role_schema(&name);
            role.can_login = create_role.login.unwrap_or(false);
            role.inherit = create_role.inherit.unwrap_or(true);
            role.bypass_rls = create_role.bypassrls.unwrap_or(false);
            role.password_set = create_role.password.is_some();
            role.superuser = create_role.superuser.unwrap_or(false);
            role.create_db = create_role.create_db.unwrap_or(false);
            role.create_role = create_role.create_role.unwrap_or(false);
            role.replication = create_role.replication.unwrap_or(false);
            if let Some(expr) = create_role.connection_limit.as_ref() {
                role.connection_limit = sql_value_i64(&eval_constant_expr(expr)?).unwrap_or(-1);
            }
            if let Some(expr) = create_role.valid_until.as_ref() {
                role.valid_until = Some(eval_constant_expr(expr)?.to_cell());
            }
            self.capture_role_undo(&name)?;
            create_role_record(self.db_mut()?, role, create_role.if_not_exists)?;
        }
        Ok(SqlResult::command("CREATE ROLE"))
    }

    pub(crate) fn execute_alter_role(
        &mut self,
        name: &Ident,
        operation: &AlterRoleOperation,
    ) -> Result<SqlResult> {
        self.require_role_management_privilege("alter roles")?;
        let role_name = normalize_role_name(&ident_value(name));
        let mut role = load_role_schema(self.db_ref(), &role_name)?.ok_or_else(|| {
            SqlError::UndefinedRole {
                name: role_name.clone(),
            }
        })?;
        self.ensure_role_target_manageable(&role, "alter")?;
        self.capture_role_undo(&role_name)?;
        match operation {
            AlterRoleOperation::RenameRole { role_name: new } => {
                let new_name = normalize_role_name(&ident_value(new));
                if role_exists(self.db_ref(), &new_name)? {
                    return Err(SqlError::DuplicateRole { name: new_name });
                }
                self.capture_role_undo(&new_name)?;
                delete_role_record(self.db_mut()?, &role_name)?;
                role.name = new_name;
                create_role_record(self.db_mut()?, role, false)?;
            }
            AlterRoleOperation::WithOptions { options } => {
                if options.iter().any(|option| {
                    matches!(
                        option,
                        RoleOption::BypassRLS(_)
                            | RoleOption::SuperUser(_)
                            | RoleOption::CreateRole(_)
                            | RoleOption::Replication(_)
                    )
                }) {
                    self.require_superuser_for_privileged_role_attributes(
                        "alter role attributes including",
                    )?;
                }
                for option in options {
                    match option {
                        RoleOption::BypassRLS(value) => role.bypass_rls = *value,
                        RoleOption::SuperUser(value) => role.superuser = *value,
                        RoleOption::Login(value) => role.can_login = *value,
                        RoleOption::Inherit(value) => role.inherit = *value,
                        RoleOption::CreateDB(value) => role.create_db = *value,
                        RoleOption::CreateRole(value) => role.create_role = *value,
                        RoleOption::Replication(value) => role.replication = *value,
                        RoleOption::Password(password) => {
                            role.password_set =
                                matches!(password, sqlparser::ast::Password::Password(_));
                        }
                        RoleOption::ConnectionLimit(expr) => {
                            role.connection_limit =
                                sql_value_i64(&eval_constant_expr(expr)?).unwrap_or(-1);
                        }
                        RoleOption::ValidUntil(expr) => {
                            role.valid_until = Some(eval_constant_expr(expr)?.to_cell());
                        }
                    }
                }
                delete_role_record(self.db_mut()?, &role_name)?;
                create_role_record(self.db_mut()?, role, false)?;
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "ALTER ROLE operation {other} is not supported"
                )));
            }
        }
        Ok(SqlResult::command("ALTER ROLE"))
    }

    pub(crate) fn execute_grant(&mut self, grant: &Grant) -> Result<SqlResult> {
        if let Some(result) =
            self.execute_raw_function_privileges(&crate::sql_text::sql_text(grant))?
        {
            return Ok(result);
        }
        if grant.with_grant_option
            || grant.as_grantor.is_some()
            || grant.granted_by.is_some()
            || grant.current_grants.is_some()
        {
            return Err(SqlError::Unsupported(
                "grant options, grantors, and current grants are not supported".to_string(),
            ));
        }
        let Some(objects) = grant.objects.as_ref() else {
            return Err(SqlError::Unsupported(
                "role membership grants are not supported".to_string(),
            ));
        };
        self.apply_privileges(&grant.privileges, objects, &grant.grantees, true)?;
        Ok(SqlResult::command("GRANT"))
    }

    pub(crate) fn execute_revoke(&mut self, revoke: &Revoke) -> Result<SqlResult> {
        if let Some(result) =
            self.execute_raw_function_privileges(&crate::sql_text::sql_text(revoke))?
        {
            return Ok(result);
        }
        if revoke.granted_by.is_some() || matches!(revoke.cascade, Some(CascadeOption::Cascade)) {
            return Err(SqlError::Unsupported(
                "GRANTED BY and CASCADE revokes are not supported".to_string(),
            ));
        }
        let Some(objects) = revoke.objects.as_ref() else {
            return Err(SqlError::Unsupported(
                "role membership revokes are not supported".to_string(),
            ));
        };
        self.apply_privileges(&revoke.privileges, objects, &revoke.grantees, false)?;
        Ok(SqlResult::command("REVOKE"))
    }

    /// CR-2: GRANT/REVOKE had no grantor check at all — any authenticated
    /// user could grant themselves any privilege on any table.
    ///
    /// The CR-2 fix covered only `Table`, and even that branch failed OPEN for
    /// views: `require_table_ownership` looks the relation up with
    /// `load_schema`, views are stored separately, so a missing table schema
    /// read as "not my call" and the grant went through. Since views execute
    /// with definer semantics, self-granting SELECT on a privileged role's view
    /// handed the attacker that role's read access to the source tables. Every
    /// object kind `grant_targets` can produce is gated here now, and the
    /// relation branch resolves views as well as tables.
    pub(crate) fn require_grant_authority(
        &self,
        objects: &[(PrivilegeObjectType, String)],
        action: &str,
    ) -> Result<()> {
        for (object_type, name) in objects {
            match object_type {
                PrivilegeObjectType::Table if oracle::storage_catalog::is_relation(name) => self
                    .require_superuser_for_admin_operation(
                        "grant or revoke storage monitoring access",
                    )?,
                PrivilegeObjectType::Table => self.require_relation_ownership(name, action)?,
                PrivilegeObjectType::Sequence => {
                    if let Some(sequence) = load_sequence(self.db_ref(), name)? {
                        self.require_object_ownership(
                            &sequence.owner,
                            &format!("sequence {name}"),
                            action,
                        )?;
                    }
                }
                PrivilegeObjectType::Schema => {
                    // A schema with no namespace record (`public`) is
                    // bootstrap-owned, matching the convention everywhere else.
                    let owner = load_namespace(self.db_ref(), name)?
                        .map(|namespace| namespace.owner)
                        .unwrap_or_else(|| BOOTSTRAP_ROLE_NAME.to_string());
                    self.require_object_ownership(&owner, &format!("schema {name}"), action)?;
                }
                PrivilegeObjectType::Database => {
                    let owner = list_databases(self.db_ref())?
                        .into_iter()
                        .find(|database| database.name.eq_ignore_ascii_case(name))
                        .map(|database| database.owner)
                        .unwrap_or_else(|| BOOTSTRAP_ROLE_NAME.to_string());
                    self.require_object_ownership(&owner, &format!("database {name}"), action)?;
                }
                PrivilegeObjectType::Function => {
                    if let Some(routine) = load_routine(self.db_ref(), RoutineKind::Function, name)?
                    {
                        // An unrecorded owner reads as bootstrap-owned
                        // (fail-closed), matching RoutineSchema::owner().
                        self.require_object_ownership(
                            routine.owner(),
                            &format!("function {name}"),
                            action,
                        )?;
                    } else if let Some(procedure) =
                        load_routine(self.db_ref(), RoutineKind::Procedure, name)?
                    {
                        // `GRANT ... ON FUNCTION` naming a procedure keeps working.
                        self.require_object_ownership(
                            procedure.owner(),
                            &format!("procedure {name}"),
                            action,
                        )?;
                    } else if is_native_extension_function(name) {
                        self.require_object_ownership(
                            BOOTSTRAP_ROLE_NAME,
                            &format!("function {name}"),
                            action,
                        )?;
                    } else {
                        // PostgreSQL rejects a grant on a routine that does
                        // not exist; accepting it would hide a broken
                        // provisioning script.
                        return Err(SqlError::undefined_function(format!(
                            "function {name} does not exist"
                        )));
                    }
                }
                // `grant_targets` cannot produce these today; if it ever does,
                // fail closed rather than silently skipping the gate.
                PrivilegeObjectType::Type => {
                    return Err(SqlError::Unsupported(format!(
                        "{action} on {name} is not supported"
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn apply_privileges(
        &mut self,
        privileges: &Privileges,
        objects: &GrantObjects,
        grantees: &[Grantee],
        grant: bool,
    ) -> Result<()> {
        let privileges = match privileges {
            Privileges::Actions(actions) => {
                let mut names = Vec::new();
                for action in actions {
                    if let Action::Update {
                        columns: Some(columns),
                    } = action
                    {
                        if !matches!(objects, GrantObjects::Tables(_)) || columns.is_empty() {
                            return Err(SqlError::Unsupported(
                                "column UPDATE grants require named tables and columns".into(),
                            ));
                        }
                        names.extend(
                            columns
                                .iter()
                                .map(|column| ("UPDATE".to_owned(), Some(ident_value(column)))),
                        );
                    } else {
                        names.push((privilege_action_name(action)?.to_owned(), None));
                    }
                }
                names
            }
            _ => privilege_names(privileges, objects)?
                .into_iter()
                .map(|name| (name, None))
                .collect(),
        };
        let targets = grant_targets(self.db_ref(), objects)?;
        if targets
            .iter()
            .any(|(_, name)| oracle::storage_catalog::is_relation(name))
            && privileges
                .iter()
                .any(|(name, column)| name != "SELECT" || column.is_some())
        {
            return Err(SqlError::Unsupported(
                "storage inventory supports whole-view SELECT grants only".into(),
            ));
        }
        // Validate the entire column list before changing any grants.
        for (_, table) in &targets {
            for (_, column) in &privileges {
                if let Some(column) = column {
                    let schema = load_schema_shared(self.db_ref(), table)?;
                    if !schema
                        .as_ref()
                        .is_some_and(|schema| schema.column(column).is_some())
                    {
                        return Err(SqlError::InvalidSql(format!(
                            "column {column} does not exist on table {table}"
                        )));
                    }
                }
            }
        }
        // CR-2: only the owner (or a superuser) may hand out or take back
        // privileges on a relation. Applied here so GRANT and REVOKE share
        // one gate.
        self.require_grant_authority(&targets, if grant { "GRANT" } else { "REVOKE" })?;
        let roles = list_roles(self.db_ref())?;
        let grantees = grantees
            .iter()
            .map(grantee_role_name)
            .collect::<Result<Vec<_>>>()?;
        for role in &grantees {
            if role != "public" && !roles.iter().any(|candidate| candidate.name == *role) {
                return Err(SqlError::UndefinedRole { name: role.clone() });
            }
        }
        for role in grantees {
            for (object_type, object_name) in &targets {
                for (privilege, column) in &privileges {
                    let grant_row = PrivilegeGrant {
                        column: column.clone(),
                        object_type: *object_type,
                        object_name: object_name.clone(),
                        grantee: role.clone(),
                        privilege: privilege.clone(),
                    };
                    if role == "public"
                        && column.is_none()
                        && is_implicit_public_privilege(*object_type, object_name, privilege)
                    {
                        let marker = revoked_default_marker(*object_type, object_name, privilege);
                        if grant {
                            self.delete_session_privilege(&marker)?;
                        } else {
                            self.save_session_privilege(&marker)?;
                        }
                    }
                    if grant {
                        self.save_session_privilege(&grant_row)?;
                    } else {
                        self.delete_session_privilege(&grant_row)?;
                        if *object_type == PrivilegeObjectType::Table && column.is_none() {
                            for existing in list_privileges(self.db_ref())? {
                                if existing.object_type == *object_type
                                    && existing.object_name == *object_name
                                    && existing.grantee == role
                                    && existing.privilege == *privilege
                                    && existing.column.is_some()
                                {
                                    self.delete_session_privilege(&existing)?;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Extension functions (pgcrypto, uuid-ossp, pg_trgm) that BicDB evaluates
/// natively instead of recording them as catalog routines. Provisioning
/// scripts grant and revoke them like any other function.
fn is_native_extension_function(name: &str) -> bool {
    let bare = name.split('(').next().unwrap_or(name).trim();
    let bare = bare.rsplit('.').next().unwrap_or(bare).trim_matches('"');
    matches!(
        bare.to_ascii_lowercase().as_str(),
        "digest"
            | "hmac"
            | "gen_random_bytes"
            | "gen_random_uuid"
            | "uuid_generate_v4"
            | "similarity"
    )
}
