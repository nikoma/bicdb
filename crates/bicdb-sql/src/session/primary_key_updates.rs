//! Change a SQL primary key through the UPDATE policy, without SQL round trips.
use super::*;

impl SqlSession<'_> {
    pub(crate) fn try_move_primary_key(
        &mut self,
        update: &sqlparser::ast::Update,
        table: &str,
        schema: &TableSchema,
        ctes: &BTreeMap<String, CteResult>,
    ) -> Result<Option<SqlResult>> {
        let keys = primary_key_columns_for_schema(schema);
        let moves_key = update.assignments.iter().any(|assignment| {
            super::row_assignments::assignment_columns(&assignment.target)
                .iter()
                .any(|name| relation_name(name).is_ok_and(|column| keys.contains(&column)))
        });
        if !moves_key {
            return Ok(None);
        }
        let refuse = |why: &str| {
            Err(SqlError::Unsupported(format!(
                "UPDATE of a primary key {why} is not supported"
            )))
        };
        // Projection and trigger forms require additional UPDATE semantics.
        // A physical move must never masquerade as DELETE/INSERT SQL operations.
        if update.from.is_some() || update.returning.is_some() {
            return refuse("with FROM or RETURNING");
        }
        if list_triggers(self.db_ref())?
            .iter()
            .any(|trigger| trigger.enabled && trigger.table_name == table)
        {
            return refuse("on a table with triggers");
        }
        let (_, alias) = table_with_joins_name_and_alias(&update.table)?;
        self.require_key_update_read_privileges(table, schema, update)?;
        let own_transaction = !self.in_transaction();
        if own_transaction {
            self.execute("BEGIN")?;
        }
        let mark = self.tx.as_ref().map(Transaction::rollback_mark);
        let ddl_undo_len = self.ddl_undo.len();
        let savepoint_len = self.savepoints.len();
        let notification_len = self.pending_notifications.len();
        let result = self.move_primary_key_records(update, table, schema, &alias, ctes);
        match result {
            Ok(count) => {
                if own_transaction {
                    self.execute("COMMIT")?;
                }
                Ok(Some(SqlResult::command(format!("UPDATE {count}"))))
            }
            Err(error) => {
                if own_transaction {
                    self.execute("ROLLBACK")?;
                } else {
                    if let (Some(tx), Some(mark)) = (self.tx.as_mut(), mark) {
                        tx.rollback_to_mark(mark)?;
                    }
                    self.rollback_ddl_to_len(ddl_undo_len)?;
                    self.savepoints.truncate(savepoint_len);
                }
                self.pending_notifications.truncate(notification_len);
                Err(error)
            }
        }
    }

    fn move_primary_key_records(
        &mut self,
        update: &sqlparser::ast::Update,
        table: &str,
        schema: &TableSchema,
        alias: &str,
        ctes: &BTreeMap<String, CteResult>,
    ) -> Result<usize> {
        let candidates = self.update_assignment_candidates_without_from(
            table,
            alias,
            Some(schema),
            update.selection.as_ref(),
            Arc::new(row_output_columns(table, alias, Some(schema))),
            ctes,
        )?;
        let reads_existing = update
            .selection
            .as_ref()
            .is_some_and(expr_references_stored_columns)
            || update
                .assignments
                .iter()
                .any(|a| expr_references_stored_columns(&a.value));
        let candidates = if reads_existing {
            self.retain_rls_select_visible(table, Some(schema), candidates, |c| &c.record)?
        } else {
            candidates
        };
        let mut pairs = Vec::with_capacity(candidates.len());
        let statement_uniqueness = crate::oracle::preview_routine_language_enabled(self.db_ref())?;
        let keys = primary_key_columns_for_schema(schema);
        let primary = unique_arbiters_for_table(self.db_ref(), table, schema)
            .iter()
            .find(|a| primary_key_columns_for_unique_arbiter(schema, a).is_some())
            .cloned()
            .ok_or_else(|| SqlError::Unsupported("primary-key arbiter is missing".into()))?;
        let mut seen = BTreeSet::new();
        for (index, candidate) in candidates.into_iter().enumerate() {
            if index % 1024 == 0 {
                self.cancellation.check()?;
            }
            let mut fields = schema
                .columns
                .iter()
                .map(|column| {
                    let value = if oracle::lob_storage::is_column(column) {
                        oracle::lob_storage::read(&candidate.record, table, &column.name)?
                    } else {
                        record_column_value(&candidate.record, schema, &column.name)
                    };
                    Ok((column.name.clone(), value))
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
            // All expressions read the original row, including when several
            // assignments mention a key that this same statement changes.
            for assignment in &update.assignments {
                if let AssignmentTarget::Tuple(names) = &assignment.target {
                    let values = self.update_row_assignment_value(
                        ctes,
                        candidate.columns.as_ref(),
                        &candidate.row,
                        assignment,
                    )?;
                    for (name, value) in names.iter().zip(values) {
                        fields.insert(relation_name(name)?, value);
                    }
                    continue;
                }
                let AssignmentTarget::ColumnName(name) = &assignment.target else {
                    return Err(SqlError::Unsupported(
                        "UPDATE supports only single-column assignments".into(),
                    ));
                };
                let column = relation_name(name)?;
                ensure_schema_column(table, schema, &column)?;
                if schema
                    .column(&column)
                    .is_some_and(|c| c.generated_expr.is_some())
                {
                    if expr_is_default(&assignment.value) {
                        continue;
                    }
                    return Err(SqlError::generated_always_violation(format!(
                        "column \"{column}\" can only be updated to DEFAULT"
                    )));
                }
                let value = if expr_is_default(&assignment.value) {
                    self.default_value_for_column(Some(schema), &column)?
                        .unwrap_or(SqlValue::Null)
                } else {
                    self.update_assignment_value(
                        ctes,
                        candidate.columns.as_ref(),
                        &candidate.row,
                        &assignment.value,
                    )?
                };
                fields.insert(column, value);
            }
            for column in &keys {
                if matches!(fields.get(column), Some(SqlValue::Null)) {
                    return Err(constraint_violation(
                        "23502",
                        format!("null value in column \"{column}\" of relation \"{}\" violates not-null constraint", catalog_display_name(table)),
                        Some(table.into()), Some(column.clone()),
                        Some(not_null_constraint_name(table, column)),
                    ));
                }
            }
            let built = record_from_fields_with_db(self.db_ref(), table, Some(schema), fields)?;
            let mut after = candidate.record.clone();
            after.id = built.id;
            // Keep non-SQL record attributes, while replacing SQL columns with
            // their typed storage values (including removal of SQL NULL JSON).
            for column in &schema.columns {
                if let Some(value) = built.metadata.get(&column.name) {
                    set_json_object_value(&mut after.metadata, &column.name, value.clone());
                } else {
                    remove_json_object_key(&mut after.metadata, &column.name);
                }
            }
            if schema.column("timestamp").is_some() {
                after.timestamp = built.timestamp;
            }
            if schema.column("payload").is_some() {
                after.payload = built.payload;
            }
            if schema
                .columns
                .iter()
                .any(|c| is_vector_column(Some(schema), &c.name))
            {
                after.vector = built.vector;
            }
            self.materialize_generated_columns(table, schema, std::slice::from_mut(&mut after))?;
            validate_record_local_constraints(table, schema, &after, Some(&self.sql_engine()))?;
            let new_key = record_column_values(&after, schema, &keys);
            if !seen.insert(typed_column_key_label(schema, &keys, &new_key)?) {
                return Err(unique_violation(schema, &primary.name));
            }
            if !statement_uniqueness {
                // PostgreSQL immediate checks observe earlier rows already
                // updated by this statement, but other old rows still own
                // their keys. This permits an ordered shift into vacated keys
                // while rejecting a cycle that needs statement-end checking.
                self.enforce_rls_write_checks(
                    table,
                    Some(schema),
                    PolicyAction::Update,
                    reads_existing,
                    std::slice::from_ref(&after),
                )?;
                self.delete_session_record(table, &candidate.record.id)?;
                validate_records_for_write(
                    self.db_ref(),
                    self.tx.as_ref(),
                    table,
                    schema,
                    std::slice::from_ref(&after),
                    false,
                    Some(&self.sql_engine()),
                )?;
                self.insert_session_records_for_key_move(
                    table,
                    std::iter::once((&candidate.record, after.clone())),
                )?;
            }
            pairs.push((candidate.record, after));
        }
        if pairs.is_empty() {
            return Ok(0);
        }
        if statement_uniqueness {
            let after = pairs
                .iter()
                .map(|(_, after)| after.clone())
                .collect::<Vec<_>>();
            self.enforce_rls_write_checks(
                table,
                Some(schema),
                PolicyAction::Update,
                reads_existing,
                &after,
            )?;
            let old_ids = pairs
                .iter()
                .map(|(before, _)| before.id.clone())
                .collect::<Vec<_>>();
            self.delete_session_records(table, &old_ids)?;
            // Oracle immediate uniqueness observes the completed statement.
            // The rollback mark also covers checks that fail after deletion.
            validate_records_for_write(
                self.db_ref(),
                self.tx.as_ref(),
                table,
                schema,
                &after,
                false,
                Some(&self.sql_engine()),
            )?;
            self.insert_session_records_for_key_move(
                table,
                pairs.iter().map(|(before, after)| (before, after.clone())),
            )?;
        }
        self.apply_foreign_key_parent_update_batch(table, schema, &pairs)?;
        self.publish_extension_database_events(
            table,
            DatabaseOperation::Update,
            pairs
                .iter()
                .map(|(before, after)| (Some(before), Some(after))),
        )?;
        Ok(pairs.len())
    }

    pub(super) fn require_key_update_read_privileges(
        &self,
        table: &str,
        schema: &TableSchema,
        update: &sqlparser::ast::Update,
    ) -> Result<()> {
        let role = current_user_from_gucs(&self.session_gucs);
        if role_has_table_privilege(self.db_ref(), &role, table, "SELECT")? {
            return Ok(());
        }
        // Column-only authorization below proves reads in the target row's
        // expression scope. Nested query scopes still need the existing
        // table SELECT contract until correlated-column ACL binding is shared
        // with the query planner; never ignore a potentially correlated read.
        if update
            .assignments
            .iter()
            .any(|a| expr_contains_subquery(&a.value))
            || update
                .selection
                .as_ref()
                .is_some_and(expr_contains_subquery)
        {
            return self.require_table_privilege(table, "SELECT");
        }
        struct ReadColumns {
            columns: BTreeSet<String>,
            query_depth: usize,
        }
        impl sqlparser::ast::Visitor for ReadColumns {
            type Break = ();
            fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
                self.query_depth += 1;
                ControlFlow::Continue(())
            }
            fn post_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
                self.query_depth -= 1;
                ControlFlow::Continue(())
            }
            fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
                if self.query_depth == 0 {
                    let id = match expr {
                        Expr::Identifier(id) => Some(id),
                        Expr::CompoundIdentifier(ids) => ids.last(),
                        _ => None,
                    };
                    if let Some(id) = id {
                        self.columns.insert(ident_value(id));
                    }
                }
                ControlFlow::Continue(())
            }
        }
        use sqlparser::ast::Visit;
        let mut reads = ReadColumns {
            columns: BTreeSet::new(),
            query_depth: 0,
        };
        for assignment in &update.assignments {
            let _ = assignment.value.visit(&mut reads);
        }
        if let Some(selection) = &update.selection {
            let _ = selection.visit(&mut reads);
        }
        let inherited = role_privilege_closure(self.db_ref(), &role)?;
        let grants = list_privileges(self.db_ref())?;
        for column in reads.columns {
            if schema.column(&column).is_some()
                && !grants.iter().any(|grant| {
                    grant.object_type == PrivilegeObjectType::Table
                        && grant.object_name == table
                        && grant.column.as_deref() == Some(column.as_str())
                        && grant.privilege == "SELECT"
                        && (grant.grantee == "public"
                            || inherited.contains(&normalize_role_name(&grant.grantee)))
                })
            {
                self.require_table_privilege(table, "SELECT")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(session: &mut SqlSession<'_>, profile: &str, sql: &str) -> Result<SqlResult> {
        if profile == "postgres" {
            return session.execute(sql);
        }
        let sql = crate::oracle::lower_session_sql(session.db_ref(), sql, profile, None, "bicdb")?;
        session.execute(&sql)
    }
    fn database(profile: &str) -> (tempfile::TempDir, BicDb) {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
        (root, db)
    }

    #[test]
    fn numeric_comparisons_ignore_decimal_spelling_in_every_row_shape() {
        for profile in ["postgres", "oracle-preview-v1"] {
            let (_root, mut db) = database(profile);
            let mut session = SqlSession::new(&mut db);
            for sql in [
                r#"CREATE TABLE "Amounts"("Id" INTEGER PRIMARY KEY,"Amt" NUMERIC(10),"Code" VARCHAR(10))"#,
                r#"CREATE TABLE "Scaled"("Id" INTEGER PRIMARY KEY,"Amt" NUMERIC(10,2))"#,
                r#"INSERT INTO "Amounts" VALUES(1,100,'007')"#,
                r#"INSERT INTO "Scaled" VALUES(1,100)"#,
            ] {
                run(&mut session, profile, sql).unwrap_or_else(|e| panic!("{profile} {sql}: {e}"));
            }
            let one = vec![vec![SqlValue::Int(1)]];
            for sql in [
                r#"SELECT a."Id" FROM "Amounts" a WHERE a."Amt"=100.000"#,
                r#"SELECT a."Id" FROM "Amounts" a WHERE a."Amt">=100.000 AND a."Amt"<=100.0"#,
                r#"SELECT a."Id" FROM "Amounts" a WHERE NOT (a."Amt"<>100.0)"#,
                r#"SELECT a."Id" FROM "Amounts" a WHERE a."Amt" IN (100.0, 7)"#,
                r#"SELECT "Id" FROM "Amounts" WHERE "Amt" IN (100.0, 7)"#,
                r#"SELECT CASE WHEN a."Amt"=100.0 THEN 1 ELSE 0 END FROM "Amounts" a"#,
                r#"SELECT CASE a."Amt" WHEN 100.0 THEN 1 ELSE 0 END FROM "Amounts" a"#,
                r#"SELECT COUNT(*) FROM "Amounts" GROUP BY "Amt" HAVING MAX("Amt")=100.0"#,
                r#"SELECT s."Id" FROM (SELECT "Id","Amt" AS "Q" FROM "Scaled") s WHERE s."Q"=100.0"#,
                r#"SELECT a."Id" FROM "Amounts" a WHERE EXISTS (SELECT 1 FROM "Scaled" s WHERE s."Amt"=a."Amt" AND s."Amt"=100.0)"#,
            ] {
                assert_eq!(
                    run(&mut session, profile, sql)
                        .unwrap_or_else(|e| panic!("{profile} {sql}: {e}"))
                        .rows,
                    one,
                    "{profile}: {sql}"
                );
            }
            // Text keeps text equality: '007' is not '7'.
            for sql in [
                r#"SELECT a."Id" FROM "Amounts" a WHERE a."Code"='7'"#,
                r#"SELECT a."Id" FROM "Amounts" a WHERE a."Code" IN ('7')"#,
            ] {
                assert!(
                    run(&mut session, profile, sql).unwrap().rows.is_empty(),
                    "{profile}: {sql}"
                );
            }
        }
    }

    #[test]
    fn ordered_ties_do_not_depend_on_whether_an_index_served_the_filter() {
        for profile in ["postgres", "oracle-preview-v1"] {
            for indexed in [false, true] {
                let (_root, mut db) = database(profile);
                let mut session = SqlSession::new(&mut db);
                run(&mut session, profile, r#"CREATE TABLE "Lists"("Id" NUMERIC(10) PRIMARY KEY,"Client" NUMERIC(10),"Flag" CHAR(1))"#).unwrap();
                if indexed {
                    run(
                        &mut session,
                        profile,
                        r#"CREATE INDEX "Lists_Client" ON "Lists"("Client")"#,
                    )
                    .unwrap();
                }
                // Inserted out of key order, so insertion order differs from key order.
                for id in [200000, 102, 150] {
                    run(
                        &mut session,
                        profile,
                        &format!(r#"INSERT INTO "Lists" VALUES({id},11,'N')"#),
                    )
                    .unwrap();
                }
                let rows = run(
                    &mut session,
                    profile,
                    r#"SELECT "Id" FROM "Lists" WHERE "Client"=11 ORDER BY "Flag" DESC"#,
                )
                .unwrap()
                .rows;
                assert_eq!(
                    rows,
                    ["102", "150", "200000"]
                        .map(|id| vec![SqlValue::String(id.into())])
                        .to_vec(),
                    "{profile} indexed={indexed}"
                );
            }
        }
    }

    #[test]
    fn update_with_correlated_exists_uses_statement_candidates() {
        for profile in ["postgres", "oracle-preview-v1"] {
            let (_root, mut db) = database(profile);
            let mut session = SqlSession::new(&mut db);
            for sql in [
                r#"CREATE TABLE "Cols"("ColId" NUMERIC(10) PRIMARY KEY,"ElemId" NUMERIC(10))"#,
                r#"CREATE TABLE "Items"("ItemId" NUMERIC(10) PRIMARY KEY,"ColId" NUMERIC(10),"Central" CHAR(1),"Name" VARCHAR(20))"#,
            ] {
                run(&mut session, profile, sql).unwrap();
            }
            for id in 1..=300 {
                run(
                    &mut session,
                    profile,
                    &format!(r#"INSERT INTO "Cols" VALUES({id},{})"#, id % 10),
                )
                .unwrap();
                run(
                    &mut session,
                    profile,
                    &format!(
                        r#"INSERT INTO "Items" VALUES({id},{id},'{}','old')"#,
                        if id % 2 == 0 { "Y" } else { "N" }
                    ),
                )
                .unwrap();
            }
            crate::SQL_PROFILE_STACK
                .with(|stack| stack.borrow_mut().push(crate::SqlProfileStats::default()));
            let updated = run(
                &mut session,
                profile,
                r#"UPDATE "Items" SET "Name"='new' WHERE "Central"='Y' AND EXISTS (SELECT * FROM "Cols" c WHERE c."ColId"="Items"."ColId" AND c."ElemId"=4)"#,
            )
            .unwrap();
            let stats = crate::SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
            // Even ids with id % 10 == 4: 4, 14, ..., 294.
            assert_eq!(updated.command_complete_tag(), "UPDATE 30", "{profile}");
            assert_eq!(
                run(
                    &mut session,
                    profile,
                    r#"SELECT COUNT(*) FROM "Items" WHERE "Name"='new'"#
                )
                .unwrap()
                .rows,
                vec![vec![SqlValue::Int(30)]],
                "{profile}"
            );
            // Candidate rows whose key has no qualifying inner row are answered
            // from the statement's candidate set, not by running the subquery.
            assert!(
                stats.index_lookup_count < 60,
                "{profile}: {} subquery lookups for 150 candidate rows",
                stats.index_lookup_count
            );
            let negated = run(
                &mut session,
                profile,
                r#"SELECT COUNT(*) FROM "Items" WHERE "Central"='Y' AND NOT EXISTS (SELECT 1 FROM "Cols" c WHERE c."ColId"="Items"."ColId" AND c."ElemId"=4)"#,
            )
            .unwrap();
            assert_eq!(negated.rows, vec![vec![SqlValue::Int(120)]], "{profile}");
        }
    }

    #[test]
    fn update_with_uncorrelated_in_subquery_runs_it_once() {
        for profile in ["postgres", "oracle-preview-v1"] {
            let (_root, mut db) = database(profile);
            let mut session = SqlSession::new(&mut db);
            for sql in [
                r#"CREATE TABLE "Cols"("ColId" NUMERIC(10) PRIMARY KEY,"ElemId" NUMERIC(10))"#,
                r#"CREATE TABLE "Fields"("FieldId" NUMERIC(10) PRIMARY KEY,"ColId" NUMERIC(10),"Central" CHAR(1),"Name" VARCHAR(20))"#,
            ] {
                run(&mut session, profile, sql).unwrap();
            }
            for id in 1..=300 {
                run(
                    &mut session,
                    profile,
                    &format!(r#"INSERT INTO "Cols" VALUES({id},{})"#, id % 10),
                )
                .unwrap();
                run(
                    &mut session,
                    profile,
                    &format!(r#"INSERT INTO "Fields" VALUES({id},{id},'Y','old')"#),
                )
                .unwrap();
            }
            crate::SQL_PROFILE_STACK
                .with(|stack| stack.borrow_mut().push(crate::SqlProfileStats::default()));
            let updated = run(
                &mut session,
                profile,
                r#"UPDATE "Fields" SET "Name"='new' WHERE "ColId" IN (SELECT "ColId" FROM "Cols" WHERE "ElemId"=4) AND "Central"='Y'"#,
            )
            .unwrap();
            let stats = crate::SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
            assert_eq!(updated.command_complete_tag(), "UPDATE 30", "{profile}");
            assert!(
                stats.full_scan_count <= 2,
                "{profile}: the uncorrelated subquery ran per row ({} scans)",
                stats.full_scan_count
            );
        }
    }

    #[test]
    fn typed_exact_key_probe_sees_same_transaction_rows_without_scanning() {
        for profile in ["postgres", "oracle-preview-v1"] {
            let (_root, mut db) = database(profile);
            let mut session = SqlSession::new(&mut db);
            for sql in [
                r#"CREATE TABLE "Parents"("P" NUMERIC(10) PRIMARY KEY,"Name" VARCHAR(20))"#,
                r#"CREATE TABLE "Children"("C" NUMERIC(10) PRIMARY KEY,"P" NUMERIC(10))"#,
                r#"INSERT INTO "Parents" VALUES(1,'one')"#,
                r#"INSERT INTO "Parents" VALUES(2,'two')"#,
                r#"INSERT INTO "Parents" VALUES(3,'three')"#,
                r#"INSERT INTO "Children" VALUES(10,1)"#,
                "BEGIN",
                r#"INSERT INTO "Children" VALUES(20,2)"#,
            ] {
                run(&mut session, profile, sql).unwrap_or_else(|e| panic!("{profile} {sql}: {e}"));
            }
            let query = r#"SELECT "Name" FROM "Parents" p WHERE EXISTS (SELECT 1 FROM "Children" c WHERE c."P"=p."P" AND c."C"=20)"#;
            crate::SQL_PROFILE_STACK
                .with(|stack| stack.borrow_mut().push(crate::SqlProfileStats::default()));
            let rows = run(&mut session, profile, query).map(|result| result.rows);
            let stats = crate::SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
            assert_eq!(
                rows.unwrap(),
                vec![vec![SqlValue::String("two".to_string())]],
                "{profile}"
            );
            assert_eq!(
                stats.record_id_prefix_scans, 0,
                "{profile}: an exact typed key written in this transaction must not scan"
            );
            // Exact keys equal by type but spelled differently still match.
            let spelled = r#"SELECT "Name" FROM "Parents" p WHERE EXISTS (SELECT 1 FROM "Children" c WHERE c."P"=p."P" AND c."C"=20.000)"#;
            assert_eq!(
                run(&mut session, profile, spelled).unwrap().rows,
                vec![vec![SqlValue::String("two".to_string())]],
                "{profile}"
            );
            // A missing NUMERIC key is answered from its canonical id alone.
            crate::SQL_PROFILE_STACK
                .with(|stack| stack.borrow_mut().push(crate::SqlProfileStats::default()));
            let missing = run(
                &mut session,
                profile,
                r#"SELECT "P" FROM "Children" WHERE "C"=999"#,
            )
            .map(|result| result.rows);
            let stats = crate::SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
            assert!(missing.unwrap().is_empty(), "{profile}");
            assert_eq!(
                stats.record_id_prefix_scans, 0,
                "{profile}: a key miss must not scan"
            );
            assert_eq!(
                run(
                    &mut session,
                    profile,
                    r#"SELECT "P" FROM "Children" WHERE "C"=10.00"#
                )
                .unwrap()
                .rows,
                vec![vec![SqlValue::String("1".to_string())]],
                "{profile}"
            );
            run(&mut session, profile, "ROLLBACK").unwrap();
            assert!(
                run(&mut session, profile, query).unwrap().rows.is_empty(),
                "{profile}: rolled-back key must not be visible"
            );
        }
    }

    #[test]
    fn key_move_qualified_names_preserve_types_aliases_and_other_schemas() {
        for profile in ["postgres", "oracle-preview-v1"] {
            let (root, mut db) = database(profile);
            {
                let mut session = SqlSession::new(&mut db);
                for sql in [
                    r#"CREATE SCHEMA "One""#,
                    r#"CREATE SCHEMA "Two""#,
                    r#"CREATE TABLE "One"."Rows"("K" INTEGER PRIMARY KEY,"V" VARCHAR(30),"N" NUMERIC(30,6),"T" TIMESTAMP(6))"#,
                    r#"CREATE TABLE "Two"."Rows"("K" INTEGER PRIMARY KEY,"V" VARCHAR(30))"#,
                    r#"INSERT INTO "One"."Rows" VALUES(1,'old',123456789012345678901234.123456,TIMESTAMP '2024-02-29 12:34:56.123456')"#,
                    r#"INSERT INTO "Two"."Rows" VALUES(1,'untouched')"#,
                ] {
                    run(&mut session, profile, sql)
                        .unwrap_or_else(|e| panic!("{profile} {sql}: {e}"));
                }
                let before = run(&mut session, profile, r#"SELECT "N","T" FROM "One"."Rows""#)
                    .unwrap()
                    .rows;
                run(
                    &mut session,
                    profile,
                    r#"UPDATE "One"."Rows" r SET "K"="K"+10,"V"="V"||'q' WHERE r."K"=1"#,
                )
                .unwrap();
                assert_eq!(
                    run(&mut session, profile, r#"SELECT "N","T" FROM "One"."Rows""#)
                        .unwrap()
                        .rows,
                    before
                );
                assert_eq!(
                    run(&mut session, profile, r#"SELECT "K","V" FROM "One"."Rows""#)
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::Int(11), SqlValue::String("oldq".into())]]
                );
                assert_eq!(
                    run(&mut session, profile, r#"SELECT "K","V" FROM "Two"."Rows""#)
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::Int(1), SqlValue::String("untouched".into())]]
                );
            }
            drop(db);
            let mut db = BicDb::open(root.path()).unwrap();
            assert_eq!(
                run(
                    &mut SqlSession::new(&mut db),
                    profile,
                    r#"SELECT "K" FROM "One"."Rows""#
                )
                .unwrap()
                .rows,
                vec![vec![SqlValue::Int(11)]]
            );
        }
    }

    #[test]
    fn key_move_uses_update_grants_and_original_assignment_values() {
        let (_root, mut db) = database("postgres");
        let mut s = SqlSession::new(&mut db);
        for sql in [
            "CREATE ROLE key_writer LOGIN",
            "CREATE SCHEMA key_acl",
            "CREATE TABLE key_acl.rows(k INT PRIMARY KEY,v INT,secret TEXT)",
            "INSERT INTO key_acl.rows VALUES(1,10,'private')",
            "GRANT USAGE ON SCHEMA key_acl TO key_writer",
            "GRANT SELECT ON key_acl.rows TO key_writer",
            "GRANT UPDATE(k) ON key_acl.rows TO key_writer",
            "SET SESSION AUTHORIZATION key_writer",
            "UPDATE key_acl.rows SET k=k+1 WHERE k=1",
        ] {
            s.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        assert_eq!(
            s.execute("UPDATE key_acl.rows SET k=3,v=4 WHERE k=2")
                .unwrap_err()
                .sqlstate(),
            "42501"
        );
        s.execute("RESET SESSION AUTHORIZATION").unwrap();
        assert_eq!(
            s.execute("SELECT k,v,secret FROM key_acl.rows")
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::Int(2),
                SqlValue::Int(10),
                SqlValue::String("private".into())
            ]]
        );
        s.execute("UPDATE key_acl.rows SET k=k+10,v=k WHERE k=2")
            .unwrap();
        assert_eq!(
            s.execute("SELECT k,v FROM key_acl.rows").unwrap().rows,
            vec![vec![SqlValue::Int(12), SqlValue::Int(2)]]
        );
        s.execute("REVOKE SELECT ON key_acl.rows FROM key_writer")
            .unwrap();
        s.execute("SET SESSION AUTHORIZATION key_writer").unwrap();
        assert_eq!(
            s.execute("UPDATE key_acl.rows SET k=v")
                .unwrap_err()
                .sqlstate(),
            "42501"
        );
        // A constant assignment with no old-column reads requires UPDATE only.
        s.execute("UPDATE key_acl.rows SET k=13").unwrap();
    }

    #[test]
    fn key_move_uses_update_policies_and_rejects_checks_without_data_loss() {
        let (_root, mut db) = database("postgres");
        let mut s = SqlSession::new(&mut db);
        for sql in [
            "CREATE ROLE key_writer LOGIN",
            "CREATE SCHEMA key_rls",
            "CREATE TABLE key_rls.rows(k INT PRIMARY KEY,tenant TEXT)",
            "INSERT INTO key_rls.rows VALUES(1,'key_writer'),(2,'other')",
            "GRANT USAGE ON SCHEMA key_rls TO key_writer",
            "GRANT SELECT,UPDATE ON key_rls.rows TO key_writer",
            "ALTER TABLE key_rls.rows ENABLE ROW LEVEL SECURITY",
            "CREATE POLICY read_rows ON key_rls.rows FOR SELECT USING(true)",
            "CREATE POLICY update_rows ON key_rls.rows FOR UPDATE USING(tenant=current_user) WITH CHECK(tenant=current_user AND k<100)",
            "SET SESSION AUTHORIZATION key_writer",
            "UPDATE key_rls.rows SET k=k+10",
        ] { s.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")); }
        assert_eq!(
            s.execute("UPDATE key_rls.rows SET k=101 WHERE k=11")
                .unwrap_err()
                .sqlstate(),
            "42501"
        );
        assert_eq!(
            s.execute("SELECT k FROM key_rls.rows ORDER BY k")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(2)], vec![SqlValue::Int(11)]]
        );
        s.execute("RESET SESSION AUTHORIZATION").unwrap();
        s.execute("DROP POLICY read_rows ON key_rls.rows").unwrap();
        s.execute("SET SESSION AUTHORIZATION key_writer").unwrap();
        s.execute("UPDATE key_rls.rows SET k=12 WHERE k=11")
            .unwrap();
        // Without column reads the SELECT policy does not suppress UPDATE.
        s.execute("UPDATE key_rls.rows SET k=13").unwrap();
        s.execute("RESET SESSION AUTHORIZATION").unwrap();
        assert_eq!(
            s.execute("SELECT k FROM key_rls.rows ORDER BY k")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(2)], vec![SqlValue::Int(13)]]
        );
    }

    #[test]
    fn key_move_constraints_errors_and_transaction_rollback_are_atomic() {
        for profile in ["postgres", "oracle-preview-v1"] {
            let (_root, mut db) = database(profile);
            let mut s = SqlSession::new(&mut db);
            for sql in [
                "CREATE SCHEMA key_atomic",
                "CREATE TABLE key_atomic.rows(k INT PRIMARY KEY,v INT UNIQUE CHECK(v>0))",
                "INSERT INTO key_atomic.rows VALUES(1,10),(2,20)",
            ] {
                run(&mut s, profile, sql).unwrap();
            }
            for (sql, code) in [
                ("UPDATE key_atomic.rows SET k=2 WHERE k=1", "23505"),
                ("UPDATE key_atomic.rows SET k=3-k", "23505"),
                ("UPDATE key_atomic.rows SET k=7", "23505"),
                ("UPDATE key_atomic.rows SET k=NULL WHERE k=1", "23502"),
                ("UPDATE key_atomic.rows SET k=7,v=0 WHERE k=1", "23514"),
                ("UPDATE key_atomic.rows SET k=7,v=20 WHERE k=1", "23505"),
            ] {
                if profile == "oracle-preview-v1" && sql == "UPDATE key_atomic.rows SET k=3-k" {
                    continue;
                }
                assert_eq!(
                    run(&mut s, profile, sql).unwrap_err().sqlstate(),
                    code,
                    "{profile} {sql}"
                );
                assert_eq!(
                    run(
                        &mut s,
                        profile,
                        "SELECT k,v FROM key_atomic.rows ORDER BY k"
                    )
                    .unwrap()
                    .rows,
                    vec![
                        vec![SqlValue::Int(1), SqlValue::Int(10)],
                        vec![SqlValue::Int(2), SqlValue::Int(20)]
                    ]
                );
            }
            run(&mut s, profile, "BEGIN").unwrap();
            run(&mut s, profile, "INSERT INTO key_atomic.rows VALUES(3,30)").unwrap();
            assert_eq!(
                run(&mut s, profile, "UPDATE key_atomic.rows SET k=3 WHERE k=1")
                    .unwrap_err()
                    .sqlstate(),
                "23505"
            );
            run(&mut s, profile, "UPDATE key_atomic.rows SET k=4 WHERE k=1").unwrap();
            run(&mut s, profile, "ROLLBACK").unwrap();
            assert_eq!(
                run(&mut s, profile, "SELECT k FROM key_atomic.rows ORDER BY k")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
            );
        }
    }

    #[test]
    fn key_move_composite_keys_defaults_generated_values_and_expression_calls() {
        let (_root, mut db) = database("postgres");
        let mut s = SqlSession::new(&mut db);
        for sql in [
            "CREATE SCHEMA key_composite",
            "CREATE SEQUENCE key_sequence START 50",
            "CREATE TABLE key_composite.rows(a NUMERIC(20,3) DEFAULT 9,b TEXT,v INT DEFAULT 7,g INT GENERATED ALWAYS AS(v*2) STORED,PRIMARY KEY(a,b))",
            "INSERT INTO key_composite.rows(a,b,v) VALUES(1.250,'x',3)",
            "UPDATE key_composite.rows SET a=nextval('key_sequence'),v=DEFAULT,g=DEFAULT WHERE b='x'",
        ] { s.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")); }
        assert_eq!(
            s.execute("SELECT currval('key_sequence')").unwrap().rows,
            vec![vec![SqlValue::Int(50)]]
        );
        let before = s
            .execute("SELECT a,b,v,g FROM key_composite.rows")
            .unwrap()
            .rows;
        assert_eq!(before[0][2..], [SqlValue::Int(7), SqlValue::Int(14)]);
        s.execute("UPDATE key_composite.rows SET a=a WHERE b='x'")
            .unwrap();
        assert_eq!(
            s.execute("SELECT a,b,v,g FROM key_composite.rows")
                .unwrap()
                .rows,
            before
        );
        s.execute("UPDATE key_composite.rows SET a=DEFAULT,b='y'")
            .unwrap();
        assert_eq!(
            s.execute("SELECT a FROM key_composite.rows WHERE b='y'")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "9.000"
        );
    }

    #[test]
    fn key_move_failed_reference_restores_old_key_and_preserves_parent_transaction() {
        for profile in ["postgres", "oracle-preview-v1"] {
            let (_root, mut db) = database(profile);
            let mut s = SqlSession::new(&mut db);
            for sql in [
                "CREATE SCHEMA key_reference",
                "CREATE TABLE key_reference.parents(k INT PRIMARY KEY)",
                "CREATE TABLE key_reference.children(k INT PRIMARY KEY,p INT REFERENCES key_reference.parents(k))",
                "INSERT INTO key_reference.parents VALUES(1)",
                "INSERT INTO key_reference.children VALUES(10,1)",
                "BEGIN",
                "INSERT INTO key_reference.children VALUES(20,1)",
            ] { run(&mut s,profile,sql).unwrap_or_else(|e|panic!("{sql}: {e}")); }
            let error = run(
                &mut s,
                profile,
                "UPDATE key_reference.children SET k=99,p=999 WHERE k=10",
            )
            .unwrap_err();
            assert_eq!(error.sqlstate(), "23503");
            assert_eq!(
                run(
                    &mut s,
                    profile,
                    "SELECT k FROM key_reference.children ORDER BY k"
                )
                .unwrap()
                .rows,
                vec![vec![SqlValue::Int(10)], vec![SqlValue::Int(20)]]
            );
            run(&mut s, profile, "COMMIT").unwrap();
            assert_eq!(
                run(
                    &mut s,
                    profile,
                    "UPDATE key_reference.parents SET k=2 WHERE k=1"
                )
                .unwrap_err()
                .sqlstate(),
                "23503"
            );
        }
    }

    #[test]
    fn key_move_keeps_record_attributes_and_reserved_name_fence() {
        let (_root, mut db) = database("postgres");
        {
            let mut s = SqlSession::new(&mut db);
            s.execute("CREATE SCHEMA key_attributes").unwrap();
            s.execute("CREATE TABLE key_attributes.rows(k INT PRIMARY KEY,v TEXT)")
                .unwrap();
            s.execute("INSERT INTO key_attributes.rows VALUES(1,'kept')")
                .unwrap();
        }
        let table = relation_name_from_parts(&["key_attributes".into(), "rows".into()]).unwrap();
        let mut record = (*db.get(&table, "1").unwrap().unwrap()).clone();
        record.payload = Some(vec![0, 255, 10]);
        record.timestamp = Some(123456);
        record.metadata["non_sql_attribute"] = JsonValue::String("kept".into());
        db.insert(&table, record.clone()).unwrap();
        {
            let mut s = SqlSession::new(&mut db);
            s.execute("UPDATE key_attributes.rows SET k=2 WHERE k=1")
                .unwrap();
            let error = s
                .execute(&format!(
                    "INSERT INTO \"{table}\"(k,v) VALUES(3,'forbidden')"
                ))
                .unwrap_err();
            assert_eq!(error.sqlstate(), "42601");
        }
        record.id = "2".into();
        assert_eq!(*db.get(&table, "2").unwrap().unwrap(), record);
        assert!(db.get(&table, "1").unwrap().is_none());
    }

    #[test]
    fn key_move_uniqueness_timing_follows_the_durable_profile() {
        for profile in ["postgres", "oracle-preview-v1"] {
            let (_root, mut db) = database(profile);
            let mut s = SqlSession::new(&mut db);
            for sql in [
                "CREATE TABLE key_swap(k INT PRIMARY KEY,v INT UNIQUE)",
                "INSERT INTO key_swap VALUES(1,10),(2,20)",
            ] {
                run(&mut s, profile, sql).unwrap();
            }
            for sql in [
                "UPDATE key_swap SET k=3-k",
                "UPDATE key_swap SET k=k,v=30-v",
            ] {
                let result = run(&mut s, profile, sql);
                if profile == "postgres" {
                    assert_eq!(result.unwrap_err().sqlstate(), "23505");
                } else {
                    result.unwrap();
                }
            }
            // Both Oracle swaps together restore the key/value association;
            // PostgreSQL rejects both statements without changing any row.
            assert_eq!(
                run(&mut s, profile, "SELECT k,v FROM key_swap ORDER BY k")
                    .unwrap()
                    .rows,
                vec![
                    vec![SqlValue::Int(1), SqlValue::Int(10)],
                    vec![SqlValue::Int(2), SqlValue::Int(20)]
                ]
            );
            run(&mut s, profile, "UPDATE key_swap SET k=k-1").unwrap();
            assert_eq!(
                run(&mut s, profile, "SELECT k,v FROM key_swap ORDER BY k")
                    .unwrap()
                    .rows,
                vec![
                    vec![SqlValue::Int(0), SqlValue::Int(10)],
                    vec![SqlValue::Int(1), SqlValue::Int(20)]
                ]
            );
        }
    }
}
