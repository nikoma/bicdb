//! SQL rename producer for the durable before-image journal. This prototype
//! retains conservative native quiescence checks. Scoped admission, concurrent
//! transaction visibility and bounded value mutation remain qualification gates.
use super::*;

fn move_source(session: &mut SqlSession<'_>, old: &str, new: &str) -> Result<()> {
    let mut owner = session.tx.take();
    let result = session.db_mut().and_then(|db| match owner.as_mut() {
        Some(tx) => db
            .rename_collection_preserving_pending_writes(tx, old, new)
            .map_err(Into::into),
        None => db.rename_collection(old, new).map_err(Into::into),
    });
    // Refused moves must retain the caller's original transaction too.
    session.tx = owner;
    result
}

pub(super) fn apply(
    session: &mut SqlSession<'_>,
    old: &str,
    new: &str,
    schema: &mut TableSchema,
) -> Result<()> {
    require_user_relation_name(new)?;
    if user_collection_names(session.db_ref())
        .iter()
        .any(|name| name == new)
        || load_schema(session.db_ref(), new)?.is_some()
        || load_view(session.db_ref(), new)?.is_some()
        || load_sequence(session.db_ref(), new)?.is_some()
    {
        return Err(SqlError::InvalidSql(format!(
            "relation \"{new}\" already exists"
        )));
    }
    // Fail shared-session fallback before publishing a journal or format flag.
    session.db_mut()?;
    match session.tx.as_ref() {
        Some(tx) => session.db_ref().check_catalog_undo_owner(tx)?,
        None => session.db_ref().check_catalog_recovery_quiescent()?,
    }
    let previous = schema.clone();
    let mut updated = previous.clone();
    updated.name = new.into();
    if updated.primary_key_name.is_none() && updated.primary_key_column().is_some() {
        updated.primary_key_name = Some(previous.primary_key_constraint_name());
    }
    // Name/type binding must still see the source's original spelling.
    let views = rename_views::plan_queries(session.db_ref(), old, new)?;
    let dependencies =
        rename_dependencies::RenameDependencies::plan(session.db_ref(), &previous, &updated)?;
    let prepared = dependencies.capture(session.db_mut()?, old, new)?;
    let explicit = session.tx.is_some();
    if explicit {
        session.ddl_undo.push(DdlUndo::RestoreRenameJournal {
            id: prepared.id().into(),
        });
    }
    let result = (|| {
        move_source(session, old, new)?;
        delete_schema(session.db_mut()?, old)?;
        *schema = updated;
        session.save_session_schema(schema)?;
        for mut sequence in list_sequences(session.db_ref())? {
            if sequence.owned_by_table.as_deref() == Some(old) {
                sequence.owned_by_table = Some(new.into());
                session.save_session_sequence(&sequence)?;
            }
        }
        for mut dependent in list_schemas(session.db_ref())? {
            let mut changed = false;
            for constraint in &mut dependent.constraints {
                if let ConstraintSchema::ForeignKey { foreign_table, .. } = constraint {
                    if foreign_table == old {
                        *foreign_table = new.into();
                        changed = true;
                    }
                }
            }
            if let Some(partition) = &mut dependent.partition_of {
                if partition.parent_table == old {
                    partition.parent_table = new.into();
                    changed = true;
                }
            }
            if changed {
                session.save_session_schema(&dependent)?;
                if dependent.name == new {
                    *schema = dependent;
                }
            }
        }
        for mut grant in list_privileges(session.db_ref())? {
            if grant.object_type == PrivilegeObjectType::Table && grant.object_name == old {
                session.delete_session_privilege(&grant)?;
                grant.object_name = new.into();
                session.save_session_privilege(&grant)?;
            }
        }
        for mut trigger in list_triggers(session.db_ref())? {
            if trigger.table_name == old {
                session.delete_session_trigger(&trigger.name, Some(old))?;
                trigger.table_name = new.into();
                session.save_session_trigger(&trigger)?;
            }
        }
        for view in views {
            session.save_session_view(&view)?;
        }
        let name = table_row_type_schema(schema)
            .column_type(false)
            .formatted_name();
        session.apply_table_row_type_change(
            &previous,
            schema,
            &CompositeAttributeMutation::RenameType { name },
        )?;
        sql_index_definitions_cache_clear();
        Ok(())
    })();
    if let Err(error) = result {
        if !explicit {
            // If durability is fenced, this refuses and retains the manifest
            // for startup recovery. Never erase uncertain durable evidence.
            rename_journal::rollback(session.db_mut()?, prepared.id())?;
        }
        return Err(error);
    }
    // The manifest decision belongs to the business transaction. A commit
    // error is propagated directly, never reinterpreted as a rollback.
    match session.tx.as_mut() {
        Some(tx) => rename_journal::decide(&prepared, tx)?,
        None => {
            let mut tx = session.db_ref().begin_transaction()?;
            rename_journal::decide(&prepared, &mut tx)?;
            tx.commit()?;
        }
    }
    Ok(())
}
