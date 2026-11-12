use crate::*;

impl SqlSession<'_> {
    /// Route the complete statement before staging any physical write. The
    /// outer statement transaction owns every leaf write and its rollback.
    pub(crate) fn insert_partition_records(
        &mut self,
        schema: &TableSchema,
        records: &[Record],
    ) -> Result<()> {
        let mut groups: BTreeMap<String, (TableSchema, Vec<Record>)> = BTreeMap::new();
        for record in records {
            let target = partition::route_record(self.db_ref(), schema, record)?;
            groups
                .entry(target.name.clone())
                .or_insert_with(|| (target, Vec::new()))
                .1
                .push(record.clone());
        }
        // Physical unique/foreign-key constraints must be checked against the
        // destination's actual heap, never just the empty parent collection.
        for (target, records) in groups.values() {
            validate_records_for_write(
                self.db_ref(),
                self.read_tx(),
                &target.name,
                target,
                records,
                false,
                Some(&self.sql_engine()),
            )?;
        }
        for (name, (target, records)) in groups {
            self.insert_session_new_records(&name, Some(&target), records)?;
        }
        Ok(())
    }
}
