//! Extended statistics objects: `CREATE STATISTICS`, `ALTER STATISTICS`,
//! `DROP STATISTICS` and `pg_statistic_ext`.
//!
//! The objects are recorded with PostgreSQL's validation (columns exist,
//! at least two columns unless an expression is given, unique names, table
//! ownership) so DDL and catalog queries behave as in PostgreSQL. BicDB's
//! planner does not use them; they carry no data (`pg_statistic_ext_data`
//! stays empty), which PostgreSQL also shows before ANALYZE.

use crate::*;

pub(crate) const STATS_COLLECTION: &str = "__bicdb_pg_statistic_ext";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct StatsObject {
    pub(crate) name: String,
    pub(crate) schema: String,
    /// Physical relation name.
    pub(crate) table: String,
    pub(crate) columns: Vec<String>,
    pub(crate) expressions: Vec<String>,
    /// 'd' ndistinct, 'f' dependencies, 'm' mcv, 'e' expressions.
    pub(crate) kinds: Vec<char>,
    pub(crate) owner: String,
    pub(crate) stattarget: Option<i64>,
}

fn key(schema: &str, name: &str) -> String {
    format!("{schema}.{name}")
}

pub(crate) fn list_stats(db: &BicDb) -> Result<Vec<StatsObject>> {
    let records = match db.scan_collection(STATS_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    let mut objects = records
        .into_iter()
        .map(|record| serde_json::from_value(record.metadata).map_err(SqlError::from))
        .collect::<Result<Vec<StatsObject>>>()?;
    // Statistics go with their table.
    objects.retain(|object| relation_exists(db, &object.table));
    objects.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(objects)
}

fn save(db: &mut BicDb, object: &StatsObject) -> Result<()> {
    db.create_collection(STATS_COLLECTION)?;
    db.insert(
        STATS_COLLECTION,
        Record::new(key(&object.schema, &object.name)).with_metadata(serde_json::to_value(object)?),
    )?;
    Ok(())
}

fn delete(db: &mut BicDb, schema: &str, name: &str) -> Result<()> {
    match db.delete(STATS_COLLECTION, &key(schema, name)) {
        Ok(_) | Err(BicDbError::CollectionNotFound(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn restore_stats(
    db: &mut BicDb,
    previous: Option<&StatsObject>,
    schema: &str,
    name: &str,
) -> Result<()> {
    match previous {
        Some(object) => save(db, object),
        None => delete(db, schema, name),
    }
}

fn syntax(message: impl Into<String>) -> SqlError {
    SqlError::data_exception_public("42601", message.into(), None)
}

fn split_name(name: &str) -> (String, String) {
    let parts = crate::sql_identifier_parts(name).unwrap_or_else(|_| vec![name.to_string()]);
    match parts.as_slice() {
        [schema, name] => (schema.clone(), name.clone()),
        _ => (
            "public".to_string(),
            parts.last().cloned().unwrap_or_default(),
        ),
    }
}

fn undefined_object(name: &str) -> SqlError {
    SqlError::data_exception_public(
        "42704",
        format!("statistics object \"{name}\" does not exist"),
        None,
    )
}

impl SqlSession<'_> {
    fn record_stats_undo(&mut self, schema: &str, name: &str) -> Result<()> {
        if self.tx.is_some() {
            let previous = list_stats(self.db_ref())?
                .into_iter()
                .find(|object| object.schema == schema && object.name == name);
            self.ddl_undo
                .push(crate::session::DdlUndo::RestoreStatistics {
                    schema: schema.to_string(),
                    name: name.to_string(),
                    previous: previous.map(Box::new),
                });
        }
        Ok(())
    }

    /// CREATE STATISTICS / ALTER STATISTICS / DROP STATISTICS.
    pub(crate) fn execute_raw_statistics(&mut self, sql: &str) -> Result<Option<SqlResult>> {
        let text = sql.trim().trim_end_matches(';').trim();
        let words = split_sql_words(text);
        let lower = words
            .iter()
            .take(2)
            .map(|word| word.to_ascii_lowercase())
            .collect::<Vec<_>>();
        match lower.as_slice() {
            [create, statistics] if create == "create" && statistics == "statistics" => {}
            [alter, statistics] if alter == "alter" && statistics == "statistics" => {
                return self.alter_statistics(&words).map(Some);
            }
            [drop, statistics] if drop == "drop" && statistics == "statistics" => {
                return self.drop_statistics(text).map(Some);
            }
            _ => return Ok(None),
        }
        if split_sql_statements(sql).len() != 1 {
            return Ok(None);
        }
        // CREATE STATISTICS [IF NOT EXISTS] [name] [(kinds)] ON items FROM table
        let rest = text[text
            .to_ascii_lowercase()
            .find("statistics")
            .expect("keyword")
            + 10..]
            .trim();
        let on_at = find_top_level_keyword(rest, "ON")
            .ok_or_else(|| syntax("CREATE STATISTICS requires ON"))?;
        let from_at = find_top_level_keyword(&rest[on_at..], "FROM")
            .map(|at| at + on_at)
            .ok_or_else(|| syntax("CREATE STATISTICS requires FROM"))?;
        let mut head = rest[..on_at].trim();
        let mut if_not_exists = false;
        if head.len() >= 13 && head[..13].eq_ignore_ascii_case("if not exists") {
            if_not_exists = true;
            head = head[13..].trim();
        }
        let (name_text, kinds_text) = match head.find('(') {
            Some(open) => (
                head[..open].trim(),
                Some(head[open + 1..head.rfind(')').unwrap_or(head.len())].trim()),
            ),
            None => (head, None),
        };
        let items = split_top_level_commas_nested(rest[on_at + 2..from_at].trim());
        let table_text = rest[from_at + 4..].trim();
        let table_name = crate::pg_operators::parse_sql_data_type(table_text)
            .ok()
            .map(|_| table_text.to_string())
            .unwrap_or_else(|| table_text.to_string());
        let parts = sql_identifier_parts(&table_name)?;
        let relation = relation_name_from_parts(&parts)?;
        let table = resolve_session_relation_name_if_exists(self.db_ref(), &relation);
        let Some(schema) = load_schema(self.db_ref(), &table)? else {
            return Err(SqlError::data_exception_public(
                "42P01",
                format!("relation \"{table_text}\" does not exist"),
                None,
            ));
        };
        let mut columns = Vec::new();
        let mut expressions = Vec::new();
        for item in &items {
            let item = item.trim();
            if item.starts_with('(') {
                expressions.push(item[1..item.len() - 1].trim().to_string());
                continue;
            }
            let parts = sql_identifier_parts(item)?;
            let column = parts.last().cloned().unwrap_or_default();
            // `lower(b)` without parentheses is a syntax error in PostgreSQL
            // unless it is a function call, which is an expression.
            if item.contains('(') {
                expressions.push(item.to_string());
                continue;
            }
            if schema
                .column(&column)
                .filter(|column| !column.hidden)
                .is_none()
            {
                return Err(SqlError::data_exception_public(
                    "42703",
                    format!("column \"{column}\" does not exist"),
                    None,
                ));
            }
            if columns.contains(&column) {
                return Err(SqlError::data_exception_public(
                    "42701",
                    "duplicate column name in statistics definition",
                    None,
                ));
            }
            columns.push(column);
        }
        if columns.len() + expressions.len() < 2 && expressions.is_empty() {
            return Err(SqlError::data_exception_public(
                "42P16",
                "extended statistics require at least 2 columns",
                None,
            ));
        }
        let mut kinds = Vec::new();
        if let Some(kinds_text) = kinds_text {
            for kind in kinds_text.split(',') {
                kinds.push(match kind.trim().to_ascii_lowercase().as_str() {
                    "ndistinct" => 'd',
                    "dependencies" => 'f',
                    "mcv" => 'm',
                    other => {
                        return Err(SqlError::data_exception_public(
                            "22023",
                            format!("unrecognized statistics kind \"{other}\""),
                            None,
                        ));
                    }
                });
            }
        } else if columns.len() + expressions.len() >= 2 {
            kinds = vec!['d', 'f', 'm'];
        }
        if !expressions.is_empty() {
            kinds.push('e');
        }
        self.require_table_ownership(&table, "create statistics on it")?;
        let existing = list_stats(self.db_ref())?;
        let (stats_schema, name) = if name_text.is_empty() {
            // PostgreSQL's chosen name: table_col1_col2_stat[N].
            let mut base = schema.name.clone();
            for column in &columns {
                base.push('_');
                base.push_str(column);
            }
            if !expressions.is_empty() {
                base.push_str("_expr");
            }
            base.push_str("_stat");
            let mut candidate = base.clone();
            let mut suffix = 1;
            while existing.iter().any(|object| object.name == candidate) {
                candidate = format!("{base}{suffix}");
                suffix += 1;
            }
            (schema.schema_name.clone(), candidate)
        } else {
            split_name(name_text)
        };
        if existing
            .iter()
            .any(|object| object.schema == stats_schema && object.name == name)
        {
            if if_not_exists {
                return Ok(Some(SqlResult::command("CREATE STATISTICS")));
            }
            return Err(SqlError::data_exception_public(
                "42P07",
                format!("statistics object \"{name}\" already exists"),
                None,
            ));
        }
        let object = StatsObject {
            name,
            schema: stats_schema,
            table,
            columns,
            expressions,
            kinds,
            owner: current_user_from_gucs(&self.session_gucs),
            stattarget: None,
        };
        self.record_stats_undo(&object.schema, &object.name)?;
        save(self.db_mut()?, &object)?;
        Ok(Some(SqlResult::command("CREATE STATISTICS")))
    }

    fn find_statistics(&self, name: &str) -> Result<Option<StatsObject>> {
        let (schema, name) = split_name(name);
        Ok(list_stats(self.db_ref())?
            .into_iter()
            .find(|object| object.name == name && object.schema == schema))
    }

    fn require_statistics_owner(&self, object: &StatsObject) -> Result<()> {
        if self.current_user_is_superuser()? || self.current_user_holds_role(&object.owner)? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "must be owner of statistics object {}",
            object.name
        ))))
    }

    fn alter_statistics(&mut self, words: &[String]) -> Result<SqlResult> {
        // ALTER STATISTICS [IF EXISTS] name { RENAME TO n | SET STATISTICS n
        // | OWNER TO r | SET SCHEMA s }
        let mut index = 2;
        let mut if_exists = false;
        if words
            .get(2)
            .is_some_and(|word| word.eq_ignore_ascii_case("if"))
            && words
                .get(3)
                .is_some_and(|word| word.eq_ignore_ascii_case("exists"))
        {
            if_exists = true;
            index = 4;
        }
        let name = words
            .get(index)
            .ok_or_else(|| syntax("ALTER STATISTICS requires a name"))?;
        let Some(mut object) = self.find_statistics(name)? else {
            if if_exists {
                return Ok(SqlResult::command("ALTER STATISTICS"));
            }
            return Err(undefined_object(&split_name(name).1));
        };
        self.require_statistics_owner(&object)?;
        let action = words[index + 1..]
            .iter()
            .map(|word| word.to_ascii_lowercase())
            .collect::<Vec<_>>();
        let original = (object.schema.clone(), object.name.clone());
        match action
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice()
        {
            ["rename", "to", _] => {
                let new_name = split_name(&words[index + 3]).1;
                if list_stats(self.db_ref())?
                    .iter()
                    .any(|other| other.schema == object.schema && other.name == new_name)
                {
                    return Err(SqlError::data_exception_public(
                        "42P07",
                        format!(
                            "statistics object \"{new_name}\" already exists in schema \"{}\"",
                            object.schema
                        ),
                        None,
                    ));
                }
                object.name = new_name;
            }
            ["set", "statistics", value] => {
                let value = if *value == "default" {
                    None
                } else {
                    let target = value
                        .parse::<i64>()
                        .map_err(|_| syntax("invalid statistics target"))?;
                    if !(-1..=10000).contains(&target) {
                        return Err(SqlError::invalid_parameter_value(format!(
                            "statistics target {target} is out of range"
                        )));
                    }
                    (target >= 0).then_some(target)
                };
                object.stattarget = value;
            }
            ["owner", "to", _] => object.owner = crate::normalize_role_name(&words[index + 3]),
            ["set", "schema", _] => object.schema = normalize_object_name(&words[index + 3]),
            _ => return Err(syntax("unsupported ALTER STATISTICS form")),
        }
        self.record_stats_undo(&original.0, &original.1)?;
        delete(self.db_mut()?, &original.0, &original.1)?;
        self.record_stats_undo(&object.schema, &object.name)?;
        save(self.db_mut()?, &object)?;
        Ok(SqlResult::command("ALTER STATISTICS"))
    }

    fn drop_statistics(&mut self, text: &str) -> Result<SqlResult> {
        let mut rest = text[text
            .to_ascii_lowercase()
            .find("statistics")
            .expect("keyword")
            + 10..]
            .trim();
        let mut if_exists = false;
        if rest.len() >= 9 && rest[..9].eq_ignore_ascii_case("if exists") {
            if_exists = true;
            rest = rest[9..].trim();
        }
        let lower = rest.to_ascii_lowercase();
        for tail in [" cascade", " restrict"] {
            if lower.ends_with(tail) {
                rest = rest[..rest.len() - tail.len()].trim_end();
            }
        }
        let mut targets = Vec::new();
        for name in split_top_level_commas_nested(rest) {
            match self.find_statistics(name.trim())? {
                Some(object) => targets.push(object),
                None if if_exists => {}
                None => return Err(undefined_object(&split_name(name.trim()).1)),
            }
        }
        for object in targets {
            self.require_statistics_owner(&object)?;
            self.record_stats_undo(&object.schema, &object.name)?;
            delete(self.db_mut()?, &object.schema, &object.name)?;
        }
        Ok(SqlResult::command("DROP STATISTICS"))
    }
}

/// `pg_statistic_ext` rows.
pub(crate) fn pg_statistic_ext_rows(db: &BicDb) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let table_oids = table_oids(db);
    let mut rows = Vec::new();
    for object in list_stats(db)? {
        let schema = load_schema(db, &object.table)?;
        let keys = object
            .columns
            .iter()
            .filter_map(|column| {
                schema
                    .as_ref()?
                    .columns
                    .iter()
                    .position(|candidate| &candidate.name == column)
                    .map(|index| (index + 1).to_string())
            })
            .collect::<Vec<_>>()
            .join(" ");
        rows.push(virtual_row([
            (
                "oid",
                SqlValue::Int(stable_name_hash_wide(
                    1_900_000_000,
                    &format!("pg_statistic_ext:{}", key(&object.schema, &object.name)),
                    100_000_000,
                )),
            ),
            (
                "stxrelid",
                SqlValue::Int(*table_oids.get(&object.table).unwrap_or(&0)),
            ),
            ("stxname", SqlValue::String(object.name.clone())),
            ("stxnamespace", SqlValue::Int(namespace_oid(&object.schema))),
            ("stxowner", SqlValue::Int(role_oid(&object.owner))),
            ("stxkeys", SqlValue::String(keys)),
            (
                "stxstattarget",
                object
                    .stattarget
                    .map(SqlValue::Int)
                    .unwrap_or(SqlValue::Null),
            ),
            (
                "stxkind",
                array_value(
                    object
                        .kinds
                        .iter()
                        .map(|kind| SqlValue::String(kind.to_string()))
                        .collect(),
                ),
            ),
            (
                "stxexprs",
                if object.expressions.is_empty() {
                    SqlValue::Null
                } else {
                    SqlValue::String(object.expressions.join(", "))
                },
            ),
        ]));
    }
    Ok(rows)
}

pub(crate) const PG_STATISTIC_EXT_COLUMNS: [(&str, &str); 9] = [
    ("oid", "oid"),
    ("stxrelid", "oid"),
    ("stxname", "name"),
    ("stxnamespace", "oid"),
    ("stxowner", "oid"),
    ("stxkeys", "int2vector"),
    ("stxstattarget", "int2"),
    ("stxkind", "char[]"),
    ("stxexprs", "pg_node_tree"),
];
