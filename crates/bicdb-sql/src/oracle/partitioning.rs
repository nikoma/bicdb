//! Oracle logical table partitioning. A partitioned table keeps its rows in
//! one collection; its partitioning method, keys, automatic list or interval
//! scheme, subpartitioning and named partitions with their HIGH_VALUE bounds
//! are recorded in the table schema. `ALTER TABLE ... MODIFY PARTITION BY`
//! maintains that record and the USER_/ALL_ partition dictionary views
//! describe it. Automatic list partitions are derived from the key values the
//! table holds, so they appear exactly as rows that require them exist.
use super::*;

/// Internal table option carrying the metadata through `ALTER TABLE ... SET`.
pub(crate) const OPTION: &str = "bicdb_oracle_partitioning";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Partitioning {
    /// `LIST` or `RANGE`.
    pub(crate) method: String,
    pub(crate) columns: Vec<String>,
    #[serde(default)]
    pub(crate) automatic: bool,
    #[serde(default)]
    pub(crate) interval: Option<String>,
    #[serde(default)]
    pub(crate) subpartition: Option<Subpartitioning>,
    #[serde(default)]
    pub(crate) partitions: Vec<Partition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Subpartitioning {
    pub(crate) method: String,
    pub(crate) columns: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Partition {
    pub(crate) name: String,
    pub(crate) high_value: String,
    #[serde(default)]
    pub(crate) subpartitions: Vec<Partition>,
}

fn ora(code: &str, message: impl Into<String>) -> SqlError {
    SqlError::InvalidSql(format!("{code}: {}", message.into()))
}

/// Normalized words and the source text of a statement's tokens.
fn words(sql: &str) -> Result<Vec<(String, String)>> {
    let dialect = sqlparser::dialect::GenericDialect {};
    let tokens = sqlparser::tokenizer::Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    Ok(tokens
        .into_iter()
        .map(|token| {
            let text = token.to_string();
            let word = match &token {
                Token::Whitespace(_) => String::new(),
                Token::Word(word) if word.quote_style.is_none() => word.value.to_uppercase(),
                Token::Word(word) => word.value.clone(),
                _ => text.clone(),
            };
            (word, text)
        })
        .collect())
}

struct Cursor {
    tokens: Vec<(String, String)>,
    at: usize,
}

impl Cursor {
    fn skip_whitespace(&mut self) {
        while self
            .tokens
            .get(self.at)
            .is_some_and(|(word, _)| word.is_empty())
        {
            self.at += 1;
        }
    }
    fn peek(&mut self) -> Option<&str> {
        self.skip_whitespace();
        self.tokens.get(self.at).map(|(word, _)| word.as_str())
    }
    fn next(&mut self) -> Option<(String, String)> {
        self.skip_whitespace();
        let token = self.tokens.get(self.at).cloned();
        self.at += 1;
        token
    }
    fn eat(&mut self, word: &str) -> bool {
        if self.peek() == Some(word) {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, word: &str) -> Result<()> {
        if self.eat(word) {
            Ok(())
        } else {
            Err(ora(
                "ORA-14020",
                format!(
                    "expected {word} in partition clause, found {:?}",
                    self.peek()
                ),
            ))
        }
    }
    /// Source text up to the parenthesis closing the one just consumed.
    fn balanced(&mut self) -> Result<String> {
        let mut depth = 1usize;
        let mut source = String::new();
        while let Some((word, text)) = self.tokens.get(self.at).cloned() {
            self.at += 1;
            match word.as_str() {
                "(" => depth += 1,
                ")" => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(source.trim().to_string());
                    }
                }
                _ => {}
            }
            source.push_str(&text);
        }
        Err(ora("ORA-00907", "missing right parenthesis"))
    }
    fn method(&mut self) -> Result<String> {
        match self.next().map(|(word, _)| word).as_deref() {
            Some(method @ ("LIST" | "RANGE")) => Ok(method.to_string()),
            other => Err(ora(
                "ORA-14151",
                format!("unsupported partitioning method {other:?}"),
            )),
        }
    }
    fn columns(&mut self) -> Result<Vec<String>> {
        self.expect("(")?;
        let mut columns = Vec::new();
        loop {
            let (word, _) = self
                .next()
                .ok_or_else(|| ora("ORA-00904", "missing partitioning column"))?;
            columns.push(word);
            if self.eat(",") {
                continue;
            }
            self.expect(")")?;
            return Ok(columns);
        }
    }
}

/// `ALTER TABLE name MODIFY PARTITION BY ...` rewritten as the internal
/// option assignment, or `None` for any other statement.
pub(super) fn lower_ddl(sql: &str) -> Result<Option<String>> {
    let head = sql.trim_start();
    if !head
        .get(..11)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("ALTER TABLE"))
        || !head.to_ascii_uppercase().contains("PARTITION")
    {
        return Ok(None);
    }
    let tokens = words(sql)?;
    let mut cursor = Cursor { tokens, at: 0 };
    if !(cursor.eat("ALTER") && cursor.eat("TABLE")) {
        return Ok(None);
    }
    let mut name = Vec::new();
    loop {
        let Some((_, text)) = cursor.next() else {
            return Ok(None);
        };
        name.push(text);
        if !cursor.eat(".") {
            break;
        }
    }
    let start = cursor.at;
    if !(cursor.eat("MODIFY") && cursor.eat("PARTITION") && cursor.eat("BY")) {
        cursor.at = start;
        let operation = cursor.peek().map(str::to_string);
        let partition_clause = cursor.tokens[cursor.at..]
            .iter()
            .any(|(word, _)| matches!(word.as_str(), "PARTITION" | "SUBPARTITION" | "INTERVAL"));
        return match operation.as_deref() {
            Some("ADD" | "SPLIT" | "EXCHANGE" | "DROP" | "TRUNCATE" | "MERGE" | "SET")
                if partition_clause =>
            {
                Err(ora(
                    "ORA-14006",
                    "this partition maintenance operation is not supported",
                ))
            }
            _ => Ok(None),
        };
    }
    let method = cursor.method()?;
    let columns = cursor.columns()?;
    let mut partitioning = Partitioning {
        method,
        columns,
        automatic: false,
        interval: None,
        subpartition: None,
        partitions: Vec::new(),
    };
    loop {
        if cursor.eat("AUTOMATIC") {
            partitioning.automatic = true;
        } else if cursor.eat("INTERVAL") {
            cursor.expect("(")?;
            partitioning.interval = Some(cursor.balanced()?);
        } else if cursor.eat("SUBPARTITION") {
            cursor.expect("BY")?;
            let method = cursor.method()?;
            let columns = cursor.columns()?;
            partitioning.subpartition = Some(Subpartitioning { method, columns });
        } else {
            break;
        }
    }
    if partitioning.automatic && partitioning.method != "LIST" {
        return Err(ora("ORA-14751", "AUTOMATIC requires LIST partitioning"));
    }
    if partitioning.interval.is_some() && partitioning.method != "RANGE" {
        return Err(ora("ORA-14752", "INTERVAL requires RANGE partitioning"));
    }
    cursor.expect("(")?;
    loop {
        cursor.expect("PARTITION")?;
        let (partition, _) = cursor
            .next()
            .ok_or_else(|| ora("ORA-14006", "invalid partition name"))?;
        cursor.expect("VALUES")?;
        if partitioning.method == "RANGE" {
            cursor.expect("LESS")?;
            cursor.expect("THAN")?;
        }
        cursor.expect("(")?;
        let high_value = cursor.balanced()?;
        partitioning.partitions.push(Partition {
            name: partition,
            high_value,
            subpartitions: Vec::new(),
        });
        if cursor.eat(",") {
            continue;
        }
        cursor.expect(")")?;
        break;
    }
    while cursor.eat("ONLINE") || (cursor.eat("UPDATE") && cursor.eat("INDEXES")) {}
    if let Some(extra) = cursor.peek() {
        if extra != ";" {
            return Err(ora(
                "ORA-14020",
                format!("unexpected {extra} after partition clause"),
            ));
        }
    }
    let json = serde_json::to_string(&partitioning)?;
    Ok(Some(format!(
        "ALTER TABLE {} SET ({OPTION} = '{}')",
        name.join("."),
        json.replace('\'', "''")
    )))
}

/// Validate and apply the internal option to a table schema.
pub(crate) fn apply_option(schema: &mut TableSchema, value: &str) -> Result<()> {
    let partitioning: Partitioning = serde_json::from_str(value)
        .map_err(|error| SqlError::InvalidSql(format!("invalid partitioning metadata: {error}")))?;
    let resolve = |name: &str| {
        schema
            .columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(name))
            .map(|column| column.name.clone())
            .ok_or_else(|| ora("ORA-00904", format!("\"{name}\": invalid identifier")))
    };
    let mut partitioning = partitioning;
    partitioning.columns = partitioning
        .columns
        .iter()
        .map(|name| resolve(name))
        .collect::<Result<_>>()?;
    if let Some(sub) = partitioning.subpartition.as_mut() {
        sub.columns = sub
            .columns
            .iter()
            .map(|name| resolve(name))
            .collect::<Result<_>>()?;
    }
    let mut names = BTreeSet::new();
    for partition in &partitioning.partitions {
        if !names.insert(partition.name.to_uppercase()) {
            return Err(ora("ORA-14014", "duplicate partition name"));
        }
    }
    schema.oracle_partitioning = Some(partitioning);
    Ok(())
}

/// Stable system name for a derived partition, in Oracle's `SYS_P...` form.
fn system_name(prefix: &str, seed: &str) -> String {
    let mut hash: u32 = 2_166_136_261;
    for byte in seed.bytes() {
        hash = (hash ^ u32::from(byte)).wrapping_mul(16_777_619);
    }
    format!("{prefix}{}", hash % 1_000_000)
}

fn literal_text(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "NULL".into(),
        SqlValue::Int(value) => value.to_string(),
        SqlValue::Float(value) => value.to_string(),
        SqlValue::String(text) if is_valid_numeric_text(text) => text.clone(),
        other => format!("'{}'", other.to_cell().replace('\'', "''")),
    }
}

/// Partitions as the dictionary reports them: declared ones, then one per
/// key value an AUTOMATIC list table holds, each with its subpartitions.
pub(crate) fn effective_partitions(
    db: &BicDb,
    collection: &str,
    schema: &TableSchema,
    partitioning: &Partitioning,
) -> Result<Vec<Partition>> {
    let mut partitions = partitioning.partitions.clone();
    if partitioning.automatic {
        let mut seen = partitions
            .iter()
            .map(|partition| partition.high_value.clone())
            .collect::<BTreeSet<_>>();
        let mut derived = BTreeSet::new();
        for record in db.scan_collection_unchecked(collection)? {
            let values = record_column_values(&record, schema, &partitioning.columns);
            derived.insert(
                values
                    .iter()
                    .map(literal_text)
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        for high_value in derived {
            if seen.insert(high_value.clone()) {
                partitions.push(Partition {
                    name: system_name("SYS_P", &format!("{collection}\u{0}{high_value}")),
                    high_value,
                    subpartitions: Vec::new(),
                });
            }
        }
    }
    if let Some(sub) = &partitioning.subpartition {
        for partition in &mut partitions {
            if partition.subpartitions.is_empty() {
                partition.subpartitions.push(Partition {
                    name: system_name("SYS_SUBP", &format!("{collection}\u{0}{}", partition.name)),
                    high_value: if sub.method == "RANGE" {
                        "MAXVALUE".into()
                    } else {
                        "DEFAULT".into()
                    },
                    subpartitions: Vec::new(),
                });
            }
        }
    }
    Ok(partitions)
}

/// Dictionary views: (public name, internal relation, columns after OWNER).
pub(crate) const VIEWS: &[(&str, &str, &[(&str, &str)])] = &[
    (
        "PART_TABLES",
        "pg_catalog.bicdb_oracle_part_tables",
        &[
            ("TABLE_NAME", "varchar"),
            ("PARTITIONING_TYPE", "varchar"),
            ("SUBPARTITIONING_TYPE", "varchar"),
            ("PARTITION_COUNT", "numeric"),
            ("PARTITIONING_KEY_COUNT", "numeric"),
            ("SUBPARTITIONING_KEY_COUNT", "numeric"),
            ("AUTOLIST", "varchar"),
            ("INTERVAL", "varchar"),
        ],
    ),
    (
        "PART_KEY_COLUMNS",
        "pg_catalog.bicdb_oracle_part_key_columns",
        &[
            ("NAME", "varchar"),
            ("OBJECT_TYPE", "varchar"),
            ("COLUMN_NAME", "varchar"),
            ("COLUMN_POSITION", "numeric"),
        ],
    ),
    (
        "SUBPART_KEY_COLUMNS",
        "pg_catalog.bicdb_oracle_subpart_key_columns",
        &[
            ("NAME", "varchar"),
            ("OBJECT_TYPE", "varchar"),
            ("COLUMN_NAME", "varchar"),
            ("COLUMN_POSITION", "numeric"),
        ],
    ),
    (
        "TAB_PARTITIONS",
        "pg_catalog.bicdb_oracle_tab_partitions",
        &[
            ("TABLE_NAME", "varchar"),
            ("COMPOSITE", "varchar"),
            ("PARTITION_NAME", "varchar"),
            ("SUBPARTITION_COUNT", "numeric"),
            ("HIGH_VALUE", "text"),
            ("HIGH_VALUE_LENGTH", "numeric"),
            ("PARTITION_POSITION", "numeric"),
        ],
    ),
    (
        "TAB_SUBPARTITIONS",
        "pg_catalog.bicdb_oracle_tab_subpartitions",
        &[
            ("TABLE_NAME", "varchar"),
            ("PARTITION_NAME", "varchar"),
            ("SUBPARTITION_NAME", "varchar"),
            ("HIGH_VALUE", "text"),
            ("HIGH_VALUE_LENGTH", "numeric"),
            ("PARTITION_POSITION", "numeric"),
            ("SUBPARTITION_POSITION", "numeric"),
        ],
    ),
    (
        "INDEXES",
        "pg_catalog.bicdb_oracle_indexes",
        &[
            ("INDEX_NAME", "varchar"),
            ("INDEX_TYPE", "varchar"),
            ("TABLE_OWNER", "varchar"),
            ("TABLE_NAME", "varchar"),
            ("TABLE_TYPE", "varchar"),
            ("UNIQUENESS", "varchar"),
            ("STATUS", "varchar"),
            ("PARTITIONED", "varchar"),
        ],
    ),
    (
        "IND_PARTITIONS",
        "pg_catalog.bicdb_oracle_ind_partitions",
        &[
            ("INDEX_NAME", "varchar"),
            ("PARTITION_NAME", "varchar"),
            ("HIGH_VALUE", "text"),
            ("PARTITION_POSITION", "numeric"),
            ("STATUS", "varchar"),
        ],
    ),
    (
        "IND_SUBPARTITIONS",
        "pg_catalog.bicdb_oracle_ind_subpartitions",
        &[
            ("INDEX_NAME", "varchar"),
            ("PARTITION_NAME", "varchar"),
            ("SUBPARTITION_NAME", "varchar"),
            ("HIGH_VALUE", "text"),
            ("SUBPARTITION_POSITION", "numeric"),
            ("STATUS", "varchar"),
        ],
    ),
];

fn view(
    relation: &str,
) -> Option<&'static (
    &'static str,
    &'static str,
    &'static [(&'static str, &'static str)],
)> {
    VIEWS.iter().find(|(_, internal, _)| *internal == relation)
}

pub(crate) fn is_relation(name: &str) -> bool {
    view(name).is_some()
}

/// Columns of an internal relation: OWNER first, then the view's columns.
pub(crate) fn columns(name: &str) -> Option<Vec<(String, Option<String>)>> {
    let (_, _, columns) = view(name)?;
    Some(
        std::iter::once(("OWNER".to_string(), Some("varchar".to_string())))
            .chain(
                columns
                    .iter()
                    .map(|(column, kind)| (column.to_string(), Some(kind.to_string()))),
            )
            .collect(),
    )
}

fn text(value: impl Into<String>) -> SqlValue {
    SqlValue::String(value.into())
}

fn number(value: usize) -> SqlValue {
    // NUMBER columns travel as exact decimal text, like other Oracle catalogs.
    SqlValue::String(value.to_string())
}

pub(crate) fn rows(db: &BicDb, name: &str) -> Result<Vec<BTreeMap<String, SqlValue>>> {
    let Some((view_name, _, _)) = view(name) else {
        return Ok(Vec::new());
    };
    let mut rows = Vec::new();
    if *view_name == "INDEXES" {
        let partitioned = |collection: &str| {
            load_schema_shared(db, collection)
                .ok()
                .flatten()
                .is_some_and(|schema| schema.oracle_partitioning.is_some())
        };
        for index in index_definitions_shared(db).iter() {
            if is_internal_sql_collection(&index.collection) {
                continue;
            }
            let owner = relation_catalog_namespace(db, &index.collection)?.to_uppercase();
            rows.push(virtual_row([
                ("OWNER", text(owner.clone())),
                (
                    "INDEX_NAME",
                    text(catalog_display_name(&index.name).to_uppercase()),
                ),
                ("INDEX_TYPE", text("NORMAL")),
                ("TABLE_OWNER", text(owner)),
                (
                    "TABLE_NAME",
                    text(catalog_display_name(&index.collection).to_uppercase()),
                ),
                ("TABLE_TYPE", text("TABLE")),
                (
                    "UNIQUENESS",
                    text(if index.unique { "UNIQUE" } else { "NONUNIQUE" }),
                ),
                ("STATUS", text("VALID")),
                (
                    "PARTITIONED",
                    text(if partitioned(&index.collection) {
                        "YES"
                    } else {
                        "NO"
                    }),
                ),
            ]));
        }
        return Ok(rows);
    }
    // Indexes are global (not partitioned locally), so they have no index
    // partitions to report.
    if matches!(*view_name, "IND_PARTITIONS" | "IND_SUBPARTITIONS") {
        return Ok(rows);
    }
    for collection in user_collection_names(db) {
        let Some(schema) = load_schema_shared(db, &collection)? else {
            continue;
        };
        let Some(partitioning) = schema.oracle_partitioning.as_ref() else {
            continue;
        };
        let owner = relation_catalog_namespace(db, &collection)?.to_uppercase();
        let table = catalog_display_name(&collection).to_uppercase();
        let key_rows = |columns: &[String], rows: &mut Vec<BTreeMap<String, SqlValue>>| {
            for (position, column) in columns.iter().enumerate() {
                rows.push(virtual_row([
                    ("OWNER", text(owner.clone())),
                    ("NAME", text(table.clone())),
                    ("OBJECT_TYPE", text("TABLE")),
                    ("COLUMN_NAME", text(column.to_uppercase())),
                    ("COLUMN_POSITION", number(position + 1)),
                ]));
            }
        };
        match *view_name {
            "PART_KEY_COLUMNS" => key_rows(&partitioning.columns, &mut rows),
            "SUBPART_KEY_COLUMNS" => {
                if let Some(sub) = &partitioning.subpartition {
                    key_rows(&sub.columns, &mut rows);
                }
            }
            "PART_TABLES" => {
                let partitions = effective_partitions(db, &collection, &schema, partitioning)?;
                rows.push(virtual_row([
                    ("OWNER", text(owner.clone())),
                    ("TABLE_NAME", text(table.clone())),
                    ("PARTITIONING_TYPE", text(partitioning.method.clone())),
                    (
                        "SUBPARTITIONING_TYPE",
                        text(
                            partitioning
                                .subpartition
                                .as_ref()
                                .map(|sub| sub.method.clone())
                                .unwrap_or_else(|| "NONE".into()),
                        ),
                    ),
                    ("PARTITION_COUNT", number(partitions.len())),
                    ("PARTITIONING_KEY_COUNT", number(partitioning.columns.len())),
                    (
                        "SUBPARTITIONING_KEY_COUNT",
                        number(
                            partitioning
                                .subpartition
                                .as_ref()
                                .map_or(0, |sub| sub.columns.len()),
                        ),
                    ),
                    (
                        "AUTOLIST",
                        text(if partitioning.automatic { "YES" } else { "NO" }),
                    ),
                    (
                        "INTERVAL",
                        partitioning
                            .interval
                            .clone()
                            .map(text)
                            .unwrap_or(SqlValue::Null),
                    ),
                ]));
            }
            "TAB_PARTITIONS" => {
                let partitions = effective_partitions(db, &collection, &schema, partitioning)?;
                for (position, partition) in partitions.iter().enumerate() {
                    rows.push(virtual_row([
                        ("OWNER", text(owner.clone())),
                        ("TABLE_NAME", text(table.clone())),
                        (
                            "COMPOSITE",
                            text(if partitioning.subpartition.is_some() {
                                "YES"
                            } else {
                                "NO"
                            }),
                        ),
                        ("PARTITION_NAME", text(partition.name.to_uppercase())),
                        ("SUBPARTITION_COUNT", number(partition.subpartitions.len())),
                        ("HIGH_VALUE", text(partition.high_value.clone())),
                        ("HIGH_VALUE_LENGTH", number(partition.high_value.len())),
                        ("PARTITION_POSITION", number(position + 1)),
                    ]));
                }
            }
            "TAB_SUBPARTITIONS" => {
                let partitions = effective_partitions(db, &collection, &schema, partitioning)?;
                for (position, partition) in partitions.iter().enumerate() {
                    for (sub_position, sub) in partition.subpartitions.iter().enumerate() {
                        rows.push(virtual_row([
                            ("OWNER", text(owner.clone())),
                            ("TABLE_NAME", text(table.clone())),
                            ("PARTITION_NAME", text(partition.name.to_uppercase())),
                            ("SUBPARTITION_NAME", text(sub.name.to_uppercase())),
                            ("HIGH_VALUE", text(sub.high_value.clone())),
                            ("HIGH_VALUE_LENGTH", number(sub.high_value.len())),
                            ("PARTITION_POSITION", number(position + 1)),
                            ("SUBPARTITION_POSITION", number(sub_position + 1)),
                        ]));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(rows)
}

/// Resolve unqualified `USER_<view>` / `ALL_<view>` names (and `SYS.` ones)
/// after local relations and CTEs: USER_ views are the current schema's rows
/// of the internal relation, without its OWNER column.
pub(super) fn lower_views<T: sqlparser::ast::VisitMut>(
    db: &BicDb,
    node: &mut T,
    schema: &str,
) -> Result<()> {
    struct Resolve<'a> {
        db: &'a BicDb,
        schema: &'a str,
        ctes: Vec<BTreeSet<String>>,
    }
    impl sqlparser::ast::VisitorMut for Resolve<'_> {
        type Break = SqlError;
        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
            self.ctes.push(
                query
                    .with
                    .as_ref()
                    .map(|with| {
                        with.cte_tables
                            .iter()
                            .map(|cte| ident_value(&cte.alias.name))
                            .collect()
                    })
                    .unwrap_or_default(),
            );
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<Self::Break> {
            self.ctes.pop();
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<Self::Break> {
            let TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } = factor
            else {
                return ControlFlow::Continue(());
            };
            let parts = object_name_parts(name);
            let public = match parts.as_slice() {
                [table] if !self.ctes.iter().rev().any(|ctes| ctes.contains(table)) => table,
                [owner, table] if owner == "SYS" => table,
                _ => return ControlFlow::Continue(()),
            };
            let (all, suffix) = if let Some(suffix) = public.strip_prefix("USER_") {
                (false, suffix)
            } else if let Some(suffix) = public.strip_prefix("ALL_") {
                (true, suffix)
            } else {
                return ControlFlow::Continue(());
            };
            let Some((_, internal, columns)) = VIEWS.iter().find(|(view, _, _)| *view == suffix)
            else {
                return ControlFlow::Continue(());
            };
            if parts.len() == 1 {
                let local =
                    match relation_name_from_parts(&[self.schema.to_string(), public.clone()]) {
                        Ok(local) => local,
                        Err(error) => return ControlFlow::Break(error),
                    };
                if resolve_relation_name_memoized(self.db, &local).is_some() {
                    return ControlFlow::Continue(());
                }
            }
            let mut projection = columns
                .iter()
                .map(|(column, _)| format!("\"{column}\""))
                .collect::<Vec<_>>();
            if all {
                projection.insert(0, "\"OWNER\"".into());
            }
            let (namespace, relation) = internal.split_once('.').expect("qualified catalog name");
            let filter = if all {
                String::new()
            } else {
                format!(
                    " WHERE \"OWNER\" = '{}'",
                    self.schema.to_uppercase().replace('\'', "''")
                )
            };
            let sql = format!(
                "SELECT {} FROM \"{namespace}\".\"{relation}\"{filter}",
                projection.join(", ")
            );
            let subquery = match parse_statements(&sql) {
                Ok(mut statements) => match statements.pop() {
                    Some(Statement::Query(query)) => query,
                    _ => return ControlFlow::Break(SqlError::InvalidSql(sql)),
                },
                Err(error) => return ControlFlow::Break(error),
            };
            let alias = alias.take().unwrap_or(TableAlias {
                explicit: true,
                name: Ident::with_quote('"', public.clone()),
                columns: Vec::new(),
                at: None,
            });
            *factor = TableFactor::Derived {
                lateral: false,
                subquery,
                alias: Some(alias),
                sample: None,
            };
            ControlFlow::Continue(())
        }
    }
    match sqlparser::ast::VisitMut::visit(
        node,
        &mut Resolve {
            db,
            schema,
            ctes: Vec::new(),
        },
    ) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}
