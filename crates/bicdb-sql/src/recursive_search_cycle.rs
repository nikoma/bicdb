//! Lower PostgreSQL SEARCH and CYCLE columns into the recursive UNION.
use crate::*;
use sqlparser::ast::{Cte, TableAliasColumnDef, VisitMut, VisitorMut};
use sqlparser::keywords::Keyword;

struct Search {
    breadth: bool,
    columns: Vec<Ident>,
    output: Ident,
}

struct Cycle {
    columns: Vec<Ident>,
    mark: Ident,
    path: Ident,
    marked: String,
    clear: String,
}

struct Clauses {
    name: Ident,
    search: Option<Search>,
    cycle: Option<Cycle>,
}

struct SourceToken {
    token: Token,
    start: usize,
    end: usize,
}

struct Cursor<'a> {
    sql: &'a str,
    tokens: &'a [SourceToken],
    at: usize,
}

impl Cursor<'_> {
    fn keyword(&self, index: usize, expected: Keyword) -> bool {
        matches!(self.tokens.get(index).map(|token| &token.token), Some(Token::Word(word))
            if word.quote_style.is_none() && word.keyword == expected)
    }

    fn consume(&mut self, expected: Keyword) -> bool {
        if !self.keyword(self.at, expected) {
            return false;
        }
        self.at += 1;
        true
    }

    fn consume_spelling(&mut self, spelling: &str) -> bool {
        if !matches!(self.tokens.get(self.at).map(|token| &token.token), Some(Token::Word(word))
            if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(spelling))
        {
            return false;
        }
        self.at += 1;
        true
    }

    fn identifier_at(&self, index: usize) -> Option<Ident> {
        let Token::Word(word) = &self.tokens.get(index)?.token else {
            return None;
        };
        Some(match word.quote_style {
            Some(quote) => Ident::with_quote(quote, &word.value),
            None => Ident::new(&word.value),
        })
    }

    fn identifier(&mut self) -> Option<Ident> {
        let value = self.identifier_at(self.at)?;
        self.at += 1;
        Some(value)
    }

    fn identifiers(&mut self) -> Option<Vec<Ident>> {
        let mut names = vec![self.identifier()?];
        while matches!(
            self.tokens.get(self.at).map(|token| &token.token),
            Some(Token::Comma)
        ) {
            self.at += 1;
            names.push(self.identifier()?);
        }
        Some(names)
    }

    fn search(&mut self) -> Option<Search> {
        let breadth = self.consume_spelling("breadth");
        if !breadth && !self.consume_spelling("depth") {
            return None;
        }
        if !self.consume(Keyword::FIRST) || !self.consume(Keyword::BY) {
            return None;
        }
        let columns = self.identifiers()?;
        if !self.consume(Keyword::SET) {
            return None;
        }
        Some(Search {
            breadth,
            columns,
            output: self.identifier()?,
        })
    }

    fn constant_until(&mut self, delimiter: Keyword) -> Option<String> {
        let start = self.tokens.get(self.at)?.start;
        let mut levels = 0usize;
        let mut end = self.at;
        while end < self.tokens.len() {
            if levels == 0 && self.keyword(end, delimiter) {
                let expression = self.sql[start..self.tokens[end].start].trim();
                self.at = end + 1;
                return (!expression.is_empty()).then(|| expression.to_string());
            }
            match self.tokens[end].token {
                Token::LParen => levels += 1,
                Token::RParen if levels > 0 => levels -= 1,
                Token::RParen | Token::SemiColon => return None,
                _ => {}
            }
            end += 1;
        }
        None
    }

    fn cycle(&mut self) -> Option<Cycle> {
        let columns = self.identifiers()?;
        if !self.consume(Keyword::SET) {
            return None;
        }
        let mark = self.identifier()?;
        let (marked, clear) = if self.consume(Keyword::TO) {
            (
                self.constant_until(Keyword::DEFAULT)?,
                self.constant_until(Keyword::USING)?,
            )
        } else {
            if !self.consume(Keyword::USING) {
                return None;
            }
            ("TRUE".to_string(), "FALSE".to_string())
        };
        Some(Cycle {
            columns,
            mark,
            path: self.identifier()?,
            marked,
            clear,
        })
    }
}

pub(crate) struct LoweredClauses {
    pub(crate) sql: String,
    clauses: BTreeMap<String, Clauses>,
}

pub(crate) fn lower(sql: &str) -> Option<LoweredClauses> {
    if !sql
        .as_bytes()
        .windows(5)
        .any(|text| text.eq_ignore_ascii_case(b"cycle"))
        && !sql
            .as_bytes()
            .windows(6)
            .any(|text| text.eq_ignore_ascii_case(b"search"))
    {
        return None;
    }
    let mut offset = 0;
    let tokens = tokens_with_source(&PostgreSqlDialect {}, sql)?
        .into_iter()
        .filter_map(|(token, source)| {
            let start = offset;
            offset += source.len();
            (!matches!(token, Token::Whitespace(_))).then_some(SourceToken {
                token,
                start,
                end: offset,
            })
        })
        .collect::<Vec<_>>();
    let mut opening = vec![None; tokens.len()];
    let mut stack = Vec::new();
    for (index, token) in tokens.iter().enumerate() {
        match token.token {
            Token::LParen => stack.push(index),
            Token::RParen => opening[index] = stack.pop(),
            _ => {}
        }
    }
    let mut cursor = Cursor {
        sql,
        tokens: &tokens,
        at: 0,
    };
    let mut clauses = BTreeMap::new();
    let mut edits = Vec::new();
    while cursor.at < tokens.len() {
        let start = cursor.at;
        cursor.at += 1;
        if !cursor.keyword(start, Keyword::SEARCH) && !cursor.keyword(start, Keyword::CYCLE) {
            continue;
        }
        let Some(mut position) = start
            .checked_sub(1)
            .and_then(|close| opening[close])
            .and_then(|open| open.checked_sub(1))
        else {
            continue;
        };
        if cursor.keyword(position, Keyword::MATERIALIZED) {
            position = position.checked_sub(1)?;
            if cursor.keyword(position, Keyword::NOT) {
                position = position.checked_sub(1)?;
            }
        }
        if !cursor.keyword(position, Keyword::AS) {
            continue;
        }
        position = position.checked_sub(1)?;
        if let Some(open) = opening[position] {
            position = open.checked_sub(1)?;
        }
        let name = cursor.identifier_at(position)?;
        cursor.at = start;
        let search = if cursor.consume(Keyword::SEARCH) {
            Some(cursor.search()?)
        } else {
            None
        };
        let cycle = if cursor.consume(Keyword::CYCLE) {
            Some(cursor.cycle()?)
        } else {
            None
        };
        let mut marker = format!("bicdb_recursive_cte_{}", clauses.len());
        while sql.contains(&marker) {
            marker.push('_');
        }
        edits.push((
            tokens[position].start,
            tokens[position].end,
            format!("\"{marker}\""),
        ));
        edits.push((
            tokens[start].start,
            tokens[cursor.at - 1].end,
            String::new(),
        ));
        clauses.insert(
            marker,
            Clauses {
                name,
                search,
                cycle,
            },
        );
    }
    if edits.is_empty() {
        return None;
    }
    edits.sort_by_key(|edit| edit.0);
    let mut lowered = sql.to_string();
    for (start, end, replacement) in edits.into_iter().rev() {
        lowered.replace_range(start..end, &replacement);
    }
    Some(LoweredClauses {
        sql: lowered,
        clauses,
    })
}

impl LoweredClauses {
    pub(crate) fn restore(&self, statements: &mut Vec<Statement>) -> Result<()> {
        struct Restore<'a> {
            clauses: &'a BTreeMap<String, Clauses>,
        }
        impl VisitorMut for Restore<'_> {
            type Break = SqlError;

            fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<SqlError> {
                if let Some(with) = &mut query.with {
                    for cte in &mut with.cte_tables {
                        if let Some(clauses) = self.clauses.get(&cte.alias.name.value) {
                            if let Err(error) = expand(cte, clauses, with.recursive) {
                                return ControlFlow::Break(error);
                            }
                        }
                    }
                }
                ControlFlow::Continue(())
            }
        }
        match statements.visit(&mut Restore {
            clauses: &self.clauses,
        }) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(error),
        }
    }
}

fn quoted(name: &Ident) -> String {
    format!("\"{}\"", ident_value(name).replace('"', "\"\""))
}

fn projected_names(body: &SetExpr) -> Option<Vec<Ident>> {
    match body {
        SetExpr::Select(select) => select
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::UnnamedExpr(expr) => {
                    Some(Ident::with_quote('"', row_expr_column_name(expr)))
                }
                SelectItem::ExprWithAlias { alias, .. } => Some(alias.clone()),
                _ => None,
            })
            .collect(),
        SetExpr::Query(query) => projected_names(&query.body),
        SetExpr::Values(values) => Some(
            (1..=values.rows.first()?.len())
                .map(|index| Ident::new(format!("column{index}")))
                .collect(),
        ),
        SetExpr::SetOperation { left, .. } => projected_names(left),
        _ => None,
    }
}

fn recursive_alias(select: &Select, name: &Ident) -> Option<Ident> {
    select
        .from
        .iter()
        .flat_map(|item| {
            std::iter::once(&item.relation).chain(item.joins.iter().map(|join| &join.relation))
        })
        .find_map(|relation| {
            let TableFactor::Table {
                name: source,
                alias,
                args: None,
                ..
            } = relation
            else {
                return None;
            };
            let parts = object_name_parts(source);
            (parts.as_slice() == [ident_value(name)]).then(|| {
                alias
                    .as_ref()
                    .map_or_else(|| name.clone(), |alias| alias.name.clone())
            })
        })
}

fn select(sql: &str) -> Result<Box<Select>> {
    let mut statements = parse_statements(sql)?;
    if let [Statement::Query(query)] = statements.as_mut_slice() {
        if let SetExpr::Select(select) = query.body.as_ref() {
            return Ok(select.clone());
        }
    }
    Err(SqlError::InvalidSql(
        "invalid SELECT in SEARCH or CYCLE clause".into(),
    ))
}

fn record_columns(columns: &[Ident], keys: &[Ident]) -> Result<String> {
    let mut seen = std::collections::BTreeSet::new();
    keys.iter()
        .map(|key| {
            let key = ident_value(key);
            if !seen.insert(key.clone()) {
                return Err(SqlError::data_exception_public(
                    "42701",
                    format!("column \"{key}\" specified more than once"),
                    None,
                ));
            }
            columns
                .iter()
                .position(|name| ident_value(name) == key)
                .map(|index| format!("search_source.c{index}"))
                .ok_or_else(|| {
                    SqlError::data_exception_public(
                        "42703",
                        format!("column \"{key}\" does not exist"),
                        None,
                    )
                })
        })
        .collect::<Result<Vec<_>>>()
        .map(|values| values.join(", "))
}

fn expand(cte: &mut Cte, clauses: &Clauses, recursive: bool) -> Result<()> {
    let requires_recursive_union = || {
        SqlError::data_exception_public(
            "42601",
            "SEARCH or CYCLE clause requires a recursive WITH query",
            None,
        )
    };
    if !recursive {
        return Err(requires_recursive_union());
    }
    let SetExpr::SetOperation {
        left,
        op: SetOperator::Union,
        right,
        ..
    } = cte.query.body.as_mut()
    else {
        return Err(requires_recursive_union());
    };
    let columns = if cte.alias.columns.is_empty() {
        projected_names(left).ok_or_else(|| {
            SqlError::Unsupported(
                "SEARCH or CYCLE requires named recursive CTE output columns".into(),
            )
        })?
    } else {
        cte.alias
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    };
    let SetExpr::Select(recursive_select) = right.as_mut() else {
        return Err(SqlError::Unsupported(
            "SEARCH or CYCLE requires a SELECT recursive term".into(),
        ));
    };
    let parent =
        recursive_alias(recursive_select, &clauses.name).ok_or_else(requires_recursive_union)?;
    if recursive_select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
        )
    }) {
        return Err(SqlError::Unsupported(
            "SEARCH or CYCLE requires explicit recursive projections".into(),
        ));
    }

    let mut output_columns = columns.clone();
    let mut append_output = |column: &Ident| -> Result<()> {
        let name = ident_value(column);
        if output_columns
            .iter()
            .any(|existing| ident_value(existing) == name)
        {
            return Err(SqlError::data_exception_public(
                "42701",
                format!("column name \"{name}\" already used in WITH query"),
                None,
            ));
        }
        output_columns.push(column.clone());
        Ok(())
    };
    let width = columns.len();
    let fields = (0..width)
        .map(|index| format!("search_source.c{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut anchor_extra = Vec::new();
    let mut recursive_extra = Vec::new();
    let mut parent_columns = Vec::new();
    if let Some(search) = &clauses.search {
        append_output(&search.output)?;
        let keys = record_columns(&columns, &search.columns)?;
        let parent_order = format!("search_source.c{}", width + parent_columns.len());
        parent_columns.push(format!("{}.{}", quoted(&parent), quoted(&search.output)));
        if search.breadth {
            anchor_extra.push(format!("ROW(0::bigint, {keys})"));
            recursive_extra.push(format!("ROW(({parent_order}).f1 + 1, {keys})"));
        } else {
            anchor_extra.push(format!("ARRAY[ROW({keys})]"));
            recursive_extra.push(format!("{parent_order} || ARRAY[ROW({keys})]"));
        }
    }
    if let Some(cycle) = &clauses.cycle {
        append_output(&cycle.mark)?;
        append_output(&cycle.path)?;
        let keys = record_columns(&columns, &cycle.columns)?;
        let parent_path = format!("search_source.c{}", width + parent_columns.len());
        let marked = &cycle.marked;
        let clear = &cycle.clear;
        for mark in [marked, clear] {
            let parsed = select(&format!("SELECT {mark}"))?;
            if !matches!(parsed.projection.as_slice(), [SelectItem::UnnamedExpr(expr)] if is_constant_mark(expr))
            {
                return Err(SqlError::data_exception_public(
                    "42601",
                    "CYCLE mark values must be constants",
                    None,
                ));
            }
        }
        parent_columns.push(format!("{}.{}", quoted(&parent), quoted(&cycle.path)));
        anchor_extra.extend([format!("({clear})"), format!("ARRAY[ROW({keys})]")]);
        recursive_extra.extend([
            format!(
                "CASE WHEN ROW({keys}) = ANY({parent_path}) THEN ({marked}) ELSE ({clear}) END"
            ),
            format!("{parent_path} || ARRAY[ROW({keys})]"),
        ]);
        let mut filter = select(&format!(
            "SELECT 1 WHERE {}.{} <> ({marked})",
            quoted(&parent),
            quoted(&cycle.mark)
        ))?;
        let guard = filter
            .selection
            .take()
            .expect("the CYCLE filter was supplied");
        recursive_select.selection = Some(match recursive_select.selection.take() {
            Some(existing) => Expr::BinaryOp {
                left: Box::new(existing),
                op: BinaryOperator::And,
                right: Box::new(guard),
            },
            None => guard,
        });
    }
    recursive_select
        .projection
        .extend(select(&format!("SELECT {}", parent_columns.join(", ")))?.projection);
    let wrap = |body: &SetExpr, extra: Vec<String>, input_width: usize| -> Result<Box<SetExpr>> {
        let slots = (0..input_width)
            .map(|index| format!("c{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let select = select(&format!(
            "SELECT {fields}, {} FROM ({}) AS search_source({slots})",
            extra.join(", "),
            crate::sql_text::sql_text(body)
        ))?;
        Ok(Box::new(SetExpr::Select(select)))
    };
    *left = wrap(left, anchor_extra, width)?;
    *right = wrap(right, recursive_extra, width + parent_columns.len())?;
    cte.alias.name = clauses.name.clone();
    cte.alias.columns = output_columns
        .into_iter()
        .map(|name| TableAliasColumnDef {
            name,
            data_type: None,
        })
        .collect();
    Ok(())
}

fn is_constant_mark(expr: &Expr) -> bool {
    match expr {
        Expr::Value(_) | Expr::TypedString(_) => true,
        Expr::Cast { expr, .. } | Expr::Nested(expr) | Expr::UnaryOp { expr, .. } => {
            is_constant_mark(expr)
        }
        _ => false,
    }
}
