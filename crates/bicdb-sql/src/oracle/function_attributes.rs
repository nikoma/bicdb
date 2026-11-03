//! Function declaration attributes, scoped to the original header tokens.
use super::*;

pub(super) fn deterministic_positions(source: &[Token]) -> BTreeSet<usize> {
    if !source.first().is_some_and(|token| word(token, "CREATE")) {
        return BTreeSet::new();
    }
    let Some(function) = source
        .iter()
        .take(4)
        .position(|token| word(token, "FUNCTION"))
    else {
        return BTreeSet::new();
    };
    let mut depth = 0usize;
    let mut returns = None;
    let mut positions = BTreeSet::new();
    for (index, token) in source.iter().enumerate().skip(function + 1) {
        match token {
            Token::LParen => depth += 1,
            Token::RParen => depth = depth.saturating_sub(1),
            _ if depth == 0 => {
                if word(token, "AS") || word(token, "IS") {
                    break;
                }
                if word(token, "RETURN") {
                    returns = Some(index);
                } else if word(token, "DETERMINISTIC")
                    && returns.is_some_and(|position| index > position + 1)
                    && source[index - 1] != Token::Period
                    && source.get(index + 1) != Some(&Token::Period)
                {
                    positions.insert(index);
                }
            }
            _ => {}
        }
    }
    positions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_is_an_attribute_only_after_the_return_type_in_a_function_header() {
        for (sql, expected) in [
            ("CREATE FUNCTION f(p NUMBER) RETURN NUMBER DETERMINISTIC AS BEGIN RETURN p;END;", 1),
            ("CREATE OR REPLACE FUNCTION f RETURN s.n%TYPE DETERMINISTIC IS BEGIN RETURN 1;END;", 1),
            ("CREATE FUNCTION f(\"DETERMINISTIC\" NUMBER) RETURN NUMBER DETERMINISTIC AS BEGIN RETURN \"DETERMINISTIC\";END;", 1),
            ("CREATE FUNCTION f RETURN s.deterministic AS BEGIN RETURN 1;END;", 0),
            ("CREATE FUNCTION f RETURN deterministic AS BEGIN RETURN 1;END;", 0),
            ("CREATE FUNCTION f RETURN NUMBER AS deterministic NUMBER:=1;BEGIN RETURN deterministic;END;", 0),
            ("CREATE PROCEDURE f DETERMINISTIC AS BEGIN NULL;END;", 0),
            ("SELECT 'CREATE FUNCTION f RETURN NUMBER DETERMINISTIC' FROM DUAL", 0),
        ] {
            assert_eq!(deterministic_positions(&tokens(sql).unwrap()).len(), expected, "{sql}");
        }
    }

    #[test]
    fn deterministic_functions_keep_quoted_parameters_and_execute_after_reopen() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let sql = lower_profile_sql(&db, "CREATE FUNCTION attr_number(\"DETERMINISTIC\" NUMBER) RETURN NUMBER DETERMINISTIC AS BEGIN RETURN \"DETERMINISTIC\"+1;END;", ORACLE_PREVIEW_PROFILE).unwrap();
        assert!(sql.contains("IMMUTABLE"));
        SqlSession::new(&mut db).execute(&sql).unwrap();
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let sql = lower_profile_sql(
            &db,
            "SELECT attr_number(7) FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        let result = SqlSession::new(&mut db).execute(&sql).unwrap();
        assert_eq!(result.rows[0][0].to_cell(), "8");
        let routine = load_routine(&db, RoutineKind::Function, "attr_number")
            .unwrap()
            .unwrap();
        assert!(routine.definition.contains("IMMUTABLE"));
        assert_eq!(routine.declared_volatility(), "i");
    }
}
