use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn postgres_format_handles_positions_and_widths() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for (expression, expected) in [
        (
            "format('%s %1$s %s', 'World', 'again')",
            "World World again",
        ),
        ("format('%2$*3$s|%s', 'unused', 'value', 8)", "   value|8"),
        ("format('%*2$s|%s', 'unused', 4, 'f', 'next')", "   f|next"),
        ("format('>%*s<', -4, 'é')", ">é   <"),
        ("format('>%4s%%<', NULL)", ">    %<"),
        ("format('>%-5I<', 'a b')", ">\"a b\"<"),
        ("format('%I:%L:%s', true, false, true)", "t:'f':t"),
        ("format('%2$s:%1$s', VARIADIC ARRAY[1, 2])", "2:1"),
        ("format('literal', VARIADIC NULL::int[])", "literal"),
    ] {
        let result = sql.execute(&format!("SELECT {expression}")).unwrap();
        assert_eq!(result.columns, ["format"], "{expression}");
        assert_eq!(
            result.rows,
            vec![vec![SqlValue::String(expected.into())]],
            "{expression}"
        );
    }
}

#[test]
fn postgres_format_validates_argument_references_and_conversion_types() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for (expression, state, message) in [
        ("format('%0$s')", "22023", "format specifies argument 0"),
        ("format('%*0$')", "22023", "format specifies argument 0"),
        ("format('%*1s', 1, 'x')", "22023", "width argument position"),
        (
            "format('%1$1', 1)",
            "22023",
            "unterminated format() type specifier",
        ),
        (
            "format('%1$x')",
            "22023",
            "unrecognized format() type specifier",
        ),
        (
            "format('%3$s', 'a')",
            "22023",
            "too few arguments for format()",
        ),
        (
            "format('%s', VARIADIC NULL::int[])",
            "22023",
            "too few arguments for format()",
        ),
        (
            "format('%I', NULL)",
            "22004",
            "null values cannot be formatted",
        ),
        (
            "format('%2147483648s', 'a')",
            "22003",
            "number is out of range",
        ),
        (
            "format('%*s', 2.5, 'a')",
            "22P02",
            "invalid input syntax for type integer",
        ),
    ] {
        let error = sql.execute(&format!("SELECT {expression}")).unwrap_err();
        assert_eq!(error.sqlstate(), state, "{expression}: {error}");
        assert!(error.to_string().contains(message), "{expression}: {error}");
    }
}
