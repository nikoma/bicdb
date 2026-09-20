//! Canonical PostgreSQL built-in type registry shared by SQL catalogs and pgwire.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgBinaryCodec {
    Bool,
    Bytea,
    SingleByte,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Numeric,
    Money,
    DateDays,
    TimeMicros,
    TimeTz,
    Interval,
    BitString,
    TextPayload,
    TimestampMicros,
    Uuid,
    JsonText,
    JsonbV1,
    JsonPathV1,
    Network,
    Mac48,
    Mac64,
    Geometry,
    Oid32,
    UInt64,
    Tid,
    Range,
    Multirange,
    Lsn64,
    Snapshot64,
    TsVector,
    TsQuery,
    Vector,
    CatalogVector,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgTextCodec {
    Canonical,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgTypeSpec {
    pub name: &'static str,
    pub display_name: &'static str,
    pub aliases: &'static [&'static str],
    pub oid: i32,
    pub array_oid: Option<i32>,
    pub len: i16,
    pub by_value: bool,
    pub category: char,
    pub align: char,
    pub storage: char,
    pub collatable: bool,
    pub pseudo: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgInternalCodecDirection {
    Input,
    Output,
    Receive,
    Send,
}

impl PgTypeSpec {
    pub fn stable_id(self) -> &'static str {
        self.name
    }

    pub fn qualified_name(self) -> String {
        format!("pg_catalog.{}", self.name)
    }

    pub fn collation_oid(self) -> i32 {
        match self.name {
            "name" => 950,
            _ if self.collatable => 100,
            _ => 0,
        }
    }

    pub fn kind(self) -> char {
        if self.pseudo {
            'p'
        } else if self.category == 'R' && self.name.ends_with("multirange") {
            'm'
        } else if self.category == 'R' {
            'r'
        } else {
            'b'
        }
    }

    pub fn element_type_oid(self) -> Option<i32> {
        self.array_oid.map(|_| self.oid)
    }

    pub fn base_element_type_oid(self) -> Option<i32> {
        match self.name {
            "name" => Some(18),
            "int2vector" => Some(21),
            "oidvector" => Some(26),
            "point" | "line" => Some(701),
            "lseg" | "box" => Some(600),
            _ => None,
        }
    }

    pub fn delimiter(self) -> char {
        if self.name == "box" {
            ';'
        } else {
            ','
        }
    }

    pub fn preferred(self) -> bool {
        matches!(self.oid, 16 | 25 | 26 | 701 | 869 | 1184 | 1186 | 1562)
    }

    pub fn array_alignment(self) -> char {
        if self.align == 'd' {
            'd'
        } else {
            'i'
        }
    }

    pub fn typmod_symbols(self) -> Option<(&'static str, &'static str)> {
        Some(match self.name {
            "bpchar" => ("bpchartypmodin", "bpchartypmodout"),
            "varchar" => ("varchartypmodin", "varchartypmodout"),
            "time" => ("timetypmodin", "timetypmodout"),
            "timestamp" => ("timestamptypmodin", "timestamptypmodout"),
            "timestamptz" => ("timestamptztypmodin", "timestamptztypmodout"),
            "interval" => ("intervaltypmodin", "intervaltypmodout"),
            "timetz" => ("timetztypmodin", "timetztypmodout"),
            "bit" => ("bittypmodin", "bittypmodout"),
            "varbit" => ("varbittypmodin", "varbittypmodout"),
            "numeric" => ("numerictypmodin", "numerictypmodout"),
            "vector" => ("vector_typmod_in", "vector_typmod_out"),
            _ => return None,
        })
    }

    pub fn subscript_symbol(self) -> Option<&'static str> {
        Some(match self.name {
            "name" | "point" | "lseg" | "box" | "line" => "raw_array_subscript_handler",
            "int2vector" | "oidvector" => "array_subscript_handler",
            "jsonb" => "jsonb_subscript_handler",
            _ => return None,
        })
    }

    pub fn analyze_symbol(self) -> Option<&'static str> {
        Some(if self.name == "tsvector" {
            "ts_typanalyze"
        } else if self.category == 'R' && self.name.ends_with("multirange") {
            "multirange_typanalyze"
        } else if self.category == 'R' {
            "range_typanalyze"
        } else {
            return None;
        })
    }

    pub fn catalog_codec_symbol(self, direction: PgInternalCodecDirection) -> Option<String> {
        if matches!(
            direction,
            PgInternalCodecDirection::Receive | PgInternalCodecDirection::Send
        ) && matches!(
            self.name,
            "table_am_handler"
                | "index_am_handler"
                | "any"
                | "trigger"
                | "language_handler"
                | "internal"
                | "anyelement"
                | "anynonarray"
                | "fdw_handler"
                | "tsm_handler"
                | "anyenum"
                | "anyrange"
                | "event_trigger"
                | "anymultirange"
                | "anycompatiblemultirange"
                | "anycompatible"
                | "anycompatiblenonarray"
                | "anycompatiblerange"
        ) {
            return None;
        }
        Some(self.internal_codec_symbol(direction))
    }

    pub fn range_subtype_oid(self) -> Option<i32> {
        match self.name {
            "int4range" => Some(23),
            "numrange" => Some(1700),
            "tsrange" => Some(1114),
            "tstzrange" => Some(1184),
            "daterange" => Some(1082),
            "int8range" => Some(20),
            _ => None,
        }
    }

    pub fn range_multirange_oid(self) -> Option<i32> {
        match self.name {
            "int4range" => Some(4451),
            "numrange" => Some(4532),
            "tsrange" => Some(4533),
            "tstzrange" => Some(4534),
            "daterange" => Some(4535),
            "int8range" => Some(4536),
            _ => None,
        }
    }

    pub fn range_subopclass_oid(self) -> Option<i32> {
        match self.name {
            "int4range" => Some(1978),
            "numrange" => Some(3125),
            "tsrange" => Some(3128),
            "tstzrange" => Some(3127),
            "daterange" => Some(3122),
            "int8range" => Some(3124),
            _ => None,
        }
    }

    pub fn range_canonical_oid(self) -> Option<i32> {
        match self.name {
            "int4range" => Some(3914),
            "daterange" => Some(3915),
            "int8range" => Some(3928),
            "numrange" | "tsrange" | "tstzrange" => Some(0),
            _ => None,
        }
    }

    pub fn range_subdiff_oid(self) -> Option<i32> {
        match self.name {
            "int4range" => Some(3922),
            "numrange" => Some(3924),
            "tsrange" => Some(3929),
            "tstzrange" => Some(3930),
            "daterange" => Some(3925),
            "int8range" => Some(3923),
            _ => None,
        }
    }

    pub fn text_codec(self) -> PgTextCodec {
        PgTextCodec::Canonical
    }

    /// PostgreSQL's registered `LANGUAGE internal` symbol for this codec.
    ///
    /// The exceptional stems live beside the canonical type registry instead
    /// of being repeated by CREATE TYPE, catalogs, and pgwire. Callers must
    /// still reject pseudo-types and directions without a runtime codec.
    pub fn internal_codec_symbol(self, direction: PgInternalCodecDirection) -> String {
        let (stem, separated) = match self.name {
            "money" => ("cash", true),
            "refcursor" => ("text", false),
            "polygon" => ("poly", true),
            "int4range" | "numrange" | "tsrange" | "tstzrange" | "daterange" | "int8range" => {
                ("range", true)
            }
            "int4multirange" | "nummultirange" | "tsmultirange" | "tstzmultirange"
            | "datemultirange" | "int8multirange" => ("multirange", true),
            "json"
            | "xml"
            | "cidr"
            | "macaddr8"
            | "macaddr"
            | "inet"
            | "date"
            | "time"
            | "timestamp"
            | "timestamptz"
            | "interval"
            | "timetz"
            | "numeric"
            | "bit"
            | "varbit"
            | "uuid"
            | "jsonb"
            | "jsonpath"
            | "point"
            | "lseg"
            | "path"
            | "box"
            | "line"
            | "circle"
            | "pg_lsn"
            | "pg_snapshot"
            | "txid_snapshot"
            | "vector"
            | "cstring"
            | "trigger"
            | "language_handler"
            | "internal"
            | "record"
            | "pg_ddl_command"
            | "table_am_handler"
            | "index_am_handler"
            | "any"
            | "anyarray"
            | "void"
            | "anyelement"
            | "anynonarray"
            | "fdw_handler"
            | "tsm_handler"
            | "anyenum"
            | "anyrange"
            | "event_trigger"
            | "anymultirange"
            | "anycompatiblemultirange"
            | "anycompatible"
            | "anycompatiblearray"
            | "anycompatiblenonarray"
            | "anycompatiblerange" => (self.name, true),
            _ => (self.name, false),
        };
        let suffix = match direction {
            PgInternalCodecDirection::Input => "in",
            PgInternalCodecDirection::Output => "out",
            PgInternalCodecDirection::Receive => "recv",
            PgInternalCodecDirection::Send => "send",
        };
        if separated {
            format!("{stem}_{suffix}")
        } else {
            format!("{stem}{suffix}")
        }
    }

    pub fn binary_codec(self) -> Option<PgBinaryCodec> {
        Some(match self.name {
            "bool" => PgBinaryCodec::Bool,
            "bytea" => PgBinaryCodec::Bytea,
            "char" => PgBinaryCodec::SingleByte,
            "name" => PgBinaryCodec::TextPayload,
            "int2" => PgBinaryCodec::Int16,
            "int4" => PgBinaryCodec::Int32,
            "int8" => PgBinaryCodec::Int64,
            "float4" => PgBinaryCodec::Float32,
            "float8" => PgBinaryCodec::Float64,
            "numeric" => PgBinaryCodec::Numeric,
            "money" => PgBinaryCodec::Money,
            "date" => PgBinaryCodec::DateDays,
            "time" => PgBinaryCodec::TimeMicros,
            "timetz" => PgBinaryCodec::TimeTz,
            "interval" => PgBinaryCodec::Interval,
            "text" | "varchar" | "bpchar" | "xml" | "refcursor" => PgBinaryCodec::TextPayload,
            "bit" | "varbit" => PgBinaryCodec::BitString,
            "timestamp" | "timestamptz" => PgBinaryCodec::TimestampMicros,
            "uuid" => PgBinaryCodec::Uuid,
            "json" => PgBinaryCodec::JsonText,
            "jsonb" => PgBinaryCodec::JsonbV1,
            "jsonpath" => PgBinaryCodec::JsonPathV1,
            "inet" | "cidr" => PgBinaryCodec::Network,
            "macaddr" => PgBinaryCodec::Mac48,
            "macaddr8" => PgBinaryCodec::Mac64,
            "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => {
                PgBinaryCodec::Geometry
            }
            "oid" | "xid" | "cid" | "regproc" | "regprocedure" | "regoper" | "regoperator"
            | "regclass" | "regcollation" | "regtype" | "regrole" | "regnamespace"
            | "regconfig" | "regdictionary" => PgBinaryCodec::Oid32,
            "xid8" => PgBinaryCodec::UInt64,
            "tid" => PgBinaryCodec::Tid,
            "int4range" | "numrange" | "tsrange" | "tstzrange" | "daterange" | "int8range" => {
                PgBinaryCodec::Range
            }
            "int4multirange" | "nummultirange" | "tsmultirange" | "tstzmultirange"
            | "datemultirange" | "int8multirange" => PgBinaryCodec::Multirange,
            "pg_lsn" => PgBinaryCodec::Lsn64,
            "pg_snapshot" | "txid_snapshot" => PgBinaryCodec::Snapshot64,
            "tsvector" => PgBinaryCodec::TsVector,
            "tsquery" => PgBinaryCodec::TsQuery,
            "vector" => PgBinaryCodec::Vector,
            "int2vector" | "oidvector" => PgBinaryCodec::CatalogVector,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PgTypeRegistry {
    specs: &'static [PgTypeSpec],
}

impl PgTypeRegistry {
    pub const fn new(specs: &'static [PgTypeSpec]) -> Self {
        Self { specs }
    }

    pub fn all(self) -> &'static [PgTypeSpec] {
        self.specs
    }

    pub fn by_stable_id(self, stable_id: &str) -> Option<&'static PgTypeSpec> {
        let stable_id = normalize_type_name(stable_id);
        self.specs.iter().find(|spec| spec.name == stable_id)
    }

    pub fn by_name(self, name: &str) -> Option<&'static PgTypeSpec> {
        let normalized = normalize_type_name(name);
        self.specs.iter().find(|spec| {
            spec.name == normalized || spec.aliases.iter().any(|alias| *alias == normalized)
        })
    }

    pub fn by_oid(self, oid: i32) -> Option<&'static PgTypeSpec> {
        self.specs.iter().find(|spec| spec.oid == oid)
    }

    pub fn by_array_oid(self, oid: i32) -> Option<&'static PgTypeSpec> {
        self.specs.iter().find(|spec| spec.array_oid == Some(oid))
    }
}

macro_rules! pg_type {
    ($name:literal, $display:literal, [$($alias:literal),* $(,)?], $oid:literal,
     $array_oid:expr, $len:literal, $by_value:literal, $category:literal,
     $align:literal, $storage:literal, $collatable:literal, $pseudo:literal) => {
        PgTypeSpec {
            name: $name,
            display_name: $display,
            aliases: &[$($alias),*],
            oid: $oid,
            array_oid: $array_oid,
            len: $len,
            by_value: $by_value,
            category: $category,
            align: $align,
            storage: $storage,
            collatable: $collatable,
            pseudo: $pseudo,
        }
    };
}

// This registry intentionally starts with the types BicDB already advertises.
// New PostgreSQL types must be added here before DDL, catalogs, or pgwire can
// expose them. That keeps partial implementations visible instead of silently
// treating an unknown logical type as text.
pub static PG_TYPE_SPECS: &[PgTypeSpec] = &[
    pg_type!(
        "bool",
        "boolean",
        ["boolean"],
        16,
        Some(1000),
        1,
        true,
        'B',
        'c',
        'p',
        false,
        false
    ),
    pg_type!(
        "bytea",
        "bytea",
        [],
        17,
        Some(1001),
        -1,
        false,
        'U',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "char",
        "\"char\"",
        [],
        18,
        Some(1002),
        1,
        true,
        'Z',
        'c',
        'p',
        false,
        false
    ),
    pg_type!(
        "name",
        "name",
        [],
        19,
        Some(1003),
        64,
        false,
        'S',
        'c',
        'p',
        true,
        false
    ),
    pg_type!(
        "int8",
        "bigint",
        ["bigint"],
        20,
        Some(1016),
        8,
        true,
        'N',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "int2",
        "smallint",
        ["smallint"],
        21,
        Some(1005),
        2,
        true,
        'N',
        's',
        'p',
        false,
        false
    ),
    pg_type!(
        "int2vector",
        "int2vector",
        [],
        22,
        Some(1006),
        -1,
        false,
        'A',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "int4",
        "integer",
        ["int", "integer"],
        23,
        Some(1007),
        4,
        true,
        'N',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "regproc",
        "regproc",
        [],
        24,
        Some(1008),
        4,
        true,
        'N',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "text",
        "text",
        [],
        25,
        Some(1009),
        -1,
        false,
        'S',
        'i',
        'x',
        true,
        false
    ),
    pg_type!(
        "oid",
        "oid",
        [],
        26,
        Some(1028),
        4,
        true,
        'N',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "tid",
        "tid",
        [],
        27,
        Some(1010),
        6,
        false,
        'U',
        's',
        'p',
        false,
        false
    ),
    pg_type!(
        "xid",
        "xid",
        [],
        28,
        Some(1011),
        4,
        true,
        'U',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "cid",
        "cid",
        [],
        29,
        Some(1012),
        4,
        true,
        'U',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "oidvector",
        "oidvector",
        [],
        30,
        Some(1013),
        -1,
        false,
        'A',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "json",
        "json",
        [],
        114,
        Some(199),
        -1,
        false,
        'U',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "xml",
        "xml",
        [],
        142,
        Some(143),
        -1,
        false,
        'U',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "cidr",
        "cidr",
        [],
        650,
        Some(651),
        -1,
        false,
        'I',
        'i',
        'm',
        false,
        false
    ),
    pg_type!(
        "float4",
        "real",
        ["real"],
        700,
        Some(1021),
        4,
        true,
        'N',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "float8",
        "double precision",
        ["double", "double precision", "float"],
        701,
        Some(1022),
        8,
        true,
        'N',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "macaddr8",
        "macaddr8",
        [],
        774,
        Some(775),
        8,
        false,
        'U',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "money",
        "money",
        [],
        790,
        Some(791),
        8,
        true,
        'N',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "macaddr",
        "macaddr",
        [],
        829,
        Some(1040),
        6,
        false,
        'U',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "inet",
        "inet",
        [],
        869,
        Some(1041),
        -1,
        false,
        'I',
        'i',
        'm',
        false,
        false
    ),
    pg_type!(
        "varchar",
        "character varying",
        ["character varying", "varchar"],
        1043,
        Some(1015),
        -1,
        false,
        'S',
        'i',
        'x',
        true,
        false
    ),
    pg_type!(
        "bpchar",
        "character",
        [],
        1042,
        Some(1014),
        -1,
        false,
        'S',
        'i',
        'x',
        true,
        false
    ),
    pg_type!(
        "date",
        "date",
        [],
        1082,
        Some(1182),
        4,
        true,
        'D',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "time",
        "time without time zone",
        ["time without time zone"],
        1083,
        Some(1183),
        8,
        true,
        'D',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "timestamp",
        "timestamp without time zone",
        ["timestamp without time zone"],
        1114,
        Some(1115),
        8,
        true,
        'D',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "timestamptz",
        "timestamp with time zone",
        ["timestamp with time zone"],
        1184,
        Some(1185),
        8,
        true,
        'D',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "interval",
        "interval",
        [],
        1186,
        Some(1187),
        16,
        false,
        'T',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "timetz",
        "time with time zone",
        ["time with time zone"],
        1266,
        Some(1270),
        12,
        false,
        'D',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "numeric",
        "numeric",
        ["decimal", "dec"],
        1700,
        Some(1231),
        -1,
        false,
        'N',
        'i',
        'm',
        false,
        false
    ),
    pg_type!(
        "refcursor",
        "refcursor",
        [],
        1790,
        Some(2201),
        -1,
        false,
        'U',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "bit",
        "bit",
        [],
        1560,
        Some(1561),
        -1,
        false,
        'V',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "varbit",
        "bit varying",
        ["bit varying"],
        1562,
        Some(1563),
        -1,
        false,
        'V',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "regprocedure",
        "regprocedure",
        [],
        2202,
        Some(2207),
        4,
        true,
        'N',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "xid8",
        "xid8",
        [],
        5069,
        Some(271),
        8,
        true,
        'U',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "regclass",
        "regclass",
        [],
        2205,
        Some(2210),
        4,
        true,
        'N',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "regtype",
        "regtype",
        [],
        2206,
        Some(2211),
        4,
        true,
        'N',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "pg_ddl_command",
        "pg_ddl_command",
        [],
        32,
        None,
        8,
        true,
        'P',
        'd',
        'p',
        false,
        true
    ),
    pg_type!(
        "table_am_handler",
        "table_am_handler",
        [],
        269,
        None,
        4,
        true,
        'P',
        'i',
        'p',
        false,
        true
    ),
    pg_type!(
        "index_am_handler",
        "index_am_handler",
        [],
        325,
        None,
        4,
        true,
        'P',
        'i',
        'p',
        false,
        true
    ),
    pg_type!(
        "unknown",
        "unknown",
        [],
        705,
        None,
        -2,
        false,
        'X',
        'c',
        'p',
        false,
        true
    ),
    pg_type!(
        "record",
        "record",
        [],
        2249,
        Some(2287),
        -1,
        false,
        'P',
        'd',
        'x',
        false,
        true
    ),
    pg_type!(
        "cstring",
        "cstring",
        [],
        2275,
        Some(1263),
        -2,
        false,
        'P',
        'c',
        'p',
        false,
        true
    ),
    pg_type!(
        "any",
        "\"any\"",
        [],
        2276,
        None,
        4,
        true,
        'P',
        'i',
        'p',
        false,
        true
    ),
    pg_type!(
        "anyarray",
        "anyarray",
        [],
        2277,
        None,
        -1,
        false,
        'P',
        'd',
        'x',
        false,
        true
    ),
    pg_type!(
        "anyelement",
        "anyelement",
        [],
        2283,
        None,
        4,
        true,
        'P',
        'i',
        'p',
        false,
        true
    ),
    pg_type!(
        "anynonarray",
        "anynonarray",
        [],
        2776,
        None,
        4,
        true,
        'P',
        'i',
        'p',
        false,
        true
    ),
    pg_type!(
        "void",
        "void",
        [],
        2278,
        None,
        4,
        true,
        'P',
        'i',
        'p',
        false,
        true
    ),
    pg_type!(
        "trigger",
        "trigger",
        [],
        2279,
        None,
        4,
        true,
        'P',
        'i',
        'p',
        false,
        true
    ),
    pg_type!(
        "language_handler",
        "language_handler",
        [],
        2280,
        None,
        4,
        true,
        'P',
        'i',
        'p',
        false,
        true
    ),
    pg_type!(
        "internal",
        "internal",
        [],
        2281,
        None,
        8,
        true,
        'P',
        'd',
        'p',
        false,
        true
    ),
    pg_type!(
        "fdw_handler",
        "fdw_handler",
        [],
        3115,
        None,
        4,
        true,
        'P',
        'i',
        'p',
        false,
        true
    ),
    pg_type!(
        "tsm_handler",
        "tsm_handler",
        [],
        3310,
        None,
        4,
        true,
        'P',
        'i',
        'p',
        false,
        true
    ),
    pg_type!(
        "anyenum",
        "anyenum",
        [],
        3500,
        None,
        4,
        true,
        'P',
        'i',
        'p',
        false,
        true
    ),
    pg_type!(
        "anyrange",
        "anyrange",
        [],
        3831,
        None,
        -1,
        false,
        'P',
        'd',
        'x',
        false,
        true
    ),
    pg_type!(
        "event_trigger",
        "event_trigger",
        [],
        3838,
        None,
        4,
        true,
        'P',
        'i',
        'p',
        false,
        true
    ),
    pg_type!(
        "anymultirange",
        "anymultirange",
        [],
        4537,
        None,
        -1,
        false,
        'P',
        'd',
        'x',
        false,
        true
    ),
    pg_type!(
        "anycompatiblemultirange",
        "anycompatiblemultirange",
        [],
        4538,
        None,
        -1,
        false,
        'P',
        'd',
        'x',
        false,
        true
    ),
    pg_type!(
        "anycompatible",
        "anycompatible",
        [],
        5077,
        None,
        4,
        true,
        'P',
        'i',
        'p',
        false,
        true
    ),
    pg_type!(
        "anycompatiblearray",
        "anycompatiblearray",
        [],
        5078,
        None,
        -1,
        false,
        'P',
        'd',
        'x',
        false,
        true
    ),
    pg_type!(
        "anycompatiblenonarray",
        "anycompatiblenonarray",
        [],
        5079,
        None,
        4,
        true,
        'P',
        'i',
        'p',
        false,
        true
    ),
    pg_type!(
        "anycompatiblerange",
        "anycompatiblerange",
        [],
        5080,
        None,
        -1,
        false,
        'P',
        'd',
        'x',
        false,
        true
    ),
    pg_type!(
        "uuid",
        "uuid",
        [],
        2950,
        Some(2951),
        16,
        false,
        'U',
        'c',
        'p',
        false,
        false
    ),
    pg_type!(
        "tsvector",
        "tsvector",
        [],
        3614,
        Some(3643),
        -1,
        false,
        'U',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "tsquery",
        "tsquery",
        [],
        3615,
        Some(3645),
        -1,
        false,
        'U',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "regconfig",
        "regconfig",
        [],
        3734,
        Some(3735),
        4,
        true,
        'N',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "regdictionary",
        "regdictionary",
        [],
        3769,
        Some(3770),
        4,
        true,
        'N',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "jsonb",
        "jsonb",
        [],
        3802,
        Some(3807),
        -1,
        false,
        'U',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "jsonpath",
        "jsonpath",
        [],
        4072,
        Some(4073),
        -1,
        false,
        'U',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "int4range",
        "int4range",
        [],
        3904,
        Some(3905),
        -1,
        false,
        'R',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "numrange",
        "numrange",
        [],
        3906,
        Some(3907),
        -1,
        false,
        'R',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "tsrange",
        "tsrange",
        [],
        3908,
        Some(3909),
        -1,
        false,
        'R',
        'd',
        'x',
        false,
        false
    ),
    pg_type!(
        "tstzrange",
        "tstzrange",
        [],
        3910,
        Some(3911),
        -1,
        false,
        'R',
        'd',
        'x',
        false,
        false
    ),
    pg_type!(
        "daterange",
        "daterange",
        [],
        3912,
        Some(3913),
        -1,
        false,
        'R',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "int8range",
        "int8range",
        [],
        3926,
        Some(3927),
        -1,
        false,
        'R',
        'd',
        'x',
        false,
        false
    ),
    pg_type!(
        "regnamespace",
        "regnamespace",
        [],
        4089,
        Some(4090),
        4,
        true,
        'N',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "point",
        "point",
        [],
        600,
        Some(1017),
        16,
        false,
        'G',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "lseg",
        "lseg",
        [],
        601,
        Some(1018),
        32,
        false,
        'G',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "path",
        "path",
        [],
        602,
        Some(1019),
        -1,
        false,
        'G',
        'd',
        'x',
        false,
        false
    ),
    pg_type!(
        "box",
        "box",
        [],
        603,
        Some(1020),
        32,
        false,
        'G',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "polygon",
        "polygon",
        [],
        604,
        Some(1027),
        -1,
        false,
        'G',
        'd',
        'x',
        false,
        false
    ),
    pg_type!(
        "line",
        "line",
        [],
        628,
        Some(629),
        24,
        false,
        'G',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "circle",
        "circle",
        [],
        718,
        Some(719),
        24,
        false,
        'G',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "regoper",
        "regoper",
        [],
        2203,
        Some(2208),
        4,
        true,
        'N',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "regoperator",
        "regoperator",
        [],
        2204,
        Some(2209),
        4,
        true,
        'N',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "txid_snapshot",
        "txid_snapshot",
        [],
        2970,
        Some(2949),
        -1,
        false,
        'U',
        'd',
        'x',
        false,
        false
    ),
    pg_type!(
        "pg_lsn",
        "pg_lsn",
        [],
        3220,
        Some(3221),
        8,
        true,
        'U',
        'd',
        'p',
        false,
        false
    ),
    pg_type!(
        "regrole",
        "regrole",
        [],
        4096,
        Some(4097),
        4,
        true,
        'N',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "regcollation",
        "regcollation",
        [],
        4191,
        Some(4192),
        4,
        true,
        'N',
        'i',
        'p',
        false,
        false
    ),
    pg_type!(
        "int4multirange",
        "int4multirange",
        [],
        4451,
        Some(6150),
        -1,
        false,
        'R',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "nummultirange",
        "nummultirange",
        [],
        4532,
        Some(6151),
        -1,
        false,
        'R',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "tsmultirange",
        "tsmultirange",
        [],
        4533,
        Some(6152),
        -1,
        false,
        'R',
        'd',
        'x',
        false,
        false
    ),
    pg_type!(
        "tstzmultirange",
        "tstzmultirange",
        [],
        4534,
        Some(6153),
        -1,
        false,
        'R',
        'd',
        'x',
        false,
        false
    ),
    pg_type!(
        "datemultirange",
        "datemultirange",
        [],
        4535,
        Some(6155),
        -1,
        false,
        'R',
        'i',
        'x',
        false,
        false
    ),
    pg_type!(
        "int8multirange",
        "int8multirange",
        [],
        4536,
        Some(6157),
        -1,
        false,
        'R',
        'd',
        'x',
        false,
        false
    ),
    pg_type!(
        "pg_snapshot",
        "pg_snapshot",
        [],
        5038,
        Some(5039),
        -1,
        false,
        'U',
        'd',
        'x',
        false,
        false
    ),
    pg_type!(
        "pg_node_tree",
        "pg_node_tree",
        [],
        194,
        None,
        -1,
        false,
        'Z',
        'i',
        'x',
        true,
        false
    ),
    pg_type!(
        "pg_ndistinct",
        "pg_ndistinct",
        [],
        3361,
        None,
        -1,
        false,
        'Z',
        'i',
        'x',
        true,
        false
    ),
    pg_type!(
        "pg_dependencies",
        "pg_dependencies",
        [],
        3402,
        None,
        -1,
        false,
        'Z',
        'i',
        'x',
        true,
        false
    ),
    pg_type!(
        "pg_mcv_list",
        "pg_mcv_list",
        [],
        5017,
        None,
        -1,
        false,
        'Z',
        'i',
        'x',
        true,
        false
    ),
    pg_type!(
        "vector",
        "vector",
        [],
        380_200,
        Some(380_201),
        -1,
        false,
        'U',
        'i',
        'x',
        false,
        false
    ),
];

pub static PG_TYPE_REGISTRY: PgTypeRegistry = PgTypeRegistry::new(PG_TYPE_SPECS);

fn normalize_type_name(name: &str) -> String {
    name.trim()
        .trim_matches('"')
        .strip_prefix("pg_catalog.")
        .unwrap_or_else(|| name.trim().trim_matches('"'))
        .to_ascii_lowercase()
}

pub fn pg_type_spec(name: &str) -> Option<&'static PgTypeSpec> {
    PG_TYPE_REGISTRY.by_name(name)
}

pub fn pg_type_spec_by_oid(oid: i32) -> Option<&'static PgTypeSpec> {
    PG_TYPE_REGISTRY.by_oid(oid)
}

pub fn pg_type_delimiter(name: &str) -> Option<char> {
    pg_type_spec(name).map(|spec| spec.delimiter())
}

pub fn pg_type_delimiter_by_oid(oid: i32) -> Option<char> {
    pg_type_spec_by_oid(oid).map(|spec| spec.delimiter())
}

pub fn pg_array_element_spec_by_oid(oid: i32) -> Option<&'static PgTypeSpec> {
    PG_TYPE_REGISTRY.by_array_oid(oid)
}

pub fn pg_type_oid_by_name(name: &str) -> Option<i32> {
    let normalized = normalize_type_name(name);
    if let Some(element) = normalized
        .strip_suffix("[]")
        .or_else(|| normalized.strip_prefix('_'))
    {
        return pg_type_spec(element).and_then(|spec| spec.array_oid);
    }
    pg_type_spec(&normalized).map(|spec| spec.oid)
}

pub fn pg_type_name_by_oid(oid: i32) -> Option<&'static str> {
    if let Some(spec) = pg_type_spec_by_oid(oid) {
        return Some(spec.display_name);
    }
    let spec = pg_array_element_spec_by_oid(oid)?;
    Some(match spec.name {
        "bool" => "boolean[]",
        "int2" => "smallint[]",
        "int4" => "integer[]",
        "int8" => "bigint[]",
        "float4" => "real[]",
        "float8" => "double precision[]",
        "varchar" => "character varying[]",
        "bpchar" => "character[]",
        "time" => "time without time zone[]",
        "timestamp" => "timestamp without time zone[]",
        "timestamptz" => "timestamp with time zone[]",
        "bytea" => "bytea[]",
        "char" => "\"char\"[]",
        "name" => "name[]",
        "int2vector" => "int2vector[]",
        "text" => "text[]",
        "oid" => "oid[]",
        "tid" => "tid[]",
        "xid" => "xid[]",
        "cid" => "cid[]",
        "xid8" => "xid8[]",
        "oidvector" => "oidvector[]",
        "regproc" => "regproc[]",
        "json" => "json[]",
        "xml" => "xml[]",
        "cidr" => "cidr[]",
        "macaddr8" => "macaddr8[]",
        "money" => "money[]",
        "macaddr" => "macaddr[]",
        "inet" => "inet[]",
        "date" => "date[]",
        "interval" => "interval[]",
        "timetz" => "time with time zone[]",
        "numeric" => "numeric[]",
        "bit" => "bit[]",
        "varbit" => "bit varying[]",
        "regprocedure" => "regprocedure[]",
        "refcursor" => "refcursor[]",
        "regclass" => "regclass[]",
        "regtype" => "regtype[]",
        "record" => "record[]",
        "uuid" => "uuid[]",
        "tsvector" => "tsvector[]",
        "tsquery" => "tsquery[]",
        "regconfig" => "regconfig[]",
        "regdictionary" => "regdictionary[]",
        "jsonb" => "jsonb[]",
        "jsonpath" => "jsonpath[]",
        "point" => "point[]",
        "lseg" => "lseg[]",
        "path" => "path[]",
        "box" => "box[]",
        "polygon" => "polygon[]",
        "line" => "line[]",
        "circle" => "circle[]",
        "regoper" => "regoper[]",
        "regoperator" => "regoperator[]",
        "txid_snapshot" => "txid_snapshot[]",
        "pg_lsn" => "pg_lsn[]",
        "regrole" => "regrole[]",
        "regcollation" => "regcollation[]",
        "int4multirange" => "int4multirange[]",
        "nummultirange" => "nummultirange[]",
        "tsmultirange" => "tsmultirange[]",
        "tstzmultirange" => "tstzmultirange[]",
        "datemultirange" => "datemultirange[]",
        "int8multirange" => "int8multirange[]",
        "pg_snapshot" => "pg_snapshot[]",
        "vector" => "vector[]",
        "int4range" => "int4range[]",
        "numrange" => "numrange[]",
        "tsrange" => "tsrange[]",
        "tstzrange" => "tstzrange[]",
        "daterange" => "daterange[]",
        "int8range" => "int8range[]",
        "regnamespace" => "regnamespace[]",
        "cstring" => "cstring[]",
        _ => return None,
    })
}

pub fn pg_internal_codec_type(
    symbol: &str,
) -> Option<(&'static PgTypeSpec, PgInternalCodecDirection)> {
    let symbol = symbol.trim().to_ascii_lowercase();
    let mut found: Option<(&'static PgTypeSpec, PgInternalCodecDirection)> = None;
    for spec in PG_TYPE_SPECS.iter().filter(|spec| !spec.pseudo) {
        for direction in [
            PgInternalCodecDirection::Input,
            PgInternalCodecDirection::Output,
            PgInternalCodecDirection::Receive,
            PgInternalCodecDirection::Send,
        ] {
            if matches!(
                direction,
                PgInternalCodecDirection::Receive | PgInternalCodecDirection::Send
            ) && spec.binary_codec().is_none()
            {
                continue;
            }
            if spec.internal_codec_symbol(direction) != symbol {
                continue;
            }
            if let Some((existing, _)) = found {
                if matches!((existing.name, spec.name), ("text", "refcursor")) {
                    continue;
                }
                // Shared polymorphic symbols such as range_in do not identify
                // a safe concrete storage codec without native typmod logic.
                return None;
            }
            found = Some((spec, direction));
        }
    }
    found
}

pub fn pg_array_element_oid(array_oid: i32) -> Option<i32> {
    match array_oid {
        22 => return Some(21),
        30 => return Some(26),
        _ => {}
    }
    pg_array_element_spec_by_oid(array_oid).map(|spec| spec.oid)
}

pub fn pg_range_statistics_bounds_type(pg_type: &str) -> Option<&'static str> {
    Some(match normalize_type_name(pg_type).as_str() {
        "int4range" | "int4multirange" => "int4range",
        "int8range" | "int8multirange" => "int8range",
        "numrange" | "nummultirange" => "numrange",
        "tsrange" | "tsmultirange" => "tsrange",
        "tstzrange" | "tstzmultirange" => "tstzrange",
        "daterange" | "datemultirange" => "daterange",
        _ => return None,
    })
}

pub fn pg_format_type(oid: i32, typmod: i32) -> Option<String> {
    if let Some(element) = pg_array_element_spec_by_oid(oid) {
        return pg_format_type(element.oid, typmod).map(|name| format!("{name}[]"));
    }
    let spec = pg_type_spec_by_oid(oid)?;
    if typmod < 0 {
        return Some(spec.display_name.to_string());
    }
    Some(match spec.name {
        "bpchar" | "varchar" if typmod >= 4 => {
            format!("{}({})", spec.display_name, typmod - 4)
        }
        "bit" | "varbit" => format!("{}({typmod})", spec.display_name),
        "numeric" if typmod >= 4 => {
            let packed = typmod - 4;
            let precision = (packed >> 16) & 0xffff;
            let mut scale = packed & 0x7ff;
            if scale & 0x400 != 0 {
                scale |= !0x7ff;
            }
            format!("numeric({precision},{scale})")
        }
        "time" => format!("time({typmod}) without time zone"),
        "timetz" => format!("time({typmod}) with time zone"),
        "timestamp" => format!("timestamp({typmod}) without time zone"),
        "timestamptz" => format!("timestamp({typmod}) with time zone"),
        "interval" => {
            let range = (typmod as u32) >> 16;
            let precision = (typmod as u32) & 0xffff;
            let fields = match range {
                0x0004 => Some("year"),
                0x0002 => Some("month"),
                0x0008 => Some("day"),
                0x0400 => Some("hour"),
                0x0800 => Some("minute"),
                0x1000 => Some("second"),
                0x0006 => Some("year to month"),
                0x0408 => Some("day to hour"),
                0x0c08 => Some("day to minute"),
                0x1c08 => Some("day to second"),
                0x0c00 => Some("hour to minute"),
                0x1c00 => Some("hour to second"),
                0x1800 => Some("minute to second"),
                _ => None,
            };
            let mut rendered = "interval".to_string();
            if let Some(fields) = fields {
                rendered.push(' ');
                rendered.push_str(fields);
            }
            if precision != 0xffff {
                rendered.push('(');
                rendered.push_str(&precision.to_string());
                rendered.push(')');
            }
            rendered
        }
        "vector" => format!("vector({typmod})"),
        _ => spec.display_name.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::collections::BTreeSet;

    #[derive(Debug, Deserialize)]
    struct OracleTypeInventory {
        fixture_version: u32,
        oracle_image: String,
        server_version_num: u32,
        types: Vec<OracleTypeRow>,
    }

    #[derive(Debug, Deserialize)]
    struct OracleTypeRow {
        oid: i32,
        schema: String,
        name: String,
        display_name: String,
        kind: String,
        category: String,
        preferred: bool,
        defined: bool,
        delimiter: String,
        length: i16,
        by_value: bool,
        alignment: String,
        storage: String,
        collation_oid: i32,
        array_oid: Option<i32>,
        element_oid: Option<i32>,
        base_type_oid: Option<i32>,
        type_modifier: i32,
        dimensions: i32,
        not_null: bool,
        range_subtype_oid: Option<i32>,
        range_multitype_oid: Option<i32>,
        modifier_input_function_oid: Option<i32>,
        modifier_output_function_oid: Option<i32>,
        subscript_function_oid: Option<i32>,
    }

    fn pg18_oracle_inventory() -> OracleTypeInventory {
        serde_json::from_str(include_str!(
            "../../../fixtures/postgresql-18/type-inventory.json"
        ))
        .expect("checked-in PostgreSQL 18 type inventory must be valid JSON")
    }

    #[test]
    fn registry_keys_and_capabilities_are_unambiguous() {
        let mut stable_ids = BTreeSet::new();
        let mut oids = BTreeSet::new();
        let mut array_oids = BTreeSet::new();

        for spec in PG_TYPE_REGISTRY.all() {
            assert!(
                stable_ids.insert(spec.stable_id()),
                "duplicate stable type id"
            );
            assert!(oids.insert(spec.oid), "duplicate type oid {}", spec.oid);
            assert_eq!(PG_TYPE_REGISTRY.by_stable_id(spec.stable_id()), Some(spec));
            assert_eq!(PG_TYPE_REGISTRY.by_name(&spec.qualified_name()), Some(spec));
            assert_eq!(PG_TYPE_REGISTRY.by_oid(spec.oid), Some(spec));
            assert_eq!(spec.text_codec(), PgTextCodec::Canonical);

            if let Some(array_oid) = spec.array_oid {
                assert!(
                    array_oids.insert(array_oid),
                    "duplicate array oid {array_oid}"
                );
                assert!(
                    !oids.contains(&array_oid),
                    "array oid collides with scalar oid"
                );
                assert_eq!(PG_TYPE_REGISTRY.by_array_oid(array_oid), Some(spec));
                assert_eq!(spec.element_type_oid(), Some(spec.oid));
            }
        }

        assert_eq!(
            PG_TYPE_REGISTRY
                .by_stable_id("numrange")
                .and_then(|spec| spec.range_subtype_oid()),
            Some(1700)
        );
        assert_eq!(
            PG_TYPE_REGISTRY
                .by_stable_id("uuid")
                .and_then(|spec| spec.binary_codec()),
            Some(PgBinaryCodec::Uuid)
        );
        assert_eq!(
            PG_TYPE_REGISTRY
                .by_stable_id("int2vector")
                .and_then(|spec| spec.base_element_type_oid()),
            Some(21)
        );
        assert_eq!(
            PG_TYPE_REGISTRY
                .by_stable_id("oidvector")
                .and_then(|spec| spec.base_element_type_oid()),
            Some(26)
        );
        assert_eq!(
            PG_TYPE_REGISTRY
                .by_stable_id("cidr")
                .and_then(|spec| spec.binary_codec()),
            Some(PgBinaryCodec::Network)
        );
        assert_eq!(
            pg_internal_codec_type("textin").map(|(spec, direction)| (spec.name, direction)),
            Some(("text", PgInternalCodecDirection::Input))
        );
        assert_eq!(pg_internal_codec_type("range_in"), None);
    }

    #[test]
    fn registry_metadata_matches_postgresql_18_oracle() {
        let inventory = pg18_oracle_inventory();
        assert_eq!(inventory.fixture_version, 1);
        assert_eq!(inventory.oracle_image, "postgres:18.4");
        assert_eq!(inventory.server_version_num, 180004);

        for spec in PG_TYPE_REGISTRY.all() {
            // vector is a BicDB/pgvector extension and is not installed in the
            // stock PostgreSQL oracle used for core type parity.
            if spec.name == "vector" {
                continue;
            }

            let row = inventory
                .types
                .iter()
                .find(|row| row.oid == spec.oid)
                .unwrap_or_else(|| {
                    panic!("PostgreSQL 18 has no OID {} for {}", spec.oid, spec.name)
                });
            assert_eq!(row.schema, "pg_catalog", "schema for {}", spec.name);
            assert_eq!(row.name, spec.name, "name for OID {}", spec.oid);
            assert_eq!(
                row.display_name, spec.display_name,
                "display name for {}",
                spec.name
            );
            assert_eq!(row.kind, spec.kind().to_string(), "kind for {}", spec.name);
            assert_eq!(
                row.category,
                spec.category.to_string(),
                "category for {}",
                spec.name
            );
            assert_eq!(row.length, spec.len, "length for {}", spec.name);
            assert_eq!(
                row.by_value, spec.by_value,
                "by-value flag for {}",
                spec.name
            );
            assert_eq!(
                row.alignment,
                spec.align.to_string(),
                "alignment for {}",
                spec.name
            );
            assert_eq!(
                row.storage,
                spec.storage.to_string(),
                "storage for {}",
                spec.name
            );
            assert_eq!(
                row.collation_oid,
                spec.collation_oid(),
                "collation for {}",
                spec.name
            );
            assert_eq!(row.array_oid, spec.array_oid, "array OID for {}", spec.name);
            assert_eq!(
                row.preferred,
                spec.preferred(),
                "preferred for {}",
                spec.name
            );
            assert!(row.defined, "built-in {} must be defined", spec.name);
            assert_eq!(
                row.delimiter,
                spec.delimiter().to_string(),
                "delimiter for {}",
                spec.name
            );
            assert_eq!(
                row.element_oid,
                spec.base_element_type_oid(),
                "element for {}",
                spec.name
            );
            assert_eq!(row.base_type_oid, None, "basetype for {}", spec.name);
            assert_eq!(row.type_modifier, -1, "typmod for {}", spec.name);
            assert_eq!(row.dimensions, 0, "dimensions for {}", spec.name);
            assert!(!row.not_null, "built-in {} cannot be not-null", spec.name);
            assert_eq!(
                row.range_subtype_oid,
                spec.range_subtype_oid(),
                "range subtype for {}",
                spec.name
            );
            assert_eq!(
                row.range_multitype_oid,
                spec.range_multirange_oid(),
                "range multitype for {}",
                spec.name
            );
            assert_eq!(
                row.modifier_input_function_oid.is_some(),
                spec.typmod_symbols().is_some(),
                "typmod input for {}",
                spec.name
            );
            assert_eq!(
                row.modifier_output_function_oid.is_some(),
                spec.typmod_symbols().is_some(),
                "typmod output for {}",
                spec.name
            );
            assert_eq!(
                row.subscript_function_oid,
                spec.subscript_symbol().map(|symbol| match symbol {
                    "array_subscript_handler" => 6179,
                    "raw_array_subscript_handler" => 6180,
                    "jsonb_subscript_handler" => 6098,
                    _ => unreachable!("unknown subscript symbol {symbol}"),
                }),
                "subscript handler for {}",
                spec.name
            );

            if let Some(array_oid) = spec.array_oid {
                let array_row = inventory
                    .types
                    .iter()
                    .find(|row| row.oid == array_oid)
                    .unwrap_or_else(|| panic!("PostgreSQL 18 has no array OID {array_oid}"));
                assert_eq!(
                    array_row.element_oid,
                    Some(spec.oid),
                    "array element OID for {}",
                    spec.name
                );
                assert_eq!(
                    array_row.category,
                    if spec.name == "record" { "P" } else { "A" },
                    "array category for {}",
                    spec.name
                );
                assert_eq!(
                    array_row.kind,
                    if spec.name == "record" { "p" } else { "b" }
                );
                assert!(!array_row.preferred);
                assert!(array_row.defined);
                assert_eq!(array_row.delimiter, spec.delimiter().to_string());
                assert_eq!(array_row.length, -1);
                assert!(!array_row.by_value);
                assert_eq!(array_row.alignment, spec.array_alignment().to_string());
                assert_eq!(array_row.storage, "x");
                assert_eq!(array_row.collation_oid, spec.collation_oid());
                assert_eq!(array_row.base_type_oid, None);
                assert_eq!(array_row.type_modifier, -1);
                assert_eq!(array_row.dimensions, 0);
                assert!(!array_row.not_null);
                assert_eq!(array_row.subscript_function_oid, Some(6179));
                assert_eq!(
                    array_row.modifier_input_function_oid.is_some(),
                    spec.typmod_symbols().is_some()
                );
                assert_eq!(
                    array_row.modifier_output_function_oid.is_some(),
                    spec.typmod_symbols().is_some()
                );
            }
        }
    }
    use std::collections::HashSet;

    #[test]
    fn registry_has_unique_scalar_and_array_oids() {
        let mut oids = HashSet::new();
        for spec in PG_TYPE_SPECS {
            assert!(oids.insert(spec.oid), "duplicate scalar oid {}", spec.oid);
            if let Some(array_oid) = spec.array_oid {
                assert!(oids.insert(array_oid), "duplicate array oid {array_oid}");
                assert_eq!(pg_array_element_oid(array_oid), Some(spec.oid));
            }
        }
    }

    #[test]
    fn names_aliases_and_arrays_round_trip() {
        for spec in PG_TYPE_SPECS {
            assert_eq!(pg_type_oid_by_name(spec.name), Some(spec.oid));
            assert_eq!(pg_type_name_by_oid(spec.oid), Some(spec.display_name));
            for alias in spec.aliases {
                assert_eq!(pg_type_oid_by_name(alias), Some(spec.oid));
            }
            if let Some(array_oid) = spec.array_oid {
                assert_eq!(
                    pg_type_oid_by_name(&format!("{}[]", spec.name)),
                    Some(array_oid)
                );
                assert!(pg_type_name_by_oid(array_oid).is_some());
            }
        }
    }

    #[test]
    fn format_type_decodes_postgresql_typmods() {
        assert_eq!(
            pg_format_type(1043, 16).as_deref(),
            Some("character varying(12)")
        );
        assert_eq!(
            pg_format_type(1015, 16).as_deref(),
            Some("character varying(12)[]")
        );
        assert_eq!(
            pg_format_type(1114, 3).as_deref(),
            Some("timestamp(3) without time zone")
        );
        assert_eq!(
            pg_format_type(1700, (19_i32 << 16) + 4 + 4).as_deref(),
            Some("numeric(19,4)")
        );
        assert_eq!(
            pg_format_type(1700, (10_i32 << 16) + 0x7fe + 4).as_deref(),
            Some("numeric(10,-2)")
        );
    }
}
