SELECT jsonb_pretty(
    jsonb_build_object(
        'fixture_version', 1,
        'oracle_image', :'oracle_image',
        'server_version_num', current_setting('server_version_num')::integer,
        'types', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'oid', type_row.oid::integer,
                    'schema', namespace.nspname,
                    'name', type_row.typname,
                    'display_name', pg_catalog.format_type(type_row.oid, NULL),
                    'kind', type_row.typtype::text,
                    'category', type_row.typcategory::text,
                    'preferred', type_row.typispreferred,
                    'defined', type_row.typisdefined,
                    'delimiter', type_row.typdelim::text,
                    'length', type_row.typlen::integer,
                    'by_value', type_row.typbyval,
                    'alignment', type_row.typalign::text,
                    'storage', type_row.typstorage::text,
                    'collation_oid', type_row.typcollation::integer,
                    'array_oid', NULLIF(type_row.typarray, 0)::integer,
                    'element_oid', NULLIF(type_row.typelem, 0)::integer,
                    'base_type_oid', NULLIF(type_row.typbasetype, 0)::integer,
                    'type_modifier', type_row.typtypmod,
                    'dimensions', type_row.typndims,
                    'not_null', type_row.typnotnull,
                    'input_function_oid', type_row.typinput::oid::integer,
                    'output_function_oid', type_row.typoutput::oid::integer,
                    'receive_function_oid', NULLIF(type_row.typreceive::oid, 0)::integer,
                    'send_function_oid', NULLIF(type_row.typsend::oid, 0)::integer,
                    'modifier_input_function_oid', NULLIF(type_row.typmodin::oid, 0)::integer,
                    'modifier_output_function_oid', NULLIF(type_row.typmodout::oid, 0)::integer,
                    'subscript_function_oid', NULLIF(type_row.typsubscript::oid, 0)::integer,
                    'range_subtype_oid', range_row.rngsubtype::integer,
                    'range_multitype_oid', range_row.rngmultitypid::integer
                )
                ORDER BY type_row.oid
            )
            FROM pg_catalog.pg_type AS type_row
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = type_row.typnamespace
            LEFT JOIN pg_catalog.pg_range AS range_row
              ON range_row.rngtypid = type_row.oid
            WHERE namespace.nspname = 'pg_catalog'
              AND type_row.oid < 16384
        )
    )
);
