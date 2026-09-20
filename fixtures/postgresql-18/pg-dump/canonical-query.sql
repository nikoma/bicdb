SELECT row_to_json(value)::text
FROM (
    SELECT
        id::text AS id,
        bool_value::text AS bool_value,
        small_value::text AS small_value,
        int_value::text AS int_value,
        big_value::text AS big_value,
        real_value::text AS real_value,
        double_value::text AS double_value,
        exact_value::text AS exact_value,
        money_value::numeric::text AS money_value,
        char_value::text AS char_value,
        varchar_value::text AS varchar_value,
        text_value,
        bytes_value::text AS bytes_value,
        bit_value::text AS bit_value,
        varbit_value::text AS varbit_value,
        date_value::text AS date_value,
        time_value::text AS time_value,
        timetz_value::text AS timetz_value,
        timestamp_value::text AS timestamp_value,
        timestamptz_value::text AS timestamptz_value,
        interval_value::text AS interval_value,
        json_value::text AS json_value,
        jsonb_value::text AS jsonb_value,
        jsonpath_value::text AS jsonpath_value,
        xml_value::text AS xml_value,
        inet_value::text AS inet_value,
        cidr_value::text AS cidr_value,
        mac_value::text AS mac_value,
        mac8_value::text AS mac8_value,
        point_value::text AS point_value,
        line_value::text AS line_value,
        lseg_value::text AS lseg_value,
        box_value::text AS box_value,
        path_value::text AS path_value,
        polygon_value::text AS polygon_value,
        circle_value::text AS circle_value,
        document_vector::text AS document_vector,
        document_query::text AS document_query,
        int4_span::text AS int4_span,
        int8_span::text AS int8_span,
        numeric_span::text AS numeric_span,
        timestamp_span::text AS timestamp_span,
        timestamptz_span::text AS timestamptz_span,
        date_span::text AS date_span,
        int4_spans::text AS int4_spans,
        int8_spans::text AS int8_spans,
        numeric_spans::text AS numeric_spans,
        timestamp_spans::text AS timestamp_spans,
        timestamptz_spans::text AS timestamptz_spans,
        date_spans::text AS date_spans,
        mood_value::text AS mood_value,
        positive_value::text AS positive_value,
        pair_value::text AS pair_value,
        custom_span::text AS custom_span,
        code_value::text AS code_value,
        code_values::text AS code_values,
        oid_value::text AS oid_value,
        lsn_value::text AS lsn_value,
        snapshot_value::text AS snapshot_value,
        xid_value::text AS xid_value,
        xid8_value::text AS xid8_value,
        cid_value::text AS cid_value,
        tid_value::text AS tid_value,
        int_values::text AS int_values,
        exact_values::text AS exact_values,
        uuid_values::text AS uuid_values,
        mood_values::text AS mood_values,
        range_values::text AS range_values,
        cardinality(pair_values)::text AS pair_values_count
    FROM dump_type_families
) AS value;

SELECT id::text || '|' || family_id::text || '|' || label
FROM dump_identity
ORDER BY id;

SELECT id::text || '|' || exact_value::text || '|' || mood_value::text
FROM dump_type_view
ORDER BY id;

SELECT id::text || '|' || item::text || '|' || items::text
FROM dump_row_holder
ORDER BY id;
