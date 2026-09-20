CREATE OR REPLACE FUNCTION dbms_random(integer,integer) RETURNS double precision LANGUAGE sql AS $$SELECT 2.0$$;
CALL neword(1,1,1,1,3,0,'','',0,0,0,CAST('2026-09-05 00:00:00' AS timestamp));
SELECT 'duplicate' AS case_name,s_quantity FROM stock WHERE s_i_id=2;
SELECT ol_o_id,ol_number,ol_i_id,ol_quantity,ol_amount FROM order_line ORDER BY ol_o_id,ol_number;
