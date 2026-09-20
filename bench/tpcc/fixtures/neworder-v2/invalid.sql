CALL neword(1,1,1,1,1,0,'','',0,0,0,CAST('2026-09-05 00:00:00' AS timestamp));
SELECT 'invalid' AS case_name,d_next_o_id,(SELECT count(*) FROM orders) AS orders,(SELECT count(*) FROM order_line WHERE ol_amount IS NULL) AS null_lines FROM district;
