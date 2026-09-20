-- Corrected HammerDB workload v2 candidate, not stock HammerDB or a TPC-C certification.
-- Payment v1 is retained. Delivery maintains its counter. NewOrder rejects
-- invalid items, joins line values by item/warehouse keys, and applies all
-- requested quantities and counters when a stock key occurs more than once.
-- The retained tax/discount-adjusted line formula remains unchanged.
-- Existing handled-conflict policy remains; CALL success alone is not a commit gate.

-- Experimental corrected workload, not stock HammerDB. Apply identically to both engines.
CREATE OR REPLACE PROCEDURE public.payment(IN p_w_id integer, IN p_d_id integer, IN p_c_w_id integer, IN p_c_d_id integer, IN byname integer, IN p_h_amount numeric, INOUT p_c_credit character, INOUT p_c_last character varying, INOUT p_c_id integer, INOUT p_w_street_1 character varying, INOUT p_w_street_2 character varying, INOUT p_w_city character varying, INOUT p_w_state character, INOUT p_w_zip character, INOUT p_d_street_1 character varying, INOUT p_d_street_2 character varying, INOUT p_d_city character varying, INOUT p_d_state character, INOUT p_d_zip character, INOUT p_c_first character varying, INOUT p_c_middle character, INOUT p_c_street_1 character varying, INOUT p_c_street_2 character varying, INOUT p_c_city character varying, INOUT p_c_state character, INOUT p_c_zip character, INOUT p_c_phone character, INOUT p_c_since timestamp without time zone, INOUT p_c_credit_lim numeric, INOUT p_c_discount numeric, INOUT p_c_balance numeric, INOUT p_c_data character varying, IN tstamp timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
                DECLARE
                name_count		SMALLINT;
                p_d_name		VARCHAR(11);
                p_w_name		VARCHAR(11);
                h_data			VARCHAR(30);
                c_byname CURSOR FOR
                SELECT c_first, c_middle, c_id,
                c_street_1, c_street_2, c_city, c_state, c_zip,
                c_phone, c_credit, c_credit_lim,
                c_discount, c_balance, c_since
                FROM customer
                WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_last = p_c_last
                ORDER BY c_first;
                BEGIN
                UPDATE warehouse
                SET w_ytd = w_ytd + p_h_amount
                WHERE w_id = p_w_id
                RETURNING w_street_1, w_street_2, w_city, w_state, w_zip, w_name
                INTO p_w_street_1, p_w_street_2, p_w_city, p_w_state, p_w_zip, p_w_name;

                UPDATE district
                SET d_ytd = d_ytd + p_h_amount
                WHERE d_w_id = p_w_id AND d_id = p_d_id
                RETURNING d_street_1, d_street_2, d_city, d_state, d_zip, d_name
                INTO p_d_street_1, p_d_street_2, p_d_city, p_d_state, p_d_zip, p_d_name;

                IF ( byname = 1 )
                THEN
                SELECT count(c_last) INTO name_count
                FROM customer
                WHERE c_last = p_c_last AND c_d_id = p_c_d_id AND c_w_id = p_c_w_id;
                OPEN c_byname;
                FOR loop_counter IN 1 .. ((name_count + 1) / 2)
                LOOP
                FETCH c_byname
                INTO p_c_first, p_c_middle, p_c_id, p_c_street_1, p_c_street_2, p_c_city, p_c_state, p_c_zip, p_c_phone, p_c_credit, p_c_credit_lim, p_c_discount, p_c_balance, p_c_since;
                END LOOP;
                CLOSE c_byname;
                ELSE
                SELECT c_first, c_middle, c_last,
                c_street_1, c_street_2, c_city, c_state, c_zip,
                c_phone, c_credit, c_credit_lim,
                c_discount, c_balance, c_since
                INTO p_c_first, p_c_middle, p_c_last,
                p_c_street_1, p_c_street_2, p_c_city, p_c_state, p_c_zip,
                p_c_phone, p_c_credit, p_c_credit_lim,
                p_c_discount, p_c_balance, p_c_since
                FROM customer
                WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = p_c_id;
                END IF;

                h_data := p_w_name || ' ' || p_d_name;

                IF p_c_credit = 'BC'
                THEN
                UPDATE customer
                SET c_balance = c_balance - p_h_amount,
                c_ytd_payment = c_ytd_payment + p_h_amount,
                c_payment_cnt = c_payment_cnt + 1,
                c_data = substr ((p_c_id || ' ' ||
                p_c_d_id || ' ' ||
                p_c_w_id || ' ' ||
                p_d_id || ' ' ||
                p_w_id || ' ' ||
                to_char (p_h_amount, '9999.99') || ' ' ||
                TO_CHAR(tstamp,'YYYYMMDDHH24MISS') || ' ' ||
                h_data || ' | ') || c_data, 1, 500)
                WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = p_c_id
                RETURNING c_balance, c_data INTO p_c_balance, p_c_data;
                ELSE
                UPDATE customer
                SET c_balance = c_balance - p_h_amount,
                c_ytd_payment = c_ytd_payment + p_h_amount,
                c_payment_cnt = c_payment_cnt + 1
                WHERE c_w_id = p_c_w_id AND c_d_id = p_c_d_id AND c_id = p_c_id
                RETURNING c_balance, c_data INTO p_c_balance, p_c_data;
                END IF;

                INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id,h_w_id, h_date, h_amount, h_data)
                VALUES (p_c_d_id, p_c_w_id, p_c_id, p_d_id,	p_w_id, tstamp, p_h_amount, h_data);

                EXCEPTION
                WHEN serialization_failure OR deadlock_detected OR no_data_found
                THEN ROLLBACK;
                END;
                $procedure$;


CREATE OR REPLACE PROCEDURE public.delivery(IN d_w_id integer, IN d_o_carrier_id integer, IN tstamp timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
                DECLARE
                loop_counter	SMALLINT;
                d_id_in_array	SMALLINT[] := ARRAY[1,2,3,4,5,6,7,8,9,10];
                d_id_array		SMALLINT[];
                o_id_array 		INT[];
                c_id_array 		INT[];
                order_count		SMALLINT;
                sum_amounts     NUMERIC[];

                customer_count INT;
                BEGIN
                WITH new_order_delete AS (
                DELETE
                FROM new_order as del_new_order
                USING UNNEST(d_id_in_array) AS d_ids
                WHERE no_d_id = d_ids
                AND no_w_id = d_w_id
                AND del_new_order.no_o_id = (select min (select_new_order.no_o_id)
                from new_order as select_new_order
                where no_d_id = d_ids
                and no_w_id = d_w_id)
                RETURNING del_new_order.no_o_id, del_new_order.no_d_id
                )
                SELECT array_agg(no_o_id), array_agg(no_d_id)
                FROM new_order_delete
                INTO o_id_array, d_id_array;

                UPDATE orders
                SET o_carrier_id = d_o_carrier_id
                FROM UNNEST(o_id_array, d_id_array) AS ids(o_id, d_id)
                WHERE orders.o_id = ids.o_id
                AND o_d_id = ids.d_id
                AND o_w_id = d_w_id;

                WITH order_line_update AS (
                UPDATE order_line
                SET ol_delivery_d = current_timestamp
                FROM UNNEST(o_id_array, d_id_array) AS ids(o_id, d_id)
                WHERE ol_o_id = ids.o_id
                AND ol_d_id = ids.d_id
                AND ol_w_id = d_w_id
                RETURNING ol_d_id, ol_o_id, ol_amount
                )
                SELECT array_agg(ol_d_id), array_agg(c_id), array_agg(sum_amount)
                FROM ( SELECT ol_d_id,
                ( SELECT DISTINCT o_c_id FROM orders WHERE o_id = ol_o_id AND o_d_id = ol_d_id AND o_w_id = d_w_id) AS c_id,
                sum(ol_amount) AS sum_amount
                FROM order_line_update
                GROUP BY ol_d_id, ol_o_id ) AS inner_sum
                INTO d_id_array, c_id_array, sum_amounts;

                UPDATE customer
                SET c_balance = COALESCE(c_balance,0) + ids_and_sums.sum_amounts,
                    c_delivery_cnt = c_delivery_cnt + 1
                FROM UNNEST(d_id_array, c_id_array, sum_amounts) AS ids_and_sums(d_id, c_id, sum_amounts)
                WHERE customer.c_id = ids_and_sums.c_id
                AND c_d_id = ids_and_sums.d_id
                AND c_w_id = d_w_id;

                EXCEPTION
                WHEN serialization_failure OR deadlock_detected OR no_data_found
                THEN ROLLBACK;
                END;
                $procedure$;

CREATE OR REPLACE PROCEDURE public.neword(IN no_w_id integer, IN no_max_w_id integer, IN no_d_id integer, IN no_c_id integer, IN no_o_ol_cnt integer, INOUT no_c_discount numeric, INOUT no_c_last character varying, INOUT no_c_credit character varying, INOUT no_d_tax numeric, INOUT no_w_tax numeric, INOUT no_d_next_o_id integer, IN tstamp timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
                DECLARE
                no_o_all_local SMALLINT;
                rbk SMALLINT;
                item_id_array INT[];
                supply_wid_array INT[];
                quantity_array SMALLINT[];
                order_line_array SMALLINT[];
                valid_item_count INTEGER;
                inserted_line_count INTEGER;
                BEGIN
                no_o_all_local := 1;
                no_d_next_o_id := 0;
                SELECT c_discount, c_last, c_credit, w_tax
                INTO no_c_discount, no_c_last, no_c_credit, no_w_tax
                FROM customer, warehouse
                WHERE warehouse.w_id = no_w_id AND customer.c_w_id = no_w_id AND customer.c_d_id = no_d_id AND customer.c_id = no_c_id;

                --#2.4.1.4
                rbk := round(DBMS_RANDOM(1,100));
                --#2.4.1.5
                FOR loop_counter IN 1 .. no_o_ol_cnt
                LOOP
                IF ((loop_counter = no_o_ol_cnt) AND (rbk = 1))
                THEN
                item_id_array[loop_counter] := 100001;
                ELSE
                item_id_array[loop_counter] := round(DBMS_RANDOM(1,100000));
                END IF;

                --#2.4.1.5.2
                IF ( no_max_w_id = 1 OR round(DBMS_RANDOM(1,100)) > 1 )
                THEN
                supply_wid_array[loop_counter] := no_w_id;
                ELSE
                no_o_all_local := 0;
                supply_wid_array[loop_counter] := 1 + MOD(CAST (no_w_id + round(DBMS_RANDOM(0,no_max_w_id-2)) AS INT), no_max_w_id);
                END IF;

                --#2.4.1.5.3
                quantity_array[loop_counter] := round(DBMS_RANDOM(1,10));
                order_line_array[loop_counter] := loop_counter;
                END LOOP;


                SELECT count(i.i_id) INTO valid_item_count
                FROM UNNEST(item_id_array) AS requested(item_id)
                LEFT JOIN item AS i ON i.i_id = requested.item_id;
                IF valid_item_count <> no_o_ol_cnt THEN
                    -- Explicit client-visible outcome for the intentional invalid-item rollback.
                    -- Zero remains reserved for an unexpected unsuccessful NewOrder.
                    no_d_next_o_id := -1;
                    RAISE EXCEPTION 'invalid NewOrder item' USING ERRCODE = 'P0002';
                END IF;

                UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = no_d_id AND d_w_id = no_w_id RETURNING d_next_o_id - 1, d_tax INTO no_d_next_o_id, no_d_tax;

                INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (no_d_next_o_id, no_d_id, no_w_id, no_c_id, current_timestamp, no_o_ol_cnt, no_o_all_local);
                INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) VALUES (no_d_next_o_id, no_d_id, no_w_id);

                WITH stock_update AS (
                    UPDATE stock
                    SET s_quantity = (((s_quantity - supplied.quantity - 10) % 91 + 91) % 91) + 10,
                        s_ytd = s_ytd + supplied.quantity,
                        s_order_cnt = s_order_cnt + supplied.line_count,
                        s_remote_cnt = s_remote_cnt + CASE WHEN supplied.supply_wid <> no_w_id THEN supplied.line_count ELSE 0 END
                    FROM (
                        SELECT requested.item_id, requested.supply_wid,
                               sum(requested.quantity) AS quantity, count(*) AS line_count
                        FROM UNNEST(item_id_array, supply_wid_array, quantity_array)
                             AS requested(item_id, supply_wid, quantity)
                        GROUP BY requested.item_id, requested.supply_wid
                    ) AS supplied
                    WHERE stock.s_i_id = supplied.item_id
                      AND stock.s_w_id = supplied.supply_wid
                      AND stock.s_w_id = ANY(supply_wid_array)
                    RETURNING stock.s_i_id AS item_id, stock.s_w_id AS supply_wid,
                              CASE no_d_id WHEN 1 THEN stock.s_dist_01 WHEN 2 THEN stock.s_dist_02 WHEN 3 THEN stock.s_dist_03 WHEN 4 THEN stock.s_dist_04 WHEN 5 THEN stock.s_dist_05 WHEN 6 THEN stock.s_dist_06 WHEN 7 THEN stock.s_dist_07 WHEN 8 THEN stock.s_dist_08 WHEN 9 THEN stock.s_dist_09 WHEN 10 THEN stock.s_dist_10 END AS district_info
                ), inserted_lines AS (
                    INSERT INTO order_line
                        (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id,
                         ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
                    SELECT no_d_next_o_id, no_d_id, no_w_id, requested.line_number,
                           requested.item_id, requested.supply_wid, requested.quantity,
                           requested.quantity * i.i_price * (1 + no_w_tax + no_d_tax) * (1 - no_c_discount),
                           updated.district_info
                    FROM UNNEST(order_line_array, item_id_array, supply_wid_array, quantity_array)
                         AS requested(line_number, item_id, supply_wid, quantity)
                    JOIN stock_update AS updated
                      ON updated.item_id = requested.item_id AND updated.supply_wid = requested.supply_wid
                    JOIN item AS i ON i.i_id = requested.item_id
                    RETURNING ol_number
                )
                SELECT count(*) INTO inserted_line_count FROM inserted_lines;
                IF inserted_line_count <> no_o_ol_cnt THEN
                    RAISE EXCEPTION 'NewOrder stock coverage mismatch';
                END IF;

                EXCEPTION
                WHEN serialization_failure OR deadlock_detected OR no_data_found
                THEN ROLLBACK;
                END;
                $procedure$;
