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
                SET c_balance = COALESCE(c_balance,0) + ids_and_sums.sum_amounts
                FROM UNNEST(d_id_array, c_id_array, sum_amounts) AS ids_and_sums(d_id, c_id, sum_amounts)
                WHERE customer.c_id = ids_and_sums.c_id
                AND c_d_id = ids_and_sums.d_id
                AND c_w_id = d_w_id;

                EXCEPTION
                WHEN serialization_failure OR deadlock_detected OR no_data_found
                THEN ROLLBACK;
                END;
                $procedure$

CREATE OR REPLACE PROCEDURE public.neword(IN no_w_id integer, IN no_max_w_id integer, IN no_d_id integer, IN no_c_id integer, IN no_o_ol_cnt integer, INOUT no_c_discount numeric, INOUT no_c_last character varying, INOUT no_c_credit character varying, INOUT no_d_tax numeric, INOUT no_w_tax numeric, INOUT no_d_next_o_id integer, IN tstamp timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$
                DECLARE
                no_s_quantity		NUMERIC;
                no_o_all_local		SMALLINT;
                rbk					SMALLINT;
                item_id_array 		INT[];
                supply_wid_array	INT[];
                quantity_array		SMALLINT[];
                order_line_array	SMALLINT[];
                stock_dist_array	CHAR(24)[];
                s_quantity_array	SMALLINT[];
                price_array			NUMERIC(5,2)[];
                amount_array		NUMERIC(6,2)[];
                BEGIN
                no_o_all_local := 1;
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
                IF ( round(DBMS_RANDOM(1,100)) > 1 )
                THEN
                supply_wid_array[loop_counter] := no_w_id;
                ELSE
                no_o_all_local := 0;
                supply_wid_array[loop_counter] := 1 + MOD(CAST (no_w_id + round(DBMS_RANDOM(0,no_max_w_id-1)) AS INT), no_max_w_id);
                END IF;

                --#2.4.1.5.3
                quantity_array[loop_counter] := round(DBMS_RANDOM(1,10));
                order_line_array[loop_counter] := loop_counter;
                END LOOP;

                UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = no_d_id AND d_w_id = no_w_id RETURNING d_next_o_id - 1, d_tax INTO no_d_next_o_id, no_d_tax;

                INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (no_d_next_o_id, no_d_id, no_w_id, no_c_id, current_timestamp, no_o_ol_cnt, no_o_all_local);
                INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) VALUES (no_d_next_o_id, no_d_id, no_w_id);

                SELECT array_agg ( i_price )
                INTO price_array
                FROM UNNEST(item_id_array) item_id
                LEFT JOIN item ON i_id = item_id;

                IF no_d_id = 1
                THEN
                WITH stock_update AS (
                UPDATE stock
                SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
                FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
                AS item_stock (item_id, supply_wid, quantity, price)
                WHERE stock.s_i_id = item_stock.item_id
                AND stock.s_w_id = item_stock.supply_wid
                AND stock.s_w_id = ANY(supply_wid_array)
                RETURNING stock.s_dist_01 as s_dist, stock.s_quantity, ( item_stock.quantity * item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
                )
                SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
                FROM stock_update
                INTO stock_dist_array, s_quantity_array, amount_array;
                ELSIF no_d_id = 2
                THEN
                WITH stock_update AS (
                UPDATE stock
                SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
                FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
                AS item_stock (item_id, supply_wid, quantity, price)
                WHERE stock.s_i_id = item_stock.item_id
                AND stock.s_w_id = item_stock.supply_wid
                AND stock.s_w_id = ANY(supply_wid_array)
                RETURNING stock.s_dist_02 as s_dist, stock.s_quantity, ( item_stock.quantity * item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
                )
                SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
                FROM stock_update
                INTO stock_dist_array, s_quantity_array, amount_array;
                ELSIF no_d_id = 3
                THEN
                WITH stock_update AS (
                UPDATE stock
                SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
                FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
                AS item_stock (item_id, supply_wid, quantity, price)
                WHERE stock.s_i_id = item_stock.item_id
                AND stock.s_w_id = item_stock.supply_wid
                AND stock.s_w_id = ANY(supply_wid_array)
                RETURNING stock.s_dist_03 as s_dist, stock.s_quantity, ( item_stock.quantity * item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
                )
                SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
                FROM stock_update
                INTO stock_dist_array, s_quantity_array, amount_array;
                ELSIF no_d_id = 4
                THEN
                WITH stock_update AS (
                UPDATE stock
                SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
                FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
                AS item_stock (item_id, supply_wid, quantity, price)
                WHERE stock.s_i_id = item_stock.item_id
                AND stock.s_w_id = item_stock.supply_wid
                AND stock.s_w_id = ANY(supply_wid_array)
                RETURNING stock.s_dist_04 as s_dist, stock.s_quantity, ( item_stock.quantity * item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
                )
                SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
                FROM stock_update
                INTO stock_dist_array, s_quantity_array, amount_array;
                ELSIF no_d_id = 5
                THEN
                WITH stock_update AS (
                UPDATE stock
                SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
                FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
                AS item_stock (item_id, supply_wid, quantity, price)
                WHERE stock.s_i_id = item_stock.item_id
                AND stock.s_w_id = item_stock.supply_wid
                AND stock.s_w_id = ANY(supply_wid_array)
                RETURNING stock.s_dist_05 as s_dist, stock.s_quantity, ( item_stock.quantity * item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
                )
                SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
                FROM stock_update
                INTO stock_dist_array, s_quantity_array, amount_array;
                ELSIF no_d_id = 6
                THEN
                WITH stock_update AS (
                UPDATE stock
                SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
                FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
                AS item_stock (item_id, supply_wid, quantity, price)
                WHERE stock.s_i_id = item_stock.item_id
                AND stock.s_w_id = item_stock.supply_wid
                AND stock.s_w_id = ANY(supply_wid_array)
                RETURNING stock.s_dist_06 as s_dist, stock.s_quantity, ( item_stock.quantity * item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
                )
                SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
                FROM stock_update
                INTO stock_dist_array, s_quantity_array, amount_array;
                ELSIF no_d_id = 7
                THEN
                WITH stock_update AS (
                UPDATE stock
                SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
                FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
                AS item_stock (item_id, supply_wid, quantity, price)
                WHERE stock.s_i_id = item_stock.item_id
                AND stock.s_w_id = item_stock.supply_wid
                AND stock.s_w_id = ANY(supply_wid_array)
                RETURNING stock.s_dist_07 as s_dist, stock.s_quantity, ( item_stock.quantity * item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
                )
                SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
                FROM stock_update
                INTO stock_dist_array, s_quantity_array, amount_array;
                ELSIF no_d_id = 8
                THEN
                WITH stock_update AS (
                UPDATE stock
                SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
                FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
                AS item_stock (item_id, supply_wid, quantity, price)
                WHERE stock.s_i_id = item_stock.item_id
                AND stock.s_w_id = item_stock.supply_wid
                AND stock.s_w_id = ANY(supply_wid_array)
                RETURNING stock.s_dist_08 as s_dist, stock.s_quantity, ( item_stock.quantity * item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
                )
                SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
                FROM stock_update
                INTO stock_dist_array, s_quantity_array, amount_array;
                ELSIF no_d_id = 9
                THEN
                WITH stock_update AS (
                UPDATE stock
                SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
                FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
                AS item_stock (item_id, supply_wid, quantity, price)
                WHERE stock.s_i_id = item_stock.item_id
                AND stock.s_w_id = item_stock.supply_wid
                AND stock.s_w_id = ANY(supply_wid_array)
                RETURNING stock.s_dist_09 as s_dist, stock.s_quantity, ( item_stock.quantity * item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
                )
                SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
                FROM stock_update
                INTO stock_dist_array, s_quantity_array, amount_array;
                ELSIF no_d_id = 10
                THEN
                WITH stock_update AS (
                UPDATE stock
                SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
                FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
                AS item_stock (item_id, supply_wid, quantity, price)
                WHERE stock.s_i_id = item_stock.item_id
                AND stock.s_w_id = item_stock.supply_wid
                AND stock.s_w_id = ANY(supply_wid_array)
                RETURNING stock.s_dist_10 as s_dist, stock.s_quantity, ( item_stock.quantity * item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
                )
                SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
                FROM stock_update
                INTO stock_dist_array, s_quantity_array, amount_array;
                END IF;

                INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
                SELECT no_d_next_o_id,
                no_d_id,
                no_w_id,
                data.line_number,
                data.item_id,
                data.supply_wid,
                data.quantity,
                data.amount,
                data.stock_dist
                FROM UNNEST(order_line_array,
                item_id_array,
                supply_wid_array,
                quantity_array,
                amount_array,
                stock_dist_array)
                AS data( line_number, item_id, supply_wid, quantity, amount, stock_dist);

                no_s_quantity := 0;
                FOR loop_counter IN 1 .. no_o_ol_cnt
                LOOP
                no_s_quantity := no_s_quantity + CAST( amount_array[loop_counter] AS NUMERIC);
                END LOOP;

                EXCEPTION
                WHEN serialization_failure OR deadlock_detected OR no_data_found
                THEN ROLLBACK;
                END;
                $procedure$

