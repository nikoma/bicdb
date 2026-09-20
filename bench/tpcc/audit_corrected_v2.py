#!/usr/bin/env python3
"""Read-only full corrected-v2 amount, stock and Delivery-counter audit.

Requires the exact canonical 16-warehouse seed captures. Financial balances,
order structure and recovery remain separate checks. Output is exclusive JSONL;
any mismatch or incomplete coverage exits nonzero. Use only a quiescent endpoint.
"""
import argparse
import csv
import hashlib
import json
import os
import time
from array import array
from collections import Counter
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

WAREHOUSES = 16
ITEMS = 100000
STOCK_SEED_SHA256 = "1bf0e9d0b33823cc55562de345870fcd398c2872b23c8d989a5d16d736f4f272"
DELIVERY_SEED_SHA256 = "4b4fb9f3e646e919377faeb0d302f5238c1bc8fedb63531e7f8e6296d74107e1"
# Exact canonical imported seed, independently counted on PostgreSQL; the
# retained stock/Delivery hashes above bind this audit to that seed instance.
SEED_ORDER_LINES = 4797556


def money(price, quantity, warehouse_tax, district_tax, discount):
    return (Decimal(str(price)) * quantity * (1 + Decimal(str(warehouse_tax)) + Decimal(str(district_tax)))
            * (1 - Decimal(str(discount)))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def final_stock_quantity(initial, total_quantity):
    return (initial - total_quantity - 10) % 91 + 10


def audit(args):
    import psycopg2
    started = time.monotonic()
    stock_path = Path(args.stock_seed)
    assert hashlib.sha256(stock_path.read_bytes()).hexdigest() == STOCK_SEED_SHA256, "Wrong stock seed capture"
    stock_seed = array("i", [-1]) * (WAREHOUSES * ITEMS)
    with stock_path.open() as file:
        for w, item, quantity in csv.reader(file):
            w, item, quantity = int(w), int(item), int(quantity)
            assert 1 <= w <= WAREHOUSES and 1 <= item <= ITEMS and 10 <= quantity <= 100
            index = (w - 1) * ITEMS + item - 1
            assert stock_seed[index] == -1, "Duplicate seed stock key"
            stock_seed[index] = quantity
    assert -1 not in stock_seed, "Incomplete seed stock capture"
    delivery_seed = {}
    assert hashlib.sha256(Path(args.delivery_seed).read_bytes()).hexdigest() == DELIVERY_SEED_SHA256, "Wrong Delivery seed capture"
    with open(args.delivery_seed) as file:
        for w, d, customer, count in csv.reader(file):
            key = int(w), int(d), int(customer)
            assert key not in delivery_seed
            delivery_seed[key] = int(count)
    assert sum(delivery_seed.values()) == 336000, "Wrong canonical seed Delivery count"
    quantities = array("q", [0]) * len(stock_seed)
    counts = array("q", [0]) * len(stock_seed)
    remote = array("q", [0]) * len(stock_seed)
    totals = Counter()
    examples = []
    db = psycopg2.connect(host="127.0.0.1", port=args.port, user=args.user,
                          password=os.environ.get("PGPASSWORD", "bicdb"), dbname=args.database)
    db.autocommit = True
    q = db.cursor()

    def rows(sql, params=()):
        q.execute(sql, params)
        columns = [column or [] for column in q.fetchone()]
        assert len({len(column) for column in columns}) == 1, "Misaligned audit arrays"
        return zip(*columns)

    def mismatch(kind, **example):
        totals[kind + "_mismatch"] += 1
        if len(examples) < 20:
            examples.append(dict(kind=kind, **example))

    prices = dict(rows("SELECT array_agg(i_id),array_agg(i_price) FROM item"))
    assert sorted(prices) == list(range(1, ITEMS + 1))
    taxes = dict(rows("SELECT array_agg(w_id),array_agg(w_tax) FROM warehouse"))
    assert sorted(taxes) == list(range(1, WAREHOUSES + 1))
    with open(args.output, "x", buffering=1) as output:
        def emit(value):
            text = json.dumps(value, default=str)
            output.write(text + "\n")
            print(text, flush=True)

        emit(dict(stage="inputs", stock_seed_sha256=STOCK_SEED_SHA256,
                  delivery_seed_sha256=hashlib.sha256(Path(args.delivery_seed).read_bytes()).hexdigest()))
        for w in range(1, WAREHOUSES + 1):
            for d in range(1, 11):
                customers = {cid: (discount, delivered) for cid, discount, delivered in rows(
                    "SELECT array_agg(c_id),array_agg(c_discount),array_agg(c_delivery_cnt) FROM customer WHERE c_w_id=%s AND c_d_id=%s", (w, d))}
                assert len(customers) == 3000
                q.execute("SELECT d_tax FROM district WHERE d_w_id=%s AND d_id=%s", (w, d))
                district_tax = q.fetchone()[0]
                orders = {}
                delivered = Counter()
                for oid, cid, carrier in rows("SELECT array_agg(o_id),array_agg(o_c_id),array_agg(o_carrier_id) FROM orders WHERE o_w_id=%s AND o_d_id=%s", (w, d)):
                    assert oid not in orders and cid in customers
                    orders[oid] = cid
                    if carrier is not None:
                        delivered[cid] += 1
                for cid, (_discount, count) in customers.items():
                    expected = delivered[cid] - delivery_seed.get((w, d, cid), 0)
                    if count != expected:
                        mismatch("delivery_counter", warehouse=w, district=d, customer=cid, actual=count, expected=expected)
                totals["customers"] += len(customers)
                for oid, number, item, supplier, quantity, amount in rows(
                    "SELECT array_agg(ol_o_id),array_agg(ol_number),array_agg(ol_i_id),array_agg(ol_supply_w_id),array_agg(ol_quantity),array_agg(ol_amount) FROM order_line WHERE ol_w_id=%s AND ol_d_id=%s AND ol_o_id>3000", (w, d)):
                    totals["postseed_lines"] += 1
                    if oid not in orders or item not in prices or amount is None or supplier is None or quantity is None or not 1 <= supplier <= WAREHOUSES or not 1 <= quantity <= 10:
                        mismatch("line_reference", warehouse=w, district=d, order=oid, line=number)
                        continue
                    expected = money(prices[item], quantity, taxes[w], district_tax, customers[orders[oid]][0])
                    if Decimal(str(amount)) != expected:
                        mismatch("line_amount", warehouse=w, district=d, order=oid, line=number, actual=amount, expected=expected)
                    index = (supplier - 1) * ITEMS + item - 1
                    quantities[index] += quantity
                    counts[index] += 1
                    remote[index] += int(supplier != w)
                emit(dict(stage="district", warehouse=w, district=d, totals=dict(totals), elapsed_seconds=time.monotonic()-started))
        for w in range(1, WAREHOUSES + 1):
            seen = set()
            for item, quantity, ytd, order_count, remote_count in rows(
                "SELECT array_agg(s_i_id),array_agg(s_quantity),array_agg(s_ytd),array_agg(s_order_cnt),array_agg(s_remote_cnt) FROM stock WHERE s_w_id=%s", (w,)):
                assert 1 <= item <= ITEMS and item not in seen
                seen.add(item)
                index = (w - 1) * ITEMS + item - 1
                actual = (quantity, ytd, order_count, remote_count)
                expected = (final_stock_quantity(stock_seed[index], quantities[index]), quantities[index], counts[index], remote[index])
                if actual != expected:
                    mismatch("stock", warehouse=w, item=item, actual=actual, expected=expected)
            assert len(seen) == ITEMS
            totals["stock_rows"] += len(seen)
            emit(dict(stage="stock", warehouse=w, totals=dict(totals), elapsed_seconds=time.monotonic()-started))
        # A full-table filtered COUNT currently materializes the recovered
        # collection and can exhaust the host. The canonical seed is immutable;
        # total rows minus its independently known line count verifies the same
        # post-seed coverage without that unbounded execution path.
        q.execute("SELECT count(*) FROM order_line")
        assert q.fetchone()[0] - SEED_ORDER_LINES == totals["postseed_lines"] > 0, "Incomplete line coverage"
        assert totals["customers"] == 480000 and totals["stock_rows"] == 1600000
        for table, expected_count in (("customer", 480000), ("stock", 1600000), ("district", 160)):
            q.execute("SELECT count(*) FROM " + table)
            assert q.fetchone()[0] == expected_count, "Incomplete " + table + " coverage"
        passed = not any(value for key, value in totals.items() if key.endswith("_mismatch"))
        emit(dict(final=True, passed=passed, totals=dict(totals), examples=examples, elapsed_seconds=time.monotonic()-started))
    db.close()
    return passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--database", default="bicdb")
    parser.add_argument("--user", default="bicdb")
    parser.add_argument("--stock-seed", required=True)
    parser.add_argument("--delivery-seed", required=True)
    parser.add_argument("--output", required=True)
    raise SystemExit(0 if audit(parser.parse_args()) else 1)
