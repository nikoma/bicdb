import random
import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from audit_corrected_v2 import final_stock_quantity, money


class CorrectedAuditTests(unittest.TestCase):
    def test_aggregate_quantity_matches_sequential_tpcc_updates(self):
        rng = random.Random(823)
        for initial in range(10, 101):
            quantities = [rng.randrange(1, 11) for _ in range(100)]
            sequential = initial
            for quantity in quantities:
                if sequential >= quantity + 10:
                    sequential -= quantity
                else:
                    sequential += 91 - quantity
            self.assertEqual(final_stock_quantity(initial, sum(quantities)), sequential)

    def test_decimal_tie_uses_declared_numeric_rounding(self):
        self.assertEqual(money("44.90", 2, "0.1300", "0.1700", "0.2500"), Decimal("87.56"))
