import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from verify_postgres_outcomes import verify


class PostgresOutcomeTests(unittest.TestCase):
    def test_expected_rollback_is_excluded_and_unexplained_loss_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name, text in {
                "hammerdb.log": "BICDB_OUTCOMES 2 101 100 1 0 90\n",
                "district-before.csv": "district_sum\n3001\n",
                "district-after.csv": "district_sum\n3101\n",
                "payment-before.csv": "payment_history_rows\n3000\n",
                "payment-after.csv": "payment_history_rows\n3090\n",
            }.items():
                (root / name).write_text(text)
            result = verify(root, 1, 2)
            self.assertTrue(result["valid"])
            self.assertEqual(result["district_sum_nopm"], 50)
            self.assertFalse(verify(root, 2, 2)["valid"])
            (root / "payment-after.csv").write_text("payment_history_rows\n3089\n")
            self.assertFalse(verify(root, 1, 2)["valid"])
            (root / "payment-after.csv").write_text("payment_history_rows\n3090\n")
            (root / "district-after.csv").write_text("district_sum\n3100\n")
            self.assertFalse(verify(root, 1, 2)["valid"])
