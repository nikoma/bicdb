"""Negative controls for the independent oracle and uncertain-commit protocol."""
import argparse
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from qualify_online_backups import Campaign, Journal


class BackupQualificationOracleTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.journal = Journal(Path(self.directory.name) / "oracle.sqlite", 2)
        self.campaign = Campaign.__new__(Campaign)
        self.campaign.journal = self.journal
        self.campaign.args = argparse.Namespace(writers=2)
        self.campaign.started = time.monotonic()
        self.campaign.report = {"oracle_checks": []}
        self.campaign.emit = lambda *args, **kwargs: None

    def tearDown(self):
        self.journal.db.close()
        self.directory.cleanup()

    def database(self, heads):
        db = self.journal.expected(heads)
        db.execute("CREATE TABLE rollback_probe(id INT)")
        self.addCleanup(db.close)
        return db

    def test_losing_a_known_acknowledgement_is_always_a_failure(self):
        self.journal.plan(0, 1, False)
        self.journal.acknowledge(0, 1)
        rolled_back_database = self.database({})
        with self.assertRaisesRegex(AssertionError, "acknowledged-before-capture"):
            self.campaign.verify_database(rolled_back_database, "lost-ack")
        # Reconciliation must never downgrade a known acknowledgement.
        self.assertEqual(self.journal.reconcile(rolled_back_database), [])
        self.assertEqual(self.journal.heads(), {0: 1})

    def test_reconcile_commit_before_oracle_ack_and_never_committed_transaction(self):
        _, committed_token = self.journal.plan(0, 1, False)
        self.journal.outcome(0, 1, "commit_sent")
        self.journal.plan(1, 1, False)
        recovered = sqlite3.connect(":memory:")
        self.addCleanup(recovered.close)
        recovered.execute("CREATE TABLE ledger(id INT,token TEXT)")
        recovered.execute("INSERT INTO ledger VALUES(1,?)", (committed_token,))
        results = self.journal.reconcile(recovered)
        self.assertEqual([r["outcome"] for r in results], ["resolved_committed", "resolved_aborted"])
        self.assertEqual(self.journal.count(), 0)  # Neither was journaled as acknowledged.
        self.assertEqual(self.journal.heads(), {0: 1})
        result = self.campaign.verify_database(self.database({0: 1}), "uncertain-resolved")
        self.assertEqual(result["ledger_rows"], 1)

    def test_uncertain_transaction_id_cannot_resolve_to_different_data(self):
        self.journal.plan(0, 1, False)
        self.journal.outcome(0, 1, "commit_sent")
        recovered = sqlite3.connect(":memory:")
        self.addCleanup(recovered.close)
        recovered.executescript("CREATE TABLE ledger(id INT,token TEXT); INSERT INTO ledger VALUES(1,'wrong');")
        with self.assertRaisesRegex(AssertionError, "different payload"):
            self.journal.reconcile(recovered)

    def test_synced_commit_intent_can_reconcile_as_aborted(self):
        self.journal.plan(0, 1, False)
        self.journal.outcome(0, 1, "commit_sent")
        recovered = sqlite3.connect(":memory:")
        self.addCleanup(recovered.close)
        recovered.execute("CREATE TABLE ledger(id INT,token TEXT)")
        results = self.journal.reconcile(recovered)
        self.assertEqual(results[0]["outcome"], "resolved_aborted")
        self.assertEqual(self.journal.heads(), {})
        self.campaign.verify_database(self.database({}), "intent-without-commit")

    def test_uncommitted_transaction_cannot_be_reclassified_as_committed(self):
        _, token = self.journal.plan(0, 1, False)
        recovered = sqlite3.connect(":memory:")
        self.addCleanup(recovered.close)
        recovered.execute("CREATE TABLE ledger(id INT,token TEXT)")
        recovered.execute("INSERT INTO ledger VALUES(1,?)", (token,))
        with self.assertRaisesRegex(AssertionError, "without a COMMIT"):
            self.journal.reconcile(recovered)

    def test_oracle_detects_cross_table_partial_commit(self):
        self.journal.plan(0, 1, False)
        self.journal.acknowledge(0, 1)
        corrupted = self.database({0: 1})
        corrupted.execute("UPDATE counters SET total=total+1")
        with self.assertRaisesRegex(AssertionError, "SQLite mismatch"):
            self.campaign.verify_database(corrupted, "partial-commit")

    def test_backup_must_exclude_transactions_planned_after_capture(self):
        self.journal.plan(0, 1, False)
        self.journal.acknowledge(0, 1)
        with self.assertRaisesRegex(AssertionError, "post-capture transaction"):
            self.campaign.verify_database(self.database({0: 1}), "future-write", lower={}, upper={})

    def test_older_backup_is_checked_against_its_capture_bounds(self):
        self.journal.plan(0, 1, False)
        self.journal.acknowledge(0, 1)
        result = self.campaign.verify_database(self.database({}), "older-cut", lower={}, upper={})
        self.assertEqual(result["ledger_rows"], 0)

    def test_cut_cannot_include_transaction_without_its_acknowledged_predecessor(self):
        self.journal.plan(0, 1, False)
        self.journal.acknowledge(0, 1)
        self.journal.plan(1, 1, False)  # Starts after observing writer zero's ack.
        self.journal.acknowledge(1, 1)
        with self.assertRaisesRegex(AssertionError, "observed commit ordering"):
            self.journal.expected({0: 0, 1: 1})


if __name__ == "__main__":
    unittest.main()
