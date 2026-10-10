import os
import unittest
import tempfile
import sqlite3
from user_data.strategies.outbox import OutboxManager, ConflictingEventError

class TestOutboxManager(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_outbox.db")
        self.outbox = OutboxManager(self.db_path)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_schema_and_wal_mode_initialized(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode;")
        journal_mode = cur.fetchone()[0]
        self.assertEqual(journal_mode.lower(), "wal")

        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='outbox';")
        self.assertIsNotNone(cur.fetchone())
        conn.close()

    def test_record_fill_and_idempotent_duplicate(self):
        event_id = "bot:1:ord-1:entry_fill"
        payload = {"pair": "SOL/USDT", "amount": 10.0, "price": 150.0}

        # First insert succeeds
        inserted = self.outbox.record_fill(event_id, payload)
        self.assertTrue(inserted)

        # Exact duplicate returns False (no-op)
        inserted_again = self.outbox.record_fill(event_id, payload)
        self.assertFalse(inserted_again)

        events = self.outbox.get_pending_events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_id"], event_id)

    def test_conflicting_event_raises_error(self):
        event_id = "bot:1:ord-1:entry_fill"
        payload1 = {"pair": "SOL/USDT", "amount": 10.0, "price": 150.0}
        payload2 = {"pair": "SOL/USDT", "amount": 20.0, "price": 150.0}  # Conflicting quantity

        self.outbox.record_fill(event_id, payload1)

        with self.assertRaises(ConflictingEventError):
            self.outbox.record_fill(event_id, payload2)

    def test_status_transitions(self):
        event_id = "bot:1:ord-2:exit_fill"
        payload = {"pair": "SOL/USDT", "amount": 5.0}
        self.outbox.record_fill(event_id, payload)

        # Retry backoff
        self.outbox.mark_retry(event_id, "HTTP 500 error", 10.0)
        status = self.outbox.get_event_status(event_id)
        self.assertEqual(status["attempt_count"], 1)
        self.assertEqual(status["last_error"], "HTTP 500 error")

        # Quarantine
        self.outbox.mark_quarantined(event_id, "HTTP 400 rejection")
        status = self.outbox.get_event_status(event_id)
        self.assertEqual(status["status"], "QUARANTINED")

        # Acknowledge
        self.outbox.mark_acknowledged(event_id)
        status = self.outbox.get_event_status(event_id)
        self.assertEqual(status["status"], "ACKNOWLEDGED")
        self.assertIsNotNone(status["sent_at"])

if __name__ == "__main__":
    unittest.main()
