import os
import unittest
import tempfile
from unittest.mock import patch, MagicMock
from user_data.strategies.outbox import OutboxManager
from user_data.strategies.dispatcher import DispatcherWorker

class TestRestartRecovery(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "recovery_test.db")

    def tearDown(self):
        self.temp_dir.cleanup()

    @patch("requests.post")
    def test_pending_fills_survive_process_crash_and_recover_on_restart(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"success": True, "replayed": False}
        mock_post.return_value = mock_resp

        # ── Process 1: Bot runs, writes 3 fills to outbox, then crashes ungracefully ──
        outbox_p1 = OutboxManager(self.db_path)
        outbox_p1.record_fill("bot:1:fill-1:entry", {"pair": "SOL/USDT", "amount": 10})
        outbox_p1.record_fill("bot:1:fill-2:entry", {"pair": "BTC/USDT", "amount": 0.1})
        outbox_p1.record_fill("bot:1:fill-3:exit", {"pair": "SOL/USDT", "amount": 10})

        # Process 1 terminates without dispatching. Verify SQLite has 3 pending.
        self.assertEqual(len(outbox_p1.get_pending_events()), 3)
        del outbox_p1  # Process 1 exits

        # ── Process 2: Bot boots up after restart ──
        outbox_p2 = OutboxManager(self.db_path)
        dispatcher_p2 = DispatcherWorker(
            outbox=outbox_p2,
            base_url="http://localhost:3000",
            api_key="test_secret",
        )

        # On startup, dispatcher recovers and drains pending batch
        processed = dispatcher_p2.process_pending_batch(limit=10)
        self.assertEqual(processed, 3)

        # Verify all 3 fills are now ACKNOWLEDGED and zero pending remain
        self.assertEqual(len(outbox_p2.get_pending_events()), 0)
        for fill_id in ["bot:1:fill-1:entry", "bot:1:fill-2:entry", "bot:1:fill-3:exit"]:
            status = outbox_p2.get_event_status(fill_id)
            self.assertEqual(status["status"], "ACKNOWLEDGED")
            self.assertIsNotNone(status["sent_at"])

if __name__ == "__main__":
    unittest.main()
