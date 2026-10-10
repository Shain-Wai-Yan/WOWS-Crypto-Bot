import os
import unittest
import tempfile
from unittest.mock import patch, MagicMock
from user_data.strategies.outbox import OutboxManager
from user_data.strategies.dispatcher import DispatcherWorker

class TestDispatcherWorker(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_outbox.db")
        self.outbox = OutboxManager(self.db_path)
        self.dispatcher = DispatcherWorker(
            outbox=self.outbox,
            base_url="http://localhost:3000",
            api_key="test_key",
            backoff_base_sec=1.0,
            jitter_sec=0.1,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    @patch("requests.post")
    def test_http_200_marks_acknowledged(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"success": True, "replayed": False}
        mock_post.return_value = mock_resp

        event_id = "bot:101:ord-1:entry_fill"
        self.outbox.record_fill(event_id, {"symbol": "SOLUSDT", "qty": 10})

        event = self.outbox.get_pending_events()[0]
        success = self.dispatcher.dispatch_event(event)

        self.assertTrue(success)
        status = self.outbox.get_event_status(event_id)
        self.assertEqual(status["status"], "ACKNOWLEDGED")

    @patch("requests.post")
    def test_http_429_handles_retry_after(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 429
        mock_resp.headers = {"Retry-After": "5"}
        mock_resp.text = "Too Many Requests"
        mock_post.return_value = mock_resp

        event_id = "bot:101:ord-2:entry_fill"
        self.outbox.record_fill(event_id, {"symbol": "SOLUSDT"})

        event = self.outbox.get_pending_events()[0]
        success = self.dispatcher.dispatch_event(event)

        self.assertFalse(success)
        status = self.outbox.get_event_status(event_id)
        self.assertEqual(status["status"], "PENDING")
        self.assertEqual(status["attempt_count"], 1)

    @patch("requests.post")
    def test_http_4xx_marks_quarantined(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 422
        mock_resp.text = "Exposure limit breached"
        mock_post.return_value = mock_resp

        event_id = "bot:101:ord-3:entry_fill"
        self.outbox.record_fill(event_id, {"symbol": "SOLUSDT"})

        event = self.outbox.get_pending_events()[0]
        success = self.dispatcher.dispatch_event(event)

        self.assertFalse(success)
        status = self.outbox.get_event_status(event_id)
        self.assertEqual(status["status"], "QUARANTINED")

if __name__ == "__main__":
    unittest.main()
