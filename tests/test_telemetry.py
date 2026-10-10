import unittest
from unittest.mock import patch, MagicMock
from user_data.strategies.telemetry import TelemetryClient

class TestTelemetryClient(unittest.TestCase):
    def setUp(self):
        self.telemetry = TelemetryClient(
            base_url="http://localhost:3000",
            api_key="test_key",
            bot_id="freqtrade-alpha-1",
        )

    @patch("requests.post")
    def test_send_heartbeat_payload_structure(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp

        self.telemetry.update_state(
            status="running",
            active_pairs=["SOL/USDT", "BTC/USDT"],
            open_trades_count=2,
            current_exposure=3000.0,
            guard_state={"consecutiveLosses": 0},
        )
        self.telemetry.log_event("signal_generated", "info", "RSI oversold crossover")

        success = self.telemetry.send_heartbeat()
        self.assertTrue(success)

        self.assertTrue(mock_post.called)
        call_kwargs = mock_post.call_args[1]
        payload = call_kwargs["json"]

        self.assertEqual(payload["botId"], "freqtrade-alpha-1")
        self.assertEqual(payload["status"], "running")
        self.assertEqual(len(payload["events"]), 1)
        self.assertEqual(payload["events"][0]["eventType"], "signal_generated")

if __name__ == "__main__":
    unittest.main()
