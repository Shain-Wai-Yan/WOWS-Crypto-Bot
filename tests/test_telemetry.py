import unittest
import json
import os
import threading
from unittest.mock import patch, MagicMock
from user_data.strategies.telemetry import TelemetryClient

class TestTelemetryClient(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"WOWS_TELEMETRY_BASE_URL": "", "HEARTBEAT_INTERVAL_SEC": "60"})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.clock = patch("user_data.strategies.telemetry.time.monotonic", return_value=1000.0)
        self.monotonic = self.clock.start()
        self.addCleanup(self.clock.stop)
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

    @patch("requests.post")
    def test_bot_loop_cannot_bypass_the_routine_interval(self, post):
        post.return_value.status_code = 200
        self.assertTrue(self.telemetry.send_heartbeat())
        for second in range(1, 60):
            self.monotonic.return_value = 1000 + second
            self.assertFalse(self.telemetry.send_heartbeat())
        self.assertEqual(post.call_count, 1)
        self.monotonic.return_value = 1060
        self.assertTrue(self.telemetry.send_heartbeat())
        self.assertEqual(post.call_count, 2)

    @patch("requests.post")
    def test_concurrent_callers_share_one_request(self, post):
        started, release = threading.Event(), threading.Event()
        def respond(*args, **kwargs):
            started.set()
            release.wait(2)
            return MagicMock(status_code=200)
        post.side_effect = respond
        worker = threading.Thread(target=self.telemetry.send_heartbeat)
        worker.start()
        try:
            self.assertTrue(started.wait(1))
            for _ in range(20):
                self.assertFalse(self.telemetry.send_heartbeat())
        finally:
            release.set()
            worker.join(2)
        self.assertEqual(post.call_count, 1)

    @patch("requests.post")
    def test_cloud_run_override_does_not_change_the_application_origin(self, post):
        post.return_value.status_code = 200
        with patch.dict(os.environ, {"WOWS_TELEMETRY_BASE_URL": "https://worker.example.test/"}):
            client = TelemetryClient("https://app.example.test", "key", "bot")
        self.assertTrue(client.send_heartbeat())
        self.assertEqual(client.base_url, "https://app.example.test")
        self.assertEqual(post.call_args.args[0], "https://worker.example.test/api/webhooks/bot/telemetry")
        self.assertFalse(post.call_args.kwargs["allow_redirects"])

    @patch("requests.post")
    def test_failed_batches_are_retained_without_falling_back_to_vercel(self, post):
        client = TelemetryClient("https://app.example.test", "key", "bot", telemetry_base_url="https://worker.example.test")
        client.log_event("signal_generated", "info", "Keep this event")
        post.return_value.status_code = 503
        self.assertFalse(client.send_heartbeat())
        failed = post.call_args.kwargs["json"]["events"]
        self.assertFalse(client.send_heartbeat())
        self.assertEqual(post.call_count, 1)
        self.monotonic.return_value = 1060
        post.return_value.status_code = 200
        self.assertTrue(client.send_heartbeat())
        self.assertEqual(post.call_args.kwargs["json"]["events"], failed)
        self.assertTrue(all(call.args[0].startswith("https://worker.example.test/") for call in post.call_args_list))

    @patch("requests.post")
    def test_critical_events_bypass_routine_interval_but_bursts_are_coalesced(self, post):
        post.return_value.status_code = 200
        self.telemetry.send_heartbeat()
        self.monotonic.return_value = 1001
        self.telemetry.log_event("risk_exit", "error", "Exit now")
        self.assertTrue(self.telemetry.send_heartbeat())
        self.monotonic.return_value = 1002
        self.telemetry.log_event("guard_blocked", "warning", "Guard update")
        self.assertFalse(self.telemetry.send_heartbeat())
        self.monotonic.return_value = 1006
        self.assertTrue(self.telemetry.send_heartbeat())
        self.assertEqual(post.call_count, 3)

    @patch("requests.post")
    def test_critical_events_wake_the_background_thread(self, post):
        first, urgent = threading.Event(), threading.Event()
        def respond(*args, **kwargs):
            (urgent if kwargs["json"]["events"] else first).set()
            return MagicMock(status_code=200)
        post.side_effect = respond
        self.telemetry.start()
        try:
            self.assertTrue(first.wait(1))
            self.monotonic.return_value = 1001
            self.telemetry.log_event("risk_exit", "error", "Critical exit")
            self.assertTrue(urgent.wait(1))
        finally:
            self.telemetry.stop()
        self.assertFalse(self.telemetry._thread.is_alive())

    @patch("requests.post")
    def test_retry_backoff_limits_critical_failures(self, post):
        post.return_value.status_code = 503
        self.telemetry.log_event("risk_exit", "error", "Pending exit")
        for timestamp in [1000, 1005, 1015, 1035, 1075, 1135]:
            self.monotonic.return_value = timestamp
            self.assertFalse(self.telemetry.send_heartbeat())
            for _ in range(20):
                self.telemetry.send_heartbeat()
        self.assertEqual(post.call_count, 6)
        self.assertEqual(self.telemetry._retry_not_before, 1195)

    @patch("requests.post")
    def test_event_batches_are_bounded_and_urgent_events_have_priority(self, post):
        post.return_value.status_code = 200
        for i in range(60):
            self.telemetry.log_event("signal_generated", "info", str(i))
        self.telemetry.log_event("risk_exit", "error", "Critical")
        self.telemetry.send_heartbeat()
        events = post.call_args.kwargs["json"]["events"]
        self.assertEqual(len(events), 50)
        self.assertEqual(events[0]["eventType"], "risk_exit")
        self.monotonic.return_value = 1060
        self.telemetry.send_heartbeat()
        self.assertEqual(len(post.call_args.kwargs["json"]["events"]), 11)

    @patch("requests.post")
    def test_unicode_payloads_fit_ingress_limit_and_overflow_is_retained(self, post):
        post.return_value.status_code = 200
        for i in range(50):
            self.telemetry.log_event("risk_exit", "error", "😀" * 500, {"index": i})
        self.telemetry.send_heartbeat()
        payload = post.call_args.kwargs["json"]
        self.assertLessEqual(len(json.dumps(payload).encode("utf-8")), self.telemetry.MAX_PAYLOAD_BYTES)
        sent = len(payload["events"])
        self.assertLess(sent, 50)
        self.monotonic.return_value = 1005
        self.telemetry.send_heartbeat()
        self.assertEqual(sent + len(post.call_args.kwargs["json"]["events"]), 50)

    def test_interval_configuration_and_invalid_destinations(self):
        with patch.dict(os.environ, {"HEARTBEAT_INTERVAL_SEC": "90"}):
            self.assertEqual(TelemetryClient().interval, 90)
        for value, expected in [(10, 60), (1000, 120), (float("nan"), 60), ("invalid", 60)]:
            self.assertEqual(TelemetryClient(interval_sec=value).interval, expected)
        for url in ["ftp://example.test", "https://user:key@example.test", "https://example.test/api/telemetry", "https://example.test?token=value"]:
            with self.assertRaises(ValueError):
                TelemetryClient(telemetry_base_url=url)

    @patch("requests.post")
    def test_urgent_event_preempts_a_failed_routine_batch(self, post):
        self.telemetry.log_event("signal_generated", "info", "Routine backlog")
        post.return_value.status_code = 503
        self.assertFalse(self.telemetry.send_heartbeat())
        self.telemetry.log_event("risk_exit", "error", "Urgent exit")
        self.monotonic.return_value = 1005
        post.return_value.status_code = 200
        self.assertTrue(self.telemetry.send_heartbeat())
        events = post.call_args.kwargs["json"]["events"]
        self.assertEqual(events[0]["eventType"], "risk_exit")
        self.assertEqual(events[1]["eventType"], "signal_generated")

if __name__ == "__main__":
    unittest.main()
