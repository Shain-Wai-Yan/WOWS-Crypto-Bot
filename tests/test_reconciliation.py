import unittest
from unittest.mock import patch, MagicMock
from user_data.strategies.reconciliation import ReconciliationClient

class TestReconciliationClient(unittest.TestCase):
    def setUp(self):
        self.client = ReconciliationClient(
            base_url="http://localhost:3000",
            api_key="test_key",
            bot_id="freqtrade-alpha-1",
        )

    @patch.object(ReconciliationClient, "fetch_wows_state")
    def test_reconciliation_passes_when_positions_match(self, mock_fetch):
        mock_fetch.return_value = {
            "botId": "freqtrade-alpha-1",
            "status": "running",
            "account": {"cash": 50000.0, "totalEquity": 51500.0, "currentExposure": 1500.0},
            "limits": {"maxAggregateExposure": 5000.0},
            "positions": [
                {"symbol": "SOLUSDT", "quantity": 10.0, "avgCostNative": 150.0, "costBasisUsd": 1500.0}
            ],
            "unresolvedQuarantineCount": 0,
        }

        local_trades = [{"pair": "SOL/USDT", "amount": 10.0}]
        report = self.client.reconcile(local_trades)

        self.assertTrue(report.in_sync)
        self.assertTrue(report.can_trade)
        self.assertEqual(report.status, "running")
        self.assertIsNone(report.discrepancy_reason)

    @patch.object(ReconciliationClient, "fetch_wows_state")
    def test_reconciliation_fails_on_quantity_mismatch(self, mock_fetch):
        mock_fetch.return_value = {
            "botId": "freqtrade-alpha-1",
            "status": "running",
            "account": {"cash": 50000.0},
            "positions": [
                {"symbol": "SOLUSDT", "quantity": 10.0}
            ],
            "unresolvedQuarantineCount": 0,
        }

        # Local trade has 8.0 instead of 10.0
        local_trades = [{"pair": "SOL/USDT", "amount": 8.0}]
        report = self.client.reconcile(local_trades)

        self.assertFalse(report.in_sync)
        self.assertFalse(report.can_trade)
        self.assertEqual(report.status, "attention_required")
        self.assertIn("Quantity mismatch on SOLUSDT", report.discrepancy_reason)

    @patch.object(ReconciliationClient, "fetch_wows_state")
    def test_reconciliation_blocks_when_quarantines_exist(self, mock_fetch):
        mock_fetch.return_value = {
            "botId": "freqtrade-alpha-1",
            "status": "attention_required",
            "positions": [],
            "unresolvedQuarantineCount": 2,
        }

        local_trades = []
        report = self.client.reconcile(local_trades)

        self.assertFalse(report.can_trade)
        self.assertEqual(report.status, "attention_required")
        self.assertIn("Unresolved quarantined events present", report.discrepancy_reason)

if __name__ == "__main__":
    unittest.main()
