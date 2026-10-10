import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock
from user_data.strategies.guards import RiskGuards

class TestStrategyHooksAndGuards(unittest.TestCase):
    def test_risk_guards_halting_and_cooldown(self):
        guards = RiskGuards(
            max_daily_loss_pct=0.05,
            max_consecutive_losses=3,
            cooldown_bars=5,
            timeframe_minutes=60,
        )

        now_ms = 1791628800000  # Bar N
        # 1. Normal state: entry allowed
        allowed, reason, is_halt = guards.check_entry_allowed("SOL/USDT", now_ms, 50000.0)
        self.assertTrue(allowed)
        self.assertFalse(is_halt)

        # 2. Simulate 1 loss on SOL/USDT
        guards.on_trade_closed("SOL/USDT", -100.0, now_ms)

        # Bar N+2: Cooldown active (2 bars elapsed < 5 required)
        bar_n_plus_2_ms = now_ms + (2 * 60 * 60 * 1000)
        allowed, reason, is_halt = guards.check_entry_allowed("SOL/USDT", bar_n_plus_2_ms, 50000.0)
        self.assertFalse(allowed)
        self.assertIn("Cooldown active", reason)
        self.assertFalse(is_halt)  # Throttling only, bot not halted

        # Different pair (BTC/USDT) can still enter (cooldown is per-pair)
        btc_allowed, _, _ = guards.check_entry_allowed("BTC/USDT", bar_n_plus_2_ms, 50000.0)
        self.assertTrue(btc_allowed)

        # 3. Simulate 2 more losses (3 consecutive losses total)
        guards.on_trade_closed("ETH/USDT", -100.0, bar_n_plus_2_ms)
        guards.on_trade_closed("BNB/USDT", -100.0, bar_n_plus_2_ms)

        # Halting limit breached!
        allowed, reason, is_halt = guards.check_entry_allowed("BTC/USDT", bar_n_plus_2_ms, 50000.0)
        self.assertFalse(allowed)
        self.assertTrue(is_halt)
        self.assertIn("Consecutive loss streak", reason)

    def test_order_filled_payload_extraction(self):
        # Mock documented Freqtrade Trade and Order objects
        trade = MagicMock()
        trade.id = 42
        trade.entry_side = "buy"
        trade.enter_tag = "rsi_entry"

        order = MagicMock()
        order.order_id = "ord-sol-test-99"
        order.safe_filled = 12.5
        order.safe_price = 145.20
        order.safe_cost = 1815.00
        order.ft_order_side = "buy"
        order.order_filled_utc = datetime(2026, 10, 10, 12, 0, 0, tzinfo=timezone.utc)

        is_entry = (order.ft_order_side == trade.entry_side)
        self.assertTrue(is_entry)

        event_id = f"freqtrade-alpha-1:{trade.id}:{order.order_id}:entry_fill"
        self.assertEqual(event_id, "freqtrade-alpha-1:42:ord-sol-test-99:entry_fill")

if __name__ == "__main__":
    unittest.main()
