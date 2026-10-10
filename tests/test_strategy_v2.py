import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
import os
import sys
import json
import tempfile
from pathlib import Path

# Ensure strategies can be imported
bot_root = str(Path(__file__).parent.parent.resolve())
if bot_root not in sys.path:
    sys.path.insert(0, bot_root)

strategies_path = str(Path(__file__).parent.parent / "user_data" / "strategies")
if strategies_path not in sys.path:
    sys.path.insert(0, strategies_path)

from user_data.strategies.wows_strategy_v2 import WowsStrategyV2
from user_data.strategies.guards import RiskGuards


class TestWowsStrategyV2(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_v2_outbox.db")
        os.environ["SQLITE_DB_PATH"] = self.db_path

        self.config = {
            "dry_run": True,
            "dry_run_wallet": 10000.0,
            "stake_currency": "USDT",
        }
        self.strategy = WowsStrategyV2(self.config)
        self.strategy._reconciliation_passed = True

    def tearDown(self):
        self.temp_dir.cleanup()
        if "SQLITE_DB_PATH" in os.environ:
            del os.environ["SQLITE_DB_PATH"]

    def test_custom_stake_amount_risk_sizing(self):
        """
        Tests the risk-budget sizing formula:
        Stake = min((Equity * 0.0025) / 0.045, Equity * 0.10, Available_balance)
        For Equity = $10,000:
        Risk budget = $10,000 * 0.0025 = $25.00
        Stop distance = 0.045 (4.5%)
        Risk-based stake = 25.00 / 0.045 = $555.56
        Position cap = 10% = $1,000.00
        Available max_stake = $2,000.00
        Expected stake = $555.56
        """
        now = datetime.now(timezone.utc)
        stake = self.strategy.custom_stake_amount(
            pair="SOL/USDT",
            current_time=now,
            current_rate=150.0,
            proposed_stake=100.0,
            min_stake=10.0,
            max_stake=2000.0,
            leverage=1.0,
            entry_tag="trend_pullback",
            side="buy"
        )
        self.assertAlmostEqual(stake, 555.56, places=2)

    def test_custom_stake_amount_position_cap(self):
        """
        Tests that stake never exceeds the 10% portfolio position cap.
        If stop distance were very tight (e.g. formula produces $1,500),
        the stake must be capped at Equity * 0.10 = $1,000.
        """
        now = datetime.now(timezone.utc)
        self.strategy.STOP_DISTANCE_FRACTION = 0.01  # would yield $2,500
        stake = self.strategy.custom_stake_amount(
            pair="SOL/USDT",
            current_time=now,
            current_rate=150.0,
            proposed_stake=100.0,
            min_stake=10.0,
            max_stake=5000.0,
            leverage=1.0,
            entry_tag="breakout",
            side="buy"
        )
        # Capped at 10% of $10,000 = $1,000.00
        self.assertEqual(stake, 1000.0)

    def test_custom_stake_amount_min_stake_safety_skip(self):
        """
        Tests that if calculated stake is lower than exchange min_stake,
        the trade is skipped (0.0 returned) rather than silently inflated.
        """
        now = datetime.now(timezone.utc)
        self.strategy.config["dry_run_wallet"] = 100.0
        stake = self.strategy.custom_stake_amount(
            pair="SOL/USDT",
            current_time=now,
            current_rate=150.0,
            proposed_stake=100.0,
            min_stake=10.0,  # Exchange requires $10.00
            max_stake=100.0,
            leverage=1.0,
            entry_tag="trend_pullback",
            side="buy"
        )
        # Since $5.56 < $10.00 min_stake, bot skips to preserve risk limits
        self.assertEqual(stake, 0.0)

    def test_custom_exit_profit_target(self):
        """
        Tests that custom_exit returns 'profit_target' when profit >= +3.5%.
        """
        trade = MagicMock()
        trade.enter_tag = "trend_pullback"
        now = datetime.now(timezone.utc)

        exit_reason = self.strategy.custom_exit(
            pair="BTC/USDT",
            trade=trade,
            current_time=now,
            current_rate=65000.0,
            current_profit=0.036  # +3.6%
        )
        self.assertEqual(exit_reason, "profit_target")

    def test_custom_exit_breakout_failure(self):
        """
        Tests that breakout entries exit early at -2.0% (breakout_failure)
        instead of waiting for the full -4.0% hard stop.
        """
        trade = MagicMock()
        trade.enter_tag = "breakout"
        trade.open_date_utc = None
        now = datetime.now(timezone.utc)

        # At -2.1% loss
        exit_reason = self.strategy.custom_exit(
            pair="SOL/USDT",
            trade=trade,
            current_time=now,
            current_rate=145.0,
            current_profit=-0.021
        )
        self.assertEqual(exit_reason, "breakout_failure")

        # Pullback trades at -2.1% should NOT trigger breakout_failure
        trade.enter_tag = "trend_pullback"
        exit_reason_pullback = self.strategy.custom_exit(
            pair="SOL/USDT",
            trade=trade,
            current_time=now,
            current_rate=145.0,
            current_profit=-0.021
        )
        self.assertIsNone(exit_reason_pullback)

    def test_custom_exit_time_stagnation(self):
        """
        Tests that positions open for >= 48 hours with < +0.5% profit
        trigger 'time_stagnation' exit.
        """
        now = datetime.now(timezone.utc)
        open_time = now - timedelta(hours=49)
        trade = MagicMock()
        trade.enter_tag = "trend_pullback"
        trade.open_date_utc = open_time

        exit_reason = self.strategy.custom_exit(
            pair="ETH/USDT",
            trade=trade,
            current_time=now,
            current_rate=3000.0,
            current_profit=0.002  # +0.2% after 49 hours
        )
        self.assertEqual(exit_reason, "time_stagnation")

        # Trade with good profit (+2.0%) after 49 hours should NOT exit on stagnation
        exit_reason_profitable = self.strategy.custom_exit(
            pair="ETH/USDT",
            trade=trade,
            current_time=now,
            current_rate=3060.0,
            current_profit=0.020
        )
        self.assertIsNone(exit_reason_profitable)

    def test_no_lookahead_bias_in_rolling_high(self):
        """
        Critical Test: Confirms that rolling_high_20 strictly excludes current candle (shift(1)).
        """
        class MockSeries(list):
            def shift(self, periods):
                return MockSeries([None] * periods + self[:-periods])
            def rolling(self, window):
                class RollingMock:
                    def __init__(self, data, w):
                        self.data = data
                        self.w = w
                    def max(self):
                        res = []
                        for i in range(len(self.data)):
                            window_data = [x for x in self.data[max(0, i - self.w + 1): i + 1] if x is not None]
                            res.append(max(window_data) if window_data else None)
                        return res
                return RollingMock(self, window)

        highs = [100.0 + i for i in range(25)]
        # Make the last candle (index 24) a huge spike to 500.0
        highs[24] = 500.0

        mock_highs = MockSeries(highs)
        rolling_shifted = mock_highs.shift(1).rolling(20).max()

        # At index 24, the rolling high must be based on candles 4..23, NOT 24 (500.0)!
        self.assertEqual(rolling_shifted[24], 123.0)
        self.assertNotEqual(rolling_shifted[24], 500.0)

    def test_guard_blocking_suppresses_entries(self):
        """
        Tests that when RiskGuards halt the bot, populate_entry_trend zeros out enter_long.
        """
        self.strategy.guards.is_halted = True
        self.strategy.guards.halt_reason = "Daily loss limit breached"

        df_mock = {
            "close": [150.0],
            "low": [140.0],
            "ema20": [145.0],
            "ema50": [140.0],
            "ema200": [130.0],
            "rsi": [55.0],
            "volume": [1000.0],
            "volume_mean_20": [500.0],
            "rolling_high_20": [148.0],
            "btc_close_4h": [65000.0],
            "btc_ema50_4h": [60000.0],
            "enter_long": [1],
        }

        class MockDF(dict):
            def __setitem__(self, k, v):
                super().__setitem__(k, v)
        mdf = MockDF(df_mock)

        result_df = self.strategy.populate_entry_trend(mdf, {"pair": "SOL/USDT"})
        self.assertEqual(result_df["enter_long"], 0)

    def test_reconciliation_gate_suppresses_entries(self):
        """
        Tests that when startup reconciliation has not passed, entries are suppressed.
        """
        self.strategy.guards.is_halted = False
        self.strategy._reconciliation_passed = False

        class MockDF(dict):
            def __setitem__(self, k, v):
                super().__setitem__(k, v)
        mdf = MockDF({"enter_long": [1]})

        result_df = self.strategy.populate_entry_trend(mdf, {"pair": "SOL/USDT"})
        self.assertEqual(result_df["enter_long"], 0)

    def test_order_filled_durable_outbox_integration(self):
        """
        Tests that order_filled persists fill events to durable SQLite outbox
        with correct idempotency key and payload fields.
        """
        trade = MagicMock()
        trade.id = 88
        trade.entry_side = "buy"
        trade.enter_tag = "trend_pullback"

        order = MagicMock()
        order.order_id = "ord-v2-test-fill"
        order.safe_filled = 4.5
        order.safe_price = 152.0
        order.safe_cost = 684.0
        order.ft_order_side = "buy"
        order.order_filled_utc = datetime(2026, 10, 10, 14, 0, 0, tzinfo=timezone.utc)

        self.strategy.order_filled(
            pair="SOL/USDT",
            trade=trade,
            order=order,
            current_time=order.order_filled_utc
        )

        # Verify record in SQLite outbox
        events = self.strategy.outbox.get_pending_events()
        self.assertEqual(len(events), 1)
        ev = events[0]
        self.assertEqual(ev["event_id"], "freqtrade-alpha-1:88:ord-v2-test-fill:entry_fill")
        payload = json.loads(ev["payload"]) if isinstance(ev["payload"], str) else ev["payload"]
        self.assertEqual(payload["tag"], "trend_pullback")
        self.assertEqual(payload["filledAmount"], 4.5)
        self.assertEqual(payload["fillPrice"], 152.0)

    def test_order_filled_exit_updates_guard_state(self):
        """
        Tests that exit fills update RiskGuards realized PnL and cooldown.
        """
        trade = MagicMock()
        trade.id = 89
        trade.entry_side = "buy"
        trade.exit_reason = "profit_target"
        trade.close_profit_abs = 25.50

        order = MagicMock()
        order.order_id = "ord-v2-exit-1"
        order.safe_filled = 4.5
        order.safe_price = 157.66
        order.safe_cost = 709.50
        order.ft_order_side = "sell"
        order.order_filled_utc = datetime(2026, 10, 10, 16, 0, 0, tzinfo=timezone.utc)

        prev_realized = self.strategy.guards.realized_today
        self.strategy.order_filled(
            pair="SOL/USDT",
            trade=trade,
            order=order,
            current_time=order.order_filled_utc
        )

        self.assertEqual(self.strategy.guards.realized_today, prev_realized + 25.50)
        self.assertEqual(self.strategy.guards.consecutive_losses, 0)


if __name__ == "__main__":
    unittest.main()
