"""
Unit tests for WowsStrategyV3 (Upgraded Keltner Squeeze & Chandelier Trailing Exit).
"""

import unittest
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import os
import sys

# Add strategy path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../user_data/strategies"))
from wows_strategy_v3 import WowsStrategyV3


class TestWowsStrategyV3(unittest.TestCase):

    def setUp(self):
        self.config = {
            "dry_run": True,
            "dry_run_wallet": 3000.0,
            "stake_currency": "USDT",
            "stake_amount": 100.0,
            "wows_strategy": {
                "enable_breakout": True,
            }
        }
        self.strategy = WowsStrategyV3(self.config)

    def test_strategy_configuration_attributes(self):
        self.assertEqual(self.strategy.timeframe, "4h")
        self.assertEqual(self.strategy.inf_timeframe, "1d")
        self.assertEqual(self.strategy.stoploss, -0.06)
        self.assertTrue(self.strategy.use_custom_stoploss)
        self.assertEqual(self.strategy.MAX_POSITION_EQUITY_RATIO, 0.25)
        self.assertEqual(self.strategy.RISK_FRACTION_PER_TRADE, 0.015)
        self.assertEqual(self.strategy.STOP_DISTANCE_FRACTION, 0.060)

    def test_keltner_and_relative_strength_indicators(self):
        dates = pd.date_range("2024-01-01", periods=100, freq="4h")
        highs = np.linspace(100, 200, 100)
        df = pd.DataFrame({
            "date": dates,
            "open": highs - 1,
            "high": highs,
            "low": highs - 2,
            "close": highs - 0.5,
            "volume": [1000.0] * 100
        })
        
        analyzed = self.strategy.populate_indicators(df, {"pair": "SOL/USDT"})
        self.assertIn("keltner_upper", analyzed.columns)
        self.assertIn("roc_42", analyzed.columns)
        self.assertIn("ema20", analyzed.columns)
        self.assertIn("atr", analyzed.columns)
        
        # Verify Keltner upper = ema20 + 1.8 * atr
        expected_keltner = analyzed["ema20"] + (1.8 * analyzed["atr"])
        np.testing.assert_allclose(analyzed["keltner_upper"].dropna(), expected_keltner.dropna(), rtol=1e-4)

    def test_tactical_sleeve_stake_sizing(self):
        # On $3,000 equity:
        # risk_budget = 3000 * 0.015 = $45.00
        # risk_stake = 45 / 0.060 = 750.00
        # position_cap = 3000 * 0.25 = $750.00
        stake = self.strategy.custom_stake_amount(
            pair="SOL/USDT",
            current_time=datetime.now(timezone.utc),
            current_rate=150.0,
            proposed_stake=100.0,
            min_stake=10.0,
            max_stake=3000.0,
            leverage=1.0,
            entry_tag="keltner_breakout",
            side="long"
        )
        self.assertAlmostEqual(stake, 750.0, places=1)
        self.assertLessEqual(stake, 750.0)

    def test_tactical_sleeve_stake_fails_closed_zero_equity(self):
        zero_strat = WowsStrategyV3({"dry_run": True, "dry_run_wallet": 0.0})
        stake = zero_strat.custom_stake_amount(
            pair="SOL/USDT",
            current_time=datetime.now(timezone.utc),
            current_rate=150.0,
            proposed_stake=100.0,
            min_stake=10.0,
            max_stake=1000.0,
            leverage=1.0,
            entry_tag="keltner_breakout",
            side="long"
        )
        self.assertEqual(stake, 0.0)

    def test_chandelier_custom_stoploss(self):
        class DummyTrade:
            open_rate = 100.0
            max_rate = 110.0  # Peak price reached was 110

        trade = DummyTrade()
        now = datetime.now(timezone.utc)

        # 1. Under +4.0% profit: return None (initial -6.0% stop stays in effect)
        stop_sub4 = self.strategy.custom_stoploss(
            pair="SOL/USDT",
            trade=trade,
            current_time=now,
            current_rate=103.0,
            current_profit=0.03,
            after_fill=False
        )
        self.assertIsNone(stop_sub4)

        # 2. At +5.0% profit: breakeven lock kicks in (+0.5% above entry)
        stop_be5 = self.strategy.custom_stoploss(
            pair="SOL/USDT",
            trade=trade,
            current_time=now,
            current_rate=105.0,
            current_profit=0.05,
            after_fill=False
        )
        self.assertIsNotNone(stop_be5)
        # Should be tightened relative to -6.0%
        self.assertGreater(stop_be5, -0.06)

        # 3. At +8.0% profit: Chandelier trailing stop anchored to trade.max_rate (110.0)
        stop_chan8 = self.strategy.custom_stoploss(
            pair="SOL/USDT",
            trade=trade,
            current_time=now,
            current_rate=108.0,
            current_profit=0.08,
            after_fill=False
        )
        self.assertIsNotNone(stop_chan8)
        self.assertGreater(stop_chan8, -0.10)

    def test_populate_exit_trend_on_ema50_breakdown(self):
        df = pd.DataFrame({
            "close": [95.0, 90.0],
            "ema50": [100.0, 100.0],
            "volume": [100.0, 100.0]
        })
        exit_df = self.strategy.populate_exit_trend(df, {"pair": "SOL/USDT"})
        self.assertEqual(exit_df["exit_long"].iloc[0], 1)
        self.assertEqual(exit_df["exit_long"].iloc[1], 1)


if __name__ == "__main__":
    unittest.main()
