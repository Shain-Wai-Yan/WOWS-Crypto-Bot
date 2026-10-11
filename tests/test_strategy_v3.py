"""
Unit tests for WowsStrategyV3 (Option C Tactical 4H Macro Trend Following).
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
        self.assertEqual(self.strategy.stoploss, -0.07)
        self.assertTrue(self.strategy.use_custom_stoploss)
        self.assertEqual(self.strategy.MAX_POSITION_EQUITY_RATIO, 0.25)
        self.assertEqual(self.strategy.RISK_FRACTION_PER_TRADE, 0.015)

    def test_rolling_high_zero_lookahead_bias(self):
        # Create synthetic 4H DataFrame
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
        self.assertIn("rolling_high_42", analyzed.columns)
        
        # Verify rolling_high_42 at index i is max of highs up to index i-1, NOT including index i
        for idx in range(50, 100):
            expected_max = df["high"].iloc[idx-42:idx].max()
            actual_val = analyzed["rolling_high_42"].iloc[idx]
            self.assertAlmostEqual(expected_max, actual_val, places=4)

    def test_tactical_sleeve_stake_sizing(self):
        # On $3,000 equity:
        # risk_budget = 3000 * 0.015 = $45.00
        # risk_stake = 45 / 0.070 = 642.86
        # position_cap = 3000 * 0.25 = $750.00
        stake = self.strategy.custom_stake_amount(
            pair="SOL/USDT",
            current_time=datetime.now(timezone.utc),
            current_rate=150.0,
            proposed_stake=100.0,
            min_stake=10.0,
            max_stake=3000.0,
            leverage=1.0,
            entry_tag="macro_breakout",
            side="long"
        )
        self.assertAlmostEqual(stake, 642.86, places=1)
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
            entry_tag="macro_breakout",
            side="long"
        )
        self.assertEqual(stake, 0.0)

    def test_custom_stoploss_asymmetry(self):
        class DummyTrade:
            open_rate = 100.0

        trade = DummyTrade()
        now = datetime.now(timezone.utc)

        # 1. Under +5.0% profit: return None (initial -7.0% stop stays intact)
        stop_sub5 = self.strategy.custom_stoploss(
            pair="SOL/USDT",
            trade=trade,
            current_time=now,
            current_rate=103.0,
            current_profit=0.03,
            after_fill=False
        )
        self.assertIsNone(stop_sub5)

        # 2. At +6.0% profit: breakeven lock kicks in at least +1.5%
        stop_profit6 = self.strategy.custom_stoploss(
            pair="SOL/USDT",
            trade=trade,
            current_time=now,
            current_rate=106.0,
            current_profit=0.06,
            after_fill=False
        )
        self.assertIsNotNone(stop_profit6)
        # Stop should be tightened closer than -6.0%
        self.assertGreater(stop_profit6, -0.06)

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
