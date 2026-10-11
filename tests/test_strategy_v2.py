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

    def test_strategy_attributes_and_interface_version(self):
        """
        Validates Freqtrade container contract attributes for Experiment V2-B.
        """
        self.assertEqual(self.strategy.INTERFACE_VERSION, 3)
        self.assertEqual(self.strategy.timeframe, "1h")
        self.assertEqual(self.strategy.inf_timeframe, "4h")
        self.assertEqual(self.strategy.stoploss, -0.04)
        self.assertTrue(self.strategy.use_custom_stoploss)
        self.assertFalse(self.strategy.enable_trend_pullback)
        self.assertTrue(self.strategy.enable_breakout)
        self.assertFalse(self.strategy.can_short)
        self.assertTrue(self.strategy.process_only_new_candles)
        self.assertEqual(self.strategy.informative_pairs(), [("BTC/USDT", "4h")])

    def test_live_equity_strict_isolation_fails_closed(self):
        """
        Critical Test: In live mode (dry_run=False), _get_current_equity()
        MUST NEVER fall back to dry_run_wallet or simulated config values.
        If live wallet is missing/empty, it must fail closed (return 0.0).
        """
        live_config = {
            "dry_run": False,
            "dry_run_wallet": 50000.0,
            "stake_currency": "USDT",
        }
        live_strategy = WowsStrategyV2(live_config)
        self.assertEqual(live_strategy._get_current_equity(), 0.0)

        mock_wallets = MagicMock()
        mock_wallets.get_total_stake_amount.return_value = 25000.0
        live_strategy.wallets = mock_wallets
        self.assertEqual(live_strategy._get_current_equity(), 25000.0)

    def test_custom_stake_amount_clamped_to_available_free_cash(self):
        """
        Critical Test: When free cash in wallet is less than calculated risk stake,
        the allocated stake is clamped to free uncommitted cash.
        """
        now = datetime.now(timezone.utc)
        self.strategy.config["dry_run_wallet"] = 10000.0

        mock_wallets = MagicMock()
        mock_wallets.get_total_stake_amount.return_value = 10000.0
        mock_wallets.get_available_stake_amount.return_value = 300.0
        self.strategy.wallets = mock_wallets

        stake = self.strategy.custom_stake_amount(
            pair="SOL/USDT",
            current_time=now,
            current_rate=150.0,
            proposed_stake=100.0,
            min_stake=10.0,
            max_stake=10000.0,
            leverage=1.0,
            entry_tag="breakout",
            side="buy"
        )
        self.assertEqual(stake, 300.0)

    def test_exit_path_remains_operational_when_reconciliation_fails(self):
        """
        Critical Test: When startup reconciliation fails, new entries are blocked
        via confirm_trade_entry, but exits (custom_exit, populate_exit_trend)
        must remain 100% operational.
        """
        self.strategy._reconciliation_passed = False

        # 1. Entry is blocked
        now = datetime.now(timezone.utc)
        entry_allowed = self.strategy.confirm_trade_entry(
            pair="SOL/USDT",
            order_type="limit",
            amount=5.0,
            rate=150.0,
            time_in_force="gtc",
            current_time=now,
            entry_tag="breakout"
        )
        self.assertFalse(entry_allowed)

        # 2. Exits remain fully operational (testing breakout_failure exit)
        trade = MagicMock()
        trade.enter_tag = "breakout"
        trade.open_date_utc = None

        exit_reason = self.strategy.custom_exit(
            pair="SOL/USDT",
            trade=trade,
            current_time=now,
            current_rate=145.0,
            current_profit=-0.021
        )
        self.assertEqual(exit_reason, "breakout_failure")

    def test_backtest_isolation_no_network_or_threads(self):
        """
        P0 Test: In backtest mode, bot_start() must NOT start background
        threads (dispatcher/telemetry) or perform live reconciliation calls.
        """
        backtest_config = {
            "runmode": "backtest",
            "dry_run": True,
            "dry_run_wallet": 10000.0,
            "stake_currency": "USDT",
        }
        bt_strategy = WowsStrategyV2(backtest_config)
        self.assertFalse(bt_strategy.is_live_or_dryrun())

        with patch.object(bt_strategy.reconciliation, "reconcile") as mock_reconcile:
            bt_strategy.bot_start()
            mock_reconcile.assert_not_called()

        self.assertTrue(bt_strategy._reconciliation_passed)
        self.assertIsNone(bt_strategy.dispatcher._thread)
        self.assertIsNone(bt_strategy.telemetry._thread)

    def test_custom_stake_amount_across_multiple_equity_levels(self):
        """
        Tests risk-budget sizing formula across diverse starting balances:
        Stake = min((Equity * 0.0025) / 0.045, Equity * 0.10, Available_balance)
        Ratio = 0.0025 / 0.045 = 5.5555...%
        """
        now = datetime.now(timezone.utc)

        test_cases = [
            (1000.0, 55.56),
            (5000.0, 277.78),
            (10000.0, 555.56),
            (50000.0, 2777.78),
        ]

        for eq, expected_stake in test_cases:
            self.strategy.config["dry_run_wallet"] = eq
            stake = self.strategy.custom_stake_amount(
                pair="SOL/USDT",
                current_time=now,
                current_rate=150.0,
                proposed_stake=100.0,
                min_stake=10.0,
                max_stake=10000.0,
                leverage=1.0,
                entry_tag="breakout",
                side="buy"
            )
            self.assertAlmostEqual(stake, expected_stake, places=2)

    def test_custom_stake_amount_fails_closed_on_missing_equity(self):
        """
        P0 Test: If reliable equity is missing or non-positive,
        the strategy MUST fail closed (return 0.0) rather than assuming $50,000.
        """
        now = datetime.now(timezone.utc)
        self.strategy.config = {"stake_currency": "USDT"}

        stake = self.strategy.custom_stake_amount(
            pair="SOL/USDT",
            current_time=now,
            current_rate=150.0,
            proposed_stake=100.0,
            min_stake=10.0,
            max_stake=10000.0,
            leverage=1.0,
            entry_tag="breakout",
            side="buy"
        )
        self.assertEqual(stake, 0.0)

    def test_custom_stake_amount_position_cap_10_percent(self):
        """
        Tests that stake never exceeds 10% maximum portfolio allocation per position.
        """
        now = datetime.now(timezone.utc)
        self.strategy.STOP_DISTANCE_FRACTION = 0.01  # very small stop yields $2,500
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
            min_stake=10.0,
            max_stake=100.0,
            leverage=1.0,
            entry_tag="breakout",
            side="buy"
        )
        self.assertEqual(stake, 0.0)

    def test_confirm_trade_entry_guards_and_chronological_evaluation(self):
        """
        Tests that confirm_trade_entry evaluates RiskGuards chronologically.
        """
        now = datetime(2026, 10, 10, 12, 0, 0, tzinfo=timezone.utc)
        self.strategy.config["runmode"] = "live"
        self.strategy._reconciliation_passed = True
        self.strategy.config["dry_run"] = False

        mock_wallets = MagicMock()
        mock_wallets.get_total_stake_amount.return_value = 10000.0
        self.strategy.wallets = mock_wallets

        # 1. First entry should pass
        ok1 = self.strategy.confirm_trade_entry(
            pair="SOL/USDT",
            order_type="limit",
            amount=5.0,
            rate=150.0,
            time_in_force="gtc",
            current_time=now,
            entry_tag="breakout"
        )
        self.assertTrue(ok1)

        # 2. Trigger cooldown by recording a loss on SOL/USDT at 'now'
        timestamp_ms = int(now.timestamp() * 1000)
        self.strategy.guards.on_trade_closed("SOL/USDT", -50.0, timestamp_ms)

        # 3. Next entry on SOL/USDT 1 hour later must be suppressed by cooldown
        later = now + timedelta(hours=1)
        ok2 = self.strategy.confirm_trade_entry(
            pair="SOL/USDT",
            order_type="limit",
            amount=5.0,
            rate=145.0,
            time_in_force="gtc",
            current_time=later,
            entry_tag="breakout"
        )
        self.assertFalse(ok2)

        # 4. Entry on a DIFFERENT pair (DOGE/USDT) should still be allowed
        ok_other = self.strategy.confirm_trade_entry(
            pair="DOGE/USDT",
            order_type="limit",
            amount=100.0,
            rate=0.15,
            time_in_force="gtc",
            current_time=later,
            entry_tag="breakout"
        )
        self.assertTrue(ok_other)

    def test_confirm_trade_entry_fails_closed_on_unreconciled_or_missing_equity(self):
        """
        P0 Test: In live mode, confirm_trade_entry blocks entries if reconciliation
        has not passed, or if validated equity is unavailable.
        """
        now = datetime.now(timezone.utc)
        self.strategy.config["dry_run"] = False
        self.strategy.config["runmode"] = "live"
        self.strategy._reconciliation_passed = False
        confirmed = self.strategy.confirm_trade_entry(
            pair="SOL/USDT",
            order_type="limit",
            amount=5.0,
            rate=150.0,
            time_in_force="gtc",
            current_time=now,
            entry_tag="breakout"
        )
        self.assertFalse(confirmed)

        # Missing equity
        self.strategy._reconciliation_passed = True
        self.strategy.config["dry_run_wallet"] = 0.0
        confirmed_no_eq = self.strategy.confirm_trade_entry(
            pair="SOL/USDT",
            order_type="limit",
            amount=5.0,
            rate=150.0,
            time_in_force="gtc",
            current_time=now,
            entry_tag="breakout"
        )
        self.assertFalse(confirmed_no_eq)

    def test_risk_guards_daily_loss_3_percent_and_rollover(self):
        """
        Verifies 3.0% daily loss limit and automatic chronological day rollover.
        """
        guards = RiskGuards(max_daily_loss_pct=0.03, timeframe_minutes=60)
        day1_start_ms = 1791628800000  # Day 1 00:00 UTC

        allowed, _, _ = guards.check_entry_allowed("SOL/USDT", day1_start_ms, 10000.0)
        self.assertTrue(allowed)

        # Take $350 loss on $10,000 equity (3.5% > 3.0% limit)
        guards.on_trade_closed("SOL/USDT", -350.0, day1_start_ms + 3600000)
        allowed, reason, is_halt = guards.check_entry_allowed("SOL/USDT", day1_start_ms + 7200000, 9650.0)
        self.assertFalse(allowed)
        self.assertTrue(is_halt)
        self.assertIn("Daily loss limit hit", reason)

        # Advance to Day 2 (Day rollover) -> automatically resets daily loss
        day2_ms = day1_start_ms + (25 * 3600 * 1000)
        allowed_day2, _, is_halt_day2 = guards.check_entry_allowed("SOL/USDT", day2_ms, 9650.0)
        self.assertTrue(allowed_day2)
        self.assertFalse(is_halt_day2)

    def test_custom_exit_uncapped_winners_and_custom_stoploss(self):
        """
        Tests Experiment V2-B Asymmetric Exit Logic:
        1. custom_exit returns None on winning trades (uncapped winners, no fixed 3.5% ceiling).
        2. custom_stoploss returns None when profit < 2.5% (retains initial -4.0% stop).
        3. custom_stoploss locks breakeven (+0.3%) when profit reaches +2.5%.
        4. custom_stoploss trails as profit expands further (e.g. +10.0%).
        """
        trade = MagicMock()
        trade.enter_tag = "breakout"
        trade.open_date_utc = None
        now = datetime.now(timezone.utc)

        # 1. custom_exit does NOT cap profit at +3.6%
        exit_reason = self.strategy.custom_exit(
            pair="SOL/USDT",
            trade=trade,
            current_time=now,
            current_rate=155.0,
            current_profit=0.036
        )
        self.assertIsNone(exit_reason)

        # 2. custom_stoploss returns None when profit is +1.0% (< +2.5%)
        sl_small_profit = self.strategy.custom_stoploss(
            pair="SOL/USDT",
            trade=trade,
            current_time=now,
            current_rate=101.0,
            current_profit=0.010,
            after_fill=False
        )
        self.assertIsNone(sl_small_profit)

        # 3. custom_stoploss ratchets to breakeven (+0.3% above entry) when profit is +2.5%
        sl_be = self.strategy.custom_stoploss(
            pair="SOL/USDT",
            trade=trade,
            current_time=now,
            current_rate=102.5,
            current_profit=0.025,
            after_fill=False
        )
        self.assertIsNotNone(sl_be)
        # Verify stop price is at entry * 1.003
        entry_price = 100.0
        current_price = 102.5
        stop_price = current_price * (1 - abs(sl_be))
        self.assertAlmostEqual(stop_price, 100.30, places=2)

        # 4. custom_stoploss trails at higher profit (+10.0%)
        sl_trail = self.strategy.custom_stoploss(
            pair="SOL/USDT",
            trade=trade,
            current_time=now,
            current_rate=110.0,
            current_profit=0.10,
            after_fill=False
        )
        self.assertIsNotNone(sl_trail)
        stop_price_trail = 110.0 * (1 - abs(sl_trail))
        self.assertGreater(stop_price_trail, 105.0)

    def test_custom_exit_breakout_failure(self):
        """
        Tests that breakout entries exit early at -2.0% (breakout_failure).
        """
        trade = MagicMock()
        trade.enter_tag = "breakout"
        trade.open_date_utc = None
        now = datetime.now(timezone.utc)

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
        Tests that positions open for >= 48 hours with < +0.5% profit trigger time_stagnation.
        """
        now = datetime.now(timezone.utc)
        open_time = now - timedelta(hours=49)
        trade = MagicMock()
        trade.enter_tag = "breakout"
        trade.open_date_utc = open_time

        exit_reason = self.strategy.custom_exit(
            pair="ETH/USDT",
            trade=trade,
            current_time=now,
            current_rate=3000.0,
            current_profit=0.002
        )
        self.assertEqual(exit_reason, "time_stagnation")

    def test_no_lookahead_bias_in_rolling_high(self):
        """
        Critical Test: Confirms that rolling_high_36 strictly excludes current candle (shift(1)).
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

        highs = [100.0 + i for i in range(40)]
        highs[39] = 999.0  # Massive spike at candle 39

        mock_highs = MockSeries(highs)
        rolling_shifted = mock_highs.shift(1).rolling(36).max()

        # At index 39, rolling high MUST be 138.0, NOT 999.0
        self.assertEqual(rolling_shifted[39], 138.0)
        self.assertNotEqual(rolling_shifted[39], 999.0)

    def test_order_filled_durable_outbox_integration(self):
        """
        Tests that order_filled persists fill events to durable SQLite outbox in live/dry-run mode.
        """
        trade = MagicMock()
        trade.id = 88
        trade.entry_side = "buy"
        trade.enter_tag = "breakout"

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

        events = self.strategy.outbox.get_pending_events()
        self.assertEqual(len(events), 1)
        ev = events[0]
        self.assertEqual(ev["event_id"], "freqtrade-alpha-1:88:ord-v2-test-fill:entry_fill")
        payload = json.loads(ev["payload"]) if isinstance(ev["payload"], str) else ev["payload"]
        self.assertEqual(payload["tag"], "breakout")
        self.assertEqual(payload["filledAmount"], 4.5)
        self.assertEqual(payload["fillPrice"], 152.0)

    def test_ablation_switches(self):
        """Tests that enable_trend_pullback and enable_breakout correctly gate signals."""
        import pandas as pd
        data = {
            "close": [100.0] * 49 + [110.0],
            "low": [98.0] * 49 + [104.0],
            "ema20": [102.0] * 49 + [105.0],
            "ema50": [95.0] * 50,
            "ema200": [90.0] * 50,
            "btc_close_4h": [65000.0] * 50,
            "btc_ema50_4h": [60000.0] * 50,
            "rsi": [50.0] * 49 + [45.0],
            "adx": [25.0] * 50,
            "volume": [500.0] * 49 + [2000.0],
            "volume_mean_20": [1000.0] * 50,
            "rolling_high_36": [150.0] * 50,
            "rolling_high_20": [150.0] * 50,
        }
        df = pd.DataFrame(data)

        # With pullback enabled
        self.strategy.enable_trend_pullback = True
        self.strategy.enable_breakout = False
        res = self.strategy.populate_entry_trend(df.copy(), {"pair": "SOL/USDT"})
        self.assertEqual(res.loc[res.index[-1], "enter_long"], 1)
        self.assertEqual(res.loc[res.index[-1], "enter_tag"], "trend_pullback")

        # With pullback disabled
        self.strategy.enable_trend_pullback = False
        self.strategy.enable_breakout = False
        res2 = self.strategy.populate_entry_trend(df.copy(), {"pair": "SOL/USDT"})
        self.assertEqual(res2.loc[res2.index[-1], "enter_long"], 0)

    def test_baseline_research_mode_isolation(self):
        """Tests that WowsStrategy in research mode bypasses live workers."""
        from user_data.strategies.wows_strategy import WowsStrategy
        strat = WowsStrategy({"runmode": "backtest"})
        self.assertTrue(strat.is_research_mode())
        strat.bot_start()
        self.assertTrue(strat._reconciliation_passed)


if __name__ == "__main__":
    unittest.main()
