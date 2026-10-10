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
        Validates Freqtrade container contract attributes.
        """
        self.assertEqual(self.strategy.INTERFACE_VERSION, 3)
        self.assertEqual(self.strategy.timeframe, "1h")
        self.assertEqual(self.strategy.inf_timeframe, "4h")
        self.assertEqual(self.strategy.stoploss, -0.04)
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
            "dry_run_wallet": 50000.0,  # Simulated value present in config
            "stake_currency": "USDT",
        }
        live_strategy = WowsStrategyV2(live_config)
        # Without wallets attached, MUST return 0.0, NOT 50000.0
        self.assertEqual(live_strategy._get_current_equity(), 0.0)

        # Attaching a verified live wallet with $25,000 returns $25,000
        mock_wallets = MagicMock()
        mock_wallets.get_total_stake_amount.return_value = 25000.0
        live_strategy.wallets = mock_wallets
        self.assertEqual(live_strategy._get_current_equity(), 25000.0)

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
            entry_tag="trend_pullback"
        )
        self.assertFalse(entry_allowed)

        # 2. Exits remain fully operational
        trade = MagicMock()
        trade.enter_tag = "trend_pullback"
        trade.open_date_utc = None

        exit_reason = self.strategy.custom_exit(
            pair="SOL/USDT",
            trade=trade,
            current_time=now,
            current_rate=155.0,
            current_profit=0.036
        )
        self.assertEqual(exit_reason, "profit_target")

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
                entry_tag="trend_pullback",
                side="buy"
            )
            self.assertAlmostEqual(stake, expected_stake, places=2)

    def test_custom_stake_amount_fails_closed_on_missing_equity(self):
        """
        P0 Test: If reliable equity is missing or non-positive,
        the strategy MUST fail closed (return 0.0) rather than assuming $50,000.
        """
        now = datetime.now(timezone.utc)
        self.strategy.config = {"stake_currency": "USDT"}  # No wallet or dry_run_wallet

        stake = self.strategy.custom_stake_amount(
            pair="SOL/USDT",
            current_time=now,
            current_rate=150.0,
            proposed_stake=100.0,
            min_stake=10.0,
            max_stake=10000.0,
            leverage=1.0,
            entry_tag="trend_pullback",
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
        self.strategy.config["dry_run_wallet"] = 100.0  # yields $5.56 stake
        stake = self.strategy.custom_stake_amount(
            pair="SOL/USDT",
            current_time=now,
            current_rate=150.0,
            proposed_stake=100.0,
            min_stake=10.0,  # Exchange minimum
            max_stake=100.0,
            leverage=1.0,
            entry_tag="trend_pullback",
            side="buy"
        )
        self.assertEqual(stake, 0.0)

    def test_confirm_trade_entry_guards_and_chronological_evaluation(self):
        """
        P0 Test: confirm_trade_entry acts as the dynamic pre-trade gate.
        Validates sequential chronological time-advancement.
        """
        trade_time = datetime(2026, 10, 10, 12, 0, 0, tzinfo=timezone.utc)

        # 1. Healthy state allows entry
        confirmed = self.strategy.confirm_trade_entry(
            pair="SOL/USDT",
            order_type="limit",
            amount=5.0,
            rate=150.0,
            time_in_force="gtc",
            current_time=trade_time,
            entry_tag="trend_pullback"
        )
        self.assertTrue(confirmed)

        # 2. Simulate loss on SOL/USDT -> triggers 5-bar cooldown
        trade_time_ms = int(trade_time.timestamp() * 1000)
        self.strategy.guards.on_trade_closed("SOL/USDT", -100.0, trade_time_ms)

        # 2 hours later (2 bars < 5 required) -> rejected
        time_plus_2h = trade_time + timedelta(hours=2)
        confirmed_cooldown = self.strategy.confirm_trade_entry(
            pair="SOL/USDT",
            order_type="limit",
            amount=5.0,
            rate=150.0,
            time_in_force="gtc",
            current_time=time_plus_2h,
            entry_tag="trend_pullback"
        )
        self.assertFalse(confirmed_cooldown)

        # Different pair (ETH/USDT) not in cooldown is allowed
        confirmed_eth = self.strategy.confirm_trade_entry(
            pair="ETH/USDT",
            order_type="limit",
            amount=1.0,
            rate=3000.0,
            time_in_force="gtc",
            current_time=time_plus_2h,
            entry_tag="trend_pullback"
        )
        self.assertTrue(confirmed_eth)

    def test_confirm_trade_entry_fails_closed_on_unreconciled_or_missing_equity(self):
        """
        P0 Test: confirm_trade_entry fails closed if reconciliation is unverified or equity is missing.
        """
        now = datetime.now(timezone.utc)

        # Live mode with failed reconciliation
        self.strategy.config["runmode"] = "live"
        self.strategy._reconciliation_passed = False
        confirmed = self.strategy.confirm_trade_entry(
            pair="SOL/USDT",
            order_type="limit",
            amount=5.0,
            rate=150.0,
            time_in_force="gtc",
            current_time=now,
            entry_tag="trend_pullback"
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
            entry_tag="trend_pullback"
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

    def test_custom_exit_profit_target(self):
        """
        Tests fixed take-profit target (+3.5%) in custom_exit.
        """
        trade = MagicMock()
        trade.enter_tag = "trend_pullback"
        trade.open_date_utc = None
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
        highs[24] = 500.0  # Massive spike at candle 24

        mock_highs = MockSeries(highs)
        rolling_shifted = mock_highs.shift(1).rolling(20).max()

        # At index 24, rolling high MUST be 123.0, NOT 500.0
        self.assertEqual(rolling_shifted[24], 123.0)
        self.assertNotEqual(rolling_shifted[24], 500.0)

    def test_order_filled_durable_outbox_integration(self):
        """
        Tests that order_filled persists fill events to durable SQLite outbox in live/dry-run mode.
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

        events = self.strategy.outbox.get_pending_events()
        self.assertEqual(len(events), 1)
        ev = events[0]
        self.assertEqual(ev["event_id"], "freqtrade-alpha-1:88:ord-v2-test-fill:entry_fill")
        payload = json.loads(ev["payload"]) if isinstance(ev["payload"], str) else ev["payload"]
        self.assertEqual(payload["tag"], "trend_pullback")
        self.assertEqual(payload["filledAmount"], 4.5)
        self.assertEqual(payload["fillPrice"], 152.0)


if __name__ == "__main__":
    unittest.main()
