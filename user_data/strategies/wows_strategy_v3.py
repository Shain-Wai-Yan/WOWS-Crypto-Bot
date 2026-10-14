"""
WOWS Crypto Bot v3 (Upgraded V4 Quantitative Architecture)
==========================================================
Option C Tactical Sleeve Implementation (Dedicated $3,000 Portfolio):
1. 4-Hour Base Timeframe with 1-Day Macro BTC Trend Filter & 4H BTC Relative Strength.
2. Cross-Sectional Relative Strength Filter:
   - Requires altcoin 14-day (42-bar on 4h) ROC > (BTC ROC + 2.0%).
   - Only trades market leaders actively outpacing Bitcoin.
3. Volatility Keltner Channel Breakout:
   - Keltner Upper Band = EMA 20 + 1.8 * ATR(14).
   - Entry when 4h close expands above Keltner Upper Band on volume > 1.2x 20 EMA.
   - Requires 4h EMA 50 > EMA 200 AND 1d BTC close > 1d BTC EMA 50.
4. True Chandelier Trailing Exit:
   - Initial Stoploss: -6.0% hard stop.
   - Breakeven Lock: Ratchet stop to +0.5% above entry once trade reaches +4.0%.
   - Chandelier Trailing Stop: Anchored to trade.max_rate * (1.0 - 3.0 * ATR) for winners above +6.0%.
   - Trend Exit: Closed 4h candle below 4h EMA 50.
5. Tactical Sleeve Sizing & Risk Sandbox:
   - RISK_FRACTION_PER_TRADE = 0.015 (1.5% risk = $45 on $3k).
   - STOP_DISTANCE_FRACTION = 0.060 (6.0% initial stop distance).
   - MAX_POSITION_EQUITY_RATIO = 0.25 ($750 max stake per trade, max 4 open positions).
6. Full WOWS Integration: OutboxManager, DispatcherWorker, Reconciliation, Telemetry, RiskGuards.
"""

# pyright: reportMissingImports=false

import os
import json
import time
import math
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, List

try:
    from freqtrade.strategy import (  # type: ignore
        IStrategy,
        informative,
        merge_informative_pair,
        stoploss_from_open,
        stoploss_from_absolute,
    )
    from freqtrade.persistence import Trade, Order  # type: ignore
except ImportError:
    class IStrategy:  # type: ignore
        def __init__(self, config: dict = None) -> None:
            self.config = config or {}
            self.dp = None
            self.wallets = None
    class Trade:  # type: ignore
        max_rate: float = 0.0
    class Order:  # type: ignore
        pass
    def informative(timeframe: str, asset: str = ""):  # type: ignore
        def decorator(f):
            return f
        return decorator
    def merge_informative_pair(dataframe, informative, timeframe, inf_timeframe, ffill=True):  # type: ignore
        return dataframe
    def stoploss_from_open(open_relative_stop: float, current_profit: float, is_short: bool = False, leverage: float = 1.0) -> float:  # type: ignore
        _current_profit = current_profit / leverage
        if (_current_profit == -1 and not is_short) or (is_short and _current_profit == 1):
            return 1.0
        if is_short is True:
            stoploss = -1 + ((1 - open_relative_stop / leverage) / (1 - _current_profit))
        else:
            stoploss = 1 - ((1 + open_relative_stop / leverage) / (1 + _current_profit))
        return max(stoploss * leverage, 0.0)
    def stoploss_from_absolute(stop_rate: float, current_rate: float, is_short: bool = False, leverage: float = 1.0) -> float:  # type: ignore
        if current_rate <= 0:
            return 1.0
        stoploss = 1 - (stop_rate / current_rate)
        if is_short:
            stoploss = -stoploss
        return max(min(stoploss, 1.0), 0.0) * leverage

try:
    from pandas import DataFrame, Series  # type: ignore
except ImportError:
    DataFrame = Any  # type: ignore
    Series = Any  # type: ignore

try:
    import talib.abstract as ta  # type: ignore
except ImportError:
    ta = None  # type: ignore

import sys
from pathlib import Path
_DIR = str(Path(__file__).parent.resolve())
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)

from outbox import OutboxManager
from dispatcher import DispatcherWorker
from reconciliation import ReconciliationClient
from telemetry import TelemetryClient
from guards import RiskGuards

logger = logging.getLogger(__name__)


class WowsStrategyV3(IStrategy):
    """
    WOWS Crypto Bot v3 Strategy Class (Option C Upgraded Architecture).
    """
    INTERFACE_VERSION = 3

    # Primary Timeframe & Execution Settings
    timeframe = "4h"
    inf_timeframe = "1d"
    process_only_new_candles = True
    startup_candle_count = 200  # Warm-up for 200 EMA
    can_short = False
    enable_breakout: bool = True

    # Risk & Protective Exits
    stoploss = -0.06  # -6.0% Hard Initial Stop
    use_custom_stoploss = True

    # Minimal ROI: Ceilings only; dynamic Chandelier trailing stops manage exits
    minimal_roi = {
        "0": 1.0  # Uncapped to capture right-tail multi-week macro runners
    }

    # Tactical Sleeve Risk Budgeting ($3,000 Sandbox)
    RISK_FRACTION_PER_TRADE = 0.015     # 1.5% equity risked per trade ($45 on $3k)
    STOP_DISTANCE_FRACTION = 0.060      # 6.0% stop distance -> $750 stake
    MAX_POSITION_EQUITY_RATIO = 0.25    # 25% max portfolio allocation ($750 max stake on $3k)
    STAGNATION_HOURS = 240.0            # Exit after 10 days (240h) if trade has zero progress
    STAGNATION_MIN_PROFIT = 0.010       # +1.0% minimum threshold for 10-day stagnation

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self.bot_id = os.getenv("BOT_ID", "freqtrade-tactical-v3")
        
        db_env = os.getenv("SQLITE_DB_PATH")
        if db_env:
            self.outbox_db_path = db_env
        elif os.path.exists("/freqtrade/user_data"):
            self.outbox_db_path = "/freqtrade/user_data/outbox.db"
        else:
            local_user_data = os.path.join(str(Path(__file__).parent.parent), "user_data")
            try:
                os.makedirs(local_user_data, exist_ok=True)
                self.outbox_db_path = os.path.join(local_user_data, "outbox.db")
            except Exception:
                import tempfile
                self.outbox_db_path = os.path.join(tempfile.gettempdir(), "outbox.db")

        self.wows_base_url = os.getenv("WOWS_BASE_URL", "http://localhost:3000")
        self.bot_api_key = os.getenv("BOT_API_KEY", "")

        strat_cfg = self.config.get("wows_strategy", {})
        self.enable_breakout = strat_cfg.get("enable_breakout", getattr(self, "enable_breakout", True))

        # Initialize durable subcomponents
        self.outbox = OutboxManager(self.outbox_db_path)
        self.dispatcher = DispatcherWorker(self.outbox, self.wows_base_url, self.bot_api_key)
        self.reconciliation = ReconciliationClient(self.wows_base_url, self.bot_api_key, self.bot_id)
        self.telemetry = TelemetryClient(self.wows_base_url, self.bot_api_key, self.bot_id, interval_sec=60.0)
        self.guards = RiskGuards(
            max_daily_loss_pct=0.05,      # 5.0% daily drawdown circuit breaker on tactical sleeve
            max_consecutive_losses=3,     # 3 consecutive losses
            cooldown_bars=3,              # 3-bar (12 hours) cooldown on losing pair
            timeframe_minutes=240,
        )

        self._reconciliation_passed = False

    def is_live_or_dryrun(self) -> bool:
        mode = self.config.get("runmode")
        if mode:
            mode_str = str(mode).lower()
            return "live" in mode_str or "dry_run" in mode_str or "dryrun" in mode_str
        return False

    def bot_start(self, **kwargs) -> None:
        if not self.is_live_or_dryrun():
            return

        logger.info(f"Starting WowsStrategyV3 in live/dry-run mode for {self.bot_id}")
        try:
            self.dispatcher.start()
            self.telemetry.start()
            report = self.reconciliation.reconcile([])
            if getattr(report, "can_trade", False) or getattr(report, "in_sync", False):
                self._reconciliation_passed = True
                logger.info("Startup reconciliation successfully passed.")
            else:
                self._reconciliation_passed = False
                reason = getattr(report, "discrepancy_reason", "Not in sync")
                logger.warning(f"Startup reconciliation pending/failed: {reason}")
        except Exception as e:
            logger.error(f"Error during bot_start initialization: {e}")
            self._reconciliation_passed = False

    def bot_loop_start(self, current_time: datetime, **kwargs) -> None:
        if self.is_live_or_dryrun():
            self.telemetry.send_heartbeat()

    def informative_pairs(self) -> List[Tuple[str, str]]:
        stake = self.config.get("stake_currency", "USDT")
        return [
            (f"BTC/{stake}", self.inf_timeframe),  # 1d BTC
            (f"BTC/{stake}", self.timeframe),      # 4h BTC
        ]

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Calculates 4H indicators, Keltner Channel, and merges 1D & 4H BTC informative trend/ROC data.
        """
        if ta is not None:
            dataframe["ema20"] = ta.EMA(dataframe, timeperiod=20)
            dataframe["ema50"] = ta.EMA(dataframe, timeperiod=50)
            dataframe["ema200"] = ta.EMA(dataframe, timeperiod=200)
            dataframe["rsi"] = ta.RSI(dataframe, timeperiod=14)
            dataframe["adx"] = ta.ADX(dataframe, timeperiod=14)
            dataframe["atr"] = ta.ATR(dataframe, timeperiod=14)
            dataframe["roc_42"] = ta.ROC(dataframe, timeperiod=42)
        else:
            dataframe["ema20"] = dataframe["close"].ewm(span=20, adjust=False).mean()
            dataframe["ema50"] = dataframe["close"].ewm(span=50, adjust=False).mean()
            dataframe["ema200"] = dataframe["close"].ewm(span=200, adjust=False).mean()
            delta = dataframe["close"].diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = -delta.clip(upper=0).rolling(14).mean()
            rs = gain / (loss + 1e-9)
            dataframe["rsi"] = 100 - (100 / (1 + rs))
            dataframe["adx"] = 25.0
            dataframe["atr"] = (dataframe["high"] - dataframe["low"]).rolling(14).mean()
            dataframe["roc_42"] = ((dataframe["close"] - dataframe["close"].shift(42)) / (dataframe["close"].shift(42) + 1e-9)) * 100

        dataframe["volume_mean_20"] = dataframe["volume"].rolling(window=20).mean()

        # Volatility Keltner Channel Upper Band: EMA 20 + 1.8 * ATR(14)
        dataframe["keltner_upper"] = dataframe["ema20"] + (1.8 * dataframe["atr"])

        # Retain rolling 42-candle high for reference/monitoring
        dataframe["rolling_high_42"] = dataframe["high"].shift(1).rolling(window=42).max()

        # Merge 1d BTC macro trend filter & 4h BTC ROC
        stake = self.config.get("stake_currency", "USDT")
        btc_pair = f"BTC/{stake}"

        if hasattr(self, "dp") and self.dp:
            try:
                # 1D BTC Macro Filter
                inf_1d = self.dp.get_pair_dataframe(pair=btc_pair, timeframe=self.inf_timeframe)
                if inf_1d is not None and not inf_1d.empty:
                    if ta is not None:
                        inf_1d["ema50"] = ta.EMA(inf_1d, timeperiod=50)
                    else:
                        inf_1d["ema50"] = inf_1d["close"].ewm(span=50, adjust=False).mean()

                    inf_1d = inf_1d.rename(columns={"close": "btc_close_1d", "ema50": "btc_ema50_1d"})
                    dataframe = merge_informative_pair(
                        dataframe, inf_1d[["date", "btc_close_1d", "btc_ema50_1d"]],
                        self.timeframe, self.inf_timeframe, ffill=True
                    )

                # 4H BTC Relative Strength Filter
                inf_4h = self.dp.get_pair_dataframe(pair=btc_pair, timeframe=self.timeframe)
                if inf_4h is not None and not inf_4h.empty:
                    if ta is not None:
                        inf_4h["btc_roc_42"] = ta.ROC(inf_4h, timeperiod=42)
                    else:
                        inf_4h["btc_roc_42"] = ((inf_4h["close"] - inf_4h["close"].shift(42)) / (inf_4h["close"].shift(42) + 1e-9)) * 100
                    
                    dataframe = merge_informative_pair(
                        dataframe, inf_4h[["date", "btc_roc_42"]],
                        self.timeframe, self.timeframe, ffill=True
                    )
            except Exception as e:
                logger.warning(f"Could not merge BTC informative data: {e}")

        # Fallbacks for standalone/mock tests
        if "btc_close_1d" not in dataframe.columns or "btc_ema50_1d" not in dataframe.columns:
            dataframe["btc_close_1d"] = 1.0
            dataframe["btc_ema50_1d"] = 0.5
        if "btc_roc_42" not in dataframe.columns:
            dataframe["btc_roc_42"] = 0.0

        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Keltner Volatility Squeeze Breakout + Cross-Sectional Relative Strength vs BTC.
        """
        dataframe["enter_long"] = 0
        dataframe["enter_tag"] = None

        # 1. Macro Trend Alignment: 4H EMA 50 > EMA 200 AND Daily BTC Close > Daily BTC EMA 50
        uptrend_structure = dataframe["ema50"] > dataframe["ema200"]
        btc_filter = dataframe["btc_close_1d"] > dataframe["btc_ema50_1d"]
        positive_volume = dataframe["volume"] > 0

        # 2. Volatility Keltner Breakout: Close > EMA 20 + 1.8 * ATR
        keltner_trigger = dataframe["close"] > dataframe["keltner_upper"]

        # 3. Cross-Sectional Relative Strength vs BTC: Altcoin must outperform BTC by >= +2.0% over 7 days
        relative_strength = dataframe["roc_42"] > (dataframe["btc_roc_42"] + 2.0)

        # 4. Momentum Velocity & Volume Expansion
        adx_filter = (dataframe["adx"] > 20.0) if "adx" in dataframe.columns else Series(True, index=dataframe.index)
        rsi_filter = (dataframe["rsi"] >= 50.0) & (dataframe["rsi"] <= 72.0)
        volume_filter = dataframe["volume"] > (1.2 * dataframe["volume_mean_20"])

        cond_breakout = (
            uptrend_structure &
            btc_filter &
            positive_volume &
            keltner_trigger &
            relative_strength &
            adx_filter &
            rsi_filter &
            volume_filter
        )

        if self.enable_breakout:
            dataframe.loc[cond_breakout, "enter_long"] = 1
            dataframe.loc[cond_breakout, "enter_tag"] = "keltner_breakout"

        return dataframe

    def confirm_trade_entry(
        self,
        pair: str,
        order_type: str,
        amount: float,
        rate: float,
        time_in_force: str,
        current_time: datetime,
        entry_tag: Optional[str],
        **kwargs
    ) -> bool:
        if self.is_live_or_dryrun() and not self._reconciliation_passed:
            logger.warning(f"Rejecting entry on {pair}: Startup reconciliation not passed.")
            return False

        current_equity = self._get_current_equity()
        if current_equity <= 0:
            logger.warning(f"Rejecting entry on {pair}: Validated equity unavailable ({current_equity}). Failing closed.")
            return False

        timestamp_ms = int(current_time.timestamp() * 1000)
        can_enter, reason, is_halt = self.guards.check_entry_allowed(pair, timestamp_ms, current_equity)
        if not can_enter:
            if self.is_live_or_dryrun() and reason:
                self.telemetry.log_event("guard_blocked", "info", f"Entry rejected on {pair}: {reason}")
            logger.info(f"Entry suppressed on {pair} at {current_time}: {reason}")
            return False

        return True

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Macro Trend Exit: Closed 4h candle below 4h EMA 50.
        """
        dataframe["exit_long"] = 0
        trend_breakdown = (
            (dataframe["close"] < dataframe["ema50"]) &
            (dataframe["volume"] > 0)
        )
        dataframe.loc[trend_breakdown, "exit_long"] = 1
        return dataframe

    def custom_exit(
        self,
        pair: str,
        trade: Trade,
        current_time: datetime,
        current_rate: float,
        current_profit: float,
        **kwargs
    ) -> Optional[str]:
        """
        Time Stagnation Exit: Exit after 10 days (240h) if trade has made < +1.0% progress.
        """
        open_date = getattr(trade, "open_date_utc", None)
        if open_date and isinstance(open_date, datetime):
            trade_duration_hours = (current_time - open_date).total_seconds() / 3600.0
            if trade_duration_hours >= self.STAGNATION_HOURS and current_profit < self.STAGNATION_MIN_PROFIT:
                return "time_stagnation"

        return None

    def custom_stoploss(
        self,
        pair: str,
        trade: Trade,
        current_time: datetime,
        current_rate: float,
        current_profit: float,
        after_fill: bool,
        **kwargs
    ) -> Optional[float]:
        """
        True Chandelier Trailing Stop:
        1. Below +4.0% profit: Initial -6.0% hard stop handles normal 4h noise.
        2. Once current_profit >= +4.0%: Ratchet stop to +0.5% above entry (fee-free lock).
        3. For trailing winners above +6.0%: Calculate Chandelier stop anchored to trade.max_rate:
           chandelier_stop_price = trade.max_rate * (1.0 - 3.0 * atr_pct)
           Converted via stoploss_from_absolute().
        """
        if current_profit < 0.040:
            return None

        # Lock in at least +0.5% above entry price once trade reaches +4.0%
        be_stop = stoploss_from_open(0.005, current_profit)

        # For runners above +6.0%, anchor trailing stop to trade.max_rate (True Chandelier Exit)
        if current_profit >= 0.060:
            atr_pct = 0.03  # Fallback 3.0%
            if hasattr(self, "dp") and self.dp:
                try:
                    analyzed_df, _ = self.dp.get_analyzed_dataframe(pair, self.timeframe)
                    if analyzed_df is not None and not analyzed_df.empty and "atr" in analyzed_df.columns:
                        last_atr = float(analyzed_df["atr"].iloc[-1])
                        if current_rate > 0 and last_atr > 0:
                            atr_pct = last_atr / current_rate
                except Exception:
                    pass

            max_rate = getattr(trade, "max_rate", current_rate) or current_rate
            chandelier_stop_price = max_rate * (1.0 - 3.0 * atr_pct)
            chandelier_stop = stoploss_from_absolute(chandelier_stop_price, current_rate, is_short=False, leverage=1.0)

            chosen_dist = min(be_stop, chandelier_stop)
            return -chosen_dist

        return -be_stop

    def custom_stake_amount(
        self,
        pair: str,
        current_time: datetime,
        current_rate: float,
        proposed_stake: float,
        min_stake: Optional[float],
        max_stake: float,
        leverage: float,
        entry_tag: Optional[str],
        side: str,
        **kwargs
    ) -> float:
        """
        Tactical Sleeve mathematical risk sizing ($3,000 Sandbox):
        Stake = min((Total_Equity * 0.015) / 0.060, Total_Equity * 0.25, Available_Cash)
        """
        total_equity = self._get_current_equity()
        if total_equity <= 0:
            logger.warning(f"Skipping stake calculation on {pair}: Validated equity unavailable ({total_equity}). Failing closed.")
            return 0.0

        risk_budget = total_equity * self.RISK_FRACTION_PER_TRADE  # e.g., $3,000 * 0.015 = $45.00
        risk_based_stake = risk_budget / self.STOP_DISTANCE_FRACTION  # e.g., $45.00 / 0.060 = $750.00
        position_cap = total_equity * self.MAX_POSITION_EQUITY_RATIO  # e.g., $3,000 * 0.25 = $750.00

        available_balance = max_stake if max_stake > 0 else proposed_stake
        if hasattr(self, "wallets") and self.wallets:
            try:
                available_balance = float(self.wallets.get_available_stake_amount())
            except Exception:
                pass

        calculated_stake = min(risk_based_stake, position_cap, available_balance)

        if min_stake is not None and calculated_stake < min_stake:
            logger.warning(
                f"Calculated stake ${calculated_stake:.2f} on {pair} is below exchange minimum ${min_stake:.2f}. "
                f"Skipping trade to preserve risk limits."
            )
            return 0.0

        return round(calculated_stake, 2)

    def order_filled(self, pair: str, trade: Trade, order: Order, current_time: datetime, **kwargs) -> None:
        filled_amount = float(order.safe_filled or 0)
        fill_price = float(order.safe_price or 0)
        order_id = str(order.order_id or "").strip()

        if not order_id or filled_amount <= 0 or fill_price <= 0:
            return

        is_entry = (order.ft_order_side == trade.entry_side)
        event_type = "entry_fill" if is_entry else "exit_fill"
        event_id = f"bot:{self.bot_id}:{order_id}:{event_type}"
        filled_at = order.order_filled_utc or current_time
        timestamp_ms = int(filled_at.timestamp() * 1000)

        if not is_entry:
            profit_abs = getattr(trade, "close_profit_abs", 0.0) or 0.0
            self.guards.on_trade_closed(pair, float(profit_abs), timestamp_ms)

        if self.is_live_or_dryrun():
            payload = {
                "botId": self.bot_id,
                "orderId": order_id,
                "tradeId": trade.id,
                "pair": pair,
                "side": order.ft_order_side,
                "price": fill_price,
                "amount": filled_amount,
                "cost": filled_amount * fill_price,
                "timestamp": timestamp_ms,
                "strategy": self.__class__.__name__,
                "tag": getattr(trade, "enter_tag", None) if is_entry else getattr(trade, "exit_reason", None),
            }
            self.outbox.record_fill(event_id, payload)
            self.telemetry.log_event("order_filled", "info", f"Filled {order.ft_order_side} {filled_amount} {pair} @ {fill_price}")

    def _get_current_equity(self) -> float:
        is_live_trading = not bool(self.config.get("dry_run", True))
        if hasattr(self, "wallets") and self.wallets:
            try:
                total = float(self.wallets.get_total_stake_amount())
                if total > 0:
                    return total
            except Exception:
                pass

        if is_live_trading:
            logger.error("Live trading active but wallet equity unavailable! Failing closed.")
            return 0.0

        dry_wallet = self.config.get("dry_run_wallet")
        if dry_wallet is not None:
            try:
                val = float(dry_wallet)
                if val > 0:
                    return val
            except (ValueError, TypeError):
                pass
        return 0.0
