"""
WOWS Crypto Bot v2: Multi-Setup Spot Strategy (WowsStrategyV2)
==============================================================
Designed for steady, risk-controlled paper-trading progress with enhanced
opportunity frequency while strictly controlling downside exposure.

Experiment V2-B Architecture:
1. Breakout-Only Engine (enable_trend_pullback = False).
2. 36-period Donchian High Lookback (shift(1).rolling(36).max()) for structural expansion.
3. Directional Velocity Filter: ADX(14) > 22.0.
4. Two-Tier Asymmetric Exits:
   - Initial stop: -4.0% hard stop.
   - Fast Breakout Failure Cut: -2.0% (custom_exit: 'breakout_failure').
   - Breakeven Lock: Ratchet stop to +0.3% (covers fees) when profit reaches +2.5%.
   - Dynamic Trailing Stop: 2.5 * ATR(14) from high (uncapped right-tail runners).
   - Closed-Candle Trend Exit: Close below 1h EMA 20 (populate_exit_trend).
   - Time Stagnation Exit: 48h with < +0.5% profit.
5. Zero lookahead bias: Breakout thresholds strictly exclude current candle via shift(1).
6. Informative Higher Timeframe: 4h BTC EMA 50 trend confirmation filter.
7. Mathematical Risk-Budget Sizing with Strict Equity Separation.
8. Production resilience (Live/Dry-run only): Outbox, Telemetry, Reconciliation, RiskGuards.
"""

# pyright: reportMissingImports=false

import os
import json
import time
import math
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, List

# Graceful imports to allow execution both within Freqtrade container and local IDE / test environments
try:
    from freqtrade.strategy import IStrategy, informative, merge_informative_pair, stoploss_from_open  # type: ignore
    from freqtrade.persistence import Trade, Order  # type: ignore
except ImportError:
    class IStrategy:  # type: ignore
        def __init__(self, config: dict = None) -> None:
            self.config = config or {}
            self.dp = None
            self.wallets = None
    class Trade:  # type: ignore
        pass
    class Order:  # type: ignore
        pass
    def informative(*args, **kwargs):  # type: ignore
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


class WowsStrategyV2(IStrategy):
    """
    WOWS Crypto Bot v2 Strategy Class (Experiment V2-B: Breakout Asymmetry & Consolidation Filter).
    """
    INTERFACE_VERSION = 3

    # Primary Timeframe & Execution Settings
    timeframe = "1h"
    inf_timeframe = "4h"
    process_only_new_candles = True
    startup_candle_count = 200  # Required for 200 EMA warm-up
    can_short = False
    enable_trend_pullback: bool = False
    enable_breakout: bool = True

    # Risk & Protective Exits (managed via Freqtrade engine + custom callbacks)
    stoploss = -0.04  # -4.0% Hard Initial Stop
    use_custom_stoploss = True

    # Minimal ROI table: Disabled/Ceiling only; custom_stoploss and custom_exit manage all exits
    minimal_roi = {
        "0": 1.0  # 100% ceiling - winners are uncapped to capture fat right-tail extensions
    }

    # Strategy Parameters
    RISK_FRACTION_PER_TRADE = 0.0025    # 0.25% equity risked per trade
    STOP_DISTANCE_FRACTION = 0.045      # 4.0% stop + 0.5% fee/slippage allowance
    MAX_POSITION_EQUITY_RATIO = 0.10    # 10% maximum portfolio allocation per position
    BREAKOUT_FAILURE_LOSS_PCT = -0.02   # Exit breakout at -2.0% if thesis fails
    STAGNATION_HOURS = 48.0             # Exit after 48h if trade has made no progress
    STAGNATION_MIN_PROFIT = 0.005       # +0.5% minimum threshold for stagnation

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self.bot_id = os.getenv("BOT_ID", "freqtrade-alpha-1")
        
        # Safe outbox path resolution
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
        self.enable_trend_pullback = strat_cfg.get("enable_trend_pullback", getattr(self, "enable_trend_pullback", False))
        self.enable_breakout = strat_cfg.get("enable_breakout", getattr(self, "enable_breakout", True))

        # Initialize durable subcomponents
        self.outbox = OutboxManager(self.outbox_db_path)
        self.dispatcher = DispatcherWorker(self.outbox, self.wows_base_url, self.bot_api_key)
        self.reconciliation = ReconciliationClient(self.wows_base_url, self.bot_api_key, self.bot_id)
        self.telemetry = TelemetryClient(self.wows_base_url, self.bot_api_key, self.bot_id, interval_sec=60.0)
        self.guards = RiskGuards(
            max_daily_loss_pct=0.03,      # 3.0% daily drawdown circuit breaker
            max_consecutive_losses=3,     # 3 consecutive losses
            cooldown_bars=5,              # 5-bar (5 hours) cooldown on losing pair
            timeframe_minutes=60,
        )

        self._reconciliation_passed = False

    def is_live_or_dryrun(self) -> bool:
        """
        Determines whether the strategy is executing in live/dry-run mode
        vs historical backtesting or hyperopt.
        """
        if hasattr(self, "dp") and self.dp and hasattr(self.dp, "runmode"):
            mode_val = getattr(self.dp.runmode, "value", str(self.dp.runmode)).lower()
            return mode_val in ("live", "dry_run")
        config_mode = str(self.config.get("runmode", "")).lower()
        if config_mode in ("backtest", "hyperopt", "plot", "util"):
            return False
        return bool(self.config.get("dry_run", True))

    def bot_start(self, **kwargs) -> None:
        """
        Freqtrade lifecycle hook: runs once when bot daemon starts.
        Enforces strict research/live separation: no network calls in backtest mode.
        """
        if not self.is_live_or_dryrun():
            logger.info("Strategy running in research/backtesting mode: live external services bypassed.")
            self._reconciliation_passed = True
            return

        logger.info("Initializing WOWS Crypto Bot v2 (Live/Dry-Run Runtime)...")
        # Start background workers
        self.dispatcher.start()
        self.telemetry.start()

        # Drain any pending fills from persistent queue
        self.dispatcher.process_pending_batch(limit=50)

        # Perform Startup Reconciliation against authoritative WOWS ledger
        local_trades = []
        try:
            open_trades = Trade.get_open_trades()
            local_trades = [{"pair": t.pair, "amount": t.amount} for t in open_trades]
        except Exception:
            local_trades = []

        report = self.reconciliation.reconcile(local_trades)
        self._reconciliation_passed = report.can_trade

        status_str = "running" if self._reconciliation_passed else "attention_required"
        self.telemetry.update_state(
            status=status_str,
            active_pairs=self.dp.current_whitelist() if hasattr(self, "dp") and self.dp else [],
            open_trades_count=len(local_trades),
            current_exposure=report.current_exposure,
            guard_state={
                "realizedToday": self.guards.realized_today,
                "consecutiveLosses": self.guards.consecutive_losses,
            },
        )

        if not self._reconciliation_passed:
            self.telemetry.log_event(
                "reconciliation_mismatch",
                "error",
                f"Startup reconciliation failed: {report.discrepancy_reason}",
            )
            logger.error(f"Startup reconciliation blocked trading: {report.discrepancy_reason}")

    def bot_loop_start(self, current_time: datetime, **kwargs) -> None:
        """Runs each iteration of the bot trading loop."""
        if not self.is_live_or_dryrun():
            return

        # Memory optimization for constrained container runtimes:
        self._loop_count = getattr(self, "_loop_count", 0) + 1
        if self._loop_count % 12 == 0:
            import gc
            gc.collect()
            try:
                import ctypes
                ctypes.CDLL("libc.so.6").malloc_trim(0)
            except Exception:
                pass

    def informative_pairs(self) -> List[Tuple[str, str]]:
        """Defines higher-timeframe informative pair requirements."""
        stake = self.config.get("stake_currency", "USDT")
        return [(f"BTC/{stake}", self.inf_timeframe)]

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Populates indicators on the primary timeframe (1h).
        Zero lookahead bias: rolling highs are strictly shifted by 1 candle.
        """
        # Ensure talib or fallback calculation
        if ta is not None:
            dataframe["ema20"] = ta.EMA(dataframe, timeperiod=20)
            dataframe["ema50"] = ta.EMA(dataframe, timeperiod=50)
            dataframe["ema200"] = ta.EMA(dataframe, timeperiod=200)
            dataframe["rsi"] = ta.RSI(dataframe, timeperiod=14)
            dataframe["adx"] = ta.ADX(dataframe, timeperiod=14)
            dataframe["atr"] = ta.ATR(dataframe, timeperiod=14)
        else:
            # Fallback for standalone/mock tests
            dataframe["ema20"] = dataframe["close"].ewm(span=20, adjust=False).mean()
            dataframe["ema50"] = dataframe["close"].ewm(span=50, adjust=False).mean()
            dataframe["ema200"] = dataframe["close"].ewm(span=200, adjust=False).mean()
            delta = dataframe["close"].diff()
            gain = delta.where(delta > 0, 0.0).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
            rs = gain / (loss + 1e-9)
            dataframe["rsi"] = 100 - (100 / (1 + rs))
            tr = DataFrame([
                dataframe["high"] - dataframe["low"],
                (dataframe["high"] - dataframe["close"].shift(1)).abs(),
                (dataframe["low"] - dataframe["close"].shift(1)).abs()
            ]).max(axis=0)
            dataframe["atr"] = tr.rolling(14).mean().fillna(dataframe["close"] * 0.02)
            dataframe["adx"] = Series(25.0, index=dataframe.index)

        # ATR percentage for dynamic trailing stop
        dataframe["atr_pct"] = dataframe["atr"] / dataframe["close"]

        # Volume baseline
        dataframe["volume_mean_20"] = dataframe["volume"].rolling(window=20).mean()

        # Rolling 36-candle high STRICTLY shifted by 1 candle to prevent lookahead bias
        dataframe["rolling_high_36"] = dataframe["high"].shift(1).rolling(window=36).max()
        # Keep rolling_high_20 for backwards-compatibility
        dataframe["rolling_high_20"] = dataframe["high"].shift(1).rolling(window=20).max()

        # Merge 4h BTC trend filter if DataProvider is available
        stake = self.config.get("stake_currency", "USDT")
        btc_pair = f"BTC/{stake}"

        if hasattr(self, "dp") and self.dp:
            try:
                inf_df = self.dp.get_pair_dataframe(pair=btc_pair, timeframe=self.inf_timeframe)
                if inf_df is not None and not inf_df.empty:
                    if ta is not None:
                        inf_df["ema50"] = ta.EMA(inf_df, timeperiod=50)
                    else:
                        inf_df["ema50"] = inf_df["close"].ewm(span=50, adjust=False).mean()

                    # Merge informative pair using Freqtrade standard utility
                    inf_df = inf_df.rename(columns={"close": "btc_close_4h", "ema50": "btc_ema50_4h"})
                    dataframe = merge_informative_pair(
                        dataframe, inf_df[["date", "btc_close_4h", "btc_ema50_4h"]],
                        self.timeframe, self.inf_timeframe, ffill=True
                    )
            except Exception as e:
                logger.warning(f"Could not merge 4h BTC informative data: {e}")

        # If BTC columns are absent, create neutral pass-through columns
        if "btc_close_4h" not in dataframe.columns or "btc_ema50_4h" not in dataframe.columns:
            dataframe["btc_close_4h"] = 1.0
            dataframe["btc_ema50_4h"] = 0.5  # Default to passing condition

        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Purely vectorized, stateless calculation of candidate entry signals for
        Setup A (Trend Pullback) and Setup B (Confirmed Breakout).
        Stateful risk gates (guards, reconciliation) are evaluated dynamically in confirm_trade_entry().
        """
        dataframe["enter_long"] = 0
        dataframe["enter_tag"] = None

        # Common market conditions
        uptrend_structure = dataframe["ema50"] > dataframe["ema200"]
        btc_filter = dataframe["btc_close_4h"] > dataframe["btc_ema50_4h"]
        positive_volume = dataframe["volume"] > 0

        # --- SETUP A: Trend Pullback ---
        reclaim_ema20 = (
            (dataframe["close"] > dataframe["ema20"]) &
            (
                (dataframe["low"] < dataframe["ema20"]) |
                (dataframe["close"].shift(1) < dataframe["ema20"].shift(1))
            )
        )
        rsi_pullback = (dataframe["rsi"] >= 40.0) & (dataframe["rsi"] <= 60.0)
        volume_pullback = dataframe["volume"] > dataframe["volume_mean_20"]

        cond_pullback = (
            uptrend_structure &
            btc_filter &
            positive_volume &
            reclaim_ema20 &
            rsi_pullback &
            volume_pullback
        )

        # --- SETUP B: Confirmed Breakout ---
        # 1. Close exceeds the rolling 36-candle high (shifted by 1 to exclude current candle)
        # 2. Directional velocity filter: ADX(14) > 22 (filters low-volatility churn)
        # 3. Broad uptrend on coin and BTC
        # 4. RSI 14 between 50 and 70 (strong momentum without being overbought > 70)
        # 5. Expansion volume exceeds 1.5x 20-candle average
        rolling_high = dataframe["rolling_high_36"] if "rolling_high_36" in dataframe.columns else dataframe.get("rolling_high_20", dataframe["close"])
        breakout_trigger = dataframe["close"] > rolling_high
        adx_filter = (dataframe["adx"] > 22.0) if "adx" in dataframe.columns else Series(True, index=dataframe.index)
        rsi_breakout = (dataframe["rsi"] >= 50.0) & (dataframe["rsi"] <= 70.0)
        volume_breakout = dataframe["volume"] > (1.5 * dataframe["volume_mean_20"])

        cond_breakout = (
            uptrend_structure &
            btc_filter &
            positive_volume &
            breakout_trigger &
            adx_filter &
            rsi_breakout &
            volume_breakout
        )

        # Apply signals and tags (Breakout prioritized if both trigger simultaneously)
        if self.enable_trend_pullback:
            dataframe.loc[cond_pullback, "enter_long"] = 1
            dataframe.loc[cond_pullback, "enter_tag"] = "trend_pullback"

        if self.enable_breakout:
            dataframe.loc[cond_breakout, "enter_long"] = 1
            dataframe.loc[cond_breakout, "enter_tag"] = "breakout"

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
        """
        Freqtrade dynamic pre-trade gate.
        Called trade-by-trade immediately before order placement in both backtesting and live execution.
        Evaluates RiskGuards and reconciliation status chronologically at current_time.
        Note: Exits are NEVER blocked here; only new entries are gated.
        """
        # 1. In live/dry-run, require healthy startup reconciliation
        if self.is_live_or_dryrun() and not self._reconciliation_passed:
            logger.warning(f"Rejecting entry on {pair}: Startup reconciliation not passed.")
            return False

        # 2. Validate reliable equity (fail closed if unavailable)
        current_equity = self._get_current_equity()
        if current_equity <= 0:
            logger.warning(f"Rejecting entry on {pair}: Validated equity unavailable ({current_equity}). Failing closed.")
            return False

        # 3. Chronological RiskGuards check at the candle's timestamp
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
        Populates closed-candle trend breakdown exit.
        Exits when candle closes below 1h EMA 20.
        Remains operational regardless of reconciliation or entry guard state.
        """
        dataframe["exit_long"] = 0

        # Confirmed breakdown: closed candle below 20 EMA
        trend_breakdown = (
            (dataframe["close"] < dataframe["ema20"]) &
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
        Protective and failure custom exit conditions.
        NOTE: Hard profit ceiling (+3.5%) is REMOVED to allow fat right-tail winners to run.
        Winners are managed dynamically via custom_stoploss (breakeven lock & ATR trailing)
        and populate_exit_trend (1h EMA 20 breakdown).
        """
        # 1. Breakout Failure Cut (-2.0%)
        # If price falls 2.0% below entry on a breakout trade, exit immediately to cut losers early
        if trade.enter_tag == "breakout" and current_profit <= self.BREAKOUT_FAILURE_LOSS_PCT:
            return "breakout_failure"

        # 2. Time Stagnation Exit
        # Free capital if position makes no progress after 48 hours
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
        Two-Tier Asymmetric Stoploss:
        1. Below +2.5% profit: Keep initial stop loss intact (-4.0% hard stop).
        2. Once trade reaches +2.5% profit:
           - Ratchet stop to +0.3% above entry price (breakeven lock covering fee friction).
           - Trail at 2.5 * ATR(14) below current_rate as profit expands further.
        """
        if current_profit < 0.025:
            return None

        # Breakeven lock distance (+0.3% above entry price)
        be_stop = stoploss_from_open(0.003, current_profit)

        # Dynamic ATR trailing stop
        atr_pct = 0.015  # Fallback 1.5% if analyzed dataframe is inaccessible
        if hasattr(self, "dp") and self.dp:
            try:
                analyzed_df, _ = self.dp.get_analyzed_dataframe(pair, self.timeframe)
                if not analyzed_df.empty and "atr_pct" in analyzed_df.columns:
                    val = analyzed_df["atr_pct"].iloc[-1]
                    if val is not None and not (math.isnan(val) or math.isinf(val)) and val > 0:
                        atr_pct = float(val)
            except Exception:
                pass

        # 2.5 * ATR(14), bounded between 2.0% and 7.0%
        atr_dist = max(0.02, min(0.07, 2.5 * atr_pct))

        # We choose the tighter stop (smaller distance from current rate = higher stop price)
        chosen_dist = min(be_stop, atr_dist)
        return -chosen_dist

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
        Calculates mathematical risk-budget stake size:
        Stake = min((Total_Equity * Risk_fraction) / Stop_distance_fraction, Position_cap, Available_cash)
        """
        total_equity = self._get_current_equity()
        if total_equity <= 0:
            logger.warning(f"Skipping stake calculation on {pair}: Validated equity unavailable ({total_equity}). Failing closed.")
            return 0.0

        # Calculate risk dollar budget based on total portfolio equity
        risk_budget = total_equity * self.RISK_FRACTION_PER_TRADE  # e.g., $1000 * 0.0025 = $2.50
        risk_based_stake = risk_budget / self.STOP_DISTANCE_FRACTION  # e.g., $2.50 / 0.045 = $55.55

        # Maximum position cap (10% of total portfolio equity)
        position_cap = total_equity * self.MAX_POSITION_EQUITY_RATIO

        # Available capital cap: bound by max_stake and actual uncommitted cash in wallet
        available_balance = max_stake if max_stake > 0 else proposed_stake
        if hasattr(self, "wallets") and self.wallets:
            try:
                free_cash = float(self.wallets.get_available_stake_amount())
                if free_cash > 0:
                    available_balance = min(available_balance, free_cash)
            except Exception:
                pass

        # Final allocated stake is bounded by risk budget, position cap, and available cash
        calculated_stake = min(risk_based_stake, position_cap, available_balance)

        # Exchange minimum stake safety gate: DO NOT silently inflate stake
        if min_stake is not None and calculated_stake < min_stake:
            logger.warning(
                f"Calculated stake ${calculated_stake:.2f} on {pair} is below exchange minimum ${min_stake:.2f}. "
                f"Skipping trade to preserve risk limits."
            )
            return 0.0

        return round(calculated_stake, 2)

    def order_filled(self, pair: str, trade: Trade, order: Order, current_time: datetime, **kwargs) -> None:
        """
        Freqtrade hook called on order fill.
        Extracts fields via documented Order attributes.
        Durably commits to SQLite outbox only during live/dry-run execution.
        Updates RiskGuards on exit fills in both backtesting and live runtimes.
        """
        filled_amount = float(order.safe_filled or 0)
        fill_price = float(order.safe_price or 0)
        filled_cost = float(order.safe_cost or (filled_amount * fill_price))
        order_id = str(order.order_id or "").strip()

        if not order_id or filled_amount <= 0 or fill_price <= 0:
            logger.warning(f"Rejecting invalid fill for {pair}: order_id={order_id}, qty={filled_amount}, price={fill_price}")
            return

        is_entry = (order.ft_order_side == trade.entry_side)
        event_type = "entry_fill" if is_entry else "exit_fill"
        event_id = f"{self.bot_id}:{trade.id}:{order_id}:{event_type}"
        filled_at = order.order_filled_utc or current_time
        timestamp_ms = int(filled_at.timestamp() * 1000)

        # Update guard state on exits (applies to both backtests and live execution)
        if not is_entry:
            profit_abs = getattr(trade, "close_profit_abs", 0.0) or 0.0
            self.guards.on_trade_closed(pair, float(profit_abs), timestamp_ms)

        # Live/dry-run external services integration
        if self.is_live_or_dryrun():
            payload = {
                "botId": self.bot_id,
                "tradeId": trade.id,
                "orderId": order_id,
                "eventType": event_type,
                "isEntry": is_entry,
                "pair": pair,
                "side": order.ft_order_side,
                "filledAmount": filled_amount,
                "fillPrice": fill_price,
                "cost": filled_cost,
                "timestamp": timestamp_ms,
                "strategy": self.__class__.__name__,
                "tag": getattr(trade, "enter_tag", None) if is_entry else getattr(trade, "exit_reason", None),
            }

            self.outbox.record_fill(event_id, payload)
            logger.info(f"Recorded {event_type} fill for {pair} ({order_id}) in durable outbox. Tag: {payload['tag']}")

            self.telemetry.log_event(
                "order_filled",
                "info",
                f"Filled {order.ft_order_side} {filled_amount} {pair} @ {fill_price} [{payload['tag']}]",
                {"tradeId": trade.id, "orderId": order_id, "tag": payload["tag"]},
            )

    def _get_current_equity(self) -> float:
        """
        Retrieves validated current equity strictly by execution mode:
        1. Live Mode (dry_run=False):
           MUST strictly use verified exchange wallet equity (`self.wallets.get_total_stake_amount()`).
           NEVER falls back to dry_run_wallet or simulated values.
           If verified live equity is unavailable, returns 0.0 (fails closed).
        2. Dry-Run / Backtest Mode:
           Uses simulated wallet equity if available, or configured dry_run_wallet/available_capital.
        """
        is_live_trading = not bool(self.config.get("dry_run", True))

        # Check wallet equity first across all modes
        if hasattr(self, "wallets") and self.wallets:
            try:
                total = float(self.wallets.get_total_stake_amount())
                if total > 0:
                    return total
            except Exception:
                pass

        # In LIVE trading, FAIL CLOSED immediately if wallet equity is unavailable
        if is_live_trading:
            logger.error("Live trading mode active but verified exchange wallet equity is unavailable! Failing closed.")
            return 0.0

        # In DRY-RUN or BACKTEST modes, simulated balance fallbacks are permitted
        dry_wallet = self.config.get("dry_run_wallet")
        if dry_wallet is not None:
            try:
                val = float(dry_wallet)
                if val > 0:
                    return val
            except (ValueError, TypeError):
                pass

        avail = self.config.get("available_capital")
        if avail is not None:
            try:
                val = float(avail)
                if val > 0:
                    return val
            except (ValueError, TypeError):
                pass

        return 0.0
