"""
WOWS Crypto Bot v2: Multi-Setup Spot Strategy (WowsStrategyV2)
==============================================================
Designed for steady, risk-controlled paper-trading progress with enhanced
opportunity frequency while strictly controlling downside exposure.

Core Invariants:
1. Strictly dry_run = True, Spot long-only, Zero leverage.
2. Multi-setup architecture:
   - Setup A (Trend Pullback): Enters healthy retracements in confirmed uptrends.
   - Setup B (Confirmed Breakout): Enters volatility expansions above 20-candle high.
3. Zero lookahead bias: Breakout thresholds strictly exclude current candle via shift(1).
4. Informative Higher Timeframe: 4h BTC EMA 50 trend confirmation filter.
5. Volatility & Risk-Budget Position Sizing:
   Stake = min((Equity * 0.0025) / 0.045, Equity * 0.10, Available_balance)
   Skips trade if calculated stake is below exchange min_stake (no silent clamping).
6. Multi-stage exits:
   - Hard maximum stop: -4.0%
   - Breakout failure cutoff: -2.0%
   - Profit target: +3.5%
   - Time stagnation: 48 hours with < +0.5% progress
   - Trend breakdown: 2 consecutive closes below EMA 20
7. Production resilience:
   - Durable thread-safe SQLite outbox with SHA256 integrity and stable event IDs.
   - 60-second telemetry heartbeats to WOWS backend.
   - Startup reconciliation parity gate (blocks entries if discrepancies or quarantines exist).
   - Two-tier RiskGuards (daily drawdown limit, loss streaks, per-pair cooldowns).
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, List

# Graceful imports to allow execution both within Freqtrade and in standalone test environments
try:
    from freqtrade.strategy import IStrategy, informative, merge_informative_pair
    from freqtrade.persistence import Trade, Order
except ImportError:
    class IStrategy:
        def __init__(self, config: dict = None) -> None:
            self.config = config or {}
    class Trade:
        pass
    class Order:
        pass
    def informative(*args, **kwargs):
        def decorator(f):
            return f
        return decorator
    def merge_informative_pair(dataframe, informative, timeframe, inf_timeframe, ffill=True):
        return dataframe

try:
    from pandas import DataFrame, Series
except ImportError:
    DataFrame = Any
    Series = Any

try:
    import talib.abstract as ta
except ImportError:
    ta = None

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

logger = logging.getLogger("WowsStrategyV2")


class WowsStrategyV2(IStrategy):
    """
    WOWS Crypto Bot v2 Strategy Class.
    """
    INTERFACE_VERSION = 3

    # Primary Timeframe & Execution Settings
    timeframe = "1h"
    inf_timeframe = "4h"
    process_only_new_candles = True
    startup_candle_count = 200  # Required for 200 EMA warm-up
    can_short = False

    # Risk & Protective Exits (managed via Freqtrade engine + custom callbacks)
    stoploss = -0.04  # -4.0% Hard Initial Stop
    use_custom_stoploss = False

    # Minimal ROI table (custom_exit handles discretionary / dynamic targets)
    minimal_roi = {
        "0": 0.10  # 10% safety ceiling, custom_exit triggers targets sooner
    }

    # Strategy Parameters
    RISK_FRACTION_PER_TRADE = 0.0025    # 0.25% equity risked per trade
    STOP_DISTANCE_FRACTION = 0.045      # 4.0% stop + 0.5% fee/slippage allowance
    MAX_POSITION_EQUITY_RATIO = 0.10    # 10% maximum portfolio allocation per position
    BREAKOUT_FAILURE_LOSS_PCT = -0.02   # Exit breakout at -2% if thesis fails
    PROFIT_TARGET_PCT = 0.035           # +3.5% take profit target
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

        # Initialize durable subcomponents
        self.outbox = OutboxManager(self.outbox_db_path)
        self.dispatcher = DispatcherWorker(self.outbox, self.wows_base_url, self.bot_api_key)
        self.reconciliation = ReconciliationClient(self.wows_base_url, self.bot_api_key, self.bot_id)
        self.telemetry = TelemetryClient(self.wows_base_url, self.bot_api_key, self.bot_id, interval_sec=60.0)
        self.guards = RiskGuards(
            max_daily_loss_pct=0.03,      # 3% daily drawdown circuit breaker
            max_consecutive_losses=3,     # 3 consecutive losses
            cooldown_bars=5,              # 5-bar (5 hours) cooldown on losing pair
            timeframe_minutes=60,
        )

        self._reconciliation_passed = False

    def bot_start(self, **kwargs) -> None:
        """Freqtrade lifecycle hook: runs once when bot daemon starts."""
        logger.info("Initializing WOWS Crypto Bot v2 (Multi-Setup)...")
        # Start background workers
        self.dispatcher.start()
        self.telemetry.start()

        # Step 1: Drain any pending fills from persistent queue
        self.dispatcher.process_pending_batch(limit=50)

        # Step 2: Perform Startup Reconciliation against authoritative WOWS ledger
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

        # Update telemetry state snapshot
        try:
            open_trades = Trade.get_open_trades()
            exposure = sum(float(t.stake_amount) for t in open_trades)
            status_str = "running" if (self._reconciliation_passed and not self.guards.is_halted) else (
                "halted" if self.guards.is_halted else "attention_required"
            )
            self.telemetry.update_state(
                status=status_str,
                active_pairs=self.dp.current_whitelist() if hasattr(self, "dp") and self.dp else [],
                open_trades_count=len(open_trades),
                current_exposure=exposure,
            )
        except Exception:
            pass

    def informative_pairs(self) -> List[Tuple[str, str]]:
        """Defines higher-timeframe informative pair requirements."""
        stake = self.config.get("stake_currency", "USDT")
        return [(f"BTC/{stake}", self.inf_timeframe)]

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """Calculates vectorized technical indicators on 1h candles + merges 4h BTC filter."""
        # Ensure talib or fallback calculation
        if ta is not None:
            dataframe["ema20"] = ta.EMA(dataframe, timeperiod=20)
            dataframe["ema50"] = ta.EMA(dataframe, timeperiod=50)
            dataframe["ema200"] = ta.EMA(dataframe, timeperiod=200)
            dataframe["rsi"] = ta.RSI(dataframe, timeperiod=14)
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

        # Volume baseline
        dataframe["volume_mean_20"] = dataframe["volume"].rolling(window=20).mean()

        # Rolling 20-candle high STRICTLY shifted by 1 candle to prevent lookahead bias
        # At candle t, compares against max(high[t-20 : t-1]), NEVER high[t]
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
        Populates entry signals for Setup A (Trend Pullback) and Setup B (Breakout)
        with explicit enter_tag tagging.
        """
        pair = metadata["pair"]
        now_ms = int(time.time() * 1000)

        # Pre-trade guard check: verify bot health, drawdown limits, and pair cooldown
        current_equity = self._get_current_equity()
        can_enter, reason, is_halt = self.guards.check_entry_allowed(pair, now_ms, current_equity)
        if not can_enter or not self._reconciliation_passed:
            dataframe["enter_long"] = 0
            if reason:
                self.telemetry.log_event("guard_blocked", "info", f"Entry suppressed on {pair}: {reason}")
            return dataframe

        # Initialize signal columns
        dataframe["enter_long"] = 0
        dataframe["enter_tag"] = None

        # Common market conditions
        uptrend_structure = dataframe["ema50"] > dataframe["ema200"]
        btc_filter = dataframe["btc_close_4h"] > dataframe["btc_ema50_4h"]
        positive_volume = dataframe["volume"] > 0

        # --- SETUP A: Trend Pullback ---
        # 1. Broad uptrend on coin and BTC
        # 2. Candle closes back above EMA 20 after pulling back into/below it
        # 3. RSI 14 between 40 and 60 (healthy consolidation without collapse)
        # 4. Volume exceeds 20-candle average
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
        # 1. Close exceeds the rolling 20-candle high (shifted by 1 to exclude current candle)
        # 2. Broad uptrend on coin and BTC
        # 3. RSI 14 between 50 and 70 (strong momentum without being overbought > 70)
        # 4. Expansion volume exceeds 1.5x 20-candle average
        breakout_trigger = dataframe["close"] > dataframe["rolling_high_20"]
        rsi_breakout = (dataframe["rsi"] >= 50.0) & (dataframe["rsi"] <= 70.0)
        volume_breakout = dataframe["volume"] > (1.5 * dataframe["volume_mean_20"])

        cond_breakout = (
            uptrend_structure &
            btc_filter &
            positive_volume &
            breakout_trigger &
            rsi_breakout &
            volume_breakout
        )

        # Apply signals and tags (Breakout prioritized if both trigger simultaneously)
        dataframe.loc[cond_pullback, "enter_long"] = 1
        dataframe.loc[cond_pullback, "enter_tag"] = "trend_pullback"

        dataframe.loc[cond_breakout, "enter_long"] = 1
        dataframe.loc[cond_breakout, "enter_tag"] = "breakout"

        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Populates baseline trend breakdown exit.
        Exits when candle closes below EMA 20 with confirmation from prior candle.
        """
        dataframe["exit_long"] = 0

        # Confirmed breakdown below 20 EMA
        trend_breakdown = (
            (dataframe["close"] < dataframe["ema20"]) &
            (dataframe["close"].shift(1) < dataframe["ema20"].shift(1)) &
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
        Trade-specific dynamic exit callback.
        Enforces:
        - Profit target (+3.5%)
        - Breakout failure cutoff (-2.0%)
        - Time stagnation (48h with < +0.5% profit)
        """
        # 1. Target Take Profit
        if current_profit >= self.PROFIT_TARGET_PCT:
            return "profit_target"

        # 2. Breakout Failure Protection
        # If entered via breakout and price reverses -2%, exit before hitting 4% stoploss
        if trade.enter_tag == "breakout" and current_profit <= self.BREAKOUT_FAILURE_LOSS_PCT:
            return "breakout_failure"

        # 3. Time Stagnation Exit
        # Free capital if position makes no progress after 48 hours
        open_date = getattr(trade, "open_date_utc", None)
        if open_date and isinstance(open_date, datetime):
            trade_duration_hours = (current_time - open_date).total_seconds() / 3600.0
            if trade_duration_hours >= self.STAGNATION_HOURS and current_profit < self.STAGNATION_MIN_PROFIT:
                return "time_stagnation"

        return None

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
        Calculates volatility/risk-budget stake size:
        Stake = min((Equity * Risk_fraction) / Stop_distance_fraction, Position_cap, Available_balance)

        Invariants:
        - Rejects trade (returns 0.0) if calculated stake is less than exchange min_stake.
        - Never exceeds 10% maximum portfolio allocation per position.
        """
        total_equity = self._get_current_equity()
        if total_equity <= 0:
            return proposed_stake

        # Calculate risk budget
        risk_budget = total_equity * self.RISK_FRACTION_PER_TRADE  # e.g., $1000 * 0.0025 = $2.50
        risk_based_stake = risk_budget / self.STOP_DISTANCE_FRACTION  # e.g., $2.50 / 0.045 = $55.55

        # Maximum notional cap (10% of portfolio)
        position_cap = total_equity * self.MAX_POSITION_EQUITY_RATIO

        # Available capital cap
        available_balance = max_stake if max_stake > 0 else proposed_stake

        # Sizing formula
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
        Documented Freqtrade hook called on order fill.
        Extracts fields via documented Order attributes and commits to SQLite outbox.
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

        # Thread-safe outbox insert (< 1ms, zero network latency on strategy loop)
        self.outbox.record_fill(event_id, payload)
        logger.info(f"Recorded {event_type} fill for {pair} ({order_id}) in durable outbox. Tag: {payload['tag']}")

        # Update guard state on exits
        if not is_entry:
            profit_abs = getattr(trade, "close_profit_abs", 0.0) or 0.0
            self.guards.on_trade_closed(pair, float(profit_abs), timestamp_ms)

        # Log telemetry event
        self.telemetry.log_event(
            "order_filled",
            "info",
            f"Filled {order.ft_order_side} {filled_amount} {pair} @ {fill_price} [{payload['tag']}]",
            {"tradeId": trade.id, "orderId": order_id, "tag": payload["tag"]},
        )

    def _get_current_equity(self) -> float:
        """Helper to get current total equity from Freqtrade wallets or config."""
        if hasattr(self, "wallets") and self.wallets:
            try:
                return float(self.wallets.get_total_stake_amount())
            except Exception:
                pass
        return float(self.config.get("dry_run_wallet", 50000.0))
