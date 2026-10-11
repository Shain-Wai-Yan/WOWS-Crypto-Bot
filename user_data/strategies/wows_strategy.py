import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from freqtrade.strategy import IStrategy
from freqtrade.persistence import Trade, Order
from pandas import DataFrame
import talib.abstract as ta

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

logger = logging.getLogger("WowsStrategy")

class WowsStrategy(IStrategy):
    """
    Production-ready Freqtrade Strategy for WOWS Paper Trading (Baseline Control).
    Invariants:
    1. Strictly dry_run = True.
    2. Signals evaluated strictly on completed closed candle bars.
    3. Fills durably persisted to SQLite outbox before returning.
    4. Two-tier risk guards with monotonic bar cooldowns.
    5. Non-blocking outbox worker and 60-second telemetry heartbeats.
    6. Startup reconciliation parity check before enabling entries.
    7. Clean research/backtest isolation: zero external network calls during backtesting.
    """
    INTERFACE_VERSION = 3

    # Timeframe & Indicators
    timeframe = "1h"
    process_only_new_candles = True
    startup_candle_count = 30

    # Risk & Protective Exits (managed within strategy)
    stoploss = -0.05
    use_custom_stoploss = False

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self.bot_id = os.getenv("BOT_ID", "freqtrade-alpha-1")
        self.outbox_db_path = os.getenv("SQLITE_DB_PATH", "/freqtrade/user_data/outbox.db")
        self.wows_base_url = os.getenv("WOWS_BASE_URL", "http://localhost:3000")
        self.bot_api_key = os.getenv("BOT_API_KEY", "")

        # Initialize subcomponents
        self.outbox = OutboxManager(self.outbox_db_path)
        self.dispatcher = DispatcherWorker(self.outbox, self.wows_base_url, self.bot_api_key)
        self.reconciliation = ReconciliationClient(self.wows_base_url, self.bot_api_key, self.bot_id)
        self.telemetry = TelemetryClient(self.wows_base_url, self.bot_api_key, self.bot_id, interval_sec=60.0)
        self.guards = RiskGuards(timeframe_minutes=60)

        self._reconciliation_passed = False

    def is_research_mode(self) -> bool:
        """Returns True if running in Backtest or Hyperopt mode."""
        try:
            from freqtrade.enums import RunMode
            runmode = self.config.get("runmode")
            if runmode in (RunMode.BACKTEST, RunMode.HYPEROPT):
                return True
        except ImportError:
            pass
        return str(self.config.get("runmode", "")).lower() in ("backtest", "hyperopt")

    def bot_start(self, **kwargs) -> None:
        """Freqtrade lifecycle hook: runs once when bot starts."""
        if self.is_research_mode():
            logger.info("WowsStrategy running in research/backtest mode: live network services bypassed.")
            self._reconciliation_passed = True
            return

        logger.info("Initializing WOWS Crypto Bot...")
        # Start background workers
        self.dispatcher.start()
        self.telemetry.start()

        # Step 1: Drain pending fills first
        self.dispatcher.process_pending_batch(limit=50)

        # Step 2: Startup Reconciliation against WOWS
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
            active_pairs=self.dp.current_whitelist() if hasattr(self, "dp") else [],
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
        """Runs each iteration of the bot loop."""
        if self.is_research_mode():
            return

        # Memory optimization for 512MB RAM containers:
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
                active_pairs=self.dp.current_whitelist() if hasattr(self, "dp") else [],
                open_trades_count=len(open_trades),
                current_exposure=exposure,
            )
        except Exception:
            pass

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # RSI Momentum indicators on closed candles
        dataframe["rsi"] = ta.RSI(dataframe, timeperiod=14)
        dataframe["ema20"] = ta.EMA(dataframe, timeperiod=20)
        dataframe["ema50"] = ta.EMA(dataframe, timeperiod=50)
        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        """
        Vectorized, stateless candidate signal generation.
        Pure baseline RSI mean-reversion setup: RSI < 30 and Close > EMA20.
        """
        dataframe["enter_long"] = 0
        dataframe.loc[
            (
                (dataframe["rsi"] < 30) &
                (dataframe["close"] > dataframe["ema20"]) &
                (dataframe["volume"] > 0)
            ),
            "enter_long",
        ] = 1
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
        Evaluates reconciliation and guards trade-by-trade immediately before order placement.
        """
        # In live/dry-run mode, require healthy startup reconciliation
        if not self.is_research_mode() and not self._reconciliation_passed:
            logger.warning(f"Rejecting entry on {pair}: Startup reconciliation not passed.")
            return False

        # Chronological RiskGuards check
        timestamp_ms = int(current_time.timestamp() * 1000)
        can_enter, reason, is_halt = self.guards.check_entry_allowed(pair, timestamp_ms, 50000.0)
        if not can_enter:
            if not self.is_research_mode() and reason:
                self.telemetry.log_event("guard_blocked", "info", f"Entry suppressed on {pair}: {reason}")
            logger.info(f"Entry suppressed on {pair} at {current_time}: {reason}")
            return False

        return True

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe["exit_long"] = 0
        dataframe.loc[
            (
                (dataframe["rsi"] > 70) &
                (dataframe["volume"] > 0)
            ),
            "exit_long",
        ] = 1
        return dataframe

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

        # Update guard state on exits across all run modes
        if not is_entry:
            profit_abs = getattr(trade, "close_profit_abs", 0.0) or 0.0
            self.guards.on_trade_closed(pair, float(profit_abs), timestamp_ms)

        # External outbox and telemetry commits run strictly in live/dry-run mode
        if not self.is_research_mode():
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
            logger.info(f"Recorded {event_type} fill for {pair} ({order_id}) in durable outbox.")
            self.telemetry.log_event(
                "order_filled",
                "info",
                f"Filled {order.ft_order_side} {filled_amount} {pair} @ {fill_price}",
                {"tradeId": trade.id, "orderId": order_id},
            )
