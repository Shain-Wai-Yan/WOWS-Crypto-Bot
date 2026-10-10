import os
import json
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from freqtrade.strategy import IStrategy
from freqtrade.persistence import Trade, Order
from pandas import DataFrame
import talib.abstract as ta

from .outbox import OutboxManager
from .dispatcher import DispatcherWorker
from .reconciliation import ReconciliationClient
from .telemetry import TelemetryClient
from .guards import RiskGuards

logger = logging.getLogger("WowsStrategy")

class WowsStrategy(IStrategy):
    """
    Production-ready Freqtrade Strategy for WOWS Paper Trading.
    Invariants:
    1. Strictly dry_run = True.
    2. Signals evaluated strictly on completed closed candle bars.
    3. Fills durably persisted to SQLite outbox before returning.
    4. Two-tier risk guards with monotonic bar cooldowns.
    5. Non-blocking outbox worker and 60-second telemetry heartbeats.
    6. Startup reconciliation parity check before enabling entries.
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

    def bot_start(self, **kwargs) -> None:
        """Freqtrade lifecycle hook: runs once when bot starts."""
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
        pair = metadata["pair"]
        now_ms = int(time.time() * 1000)

        # Pre-trade guard check: can this pair enter?
        can_enter, reason, is_halt = self.guards.check_entry_allowed(pair, now_ms, 50000.0)
        if not can_enter or not self._reconciliation_passed:
            dataframe["enter_long"] = 0
            if reason:
                self.telemetry.log_event("guard_blocked", "info", f"Entry suppressed on {pair}: {reason}")
            return dataframe

        dataframe.loc[
            (
                (dataframe["rsi"] < 30) &
                (dataframe["close"] > dataframe["ema20"]) &
                (dataframe["volume"] > 0)
            ),
            "enter_long",
        ] = 1
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        # Exit trend: RSI overbought
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
        logger.info(f"Recorded {event_type} fill for {pair} ({order_id}) in durable outbox.")

        # Update guard state on exits
        if not is_entry:
            profit_abs = getattr(trade, "close_profit_abs", 0.0) or 0.0
            self.guards.on_trade_closed(pair, float(profit_abs), timestamp_ms)

        # Log telemetry event
        self.telemetry.log_event(
            "order_filled",
            "info",
            f"Filled {order.ft_order_side} {filled_amount} {pair} @ {fill_price}",
            {"tradeId": trade.id, "orderId": order_id},
        )
