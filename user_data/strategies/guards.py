import time
from typing import Dict, Any, Optional, Tuple

class RiskGuards:
    """
    Two-tier pre-trade risk guard engine.
    Differentiates Halting limits (stops trading entirely) from
    Throttling limits (skips single signal).

    Supports chronological time-advancement for both live execution and backtesting.
    """
    def __init__(
        self,
        max_daily_loss_pct: float = 0.03,      # 3.0% max daily drawdown (aligned across specs)
        max_consecutive_losses: int = 3,         # 3 consecutive losses
        cooldown_bars: int = 5,                  # 5 closed bars cooldown after loss
        timeframe_minutes: int = 60,             # 1h candle = 60 mins
    ):
        self.max_daily_loss_pct = max_daily_loss_pct
        self.max_consecutive_losses = max_consecutive_losses
        self.cooldown_bars = cooldown_bars
        self.timeframe_ms = timeframe_minutes * 60 * 1000

        # Mutable state
        self.current_day_index: Optional[int] = None
        self.day_start_equity: float = 0.0
        self.realized_today: float = 0.0
        self.consecutive_losses: int = 0
        self.pair_last_loss_bar: Dict[str, int] = {}
        self.is_halted: bool = False
        self.halt_reason: Optional[str] = None

    def bar_index_of(self, timestamp_ms: int) -> int:
        """Monotonic absolute candle index derived from timestamp."""
        return timestamp_ms // self.timeframe_ms

    def _rollover_day_if_needed(self, current_timestamp_ms: int, current_equity: float) -> None:
        """Chronologically rolls over daily tracking when UTC day advances."""
        day_index = current_timestamp_ms // (86400 * 1000)
        if self.current_day_index is None:
            self.current_day_index = day_index
            self.day_start_equity = current_equity
        elif day_index > self.current_day_index:
            self.current_day_index = day_index
            self.realized_today = 0.0
            self.day_start_equity = current_equity
            if self.is_halted and "Daily loss" in (self.halt_reason or ""):
                self.is_halted = False
                self.halt_reason = None

    def check_entry_allowed(self, pair: str, current_timestamp_ms: int, current_equity: float) -> Tuple[bool, Optional[str], bool]:
        """
        Evaluates pre-trade guards.
        Returns: (allowed, reason, is_halting)
        Fails closed if current_equity <= 0.
        """
        # Fail closed on non-positive / missing equity
        if current_equity <= 0 and self.day_start_equity <= 0:
            return False, f"Invalid equity ({current_equity}): cannot evaluate risk limits safely", True

        # Advance chronological day tracking
        self._rollover_day_if_needed(current_timestamp_ms, current_equity)

        if self.is_halted:
            return False, f"Bot is halted: {self.halt_reason}", True

        # 1. Halting: Daily loss cap (3.0% of starting daily equity)
        base_equity = self.day_start_equity if self.day_start_equity > 0 else current_equity
        if base_equity > 0:
            allowed_loss = base_equity * self.max_daily_loss_pct
            if self.realized_today <= -allowed_loss:
                self.is_halted = True
                self.halt_reason = f"Daily loss limit hit: {self.realized_today:.2f} <= -{allowed_loss:.2f} (cap: {self.max_daily_loss_pct*100:.1f}%)"
                return False, self.halt_reason, True

        # 2. Halting: Consecutive losses
        if self.consecutive_losses >= self.max_consecutive_losses:
            self.is_halted = True
            self.halt_reason = f"Consecutive loss streak hit: {self.consecutive_losses} >= {self.max_consecutive_losses}"
            return False, self.halt_reason, True

        # 3. Throttling: Pair cooldown bars
        current_bar = self.bar_index_of(current_timestamp_ms)
        last_loss_bar = self.pair_last_loss_bar.get(pair)
        if last_loss_bar is not None:
            elapsed = current_bar - last_loss_bar
            if elapsed < self.cooldown_bars:
                remaining = self.cooldown_bars - elapsed
                return False, f"Cooldown active on {pair}: {remaining} bars remaining", False

        return True, None, False

    def on_trade_closed(self, pair: str, pnl: float, close_timestamp_ms: int) -> None:
        """Updates rolling guard counters when a trade closes."""
        self.realized_today += pnl
        if pnl < 0:
            self.consecutive_losses += 1
            self.pair_last_loss_bar[pair] = self.bar_index_of(close_timestamp_ms)
        else:
            self.consecutive_losses = 0

    def reset_daily_if_new_day(self, current_day_start_equity: float) -> None:
        """Manual reset for daily loss tracking."""
        self.realized_today = 0.0
        self.day_start_equity = current_day_start_equity
        if self.is_halted and "Daily loss" in (self.halt_reason or ""):
            self.is_halted = False
            self.halt_reason = None
