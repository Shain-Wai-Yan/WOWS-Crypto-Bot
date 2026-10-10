import os
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import requests

logger = logging.getLogger("WowsReconciliation")

@dataclass
class PositionSummary:
    symbol: str
    quantity: float
    avg_cost: float
    cost_basis_usd: float

@dataclass
class ReconciliationReport:
    in_sync: bool
    can_trade: bool
    status: str
    cash: float
    total_equity: float
    current_exposure: float
    max_exposure: float
    quarantine_count: int
    wows_positions: List[PositionSummary]
    discrepancy_reason: Optional[str] = None

class ReconciliationClient:
    """
    Performs startup and periodic parity verification between local Freqtrade
    trades and the authoritative WOWS paper-trading ledger.
    """
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        bot_id: Optional[str] = None,
        quantity_tolerance: float = 1e-6,
    ):
        self.base_url = (base_url or os.getenv("WOWS_BASE_URL", "http://localhost:3000")).rstrip("/")
        self.api_key = api_key or os.getenv("BOT_API_KEY", "")
        self.bot_id = bot_id or os.getenv("BOT_ID", "freqtrade-alpha-1")
        self.tolerance = quantity_tolerance

    def fetch_wows_state(self) -> Optional[Dict[str, Any]]:
        """Queries GET /api/webhooks/bot/positions."""
        url = f"{self.base_url}/api/webhooks/bot/positions"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        params = {"botId": self.bot_id}

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=5.0)
            if resp.status_code == 200:
                return resp.json()
            logger.error(f"Reconciliation query failed with HTTP {resp.status_code}: {resp.text}")
            return None
        except Exception as e:
            logger.error(f"Reconciliation network error: {e}")
            return None

    def reconcile(self, local_open_trades: List[Dict[str, Any]]) -> ReconciliationReport:
        """
        Compares local Freqtrade open trades with authoritative WOWS positions.
        Returns a ReconciliationReport with can_trade determination.
        """
        data = self.fetch_wows_state()
        if not data:
            return ReconciliationReport(
                in_sync=False,
                can_trade=False,
                status="unreachable",
                cash=0.0,
                total_equity=0.0,
                current_exposure=0.0,
                max_exposure=0.0,
                quarantine_count=0,
                wows_positions=[],
                discrepancy_reason="Could not reach WOWS reconciliation endpoint",
            )

        status = data.get("status", "running")
        account = data.get("account", {})
        limits = data.get("limits", {})
        quarantine_count = data.get("unresolvedQuarantineCount", 0)

        cash = float(account.get("cash", 0.0))
        total_equity = float(account.get("totalEquity", 0.0))
        current_exposure = float(account.get("currentExposure", 0.0))
        max_exposure = float(limits.get("maxAggregateExposure", 0.0))

        raw_positions = data.get("positions", [])
        wows_pos_map: Dict[str, PositionSummary] = {}
        for p in raw_positions:
            sym = p["symbol"]
            wows_pos_map[sym] = PositionSummary(
                symbol=sym,
                quantity=float(p.get("quantity", 0.0)),
                avg_cost=float(p.get("avgCostNative", 0.0)),
                cost_basis_usd=float(p.get("costBasisUsd", 0.0)),
            )

        # Build local map: symbol (normalized) -> total open amount
        local_pos_map: Dict[str, float] = {}
        for t in local_open_trades:
            # Pair e.g. 'SOL/USDT' -> 'SOLUSDT'
            clean_sym = t["pair"].replace("/", "").replace("-", "").replace("_", "").upper()
            local_pos_map[clean_sym] = local_pos_map.get(clean_sym, 0.0) + float(t.get("amount", 0.0))

        # Check 1: Quarantine operational gate
        if quarantine_count > 0 or status == "attention_required":
            reason = f"Unresolved quarantined events present ({quarantine_count}). Status: {status}."
            logger.warning(f"Reconciliation blocked: {reason}")
            return ReconciliationReport(
                in_sync=False,
                can_trade=False,
                status="attention_required",
                cash=cash,
                total_equity=total_equity,
                current_exposure=current_exposure,
                max_exposure=max_exposure,
                quarantine_count=quarantine_count,
                wows_positions=list(wows_pos_map.values()),
                discrepancy_reason=reason,
            )

        # Check 2: Position set parity
        all_symbols = set(wows_pos_map.keys()).union(set(local_pos_map.keys()))
        for sym in all_symbols:
            wows_qty = wows_pos_map[sym].quantity if sym in wows_pos_map else 0.0
            local_qty = local_pos_map.get(sym, 0.0)

            if abs(wows_qty - local_qty) > self.tolerance:
                reason = f"Quantity mismatch on {sym}: WOWS has {wows_qty}, Freqtrade has {local_qty} (diff: {abs(wows_qty - local_qty):.6f})"
                logger.error(f"Reconciliation mismatch: {reason}")
                return ReconciliationReport(
                    in_sync=False,
                    can_trade=False,
                    status="attention_required",
                    cash=cash,
                    total_equity=total_equity,
                    current_exposure=current_exposure,
                    max_exposure=max_exposure,
                    quarantine_count=quarantine_count,
                    wows_positions=list(wows_pos_map.values()),
                    discrepancy_reason=reason,
                )

        # Positions match cleanly
        logger.info(f"Reconciliation passed: {len(all_symbols)} positions in sync with WOWS.")
        return ReconciliationReport(
            in_sync=True,
            can_trade=(status == "running"),
            status=status,
            cash=cash,
            total_equity=total_equity,
            current_exposure=current_exposure,
            max_exposure=max_exposure,
            quarantine_count=0,
            wows_positions=list(wows_pos_map.values()),
            discrepancy_reason=None,
        )
