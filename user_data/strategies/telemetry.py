import os
import time
import queue
import threading
import logging
from typing import Optional, List, Dict, Any
import requests

logger = logging.getLogger("WowsTelemetry")

class TelemetryClient:
    """
    Non-blocking 60-second heartbeat and operational event reporter.
    Sends heartbeats and audit logs to POST /api/webhooks/bot/telemetry.
    """
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        bot_id: Optional[str] = None,
        interval_sec: float = 60.0,
    ):
        self.base_url = (base_url or os.getenv("WOWS_BASE_URL", "http://localhost:3000")).rstrip("/")
        self.api_key = api_key or os.getenv("BOT_API_KEY", "")
        self.bot_id = bot_id or os.getenv("BOT_ID", "freqtrade-alpha-1")
        self.interval = interval_sec

        self._event_queue: queue.Queue = queue.Queue(maxsize=1000)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Mutable bot state snapshot updated by strategy
        self._status: str = "warming_up"
        self._active_pairs: List[str] = []
        self._open_trades_count: int = 0
        self._current_exposure: float = 0.0
        self._guard_state: Dict[str, Any] = {}
        self._state_lock = threading.Lock()

    def update_state(
        self,
        status: str,
        active_pairs: List[str],
        open_trades_count: int,
        current_exposure: float,
        guard_state: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._state_lock:
            self._status = status
            self._active_pairs = list(active_pairs)
            self._open_trades_count = open_trades_count
            self._current_exposure = current_exposure
            if guard_state is not None:
                self._guard_state = dict(guard_state)

    def log_event(self, event_type: str, level: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Enqueue an operational event (non-blocking)."""
        item = {
            "eventType": event_type,
            "level": level if level in ("info", "warning", "error") else "info",
            "message": message[:500],
            "details": details,
        }
        try:
            self._event_queue.put_nowait(item)
        except queue.Full:
            logger.warning(f"Telemetry event queue full, dropping event: {event_type}")

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="WowsTelemetryThread", daemon=True)
        self._thread.start()
        logger.info("Telemetry heartbeat thread started.")

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        logger.info("Telemetry heartbeat thread stopped.")

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.send_heartbeat()
            except Exception as e:
                logger.error(f"Error sending telemetry: {e}")
            self._stop_event.wait(timeout=self.interval)

    def send_heartbeat(self) -> bool:
        with self._state_lock:
            status = self._status
            active_pairs = list(self._active_pairs)
            trades_count = self._open_trades_count
            exposure = self._current_exposure
            guards = dict(self._guard_state)

        # Drain queued events up to 50
        events: List[Dict[str, Any]] = []
        while not self._event_queue.empty() and len(events) < 50:
            try:
                events.append(self._event_queue.get_nowait())
            except queue.Empty:
                break

        payload = {
            "botId": self.bot_id,
            "status": status,
            "timestamp": int(time.time() * 1000),
            "activePairs": active_pairs,
            "openTradesCount": trades_count,
            "currentExposureQuote": exposure,
            "guardState": guards,
            "events": events,
        }

        url = f"{self.base_url}/api/webhooks/bot/telemetry"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=5.0)
            if resp.status_code == 200:
                logger.debug("Heartbeat dispatched successfully.")
                return True
            logger.warning(f"Telemetry rejected with HTTP {resp.status_code}: {resp.text}")
            return False
        except Exception as e:
            logger.warning(f"Telemetry dispatch network error: {e}")
            return False
