import json
import logging
import math
import os
import queue
import threading
import time
from typing import Optional, List, Dict, Any
from urllib.parse import urlsplit

import requests

logger = logging.getLogger("WowsTelemetry")


class TelemetryClient:
    """Bounded background reporting; all callers share the heartbeat throttle."""

    CRITICAL_EVENTS = frozenset({
        "risk_exit", "order_rejected", "reconciliation_mismatch",
        "status_change", "order_filled",
    })
    CRITICAL_INTERVAL_SEC = 5.0
    MAX_PAYLOAD_BYTES = 240 * 1024  # Leave room below the receiver's 256 KiB cap.

    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None,
                 bot_id: Optional[str] = None, interval_sec: Optional[float] = None,
                 telemetry_base_url: Optional[str] = None):
        self.base_url = (base_url or os.getenv("WOWS_BASE_URL", "http://localhost:3000")).rstrip("/")
        target = telemetry_base_url or os.getenv("WOWS_TELEMETRY_BASE_URL", "").strip() or self.base_url
        parsed = urlsplit(target)
        if (parsed.scheme not in ("http", "https") or not parsed.netloc
                or parsed.username or parsed.password or parsed.query or parsed.fragment
                or parsed.path not in ("", "/")):
            raise ValueError("Telemetry base URL must be an HTTP(S) origin without credentials or a path")
        self.telemetry_base_url = target.rstrip("/")
        self.api_key = api_key or os.getenv("BOT_API_KEY", "")
        self.bot_id = bot_id or os.getenv("BOT_ID", "freqtrade-alpha-1")
        try:
            interval = float(interval_sec if interval_sec is not None else os.getenv("HEARTBEAT_INTERVAL_SEC", "60"))
            if not math.isfinite(interval):
                raise ValueError("Non-finite heartbeat interval")
            # Receiver stale threshold is three minutes; routine dispatch is bounded.
            self.interval = min(120.0, max(60.0, interval))
        except (ValueError, TypeError):
            self.interval = 60.0

        self._event_queue: queue.Queue = queue.Queue(maxsize=900)
        self._urgent_queue: queue.Queue = queue.Queue(maxsize=100)
        self._pending_events: List[Dict[str, Any]] = []
        self._pending_critical = False
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._send_lock = threading.Lock()
        self._last_attempt = float("-inf")
        self._last_critical_attempt = float("-inf")
        self._retry_not_before = float("-inf")
        self._failures = 0
        self._status: str = "warming_up"
        self._active_pairs: List[str] = []
        self._open_trades_count: int = 0
        self._current_exposure: float = 0.0
        self._guard_state: Dict[str, Any] = {}
        self._state_lock = threading.Lock()

    @staticmethod
    def _bounded_details(details: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if details is None:
            return None
        try:
            encoded = json.dumps(details, allow_nan=False)
            if isinstance(details, dict) and len(encoded.encode("utf-8")) <= 2048:
                return json.loads(encoded)
        except (ValueError, TypeError, OverflowError):
            pass
        return {"truncated": True}

    def update_state(self, status: str, active_pairs: List[str], open_trades_count: int,
                     current_exposure: float, guard_state: Optional[Dict[str, Any]] = None) -> None:
        with self._state_lock:
            self._status = status
            self._active_pairs = [str(pair)[:64] for pair in active_pairs[:50]]
            self._open_trades_count = open_trades_count
            self._current_exposure = current_exposure
            if guard_state is not None:
                self._guard_state = self._bounded_details(guard_state) or {}

    def log_event(self, event_type: str, level: str, message: str,
                  details: Optional[Dict[str, Any]] = None) -> None:
        level = level if level in ("info", "warning", "error") else "info"
        urgent = level in ("warning", "error") or event_type in self.CRITICAL_EVENTS
        item = {"eventType": event_type, "level": level, "message": message[:500],
                "details": self._bounded_details(details)}
        try:
            (self._urgent_queue if urgent else self._event_queue).put_nowait(item)
            if urgent:
                self._wake_event.set()
        except queue.Full:
            logger.warning("Telemetry event queue full, dropping event: %s", event_type)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="WowsTelemetryThread", daemon=True)
        self._thread.start()
        logger.info("Telemetry heartbeat thread started.")

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        self._wake_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        logger.info("Telemetry heartbeat thread stop requested.")

    def _is_urgent(self, event: Dict[str, Any]) -> bool:
        return event["level"] in ("warning", "error") or event["eventType"] in self.CRITICAL_EVENTS

    def _critical_due(self) -> bool:
        return self._pending_critical or not self._urgent_queue.empty()

    def _next_delay(self) -> float:
        now = time.monotonic()
        due = self._last_attempt + self.interval
        if self._critical_due():
            due = min(due, self._last_critical_attempt + self.CRITICAL_INTERVAL_SEC)
        return max(0.05, max(due, self._retry_not_before) - now)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self._wake_event.clear()
            try:
                self.send_heartbeat()
            except Exception:
                logger.exception("Unexpected telemetry reporter error")
            self._wake_event.wait(timeout=self._next_delay())

    def send_heartbeat(self) -> bool:
        """Return False when throttled or unsuccessful; never bypass the throttle."""
        if not self._send_lock.acquire(blocking=False):
            return False
        try:
            now = time.monotonic()
            urgent = self._critical_due()
            if now < self._retry_not_before:
                return False
            if now - self._last_attempt < self.interval and not (
                    urgent and now - self._last_critical_attempt >= self.CRITICAL_INTERVAL_SEC):
                return False
            # Failed/partial routine batches must not block newly urgent events.
            urgent_pending = [event for event in self._pending_events if self._is_urgent(event)]
            routine_pending = [event for event in self._pending_events if not self._is_urgent(event)]
            while len(urgent_pending) < 50:
                try:
                    urgent_pending.append(self._urgent_queue.get_nowait())
                except queue.Empty:
                    break
            self._pending_events = urgent_pending + routine_pending
            while len(self._pending_events) < 50:
                try:
                    self._pending_events.append(self._event_queue.get_nowait())
                except queue.Empty:
                    break
            self._pending_critical = bool(urgent_pending)
            with self._state_lock:
                payload = {"botId": self.bot_id, "status": self._status,
                           "timestamp": int(time.time() * 1000),
                           "activePairs": list(self._active_pairs),
                           "openTradesCount": self._open_trades_count,
                           "currentExposureQuote": self._current_exposure,
                           "guardState": dict(self._guard_state),
                           "events": list(self._pending_events[:50])}
            # JSON escapes can expand non-ASCII messages far beyond 500 bytes.
            # Send a fitting prefix and retain the rest for later batches.
            while len(json.dumps(payload).encode("utf-8")) > self.MAX_PAYLOAD_BYTES and payload["events"]:
                payload["events"].pop()
            sent_events = len(payload["events"])
            self._last_attempt = now
            if any(self._is_urgent(event) for event in payload["events"]):
                self._last_critical_attempt = now
            try:
                response = requests.post(
                    f"{self.telemetry_base_url}/api/webhooks/bot/telemetry",
                    json=payload,
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"},
                    timeout=5.0, allow_redirects=False,
                )
                if response.status_code == 200:
                    self._pending_events = self._pending_events[sent_events:]
                    self._pending_critical = any(
                        event["level"] in ("warning", "error") or event["eventType"] in self.CRITICAL_EVENTS
                        for event in self._pending_events
                    )
                    self._failures = 0
                    self._retry_not_before = float("-inf")
                    return True
                logger.warning("Telemetry rejected with HTTP %s", response.status_code)
            except requests.RequestException:
                logger.warning("Telemetry dispatch network error; retaining pending events")
            except (ValueError, TypeError):
                logger.warning("Telemetry payload could not be encoded; retaining pending events")
            self._failures += 1
            self._retry_not_before = time.monotonic() + min(60.0, 5.0 * 2 ** min(self._failures - 1, 4))
            return False
        finally:
            self._send_lock.release()
