import os
import json
import time
import random
import threading
import logging
from typing import Optional
import requests
from requests.exceptions import RequestException, Timeout

import sys
from pathlib import Path
_DIR = str(Path(__file__).parent.resolve())
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)
from outbox import OutboxManager

logger = logging.getLogger("WowsDispatcher")

class DispatcherWorker:
    """
    Background worker thread that drains the SQLite outbox and dispatches
    fill events to the WOWS Webhook API with exponential backoff and jitter.
    """
    def __init__(
        self,
        outbox: OutboxManager,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        poll_interval_sec: float = 1.0,
        backoff_base_sec: float = 2.0,
        max_backoff_sec: float = 60.0,
        jitter_sec: float = 1.0,
    ):
        self.outbox = outbox
        self.base_url = (base_url or os.getenv("WOWS_BASE_URL", "http://localhost:3000")).rstrip("/")
        self.api_key = api_key or os.getenv("BOT_API_KEY", "")
        self.poll_interval = poll_interval_sec
        self.backoff_base = backoff_base_sec
        self.max_backoff = max_backoff_sec
        self.jitter = jitter_sec

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="WowsDispatcherThread", daemon=True)
        self._thread.start()
        logger.info("Outbox Dispatcher worker started.")

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        logger.info("Outbox Dispatcher worker stopped.")

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                processed = self.process_pending_batch(limit=10)
                if not processed:
                    self._stop_event.wait(timeout=self.poll_interval)
            except Exception as e:
                logger.error(f"Error in dispatcher loop: {e}", exc_info=True)
                self._stop_event.wait(timeout=self.poll_interval)

    def process_pending_batch(self, limit: int = 10) -> int:
        """Processes up to `limit` pending outbox events. Returns count of events handled."""
        events = self.outbox.get_pending_events(limit=limit)
        if not events:
            return 0

        for event in events:
            if self._stop_event.is_set():
                break
            self.dispatch_event(event)
        return len(events)

    def dispatch_event(self, event: dict) -> bool:
        event_id = event["event_id"]
        attempt = event["attempt_count"]
        payload_str = event["payload"]
        url = f"{self.base_url}/api/webhooks/bot"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

        try:
            resp = requests.post(url, data=payload_str, headers=headers, timeout=5.0)

            # Success: 200 OK (fresh execution or idempotent replay)
            if resp.status_code == 200:
                self.outbox.mark_acknowledged(event_id)
                logger.info(f"Event {event_id} acknowledged by WOWS.")
                return True

            # Transient Rate Limiting: 429
            if resp.status_code == 429:
                retry_after_str = resp.headers.get("Retry-After")
                if retry_after_str and retry_after_str.isdigit():
                    delay = float(retry_after_str)
                else:
                    delay = self._calculate_backoff(attempt)
                error_msg = f"HTTP 429 Rate Limited: {resp.text}"
                self.outbox.mark_retry(event_id, error_msg, delay)
                logger.warning(f"Event {event_id} rate limited. Retrying in {delay:.1f}s.")
                return False

            # Transient Errors: 408 Timeout, 500/502/503/504 Server Error
            if resp.status_code in (408, 500, 502, 503, 504):
                delay = self._calculate_backoff(attempt)
                error_msg = f"HTTP {resp.status_code} Transient Error: {resp.text}"
                self.outbox.mark_retry(event_id, error_msg, delay)
                logger.warning(f"Event {event_id} failed with {resp.status_code}. Retrying in {delay:.1f}s.")
                return False

            # Permanent Client Errors: 400, 401, 403, 409, 422
            # Do NOT retry blindly. Quarantine for operator attention.
            error_msg = f"HTTP {resp.status_code} Permanent Error: {resp.text}"
            self.outbox.mark_quarantined(event_id, error_msg)
            logger.error(f"Event {event_id} quarantined: {error_msg}")
            return False

        except (Timeout, RequestException) as net_err:
            delay = self._calculate_backoff(attempt)
            error_msg = f"Network Exception: {str(net_err)}"
            self.outbox.mark_retry(event_id, error_msg, delay)
            logger.warning(f"Event {event_id} network error. Retrying in {delay:.1f}s: {net_err}")
            return False

    def _calculate_backoff(self, attempt: int) -> float:
        exponential = self.backoff_base * (2 ** min(attempt, 6))
        capped = min(exponential, self.max_backoff)
        jitter = random.uniform(0, self.jitter)
        return capped + jitter
