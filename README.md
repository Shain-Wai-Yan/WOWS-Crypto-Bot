# WOWS Crypto Bot

Standalone autonomous Freqtrade client integration for the WOWS paper-trading platform.

## Architecture
- **Engine:** Pinned Freqtrade (dry-run mode).
- **Outbox:** Local SQLite outbox (`outbox.db` in WAL mode) with dedicated background dispatcher.
- **Delivery:** Authenticated HTTP webhook integration with WOWS (`/api/webhooks/bot`).
- **Telemetry:** 60-second heartbeats and structured decision audit logs (`/api/webhooks/bot/telemetry`).
- **Reconciliation:** Startup parity check (`/api/webhooks/bot/positions`) before opening new positions.
