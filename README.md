# WOWS Crypto Bot

Standalone autonomous Freqtrade client integration for the WOWS paper-trading platform.

## Architecture
- **Engine:** Pinned Freqtrade (dry-run mode).
- **Outbox:** Local SQLite outbox (`outbox.db` in WAL mode) with dedicated background dispatcher.
- **Delivery:** Authenticated HTTP webhook integration with WOWS (`/api/webhooks/bot`).
- **Telemetry:** 60-second heartbeats and structured decision audit logs (`/api/webhooks/bot/telemetry`).
- **Reconciliation:** Startup parity check (`/api/webhooks/bot/positions`) before opening new positions.

## Telemetry cost controls and cutover

The telemetry client enforces one routine request per 60 seconds across all
callers, including the background thread and strategy hooks. V3 no longer sends
a second heartbeat on every bot loop. `HEARTBEAT_INTERVAL_SEC` defaults to 60;
values are bounded to 60–120 seconds to stay below the receiver's three-minute
stale threshold. Trading, order dispatch and reconciliation retain their existing
timing and destination.

Routine events are batched (at most 50 per request). Critical events and warning/
error logs wake the background reporter without waiting for the routine interval;
critical bursts are coalesced with a five-second minimum between critical sends.
Failed batches remain pending, with bounded retry backoff and no automatic
fallback to the application origin. Delivery is best-effort and may be repeated
after an ambiguous timeout; it is not exactly-once. Queues hold up to 1,000 events
plus the pending batch. Oversized/unserializable event details are replaced by
`{"truncated": true}` to stay within the worker's request-size limit.

Roll out in order:

1. Deploy the heartbeat fix with `WOWS_TELEMETRY_BASE_URL` empty and observe the
   existing WOWS endpoint for 48 hours.
2. Pass the financial gate in the WOWS app and enable/verify the Cloud Run ingress
   with the same bot IDs, authentication and backup-write gate.
3. Set `WOWS_TELEMETRY_BASE_URL` to the Cloud Run **origin** (no path), then redeploy
   this bot. Only telemetry changes destination; `WOWS_BASE_URL` stays on WOWS for
   fills and reconciliation. Redirects are not followed.
4. Observe successful records, event delivery and monitoring for seven days before
   retiring the legacy WOWS telemetry POST endpoint. Clear the telemetry override
   and redeploy to roll back routing if needed.

These changes do not deploy the bot or enable any Cloud Run workload automatically.
