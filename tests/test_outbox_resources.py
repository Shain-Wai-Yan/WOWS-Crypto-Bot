import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "user_data" / "strategies"))
import outbox


def test_empty_polling_and_early_returns_release_connections_without_gc(tmp_path, monkeypatch):
    connections = []
    connect = sqlite3.connect

    def tracked(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connections.append(connection)  # Strong references defeat GC as a cleanup mechanism.
        return connection

    monkeypatch.setattr(outbox.sqlite3, "connect", tracked)
    manager = outbox.OutboxManager(str(tmp_path / "outbox.db"))
    for _ in range(200):
        assert manager.get_pending_events() == []
    manager.record_fill("id", {"pair": "BTC/USDT"})
    assert manager.record_fill("id", {"pair": "BTC/USDT"}) is False
    for connection in connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("SELECT 1")


def test_failed_transaction_rolls_back_and_closes_connection(tmp_path):
    manager = outbox.OutboxManager(str(tmp_path / "outbox.db"))
    with pytest.raises(RuntimeError):
        with manager._get_connection() as connection:
            connection.execute("INSERT INTO outbox (event_id,payload,payload_hash,created_at) VALUES ('failed','{}','hash',0)")
            raise RuntimeError("interrupt")
    assert manager.get_event_status("failed") is None
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connection.execute("SELECT 1")
