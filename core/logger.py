"""Trade journal — append-only SQLite log of every entry/exit with full context."""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


_SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       TEXT    NOT NULL,
    strategy        TEXT    NOT NULL,
    market_id       TEXT    NOT NULL,
    market_question TEXT,
    direction       TEXT    NOT NULL,
    entry_price     REAL    NOT NULL,
    size_usd        REAL    NOT NULL,
    shares          REAL    NOT NULL,
    edge_at_entry   REAL    NOT NULL,
    outcome         TEXT    NOT NULL DEFAULT 'pending',
    pnl             REAL,
    metadata_json   TEXT    NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_trades_strategy   ON trades(strategy);
CREATE INDEX IF NOT EXISTS idx_trades_outcome    ON trades(outcome);
CREATE INDEX IF NOT EXISTS idx_trades_market     ON trades(market_id);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp  ON trades(timestamp);
"""


@dataclass
class TradeRecord:
    strategy: str
    market_id: str
    direction: str
    entry_price: float
    size_usd: float
    shares: float
    edge_at_entry: float
    market_question: str | None = None
    outcome: str = "pending"
    pnl: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class TradeJournal:
    """Thread-safe SQLite-backed trade journal."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with self._connect() as conn:
            conn.executescript(_SCHEMA)
            conn.commit()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.db_path), isolation_level=None, timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def record_entry(self, trade: TradeRecord) -> int:
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO trades (
                    timestamp, strategy, market_id, market_question, direction,
                    entry_price, size_usd, shares, edge_at_entry, outcome, pnl, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade.timestamp,
                    trade.strategy,
                    trade.market_id,
                    trade.market_question,
                    trade.direction,
                    trade.entry_price,
                    trade.size_usd,
                    trade.shares,
                    trade.edge_at_entry,
                    trade.outcome,
                    trade.pnl,
                    json.dumps(trade.metadata, default=str),
                ),
            )
            return int(cur.lastrowid)

    def record_exit(self, trade_id: int, outcome: str, pnl: float, metadata: dict[str, Any] | None = None) -> None:
        if outcome not in ("won", "lost"):
            raise ValueError(f"outcome must be 'won' or 'lost', got {outcome!r}")
        with self._lock, self._connect() as conn:
            if metadata is None:
                conn.execute(
                    "UPDATE trades SET outcome = ?, pnl = ? WHERE id = ?",
                    (outcome, pnl, trade_id),
                )
            else:
                row = conn.execute("SELECT metadata_json FROM trades WHERE id = ?", (trade_id,)).fetchone()
                merged = json.loads(row["metadata_json"]) if row else {}
                merged.update(metadata)
                conn.execute(
                    "UPDATE trades SET outcome = ?, pnl = ?, metadata_json = ? WHERE id = ?",
                    (outcome, pnl, json.dumps(merged, default=str), trade_id),
                )

    def open_positions(self) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT * FROM trades WHERE outcome = 'pending'").fetchall()
            return [dict(r) for r in rows]

    def realized_pnl_since(self, since_iso: str) -> float:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(pnl), 0.0) AS total FROM trades "
                "WHERE outcome IN ('won','lost') AND timestamp >= ?",
                (since_iso,),
            ).fetchone()
            return float(row["total"])

    def count_entries_since(self, since_iso: str) -> int:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM trades WHERE timestamp >= ?",
                (since_iso,),
            ).fetchone()
            return int(row["n"])


def configure_console_logging(level: str = "INFO") -> logging.Logger:
    """Configure root logger to stdout with a consistent format."""
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logging.getLogger("polymarket_bot")
