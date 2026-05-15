"""Trade journal — append-only SQLite log of every entry/exit with full context."""
from __future__ import annotations

import json
import logging
import math
import sqlite3
import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


def _sanitize_for_json(value: Any) -> Any:
    """Recursively replace non-finite floats (Infinity/-Infinity/NaN) with None.

    Python's `json.dumps` accepts these by default and emits the literals
    `Infinity` / `-Infinity` / `NaN`, which are NOT valid in strict JSON.
    SQLite's `json_extract` (used by perf reports and the reviewer) rejects
    the entire column as malformed when it sees them. Weather strategies
    emit ±inf for edge buckets ('<75°F', '>100°F'), so this triggers in
    practice — see TemperatureBucket.low/high.
    """
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    if isinstance(value, dict):
        return {k: _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_for_json(v) for v in value]
    return value


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
    metadata_json   TEXT    NOT NULL DEFAULT '{}',
    exchange        TEXT    NOT NULL DEFAULT 'polymarket'
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
    exchange: str = "polymarket"


class TradeJournal:
    """Thread-safe SQLite-backed trade journal."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with self._connect() as conn:
            conn.executescript(_SCHEMA)
            conn.commit()
            try:
                conn.execute(
                    "ALTER TABLE trades ADD COLUMN exchange TEXT NOT NULL DEFAULT 'polymarket'"
                )
                conn.commit()
            except sqlite3.OperationalError:
                pass  # column already exists
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_exchange ON trades(exchange)"
            )
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
                    entry_price, size_usd, shares, edge_at_entry, outcome, pnl, metadata_json,
                    exchange
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    json.dumps(_sanitize_for_json(trade.metadata), default=str),
                    trade.exchange,
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
                    (outcome, pnl, json.dumps(_sanitize_for_json(merged), default=str), trade_id),
                )

    def open_positions(self) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute("SELECT * FROM trades WHERE outcome = 'pending'").fetchall()
            return [dict(r) for r in rows]

    def closed_since(self, since_iso: str) -> list[dict[str, Any]]:
        """Trades resolved on or after `since_iso`, ordered by resolution time.

        Resolution time is `metadata.resolved_at` (set by the reviewer);
        falls back to `timestamp` for any historical row that pre-dates that
        field, or for rows whose metadata_json fails json_valid (legacy
        rows where ±inf leaked in). Used by the daily summary to list
        newly-settled trades.
        """
        # `json_extract` raises "malformed JSON" on invalid columns and
        # tears down the whole query, so guard with json_valid().
        resolved_at_expr = (
            "COALESCE("
            "  CASE WHEN json_valid(metadata_json) "
            "       THEN json_extract(metadata_json, '$.resolved_at') "
            "       ELSE NULL END,"
            "  timestamp)"
        )
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *, {resolved_at_expr} AS _resolved_at
                FROM trades
                WHERE outcome IN ('won','lost')
                  AND {resolved_at_expr} >= ?
                ORDER BY _resolved_at ASC
                """,
                (since_iso,),
            ).fetchall()
            return [dict(r) for r in rows]

    def realized_pnl_since(self, since_iso: str) -> float:
        """Sum of pnl for trades RESOLVED on/after `since_iso`.

        Resolution time is `metadata.resolved_at` (set by the reviewer when
        it settles a trade); falls back to entry `timestamp` for any
        historical row that pre-dates that field. Without this filter,
        a trade entered yesterday and resolved today wouldn't count
        toward today's realized PnL.
        """
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(SUM(pnl), 0.0) AS total FROM trades
                WHERE outcome IN ('won','lost')
                  AND COALESCE(
                        CASE WHEN json_valid(metadata_json)
                             THEN json_extract(metadata_json, '$.resolved_at')
                             ELSE NULL END,
                        timestamp
                      ) >= ?
                """,
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
