"""Repair trade-journal rows with malformed metadata_json.

Background: weather strategies emit ±inf for edge buckets ('<75°F',
'>100°F'). Python's `json.dumps` happily writes those as the literals
`-Infinity` / `Infinity` / `NaN`, but those are NOT valid JSON. SQLite's
`json_extract` rejects the whole column with "malformed JSON", which used
to crash the perf report and the daily summary.

The bot now sanitizes those at write time (core.logger._sanitize_for_json),
but rows written before that fix are still corrupt. This script rewrites
them: parses each invalid metadata_json with Python's lenient json (which
accepts the bad literals), replaces non-finite floats with None, and
re-serializes as valid JSON.

By default this is a DRY RUN — it tells you how many rows would change
and prints a few diffs. Pass --apply to actually write.

    python -m scripts.repair_journal_metadata
    python -m scripts.repair_journal_metadata --apply
    python -m scripts.repair_journal_metadata --db data/trade_journal.remote.db --apply
"""
from __future__ import annotations

import argparse
import json
import math
import shutil
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = PROJECT_ROOT / "data" / "trade_journal.db"


def _sanitize(value: Any) -> Any:
    """Same logic as core.logger._sanitize_for_json — kept inline so this
    script can run standalone against a copied DB without project deps."""
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    if isinstance(value, dict):
        return {k: _sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize(v) for v in value]
    return value


@contextmanager
def _connect(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _backup(db_path: Path) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = db_path.with_suffix(db_path.suffix + f".bak.{ts}")
    shutil.copy2(db_path, dest)
    return dest


def repair(db_path: Path, *, apply: bool, sample_size: int = 3) -> int:
    """Repair rows where metadata_json fails json_valid. Returns # of rows
    that need (or got) fixed. Dry-run mode just counts and prints a sample."""
    if not db_path.exists():
        raise SystemExit(f"trade journal not found at {db_path}")

    with _connect(db_path) as conn:
        bad = [
            dict(r) for r in conn.execute(
                "SELECT id, strategy, market_id, metadata_json "
                "FROM trades WHERE NOT json_valid(metadata_json)"
            ).fetchall()
        ]
        if not bad:
            print(f"OK — all {conn.execute('SELECT COUNT(*) FROM trades').fetchone()[0]} "
                  f"rows have valid metadata_json. Nothing to do.")
            return 0

        print(f"Found {len(bad)} row(s) with malformed metadata_json.")

        # Build the replacement pairs first so we can show a diff before any write.
        repairs: list[tuple[int, str]] = []
        unparseable: list[int] = []
        for r in bad:
            raw = r["metadata_json"] or "{}"
            try:
                # Python's json.loads accepts -Infinity/Infinity/NaN by default
                # via parse_constant, so this round-trips cleanly.
                parsed = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                unparseable.append(int(r["id"]))
                continue
            cleaned = json.dumps(_sanitize(parsed), default=str)
            repairs.append((int(r["id"]), cleaned))

        if unparseable:
            print(f"WARN: {len(unparseable)} row(s) couldn't be parsed even with "
                  f"Python's lenient JSON; leaving them alone. IDs: "
                  f"{unparseable[:10]}{'...' if len(unparseable) > 10 else ''}")

        # Print a sample diff.
        print(f"\nSample (first {min(sample_size, len(repairs))}):")
        for tid, cleaned in repairs[:sample_size]:
            orig = next(r["metadata_json"] for r in bad if int(r["id"]) == tid)
            print(f"  trade #{tid}:")
            print(f"    BEFORE: {orig[:160]}{'...' if len(orig) > 160 else ''}")
            print(f"    AFTER : {cleaned[:160]}{'...' if len(cleaned) > 160 else ''}")

        if not apply:
            print("\nDry run — no changes written. Re-run with --apply to fix.")
            return len(repairs)

        backup_path = _backup(db_path)
        print(f"\nBackup written to {backup_path}")
        # Single transaction so we either fix all or none.
        conn.execute("BEGIN")
        try:
            for tid, cleaned in repairs:
                conn.execute(
                    "UPDATE trades SET metadata_json = ? WHERE id = ?",
                    (cleaned, tid),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        # Verify.
        remaining = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE NOT json_valid(metadata_json)"
        ).fetchone()[0]
        print(f"Repaired {len(repairs)} row(s). Remaining invalid: {remaining}.")
        return len(repairs)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--db", default=str(DEFAULT_DB),
                   help=f"Path to trade journal SQLite. Default: {DEFAULT_DB}")
    p.add_argument("--apply", action="store_true",
                   help="Actually write the repairs (default is dry run).")
    args = p.parse_args(argv)
    repair(Path(args.db), apply=args.apply)
    return 0


if __name__ == "__main__":
    sys.exit(main())
