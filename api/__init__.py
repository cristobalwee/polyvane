"""HTTP API layer for the PolyVane bot.

A separate FastAPI process that exposes read-only views over the bot's
state — trade journal, performance metrics, runtime status — for a remote
dashboard. Runs alongside the bot on the same VPS, reads from the same
SQLite database file, and never writes to it.
"""

API_VERSION = "1.0.0"
