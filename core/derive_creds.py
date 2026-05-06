"""Derive Polymarket API credentials from your wallet key.

V2 API credentials (api_key / api_secret / api_passphrase) are derived from
your wallet by signing an L1 challenge. This script does that once and prints
the values, ready to paste into your .env. The credentials are stable for a
given wallet — you only need to derive them once.

Usage:
    PK=0x... python -m core.derive_creds

If you want to use a custom CLOB host or chain id:
    CLOB_HOST=https://clob.polymarket.com CHAIN_ID=137 PK=0x... python -m core.derive_creds

The script is unauthenticated against the bot's regular config — it talks
straight to Polymarket. It does NOT require py-clob-client-v2's API creds to
already exist; that's the whole point.
"""
from __future__ import annotations

import os
import sys


def main() -> int:
    pk = os.environ.get("PK", "").strip()
    if not pk:
        sys.stderr.write("ERROR: set PK=0x... in the environment.\n")
        sys.stderr.write("Usage: PK=0x... python -m core.derive_creds\n")
        return 2
    if not pk.startswith("0x"):
        sys.stderr.write("ERROR: PK must be a 0x-prefixed hex string.\n")
        return 2

    host = os.environ.get("CLOB_HOST", "https://clob.polymarket.com")
    try:
        chain_id = int(os.environ.get("CHAIN_ID", "137"))
    except ValueError:
        sys.stderr.write("ERROR: CHAIN_ID must be an integer (default 137).\n")
        return 2

    try:
        from py_clob_client_v2 import ClobClient
    except ImportError:
        sys.stderr.write(
            "ERROR: py-clob-client-v2 is not installed.\n"
            "Run: pip install -r requirements.txt\n"
        )
        return 1

    client = ClobClient(host=host, chain_id=chain_id, key=pk)

    try:
        creds = client.create_or_derive_api_key()
    except Exception as e:
        sys.stderr.write(f"ERROR: failed to derive credentials: {e}\n")
        return 1

    print()
    print("# === Polymarket V2 API credentials ===")
    print("# Paste these into your .env (or /opt/polyvane/.env on the VPS).")
    print()
    print(f"CLOB_API_KEY={creds.api_key}")
    print(f"CLOB_SECRET={creds.api_secret}")
    print(f"CLOB_PASS_PHRASE={creds.api_passphrase}")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
