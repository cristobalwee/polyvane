"""API key auth — single shared secret in the X-API-Key header.

Intentionally simple: this dashboard is for one operator, not a public API.
The key is a random 32+ char string from the .env file. We compare with
hmac.compare_digest to avoid timing leaks (overkill for one user, but
costs nothing). All routes except /ping require the header.
"""
from __future__ import annotations

import hmac
import os

from fastapi import Header, HTTPException, status


def _expected_key() -> str:
    """Read API_SECRET_KEY from env at request time so .env edits take effect
    on the next request rather than requiring a restart."""
    key = os.environ.get("API_SECRET_KEY", "").strip()
    if not key:
        # Fail closed: a missing key means we cannot authenticate anyone, so
        # we refuse all requests rather than letting them through unchecked.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API_SECRET_KEY is not configured on the server",
        )
    return key


def require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    """FastAPI dependency. Raises 401 on missing/wrong key."""
    expected = _expected_key()
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing X-API-Key header",
        )
    if not hmac.compare_digest(x_api_key, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid API key",
        )
