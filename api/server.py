"""FastAPI app entry point.

Wires together routes, CORS, rate limiting, auth, and DB lifecycle. The
app is created via `create_app()` so tests can build their own instance,
but the module also exposes a top-level `app` for the systemd unit:

    uvicorn api.server:app --host 0.0.0.0 --port 8099
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from api import API_VERSION
from api.auth import require_api_key
from api.deps import PROJECT_ROOT, close_db, open_db
from api.models import PingResponse
from api.routes import (
    markets as markets_routes,
    performance as performance_routes,
    positions as positions_routes,
    signals as signals_routes,
    status as status_routes,
    trades as trades_routes,
)


# Load .env once at import time so uvicorn workers all see the same vars.
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_ORIGINS = ["http://localhost:3000", "http://localhost:5173", "https://cristobalgrana.me"]


def _allowed_origins() -> list[str]:
    """Comma-separated DASHBOARD_ORIGINS overrides the defaults."""
    raw = (os.environ.get("DASHBOARD_ORIGINS") or "").strip()
    if not raw:
        return DEFAULT_ORIGINS
    return [o.strip() for o in raw.split(",") if o.strip()]


limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"], headers_enabled=True)


class _RateLimitMiddleware(SlowAPIMiddleware):
    """SlowAPIMiddleware builds its own 429 response and never raises, so a
    `@app.exception_handler(RateLimitExceeded)` is never invoked. We
    subclass to rewrite the body into our shape and guarantee Retry-After.
    """
    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        response = await super().dispatch(request, call_next)
        if response.status_code != 429:
            return response
        retry_after = response.headers.get("Retry-After", "60")
        body = {"error": "rate_limited", "detail": "rate limit exceeded (60/minute)"}
        new_response = JSONResponse(status_code=429, content=body)
        # Preserve Retry-After + any X-RateLimit-* headers the limiter added.
        for k, v in response.headers.items():
            if k.lower().startswith("x-ratelimit") or k.lower() == "retry-after":
                new_response.headers[k] = v
        new_response.headers.setdefault("Retry-After", retry_after)
        return new_response


@asynccontextmanager
async def lifespan(app: FastAPI):
    log = logging.getLogger("polyvane.api")
    await open_db(app.state)
    log.info("polyvane-api: started, db=%s", getattr(app.state, "db", None))
    try:
        yield
    finally:
        await close_db(app.state)
        log.info("polyvane-api: stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="PolyVane API",
        version=API_VERSION,
        description="Read-only HTTP API over the PolyVane bot's state.",
        lifespan=lifespan,
    )

    app.state.limiter = limiter
    app.add_middleware(_RateLimitMiddleware)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=_allowed_origins(),
        allow_credentials=False,
        allow_methods=["GET"],
        allow_headers=["X-API-Key", "Content-Type"],
    )

    # ---- /ping (no auth) — kept off the /api/v1 prefix so uptime
    # monitors can hit a stable URL even if we version-bump the API.
    @app.get("/api/v1/ping", response_model=PingResponse, tags=["meta"])
    async def ping() -> PingResponse:
        return PingResponse(
            ok=True,
            service="polyvane-api",
            version=API_VERSION,
            time=datetime.now(timezone.utc),
        )

    # ---- authenticated routes
    auth = [Depends(require_api_key)]
    api_v1 = "/api/v1"

    app.include_router(
        status_routes.router, prefix=f"{api_v1}/status",
        tags=["status"], dependencies=auth,
    )
    app.include_router(
        positions_routes.router, prefix=f"{api_v1}/positions",
        tags=["positions"], dependencies=auth,
    )
    app.include_router(
        performance_routes.router, prefix=f"{api_v1}/performance",
        tags=["performance"], dependencies=auth,
    )
    app.include_router(
        trades_routes.router, prefix=f"{api_v1}/trades",
        tags=["trades"], dependencies=auth,
    )
    app.include_router(
        markets_routes.router, prefix=f"{api_v1}/markets",
        tags=["markets"], dependencies=auth,
    )
    app.include_router(
        signals_routes.router, prefix=f"{api_v1}/signals",
        tags=["signals"], dependencies=auth,
    )

    return app


app = create_app()
