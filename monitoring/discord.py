"""Lightweight Discord webhook helper.

This is a *direct* webhook poster — separate from `monitoring.alerts.AlertBus`.
The AlertBus is the right tool for in-process trade-time alerts (queued,
cooldown'd, multi-channel). This helper is for callers that just want to fire
a single embed and move on:
  * shell scripts (healthcheck.sh) via `python -m monitoring.discord`
  * the executor when a trade lands
  * the risk module when the circuit breaker trips

If `DISCORD_WEBHOOK_URL` is empty / unset, every entry point becomes a no-op.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import urllib.error
import urllib.request
from typing import Any


log = logging.getLogger(__name__)


# Discord embed color codes (decimal RGB).
COLOR_GREEN = 0x2ECC71   # profit / success
COLOR_RED = 0xE74C3C     # loss / error
COLOR_YELLOW = 0xF1C40F  # warning
COLOR_BLUE = 0x3498DB    # info / neutral


def send_discord_alert(
    webhook_url: str,
    title: str,
    message: str,
    color: int = COLOR_BLUE,
    fields: dict[str, Any] | None = None,
    *,
    timeout_sec: float = 5.0,
) -> bool:
    """POST a single embed to a Discord webhook. Returns True on 2xx.

    No-op (returns True) when `webhook_url` is empty so callers can pass an
    unset env var and not have to gate the call themselves.
    """
    if not webhook_url:
        return True

    embed: dict[str, Any] = {
        "title": title[:256],
        "description": message[:4000],
        "color": color,
    }
    if fields:
        embed["fields"] = [
            {"name": str(k)[:256], "value": str(v)[:1024], "inline": True}
            for k, v in fields.items()
        ]

    body = {
        "username": "polyvane",
        "embeds": [embed],
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            return 200 <= resp.status < 300
    except urllib.error.HTTPError as e:
        log.warning("Discord webhook returned HTTP %s: %s", e.code, e.reason)
        return False
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        log.warning("Discord webhook failed: %s", e)
        return False


def _cli() -> int:
    """`python -m monitoring.discord <title> <message> [color]`

    Used by deploy/healthcheck.sh to fire alerts without standing up the
    full bot env.
    """
    if len(sys.argv) < 3:
        sys.stderr.write("usage: python -m monitoring.discord <title> <message> [green|red|yellow|blue]\n")
        return 2
    title = sys.argv[1]
    message = sys.argv[2]
    color_name = sys.argv[3] if len(sys.argv) > 3 else "blue"
    color = {
        "green": COLOR_GREEN, "red": COLOR_RED,
        "yellow": COLOR_YELLOW, "blue": COLOR_BLUE,
    }.get(color_name.lower(), COLOR_BLUE)
    webhook = os.environ.get("DISCORD_WEBHOOK_URL", "")
    ok = send_discord_alert(webhook, title, message, color)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(_cli())
