#!/usr/bin/env bash
# Healthcheck — runs every 5 minutes as root via cron.
# Two-part check:
#   1. systemd unit is active
#   2. heartbeat file is < 20 minutes old
# If either fails: try to restart, then fire a Discord alert + ping
# HEALTHCHECK_URL (healthchecks.io style) on success.
#
# Sourced by /etc/cron.d/polyvane-healthcheck.

set -uo pipefail

INSTALL_DIR="/opt/polyvane"
ENV_FILE="$INSTALL_DIR/.env"
HEARTBEAT_FILE="${HEARTBEAT_FILE:-$INSTALL_DIR/data/heartbeat}"
SERVICE_NAME="polyvane"
HEARTBEAT_MAX_AGE_SEC=$((20 * 60))

# Pull DISCORD_WEBHOOK_URL / HEALTHCHECK_URL from .env if present.
if [[ -r "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"
HEALTHCHECK_URL="${HEALTHCHECK_URL:-}"

# Use the project's discord helper for embeds (no extra deps required).
PY="$INSTALL_DIR/venv/bin/python"
notify() {
    local title="$1" message="$2" color="${3:-yellow}"
    if [[ -n "$DISCORD_WEBHOOK_URL" && -x "$PY" ]]; then
        DISCORD_WEBHOOK_URL="$DISCORD_WEBHOOK_URL" \
            "$PY" -m monitoring.discord "$title" "$message" "$color" \
            >/dev/null 2>&1 || true
    fi
    logger -t polyvane-healthcheck "$title: $message" || true
}

ping_healthchecks_io() {
    local endpoint="${1:-}"
    if [[ -n "$HEALTHCHECK_URL" ]]; then
        curl -fsS -m 10 --retry 2 \
            "${HEALTHCHECK_URL}${endpoint}" \
            >/dev/null 2>&1 || true
    fi
}

cd "$INSTALL_DIR" 2>/dev/null || true

# Check 1: service active.
if ! systemctl is-active --quiet "$SERVICE_NAME"; then
    notify "PolyVane DOWN" "polyvane.service not active — attempting restart" "red"
    systemctl restart "$SERVICE_NAME" || true
    sleep 5
    if ! systemctl is-active --quiet "$SERVICE_NAME"; then
        notify "PolyVane FAILED" "restart did not bring service back" "red"
        ping_healthchecks_io "/fail"
        exit 1
    fi
    notify "PolyVane RECOVERED" "service restarted successfully" "yellow"
fi

# Check 2: heartbeat freshness.
if [[ ! -f "$HEARTBEAT_FILE" ]]; then
    notify "PolyVane heartbeat missing" "$HEARTBEAT_FILE does not exist — restarting" "yellow"
    systemctl restart "$SERVICE_NAME" || true
    ping_healthchecks_io "/fail"
    exit 1
fi

now=$(date +%s)
mtime=$(stat -c '%Y' "$HEARTBEAT_FILE")
age=$((now - mtime))

if (( age > HEARTBEAT_MAX_AGE_SEC )); then
    notify "PolyVane stalled" \
        "heartbeat is ${age}s old (>${HEARTBEAT_MAX_AGE_SEC}s) — restarting" "red"
    systemctl restart "$SERVICE_NAME" || true
    ping_healthchecks_io "/fail"
    exit 1
fi

# All good.
ping_healthchecks_io ""
exit 0
