#!/usr/bin/env bash
# One-time provisioning for a fresh Ubuntu 24.04 VPS.
# Run as root:  bash setup-server.sh
#
# Idempotent: rerunning is safe — existing users/dirs/units are skipped or
# updated in place rather than re-created from scratch.

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "ERROR: must be run as root (try: sudo bash $0)" >&2
    exit 1
fi

USER_NAME="polyvane"
INSTALL_DIR="/opt/polyvane"
SERVICE_NAME="polyvane"
API_SERVICE_NAME="polyvane-api"
API_PORT="8099"
HEALTHCHECK_INTERVAL="*/5 * * * *"

log()  { printf '\e[1;34m[setup]\e[0m %s\n' "$*"; }
warn() { printf '\e[1;33m[warn]\e[0m %s\n' "$*"; }
die()  { printf '\e[1;31m[fatal]\e[0m %s\n' "$*" >&2; exit 1; }

# Count syntactically-plausible SSH pubkeys in $1 (one per line, ignoring
# blanks and # comments). Returns the count on stdout. 0 means "no usable key"
# even if the file exists. Uses awk so we always print exactly one number
# and never trip `set -e` on a no-match exit code.
_count_valid_keys() {
    local f="$1"
    [[ -r "$f" ]] || { echo 0; return; }
    awk '
        /^[[:space:]]*(ssh-(rsa|ed25519|dss)|ecdsa-sha2-[^ ]+|sk-(ssh-ed25519|ecdsa-sha2-[^ ]+))[[:space:]]+/ {
            c++
        }
        END { print c + 0 }
    ' "$f"
}

# ---- 0. Pre-flight: do NOT lock the operator out --------------------------
# This script disables root SSH and password auth. If we run that against a
# machine with no key on file, the next ssh attempt is permanently denied
# and the only recovery is the provider's web console. Refuse to start in
# that state.

ROOT_AUTH_KEYS="/root/.ssh/authorized_keys"
root_key_count="$(_count_valid_keys "$ROOT_AUTH_KEYS")"
if (( root_key_count == 0 )); then
    cat >&2 <<EOF
[fatal] No usable SSH public key found in $ROOT_AUTH_KEYS.

This script is about to disable root SSH login and password authentication.
With no key on file, that lockout is permanent — recovery requires the VPS
provider's web console. Refusing to proceed.

To recover from THIS machine right now (before running setup):
  1. On your laptop:   cat ~/.ssh/id_ed25519.pub      (or id_rsa.pub)
  2. Paste the line into $ROOT_AUTH_KEYS on this VPS
  3. chmod 700 /root/.ssh && chmod 600 $ROOT_AUTH_KEYS
  4. Verify from your laptop:   ssh root@<this-vps-ip>
  5. Then re-run this script.

Tip for next time: most providers (Vultr, Hetzner, DO, Linode) let you
attach an account-level SSH key at deploy time. Always do that — it
guarantees /root/.ssh/authorized_keys is populated before you ever SSH in.
EOF
    exit 2
fi
log "preflight: found $root_key_count usable key(s) in $ROOT_AUTH_KEYS"

# ---- 1. System packages -----------------------------------------------------

log "apt-get update + base packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends \
    ca-certificates curl git rsync sudo \
    python3 python3-venv python3-pip python3-dev \
    build-essential pkg-config \
    ufw logrotate cron

# ---- 2. Non-root user -------------------------------------------------------

if id "$USER_NAME" &>/dev/null; then
    log "user $USER_NAME already exists"
else
    log "creating user $USER_NAME"
    adduser --disabled-password --gecos "PolyVane bot" "$USER_NAME"
fi
usermod -aG sudo "$USER_NAME"

# Ensure the new user has an authorized_keys file. We don't generate keys here —
# the operator is expected to deploy via SSH, so they must already have a key.
mkdir -p "/home/$USER_NAME/.ssh"
chmod 700 "/home/$USER_NAME/.ssh"
if [[ -f "/root/.ssh/authorized_keys" && ! -f "/home/$USER_NAME/.ssh/authorized_keys" ]]; then
    log "copying root authorized_keys to $USER_NAME"
    cp "/root/.ssh/authorized_keys" "/home/$USER_NAME/.ssh/authorized_keys"
    chmod 600 "/home/$USER_NAME/.ssh/authorized_keys"
fi
chown -R "$USER_NAME:$USER_NAME" "/home/$USER_NAME/.ssh"

# Allow passwordless sudo for the bot user. Operationally convenient for
# deploys; revoke if the threat model demands it.
echo "$USER_NAME ALL=(ALL) NOPASSWD:ALL" > "/etc/sudoers.d/90-$USER_NAME"
chmod 440 "/etc/sudoers.d/90-$USER_NAME"

# ---- 2b. Verify the bot user can actually be reached over SSH -------------
# Belt to the preflight braces: even if /root had a key, the copy above can
# be skipped (e.g. if a stale empty authorized_keys already exists for the
# bot user). Re-check before we disable root SSH.

USER_AUTH_KEYS="/home/$USER_NAME/.ssh/authorized_keys"
user_key_count="$(_count_valid_keys "$USER_AUTH_KEYS")"
if (( user_key_count == 0 )); then
    cat >&2 <<EOF
[fatal] No usable SSH public key found in $USER_AUTH_KEYS.

Refusing to disable root SSH — that would leave you locked out (root SSH off,
$USER_NAME with no key, password auth off). Either:

  a) Add a key for $USER_NAME, then re-run this script:
       cp $ROOT_AUTH_KEYS $USER_AUTH_KEYS
       chmod 600 $USER_AUTH_KEYS
       chown $USER_NAME:$USER_NAME $USER_AUTH_KEYS

  b) Or just delete the empty file and re-run — this script will copy
       /root/.ssh/authorized_keys in for you on the next pass:
       rm -f $USER_AUTH_KEYS
EOF
    exit 2
fi
log "verified: $user_key_count usable key(s) in $USER_AUTH_KEYS — safe to disable root SSH"

# ---- 3. SSH hardening -------------------------------------------------------

log "hardening sshd: PermitRootLogin no, PasswordAuthentication no"
SSHD_CONFIG="/etc/ssh/sshd_config"
sed -i 's/^[#[:space:]]*PermitRootLogin.*/PermitRootLogin no/' "$SSHD_CONFIG"
sed -i 's/^[#[:space:]]*PasswordAuthentication.*/PasswordAuthentication no/' "$SSHD_CONFIG"
# In Ubuntu 24.04, distro overrides live in /etc/ssh/sshd_config.d/.
mkdir -p /etc/ssh/sshd_config.d
cat > /etc/ssh/sshd_config.d/99-polyvane-hardening.conf <<EOF
PermitRootLogin no
PasswordAuthentication no
ChallengeResponseAuthentication no
EOF
systemctl reload ssh || systemctl reload sshd || warn "could not reload sshd"

# ---- 4. Firewall ------------------------------------------------------------

log "ufw: allow SSH + API port"
ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp
# API for the remote dashboard. If you put nginx in front later, drop this
# and only expose 443.
ufw allow ${API_PORT}/tcp
ufw --force enable

# ---- 5. Directory layout ----------------------------------------------------

log "creating $INSTALL_DIR layout"
mkdir -p "$INSTALL_DIR"/{data,logs,config}

# ---- 6. Python venv ---------------------------------------------------------

if [[ ! -d "$INSTALL_DIR/venv" ]]; then
    log "creating venv at $INSTALL_DIR/venv"
    python3 -m venv "$INSTALL_DIR/venv"
fi
"$INSTALL_DIR/venv/bin/pip" install --upgrade pip wheel

# ---- 7. .env stub -----------------------------------------------------------

if [[ ! -f "$INSTALL_DIR/.env" ]]; then
    log "creating $INSTALL_DIR/.env stub"
    cat > "$INSTALL_DIR/.env" <<'EOF'
# Production env — fill in before flipping to live mode.
# Defaults to paper trading.

PK=
WALLET_ADDRESS=

CLOB_API_KEY=
CLOB_SECRET=
CLOB_PASS_PHRASE=

TRADING_MODE=paper

DISCORD_WEBHOOK_URL=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
HEALTHCHECK_URL=

POLYGON_RPC_URL=https://polygon-rpc.com

LOGS_DIR=/opt/polyvane/logs
HEARTBEAT_FILE=/opt/polyvane/data/heartbeat

# API — fill these in before starting polyvane-api.service.
# Generate with: python -c "import secrets; print(secrets.token_urlsafe(32))"
API_SECRET_KEY=
DASHBOARD_ORIGINS=https://cristobalgrana.me,http://localhost:5173
EOF
fi
chmod 600 "$INSTALL_DIR/.env"

# ---- 8. Install deploy assets to canonical paths ----------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
log "installing systemd unit + helper scripts"
install -m 0644 "$SCRIPT_DIR/polyvane.service"      /etc/systemd/system/$SERVICE_NAME.service
install -m 0644 "$SCRIPT_DIR/polyvane-api.service"  /etc/systemd/system/$API_SERVICE_NAME.service
install -m 0755 "$SCRIPT_DIR/healthcheck.sh"        /usr/local/bin/polyvane-healthcheck
install -m 0644 "$SCRIPT_DIR/logrotate.conf"        /etc/logrotate.d/polyvane

systemctl daemon-reload
systemctl enable "$SERVICE_NAME.service"
systemctl enable "$API_SERVICE_NAME.service"

# ---- 9. Healthcheck cron ----------------------------------------------------

CRON_FILE="/etc/cron.d/polyvane-healthcheck"
cat > "$CRON_FILE" <<EOF
# PolyVane healthcheck — runs every 5 minutes as root so it can restart the
# service if it has wedged. Sources /opt/polyvane/.env for DISCORD_WEBHOOK_URL
# and HEALTHCHECK_URL.
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
$HEALTHCHECK_INTERVAL root /usr/local/bin/polyvane-healthcheck >> /var/log/polyvane-healthcheck.log 2>&1
EOF
chmod 644 "$CRON_FILE"

# ---- 10. Ownership ----------------------------------------------------------

chown -R "$USER_NAME:$USER_NAME" "$INSTALL_DIR"
chmod 600 "$INSTALL_DIR/.env"

# ---- 11. Summary ------------------------------------------------------------

cat <<EOF

============================================================
Server provisioning complete.

Installation:
  user           $USER_NAME
  install dir    $INSTALL_DIR
  venv           $INSTALL_DIR/venv
  service        $SERVICE_NAME.service (enabled, NOT started yet)
  api service    $API_SERVICE_NAME.service (enabled, NOT started yet)
  api port       $API_PORT/tcp (open in ufw)
  env file       $INSTALL_DIR/.env (chmod 600)
  log dir        $INSTALL_DIR/logs
  healthcheck    cron every 5 minutes -> polyvane-healthcheck

Next steps (from your local machine):

  1. Push the bot code:
       POLYVANE_SSH=$USER_NAME@<vps-ip> ./deploy/deploy.sh
     This rsyncs the project, installs requirements into the venv,
     and starts the service in PAPER mode.

  2. Tail the logs:
       ssh $USER_NAME@<vps-ip> 'tail -f /opt/polyvane/logs/polyvane.log'
     or:
       make logs

  3. Generate the dashboard API secret and start the API service:
       a. python -c "import secrets; print(secrets.token_urlsafe(32))"
       b. Paste the value as API_SECRET_KEY=... in $INSTALL_DIR/.env
       c. sudo systemctl start $API_SERVICE_NAME
       d. Verify:  curl http://<vps-ip>:$API_PORT/api/v1/ping

  4. When ready to flip to live trading:
       a. Derive API creds locally with: PK=0x... python -m core.derive_creds
       b. Paste PK and creds into $INSTALL_DIR/.env on the VPS
       c. Run: make go-live

============================================================
EOF
