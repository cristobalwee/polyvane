#!/usr/bin/env bash
# Push code + restart. Runs on your LOCAL machine.
#
#   POLYVANE_SSH=polyvane@1.2.3.4 ./deploy/deploy.sh
#   ./deploy/deploy.sh polyvane@1.2.3.4
#
# What it does:
#   1. rsync the project to /opt/polyvane/ (excluding env, vcs, caches, logs)
#   2. install requirements into the remote venv
#   3. systemctl restart polyvane
#   4. show the last 10 log lines
#
# Should complete in well under 30s on a small project.

set -euo pipefail

REMOTE="${1:-${POLYVANE_SSH:-}}"
if [[ -z "$REMOTE" ]]; then
    echo "ERROR: pass user@host as first arg or set POLYVANE_SSH" >&2
    echo "Usage: $0 polyvane@<vps-ip>" >&2
    exit 2
fi

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_DIR="/opt/polyvane"

cd "$PROJECT_ROOT"

echo "==> rsync $PROJECT_ROOT -> $REMOTE:$INSTALL_DIR"
rsync -az --delete \
    --exclude='.git/' \
    --exclude='.venv/' \
    --exclude='venv/' \
    --exclude='__pycache__/' \
    --exclude='*.pyc' \
    --exclude='.env' \
    --exclude='.env.*' \
    --exclude='data/' \
    --exclude='logs/' \
    --exclude='.live-trading-enabled' \
    --exclude='.backtest-cache/' \
    --exclude='.calibrate-cache/' \
    --exclude='.DS_Store' \
    --exclude='.claude/' \
    --exclude='secrets/' \
    --rsync-path='sudo rsync' \
    "$PROJECT_ROOT/" "$REMOTE:$INSTALL_DIR/"

echo "==> installing requirements into remote venv"
# shellcheck disable=SC2087
ssh "$REMOTE" bash <<'REMOTE_EOF'
set -euo pipefail
cd /opt/polyvane
sudo chown -R polyvane:polyvane /opt/polyvane
/opt/polyvane/venv/bin/pip install --quiet --upgrade -r requirements.txt
echo "==> restarting polyvane.service"
sudo systemctl restart polyvane && RESTART_OK=1 || RESTART_OK=0
sleep 2
echo "==> systemctl status (head)"
systemctl --no-pager --full status polyvane | head -n 15 || true
echo "==> last 20 log lines:"
sudo tail -n 20 /opt/polyvane/logs/polyvane.log 2>/dev/null \
    || sudo journalctl -u polyvane -n 20 --no-pager
if [[ "$RESTART_OK" -eq 0 ]]; then
    echo "==> ERROR: polyvane.service failed to start (see logs above)"
    exit 1
fi
REMOTE_EOF

echo "==> done"
