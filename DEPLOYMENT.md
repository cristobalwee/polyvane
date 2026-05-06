# Deployment

This guide deploys PolyVane to a fresh Ubuntu 24.04 VPS. The bot starts in
**paper mode** — no funds, no API keys, no risk. Switching to live trading
is a separate, deliberate flip.

## Prerequisites

- An Ubuntu 24.04 VPS (Hetzner CX22 / 2 vCPU / 4 GB RAM is plenty)
- An SSH key on your local machine (`~/.ssh/id_*.pub`) added to the VPS
- `rsync`, `ssh`, `make`, and `python3.12+` locally
- A wallet private key on Polygon **(only required for live trading — skip for paper)**

## One-time server provisioning

SSH in as root, clone or scp the project, then run the setup script:

```bash
ssh root@<vps-ip>
git clone <your-repo-url> /tmp/polyvane   # or scp the project
bash /tmp/polyvane/deploy/setup-server.sh
```

This:

- Creates a non-root `polyvane` user with sudo, copies your root authorized_keys
- Disables root SSH and password auth (key-only)
- Enables UFW with only port 22 open
- Installs Python 3.12, the venv, build deps
- Lays out `/opt/polyvane/{data,logs,config}`
- Installs the systemd unit (enabled, not started)
- Installs logrotate + a 5-minute healthcheck cron
- Writes `/opt/polyvane/.env` with paper-mode defaults (chmod 600)

Disconnect after it finishes — you'll connect as `polyvane` from now on.

## First deployment (paper mode)

From your local machine:

```bash
export POLYVANE_SSH=polyvane@<vps-ip>
make deploy
```

`make deploy` rsyncs the code (excluding `.env`, `data/`, `logs/`,
`.live-trading-enabled`), installs requirements into the venv, and starts the
service. It should complete in well under 30 seconds.

You should see `EXECUTION MODE: paper` in the tail-output. The bot is now
discovering Polymarket V2 markets, fetching forecasts, and emitting structured
log events — without touching any wallet.

## Day-to-day monitoring

```bash
make status         # systemctl status + last 20 log lines
make logs           # tail -f polyvane.log (the everything log)
make logs-trades    # tail -f trades.log (TRADE-level events only)
make logs-errors    # tail -f polyvane-error.log (uncaught crashes)
```

If you set `DISCORD_WEBHOOK_URL` in `/opt/polyvane/.env`, the in-process
alert bus posts trade events, drawdown warnings, and circuit-breaker trips.
The healthcheck cron also posts to Discord on `service down` / `heartbeat
stalled` events.

If you set `HEALTHCHECK_URL` to a healthchecks.io endpoint, the cron pings
it every 5 minutes (`/fail` on failure, naked URL on success) — so you'll
get a phone push if the VPS itself goes silent.

## Switching paper → live

1. **On your local machine**, derive your V2 API credentials:
   ```bash
   PK=0x<your-key> make derive-creds
   ```
   This prints `CLOB_API_KEY`, `CLOB_SECRET`, `CLOB_PASS_PHRASE`.

2. SSH in and edit `/opt/polyvane/.env`. Set:
   - `PK=0x...`
   - `WALLET_ADDRESS=0x...`
   - The three `CLOB_*` values from step 1
   - `POLYGON_RPC_URL=...` (a private RPC if you have one — public works for low volume)

3. Fund the wallet with **pUSD** on Polygon (V2 collateral, replaces USDC.e).
   Use Polymarket's CollateralOnramp at
   `0x93070a847efEf7F70739046A929D47a521F5B8ee` to wrap USDC into pUSD.

4. Flip the bot:
   ```bash
   make go-live
   ```
   This sets `TRADING_MODE=live`, creates `/opt/polyvane/.live-trading-enabled`,
   and restarts the service. The startup banner should now read
   `Mode: LIVE`.

To go back to paper:
```bash
make go-paper
```

The `.live-trading-enabled` safety file is the failsafe: if you set
`TRADING_MODE=live` without the file, the bot exits at startup with a
`FATAL` log line.

## Updating the bot

Just push again:
```bash
make deploy
```

Code changes take effect on restart (which `deploy` triggers automatically).
Config changes (`config/config.yaml`) require a `make restart` — they're
loaded at process start.

The deploy script preserves `/opt/polyvane/.env`, `data/`, `logs/`, and
`.live-trading-enabled` across deploys. Your trade journal SQLite, your
wallet key, and your live/paper flag all survive code updates.

## Log format reference (grep cheatsheet)

All log lines follow:
```
[<ISO 8601 timestamp>] [<LEVEL>] [<logger name>] <event-prefix> | key=value | ...
```

Useful greps:

| What | Command |
|------|---------|
| All trades today | `grep TRADE_PAPER\\|TRADE_LIVE polyvane.log` |
| All rejections | `grep TRADE_REJECTED polyvane.log` |
| Risk events | `grep '^\\[.*\\] \\[.*\\] \\[.*\\] RISK_' polyvane.log` |
| API errors | `grep HEALTH_API_ERROR polyvane.log` |
| Heartbeats | `grep HEALTH_HEARTBEAT polyvane.log` |
| Slow scans | `grep SCAN_COMPLETE polyvane.log \| awk -F'duration=' '$2 > 5'` |

Event prefixes used by the bot:

- `SCAN_START`, `SCAN_COMPLETE` — strategy scan boundaries
- `TRADE_SIGNAL`, `TRADE_REJECTED`, `TRADE_PAPER`, `TRADE_LIVE`,
  `TRADE_FILLED`, `TRADE_RESOLVED` — trade lifecycle (TRADE level → trades.log)
- `RISK_CIRCUIT_BREAKER`, `RISK_POSITION_LIMIT`, `RISK_CHECK` — risk events
- `HEALTH_HEARTBEAT`, `HEALTH_API_ERROR`, `HEALTH_WALLET` — system health

## Security notes

- **`.env` is `chmod 600` and owned by `polyvane`.** Never commit it; the
  deploy script's rsync excludes it.
- The systemd unit runs as the unprivileged `polyvane` user with
  `ProtectSystem=strict`, `NoNewPrivileges=true`, and write access only to
  `/opt/polyvane/{data,logs}`. A bot RCE cannot mutate the venv or system.
- Root SSH and password auth are disabled by `setup-server.sh`. Make sure
  your key works as `polyvane` *before* you log out as root.
- The `.live-trading-enabled` file gate is the last-line defense against
  accidentally enabling live trading. If `TRADING_MODE=live` and the file
  is missing, the bot refuses to start. Don't `touch` it manually unless
  you've actually finished the live setup.

## Troubleshooting

**`FATAL | Cannot start in LIVE mode — safety file missing`**
You set `TRADING_MODE=live` in `.env` but `/opt/polyvane/.live-trading-enabled`
doesn't exist. Either run `make go-live` (which creates it), or `make
go-paper` if you didn't actually want live mode.

**`FATAL | Live mode requires PK in environment`**
`PK=` is empty in `/opt/polyvane/.env`. Paste your wallet key in.

**`FATAL | Live mode requires CLOB_API_KEY, CLOB_SECRET, CLOB_PASS_PHRASE`**
Run `PK=0x... make derive-creds` locally and paste the three values into
`/opt/polyvane/.env`.

**Service starts but heartbeat never updates**
Check `make logs-errors`. Most often: a strategy `setup()` is hanging on
a network call, or `data/` isn't writable. The healthcheck will restart
the service after 20 minutes of stale heartbeat — escape valve, not a fix.

**`HEALTH_API_ERROR | endpoint=get_order_book | status=429`**
You're hitting rate limits. Lower `polling.market_scan_interval_sec` in
`config.yaml`, or get a private CLOB API allowance.

**Bot won't pick up edited `config.yaml`**
Config is loaded at process start. Run `make restart`.

**Can't SSH in as `polyvane`**
The setup script copies `/root/.ssh/authorized_keys` to the polyvane user,
but only if it exists at provision time. If you added your key after, copy
it manually: `sudo cp /root/.ssh/authorized_keys /home/polyvane/.ssh/ && sudo
chown polyvane:polyvane /home/polyvane/.ssh/authorized_keys`.

**Want to see the dashboard against production data**
```bash
make remote-dashboard
```
This scp's the SQLite journal locally and runs `monitoring.dashboard`
against it — no new SSH session per refresh.
