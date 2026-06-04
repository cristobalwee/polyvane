.PHONY: help run run-live kalshi-paper-smoke dashboard backtest calibrate add-city verify-sources test lint \
        deploy logs logs-trades logs-errors status restart go-live go-paper scan-now \
        remote-dashboard derive-creds perf perf-7d perf-all remote-perf \
        report report-7d report-all remote-report \
        repair-journal-dry repair-journal-apply remote-repair-journal \
        api-run api-logs api-restart api-status

# Prefer the project venv when present — Make's subshell doesn't inherit
# the user's activated venv from their interactive shell, so falling back
# to a bare `python3` would resolve to the system framework Python and
# miss every project dep (aiohttp, yaml, etc.).
PY ?= $(if $(wildcard .venv/bin/python3),.venv/bin/python3,python3)
CONFIG ?= config/config.yaml
START ?= $(shell $(PY) -c "from datetime import date, timedelta; print(date.today() - timedelta(days=30))")
END ?= $(shell $(PY) -c "from datetime import date; print(date.today())")

# SSH target for the production VPS, e.g. POLYVANE_SSH=polyvane@1.2.3.4
SSH ?= $(POLYVANE_SSH)
INSTALL_DIR ?= /opt/polyvane

help:
	@echo "polyvane — common commands"
	@echo ""
	@echo " local"
	@echo "  make run             Start the bot in paper mode (config/config.yaml)"
	@echo "  make run-live        Start in live mode (requires LIVE=true env var)"
	@echo "  make kalshi-paper-smoke  One-shot Kalshi scan + paper execution test"
	@echo "  make dashboard       Show the local P&L dashboard"
	@echo "  make backtest        Run a backtest over the last 30 days (override START/END)"
	@echo "  make calibrate       Run the per-model + per-city calibration script"
	@echo "  make add-city        Interactive prompt for a new resolution-registry entry"
	@echo "  make verify-sources  Cross-check registry vs Polymarket's active markets"
	@echo "  make test            Run the test suite (pytest)"
	@echo "  make lint            Run ruff over the project"
	@echo "  make derive-creds    Derive Polymarket V2 API credentials from PK"
	@echo "  make perf            Per-strategy performance for today (UTC)"
	@echo "  make perf-7d         Per-strategy performance for the last 7 days"
	@echo "  make perf-all        Per-strategy lifetime performance"
	@echo "  make report          Full report: lifetime + today, calibration, Brier, recent settled"
	@echo "  make report-7d       Full report with 7d window"
	@echo "  make report-all      Full report (window collapses to lifetime)"
	@echo "  make repair-journal-dry   Dry-run scan of malformed metadata_json rows"
	@echo "  make repair-journal-apply Repair malformed metadata_json rows (writes backup first)"
	@echo "  make api-run         Run the FastAPI server locally on :8099"
	@echo ""
	@echo " remote (set POLYVANE_SSH=user@host)"
	@echo "  make deploy          rsync code + restart systemd"
	@echo "  make logs            tail -f polyvane.log"
	@echo "  make logs-trades     tail -f trades.log"
	@echo "  make logs-errors     tail -f polyvane-error.log"
	@echo "  make status          systemctl status + last 20 log lines"
	@echo "  make restart         systemctl restart polyvane"
	@echo "  make go-live         Switch the deployed bot to live mode"
	@echo "  make go-paper        Switch the deployed bot back to paper mode"
	@echo "  make scan-now        Trigger an immediate scan (restarts the service)"
	@echo "  make remote-dashboard scp the journal DB and run dashboard locally"
	@echo "  make remote-perf     scp the journal DB and run perf report locally"
	@echo "  make remote-report   scp the journal DB and run the full report locally"
	@echo "  make remote-repair-journal  Repair malformed metadata_json on the VPS journal"
	@echo "  make api-logs        tail the API service logs"
	@echo "  make api-restart     restart the API service"
	@echo "  make api-status      check API service health"
	@echo ""

# ---- local --------------------------------------------------------------

run:
	$(PY) main.py $(CONFIG)

run-live:
	@if [ "$(LIVE)" != "true" ]; then \
		echo "REFUSED: live mode requires LIVE=true (e.g. LIVE=true make run-live)"; \
		echo "        Also confirm execution.mode is 'live' in $(CONFIG)."; \
		exit 1; \
	fi
	@if [ ! -f .live-trading-enabled ]; then \
		echo "REFUSED: .live-trading-enabled file is missing."; \
		echo "        Create it with: touch .live-trading-enabled"; \
		exit 1; \
	fi
	TRADING_MODE=live $(PY) main.py $(CONFIG)

kalshi-paper-smoke:
	$(PY) -m scripts.kalshi_paper_smoke --config $(CONFIG)

dashboard:
	$(PY) -m monitoring.dashboard

backtest:
	$(PY) -m backtesting.runner --start $(START) --end $(END) --strategy weather --cache-dir .backtest-cache

calibrate:
	$(PY) -m strategies.weather.calibrate

add-city:
	$(PY) -m scripts.add_city

verify-sources:
	$(PY) -m scripts.verify_sources

test:
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check .

derive-creds:
	@if [ -z "$$PK" ]; then echo "ERROR: set PK=0x... in env"; exit 2; fi
	$(PY) -m core.derive_creds

perf:
	$(PY) -m monitoring.perf_report --since today

perf-7d:
	$(PY) -m monitoring.perf_report --since 7d

perf-all:
	$(PY) -m monitoring.perf_report --all

# Full per-strategy report — lifetime + windowed, with calibration and Brier.
# This is the "is the daily summary lying to me?" tool — run it any time.
report:
	$(PY) -m monitoring.report --since today

report-7d:
	$(PY) -m monitoring.report --since 7d

# `--since 7d` is the widest window the CLI accepts; for "lifetime" the
# windowed table will essentially equal lifetime once you've been running
# longer than that. Use `make report-all` for an effectively-lifetime view.
report-all:
	$(PY) -m monitoring.report --since 36500d

# Run the API locally. Reads .env for API_SECRET_KEY + DASHBOARD_ORIGINS.
# `--reload` keeps it convenient for dashboard development.
api-run:
	$(PY) -m uvicorn api.server:app --host 127.0.0.1 --port 8099 --reload

# ---- remote -------------------------------------------------------------

_check-ssh:
	@if [ -z "$(SSH)" ]; then \
		echo "ERROR: set POLYVANE_SSH=user@host (e.g. polyvane@1.2.3.4)"; exit 2; \
	fi

deploy: _check-ssh
	./deploy/deploy.sh $(SSH)

logs: _check-ssh
	ssh -t $(SSH) "sudo tail -f $(INSTALL_DIR)/logs/polyvane.log"

logs-trades: _check-ssh
	ssh -t $(SSH) "sudo tail -f $(INSTALL_DIR)/logs/trades.log"

logs-errors: _check-ssh
	ssh -t $(SSH) "sudo tail -f $(INSTALL_DIR)/logs/polyvane-error.log"

status: _check-ssh
	ssh -t $(SSH) "systemctl --no-pager --full status polyvane | head -n 20 && echo --- && sudo tail -n 20 $(INSTALL_DIR)/logs/polyvane.log"

restart: _check-ssh
	ssh -t $(SSH) "sudo systemctl restart polyvane && sleep 2 && systemctl --no-pager status polyvane | head -n 12"

# Switch the deployed bot to Kalshi LIVE mode. Requires Kalshi creds already
# pasted into /opt/polyvane/.env. Polymarket stays paper by leaving
# TRADING_MODE=paper.
go-live: _check-ssh
	@echo "==> switching $(SSH) to KALSHI live mode"
	ssh $(SSH) "sudo grep -q '^KALSHI_MODE=' $(INSTALL_DIR)/.env && \
	             sudo sed -i 's/^KALSHI_MODE=.*/KALSHI_MODE=live/' $(INSTALL_DIR)/.env || \
	             echo KALSHI_MODE=live | sudo tee -a $(INSTALL_DIR)/.env >/dev/null && \
	             sudo sed -i 's/^TRADING_MODE=.*/TRADING_MODE=paper/' $(INSTALL_DIR)/.env && \
	             sudo touch $(INSTALL_DIR)/.live-trading-enabled && \
	             sudo chown polyvane:polyvane $(INSTALL_DIR)/.live-trading-enabled && \
	             sudo systemctl restart polyvane && sleep 3 && \
	             systemctl --no-pager status polyvane | head -n 12 && \
	             sudo tail -n 20 $(INSTALL_DIR)/logs/polyvane.log"

go-paper: _check-ssh
	@echo "==> switching $(SSH) to PAPER mode"
	ssh $(SSH) "sudo sed -i 's/^TRADING_MODE=.*/TRADING_MODE=paper/' $(INSTALL_DIR)/.env && \
	             sudo grep -q '^KALSHI_MODE=' $(INSTALL_DIR)/.env && sudo sed -i 's/^KALSHI_MODE=.*/KALSHI_MODE=paper/' $(INSTALL_DIR)/.env || true && \
	             sudo rm -f $(INSTALL_DIR)/.live-trading-enabled && \
	             sudo systemctl restart polyvane && sleep 3 && \
	             systemctl --no-pager status polyvane | head -n 12"

scan-now: _check-ssh
	@echo "==> restarting service to force a fresh scan cycle"
	ssh $(SSH) "sudo systemctl restart polyvane && sleep 2 && sudo tail -n 30 $(INSTALL_DIR)/logs/polyvane.log"

# Pull the remote SQLite trade journal locally and run the dashboard against it.
remote-dashboard: _check-ssh
	@mkdir -p data
	scp $(SSH):$(INSTALL_DIR)/data/trade_journal.db data/trade_journal.remote.db
	$(PY) -m monitoring.dashboard --db data/trade_journal.remote.db || \
	    $(PY) -m monitoring.dashboard

# Same idea for the perf report. Override window with PERF_SINCE=7d, etc.
PERF_SINCE ?= today
remote-perf: _check-ssh
	@mkdir -p data
	scp $(SSH):$(INSTALL_DIR)/data/trade_journal.db data/trade_journal.remote.db
	$(PY) -m monitoring.perf_report --db data/trade_journal.remote.db --since $(PERF_SINCE)

# Full report against a fresh copy of the production journal.
# Override window with REPORT_SINCE=7d (default: today). Recent-trades
# count is REPORT_RECENT (default: 30).
REPORT_SINCE ?= today
REPORT_RECENT ?= 30
remote-report: _check-ssh
	@mkdir -p data
	scp $(SSH):$(INSTALL_DIR)/data/trade_journal.db data/trade_journal.remote.db
	$(PY) -m monitoring.report \
	    --db data/trade_journal.remote.db \
	    --since $(REPORT_SINCE) \
	    --recent $(REPORT_RECENT)

# Repair malformed metadata_json rows (legacy ±Infinity from edge buckets).
# `repair-journal-dry` reports without writing; `repair-journal-apply`
# writes a backup alongside the DB and then fixes the rows.
repair-journal-dry:
	$(PY) -m scripts.repair_journal_metadata

repair-journal-apply:
	$(PY) -m scripts.repair_journal_metadata --apply

# Repair the production journal in-place on the VPS. The script writes a
# timestamped .bak.* sibling first, then applies the fix in a single
# transaction, so a crash mid-write rolls back. Stop the bot first to
# avoid contending with concurrent writes:
#     make restart   # ... after verifying repair completed
remote-repair-journal: _check-ssh
	ssh $(SSH) "cd $(INSTALL_DIR) && \
	    sudo -u polyvane $(INSTALL_DIR)/.venv/bin/python3 -m scripts.repair_journal_metadata --apply"

# ---- API service (remote) ----------------------------------------------

api-logs: _check-ssh
	ssh -t $(SSH) "sudo journalctl -u polyvane-api -f -n 100"

api-restart: _check-ssh
	ssh -t $(SSH) "sudo systemctl restart polyvane-api && sleep 2 && systemctl --no-pager status polyvane-api | head -n 12"

# Service health: systemd state + an unauthenticated /ping check from the
# VPS itself (no API key needed for /ping).
api-status: _check-ssh
	ssh -t $(SSH) "systemctl --no-pager --full status polyvane-api | head -n 20 && echo --- && curl -sS -m 5 http://127.0.0.1:8099/api/v1/ping || echo 'ping failed'"

# Archive trade journal and wipe logs clean
journal-archive: _check-ssh
	@echo "==> archiving journal + logs on $(SSH)"
	@read -p "Confirm archive (overwrites no data, renames only) [y/N] " ans; [ "$$ans" = "y" ]
	ssh $(SSH) "sudo systemctl stop polyvane polyvane-api && \
	    TS=\$$(date -u +%Y-%m-%dT%H%M%SZ) && \
	    sudo -u polyvane mv $(INSTALL_DIR)/data/trade_journal.db $(INSTALL_DIR)/data/trade_journal.archive-\$$TS.db && \
	    sudo -u polyvane mv $(INSTALL_DIR)/logs/polyvane.log $(INSTALL_DIR)/logs/polyvane.archive-\$$TS.log && \
	    sudo -u polyvane mv $(INSTALL_DIR)/logs/trades.log $(INSTALL_DIR)/logs/trades.archive-\$$TS.log 2>/dev/null || true && \
	    sudo systemctl start polyvane polyvane-api && sleep 2 && \
	    systemctl --no-pager status polyvane | head -n 8"
