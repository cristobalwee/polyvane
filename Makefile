.PHONY: help run run-live dashboard backtest calibrate add-city verify-sources test lint \
        deploy logs logs-trades logs-errors status restart go-live go-paper scan-now \
        remote-dashboard derive-creds perf perf-7d perf-all remote-perf

PY ?= python
CONFIG ?= config/config.yaml
START ?= $(shell python -c "from datetime import date, timedelta; print(date.today() - timedelta(days=30))")
END ?= $(shell python -c "from datetime import date; print(date.today())")

# SSH target for the production VPS, e.g. POLYVANE_SSH=polyvane@1.2.3.4
SSH ?= $(POLYVANE_SSH)
INSTALL_DIR ?= /opt/polyvane

help:
	@echo "polyvane — common commands"
	@echo ""
	@echo " local"
	@echo "  make run             Start the bot in paper mode (config/config.yaml)"
	@echo "  make run-live        Start in live mode (requires LIVE=true env var)"
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

# Switch the deployed bot to LIVE mode. Requires creds already pasted into
# /opt/polyvane/.env (PK + CLOB_API_KEY + CLOB_SECRET + CLOB_PASS_PHRASE).
go-live: _check-ssh
	@echo "==> switching $(SSH) to LIVE mode"
	ssh $(SSH) "sudo sed -i 's/^TRADING_MODE=.*/TRADING_MODE=live/' $(INSTALL_DIR)/.env && \
	             sudo touch $(INSTALL_DIR)/.live-trading-enabled && \
	             sudo chown polyvane:polyvane $(INSTALL_DIR)/.live-trading-enabled && \
	             sudo systemctl restart polyvane && sleep 3 && \
	             systemctl --no-pager status polyvane | head -n 12 && \
	             sudo tail -n 20 $(INSTALL_DIR)/logs/polyvane.log"

go-paper: _check-ssh
	@echo "==> switching $(SSH) to PAPER mode"
	ssh $(SSH) "sudo sed -i 's/^TRADING_MODE=.*/TRADING_MODE=paper/' $(INSTALL_DIR)/.env && \
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
