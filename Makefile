# Game Trend Lakehouse — convenience targets.
# Tested on Linux / macOS with Python 3.10+.

PYTHON ?= python3
VENV   ?= .venv
PIP    := $(VENV)/bin/pip
PY     := $(VENV)/bin/python

.PHONY: help install test extract-sample clean

help:
	@echo "Targets:"
	@echo "  install         Create venv and install requirements."
	@echo "  test            Run pytest against pure-Python transforms."
	@echo "  extract-sample  Pull a small Steam/SteamSpy sample to data/raw/."
	@echo "  clean           Remove venv and build artefacts."

$(VENV)/bin/activate:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

install: $(VENV)/bin/activate

test: install
	$(PY) -m pytest -q

extract-sample: install
	$(PY) -m game_trends.extract_steam --limit $${GAME_TRENDS_SAMPLE_N:-25}

clean:
	rm -rf $(VENV) build dist *.egg-info .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
