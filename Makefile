.PHONY: install install-dev install-kafka test test-unit test-integration lint format typecheck clean backtest run-paper help

PYTHON ?= python3

help:
	@echo "Available targets:"
	@echo "  install         Install runtime dependencies (pydantic only)"
	@echo "  install-dev     Install runtime + dev dependencies (pytest, ruff, mypy)"
	@echo "  install-kafka   Install with the optional Kafka backend"
	@echo "  test            Run the full pytest suite"
	@echo "  test-unit       Run unit tests only (skip integration)"
	@echo "  test-integration Run integration tests only"
	@echo "  lint            Run ruff"
	@echo "  format          Run ruff --fix"
	@echo "  typecheck       Run mypy on the src tree"
	@echo "  backtest        Run the example backtest"
	@echo "  run-paper       Run the example paper-trading app (Ctrl-C to stop)"
	@echo "  clean           Remove caches and build artifacts"

install:
	$(PYTHON) -m pip install -e .

install-dev:
	$(PYTHON) -m pip install -e ".[dev]"

install-kafka:
	$(PYTHON) -m pip install -e ".[kafka]"

test:
	$(PYTHON) -m pytest tests/ -v

test-unit:
	$(PYTHON) -m pytest tests/unit -v

test-integration:
	$(PYTHON) -m pytest tests/integration -v

lint:
	$(PYTHON) -m ruff check src tests

format:
	$(PYTHON) -m ruff check --fix src tests
	$(PYTHON) -m ruff format src tests

typecheck:
	$(PYTHON) -m mypy src/trading

backtest:
	$(PYTHON) -m trading.runners.run_backtest --config configs/backtest_example.toml

run-paper:
	$(PYTHON) -m trading.runners.run_live --config configs/paper_example.toml

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf build dist
