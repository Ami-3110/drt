.PHONY: install dev lint fmt test clean topics sync-labels

install:
	uv pip install -e .

dev:
	uv pip install -e ".[dev,bigquery]"

lint:
	ruff check drt tests
	mypy drt

fmt:
	ruff format drt tests
	ruff check --fix drt tests

test:
	pytest

clean:
	rm -rf dist build .eggs *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +

# ── Repo maintenance (maintainer only) ───────────────────────────────────────

topics:  ## Sync repository topics to GitHub
	gh repo edit drt-hub/drt \
	  --add-topic reverse-etl \
	  --add-topic dbt \
	  --add-topic bigquery \
	  --add-topic duckdb \
	  --add-topic python \
	  --add-topic cli \
	  --add-topic etl \
	  --add-topic data-engineering \
	  --add-topic postgres

sync-labels:  ## Trigger label sync workflow on GitHub
	gh workflow run sync-labels.yml --repo drt-hub/drt
