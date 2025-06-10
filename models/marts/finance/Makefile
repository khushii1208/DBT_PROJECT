.PHONY: help setup daily weekly full-refresh test docs clean

help:
	@echo "Available commands:"
	@echo "  setup        - Install dependencies and seed data"
	@echo "  daily        - Run daily refresh job"
	@echo "  weekly       - Run weekly finance job"
	@echo "  full-refresh - Full refresh of entire warehouse"
	@echo "  test         - Run all tests"
	@echo "  docs         - Generate and serve documentation"

setup:
	dbt deps
	dbt seed
	@echo "Setup completed!"

daily:
	@echo "Starting daily refresh..."
	dbt run --models tag:staging
	dbt run --models tag:core
	dbt test --models tag:staging tag:core
	@echo "Daily refresh completed!"

weekly:
	@echo "Starting weekly finance refresh..."
	dbt run --models tag:staging
	dbt run --models tag:core
	dbt run --models tag:finance
	dbt test
	dbt docs generate
	@echo "Weekly finance refresh completed!"

full-refresh:
	@echo "Starting full refresh..."
	dbt seed --full-refresh
	dbt run --full-refresh
	dbt test
	dbt docs generate
	@echo "Full refresh completed!"

test:
	dbt test

docs:
	dbt docs generate
	dbt docs serve --port 8080

clean:
	dbt clean