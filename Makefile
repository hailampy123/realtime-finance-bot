.PHONY: lint test typecheck check

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy ingest

test:
	uv run pytest -v

check: lint typecheck test

.PHONY: compose-up compose-down test-integration

compose-up:
	docker compose -f docker/compose.yaml up -d --wait kafka

compose-down:
	docker compose -f docker/compose.yaml down -v

test-integration: compose-up
	RUN_INTEGRATION=1 uv run pytest tests/integration -v
