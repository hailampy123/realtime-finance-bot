.PHONY: lint test typecheck check

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy ingest

test:
	uv run pytest -v

check: lint typecheck test
