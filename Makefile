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

.PHONY: up down rebuild smoke

PROJECT ?= fdai
DEV := infra/envs/dev

up:
	./scripts/bootstrap.sh

down:
	terraform -chdir=$(DEV) destroy -auto-approve -var="msk_public_access=true" || true
	terraform -chdir=infra/bootstrap destroy -auto-approve

rebuild: down up

smoke:
	uv run python -m scripts.smoke_test \
	  --bootstrap "$$(terraform -chdir=$(DEV) output -raw bootstrap_brokers_public)" \
	  --username  "$$(terraform -chdir=$(DEV) output -raw sasl_username)" \
	  --password  "$$(terraform -chdir=$(DEV) output -raw sasl_password)"
