.PHONY: lint test typecheck check

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy ingest devlab

test:
	uv run pytest -v

check: lint typecheck test

.PHONY: compose-up compose-down test-integration

compose-up:
	docker compose -f docker/compose.yaml up -d --wait kafka

# --profile live is required or `producers` is left running: `down` skips
# services behind a profile that wasn't activated, and reports success anyway.
compose-down:
	docker compose -f docker/compose.yaml --profile live down -v

test-integration: compose-up
	RUN_INTEGRATION=1 uv run pytest tests/integration -v

.PHONY: stream-local notebook notebook-test notebook-clean

LOCAL_BOOTSTRAP ?= localhost:9092

# The local path to actually having data to look at. `compose-up` alone gives
# an empty broker: auto-create is off, so the topics must be made first, and
# the `producers` compose service cannot reach the broker from inside a
# sibling container (see README "Known limitations"). Running the producer on
# the host sidesteps that entirely. Runs in the foreground -- leave it in its
# own terminal and open the notebooks in another.
stream-local: compose-up
	uv run python -m scripts.create_topics --bootstrap $(LOCAL_BOOTSTRAP) --replication-factor 1
	INGEST_BOOTSTRAP_SERVERS=$(LOCAL_BOOTSTRAP) uv run python -m ingest.cli

notebook:
	uv sync --group notebook
	uv run --group notebook jupyter lab notebooks/

# devlab's pandas-dependent tests skip under plain `make test`, which runs
# without the notebook group. This runs the whole suite with it installed.
notebook-test:
	uv sync --group notebook
	uv run --group notebook pytest tests/devlab -v

notebook-clean:
	uv run --group notebook nbstripout notebooks/*.ipynb

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
