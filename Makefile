.PHONY: lint test typecheck check

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run --group lakehouse --group awsnative mypy ingest devlab lakehouse awsnative

test:
	uv run pytest -v

check: lint typecheck test

.PHONY: lakehouse-test

# pyspark needs a JDK, and openjdk@17 is installed via brew but not linked, so
# `java` is not on PATH. Setting JAVA_HOME here keeps that detail out of every
# developer's shell profile. spark-avro is fetched from Maven on first run and
# cached in ~/.ivy2 afterwards.
JAVA_HOME_17 ?= /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

lakehouse-test:
	JAVA_HOME=$(JAVA_HOME_17) uv run --group lakehouse pytest tests/lakehouse -v

.PHONY: compose-up compose-down test-integration

compose-up:
	docker compose -f docker/compose.yaml up -d --wait kafka

# --profile live is required or `producers` is left running: `down` skips
# services behind a profile that wasn't activated, and reports success anyway.
compose-down:
	docker compose -f docker/compose.yaml --profile live down -v

test-integration: compose-up
	RUN_INTEGRATION=1 uv run pytest tests/integration -v

.PHONY: stream-local notebook notebook-local notebook-msk notebook-test notebook-clean

LOCAL_BOOTSTRAP ?= 127.0.0.1:9092
TARGET ?= local
FDAI_AWS_PROFILE ?= $(if $(strip $(AWS_PROFILE)),$(AWS_PROFILE),fdai-sandbox)
CLEAN_AWS_ENV := env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY \
	-u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN

# The local path to actually having data to look at. `compose-up` alone gives
# an empty broker: auto-create is off, so the topics must be made first, and
# the `producers` compose service cannot reach the broker from inside a
# sibling container (see README "Known limitations"). Running the producer on
# the host sidesteps that entirely. Runs in the foreground -- leave it in its
# own terminal and open the notebooks in another.
stream-local: compose-up
	uv run python -m scripts.create_topics --bootstrap $(LOCAL_BOOTSTRAP) --replication-factor 1
	INGEST_BOOTSTRAP_SERVERS=$(LOCAL_BOOTSTRAP) uv run python -m ingest.cli

notebook: notebook-$(TARGET)

notebook-local:
	uv sync --group notebook
	FDAI_TARGET=local uv run --group notebook jupyter lab notebooks/

notebook-msk:
	uv sync --group notebook
	$(CLEAN_AWS_ENV) AWS_PROFILE=$(FDAI_AWS_PROFILE) \
		uv run python -m scripts.prepare_msk_notebook --profile $(FDAI_AWS_PROFILE)
	$(CLEAN_AWS_ENV) AWS_PROFILE=$(FDAI_AWS_PROFILE) FDAI_TARGET=terraform \
		uv run --group notebook jupyter lab notebooks/

# devlab's pandas-dependent tests skip under plain `make test`, which runs
# without the notebook group. This runs the whole suite with it installed.
notebook-test:
	uv sync --group notebook
	uv run --group notebook pytest tests/devlab -v

notebook-clean:
	uv run --group notebook nbstripout notebooks/*.ipynb

.PHONY: pipeline-validate pipeline-deploy pipeline-run pipeline-refresh-bronze pipeline-status

DB_PROFILE ?= tw
DB_TARGET  ?= dev

pipeline-validate:
	databricks bundle validate -t $(DB_TARGET) --profile $(DB_PROFILE)

pipeline-deploy:
	databricks bundle deploy -t $(DB_TARGET) --profile $(DB_PROFILE)

# Code changes take effect only after a deploy, so never run without one.
pipeline-run: pipeline-deploy
	databricks bundle run trades_bronze_silver -t $(DB_TARGET) --profile $(DB_PROFILE)

pipeline-status:
	databricks bundle summary -t $(DB_TARGET) --profile $(DB_PROFILE)

# THE ONLY SANCTIONED RECOVERY after the weekly AWS sandbox wipe.
#
# The wipe destroys MSK, so bronze_trades_stream's Kafka checkpoint references
# offsets on a topic that no longer exists. A whole-pipeline full refresh would
# fix that and ALSO full-refresh silver_trades -- destroying accumulated history
# whose source data is already gone. That is unrecoverable data loss.
#
# Refreshing only Bronze is safe: silver_trades is a keyed upsert, so replaying
# Bronze re-upserts the same (venue, trade_id) keys and converges.
#
# There is deliberately no target for a full-pipeline refresh.
pipeline-refresh-bronze: pipeline-deploy
	databricks bundle run trades_bronze_silver -t $(DB_TARGET) --profile $(DB_PROFILE) \
	  --full-refresh bronze_trades_stream

.PHONY: up down rebuild smoke unlock

PROJECT ?= fdai
DEV := infra/envs/dev

up:
	./scripts/bootstrap.sh

down:
	terraform -chdir=$(DEV) destroy -auto-approve -var="msk_public_access=true" || true
	terraform -chdir=infra/bootstrap destroy -auto-approve

rebuild: down up

# Recovery hatch. A cluster whose ACLs were enforced before
# scripts/create_acls.py ran denies every client, including one trying to add
# ACLs, so it cannot be repaired from a Kafka client — only by loosening the
# broker configuration again.
#
# One apply is enough despite touching both settings: the AWS provider updates
# connectivity before configuration, which is the wrong order for locking down
# but exactly the right one for backing out.
unlock:
	terraform -chdir=$(DEV) apply -auto-approve \
	  -var="msk_public_access=false" -var="msk_restrict_acls=false"

smoke:
	uv run python -m scripts.smoke_test \
	  --bootstrap "$$(terraform -chdir=$(DEV) output -raw bootstrap_brokers_public)" \
	  --username  "$$(terraform -chdir=$(DEV) output -raw sasl_username)" \
	  --password  "$$(terraform -chdir=$(DEV) output -raw sasl_password)"

.PHONY: up-aws down-aws rebuild-aws preflight-aws logs-aws validate-aws

NATIVE := infra/envs/native

preflight-aws:
	./scripts/native_preflight.sh

# Offline: -backend=false skips the S3 backend, so this needs no AWS
# credentials and can run in CI on a pull request. Deliberately not folded into
# `make check`, which must stay runnable with no cloud tooling installed at all.
validate-aws:
	terraform -chdir=$(NATIVE) init -backend=false -input=false >/dev/null
	terraform -chdir=$(NATIVE) validate
	terraform -chdir=$(NATIVE) fmt -check -recursive ../../modules
	@command -v tflint  >/dev/null && tflint --chdir=$(NATIVE) || echo "tflint not installed, skipped"
	@command -v checkov >/dev/null && checkov -d $(NATIVE) --quiet --compact || echo "checkov not installed, skipped"

up-aws:
	./scripts/native_up.sh

# force_destroy / force_delete are set on the lake bucket and the ECR repo, so
# a non-empty bucket or a repo with images does not block teardown. Everything
# here is re-derivable (spec section 6), which is what makes that safe.
down-aws:
	terraform -chdir=$(NATIVE) destroy -auto-approve

rebuild-aws: down-aws up-aws

logs-aws:
	aws logs tail "$$(terraform -chdir=$(NATIVE) output -raw producer_log_group)" --follow
