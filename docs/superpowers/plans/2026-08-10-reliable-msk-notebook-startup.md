# Reliable MSK Notebook Startup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `make notebook TARGET=msk` refresh credentials and Terraform-managed laptop access, verify Kafka, and launch a correctly configured notebook without ad-hoc cells or shell exports.

**Architecture:** A focused Python preflight owns external command orchestration and is unit-tested with the AWS/Terraform/Kafka boundaries replaced. The Make target selects local or MSK mode and passes one clean environment through preflight and Jupyter. `devlab.from_terraform()` gains an AWS API fallback for the provider's transient empty broker output.

**Tech Stack:** Python 3.12, pytest, GNU Make, Terraform, AWS CLI v2, confluent-kafka through `devlab`.

## Global Constraints

- Only Terraform may mutate security-group rules.
- Local notebook mode must perform no AWS calls.
- A missing stack must fail before `terraform apply`.
- Named-profile execution must remove stale raw AWS credential variables.
- Notebook analysis remains read-only.
- Do not print the SASL password.

---

### Task 1: Live endpoint fallback

**Files:**
- Modify: `devlab/config.py`
- Test: `tests/devlab/test_config.py`

**Interfaces:**
- Consumes: `_terraform_output(chdir: str | Path, name: str) -> str`
- Produces: `_aws_public_brokers(cluster_arn: str) -> str`; `from_terraform()` that falls back when the Terraform broker output is empty.

- [ ] **Step 1: Write failing tests**

Add a case where Terraform returns an empty public broker output and a valid
cluster ARN, while the AWS CLI returns a public broker. Assert that
`from_terraform()` builds a usable `Target`. Add failure cases for an empty AWS
result and invalid AWS credentials.

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run pytest tests/devlab/test_config.py -q
```

Expected: the existing empty-output exception prevents fallback.

- [ ] **Step 3: Implement the minimal fallback**

Make `_terraform_output()` return its actual output, including empty. Derive
the Kafka region from `cluster_arn`, invoke `aws kafka
get-bootstrap-brokers`, and use the result only when the Terraform output is
empty.

- [ ] **Step 4: Verify GREEN**

```bash
uv run pytest tests/devlab/test_config.py -q
```

Expected: all config tests pass.

### Task 2: Tested MSK notebook preflight

**Files:**
- Create: `scripts/prepare_msk_notebook.py`
- Create: `tests/test_prepare_msk_notebook.py`

**Interfaces:**
- Produces: `clean_aws_environment(profile: str) -> dict[str, str]`
- Produces: `prepare(profile: str, dev_dir: Path) -> None`
- CLI: `python -m scripts.prepare_msk_notebook --profile NAME`

- [ ] **Step 1: Write failing tests**

Test valid credentials, expired SSO credentials followed by one login, stale
raw credential removal, missing-stack refusal before apply, current-IP
validation, exact final-state Terraform variables, and Kafka metadata
verification.

- [ ] **Step 2: Verify RED**

```bash
uv run pytest tests/test_prepare_msk_notebook.py -q
```

Expected: collection fails because the module does not exist.

- [ ] **Step 3: Implement minimal orchestration**

Use `subprocess.run` for AWS/Terraform, `urllib.request.urlopen` for public IP,
and `devlab.topics(devlab.from_terraform(dev_dir))` for the final access gate.
Raise a single `PreparationError` with actionable messages.

- [ ] **Step 4: Verify GREEN**

```bash
uv run pytest tests/test_prepare_msk_notebook.py -q
```

Expected: all preflight tests pass.

### Task 3: One-command Make interface and documentation

**Files:**
- Modify: `Makefile`
- Create: `tests/test_notebook_make_target.py`
- Modify: `README.md`
- Modify: `notebooks/README.md`
- Modify: `docs/AWS_DEPLOYMENT_DEBUGGING.md`

**Interfaces:**
- CLI: `make notebook TARGET=local`
- CLI: `make notebook TARGET=msk [FDAI_AWS_PROFILE=name]`

- [ ] **Step 1: Write failing Make dry-run tests**

Execute `make -n notebook TARGET=local` and `TARGET=msk`. Assert that local
mode omits the preflight and AWS environment, while MSK mode includes the
preflight and launches Jupyter with `FDAI_TARGET=terraform`.

- [ ] **Step 2: Verify RED**

```bash
uv run pytest tests/test_notebook_make_target.py -q
```

Expected: the current target-independent recipe fails the assertions.

- [ ] **Step 3: Implement Make target and docs**

Validate `TARGET`, choose the profile with `FDAI_AWS_PROFILE` overriding
`AWS_PROFILE` and `fdai-sandbox` as fallback, remove raw credential variables,
run the preflight only for MSK, and launch Jupyter with the resolved
`FDAI_TARGET`.

- [ ] **Step 4: Verify focused and full suites**

```bash
uv run pytest tests/test_notebook_make_target.py tests/test_prepare_msk_notebook.py tests/devlab/test_config.py -q
make check
```

Expected: all tests, lint, formatting, and type checking pass.

- [ ] **Step 5: Verify against live MSK**

```bash
uv run python -m scripts.prepare_msk_notebook --profile fdai-sandbox
AWS_PROFILE=fdai-sandbox uv run python -c 'import devlab; print(devlab.rate(devlab.from_terraform(), seconds=15))'
```

Expected: preflight succeeds and the original notebook read completes without
`BrokerUnavailable`.
