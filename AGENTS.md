# Repository Agent Guide

## Scope

These instructions apply to the entire repository. A closer `AGENTS.md` or
`AGENTS.override.md` may refine them for a specific subtree.

## Project context

- Python 3.12 project managed with `uv`; do not use a global Python environment.
- Streaming market-data platform with shared ingestion contracts and two
  downstream implementations: Kafka/Databricks and AWS-native managed services.
- `ingest/` is cloud-neutral. The implementations may share its models, universe,
  and Avro schema, but diverge below the sink boundary.
- Start with `docs/README.md`, the documentation map. It names each document's job
  and its boundary. For system context, the current-state set is
  `docs/ARCHITECTURE.md` (topology), `docs/DATA_LAYER.md` (tables), and
  `docs/CODEBASE_EXPLAINED.md` (module boundaries).
- Treat design-only components as unimplemented unless the current-state
  documentation or the code proves otherwise. Specs under `docs/superpowers/` record
  intent at the time of writing, not current state.

## Working agreements

- Preserve unrelated user changes and follow existing structure and naming.
- Prefer the smallest complete change. Do not introduce dependencies, services,
  abstractions, or infrastructure unless the task requires them.
- Use `rg` and `rg --files` for repository search.
- Use `apply_patch` for focused file edits.
- Use `uv run` and existing Makefile targets for Python commands.
- Never add credentials, populated `tfvars`, private keys, `.env` files, copied
  production data, Terraform state, or generated `.terraform/` content.
- Update the relevant documentation when behavior, operations, or architecture
  changes.

## Verification

- Run the smallest relevant test while iterating.
- Before completing code changes, run `make check`:
  - Ruff lint and formatting check
  - strict mypy across the Python packages
  - the default pytest suite
- Add the specialized gate when the changed area requires it:
  - `make lakehouse-test` for Spark/lakehouse behavior
  - `make notebook-test` for notebook and devlab behavior
  - `make test-integration` for the local Kafka end-to-end path
  - `make validate-aws` for AWS-native Terraform and rendered SQL
- For documentation-only or agent-guidance changes, validate formatting and links
  proportionately and always run `git diff --check`.
- Report skipped tests and any gate that could not run; never imply it passed.

## Data and architecture invariants

- Preserve event-time semantics and microsecond timestamp normalization.
- Preserve deterministic natural keys, idempotent writes, quarantine behavior,
  and the bare Avro datum contract unless an approved design changes them.
- Keep `ingest/` free of AWS- or Databricks-specific dependencies.
- The AWS sandbox is ephemeral and wiped weekly. Unity Catalog in the permanent
  Databricks account is the durable system of record for that path.
- Never perform a full Databricks pipeline refresh. The only sanctioned recovery
  after a sandbox wipe is `make pipeline-refresh-bronze`; Silver is a keyed upsert.
- Do not edit generated Terraform files or state. Change source HCL and checked-in
  examples, then validate through the repository targets.

## External mutations

- Deploy, apply, destroy, rebuild, refresh, state mutation, direct cloud CLI, and
  credential operations require an explicit user request in the current turn.
- Before a cloud mutation, inspect the target and state the AWS profile/account,
  Terraform environment, or Databricks target being changed.
- Prefer project Makefile targets over ad-hoc cloud commands, and verify remote
  state after every mutation.
- Do not commit, push, open a pull request, or send external messages unless the
  user explicitly asks.

## Code review rules

Flag changes that:

- couple `ingest/` to a cloud-specific implementation;
- weaken schema compatibility, idempotency, ordering, or quarantine guarantees;
- use processing time where event time is required;
- introduce a destructive or full-refresh recovery path;
- change infrastructure without an offline validation path;
- change behavior without focused regression coverage.

## Output style

The reader has ADHD. Shape every response so it can be acted on:

1. Lead with the answer or next action: command, path, or snippet first.
2. Number multi-step work; one bounded action per step.
3. End with one next action doable in under two minutes.
4. Finish the current issue before raising a new one.
5. Restate progress each turn ("step 3 of 5 done").
6. Give time estimates in concrete units, never "a bit".
7. After a change, show what now works.
8. Errors: state location, cause, and fix. No drama.
9. Cap lists at 5 items.
10. No preamble, no recaps, no closers.

Exceptions: explain fully when asked to explain. Confirm before destructive actions. After three failed fixes, stop and name the doubtful assumption. If the request is ambiguous, ask one short question.