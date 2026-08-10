# Reliable MSK Notebook Startup — Design

## Goal

Starting an MSK-backed notebook must be one supported command, even when the
laptop's public IP changed, the default AWS credential entry is stale, or
Terraform cached an empty public broker output:

```bash
make notebook TARGET=msk
```

Local notebook startup remains:

```bash
make notebook TARGET=local
```

## Root causes being removed

The August 10 failure combined three independent pieces of mutable state:

1. The laptop public IP changed from the `/32` stored in the MSK security
   group, so TCP `9196` timed out before Kafka authentication.
2. The unscoped `default` AWS credentials were invalid while the configured
   `fdai-sandbox` SSO profile was valid.
3. Terraform state held an empty `bootstrap_brokers_public` value even though
   AWS had a valid live endpoint.

The immediate repair used a profile-scoped `make up`, which updated the two
security groups in place. No source file participated in that repair, so it
could recur after the next IP or credential change.

## Approaches considered

### Selected: target-aware notebook preflight

`make notebook TARGET=msk` runs a small, tested Python preflight before
Jupyter. The preflight validates or refreshes the configured SSO session,
detects the current public IP, applies the finished Terraform state with that
`/32`, and verifies Kafka metadata access. Jupyter then inherits the same AWS
profile and `FDAI_TARGET=terraform`.

This is faster than the complete bootstrap and keeps Terraform as the owner of
the security-group rules.

### Rejected: run the full `make up` before every notebook

This is safe and is what repaired the incident, but it also repeats ACL,
topic, Databricks-secret, producer-restart, and 90-second smoke-test work that
is unrelated to notebook access.

### Rejected: update the security group with the AWS CLI

This is fast, but it creates Terraform drift. A later apply would reverse or
replace the manual rule, recreating the same class of failure.

## Command contract

`TARGET` accepts only `local` or `msk` and defaults to `local`.

The AWS profile selection order for MSK is:

1. `FDAI_AWS_PROFILE`, when explicitly supplied;
2. the existing `AWS_PROFILE` value; or
3. `fdai-sandbox`.

When a named profile is selected, raw `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, and `AWS_SECURITY_TOKEN` values
are removed from the preflight and Jupyter environments. Otherwise stale raw
credentials would take precedence over the valid profile.

If STS rejects an SSO profile, the preflight invokes `aws sso login` once and
then retries STS. Non-SSO credentials cannot be refreshed automatically and
produce a concise error naming the profile.

## MSK preflight flow

1. Validate the selected AWS identity.
2. Initialize the existing `infra/envs/dev` backend.
3. Read `cluster_arn`; fail without applying if no deployed stack exists.
4. Confirm AWS publishes public SASL/SCRAM brokers for that cluster.
5. Resolve the current public IP and validate it as an IP address.
6. Apply the existing Terraform configuration in its final state with
   `operator_cidrs=["<current-ip>/32"]`, public MSK access enabled, and ACL
   restriction enabled.
7. Build `devlab.from_terraform()` and list topics to prove endpoint, TLS,
   SCRAM, ACL, and topic metadata access.
8. Launch Jupyter with `FDAI_TARGET=terraform` and the selected profile.

The preflight never creates a missing stack. The first deployment remains
`make up`; normal later notebook runs are one command.

## Endpoint fallback

`devlab.from_terraform()` continues to prefer the Terraform public broker
output. If that output is empty, it reads `cluster_arn`, derives the AWS region
from the ARN, and calls `aws kafka get-bootstrap-brokers`. It never asks the
user to copy an endpoint into `.env` or construct a `Target` manually.

## Safety and data behavior

- Terraform remains the only writer of security-group rules.
- The preflight applies no resource when `cluster_arn` cannot be read.
- The notebook remains read-only: no producer, offset commit, topic mutation,
  file export, or infrastructure call occurs in analysis cells.
- Local mode performs no AWS or Terraform operation.
- Secrets are never printed or placed in command-line arguments by the new
  preflight.

## Verification

Automated tests cover profile precedence, stale raw-credential removal, SSO
refresh, missing-stack refusal, the exact Terraform final-state arguments,
Kafka metadata verification, empty Terraform endpoint fallback, and dry-run
Make behavior for both targets.

Live verification runs the MSK preflight against the deployed stack, confirms
both broker sockets, and executes the original `devlab.rate()` path.
