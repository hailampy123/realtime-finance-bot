#!/usr/bin/env bash
# Probes every AWS service the AWS-native stack needs, before any of it is built.
# Read-only except the IAM probe, which creates and immediately deletes a role --
# the only way to actually test iam:CreateRole is to call it.
set -uo pipefail

REGION="${AWS_REGION:-$(aws configure get region)}"
FAIL=0

probe() {
  local label="$1"; shift
  if "$@" >/dev/null 2>&1; then
    printf '  ok    %s\n' "$label"
  else
    printf '  FAIL  %s\n' "$label"
    FAIL=1
  fi
}

printf 'Region: %s\nAccount: %s\n\nRead/list probes:\n' \
  "$REGION" "$(aws sts get-caller-identity --query Account --output text)"

probe "kinesis"        aws kinesis list-streams
probe "firehose"       aws firehose list-delivery-streams
probe "s3"             aws s3api list-buckets
probe "glue"           aws glue get-databases
probe "athena"         aws athena list-work-groups
probe "ecr"            aws ecr describe-repositories
probe "ecs"            aws ecs list-clusters
probe "ec2 (vpc)"      aws ec2 describe-vpcs
probe "logs"           aws logs describe-log-groups
probe "stepfunctions"  aws stepfunctions list-state-machines
probe "lambda"         aws lambda list-functions
probe "budgets"        aws budgets describe-budgets --account-id "$(aws sts get-caller-identity --query Account --output text)"

# The load-bearing probe. Everything in this stack needs a service role.
printf '\nIAM create probe (creates then deletes fdai-native-preflight):\n'
TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"firehose.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
if aws iam create-role --role-name fdai-native-preflight \
     --assume-role-policy-document "$TRUST" >/dev/null 2>&1; then
  printf '  ok    iam:CreateRole\n'
  aws iam delete-role --role-name fdai-native-preflight >/dev/null 2>&1 \
    && printf '  ok    iam:DeleteRole\n' \
    || { printf '  FAIL  iam:DeleteRole (role fdai-native-preflight left behind)\n'; FAIL=1; }
else
  printf '  FAIL  iam:CreateRole -- this stack cannot be built without it\n'
  FAIL=1
fi

printf '\n'
if [ "$FAIL" -eq 0 ]; then
  printf 'All probes passed.\n'
else
  printf 'One or more probes FAILED. See spec section 11 (A4, A5) for fallbacks.\n'
fi
exit "$FAIL"
