#!/usr/bin/env bash
# Empty AWS account -> streaming data, in one command.
#
# MSK rejects public access while the cluster is CREATING, so the cluster is
# applied twice: once private, once with public access enabled.
set -euo pipefail

PROJECT="${PROJECT:-fdai}"
REGION="${AWS_REGION:-us-east-1}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEV="${ROOT}/infra/envs/dev"

echo "==> 1/6 bootstrapping Terraform state backend"
terraform -chdir="${ROOT}/infra/bootstrap" init -input=false
terraform -chdir="${ROOT}/infra/bootstrap" apply -auto-approve \
  -var="project=${PROJECT}" -var="region=${REGION}"

STATE_BUCKET="$(terraform -chdir="${ROOT}/infra/bootstrap" output -raw state_bucket)"
LOCK_TABLE="$(terraform -chdir="${ROOT}/infra/bootstrap" output -raw lock_table)"

echo "==> 2/6 rendering backend config"
sed -e "s|\${state_bucket}|${STATE_BUCKET}|" \
    -e "s|\${lock_table}|${LOCK_TABLE}|" \
    -e "s|\${region}|${REGION}|" \
    "${DEV}/backend.tf.tftpl" > "${DEV}/backend.tf"

echo "==> 3/6 applying infrastructure (private)"
terraform -chdir="${DEV}" init -input=false -reconfigure
terraform -chdir="${DEV}" apply -auto-approve -var="msk_public_access=false"

echo "==> 4/6 enabling MSK public access"
terraform -chdir="${DEV}" apply -auto-approve -var="msk_public_access=true"

BOOTSTRAP_PUBLIC="$(terraform -chdir="${DEV}" output -raw bootstrap_brokers_public)"
BOOTSTRAP_PRIVATE="$(terraform -chdir="${DEV}" output -raw bootstrap_brokers_private)"
SASL_USER="$(terraform -chdir="${DEV}" output -raw sasl_username)"
SASL_PASS="$(terraform -chdir="${DEV}" output -raw sasl_password)"

echo "==> 5/6 creating topics and publishing endpoints to Databricks"
uv run python -m scripts.create_topics \
  --bootstrap "${BOOTSTRAP_PUBLIC}" \
  --username "${SASL_USER}" --password "${SASL_PASS}"

# Broker DNS changes on every rebuild, so Databricks reads it from here rather
# than from a hardcoded value in a notebook.
databricks secrets create-scope "${PROJECT}" 2>/dev/null || true
databricks secrets put-secret "${PROJECT}" kafka_bootstrap --string-value "${BOOTSTRAP_PUBLIC}"
databricks secrets put-secret "${PROJECT}" kafka_username  --string-value "${SASL_USER}"
databricks secrets put-secret "${PROJECT}" kafka_password  --string-value "${SASL_PASS}"

echo "==> 6/6 smoke test (waiting for the producer host to build and connect)"
sleep 180
uv run python -m scripts.smoke_test \
  --bootstrap "${BOOTSTRAP_PUBLIC}" \
  --username "${SASL_USER}" --password "${SASL_PASS}"

echo
echo "Ready."
echo "  public brokers : ${BOOTSTRAP_PUBLIC}"
echo "  in-VPC brokers : ${BOOTSTRAP_PRIVATE}"
echo "  producer host  : $(terraform -chdir="${DEV}" output -raw producer_public_ip)"
