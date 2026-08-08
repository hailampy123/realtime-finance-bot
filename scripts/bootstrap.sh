#!/usr/bin/env bash
# Empty AWS account -> streaming data, in one command.
#
# The cluster is applied three times, and the order is forced by MSK, not by
# preference:
#
#   1. private, permissive    - AWS rejects public access on a CREATING cluster.
#   2. private, ACLs enforced - AWS rejects public access unless
#      allow.everyone.if.no.acl.found is false. That setting makes Kafka
#      deny-by-default, so the ACLs must already exist or every client is
#      locked out, including one trying to add ACLs.
#   3. public                 - only now is it legal.
#
# Steps 2 and 3 cannot be collapsed into one apply: the AWS provider handles
# connectivity before configuration within a single update, so it would try
# public access first and fail exactly as it does with no configuration at all.
#
# The ACLs themselves can only be written from inside the VPC, because until
# step 3 the private endpoint is the only one that resolves. The producer host
# is the one thing in there, so this script SSHes to it. That is also why it
# waits on a readiness file rather than sleeping: tightening the cluster before
# the ACLs land bricks it until someone re-applies with msk_restrict_acls=false.
set -euo pipefail

PROJECT="${PROJECT:-fdai}"
REGION="${AWS_REGION:-ap-southeast-1}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEV="${ROOT}/infra/envs/dev"
SSH_KEY="${DEV}/.ssh/${PROJECT}-producer.pem"
HOST_READY_TIMEOUT="${HOST_READY_TIMEOUT:-900}"

# The two layers take their region independently — this one from AWS_REGION,
# the dev stack from terraform.tfvars — so they can drift apart without any
# error. That works, but it puts the state bucket in a different region from
# the resources it tracks, which is worth knowing before `make down`.
DEV_REGION="$(sed -n 's/^[[:space:]]*region[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' \
  "${DEV}/terraform.tfvars" 2>/dev/null | head -1)"
if [[ -n "${DEV_REGION}" && "${DEV_REGION}" != "${REGION}" ]]; then
  echo "note: state backend in ${REGION}, stack in ${DEV_REGION} (terraform.tfvars)." >&2
  echo "      Valid, but they are meant to match — export AWS_REGION=${DEV_REGION} to align." >&2
fi

echo "==> 1/8 bootstrapping Terraform state backend"
terraform -chdir="${ROOT}/infra/bootstrap" init -input=false
terraform -chdir="${ROOT}/infra/bootstrap" apply -auto-approve \
  -var="project=${PROJECT}" -var="region=${REGION}"

STATE_BUCKET="$(terraform -chdir="${ROOT}/infra/bootstrap" output -raw state_bucket)"
LOCK_TABLE="$(terraform -chdir="${ROOT}/infra/bootstrap" output -raw lock_table)"

echo "==> 2/8 rendering backend config"
sed -e "s|\${state_bucket}|${STATE_BUCKET}|" \
    -e "s|\${lock_table}|${LOCK_TABLE}|" \
    -e "s|\${region}|${REGION}|" \
    "${DEV}/backend.tf.tftpl" > "${DEV}/backend.tf"

# Detected rather than configured: it changes, and a stale /32 in tfvars shows
# up as a hang at the SSH step rather than as anything that names the cause.
OPERATOR_IP="$(curl -fsS --max-time 15 https://checkip.amazonaws.com)" || {
  echo "could not determine this machine's public IP; is there network access?" >&2
  exit 1
}
OPERATOR_CIDRS="[\"${OPERATOR_IP}/32\"]"
echo "    operator IP ${OPERATOR_IP}/32 (producer-host SSH + broker access)"

echo "==> 3/8 applying infrastructure (private, ACLs permissive)"
terraform -chdir="${DEV}" init -input=false -reconfigure
terraform -chdir="${DEV}" apply -auto-approve \
  -var="msk_public_access=false" \
  -var="msk_restrict_acls=false" \
  -var="operator_cidrs=${OPERATOR_CIDRS}"

PRODUCER_IP="$(terraform -chdir="${DEV}" output -raw producer_public_ip)"
SSH_OPTS=(-i "${SSH_KEY}" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
          -o LogLevel=ERROR -o ConnectTimeout=10)
ssh_producer() { ssh "${SSH_OPTS[@]}" "ec2-user@${PRODUCER_IP}" "$@"; }

echo "==> 4/8 waiting for the producer host (${PRODUCER_IP}) to finish building"
deadline=$((SECONDS + HOST_READY_TIMEOUT))
until ssh_producer 'test -f /opt/fdai/ready' 2>/dev/null; do
  if ((SECONDS > deadline)); then
    echo "producer host never signalled ready after ${HOST_READY_TIMEOUT}s." >&2
    echo "Inspect its boot log:" >&2
    echo "  ssh -i ${SSH_KEY} ec2-user@${PRODUCER_IP} 'sudo cat /var/log/cloud-init-output.log'" >&2
    exit 1
  fi
  sleep 15
done

# Credentials come from the host's own .env, so nothing sensitive lands in argv
# (readable via ps for the life of the call) or in a shell history file.
echo "==> 5/8 granting Kafka ACLs from inside the VPC"
ssh_producer 'sudo docker run --rm --env-file /opt/fdai/app/.env fdai-producers \
  python -m scripts.create_acls'

echo "==> 6/8 enforcing ACLs (allow.everyone.if.no.acl.found=false)"
terraform -chdir="${DEV}" apply -auto-approve \
  -var="msk_public_access=false" \
  -var="msk_restrict_acls=true" \
  -var="operator_cidrs=${OPERATOR_CIDRS}"

echo "==> 7/8 enabling MSK public access"
terraform -chdir="${DEV}" apply -auto-approve \
  -var="msk_public_access=true" \
  -var="msk_restrict_acls=true" \
  -var="operator_cidrs=${OPERATOR_CIDRS}"

BOOTSTRAP_PUBLIC="$(terraform -chdir="${DEV}" output -raw bootstrap_brokers_public)"
BOOTSTRAP_PRIVATE="$(terraform -chdir="${DEV}" output -raw bootstrap_brokers_private)"
SASL_USER="$(terraform -chdir="${DEV}" output -raw sasl_username)"
SASL_PASS="$(terraform -chdir="${DEV}" output -raw sasl_password)"

echo "==> 8/8 creating topics and publishing endpoints to Databricks"
uv run python -m scripts.create_topics \
  --bootstrap "${BOOTSTRAP_PUBLIC}" \
  --username "${SASL_USER}" --password "${SASL_PASS}"

# The producer has been up since step 4, failing on topics that did not exist
# yet. librdkafka only refreshes metadata for an unknown topic every few
# minutes, so restart it rather than waiting that out.
echo "    restarting the producer now that its topics exist"
ssh_producer 'sudo docker restart fdai-producers' >/dev/null

# Broker DNS changes on every rebuild, so Databricks reads it from here rather
# than from a hardcoded value in a notebook.
databricks secrets create-scope "${PROJECT}" 2>/dev/null || true
databricks secrets put-secret "${PROJECT}" kafka_bootstrap --string-value "${BOOTSTRAP_PUBLIC}"
databricks secrets put-secret "${PROJECT}" kafka_username  --string-value "${SASL_USER}"
databricks secrets put-secret "${PROJECT}" kafka_password  --string-value "${SASL_PASS}"

echo "    smoke test (waiting for the producer to reconnect)"
sleep 90
uv run python -m scripts.smoke_test \
  --bootstrap "${BOOTSTRAP_PUBLIC}" \
  --username "${SASL_USER}" --password "${SASL_PASS}"

echo
echo "Ready."
echo "  public brokers : ${BOOTSTRAP_PUBLIC}"
echo "  in-VPC brokers : ${BOOTSTRAP_PRIVATE}"
echo "  producer host  : ${PRODUCER_IP}"
echo "  producer shell : ssh -i ${SSH_KEY} ec2-user@${PRODUCER_IP}"
