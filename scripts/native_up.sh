#!/usr/bin/env bash
# Empty AWS account -> streaming trades in Bronze, in one command.
#
# Order is load-bearing: Terraform will happily create the ECS service before
# any image exists in ECR, and the service then sits retrying
# CannotPullContainerError. So infrastructure first, image second, redeploy
# third.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$ROOT/infra/envs/native"

cd "$ROOT"

printf '==> preflight\n'
./scripts/native_preflight.sh

printf '\n==> rendering the backend config\n'
ACCT=$(aws sts get-caller-identity --query Account --output text)
REGION="${AWS_REGION:-$(aws configure get region)}"
sed -e "s|\${state_bucket}|fdai-tfstate-$ACCT|" \
    -e "s|\${region}|$REGION|" \
    -e "s|\${lock_table}|fdai-tflock|" \
    "$DIR/backend.tf.tftpl" > "$DIR/backend.tf"

printf '\n==> terraform apply\n'
terraform -chdir="$DIR" init -input=false
terraform -chdir="$DIR" apply -auto-approve

printf '\n==> building and pushing the producer image\n'
REPO=$(terraform -chdir="$DIR" output -raw ecr_repository_url)
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "${REPO%%/*}"
# --platform is not optional: an arm64 image pushes fine and then dies in
# Fargate with "exec format error".
docker build --platform linux/amd64 -f docker/Dockerfile.awsnative -t "$REPO:latest" .
docker push "$REPO:latest"

printf '\n==> forcing a new deployment so the service picks up the image\n'
CLUSTER=$(terraform -chdir="$DIR" output -raw ecs_cluster)
SERVICE=$(terraform -chdir="$DIR" output -raw ecs_service)
aws ecs update-service --cluster "$CLUSTER" --service "$SERVICE" \
  --force-new-deployment >/dev/null
aws ecs wait services-stable --cluster "$CLUSTER" --service "$SERVICE"

printf '\n==> up.\n'
printf 'Producer logs : aws logs tail %s --follow\n' \
  "$(terraform -chdir="$DIR" output -raw producer_log_group)"
printf 'Bronze lands in ~2 min (Firehose buffers for 120s).\n'
printf 'Acceptance queries: awsnative/sql/verify_bronze.sql\n'
