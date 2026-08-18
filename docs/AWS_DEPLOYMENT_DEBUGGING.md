# AWS post-deployment verification and debugging

Use this runbook after `make up`, or when Terraform, MSK, the EC2 producer, or
Kafka connectivity fails. It covers Stack A only. For Stack B, run
`make verify-aws` and `make sfn-logs-aws`.

Check in this order:

```text
AWS identity
  -> Terraform backend
  -> Terraform deployment state
  -> MSK cluster
  -> DNS/TCP/TLS
  -> security groups
  -> SCRAM credentials
  -> EC2 producer
  -> Kafka data flow
```

Companion documentation:

- [`MAKE_UP_EXPLAINED.md`](MAKE_UP_EXPLAINED.md) explains the deployment
  sequence.
- [`AWS_SERVICES_EXPLAINED.md`](AWS_SERVICES_EXPLAINED.md) explains each AWS
  service.
- [`KAFKA_EXPLAINED.md`](KAFKA_EXPLAINED.md) explains the Kafka configuration
  and security model.
- [`SETUP.md`](SETUP.md) covers credentials, installation, and prerequisites.

Run all commands from the repository root. The manual AWS inspection commands
in this guide are read-only. `make smoke` creates a short-lived Kafka consumer
group but does not change AWS infrastructure or topic data. The automated MSK
notebook preflight below may reconcile the two Terraform-managed operator
security-group rules when the laptop IP changed.

Two notebook failures recur: expired profile credentials, and a changed laptop
public IP. For both, use the automated entry point rather than the manual
sections below:

```bash
make notebook TARGET=msk
```

It refreshes SSO when possible, reconciles the current operator `/32` through
Terraform, verifies Kafka metadata access, and only then starts Jupyter. The
remaining commands are for investigation when that preflight itself fails.

---

## 1. Establish the correct environment

Do not hardcode a region. The Terraform backend and deployed stack may be in
different regions.

```bash
FDAI_DEV_DIR=infra/envs/dev
FDAI_BOOTSTRAP_DIR=infra/bootstrap

FDAI_STACK_REGION="$(awk -F'"' '/^[[:space:]]*region[[:space:]]*=/{print $2; exit}' "$FDAI_DEV_DIR/terraform.tfvars")"
FDAI_BACKEND_REGION="$(terraform -chdir="$FDAI_BOOTSTRAP_DIR" output -raw region)"
FDAI_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

echo "AWS account:    $FDAI_ACCOUNT_ID"
echo "Backend region: $FDAI_BACKEND_REGION"
echo "Stack region:   $FDAI_STACK_REGION"

aws sts get-caller-identity
aws configure list
```

Compare `FDAI_ACCOUNT_ID` with the sandbox account assigned to you. If it is
wrong, stop and renew or select the correct AWS SSO profile before running any
Terraform command:

```bash
aws sso login --profile fdai-sandbox
export AWS_PROFILE=fdai-sandbox
aws sts get-caller-identity
```

---

## 2. Verify the Terraform backend

Load the backend resource names:

```bash
FDAI_STATE_BUCKET="$(terraform -chdir="$FDAI_BOOTSTRAP_DIR" output -raw state_bucket)"
FDAI_LOCK_TABLE="$(terraform -chdir="$FDAI_BOOTSTRAP_DIR" output -raw lock_table)"

echo "State bucket: $FDAI_STATE_BUCKET"
echo "Lock table:   $FDAI_LOCK_TABLE"
```

Check that the S3 state bucket is reachable:

```bash
aws s3api head-bucket \
  --bucket "$FDAI_STATE_BUCKET" \
  --region "$FDAI_BACKEND_REGION" &&
echo "Terraform state bucket is reachable"
```

List its state objects:

```bash
aws s3api list-objects-v2 \
  --bucket "$FDAI_STATE_BUCKET" \
  --region "$FDAI_BACKEND_REGION" \
  --query 'Contents[].{Key:Key,Modified:LastModified,Size:Size}' \
  --output table
```

Check the DynamoDB lock table:

```bash
aws dynamodb describe-table \
  --table-name "$FDAI_LOCK_TABLE" \
  --region "$FDAI_BACKEND_REGION" \
  --query 'Table.{Name:TableName,Status:TableStatus,Items:ItemCount}' \
  --output table
```

Then confirm that Terraform can read the deployed stack:

```bash
terraform -chdir="$FDAI_DEV_DIR" output
terraform -chdir="$FDAI_DEV_DIR" state list
```

Do not use `terraform output -json` casually. Sensitive outputs can include the
Kafka SASL password even though the normal human-readable output redacts it.

### If Terraform reports `NoSuchBucket`

Check these in order:

1. `aws sts get-caller-identity` points to the intended account.
2. `FDAI_BACKEND_REGION` matches the bootstrap output.
3. `terraform -chdir=infra/bootstrap output` still knows the expected bucket.
4. `aws s3api head-bucket` can reach it.

Do not immediately recreate the bucket. First establish whether the sandbox was
wiped or the shell is using the wrong account. Recreating a missing backend in
the wrong account creates a second, unrelated state store.

---

## 3. Verify the MSK cluster

Load the deployed cluster ARN:

```bash
FDAI_CLUSTER_ARN="$(terraform -chdir="$FDAI_DEV_DIR" output -raw cluster_arn)"
echo "$FDAI_CLUSTER_ARN"
```

Check its main status:

```bash
aws kafka describe-cluster-v2 \
  --cluster-arn "$FDAI_CLUSTER_ARN" \
  --region "$FDAI_STACK_REGION" \
  --query 'ClusterInfo.{Name:ClusterName,State:State,Type:ClusterType,KafkaVersion:Provisioned.CurrentBrokerSoftwareInfo.KafkaVersion,Brokers:Provisioned.NumberOfBrokerNodes,Created:CreationTime}' \
  --output table
```

A healthy result has:

```text
Name  = fdai-kafka
State = ACTIVE
```

List the broker nodes:

```bash
aws kafka list-nodes \
  --cluster-arn "$FDAI_CLUSTER_ARN" \
  --region "$FDAI_STACK_REGION" \
  --output json
```

Check recent MSK operations:

```bash
aws kafka list-cluster-operations-v2 \
  --cluster-arn "$FDAI_CLUSTER_ARN" \
  --region "$FDAI_STACK_REGION" \
  --max-results 10 \
  --query 'ClusterOperationInfoList[].[OperationType,OperationState,StartTime,EndTime,ErrorInfo.ErrorString]' \
  --output table
```

Read the current broker endpoints directly from AWS:

```bash
aws kafka get-bootstrap-brokers \
  --cluster-arn "$FDAI_CLUSTER_ARN" \
  --region "$FDAI_STACK_REGION" \
  --output table
```

Use the AWS result for connectivity debugging. Immediately after public access
is enabled, Terraform's `bootstrap_brokers_public` output can briefly be empty
while AWS finishes publishing the public endpoint.

---

## 4. Test DNS, TCP, and TLS connectivity

Get the current public SASL/SCRAM brokers:

```bash
FDAI_PUBLIC_BROKERS="$(
  aws kafka get-bootstrap-brokers \
    --cluster-arn "$FDAI_CLUSTER_ARN" \
    --region "$FDAI_STACK_REGION" \
    --query BootstrapBrokerStringPublicSaslScram \
    --output text
)"

echo "$FDAI_PUBLIC_BROKERS"
```

Extract one endpoint into separate hostname and port values:

```bash
FDAI_BROKER_ENDPOINT="${FDAI_PUBLIC_BROKERS%%,*}"
FDAI_BROKER_HOST="${FDAI_BROKER_ENDPOINT%:*}"
FDAI_BROKER_PORT="${FDAI_BROKER_ENDPOINT##*:}"

echo "Endpoint: $FDAI_BROKER_ENDPOINT"
echo "Host:     $FDAI_BROKER_HOST"
echo "Port:     $FDAI_BROKER_PORT"
```

Check DNS resolution:

```bash
dig +short "$FDAI_BROKER_HOST"
```

Check TCP connectivity from macOS:

```bash
nc -vz -G 5 "$FDAI_BROKER_HOST" "$FDAI_BROKER_PORT"
```

Check the TLS handshake and certificate:

```bash
openssl s_client \
  -connect "$FDAI_BROKER_ENDPOINT" \
  -servername "$FDAI_BROKER_HOST" \
  </dev/null 2>/dev/null |
openssl x509 -noout -subject -issuer -dates
```

### Why `nc` can report `getaddrinfo`

`nc` requires the hostname and port as separate arguments. This is wrong:

```bash
nc -vz "broker.amazonaws.com:9196" 9196
```

It asks DNS to resolve the literal name `broker.amazonaws.com:9196`, including
the colon and port. Use the extracted values instead:

```bash
nc -vz "$FDAI_BROKER_HOST" "$FDAI_BROKER_PORT"
```

---

## 5. Verify the VPC and security groups

Find the project VPC:

```bash
FDAI_VPC_ID="$(
  aws ec2 describe-vpcs \
    --region "$FDAI_STACK_REGION" \
    --filters "Name=tag:Name,Values=fdai-vpc" \
    --query 'Vpcs[0].VpcId' \
    --output text
)"

echo "$FDAI_VPC_ID"
```

Check its subnets:

```bash
aws ec2 describe-subnets \
  --region "$FDAI_STACK_REGION" \
  --filters "Name=vpc-id,Values=$FDAI_VPC_ID" \
  --query 'Subnets[].{Subnet:SubnetId,AZ:AvailabilityZone,CIDR:CidrBlock,PublicIP:MapPublicIpOnLaunch}' \
  --output table
```

Check its route tables:

```bash
aws ec2 describe-route-tables \
  --region "$FDAI_STACK_REGION" \
  --filters "Name=vpc-id,Values=$FDAI_VPC_ID" \
  --query 'RouteTables[].{Table:RouteTableId,Routes:Routes}' \
  --output json
```

Find the MSK security group:

```bash
FDAI_MSK_SG_ID="$(
  aws ec2 describe-security-groups \
    --region "$FDAI_STACK_REGION" \
    --filters \
      "Name=group-name,Values=fdai-msk" \
      "Name=vpc-id,Values=$FDAI_VPC_ID" \
    --query 'SecurityGroups[0].GroupId' \
    --output text
)"

echo "$FDAI_MSK_SG_ID"
```

Show all of its rules:

```bash
aws ec2 describe-security-group-rules \
  --region "$FDAI_STACK_REGION" \
  --filters "Name=group-id,Values=$FDAI_MSK_SG_ID" \
  --query 'SecurityGroupRules[].{Egress:IsEgress,Protocol:IpProtocol,From:FromPort,To:ToPort,CIDR:CidrIpv4,SourceSG:ReferencedGroupInfo.GroupId,Description:Description}' \
  --output table
```

Compare your current public IP with the CIDRs allowed on public Kafka port
`9196`:

```bash
FDAI_OPERATOR_IP="$(curl -fsS https://checkip.amazonaws.com | tr -d '\n')"
echo "Current public IP: $FDAI_OPERATOR_IP"
```

```bash
aws ec2 describe-security-group-rules \
  --region "$FDAI_STACK_REGION" \
  --filters "Name=group-id,Values=$FDAI_MSK_SG_ID" \
  --query 'SecurityGroupRules[?FromPort==`9196`].{From:FromPort,To:ToPort,CIDR:CidrIpv4,Description:Description}' \
  --output table
```

The output should contain your current public address as `YOUR.PUBLIC.IP/32`.
If it does not, the cluster can be healthy while connections from the laptop
still time out.

---

## 6. Verify SCRAM authentication resources

Check that the SCRAM secret is associated with the MSK cluster:

```bash
aws kafka list-scram-secrets \
  --cluster-arn "$FDAI_CLUSTER_ARN" \
  --region "$FDAI_STACK_REGION" \
  --output table
```

Inspect the secret metadata without printing its value:

```bash
aws secretsmanager describe-secret \
  --secret-id AmazonMSK_fdai_producer \
  --region "$FDAI_STACK_REGION" \
  --query '{Name:Name,ARN:ARN,KMSKeyId:KmsKeyId,Changed:LastChangedDate}' \
  --output table
```

Check the KMS key used to encrypt the secret:

```bash
aws kms describe-key \
  --key-id alias/fdai-msk-scram \
  --region "$FDAI_STACK_REGION" \
  --query 'KeyMetadata.{ARN:Arn,Enabled:Enabled,State:KeyState,Manager:KeyManager}' \
  --output table
```

Avoid `aws secretsmanager get-secret-value` during normal diagnostics because it
prints credentials. Use the Terraform-backed project commands when an actual
Kafka authentication test is required.

---

## 7. Verify the EC2 producer

Find the producer instance:

```bash
FDAI_PRODUCER_INSTANCE_ID="$(
  aws ec2 describe-instances \
    --region "$FDAI_STACK_REGION" \
    --filters \
      "Name=tag:Name,Values=fdai-producer" \
      "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[0].Instances[0].InstanceId' \
    --output text
)"

echo "$FDAI_PRODUCER_INSTANCE_ID"
```

Check its state and addresses:

```bash
aws ec2 describe-instances \
  --instance-ids "$FDAI_PRODUCER_INSTANCE_ID" \
  --region "$FDAI_STACK_REGION" \
  --query 'Reservations[0].Instances[0].{State:State.Name,Type:InstanceType,PublicIP:PublicIpAddress,PrivateIP:PrivateIpAddress,AZ:Placement.AvailabilityZone,Started:LaunchTime}' \
  --output table
```

Check both AWS health checks:

```bash
aws ec2 describe-instance-status \
  --instance-ids "$FDAI_PRODUCER_INSTANCE_ID" \
  --include-all-instances \
  --region "$FDAI_STACK_REGION" \
  --query 'InstanceStatuses[0].{State:InstanceState.Name,System:SystemStatus.Status,Instance:InstanceStatus.Status}' \
  --output table
```

A healthy instance has both `System` and `Instance` set to `ok`.

Get its boot console output:

```bash
aws ec2 get-console-output \
  --instance-id "$FDAI_PRODUCER_INSTANCE_ID" \
  --latest \
  --region "$FDAI_STACK_REGION" \
  --query Output \
  --output text
```

Load the Terraform SSH outputs:

```bash
FDAI_PRODUCER_IP="$(terraform -chdir="$FDAI_DEV_DIR" output -raw producer_public_ip)"
FDAI_PRODUCER_KEY="$(terraform -chdir="$FDAI_DEV_DIR" output -raw producer_ssh_key_path)"
```

Inspect the bootstrap marker, container status, and recent producer logs:

```bash
ssh \
  -o ConnectTimeout=10 \
  -o StrictHostKeyChecking=accept-new \
  -i "$FDAI_PRODUCER_KEY" \
  "ec2-user@$FDAI_PRODUCER_IP" \
  'test -f /opt/fdai/ready && echo "bootstrap ready"; sudo docker ps; sudo docker logs --since 10m --tail 200 fdai-producers'
```

If the `/opt/fdai/ready` marker is missing, inspect cloud-init:

```bash
ssh -i "$FDAI_PRODUCER_KEY" "ec2-user@$FDAI_PRODUCER_IP" \
  'sudo tail -n 200 /var/log/cloud-init-output.log'
```

The EC2 producer does not currently publish its container or cloud-init logs to
CloudWatch Logs, so SSH is the authoritative application-log path.

---

## 8. Verify actual Kafka data flow

Run the repository smoke test:

```bash
make smoke
```

Expected final line:

```text
SMOKE OK: 5 live trades decoded
```

Inspect topics through the project client:

```bash
uv run python - <<'PY'
import devlab

target = devlab.from_terraform()
for topic in devlab.topics(target):
    print(topic)
PY
```

Measure the live trade rate for ten seconds:

```bash
uv run python - <<'PY'
import devlab

target = devlab.from_terraform()
report = devlab.rate(target, seconds=10)
print(report)
PY
```

These checks prove more than an open TCP port: DNS, TLS, SASL/SCRAM, Kafka ACLs,
topic existence, Avro decoding, and live producer delivery must all work.

---

## 9. Inspect CloudWatch metrics

List the metrics AWS is publishing for the cluster:

```bash
aws cloudwatch list-metrics \
  --namespace AWS/Kafka \
  --region "$FDAI_STACK_REGION" \
  --dimensions Name="Cluster Name",Value=fdai-kafka \
  --query 'Metrics[].MetricName' \
  --output text |
tr '\t' '\n' |
sort -u
```

The most useful MSK metrics are:

- `OfflinePartitionsCount` should be `0`.
- `UnderReplicatedPartitions` should be `0`.
- `ActiveControllerCount` should be `1`.
- `BytesInPerSec` confirms producer traffic.
- `BytesOutPerSec` confirms consumer traffic.
- `CpuUser` shows broker CPU load.
- `KafkaDataLogsDiskUsed` shows broker storage use.

List available EC2 metrics for the producer:

```bash
aws cloudwatch list-metrics \
  --namespace AWS/EC2 \
  --region "$FDAI_STACK_REGION" \
  --dimensions Name=InstanceId,Value="$FDAI_PRODUCER_INSTANCE_ID" \
  --query 'Metrics[].MetricName' \
  --output text |
tr '\t' '\n' |
sort -u
```

---

## 10. Failure guide

| Symptom | Check first |
|---|---|
| `NoSuchBucket` | AWS account, backend outputs, then `s3api head-bucket` |
| Terraform cannot load state | S3 bucket and DynamoDB lock table |
| MSK broker transport failure | Cluster state, DNS, TCP `9196`, and security-group CIDR |
| `getaddrinfo` from `nc` | Split `host:port` before calling `nc` |
| TCP timeout | Current public IP is probably not allowlisted |
| TCP connection refused | Empty or stale endpoint, or MSK is not `ACTIVE` |
| SASL authentication failure | SCRAM association, secret metadata, and KMS key |
| Topic authorization failure | Kafka ACL configuration |
| No incoming trades | EC2 health, cloud-init log, then producer container log |
| MSK stuck in `UPDATING` | Recent cluster operations and their error details |
| Terraform state locked | Use only the lock ID reported by Terraform |

For a genuine stale Terraform lock, use the exact lock ID printed by Terraform:

```bash
terraform -chdir=infra/envs/dev force-unlock <LOCK_ID>
```

Never delete a DynamoDB lock row manually, and never guess the lock ID.

---

## 11. Fast verification checklist

For a normal post-deployment check, this is the minimum useful sequence:

```bash
aws sts get-caller-identity
terraform -chdir=infra/envs/dev output

FDAI_STACK_REGION="$(awk -F'"' '/^[[:space:]]*region[[:space:]]*=/{print $2; exit}' infra/envs/dev/terraform.tfvars)"
FDAI_CLUSTER_ARN="$(terraform -chdir=infra/envs/dev output -raw cluster_arn)"

aws kafka describe-cluster-v2 \
  --cluster-arn "$FDAI_CLUSTER_ARN" \
  --region "$FDAI_STACK_REGION" \
  --query 'ClusterInfo.{Name:ClusterName,State:State}' \
  --output table

aws kafka get-bootstrap-brokers \
  --cluster-arn "$FDAI_CLUSTER_ARN" \
  --region "$FDAI_STACK_REGION" \
  --output table

make smoke
```

If that sequence passes, the AWS identity, Terraform state, MSK cluster,
network path, authentication, ACLs, topics, EC2 producer, and live trade stream
are all functioning end to end.
