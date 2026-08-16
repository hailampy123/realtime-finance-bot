locals {
  name = "${var.project}-native-producer"
}

resource "aws_ecr_repository" "producer" {
  name                 = local.name
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

# The account is wiped weekly, so images never accumulate for long -- but a
# rebuild loop during development pushes many `latest` layers, and untagged
# parents are pure cost.
resource "aws_ecr_lifecycle_policy" "producer" {
  repository = aws_ecr_repository.producer.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Expire untagged images after 1 day"
      selection = {
        tagStatus   = "untagged"
        countType   = "sinceImagePushed"
        countUnit   = "days"
        countNumber = 1
      }
      action = { type = "expire" }
    }]
  })
}

# --- Roles -----------------------------------------------------------------
# Two roles, two actors. The execution role is used by the ECS agent BEFORE the
# container starts (pull the image, create the log stream). The task role is
# used by the running code. Different lifetimes, different permissions.

data "aws_iam_policy_document" "ecs_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${local.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust.json
}

# AWS's managed policy covers exactly ECR pull + CloudWatch Logs write.
resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "task" {
  name               = "${local.name}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust.json
}

data "aws_iam_policy_document" "task_permissions" {
  # Least privilege: write to one stream, and nothing else. No read actions --
  # the producer never consumes.
  statement {
    effect    = "Allow"
    actions   = ["kinesis:PutRecord", "kinesis:PutRecords", "kinesis:DescribeStreamSummary"]
    resources = [var.stream_arn]
  }

  # The stream is KMS-encrypted, so producing needs a data key.
  statement {
    effect    = "Allow"
    actions   = ["kms:GenerateDataKey"]
    resources = ["arn:aws:kms:${var.region}:${var.account_id}:alias/aws/kinesis"]
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "${local.name}-task"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_permissions.json
}

# --- Compute ---------------------------------------------------------------

resource "aws_cloudwatch_log_group" "producer" {
  name              = "/ecs/${local.name}"
  retention_in_days = 7
}

resource "aws_ecs_cluster" "this" {
  name = "${var.project}-native"

  setting {
    name  = "containerInsights"
    value = "disabled" # billed per metric; the log stream is enough at one task
  }
}

resource "aws_ecs_task_definition" "producer" {
  family                   = local.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    # Matches docker build --platform linux/amd64. If these disagree the task
    # starts and dies with "exec format error".
    cpu_architecture = "X86_64"
  }

  container_definitions = jsonencode([{
    name      = "producer"
    image     = "${aws_ecr_repository.producer.repository_url}:${var.image_tag}"
    essential = true

    environment = [
      { name = "FDAI_NATIVE_STREAM_NAME", value = var.stream_name },
      { name = "AWS_REGION", value = var.region },
      # boto3's default retry mode gives up sooner than KinesisSink's own
      # backoff; standard mode defers to ours rather than fighting it.
      { name = "AWS_RETRY_MODE", value = "standard" },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.producer.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "producer"
      }
    }
  }])
}

# desired_count = 1 deliberately. Two tasks would each open their own Binance
# and Coinbase connections and publish every trade twice; dedupe downstream
# would absorb it, but doubling the bill to create work for the dedupe is not a
# feature.
resource "aws_ecs_service" "producer" {
  name            = local.name
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.producer.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.subnet_ids
    security_groups = [var.security_group_id]
    # Public IP instead of a NAT gateway: the task needs outbound WSS and
    # nothing inbound, and a NAT would cost more per month than this whole
    # stack (spec section 4.4).
    assign_public_ip = true
  }

  # A rolling replacement of a single task briefly runs two, which double-writes
  # for a few seconds. Stopping the old one first accepts a small gap instead --
  # and a gap is detectable and repairable, where a duplicate is a silent volume
  # error until dedupe runs.
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  enable_execute_command = true # lets you shell into the task to debug
}
