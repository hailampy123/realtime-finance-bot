# The health-metrics Lambda, and everything that watches its output:
# CloudWatch alarms and the SNS topic they notify. The Lambda itself is
# invoked as a tail state inside native_medallion and native_enrichment's own
# state machines, not scheduled here -- see those modules for the wiring.
locals {
  name = "${var.project}-native-monitoring"

  # CONSTRUCTED rather than taken as module outputs, the same technique
  # native_medallion's own state_machine_arn local already uses: this
  # module's Lambda ARN is an input to native_medallion and
  # native_enrichment (they must invoke it), so taking their state-machine
  # ARNs as inputs here in return would be a circular module dependency.
  microbatch_state_machine_arn  = "arn:aws:states:${var.region}:${var.account_id}:stateMachine:${var.project}-native-microbatch"
  merge_perp_state_machine_arn  = "arn:aws:states:${var.region}:${var.account_id}:stateMachine:${var.project}-native-enrichment-merge-perp"
  merge_macro_state_machine_arn = "arn:aws:states:${var.region}:${var.account_id}:stateMachine:${var.project}-native-enrichment-merge-macro"
}

# --- the Lambda --------------------------------------------------------------

data "archive_file" "package" {
  type        = "zip"
  output_path = "${path.module}/.build/health_metrics.zip"

  source {
    content  = file("${var.source_dir}/awsnative/__init__.py")
    filename = "awsnative/__init__.py"
  }
  source {
    content  = file("${var.source_dir}/awsnative/athena.py")
    filename = "awsnative/athena.py"
  }
  source {
    content  = file("${var.source_dir}/awsnative/render.py")
    filename = "awsnative/render.py"
  }
  dynamic "source" {
    for_each = fileset("${var.source_dir}/awsnative/monitoring", "*.py")
    content {
      content  = file("${var.source_dir}/awsnative/monitoring/${source.value}")
      filename = "awsnative/monitoring/${source.value}"
    }
  }
  # render.py resolves SQL_DIR relative to its own file location at runtime,
  # so the whole sql/ tree has to ship as its sibling in the zip, not just
  # health_metrics_row.sql.
  dynamic "source" {
    for_each = fileset(var.sql_dir, "**/*.sql")
    content {
      content  = file("${var.sql_dir}/${source.value}")
      filename = "awsnative/sql/${source.value}"
    }
  }
}

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = local.name
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

data "aws_iam_policy_document" "lambda" {
  statement {
    sid    = "RunAthenaQueries"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetWorkGroup",
      "athena:GetDataCatalog",
    ]
    resources = [
      "arn:aws:athena:${var.region}:${var.account_id}:workgroup/${var.athena_workgroup_name}",
      "arn:aws:athena:${var.region}:${var.account_id}:datacatalog/AwsDataCatalog",
    ]
  }

  # Reads every maintained table's metadata (row counts, $files, $snapshots)
  # and commits to native_health_metrics, the only table this role writes.
  statement {
    sid    = "ReadEveryTableWriteHealthMetrics"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:UpdateTable",
      "glue:CreateTable",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${var.account_id}:catalog",
      "arn:aws:glue:${var.region}:${var.account_id}:database/${var.glue_database_name}",
      "arn:aws:glue:${var.region}:${var.account_id}:table/${var.glue_database_name}/*",
    ]
  }

  statement {
    sid    = "ReadWholeLakeWriteHealthMetrics"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [var.lake_bucket_arn, "${var.lake_bucket_arn}/*"]
  }

  statement {
    sid       = "PublishHealthMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"] # PutMetricData has no resource-level scoping.
  }

  statement {
    sid    = "WriteItsOwnLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/lambda/${local.name}:*"]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = local.name
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "health_metrics" {
  function_name    = local.name
  role             = aws_iam_role.lambda.arn
  handler          = "awsnative.monitoring.collect.health_metrics_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.package.output_path
  source_code_hash = data.archive_file.package.output_base64sha256

  # At most 3 tables per invocation, each a handful of small Athena queries
  # plus one SELECT and one INSERT -- generous headroom over the
  # merge/maintenance tail states this runs after, which already fit inside
  # the 5-minute tick.
  timeout     = 180
  memory_size = 256

  depends_on = [aws_cloudwatch_log_group.lambda]
}

# --- alerting: one topic, every alarm notifies it ----------------------------

resource "aws_sns_topic" "alerts" {
  name = "${var.project}-native-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_notification_email
}

# Freshness, per table, threshold from DATA_LAYER.md section 7's own SLOs.
# Two consecutive 5-minute periods for the fast tier so one missed tick does
# not alarm; one daily period for the slow tier so its own normal cadence
# does not either.
locals {
  freshness_alarms = {
    silver_trades = {
      threshold          = 360
      period             = 300
      evaluation_periods = 2
    }
    gold_bars_1m = {
      threshold          = 420
      period             = 300
      evaluation_periods = 2
    }
    silver_perp_context = {
      threshold          = 600
      period             = 300
      evaluation_periods = 2
    }
    silver_macro = {
      threshold          = 108000 # 30h: the 24h SLO plus a buffer against the daily poll's own cadence
      period             = 86400
      evaluation_periods = 1
    }
  }

  # "Maintenance stalled": small_file_pct still high two hours after the
  # hourly OPTIMIZE tail state should have run (design 2026-08-17 practice
  # 9 -- alarm on maintenance, not only on the pipeline). Fast tier only:
  # silver_macro never runs OPTIMIZE (design 2026-08-19 section 3.2).
  maintenance_alarms = toset([
    "silver_trades",
    "silver_trades_quarantine",
    "gold_bars_1m",
    "silver_perp_context",
    "native_health_metrics",
  ])

  watched_state_machines = {
    microbatch  = local.microbatch_state_machine_arn
    merge_perp  = local.merge_perp_state_machine_arn
    merge_macro = local.merge_macro_state_machine_arn
  }
}

resource "aws_cloudwatch_metric_alarm" "freshness" {
  for_each = local.freshness_alarms

  alarm_name          = "${var.project}-native-freshness-${each.key}"
  namespace           = "FDAI/Native"
  metric_name         = "FreshnessLagSeconds"
  dimensions          = { TableName = each.key }
  statistic           = "Maximum"
  period              = each.value.period
  evaluation_periods  = each.value.evaluation_periods
  threshold           = each.value.threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching" # an absent reading means the collection tail state itself stopped
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "quarantine_rate" {
  alarm_name          = "${var.project}-native-quarantine-rate"
  namespace           = "FDAI/Native"
  metric_name         = "QuarantineRatePct"
  dimensions          = { TableName = "silver_trades_quarantine" }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 0.1
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching" # no quarantined rows yet is not a problem, unlike a stale table
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "maintenance_stalled" {
  for_each = local.maintenance_alarms

  alarm_name          = "${var.project}-native-maintenance-stalled-${each.key}"
  namespace           = "FDAI/Native"
  metric_name         = "SmallFilePct"
  dimensions          = { TableName = each.key }
  statistic           = "Minimum" # the lowest reading in the window must still be high for this to be a real stall
  period              = 300
  evaluation_periods  = 24 # 2 hours at 5-minute readings
  threshold           = 20
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

# Free: AWS/States publishes ExecutionsFailed for every state machine with no
# custom code needed. Catches a state machine that stopped entirely, which a
# health-metrics reading cannot: if the writer never ran, it never published
# a stale-but-present reading either.
resource "aws_cloudwatch_metric_alarm" "execution_failed" {
  for_each = local.watched_state_machines

  alarm_name          = "${var.project}-native-executions-failed-${each.key}"
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  dimensions          = { StateMachineArn = each.value }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching" # no executions in the window is not a failure
  alarm_actions       = [aws_sns_topic.alerts.arn]
}
