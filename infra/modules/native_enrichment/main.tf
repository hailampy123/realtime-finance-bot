# Slices E1 and E3: Binance perpetual context and FRED macro.
#
# TWO SCHEDULED LAMBDAS AND NOTHING ELSE. No Kinesis stream, no Firehose, no
# Secrets Manager secret, no layer, no container image.
#
#   No stream, because funding changes every eight hours and open interest has no
#   native WebSocket. A 1-second stream would repeat one value 28,800 times.
#   No Firehose, because 288 polls a day at ~4 KB is 1.2 MB a day. The buffering
#   it would provide solves a small-file problem that measurement says is absent.
#   No secret, because ALFRED's CSV export is keyless -- verified against the live
#   endpoint for all six series -- so the parent design's "no API key anywhere at
#   all" survives this slice intact.
#   No layer, because awsnative/enrichment imports only the standard library and
#   boto3, which the runtime already provides.

locals {
  name       = "${var.project}-native-enrichment"
  perp_name  = "${local.name}-perp"
  macro_name = "${local.name}-macro"

  merge_perp_name  = "${local.name}-merge-perp"
  merge_macro_name = "${local.name}-merge-macro"

  # Constructed rather than referenced, matching native_medallion: the merge
  # role needs states:ListExecutions on each state machine, and each state
  # machine needs the role -- a circular dependency if taken from the resource
  # attribute instead.
  merge_perp_arn  = "arn:aws:states:${var.region}:${var.account_id}:stateMachine:${local.merge_perp_name}"
  merge_macro_arn = "arn:aws:states:${var.region}:${var.account_id}:stateMachine:${local.merge_macro_name}"

  # Same files awsnative/render.py reads for enrichment_statements(). Keep the
  # parameter set in step with render.KNOWN_PLACEHOLDERS; tests/awsnative/
  # test_render.py fails if a .sql file starts using a name outside it.
  merge_perp_context = templatefile("${var.sql_dir}/merge_silver_perp_context.sql", {
    database      = var.glue_database_name
    lookback_days = var.merge_lookback_days
  })

  merge_macro = templatefile("${var.sql_dir}/merge_silver_macro.sql", {
    database = var.glue_database_name
  })

  # Both merges are idempotent MERGEs against Iceberg, so a blind retry
  # converges rather than double-counting -- same reasoning as
  # native_medallion's athena_retry.
  athena_retry = [{
    ErrorEquals     = ["States.ALL"]
    IntervalSeconds = 20
    MaxAttempts     = 3
    BackoffRate     = 2.0
  }]
}

# The package is awsnative/enrichment plus the namespace __init__, and nothing
# else. Excluding tests and caches keeps the zip a few kilobytes, which is what
# makes a plain zip viable where a dependency would have forced a layer.
data "archive_file" "package" {
  type        = "zip"
  output_path = "${path.module}/.build/enrichment.zip"

  source {
    content  = file("${var.source_dir}/awsnative/__init__.py")
    filename = "awsnative/__init__.py"
  }
  dynamic "source" {
    for_each = fileset("${var.source_dir}/awsnative/enrichment", "*.py")
    content {
      content  = file("${var.source_dir}/awsnative/enrichment/${source.value}")
      filename = "awsnative/enrichment/${source.value}"
    }
  }
}

# --- one role for both functions -------------------------------------------
#
# They do the same thing to the same prefix of the same bucket: fetch a public
# URL and write one object. Two roles would be two copies of one policy.

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
  # PutObject only. Neither function reads the lake, deletes from it, or touches
  # any prefix but its own Bronze one -- so the policy says exactly that rather
  # than granting the bucket.
  statement {
    sid     = "WriteBronzeEnrichment"
    effect  = "Allow"
    actions = ["s3:PutObject"]
    resources = [
      "${var.lake_bucket_arn}/bronze_perp_context/*",
      "${var.lake_bucket_arn}/bronze_macro_observations/*",
    ]
  }

  statement {
    sid    = "WriteItsOwnLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/lambda/${local.perp_name}:*",
      "arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/lambda/${local.macro_name}:*",
    ]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = local.name
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}

# Created here rather than left to the service, so retention is set from the
# start; a group Lambda creates itself never expires.
resource "aws_cloudwatch_log_group" "perp" {
  name              = "/aws/lambda/${local.perp_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "macro" {
  name              = "/aws/lambda/${local.macro_name}"
  retention_in_days = var.log_retention_days
}

# --- the two functions ------------------------------------------------------

resource "aws_lambda_function" "perp" {
  function_name    = local.perp_name
  role             = aws_iam_role.lambda.arn
  handler          = "awsnative.enrichment.collect.perp_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.package.output_path
  source_code_hash = data.archive_file.package.output_base64sha256

  # 41 sequential HTTPS requests at a 20-second per-request timeout. 120s is
  # comfortably above the observed case and well under the 5-minute cadence, so a
  # hung poll cannot overlap the next one.
  timeout     = 120
  memory_size = 256

  depends_on = [aws_cloudwatch_log_group.perp]
}

resource "aws_lambda_function" "macro" {
  function_name    = local.macro_name
  role             = aws_iam_role.lambda.arn
  handler          = "awsnative.enrichment.collect.macro_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.package.output_path
  source_code_hash = data.archive_file.package.output_base64sha256

  # Six CSV exports, the largest around 18 KB.
  timeout     = 120
  memory_size = 256

  depends_on = [aws_cloudwatch_log_group.macro]
}

# --- the two schedules ------------------------------------------------------

data "aws_iam_policy_document" "scheduler_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.account_id]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${local.name}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_trust.json
}

resource "aws_iam_role_policy" "scheduler" {
  name = "${local.name}-scheduler"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = [aws_lambda_function.perp.arn, aws_lambda_function.macro.arn]
      },
      {
        Effect   = "Allow"
        Action   = "states:StartExecution"
        Resource = [local.merge_perp_arn, local.merge_macro_arn]
      },
    ]
  })
}

resource "aws_scheduler_schedule" "perp" {
  name  = local.perp_name
  state = var.schedules_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.perp_schedule
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_lambda_function.perp.arn
    role_arn = aws_iam_role.scheduler.arn
    input = jsonencode({
      bucket = var.lake_bucket_name
      pairs  = var.instrument_pairs
    })

    # Zero retries, matching the micro-batch: a failed poll is not worth retrying
    # when the next one is five minutes away and reads a fresher grid point.
    retry_policy {
      maximum_retry_attempts = 0
    }
  }
}

resource "aws_scheduler_schedule" "macro" {
  name  = local.macro_name
  state = var.schedules_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.macro_schedule
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_lambda_function.macro.arn
    role_arn = aws_iam_role.scheduler.arn
    input = jsonencode({
      bucket = var.lake_bucket_name
      since  = var.macro_since
    })

    # Two retries here, unlike the perp poller: the next attempt is a day away,
    # not five minutes, so a transient failure is worth one more try.
    retry_policy {
      maximum_retry_attempts = 2
    }
  }
}

# --- the two Silver merges ---------------------------------------------------
#
# Each gets its own state machine on its own schedule, rather than a state
# folded into either the trades micro-batch or each other. Two reasons, not
# one: microbatch_enabled and enrichment_enabled are meant to arm independent
# things, so a shared machine would make one flag silently control the other;
# and the macro merge has no lookback window, so running it every 5 minutes
# alongside the perp merge would rescan bronze_macro_observations and
# silver_macro in full for data that changes at most once a day.
#
# Same overlap-guard shape as native_medallion. One Task each instead of a
# Parallel plus a sequential Gold: each merge is a single independent
# statement, so there is nothing here to sequence.

data "aws_iam_policy_document" "merge_sfn_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

# One role for both machines: each runs a single idempotent MERGE against the
# same lake bucket and the same Glue database, differing only in which table --
# two roles would be two copies of one policy, echoing the Lambda role above.
resource "aws_iam_role" "merge_sfn" {
  name               = "${local.name}-merge"
  assume_role_policy = data.aws_iam_policy_document.merge_sfn_trust.json
}

data "aws_iam_policy_document" "merge_sfn_permissions" {
  statement {
    sid    = "RunAthenaQueries"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
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

  # Iceberg commits go through Glue, same as native_medallion: a commit is an
  # UpdateTable with optimistic locking on the current metadata pointer.
  # Scoped to the four tables this role actually touches -- two Bronze reads,
  # two Silver writes -- narrower than native_medallion's database-wide
  # grant. The original version of this statement listed only the two Silver
  # write targets and omitted the two Bronze sources these merges read FROM,
  # which is why bronze_macro_observations returned TABLE_NOT_FOUND to this
  # role even though the table exists and is readable under any other
  # identity: found live, 2026-08-20.
  statement {
    sid    = "ReadAndCommitIcebergMetadata"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:UpdateTable",
      "glue:CreateTable",
      "glue:DeleteTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:UpdatePartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${var.account_id}:catalog",
      "arn:aws:glue:${var.region}:${var.account_id}:database/${var.glue_database_name}",
      "arn:aws:glue:${var.region}:${var.account_id}:table/${var.glue_database_name}/bronze_perp_context",
      "arn:aws:glue:${var.region}:${var.account_id}:table/${var.glue_database_name}/bronze_macro_observations",
      "arn:aws:glue:${var.region}:${var.account_id}:table/${var.glue_database_name}/silver_perp_context",
      "arn:aws:glue:${var.region}:${var.account_id}:table/${var.glue_database_name}/silver_macro",
    ]
  }

  # Every Athena query writes a result manifest to the workgroup's output
  # location before it can report SUCCEEDED, regardless of whether anything
  # ever reads that manifest back -- the merge state machines never do, but
  # the write still has to succeed. Missing this produces "Access denied
  # when writing output" on every query, found live 2026-08-20 after the
  # Glue fix above got far enough for this to be the next blocker.
  # _athena-results/ expires on its own lifecycle rule (DATA_LAYER.md
  # section 3), so nothing here needs DeleteObject.
  statement {
    sid    = "WriteAthenaQueryResults"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
    ]
    resources = ["${var.lake_bucket_arn}/_athena-results/*"]
  }

  # DeleteObject is required and easy to miss: an Iceberg MERGE rewrites data
  # files and expires the old ones. Scoped to the two Bronze prefixes this role
  # reads and the two Silver prefixes it writes, not the whole bucket.
  statement {
    sid    = "ReadBronzeAndWriteSilver"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
    ]
    resources = [
      "${var.lake_bucket_arn}/bronze_perp_context/*",
      "${var.lake_bucket_arn}/bronze_macro_observations/*",
      "${var.lake_bucket_arn}/silver_perp_context/*",
      "${var.lake_bucket_arn}/silver_macro/*",
    ]
  }

  # GetBucketLocation is its own statement, unconditional: Athena calls it to
  # verify the workgroup's output bucket before running any query, and that
  # call carries no s3:prefix -- bundled into the conditional statement below,
  # the condition never matches, the grant never applies, and every query
  # fails before it starts with "Unable to verify/create output bucket".
  # GetBucketLocation only reveals the bucket's region, so granting it
  # unconditionally leaks nothing this role does not already know.
  statement {
    sid       = "GetBucketLocationForAthenaOutput"
    effect    = "Allow"
    actions   = ["s3:GetBucketLocation"]
    resources = [var.lake_bucket_arn]
  }

  # ListBucket is a bucket-level action -- it cannot be scoped by object ARN --
  # so the s3:prefix condition is what keeps it from seeing the rest of the
  # lake. _athena-results included so Athena can list its own output prefix
  # while verifying/writing the result manifest, same reasoning as
  # WriteAthenaQueryResults above.
  statement {
    sid       = "ListOnlyTheseFivePrefixes"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [var.lake_bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "bronze_perp_context/*",
        "bronze_macro_observations/*",
        "silver_perp_context/*",
        "silver_macro/*",
        "_athena-results/*",
      ]
    }
  }

  # The overlap guard on each machine reads its own execution list.
  statement {
    sid       = "SeeItsOwnExecutions"
    effect    = "Allow"
    actions   = ["states:ListExecutions"]
    resources = [local.merge_perp_arn, local.merge_macro_arn]
  }

  # Step Functions' logging configuration requires these on "*", same as
  # native_medallion: the delivery is created by the service, not by us, so it
  # cannot be scoped to a log group.
  statement {
    sid    = "DeliverExecutionLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "merge_sfn" {
  name   = "${local.name}-merge"
  role   = aws_iam_role.merge_sfn.id
  policy = data.aws_iam_policy_document.merge_sfn_permissions.json
}

resource "aws_cloudwatch_log_group" "merge_perp" {
  name              = "/aws/vendedlogs/states/${local.merge_perp_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "merge_macro" {
  name              = "/aws/vendedlogs/states/${local.merge_macro_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_sfn_state_machine" "merge_perp" {
  name     = local.merge_perp_name
  role_arn = aws_iam_role.merge_sfn.arn

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.merge_perp.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  definition = jsonencode({
    Comment = "bronze_perp_context -> silver_perp_context, on the perp poll's own schedule"
    StartAt = "CountRunningExecutions"
    States = {
      CountRunningExecutions = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sfn:listExecutions"
        Parameters = {
          "StateMachineArn.$" = "$$.StateMachine.Id"
          StatusFilter        = "RUNNING"
          MaxResults          = 2
        }
        ResultSelector = { "running.$" = "States.ArrayLength($.Executions)" }
        ResultPath     = "$.overlap"
        Next           = "AlreadyRunning"
      }

      AlreadyRunning = {
        Type = "Choice"
        Choices = [{
          Variable           = "$.overlap.running"
          NumericGreaterThan = 1
          Next               = "SkippedOverlappingRun"
        }]
        Default = "MergePerpContext"
      }

      SkippedOverlappingRun = {
        Type = "Succeed"
      }

      MergePerpContext = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.merge_perp_context
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        End            = true
      }
    }
  })
}

resource "aws_sfn_state_machine" "merge_macro" {
  name     = local.merge_macro_name
  role_arn = aws_iam_role.merge_sfn.arn

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.merge_macro.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  definition = jsonencode({
    Comment = "bronze_macro_observations -> silver_macro, once a day after the macro pull"
    StartAt = "CountRunningExecutions"
    States = {
      CountRunningExecutions = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sfn:listExecutions"
        Parameters = {
          "StateMachineArn.$" = "$$.StateMachine.Id"
          StatusFilter        = "RUNNING"
          MaxResults          = 2
        }
        ResultSelector = { "running.$" = "States.ArrayLength($.Executions)" }
        ResultPath     = "$.overlap"
        Next           = "AlreadyRunning"
      }

      AlreadyRunning = {
        Type = "Choice"
        Choices = [{
          Variable           = "$.overlap.running"
          NumericGreaterThan = 1
          Next               = "SkippedOverlappingRun"
        }]
        Default = "MergeMacro"
      }

      SkippedOverlappingRun = {
        Type = "Succeed"
      }

      MergeMacro = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.merge_macro
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        End            = true
      }
    }
  })
}

resource "aws_scheduler_schedule" "merge_perp" {
  name  = local.merge_perp_name
  state = var.schedules_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.perp_schedule
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_sfn_state_machine.merge_perp.arn
    role_arn = aws_iam_role.scheduler.arn

    # Zero retries, matching the perp poll's own schedule: the next tick is
    # five minutes away and reads a fresher Bronze window regardless.
    retry_policy {
      maximum_retry_attempts = 0
    }
  }
}

resource "aws_scheduler_schedule" "merge_macro" {
  name  = local.merge_macro_name
  state = var.schedules_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.macro_schedule
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_sfn_state_machine.merge_macro.arn
    role_arn = aws_iam_role.scheduler.arn

    # Two retries, matching the macro poll's own schedule: the next attempt is
    # a day away, so a transient failure to even start is worth retrying.
    retry_policy {
      maximum_retry_attempts = 2
    }
  }
}
