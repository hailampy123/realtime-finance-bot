locals {
  name = "${var.project}-native-microbatch"

  # Constructed rather than referenced, to break a circular dependency: the
  # state machine's role needs states:ListExecutions on the state machine, and
  # the state machine needs the role.
  state_machine_arn = "arn:aws:states:${var.region}:${var.account_id}:stateMachine:${local.name}"

  # --- SQL, rendered from the same files awsnative/render.py reads ----------
  #
  # Terraform's templatefile() and Python's string.Template both use ${...},
  # which is why these files can have exactly one copy. Keep the parameter set
  # in step with render.KNOWN_PLACEHOLDERS; tests/awsnative/test_render.py
  # fails if a .sql file starts using a name outside it.
  #
  # valid_expr is passed unwrapped. The COALESCE and the NOT live inside the
  # two merge files, so neither language can apply them differently.
  valid_expr = file("${var.sql_dir}/fragments/valid_trade.sql")

  dirty_cte = templatefile("${var.sql_dir}/fragments/dirty_from_bronze.sql", {
    database      = var.glue_database_name
    lookback_days = var.lookback_days
  })

  merge_silver = templatefile("${var.sql_dir}/merge_silver_trades.sql", {
    database      = var.glue_database_name
    lookback_days = var.lookback_days
    valid_expr    = local.valid_expr
  })

  merge_quarantine = templatefile("${var.sql_dir}/merge_silver_quarantine.sql", {
    database      = var.glue_database_name
    lookback_days = var.lookback_days
    valid_expr    = local.valid_expr
  })

  merge_gold = templatefile("${var.sql_dir}/merge_gold_bars_1m.sql", {
    database  = var.glue_database_name
    dirty_cte = local.dirty_cte
  })

  # Every Athena state gets the same retry. Blind retries are safe here for one
  # specific reason: all three statements are idempotent MERGEs against Iceberg,
  # so re-running a partially-failed one converges rather than double-counting.
  # That is not a general property of retries and should not be copied to a
  # state whose task is not idempotent.
  athena_retry = [{
    ErrorEquals     = ["States.ALL"]
    IntervalSeconds = 20
    MaxAttempts     = 3
    BackoffRate     = 2.0
  }]

  # --- maintenance SQL, spec 2026-08-19 section 3 ---------------------------
  #
  # One partition predicate per table, matching each table's own partition
  # column (spec 2026-08-17 section 8.2). Rendered independently from
  # awsnative/render.py's maintenance_statements(), which the same two files
  # feed for tests; scripts/native_render_parity.sh checks the two agree.
  maintenance_predicates = {
    silver_trades            = "event_ts >= current_date - interval '${var.lookback_days}' day"
    silver_trades_quarantine = "ingest_date >= date_format(current_date - interval '${var.lookback_days}' day, '%Y-%m-%d')"
    gold_bars_1m             = "window_end_ts >= current_date - interval '${var.lookback_days}' day"
    # Written by three state machines, so this module is the sole
    # maintainer of native_health_metrics -- neither merge_perp nor
    # merge_macro runs OPTIMIZE/VACUUM against it (spec
    # 2026-08-19-iceberg-housekeeping-monitoring-design.md section 4.3).
    native_health_metrics = "metric_ts >= current_date - interval '${var.lookback_days}' day"
  }

  optimize_sql = {
    for table, predicate in local.maintenance_predicates : table => templatefile("${var.sql_dir}/optimize_table.sql", {
      database            = var.glue_database_name
      table               = table
      partition_predicate = predicate
    })
  }

  vacuum_sql = {
    for table in keys(local.maintenance_predicates) : table => templatefile("${var.sql_dir}/vacuum_table.sql", {
      database = var.glue_database_name
      table    = table
    })
  }
}

# --- the state machine's role ----------------------------------------------

data "aws_iam_policy_document" "sfn_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = local.name
  assume_role_policy = data.aws_iam_policy_document.sfn_trust.json
}

data "aws_iam_policy_document" "sfn_permissions" {
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

  # Iceberg commits go through Glue: a commit is an UpdateTable with optimistic
  # locking on the current metadata pointer. Without UpdateTable the MERGE runs,
  # writes its data files, and then fails to publish them.
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
      "arn:aws:glue:${var.region}:${var.account_id}:table/${var.glue_database_name}/*",
    ]
  }

  # DeleteObject is required and easy to miss: an Iceberg MERGE rewrites data
  # files and expires the old ones, and a merge that cannot delete fails partway
  # with an error that reads like a bug in the SQL.
  statement {
    sid    = "ReadAndWriteTheLake"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:GetBucketLocation",
    ]
    resources = [var.lake_bucket_arn, "${var.lake_bucket_arn}/*"]
  }

  # The overlap guard reads its own execution list. $$.StateMachine.Id in the
  # definition resolves to exactly this ARN.
  statement {
    sid       = "SeeItsOwnExecutions"
    effect    = "Allow"
    actions   = ["states:ListExecutions"]
    resources = [local.state_machine_arn]
  }

  statement {
    sid       = "InvokeHealthMetricsLambda"
    effect    = "Allow"
    actions   = ["lambda:InvokeFunction"]
    resources = [var.health_metrics_function_arn]
  }

  # Step Functions' logging configuration requires these on "*" -- the delivery
  # is created by the service, not by us, so it cannot be scoped to a log group.
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

resource "aws_iam_role_policy" "sfn" {
  name   = local.name
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn_permissions.json
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${local.name}"
  retention_in_days = var.log_retention_days
}

# --- the state machine ------------------------------------------------------

resource "aws_sfn_state_machine" "microbatch" {
  name     = local.name
  role_arn = aws_iam_role.sfn.arn

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  definition = jsonencode({
    Comment = "Bronze -> Silver (+ quarantine) -> Gold, every ${var.schedule_expression}"
    StartAt = "CountRunningExecutions"
    States = {

      # A five-minute schedule and a merge that occasionally takes longer would
      # otherwise overlap. Two concurrent MERGEs into one Iceberg table do not
      # corrupt it -- the second commit loses the optimistic lock and fails --
      # but they do pay twice for one result, and the failure looks like a bug.
      # Skipping is right rather than queueing: the next tick reads the same
      # window, so nothing is missed by not running now.
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
          # Greater than one, not zero: this execution is in its own list.
          Variable           = "$.overlap.running"
          NumericGreaterThan = 1
          Next               = "SkippedOverlappingRun"
        }]
        Default = "SilverAndQuarantine"
      }

      SkippedOverlappingRun = {
        Type = "Succeed"
      }

      # Parallel, matching spec 3.1's M1. The two branches write different
      # tables so they cannot conflict, and both are idempotent, so a branch
      # failing after the other succeeded leaves a state a plain retry fixes.
      SilverAndQuarantine = {
        Type       = "Parallel"
        ResultPath = null
        Next       = "MergeGold"
        Branches = [
          {
            StartAt = "MergeSilver"
            States = {
              MergeSilver = {
                Type     = "Task"
                Resource = "arn:aws:states:::athena:startQueryExecution.sync"
                Parameters = {
                  QueryString           = local.merge_silver
                  WorkGroup             = var.athena_workgroup_name
                  QueryExecutionContext = { Database = var.glue_database_name }
                }
                Retry          = local.athena_retry
                TimeoutSeconds = var.query_timeout_seconds
                End            = true
              }
            }
          },
          {
            StartAt = "MergeQuarantine"
            States = {
              MergeQuarantine = {
                Type     = "Task"
                Resource = "arn:aws:states:::athena:startQueryExecution.sync"
                Parameters = {
                  QueryString           = local.merge_quarantine
                  WorkGroup             = var.athena_workgroup_name
                  QueryExecutionContext = { Database = var.glue_database_name }
                }
                Retry          = local.athena_retry
                TimeoutSeconds = var.query_timeout_seconds
                End            = true
              }
            }
          },
        ]
      }

      # Consecutive rather than parallel, and that ordering is spec D3: one
      # execution owns the Silver merge AND the Gold rebuild, which is why
      # nothing here needs a Change Data Feed to discover what moved.
      MergeGold = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.merge_gold
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "DeriveMaintenanceClock"
      }

      # Housekeeping tail, spec 2026-08-17 section 8.1: one state machine
      # stays one writer, so maintenance runs after the merge that owns
      # these tables rather than on a separate schedule. $$.Execution.StartTime
      # is the tick this execution was scheduled for, which is what the
      # minute/hour gates below key off (spec 2026-08-17, assumption M3).
      DeriveMaintenanceClock = {
        Type = "Pass"
        Parameters = {
          "hour.$"   = "States.ArrayGetItem(States.StringSplit(States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 1), ':'), 0)"
          "minute.$" = "States.ArrayGetItem(States.StringSplit(States.ArrayGetItem(States.StringSplit($$.Execution.StartTime, 'T'), 1), ':'), 1)"
        }
        ResultPath = "$.clock"
        Next       = "IsTopOfHour"
      }

      IsTopOfHour = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.clock.minute"
          StringEquals = "00"
          Next         = "OptimizeSilverTrades"
        }]
        Default = "CollectHealthMetrics"
      }

      OptimizeSilverTrades = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.optimize_sql["silver_trades"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "OptimizeQuarantine"
      }

      OptimizeQuarantine = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.optimize_sql["silver_trades_quarantine"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "OptimizeGold"
      }

      OptimizeGold = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.optimize_sql["gold_bars_1m"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "OptimizeNativeHealthMetrics"
      }

      OptimizeNativeHealthMetrics = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.optimize_sql["native_health_metrics"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "IsTopOfDay"
      }

      # VACUUM runs daily regardless of which hour triggered OPTIMIZE above:
      # expiry and orphan removal address storage, the slow-moving problem,
      # while OPTIMIZE addresses reads, the fast one (spec 2026-08-17,
      # section 8.2). Every path -- whether or not this is the top of the
      # hour or the day -- converges on CollectHealthMetrics: a tick that
      # skips maintenance must not also skip reporting on the tables.
      IsTopOfDay = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.clock.hour"
          StringEquals = "00"
          Next         = "VacuumSilverTrades"
        }]
        Default = "CollectHealthMetrics"
      }

      VacuumSilverTrades = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.vacuum_sql["silver_trades"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "VacuumQuarantine"
      }

      VacuumQuarantine = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.vacuum_sql["silver_trades_quarantine"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "VacuumGold"
      }

      VacuumGold = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.vacuum_sql["gold_bars_1m"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "VacuumNativeHealthMetrics"
      }

      VacuumNativeHealthMetrics = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString           = local.vacuum_sql["native_health_metrics"]
          WorkGroup             = var.athena_workgroup_name
          QueryExecutionContext = { Database = var.glue_database_name }
        }
        Retry          = local.athena_retry
        TimeoutSeconds = var.query_timeout_seconds
        Next           = "CollectHealthMetrics"
      }

      # Replaces Plan 1's MaintenanceDone Succeed state: every path through
      # maintenance, run or skipped, ends here. Monitoring section 4.1.
      CollectHealthMetrics = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.health_metrics_function_arn
          Payload = {
            database  = var.glue_database_name
            workgroup = var.athena_workgroup_name
            tables    = ["silver_trades", "silver_trades_quarantine", "gold_bars_1m"]
          }
        }
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 10
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        TimeoutSeconds = 200
        End            = true
      }
    }
  })
}

# --- the schedule -----------------------------------------------------------

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
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.microbatch.arn
    }]
  })
}

# EventBridge Scheduler rather than an EventBridge rule with a target: one
# resource instead of two, and it can be disabled through the `state` argument
# without Terraform seeing drift the next time it plans.
resource "aws_scheduler_schedule" "microbatch" {
  name  = local.name
  state = var.schedule_enabled ? "ENABLED" : "DISABLED"

  # OFF, not a jitter window: the merges are cheap and the freshness SLO is
  # stated in minutes, so there is nothing to smooth out.
  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_sfn_state_machine.microbatch.arn
    role_arn = aws_iam_role.scheduler.arn

    # Zero retries on purpose. A failed start is not worth retrying when
    # another tick is at most five minutes away and reads the same window.
    retry_policy {
      maximum_retry_attempts = 0
    }
  }
}
