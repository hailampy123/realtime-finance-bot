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
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = [aws_lambda_function.perp.arn, aws_lambda_function.macro.arn]
    }]
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
