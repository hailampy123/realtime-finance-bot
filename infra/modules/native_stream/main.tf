locals {
  stream_name   = "${var.project}-native-md-trades-v1"
  firehose_name = "${var.project}-native-bronze-trades"
}

# On-demand rather than provisioned shards. Steady state is ~300 msg/s, but
# crypto trade rates burst 5-10x during volatility and a single provisioned
# shard caps at 1000 records/s -- it would throttle exactly when the data
# matters most. Two provisioned shards is the documented cost lever
# (~$3/mo cheaper, spec section 10), not the default: shard math is a thing to
# get wrong, and getting it wrong drops trades.
resource "aws_kinesis_stream" "trades" {
  name             = local.stream_name
  retention_period = var.retention_hours
  encryption_type  = "KMS"
  kms_key_id       = "alias/aws/kinesis"

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
}

# --- Firehose service role -------------------------------------------------
# Two halves: a trust policy saying Firehose may assume this role, and
# permission policies saying what it may then do.

data "aws_iam_policy_document" "firehose_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
    # Belt and braces: only this account's Firehose may assume it, so the role
    # cannot be used cross-account even if its ARN leaks.
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.account_id]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${var.project}-native-firehose"
  assume_role_policy = data.aws_iam_policy_document.firehose_trust.json
}

data "aws_iam_policy_document" "firehose_permissions" {
  # Read the stream it is sourced from.
  statement {
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards",
    ]
    resources = [aws_kinesis_stream.trades.arn]
  }

  # Decrypt the stream, which is KMS-encrypted above.
  statement {
    effect    = "Allow"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = ["arn:aws:kms:${var.region}:${var.account_id}:alias/aws/kinesis"]
  }

  # Write objects, and list the bucket (Firehose checks before writing).
  statement {
    effect = "Allow"
    actions = ["s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:GetObject",
    "s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:PutObject"]
    resources = [
      var.lake_bucket_arn,
      "${var.lake_bucket_arn}/*",
    ]
  }

  # Read the Glue table schema -- this is how it knows how to write Parquet.
  statement {
    effect  = "Allow"
    actions = ["glue:GetTable", "glue:GetTableVersion", "glue:GetTableVersions"]
    resources = [
      "arn:aws:glue:${var.region}:${var.account_id}:catalog",
      "arn:aws:glue:${var.region}:${var.account_id}:database/${var.glue_database_name}",
      "arn:aws:glue:${var.region}:${var.account_id}:table/${var.glue_database_name}/${var.bronze_table_name}",
    ]
  }

  statement {
    effect    = "Allow"
    actions   = ["logs:PutLogEvents", "logs:CreateLogStream"]
    resources = ["arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/kinesisfirehose/*"]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "${var.project}-native-firehose"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose_permissions.json
}

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${local.firehose_name}"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

resource "aws_kinesis_firehose_delivery_stream" "bronze" {
  name        = local.firehose_name
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.trades.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = var.lake_bucket_arn

    # Time-based custom prefix, which is free. NOT dynamic partitioning, which
    # keys off record content and is billed per GB. Partitioning on arrival date
    # (not event date) is deliberate: it matches the Databricks workstream's
    # ingest_date Bronze partitioning, and it means a late record never rewrites
    # an old partition.
    prefix              = "${var.bronze_prefix}/ingest_date=!{timestamp:yyyy-MM-dd}/"
    error_output_prefix = "_errors/${var.bronze_prefix}/!{firehose:error-output-type}/"

    buffering_size     = var.buffer_size_mb
    buffering_interval = var.buffer_interval_seconds

    # No compression_format here: Parquet carries its own (SNAPPY, from the Glue
    # table's parquet.compression property). Setting GZIP alongside format
    # conversion is rejected.
    data_format_conversion_configuration {
      input_format_configuration {
        deserializer {
          open_x_json_ser_de {}
        }
      }
      output_format_configuration {
        serializer {
          parquet_ser_de {}
        }
      }
      schema_configuration {
        role_arn      = aws_iam_role.firehose.arn
        database_name = var.glue_database_name
        table_name    = var.bronze_table_name
      }
    }

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose.name
    }
  }
}
