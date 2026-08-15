locals {
  bucket_name   = "${var.project}-native-lake-${var.account_id}"
  bronze_table  = "bronze_trades_stream"
  bronze_prefix = "bronze_trades_stream"
}

# force_destroy because this bucket holds only re-derivable data (spec section 6)
# and `make down-aws` must not fail on a non-empty bucket.
resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Athena query results and Firehose error output accumulate and are never read
# after the fact; expiring them keeps the bill honest.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "_athena-results/"
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-firehose-errors"
    status = "Enabled"
    filter {
      prefix = "_errors/"
    }
    expiration {
      days = 14
    }
  }
}

resource "aws_glue_catalog_database" "lake" {
  name        = "${replace(var.project, "-", "_")}_native"
  description = "AWS-native workstream lakehouse"
}

# Bronze is plain Parquet, not Iceberg: it is append-only, so MERGE, time travel
# and snapshot expiry all buy nothing (spec D1).
resource "aws_glue_catalog_table" "bronze" {
  name          = local.bronze_table
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"

    # Partition projection: Athena computes partition locations from this pattern
    # instead of listing them, which removes the Glue crawler entirely and with
    # it any window where S3 has a partition the catalog does not.
    "projection.enabled"                   = "true"
    "projection.ingest_date.type"          = "date"
    "projection.ingest_date.format"        = "yyyy-MM-dd"
    "projection.ingest_date.range"         = "${var.projection_start_date},NOW"
    "projection.ingest_date.interval"      = "1"
    "projection.ingest_date.interval.unit" = "DAYS"
    # $${} escapes Terraform interpolation -- Athena needs the literal ${ingest_date}.
    "storage.location.template" = "s3://${aws_s3_bucket.lake.bucket}/${local.bronze_prefix}/ingest_date=$${ingest_date}/"
  }

  partition_keys {
    name = "ingest_date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/${local.bronze_prefix}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # Must match awsnative/encode.py field-for-field. The contract test in
    # tests/awsnative/test_encode.py keeps the encoder honest against
    # trade.v1.avsc; this table is the third party to that agreement, and
    # the stage N1 verification queries are what proves it lines up.
    columns {
      name = "venue"
      type = "string"
    }
    columns {
      name = "venue_symbol"
      type = "string"
    }
    columns {
      name = "instrument_id"
      type = "string"
    }
    columns {
      name = "trade_id"
      type = "string"
    }
    columns {
      name = "event_ts_us"
      type = "bigint"
    }
    columns {
      name = "ingest_ts_us"
      type = "bigint"
    }
    columns {
      name = "price"
      type = "string"
    }
    columns {
      name = "size"
      type = "string"
    }
    columns {
      name = "side"
      type = "string"
    }
    columns {
      name = "sequence"
      type = "bigint"
    }
    columns {
      name = "is_backfill"
      type = "boolean"
    }
    columns {
      name = "source"
      type = "string"
    }
    columns {
      name = "schema_version"
      type = "int"
    }
  }
}

resource "aws_athena_workgroup" "native" {
  name          = "${var.project}-native"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/_athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
