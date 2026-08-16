output "bucket_name" {
  value = aws_s3_bucket.lake.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.lake.arn
}

output "glue_database_name" {
  value = aws_glue_catalog_database.lake.name
}

output "bronze_table_name" {
  value = aws_glue_catalog_table.bronze.name
}

output "bronze_prefix" {
  value = local.bronze_prefix
}

output "athena_workgroup_name" {
  value = aws_athena_workgroup.native.name
}
