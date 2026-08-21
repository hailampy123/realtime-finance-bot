variable "project" {
  type = string
}

variable "region" {
  type = string
}

variable "account_id" {
  type = string
}

variable "lake_bucket_arn" {
  type = string
}

variable "glue_database_name" {
  type = string
}

variable "athena_workgroup_name" {
  type = string
}

variable "source_dir" {
  type        = string
  description = "Absolute path to the repo root, for packaging the health-metrics Lambda."
}

variable "sql_dir" {
  type        = string
  description = <<-EOT
    Absolute path to awsnative/sql. Packaged into the Lambda so
    awsnative/render.py -- which resolves its SQL_DIR relative to its own
    file location -- finds its templates at runtime the same way it does
    when imported outside a Lambda.
  EOT
}

variable "alert_notification_email" {
  type        = string
  description = "Where every CloudWatch alarm in this module sends mail."
}

variable "log_retention_days" {
  type    = number
  default = 7
}
