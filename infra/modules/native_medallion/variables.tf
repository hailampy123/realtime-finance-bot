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

variable "sql_dir" {
  type        = string
  description = <<-EOT
    Absolute path to awsnative/sql. The merge statements are rendered from the
    same .sql files awsnative/render.py reads, so the transform has one
    definition rather than one per consumer.
  EOT
}

variable "lookback_days" {
  type        = number
  default     = 1
  description = <<-EOT
    How far back into Bronze each micro-batch reads. 1 means today and
    yesterday, which covers the UTC midnight boundary.

    This is a cost dial, not a correctness one: the merges are idempotent, so a
    wider window costs money and changes no result. Widen it after an outage
    long enough that unmerged Bronze partitions fell outside it, then put it
    back.
  EOT
}

variable "schedule_expression" {
  type        = string
  default     = "rate(5 minutes)"
  description = <<-EOT
    The freshness/cost trade-off in one string. Spec section 10 names
    lengthening this to 15 minutes as the third-largest cost lever, at the
    price of the p50 < 6 min end-to-end SLO.
  EOT
}

variable "schedule_enabled" {
  type        = bool
  default     = true
  description = <<-EOT
    Set false to deploy the state machine without arming the schedule, so the
    first executions can be triggered by hand and watched. Managing this
    through Terraform rather than by clicking Disable in the console keeps the
    console and the state file from disagreeing.
  EOT
}

variable "query_timeout_seconds" {
  type        = number
  default     = 600
  description = "Per-Athena-statement timeout. A merge that takes ten minutes is a bug, not a big day."
}

variable "log_retention_days" {
  type    = number
  default = 7
}

variable "health_metrics_function_arn" {
  type        = string
  description = "ARN of the health-metrics Lambda (native_monitoring module), invoked as this state machine's final tail state."
}
