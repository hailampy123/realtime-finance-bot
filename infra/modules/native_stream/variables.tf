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

variable "bronze_table_name" {
  type = string
}

variable "bronze_prefix" {
  type = string
}

variable "retention_hours" {
  description = "24h matches the Kafka topic's retention, so the two workstreams lose the same amount on teardown."
  type        = number
  default     = 24
}

variable "buffer_interval_seconds" {
  description = <<-DESC
    Firehose writes when size OR interval is hit. 120s puts producer->Bronze lag
    around 2 minutes, inside the parent spec's p50 < 6 min SLO once the 5-minute
    merge cadence is added. Raise it for fewer, larger files; lower it for
    fresher data.
  DESC
  type        = number
  default     = 120
}

variable "buffer_size_mb" {
  description = "Minimum is 64 when data format conversion is enabled."
  type        = number
  default     = 128
}
