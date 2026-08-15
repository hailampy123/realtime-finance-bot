variable "project" {
  type = string
}

variable "account_id" {
  type = string
}

variable "projection_start_date" {
  description = <<-DESC
    Lower bound for Athena partition projection on ingest_date. Projection
    computes partition locations arithmetically instead of listing them, so this
    bounds the search space -- set it to roughly when the account was created,
    not to 1970, or every query enumerates decades of nonexistent partitions.
  DESC
  type        = string
  default     = "2026-01-01"
}
