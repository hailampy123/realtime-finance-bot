variable "project" {
  type = string
}

variable "region" {
  type = string
}

variable "account_id" {
  type = string
}

variable "lake_bucket_name" {
  type = string
}

variable "lake_bucket_arn" {
  type = string
}

variable "source_dir" {
  type        = string
  description = <<-EOT
    Absolute path to the repo root. The Lambda package is built from
    awsnative/enrichment/, which imports nothing outside the standard library --
    that is why there is no layer, no container image and no build step here.
  EOT
}

variable "instrument_pairs" {
  type        = list(list(string))
  description = "[[instrument_id, venue_symbol], ...] for the Binance universe."
}

variable "perp_schedule" {
  type        = string
  default     = "rate(5 minutes)"
  description = <<-EOT
    Matches the micro-batch cadence and the 5-minute grid the Binance ratio
    endpoints answer on. One poll is 41 requests against a limit of 1000 per
    five minutes per IP, so eight instruments use ~4% of the budget.
  EOT
}

variable "macro_schedule" {
  type        = string
  default     = "cron(30 6 * * ? *)"
  description = <<-EOT
    Once a day, after the US market series settle. A stream would be absurd for
    data that changes daily, and a second pull of the same vintage is the same
    answer.
  EOT
}

variable "macro_since" {
  type        = string
  default     = "2023-01-01"
  description = "Oldest observation to pull. Bounds the object each run writes."
}

variable "schedules_enabled" {
  type    = bool
  default = false
}

variable "log_retention_days" {
  type    = number
  default = 7
}
