variable "project" {
  description = "Resource name prefix. Shared with the MSK stack; native resources add a -native- infix."
  type        = string
  default     = "fdai"
}

variable "region" {
  type = string
}

variable "budget_notification_email" {
  description = "Where the monthly cost alarm sends mail. A wiped account still bills for the days it ran."
  type        = string
}

variable "alert_notification_email" {
  description = "Where CloudWatch alarms for the AWS-native stack send mail."
  type        = string
}

variable "monthly_budget_usd" {
  description = "Alarm threshold. Spec section 10 estimates ~$25-35/mo at 30h/week."
  type        = number
  default     = 50
}

variable "microbatch_schedule" {
  description = "How often Bronze is merged into Silver and Gold. Freshness against Athena spend."
  type        = string
  default     = "rate(5 minutes)"
}

variable "microbatch_enabled" {
  description = <<-EOT
    Arm the schedule. Set false to deploy the state machine and trigger it by
    hand first -- the recommended way to meet stage N2, because a failure you
    caused is much easier to read than one that arrived on a timer.
  EOT
  type        = bool
  default     = true
}

variable "microbatch_lookback_days" {
  description = <<-EOT
    How far back into Bronze each run reads. 1 means today and yesterday, which
    covers the UTC midnight boundary. A cost dial, not a correctness one: the
    merges are idempotent.
  EOT
  type        = number
  default     = 1
}

variable "enrichment_enabled" {
  type        = bool
  default     = false
  description = <<-EOT
    Arms the four enrichment schedules: the perp and macro collectors, and
    their Silver merges. Off by default for the same reason the micro-batch is:
    the first apply of a stage should not start spending before anyone has
    looked at it.
  EOT
}

variable "enrichment_instrument_pairs" {
  type = list(list(string))
  default = [
    ["BTC-USD", "BTCUSDT"],
    ["ETH-USD", "ETHUSDT"],
    ["SOL-USD", "SOLUSDT"],
    ["XRP-USD", "XRPUSDT"],
    ["ADA-USD", "ADAUSDT"],
    ["LINK-USD", "LINKUSDT"],
    ["AVAX-USD", "AVAXUSDT"],
    ["DOGE-USD", "DOGEUSDT"],
  ]
  description = <<-EOT
    [[instrument_id, venue_symbol], ...]. Mirrors config/universe.yaml's binance
    entries; Terraform cannot read the YAML, so the two are kept in step by
    tests/awsnative/test_enrichment_wiring.py rather than by hope.
  EOT
}
