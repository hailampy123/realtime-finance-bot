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
