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
