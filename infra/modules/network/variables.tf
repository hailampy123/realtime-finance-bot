variable "project" {
  type = string
}

variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}

variable "az_count" {
  description = "MSK requires at least two AZs."
  type        = number
  default     = 2
}

variable "kafka_client_cidrs" {
  description = "CIDRs allowed to reach the brokers — the Databricks workspace NAT EIP, plus your own IP."
  type        = list(string)
}
