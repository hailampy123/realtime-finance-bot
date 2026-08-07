variable "project" {
  type = string
}

variable "subnet_id" {
  type = string
}

variable "security_group_id" {
  type = string
}

variable "instance_type" {
  type    = string
  default = "t3.small"
}

variable "repo_url" {
  description = "Public HTTPS clone URL for this repository."
  type        = string
}

variable "repo_ref" {
  type    = string
  default = "main"
}

variable "bootstrap_servers" {
  type = string
}

variable "sasl_username" {
  type = string
}

variable "sasl_password" {
  type      = string
  sensitive = true
}

variable "venues" {
  type    = list(string)
  default = ["binance", "coinbase"]
}

variable "instance_profile_name" {
  description = "Pre-existing instance profile, if the account provides one. Null means none."
  type        = string
  default     = null
}
