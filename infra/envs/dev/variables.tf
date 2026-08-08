variable "project" {
  type    = string
  default = "fdai"
}

variable "region" {
  type    = string
  default = "ap-southeast-1"
}

variable "kafka_client_cidrs" {
  description = "Databricks workspace NAT EIP as /32, plus your own IP as /32."
  type        = list(string)
}

variable "repo_url" {
  type = string
}

variable "repo_ref" {
  type    = string
  default = "main"
}

variable "msk_public_access" {
  description = "False on the first apply, true on the second. make up handles both."
  type        = bool
  default     = false
}

variable "instance_profile_name" {
  description = "Pre-existing instance profile if the account has one; null otherwise."
  type        = string
  default     = null
}
