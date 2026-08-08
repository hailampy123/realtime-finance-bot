variable "project" {
  description = "Prefix for all resource names."
  type        = string
  default     = "fdai"
}

variable "region" {
  description = "AWS region for the sandbox stack."
  type        = string
  default     = "ap-southeast-1"
}
