variable "project" {
  type = string
}

variable "vpc_cidr" {
  description = "Deliberately not 10.42.0.0/16 -- that is the MSK stack's VPC, and non-overlapping ranges keep peering an option later."
  type        = string
  default     = "10.43.0.0/16"
}

variable "az_count" {
  description = "Two so ECS has an alternative placement if one AZ is unavailable."
  type        = number
  default     = 2
}
