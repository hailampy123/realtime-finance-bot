variable "project" {
  type = string
}

variable "region" {
  type = string
}

variable "account_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_id" {
  type = string
}

variable "stream_arn" {
  type = string
}

variable "stream_name" {
  type = string
}

variable "task_cpu" {
  description = "Fargate CPU units. 256 = 0.25 vCPU. Two WebSocket connections and JSON encoding is not CPU-bound."
  type        = number
  default     = 512
}

variable "task_memory" {
  description = "MiB. Must be a valid pairing with task_cpu -- 512 CPU allows 1024-4096."
  type        = number
  default     = 1024
}

variable "image_tag" {
  type    = string
  default = "latest"
}

variable "desired_count" {
  description = "One. Two tasks would double every trade, since both would consume the same WebSocket streams."
  type        = number
  default     = 1
}
