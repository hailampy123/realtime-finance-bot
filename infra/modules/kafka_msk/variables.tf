variable "project" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "security_group_id" {
  type = string
}

variable "kafka_version" {
  type    = string
  default = "3.6.0"
}

variable "broker_instance_type" {
  type    = string
  default = "kafka.t3.small"
}

variable "broker_count" {
  description = "Must be a multiple of the subnet count."
  type        = number
  default     = 2
}

variable "broker_ebs_gb" {
  description = "50 GB covers 24h retention at ~5 GB/day with headroom."
  type        = number
  default     = 50
}

variable "public_access" {
  description = "Enable on the last apply; AWS rejects it while the cluster is CREATING, and again unless restrict_acls is already true."
  type        = bool
  default     = false
}

variable "restrict_acls" {
  description = <<-DESC
    Sets allow.everyone.if.no.acl.found. Must be true before public_access can
    be enabled, and must not be turned on until scripts/create_acls.py has run
    against the cluster -- a cluster with deny-by-default and no ACLs rejects
    every client, including one trying to add ACLs. Set false to unlock such a
    cluster.
  DESC
  type        = bool
  default     = false
}
