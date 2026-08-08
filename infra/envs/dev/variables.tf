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
  description = "Last of three applies. make up handles the sequence; see scripts/bootstrap.sh."
  type        = bool
  default     = false
}

variable "msk_restrict_acls" {
  description = <<-DESC
    Sets allow.everyone.if.no.acl.found=false, which MSK requires before it
    will enable public access. Only turn this on after scripts/create_acls.py
    has run: deny-by-default with no ACLs rejects every client, including one
    trying to add ACLs. make up sequences this correctly; set it back to false
    to unlock a cluster that got tightened too early.
  DESC
  type        = bool
  default     = false
}

variable "operator_cidrs" {
  description = <<-DESC
    The machine running make up, as a /32. Detected and passed by
    scripts/bootstrap.sh, so it needs no value in terraform.tfvars and cannot
    go stale when your IP changes.

    Used for two things: SSH to the producer host (the ACL bootstrap step), and
    appended to kafka_client_cidrs so create_topics and the smoke test can
    reach the brokers. Keeping it separate from kafka_client_cidrs is what lets
    that file hold only the Databricks NAT EIP, which is the part a human
    actually has to look up.
  DESC
  type        = list(string)
  default     = []
}

variable "instance_profile_name" {
  description = "Pre-existing instance profile if the account has one; null otherwise."
  type        = string
  default     = null
}
