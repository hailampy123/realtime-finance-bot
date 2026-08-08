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

variable "ssh_ingress_cidrs" {
  description = <<-DESC
    CIDRs allowed to SSH to the producer host. Empty (the default) means no
    SSH rule at all, so the host stays egress-only. scripts/bootstrap.sh sets
    this to the operator's current /32 for the one step that needs it: the
    producer host is the only thing inside the VPC, so it is the only place
    Kafka ACLs can be written from before public access exists. Deliberately
    not folded into kafka_client_cidrs — the Databricks NAT EIP belongs on the
    broker port, not on port 22.
  DESC
  type        = list(string)
  default     = []
}
