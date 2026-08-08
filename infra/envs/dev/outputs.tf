output "bootstrap_brokers_public" {
  description = <<-DESC
    Can read as empty for a short window right after public access is
    enabled: Terraform's provider calls GetBootstrapBrokers exactly once, at
    the moment the connectivity update finishes, and AWS does not always have
    the public endpoint ready by then. scripts/bootstrap.sh does not trust
    this output for that reason -- it polls the AWS CLI directly (see
    cluster_arn below) until the value is non-empty before using it.
  DESC
  value       = module.kafka.bootstrap_brokers_sasl_scram_public
}

output "bootstrap_brokers_private" {
  value = module.kafka.bootstrap_brokers_sasl_scram
}

output "sasl_username" {
  value = module.kafka.sasl_username
}

output "sasl_password" {
  value     = module.kafka.sasl_password
  sensitive = true
}

output "producer_public_ip" {
  value = module.producer_host.public_ip
}

output "producer_ssh_key_path" {
  description = "Private key for the producer host. Gitignored; regenerated every make up."
  value       = local_sensitive_file.producer_key.filename
}

output "cluster_arn" {
  description = "Used by scripts/bootstrap.sh to poll `aws kafka get-bootstrap-brokers` directly."
  value       = module.kafka.cluster_arn
}
