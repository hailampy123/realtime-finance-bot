output "bootstrap_brokers_public" {
  value = module.kafka.bootstrap_brokers_sasl_scram_public
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
