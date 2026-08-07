output "bootstrap_brokers_sasl_scram" {
  value = aws_msk_cluster.this.bootstrap_brokers_sasl_scram
}

output "bootstrap_brokers_sasl_scram_public" {
  value = aws_msk_cluster.this.bootstrap_brokers_public_sasl_scram
}

output "sasl_username" {
  value = "${var.project}-producer"
}

output "sasl_password" {
  value     = random_password.sasl.result
  sensitive = true
}

output "cluster_arn" {
  value = aws_msk_cluster.this.arn
}
