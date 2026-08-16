output "ecr_repository_url" {
  value = aws_ecr_repository.producer.repository_url
}

output "ecr_repository_name" {
  value = aws_ecr_repository.producer.name
}

output "cluster_name" {
  value = aws_ecs_cluster.this.name
}

output "service_name" {
  value = aws_ecs_service.producer.name
}

output "log_group" {
  value = aws_cloudwatch_log_group.producer.name
}
