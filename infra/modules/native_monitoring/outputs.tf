output "health_metrics_function_arn" {
  value = aws_lambda_function.health_metrics.arn
}

output "health_metrics_function_name" {
  value = aws_lambda_function.health_metrics.function_name
}

output "alert_topic_arn" {
  value = aws_sns_topic.alerts.arn
}
