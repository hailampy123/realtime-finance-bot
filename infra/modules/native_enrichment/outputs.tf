output "perp_function_name" {
  value = aws_lambda_function.perp.function_name
}

output "macro_function_name" {
  value = aws_lambda_function.macro.function_name
}

output "perp_log_group" {
  value = aws_cloudwatch_log_group.perp.name
}

output "macro_log_group" {
  value = aws_cloudwatch_log_group.macro.name
}
