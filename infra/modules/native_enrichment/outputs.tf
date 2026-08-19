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

output "merge_perp_state_machine_arn" {
  value = aws_sfn_state_machine.merge_perp.arn
}

output "merge_macro_state_machine_arn" {
  value = aws_sfn_state_machine.merge_macro.arn
}

output "merge_perp_log_group" {
  value = aws_cloudwatch_log_group.merge_perp.name
}

output "merge_macro_log_group" {
  value = aws_cloudwatch_log_group.merge_macro.name
}
