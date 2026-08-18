output "state_machine_arn" {
  value = aws_sfn_state_machine.microbatch.arn
}

output "state_machine_name" {
  value = aws_sfn_state_machine.microbatch.name
}

output "log_group" {
  value = aws_cloudwatch_log_group.sfn.name
}

output "schedule_name" {
  value = aws_scheduler_schedule.microbatch.name
}

output "schedule_enabled" {
  value = var.schedule_enabled
}
