output "stream_name" {
  value = aws_kinesis_stream.trades.name
}

output "stream_arn" {
  value = aws_kinesis_stream.trades.arn
}

output "firehose_name" {
  value = aws_kinesis_firehose_delivery_stream.bronze.name
}

output "firehose_log_group" {
  value = aws_cloudwatch_log_group.firehose.name
}
