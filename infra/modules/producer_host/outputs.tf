output "instance_id" {
  value = aws_instance.producer.id
}

output "public_ip" {
  value = aws_instance.producer.public_ip
}
