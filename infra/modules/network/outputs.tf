output "vpc_id" {
  value = aws_vpc.this.id
}

output "public_subnet_ids" {
  value = aws_subnet.public[*].id
}

output "msk_security_group_id" {
  value = aws_security_group.msk.id
}

output "producer_security_group_id" {
  value = aws_security_group.producer.id
}
