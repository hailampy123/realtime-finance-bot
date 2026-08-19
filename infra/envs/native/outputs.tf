output "region" {
  value = data.aws_region.current.name
}

output "account_id" {
  value = data.aws_caller_identity.current.account_id
}

output "vpc_id" {
  value = module.network.vpc_id
}

output "lake_bucket" {
  value = module.lakehouse.bucket_name
}

output "glue_database" {
  value = module.lakehouse.glue_database_name
}

output "athena_workgroup" {
  value = module.lakehouse.athena_workgroup_name
}

output "stream_name" {
  value = module.stream.stream_name
}

output "firehose_log_group" {
  value = module.stream.firehose_log_group
}

output "ecr_repository_url" {
  value = module.producer.ecr_repository_url
}

output "ecs_cluster" {
  value = module.producer.cluster_name
}

output "ecs_service" {
  value = module.producer.service_name
}

output "producer_log_group" {
  value = module.producer.log_group
}

output "microbatch_state_machine_arn" {
  value = module.medallion.state_machine_arn
}

output "microbatch_state_machine" {
  value = module.medallion.state_machine_name
}

output "microbatch_log_group" {
  value = module.medallion.log_group
}

output "microbatch_schedule" {
  value = module.medallion.schedule_name
}

output "enrichment_perp_function" {
  value = module.enrichment.perp_function_name
}

output "enrichment_macro_function" {
  value = module.enrichment.macro_function_name
}

output "enrichment_perp_log_group" {
  value = module.enrichment.perp_log_group
}

output "enrichment_macro_log_group" {
  value = module.enrichment.macro_log_group
}
