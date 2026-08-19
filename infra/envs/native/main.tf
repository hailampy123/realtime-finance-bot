# A wiped account still bills for the hours it ran, and per-GB streaming charges
# are the one line item here that scales with traffic rather than with time.
resource "aws_budgets_budget" "monthly" {
  name         = "${var.project}-native-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_notification_email]
  }
}

module "network" {
  source  = "../../modules/native_network"
  project = var.project
}

module "lakehouse" {
  source     = "../../modules/native_lakehouse"
  project    = var.project
  account_id = data.aws_caller_identity.current.account_id
}

module "stream" {
  source             = "../../modules/native_stream"
  project            = var.project
  region             = data.aws_region.current.name
  account_id         = data.aws_caller_identity.current.account_id
  lake_bucket_arn    = module.lakehouse.bucket_arn
  glue_database_name = module.lakehouse.glue_database_name
  bronze_table_name  = module.lakehouse.bronze_table_name
  bronze_prefix      = module.lakehouse.bronze_prefix
}

module "medallion" {
  source     = "../../modules/native_medallion"
  project    = var.project
  region     = data.aws_region.current.name
  account_id = data.aws_caller_identity.current.account_id

  lake_bucket_arn       = module.lakehouse.bucket_arn
  glue_database_name    = module.lakehouse.glue_database_name
  athena_workgroup_name = module.lakehouse.athena_workgroup_name

  # The merge SQL has one home, awsnative/sql, read from here and from
  # awsnative/render.py. path.root is infra/envs/native.
  sql_dir = abspath("${path.root}/../../../awsnative/sql")

  lookback_days       = var.microbatch_lookback_days
  schedule_expression = var.microbatch_schedule
  schedule_enabled    = var.microbatch_enabled
}

module "producer" {
  source            = "../../modules/native_producer"
  project           = var.project
  region            = data.aws_region.current.name
  account_id        = data.aws_caller_identity.current.account_id
  subnet_ids        = module.network.public_subnet_ids
  security_group_id = module.network.egress_security_group_id
  stream_arn        = module.stream.stream_arn
  stream_name       = module.stream.stream_name
}

# Slices E1 and E3. Two scheduled Lambdas, no stream and no secret -- see the
# module header for why each of those is absent rather than forgotten.
module "enrichment" {
  source                = "../../modules/native_enrichment"
  project               = var.project
  region                = var.region
  account_id            = data.aws_caller_identity.current.account_id
  lake_bucket_name      = module.lakehouse.bucket_name
  lake_bucket_arn       = module.lakehouse.bucket_arn
  glue_database_name    = module.lakehouse.glue_database_name
  athena_workgroup_name = module.lakehouse.athena_workgroup_name
  source_dir            = abspath("${path.root}/../../..")

  # The merge SQL has one home, awsnative/sql, same as module "medallion" above.
  sql_dir = abspath("${path.root}/../../../awsnative/sql")

  instrument_pairs  = var.enrichment_instrument_pairs
  schedules_enabled = var.enrichment_enabled
}
