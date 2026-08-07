terraform {
  required_version = ">= 1.9.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
      Ephemeral = "true"
    }
  }
}

module "network" {
  source             = "../../modules/network"
  project            = var.project
  kafka_client_cidrs = var.kafka_client_cidrs
}

module "kafka" {
  source            = "../../modules/kafka_msk"
  project           = var.project
  subnet_ids        = module.network.public_subnet_ids
  security_group_id = module.network.msk_security_group_id
  public_access     = var.msk_public_access
}

module "producer_host" {
  source                = "../../modules/producer_host"
  project               = var.project
  subnet_id             = module.network.public_subnet_ids[0]
  security_group_id     = module.network.producer_security_group_id
  repo_url              = var.repo_url
  repo_ref              = var.repo_ref
  bootstrap_servers     = module.kafka.bootstrap_brokers_sasl_scram
  sasl_username         = module.kafka.sasl_username
  sasl_password         = module.kafka.sasl_password
  instance_profile_name = var.instance_profile_name
}
