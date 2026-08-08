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
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
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
  source  = "../../modules/network"
  project = var.project
  # The operator's IP is detected at run time rather than written down, so a
  # laptop that changed networks does not surface as a broker timeout.
  kafka_client_cidrs = distinct(concat(var.kafka_client_cidrs, var.operator_cidrs))
  ssh_ingress_cidrs  = var.operator_cidrs
}

module "kafka" {
  source            = "../../modules/kafka_msk"
  project           = var.project
  subnet_ids        = module.network.public_subnet_ids
  security_group_id = module.network.msk_security_group_id
  public_access     = var.msk_public_access
  restrict_acls     = var.msk_restrict_acls
}

# Generated rather than supplied: the account is wiped weekly, so a key the
# operator manages by hand is one more thing to re-create every cycle. The
# private key is written next to the state it belongs to and gitignored; it
# grants shell on an ephemeral host that holds nothing the Terraform state
# does not already hold in plaintext.
resource "tls_private_key" "producer" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "producer" {
  key_name   = "${var.project}-producer"
  public_key = tls_private_key.producer.public_key_openssh
}

resource "local_sensitive_file" "producer_key" {
  filename        = "${path.root}/.ssh/${var.project}-producer.pem"
  content         = tls_private_key.producer.private_key_pem
  file_permission = "0600"
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
  key_name              = aws_key_pair.producer.key_name
}
