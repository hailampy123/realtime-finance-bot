terraform {
  required_version = ">= 1.9.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # Builds the enrichment Lambda zip in-plan. The package is a few kilobytes of
    # pure standard-library Python, which is the whole reason no build step or
    # container image is needed here.
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
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

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
