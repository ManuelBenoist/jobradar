# ============================================================================
# TERRAFORM & PROVIDERS CONFIGURATION
# ============================================================================

terraform {
  required_version = ">= 1.5.0" # Garantit la compatibilité des fonctionnalités utilisées

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7.1"
    }
  }

  # Stockage distant de l'état (State) avec verrouillage DynamoDB
  backend "s3" {
    bucket         = "jobradar-tfstate-manuel-cloud"
    key            = "terraform.tfstate"
    region         = "eu-west-3"
    dynamodb_table = "jobradar-tfstate-locks"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "JobRadar"
      ManagedBy = "Terraform"
      Owner     = "Manuel"
    }
  }
}