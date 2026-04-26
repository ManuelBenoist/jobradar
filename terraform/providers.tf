terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  # Configuration du backend S3 pour stocker l'état de Terraform
  backend "s3" {
    bucket = "jobradar-tfstate-manuel-cloud" # Le nom du bucket créé à l'étape 1
    key    = "terraform.tfstate"
    region = "eu-west-3"
  }
}

provider "aws" {
  region = "eu-west-3" 
}