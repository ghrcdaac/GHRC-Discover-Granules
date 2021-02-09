terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

provider "aws" {
   region = var.region
   profile = var.aws_profile
}

module "aws_lambda_function" {
   source = "./modules/discover_granules_lambda"
   prefix = var.prefix
   cumulus_lambda_role = var.cumulus_lambda_role
   lambda_subnet_ids = var.lambda_subnet_ids
   lambda_security_group_ids = var.lambda_security_group_ids
}
