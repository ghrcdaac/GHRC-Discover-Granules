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
}
