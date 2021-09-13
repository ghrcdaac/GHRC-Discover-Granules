terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

module "aws_lambda_function" {
   source = "./modules/discover_granules_lambda"
   prefix = var.prefix
   region = var.region
   s3_bucket_name = var.s3_bucket_name
   cumulus_lambda_role_arn = var.cumulus_lambda_role_arn
   lambda_subnet_ids = var.lambda_subnet_ids
   lambda_security_group_ids = var.lambda_security_group_ids
   env_variables = var.env_variables
   layers = var.layers
   timeout = var.timeout
   memory_size = var.memory_size
}
