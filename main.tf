terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

module "aws_lambda_function" {
  source                    = "./modules/ghrc_discover_granules_lambda"
  prefix                    = var.prefix
  region                    = var.region
  s3_bucket_name            = var.s3_bucket_name
  cumulus_lambda_role_arn   = var.cumulus_lambda_role_arn
  cumulus_lambda_role_name  = var.cumulus_lambda_role_name
  security_group_ids        = var.security_group_ids
  subnet_ids                = var.subnet_ids
  env_variables             = var.env_variables
  layers                    = var.layers
  timeout                   = var.timeout
  memory_size               = var.memory_size

  # DB Config
  db_type = var.db_type

  # Cumulus
  cumulus_user_credentials_secret_arn = var.cumulus_user_credentials_secret_arn

  # Sqlite
  sqlite_transaction_size = var.sqlite_transaction_size
  sqlite_temp_store = var.sqlite_temp_store
  sqlite_cache_size = var.sqlite_cache_size

  # RDS Config
  db_identifier             = var.db_identifier
  db_instance_class         = var.db_instance_class
  db_allocated_storage      = var.db_allocated_storage
  db_username               = var.db_username
}
