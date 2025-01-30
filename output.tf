output "arn" {
  value = module.aws_lambda_function.arn
}

output "gdg_db_credentials_arn" {
  value = module.aws_lambda_function.gdg_db_credentials_arn
}
