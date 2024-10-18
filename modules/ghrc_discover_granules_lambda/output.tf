output "arn" {
  value = aws_lambda_function.ghrc_discover_granules.arn
}

output "gdg_db_credentials_arn" {
  value = one(aws_secretsmanager_secret.gdg_db_credentials[*].id)
}