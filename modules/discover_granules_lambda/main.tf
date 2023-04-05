locals {
  default_tags = {
    Deployment = var.prefix
  }
}

resource "aws_lambda_function" "discover_granules" {
  function_name                  = "${var.prefix}-discover-granules-tf-module"
  source_code_hash               = filebase64sha256("${path.module}/../../package.zip")
  handler                        = "task.lambda_function.handler"
  runtime                        = "python3.8"
  filename                       = "${path.module}/../../package.zip"
  role                           = var.cumulus_lambda_role_arn
  timeout                        = var.timeout
  memory_size                    = var.memory_size
  tags                           = local.default_tags

  vpc_config {
    security_group_ids = var.security_group_ids
    subnet_ids         = var.subnet_ids
  }

  environment {
    variables = merge({
      bucket_name   = var.s3_bucket_name
      s3_key_prefix = var.s3_key_prefix
      efs_path      = var.efs_mount_path
      sqlite_transaction_size = var.sqlite_transaction_size
      sqlite_temp_store = var.sqlite_temp_store
      sqlite_cache_size = var.sqlite_cache_size
      postgresql_secret_arn = length(aws_secretsmanager_secret.dg_db_credentials) > 0 ? aws_secretsmanager_secret.dg_db_credentials[0].arn : ""
    }, var.env_variables)
  }

  depends_on = [aws_secretsmanager_secret.dg_db_credentials]
}

resource "aws_iam_policy" "ssm_test" {
  policy = jsonencode(
  {
    Version = "2012-10-17"
    "Statement" = [
      {
        Effect = "Allow",
        Action = "ssm:GetParameter",
        Resource = [
          "arn:aws:ssm:*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glm-ssm-policy-attach" {
  policy_arn = aws_iam_policy.ssm_test.arn
  role = var.cumulus_lambda_role_name
}

resource "aws_db_subnet_group" "dg-db-subnet-group" {
  count = (var.db_type == "postgresql") ? 1 : 0
  name = "dg-db-subnet-group"
  subnet_ids = var.subnet_ids

  tags = {
    Name = "dg-db-subnet-group"
  }
}

resource "aws_rds_cluster" "dg_db_cluster" {
  count = (var.db_type == "postgresql") ? 1 : 0
  cluster_identifier      = "dg-db-cluster"
  engine                  = "aurora-postgresql"
  engine_mode             = "serverless"
  enable_http_endpoint    = true
  scaling_configuration {
    min_capacity = 2
  }

  database_name           = var.db_identifier
  master_username         = var.db_username
  master_password         = random_password.master_password[0].result
  backup_retention_period = 1
  db_subnet_group_name    = aws_db_subnet_group.dg-db-subnet-group[0].name
  skip_final_snapshot     = true
  apply_immediately       = true
  vpc_security_group_ids  = var.security_group_ids
}

resource "random_password" "master_password" {
  count = (var.db_type == "postgresql") ? 1 : 0
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "aws_secretsmanager_secret" "dg_db_credentials" {
  count = (var.db_type == "postgresql") ? 1 : 0
  recovery_window_in_days = 0
  name = "dg_db_credentials"
}

resource "aws_secretsmanager_secret_version" "dg_db_credentials" {
  count = (var.db_type == "postgresql") ? 1 : 0
  depends_on = [
    aws_secretsmanager_secret.dg_db_credentials,
    aws_rds_cluster.dg_db_cluster,
    random_password.master_password
  ]
  secret_id = aws_secretsmanager_secret.dg_db_credentials[0].id
  secret_string = jsonencode({
    "username": aws_rds_cluster.dg_db_cluster[0].master_username,
    "password": random_password.master_password[0].result,
    "host": aws_rds_cluster.dg_db_cluster[0].endpoint,
    "port": aws_rds_cluster.dg_db_cluster[0].port,
    "database": aws_rds_cluster.dg_db_cluster[0].database_name
  })
}
