locals {
  default_tags = {
    Deployment = var.prefix
  }
  package_name = "ghrc_discover_granules_lambda.zip"
}

resource "aws_lambda_function" "ghrc_discover_granules" {
  function_name = "${var.prefix}-ghrc-discover-granules"
  source_code_hash = filebase64sha256("${path.module}/../../${local.package_name}")
  handler = "task.lambda_function.handler"
  runtime = "python3.10"
  filename = "${path.module}/../../${local.package_name}"
  role = var.cumulus_lambda_role_arn
  timeout = var.timeout
  memory_size = var.memory_size
  tags = local.default_tags

  environment {
    variables = merge({
      ignore_discovered = var.ignore_discovered
      bucket_name = var.s3_bucket_name
      s3_key_prefix = var.s3_key_prefix
      db_type = var.db_type
      early_return_threshold = var.early_return_threshold
      sqlite_transaction_size = var.sqlite_transaction_size
      sqlite_temp_store = var.sqlite_temp_store
      sqlite_cache_size = var.sqlite_cache_size
      postgresql_secret_arn = length(aws_secretsmanager_secret.gdg_db_credentials) > 0 ? aws_secretsmanager_secret.gdg_db_credentials[0].arn : ""
      cumulus_credentials_arn = var.cumulus_user_credentials_secret_arn
    }, var.env_variables)
  }

  vpc_config {
    security_group_ids = var.security_group_ids
    subnet_ids = var.subnet_ids
  }

  depends_on = [
    aws_secretsmanager_secret.gdg_db_credentials]
}

resource "aws_iam_policy" "ssm_policy" {
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

resource "aws_iam_role_policy_attachment" "glm_ssm_policy_attach" {
  policy_arn = aws_iam_policy.ssm_policy.arn
  role = var.cumulus_lambda_role_name
}

resource "aws_iam_policy" "cumulus_secrets_manager_read" {
  count = var.cumulus_user_credentials_secret_arn != null ? 1 : 0
  policy = jsonencode(
  {
    Version = "2012-10-17"
    "Statement" = [
      {
        Effect = "Allow",
        Action = ["secretsmanager:GetSecretValue"],
        Resource = [
          var.cumulus_user_credentials_secret_arn]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "cumulus_secrets_manager_policy_attach" {
  count = var.cumulus_user_credentials_secret_arn != null ? 1 : 0
  policy_arn = aws_iam_policy.cumulus_secrets_manager_read[0].arn
  role = var.cumulus_lambda_role_name
}


### Postgresql Configuration ###

resource "aws_iam_policy" "secrets_manager_read" {
  count = (var.db_type == "postgresql") ? 1 : 0
  policy = jsonencode(
  {
    Version = "2012-10-17"
    "Statement" = [
      {
        Effect = "Allow",
        Action = [
          "secretsmanager:GetSecretValue"],
        Resource = [
          aws_secretsmanager_secret.gdg_db_credentials[0].arn]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "secrets_manager_policy_attach" {
  count = (var.db_type == "postgresql") ? 1 : 0
  policy_arn = aws_iam_policy.secrets_manager_read[0].arn
  role = var.cumulus_lambda_role_name
}

resource "aws_db_subnet_group" "gdg-db-subnet-group" {
  count = (var.db_type == "postgresql") ? 1 : 0
  name = "${var.prefix}-${var.db_identifier}-subnet-group"
  subnet_ids = var.subnet_ids

  tags = {
    Name = "${var.prefix}-${var.db_identifier}-subnet-group"
  }
}

resource "aws_rds_cluster" "gdg_db_cluster_v2" {
  count = (var.db_type == "postgresql") ? 1 : 0
  cluster_identifier = "${var.prefix}-${var.db_identifier}-cluster-v2"
  engine = "aurora-postgresql"
  engine_mode = "provisioned"
  engine_version = "13.12"
  enable_http_endpoint = true

  serverlessv2_scaling_configuration {
      max_capacity             = var.maximum_acu
      min_capacity             = var.minimum_acu
    }

  database_name = var.db_identifier
  master_username = var.db_username
  master_password = random_password.master_password[0].result
  backup_retention_period = 1
  db_subnet_group_name = aws_db_subnet_group.gdg-db-subnet-group[0].name
  skip_final_snapshot = true
  apply_immediately = true
  vpc_security_group_ids = var.security_group_ids

}

resource "aws_rds_cluster_instance" "gdg_db_cluster_instance" {
  identifier = "${var.prefix}-${var.db_identifier}-instance-1"
  cluster_identifier = aws_rds_cluster.gdg_db_cluster_v2[0].id
  instance_class     = "db.serverless"
  engine             = aws_rds_cluster.gdg_db_cluster_v2[0].engine
  engine_version     = aws_rds_cluster.gdg_db_cluster_v2[0].engine_version_actual
}

resource "random_password" "master_password" {
  count = (var.db_type == "postgresql") ? 1 : 0
  length = 16
  special = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "aws_secretsmanager_secret" "gdg_db_credentials" {
  count = (var.db_type == "postgresql") ? 1 : 0
  recovery_window_in_days = 0
  name = "${var.prefix}-${var.db_identifier}-credentials"
}

resource "aws_secretsmanager_secret_version" "gdg_db_credentials" {
  count = (var.db_type == "postgresql") ? 1 : 0
  depends_on = [
    aws_secretsmanager_secret.gdg_db_credentials,
    aws_rds_cluster.gdg_db_cluster_v2,
    random_password.master_password
  ]
  secret_id = aws_secretsmanager_secret.gdg_db_credentials[0].id

  secret_string = jsonencode({
    "user": var.db_username,
    "password": random_password.master_password[0].result,
    "host": aws_rds_cluster.gdg_db_cluster_v2[0].endpoint,
    "port": aws_rds_cluster.gdg_db_cluster_v2[0].port,
    "database": var.db_identifier
  })
}
