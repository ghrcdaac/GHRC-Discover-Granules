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
  layers                         = var.layers
  reserved_concurrent_executions = 1

  vpc_config {
    security_group_ids = var.lambda_security_group_ids
    subnet_ids         = var.lambda_subnet_ids
  }

  environment {
    variables = merge({
      bucket_name   = var.s3_bucket_name
      s3_key_prefix = var.s3_key_prefix
      efs_path      = var.efs_mount_path
    }, var.env_variables)
  }

  file_system_config {
    local_mount_path = var.efs_mount_path
    arn              = var.efs_arn
  }
}
