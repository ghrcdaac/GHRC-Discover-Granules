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

  vpc_config {
    security_group_ids = var.lambda_security_group_ids
    subnet_ids         = var.lambda_subnet_ids
  }

  environment {
    variables = merge({
      bucket_name   = var.s3_bucket_name
      s3_key_prefix = var.s3_key_prefix
      efs_path      = var.efs_mount_path
      no_return     = var.no_return
    }, var.env_variables)
  }

  file_system_config {
    local_mount_path = var.efs_mount_path
    arn              = var.efs_arn
  }
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
          var.access_key_id_glm_arn,
          var.aws_secret_key_glm_arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glm-ssm-policy-attach" {
  policy_arn = aws_iam_policy.ssm_test.arn
  role = var.cumulus_lambda_role_name
}
