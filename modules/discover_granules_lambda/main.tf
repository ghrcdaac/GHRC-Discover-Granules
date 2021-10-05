locals {
  default_tags = {
    Deployment = var.prefix
  }
}

resource "aws_lambda_function" "discover_granules" {
  function_name    = "${var.prefix}-discover-granules-tf-module"
  source_code_hash = filebase64sha256("${path.module}/../../package.zip")
  handler          = "task.lambda_function.handler"
  runtime          = "python3.8"
  filename         = "${path.module}/../../package.zip"
  role             = var.cumulus_lambda_role_arn
  timeout          = var.timeout
  memory_size      = var.memory_size
  tags             = local.default_tags
  layers           = var.layers
  vpc_config {
    security_group_ids = var.lambda_security_group_ids
    subnet_ids         = var.lambda_subnet_ids
  }
  environment {
    variables = merge({
      bucket_name   = var.s3_bucket_name
      s3_key_prefix = var.s3_key_prefix
      table_name    = aws_dynamodb_table.discover-granules-lock.name
    }, var.env_variables)
  }
}

resource "aws_dynamodb_table" "discover-granules-lock" {
  name           = "${var.prefix}-DiscoverGranulesLock"
  billing_mode   = "PROVISIONED"
  read_capacity  = 20
  write_capacity = 20
  hash_key       = "DatabaseLocked"

  attribute {
    name = "DatabaseLocked"
    type = "S"
  }

  ttl {
    enabled = true
    attribute_name = "LockDuration"
  }
}

resource "aws_iam_policy" "dynamodb_put_delete_item" {
  name        = "dynamo-record-management"
  description = "Allows for the insertion and deletion of a record."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Resource = aws_dynamodb_table.discover-granules-lock.arn
        Action = [
          "dynamodb:PutItem",
          "dynamodb:Delete*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_dynamo_put_delete" {
  role       = var.cumulus_lambda_role_name
  policy_arn = aws_iam_policy.dynamodb_put_delete_item.arn
}

