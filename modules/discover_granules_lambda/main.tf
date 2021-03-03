resource "null_resource" "temp" {
   triggers = {
      path = "${path.module}/../../discover-granules-tf-module.zip"
   }
}

resource "aws_lambda_function" "discover_granules" {
   function_name = "${var.prefix}-discover_granules-tf-module"
   handler = "lambda_function.lambda_handler"
   runtime = "python3.8"
   filename = "${path.module}/../../discover-granules-tf-module.zip"
   role = var.cumulus_lambda_role_arn
   timeout = 300
   tags = local.default_tags
   vpc_config {
      security_group_ids = var.lambda_security_group_ids
      subnet_ids = var.lambda_subnet_ids
   }
   environment {
      variables = {
         bucket_name = var.s3_bucket_name
         prefix = var.prefix
      }
   }
}

locals {
   default_tags = {
      Deployment = var.prefix
   }
}