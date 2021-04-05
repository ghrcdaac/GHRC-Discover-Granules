
locals {
   default_tags = {
      Deployment = var.prefix
   }
}

resource "aws_lambda_function" "discover_granules" {
   function_name = "${var.prefix}-discover-granules-tf-module"
   source_code_hash = filebase64sha256("${path.module}/../../package.zip") 
   handler = "lambda_function.handler"
   runtime = "python3.8"
   filename = "${path.module}/../../package.zip"
   role = var.cumulus_lambda_role_arn
   timeout = var.timeout
   tags = local.default_tags
   layers           = var.layers
   vpc_config {
      security_group_ids = var.lambda_security_group_ids
      subnet_ids = var.lambda_subnet_ids
   }
   environment {
      variables = merge({
         bucket_name = var.s3_bucket_name
         s3_key_prefix = var.s3_key_prefix
      }, var.env_variables)
   }
}

