resource "aws_lambda_function" "discover_granules" {
   function_name = "${var.prefix}-discover_granules"
   filename = "./task/dist/package.zip"
   handler = "lambda_function.lambda_handler"
   role = var.cumulus_lambda_role
   runtime = "python3.8"
   timeout = 300 # 5 minutes
   tags = local.default_tags

   vpc_config {
      subnet_ids         = var.lambda_subnet_ids
      security_group_ids = var.lambda_security_group_ids
   }
}

locals {
  default_tags = {
    Deployment = var.prefix
  }
}