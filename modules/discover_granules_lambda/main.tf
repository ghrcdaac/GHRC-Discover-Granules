resource "aws_lambda_function" "discover_granules" {
   function_name = "${prefix}-discover_granules"
   handler = "lambda_function.lambda_handler"
   runtime = "python3.8"
   filename = "./task/dist/package.zip"
   role = aws_iam_role.lambda_exec.arn
   timeout = 300

   tags = local.default_tags
}

resource "aws_iam_role" "lambda_exec" {
   name = "aws_iam_lambda"
   assume_role_policy = cumulus_lambda_role
}

locals {
   default_tags = {
      Deployment = prefix
   }
}

