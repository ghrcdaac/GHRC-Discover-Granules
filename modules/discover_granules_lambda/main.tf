resource "aws_lambda_function" "discover_granules" {
   function_name = "discover_granules"
   handler = "lambda_function.lambda_handler"
   runtime = "python3.8"
   filename = "./task/dist/package.zip"
   role = aws_iam_role.lambda_exec.arn
}

resource "aws_iam_role" "lambda_exec" {
   name = "aws_iam_lambda"
   assume_role_policy = file("./modules/discover_granules_lambda/policy.json")
}
