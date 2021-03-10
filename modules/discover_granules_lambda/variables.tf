variable "region" {
  type    = string
  default = "us-east-2"
}

variable "prefix" {
  type = string
}

variable "s3_bucket_name" {
  type = string
}

variable "cumulus_lambda_role_arn" {
  type = string
}

variable "lambda_subnet_ids" {
  type = list(string)
}

variable "lambda_security_group_ids" {
  type = list(string)
}

variable "timeout" {
  description = "Lambda function time-out"
  default = 300
}
