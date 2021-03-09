variable "aws_profile" {
  type    = string
  default = "SBX"
}

variable "region" {
  type    = string
  default = "us-west-2"
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
  default = null
}

variable "lambda_security_group_ids" {
  type = list(string)
  default = null
}