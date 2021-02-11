variable "aws_profile" {
  type    = string
  default = "default"
}

variable "region" {
  type    = string
  default = "us-east-2"
}

variable "prefix" {
  type = string
  default = null
}

variable "cumulus_lambda_role_arn" {
  type = string
  default = null
}

variable "lambda_subnet_ids" {
  type = list(string)
  default = null
}

variable "lambda_security_group_ids" {
  type = list(string)
  default = null
}