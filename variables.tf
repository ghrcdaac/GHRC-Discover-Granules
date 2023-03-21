variable "aws_profile" {
  type    = string
  default = "SBX"
}

variable "cumulus_lambda_role_arn" {
  type = string
}

variable "cumulus_lambda_role_name" {
  type = string
}

variable "efs_arn" {
  type = string
}

variable "efs_mount_path" {
  type = string
}

variable "env_variables" {
  type    = map(string)
  default = {}
}

variable "lambda_security_group_ids" {
  type    = list(string)
  default = null
}

variable "lambda_subnet_ids" {
  type    = list(string)
  default = null
}

variable "layers" {
  type    = list(string)
  default = []
}

variable "memory_size" {
  description = "Lambda RAM limit"
  default     = 2048
}

variable "prefix" {
  type = string
}

variable "region" {
  type    = string
  default = "us-west-2"
}

variable "s3_bucket_name" {
  type = string
}

variable "timeout" {
  description = "Lambda function time-out"
  default     = 900
}

# Sqlite Configuration
variable "sqlite_transaction_size" {
  type = number
  default = 100000
}

variable "sqlite_temp_store" {
  type = number
  default = 0
}

variable "sqlite_cache_size" {
  type = number
  default = (-1 * 64000)
}
