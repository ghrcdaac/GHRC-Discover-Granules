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

variable "env_variables" {
  type    = map(string)
  default = {}
}

variable "security_group_ids" {
  type    = list(string)
  default = null
}

variable "subnet_ids" {
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

variable "no_return" {
  description = "Can be used in testing so no output is sent to QueueGranules"
  default     = false
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

variable "s3_key_prefix" {
  description = "Path to lookup file"
  default     = "discover-granule/lookup"
}

variable "timeout" {
  description = "Lambda function time-out"
  default     = 900
}

variable "early_return_threshold" {
  description = "If the remaining execution time of the lambda goes below this an early return will be performed."
  default = 30
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

# RDS Configuration
variable "perform_switchover" {
  type = bool
  default = false
}

variable "deprovision_v1" {
  type = bool
  default = false
}

variable "db_identifier" {
  type = string
}

variable "db_instance_class" {
  type = string
  default = "db.t3.micro"
}

variable "db_username" {
  type = string
}

variable "db_allocated_storage" {
  type = number
  default = 5
}

variable "db_type" {
  type = string
}

variable "cumulus_user_credentials_secret_arn" {
  type = string
}

variable "minimum_acu" {
  default = 2
}

variable "maximum_acu" {
  default = 16
}
