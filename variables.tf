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

variable "efs_access_point_arn" {
  type = string
}

variable "efs_path" {
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

# RDS Configuration
variable "db_identifier" {
  type = string
  default = "dgdb"
}

variable "db_instance_class" {
  type = string
  default = "db.t3.micro"
}

variable "db_username" {
  type = string
  default = "dgdbadmin"
}

variable "db_allocated_storage" {
  type = number
  default = 5
}

variable "db_type" {
  type = string

  validation {
    condition = contains(["postgresql", "sqlite", "cumulus"], var.db_type)
    error_message = "The variable db_type must be one of: postgresql, sqlite, or cumulus."
  }
}

variable "user_credentials_secret_arn" {
  type = string
  default = null
}
